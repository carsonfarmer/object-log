use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use bytes::Bytes;

use crate::Error;

#[path = "request_guard.rs"]
mod request_guard;

pub(crate) const LIVE_BYTES: usize = 88 * 1024 * 1024;
pub(crate) const STATE_BYTES: usize = 24 * 1024 * 1024;
// Fixed metadata allowance plus bounded pack passes, shared by the one retry.
pub(crate) const CALLS: usize = 512 + 12 * crate::MAX_STREAM_PACK_BYTES.div_ceil(1024 * 1024);
// Two complete 1,024-entry materialization/publication attempts plus head and
// collection-plan requests, plus the serving allowance for pack passes.
// All byte, work, state, and live limits stay shared.
const MAINTENANCE_CALLS: usize = 8192 + CALLS;
pub(crate) const TRANSFER_BYTES: u64 = 12 * crate::MAX_STREAM_PACK_BYTES as u64;
pub(crate) const WORK_BYTES: u64 = 24 * crate::MAX_STREAM_PACK_BYTES as u64;
pub(crate) const THIN_ROUNDS: usize = 32;
pub(crate) const RETRIES: usize = 1;
const MEMORY_LIMIT: &str = "Git live-memory limit exceeded";

static POOL: OnceLock<Pool> = OnceLock::new();

#[derive(Clone)]
pub(crate) struct Pool(Arc<PoolState>);

struct PoolState {
    active: AtomicBool,
    live: AtomicUsize,
    limit: usize,
}

impl Pool {
    pub(crate) fn shared() -> &'static Self {
        POOL.get_or_init(|| {
            Self(Arc::new(PoolState {
                active: AtomicBool::new(false),
                live: AtomicUsize::new(0),
                limit: LIVE_BYTES,
            }))
        })
    }

    #[cfg(test)]
    pub(crate) fn new(limit: usize) -> Self {
        Self(Arc::new(PoolState {
            active: AtomicBool::new(false),
            live: AtomicUsize::new(0),
            limit,
        }))
    }

    pub(crate) fn admit(&self) -> Result<Operation, Error> {
        self.admit_with_calls(CALLS)
    }

    pub(crate) fn admit_maintenance(&self) -> Result<Operation, Error> {
        self.admit_with_calls(MAINTENANCE_CALLS)
    }

    fn admit_with_calls(&self, call_limit: usize) -> Result<Operation, Error> {
        if self
            .0
            .active
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(Error::Busy);
        }
        Ok(Operation(Arc::new(OperationState {
            pool: self.0.clone(),
            io: Mutex::new(IoUsage::default()),
            call_limit,
            work: AtomicU64::new(0),
            thin_rounds: AtomicUsize::new(0),
            retries: AtomicUsize::new(0),
            state: AtomicUsize::new(0),
        })))
    }
}

#[derive(Default)]
struct IoUsage {
    calls: usize,
    transfer: u64,
}

struct OperationState {
    pool: Arc<PoolState>,
    io: Mutex<IoUsage>,
    call_limit: usize,
    work: AtomicU64,
    thin_rounds: AtomicUsize,
    retries: AtomicUsize,
    state: AtomicUsize,
}
impl Drop for OperationState {
    fn drop(&mut self) {
        self.pool.active.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
pub(crate) struct Operation(Arc<OperationState>);

impl Operation {
    pub(crate) fn call_limit(&self) -> usize {
        self.0.call_limit
    }

    pub(crate) fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(crate) fn io(&self, bytes: impl TryInto<u64>) -> Result<(), Error> {
        let bytes = bytes
            .try_into()
            .map_err(|_| Error::InvalidPack("transfer amount exceeds u64".into()))?;
        let mut usage = self
            .0
            .io
            .lock()
            .map_err(|_| Error::InvalidPack("object-log admission lock poisoned".into()))?;
        let calls = usage
            .calls
            .checked_add(1)
            .filter(|calls| *calls <= self.0.call_limit)
            .ok_or_else(|| Error::InvalidPack("object-log call limit exceeded".into()))?;
        let transfer = usage
            .transfer
            .checked_add(bytes)
            .filter(|transfer| *transfer <= TRANSFER_BYTES)
            .ok_or_else(|| Error::InvalidPack("object-log transfer limit exceeded".into()))?;
        // Commit both cumulative counters together. A refusal consumes neither;
        // cancellation after successful admission refunds neither.
        *usage = IoUsage { calls, transfer };
        Ok(())
    }

    pub(crate) fn work(&self, bytes: impl TryInto<u64>) -> Result<(), Error> {
        let bytes = bytes
            .try_into()
            .map_err(|_| Error::InvalidPack("work amount exceeds u64".into()))?;
        self.0
            .work
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current
                    .checked_add(bytes)
                    .filter(|&next| next <= WORK_BYTES)
            })
            .map(|_| ())
            .map_err(|_| Error::InvalidPack("Git work limit exceeded".into()))
    }

    pub(crate) fn thin_round(&self) -> Result<(), Error> {
        charge(
            &self.0.thin_rounds,
            1,
            THIN_ROUNDS,
            "thin-pack round limit exceeded",
        )
    }

    pub(crate) fn retry(&self) -> Result<(), Error> {
        charge(&self.0.retries, 1, RETRIES, "Git retry limit exceeded")
    }

    pub(crate) fn reserve(&self, bytes: usize) -> Result<Reservation, Error> {
        charge(&self.0.pool.live, bytes, self.0.pool.limit, MEMORY_LIMIT)?;
        Ok(Reservation {
            operation: self.0.clone(),
            bytes,
            state: false,
        })
    }

    pub(crate) fn reserve_state(&self, bytes: usize) -> Result<Reservation, Error> {
        let mut reservation = self.reserve(bytes)?;
        charge(
            &self.0.state,
            bytes,
            STATE_BYTES,
            "Git state-memory limit exceeded",
        )?;
        reservation.state = true;
        Ok(reservation)
    }

    pub(crate) fn has_headroom(&self, calls: usize, transfer: usize, work: usize) -> bool {
        let Ok(usage) = self.0.io.lock() else {
            return false;
        };
        usage
            .calls
            .checked_add(calls)
            .is_some_and(|value| value <= self.0.call_limit)
            && usage
                .transfer
                .checked_add(transfer as u64)
                .is_some_and(|value| value <= TRANSFER_BYTES)
            && self
                .0
                .work
                .load(Ordering::Relaxed)
                .checked_add(work as u64)
                .is_some_and(|value| value <= WORK_BYTES)
    }

    #[cfg(test)]
    pub(crate) fn calls(&self) -> usize {
        self.0
            .io
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .calls
    }

    #[cfg(test)]
    pub(crate) fn work_bytes(&self) -> u64 {
        self.0.work.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn live_bytes(&self) -> usize {
        self.0.pool.live.load(Ordering::Relaxed)
    }
}

fn charge(
    counter: &AtomicUsize,
    amount: usize,
    limit: usize,
    message: &'static str,
) -> Result<(), Error> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(amount).filter(|next| *next <= limit)
        })
        .map(|_| ())
        .map_err(|_| Error::InvalidPack(message.into()))
}

pub(crate) struct Reservation {
    operation: Arc<OperationState>,
    bytes: usize,
    state: bool,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.state {
            self.operation
                .state
                .fetch_sub(self.bytes, Ordering::Relaxed);
        }
        self.operation
            .pool
            .live
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

impl Reservation {
    pub(crate) fn grow(&mut self, bytes: usize) -> Result<(), Error> {
        let next = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| Error::InvalidPack("Git live-memory size overflowed".into()))?;
        let pool = &self.operation.pool;
        charge(&pool.live, bytes, pool.limit, MEMORY_LIMIT)?;
        if self.state
            && let Err(error) = charge(
                &self.operation.state,
                bytes,
                STATE_BYTES,
                "Git state-memory limit exceeded",
            )
        {
            pool.live.fetch_sub(bytes, Ordering::Relaxed);
            return Err(error);
        }
        self.bytes = next;
        Ok(())
    }

    pub(crate) fn shrink(&mut self, bytes: usize) -> Result<(), Error> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| Error::InvalidPack("Git live-memory size underflowed".into()))?;
        self.operation.pool.live.fetch_sub(bytes, Ordering::Relaxed);
        if self.state {
            self.operation.state.fetch_sub(bytes, Ordering::Relaxed);
        }
        Ok(())
    }
}

struct BytesOwner {
    bytes: Bytes,
    _reservation: Reservation,
}

impl AsRef<[u8]> for BytesOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

pub(crate) fn hold(bytes: Bytes, reservation: Reservation) -> Bytes {
    Bytes::from_owner(BytesOwner {
        bytes,
        _reservation: reservation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_changes_only_the_call_limit_and_preserves_admission() -> Result<(), Error> {
        let pool = Pool::new(LIVE_BYTES);
        let operation = pool.admit_maintenance()?;
        assert!(pool.admit().is_err());
        for _ in 0..MAINTENANCE_CALLS {
            operation.io(0)?;
        }
        assert!(operation.io(0).is_err());
        assert!(operation.work(WORK_BYTES + 1).is_err());
        assert!(operation.reserve_state(STATE_BYTES + 1).is_err());
        assert!(operation.reserve(LIVE_BYTES + 1).is_err());
        operation.retry()?;
        assert!(operation.retry().is_err());
        drop(operation);
        let serving = pool.admit()?;
        for _ in 0..CALLS {
            serving.io(0)?;
        }
        assert!(serving.io(0).is_err());
        Ok(())
    }

    #[test]
    fn state_phase_is_shared_and_failed_growth_rolls_back() -> Result<(), Error> {
        let operation = Pool::new(LIVE_BYTES).admit()?;
        let mut first = operation.reserve_state(STATE_BYTES - 1)?;
        let last = operation.reserve_state(1)?;
        assert!(operation.reserve_state(1).is_err());
        assert!(first.grow(1).is_err());
        assert_eq!(operation.live_bytes(), STATE_BYTES);
        drop(last);
        first.grow(1)?;
        first.shrink(1)?;
        assert_eq!(operation.live_bytes(), STATE_BYTES - 1);
        drop(first);
        assert_eq!(operation.live_bytes(), 0);
        assert!(operation.reserve_state(STATE_BYTES).is_ok());
        Ok(())
    }

    #[test]
    fn admission_reservations_and_slices_release_on_last_drop() -> Result<(), Error> {
        let pool = Pool::new(4);
        let operation = pool.admit()?;
        assert!(pool.admit().is_err());
        let bytes = hold(Bytes::from_static(b"1234"), operation.reserve(4)?);
        assert!(operation.reserve(1).is_err());
        let slice = bytes.slice(1..3);
        drop(bytes);
        assert!(operation.reserve(1).is_err());
        drop(operation);
        assert!(pool.admit().is_err());
        drop(slice);
        let next = pool.admit()?;
        assert!(next.reserve(4).is_ok());
        Ok(())
    }

    #[test]
    fn counters_accept_exact_limits_and_reject_the_next_unit() -> Result<(), Error> {
        let operation = Pool::new(1).admit()?;
        operation.io(TRANSFER_BYTES)?;
        for _ in 1..CALLS {
            operation.io(0)?;
        }
        assert!(operation.io(0).is_err());
        operation.work(WORK_BYTES)?;
        assert!(operation.work(1).is_err());
        for _ in 0..THIN_ROUNDS {
            operation.thin_round()?;
        }
        assert!(operation.thin_round().is_err());
        operation.retry()?;
        assert!(operation.retry().is_err());
        let transfer = Pool::new(0).admit()?;
        assert!(transfer.io(TRANSFER_BYTES + 1).is_err());
        Ok(())
    }

    #[test]
    fn cumulative_bytes_cross_u32_without_resetting_on_retry() -> Result<(), Error> {
        let operation = Pool::new(0).admit()?;
        for _ in 0..2 {
            operation.io(1_u64 << 31)?;
            operation.work(1_u64 << 31)?;
        }
        operation.retry()?;
        assert_eq!(operation.work_bytes(), 1_u64 << 32);
        assert_eq!(
            operation
                .0
                .io
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .transfer,
            1_u64 << 32
        );
        assert_eq!(operation.calls(), 2);
        Ok(())
    }

    #[test]
    fn reservation_growth_and_shrink_have_exact_bounds() -> Result<(), Error> {
        let operation = Pool::new(4).admit()?;
        let mut memory = operation.reserve(1)?;
        memory.grow(3)?;
        assert!(memory.grow(1).is_err());
        memory.shrink(3)?;
        assert!(memory.shrink(2).is_err());
        assert_eq!(operation.live_bytes(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_releases_admission_and_reserved_memory() -> Result<(), Error> {
        let pool = Pool::new(4);
        let operation = pool.admit()?;
        let (ready, observed) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _memory = operation.reserve(4)?;
            ready
                .send(())
                .map_err(|()| Error::InvalidPack("test receiver stopped".into()))?;
            std::future::pending::<()>().await;
            Ok::<(), Error>(())
        });
        observed
            .await
            .map_err(|_| Error::InvalidPack("test operation stopped".into()))?;
        assert!(pool.admit().is_err());
        task.abort();
        let _ = task.await;
        assert!(pool.admit()?.reserve(4).is_ok());
        Ok(())
    }
}
