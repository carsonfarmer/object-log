use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use bytes::Bytes;

use crate::Error;

pub(crate) const LIVE_BYTES: usize = 88 * 1024 * 1024;
pub(crate) const CALLS: usize = 512;
pub(crate) const TRANSFER_BYTES: usize = 96 * 1024 * 1024;
pub(crate) const WORK_BYTES: usize = 256 * 1024 * 1024;
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
        if self
            .0
            .active
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(Error::InvalidPack("another Git operation is active".into()));
        }
        Ok(Operation(Arc::new(OperationState {
            pool: self.0.clone(),
            calls: AtomicUsize::new(0),
            transfer: AtomicUsize::new(0),
            work: AtomicUsize::new(0),
            thin_rounds: AtomicUsize::new(0),
            retries: AtomicUsize::new(0),
        })))
    }
}

struct OperationState {
    pool: Arc<PoolState>,
    calls: AtomicUsize,
    transfer: AtomicUsize,
    work: AtomicUsize,
    thin_rounds: AtomicUsize,
    retries: AtomicUsize,
}
impl Drop for OperationState {
    fn drop(&mut self) {
        self.pool.active.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
pub(crate) struct Operation(Arc<OperationState>);

impl Operation {
    pub(crate) fn io(&self, bytes: usize) -> Result<(), Error> {
        charge(&self.0.calls, 1, CALLS, "object-log call limit exceeded")?;
        charge(
            &self.0.transfer,
            bytes,
            TRANSFER_BYTES,
            "object-log transfer limit exceeded",
        )
    }

    pub(crate) fn work(&self, bytes: usize) -> Result<(), Error> {
        charge(&self.0.work, bytes, WORK_BYTES, "Git work limit exceeded")
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
        })
    }

    #[cfg(test)]
    pub(crate) fn calls(&self) -> usize {
        self.0.calls.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn work_bytes(&self) -> usize {
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
}

impl Drop for Reservation {
    fn drop(&mut self) {
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
        self.bytes = next;
        Ok(())
    }

    pub(crate) fn shrink(&mut self, bytes: usize) -> Result<(), Error> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| Error::InvalidPack("Git live-memory size underflowed".into()))?;
        self.operation.pool.live.fetch_sub(bytes, Ordering::Relaxed);
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
