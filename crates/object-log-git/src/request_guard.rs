//! Guard adapter only; attachment and manual-precharge removal land together.

use object_log::{Request, RequestDenied, RequestGuard};

use super::Operation;

impl std::fmt::Debug for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Operation").finish_non_exhaustive()
    }
}

impl RequestGuard for Operation {
    fn before_request(&self, request: Request) -> Result<(), RequestDenied> {
        let bytes = match request {
            Request::Read { max_bytes } => max_bytes,
            Request::Write { bytes } => bytes,
            // These are logical client invocations; metadata, provider pages,
            // batching and hidden HTTP retries are not payload-bounded here.
            Request::List | Request::Delete { .. } => 0,
            _ => return Err(RequestDenied),
        };
        self.io(bytes).map_err(|_| RequestDenied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::pack::budget::{CALLS, Pool, TRANSFER_BYTES};

    #[test]
    fn refusal_preserves_both_counters_and_retry_keeps_admissions() -> Result<(), Error> {
        let operation = Pool::new(0).admit()?;
        let mut remaining = TRANSFER_BYTES;
        while remaining > 0 {
            let bytes = remaining.min(1024 * 1024 * 1024) as usize;
            assert!(
                operation
                    .before_request(Request::Read { max_bytes: bytes })
                    .is_ok()
            );
            remaining -= bytes as u64;
        }
        let admitted = operation.calls();
        assert!(
            operation
                .before_request(Request::Write { bytes: 1 })
                .is_err()
        );
        assert_eq!(operation.calls(), admitted);
        operation.retry()?;
        for _ in admitted..CALLS {
            assert!(operation.before_request(Request::List).is_ok());
        }
        assert!(
            operation
                .before_request(Request::Delete { objects: 1 })
                .is_err()
        );
        assert_eq!(operation.calls(), CALLS);
        Ok(())
    }

    #[test]
    fn concurrent_transfer_admission_is_atomic_and_retains_the_permit() -> Result<(), Error> {
        let pool = Pool::new(0);
        let operation = pool.admit()?;
        let chunk = usize::try_from(TRANSFER_BYTES / 4).map_err(crate::pack::pack_error)?;
        let successes = std::thread::scope(|scope| {
            let handles = (0..32)
                .map(|_| {
                    let operation = operation.clone();
                    scope.spawn(move || {
                        operation
                            .before_request(Request::Write { bytes: chunk })
                            .is_ok()
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| usize::from(handle.join().unwrap_or(false)))
                .sum::<usize>()
        });
        assert_eq!(successes, 4);
        assert_eq!(operation.calls(), 4);
        assert!(
            operation
                .before_request(Request::Read { max_bytes: 1 })
                .is_err()
        );
        let guard: std::sync::Arc<dyn RequestGuard> = std::sync::Arc::new(operation.clone());
        drop(operation);
        assert!(pool.admit().is_err());
        drop(guard);
        assert!(pool.admit().is_ok());
        Ok(())
    }
}
