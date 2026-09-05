use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use object_log::sim::FaultStore;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Debug, Clone)]
pub struct TimedStore {
    pub faults: FaultStore,
    epoch: Instant,
    intervals: Arc<Mutex<Vec<(u128, u128)>>>,
}
impl TimedStore {
    pub fn new() -> Self {
        Self {
            faults: FaultStore::new(object_store::memory::InMemory::new()),
            epoch: Instant::now(),
            intervals: Arc::default(),
        }
    }
    pub fn reset(&self) {
        self.faults.reset();
        self.intervals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
    pub fn intervals(&self) -> Vec<(u128, u128)> {
        self.intervals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    fn finish(&self, start: u128) {
        self.intervals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((start, self.epoch.elapsed().as_nanos()));
    }
}
impl fmt::Display for TimedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "timed-memory")
    }
}
#[async_trait]
impl ObjectStore for TimedStore {
    async fn put_opts(
        &self,
        p: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        let start = self.epoch.elapsed().as_nanos();
        let result = self.faults.put_opts(p, bytes, options).await;
        self.finish(start);
        result
    }
    async fn get_opts(&self, p: &Path, options: GetOptions) -> Result<GetResult> {
        let start = self.epoch.elapsed().as_nanos();
        let result = self.faults.get_opts(p, options).await;
        let result = match result {
            Ok(mut result) => {
                let payload = std::mem::replace(
                    &mut result.payload,
                    GetResultPayload::Stream(futures::stream::empty().boxed()),
                );
                let owned = GetResult {
                    payload,
                    meta: result.meta.clone(),
                    range: result.range.clone(),
                    attributes: result.attributes.clone(),
                    extensions: result.extensions.clone(),
                }
                .bytes()
                .await?;
                result.payload = GetResultPayload::Stream(
                    futures::stream::once(async move { Ok(owned) }).boxed(),
                );
                Ok(result)
            }
            Err(error) => Err(error),
        };
        self.finish(start);
        result
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.faults.put_multipart_opts(p, options).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.faults.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.faults.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.faults.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        self.faults.copy_opts(from, to, opts).await
    }
}

// Longest chain of non-overlapping requests, not backend latency or a causal DAG.
pub fn serial_depth(intervals: &[(u128, u128)]) -> usize {
    let mut sorted = intervals.to_vec();
    sorted.sort_unstable_by_key(|&(_, end)| end);
    let mut end = 0;
    let mut depth = 0;
    for (start, finish) in sorted {
        if start >= end {
            depth += 1;
            end = finish;
        }
    }
    depth
}

#[test]
fn serial_depth_counts_chains_and_overlapping_requests() {
    assert_eq!(serial_depth(&[]), 0);
    assert_eq!(serial_depth(&[(0, 10), (2, 3), (3, 4), (10, 11)]), 3);
    assert_eq!(serial_depth(&[(0, 2), (2, 3), (3, 4)]), 3);
}
