//! Deterministic object-store fault injection and request accounting.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result, UploadPart,
};
use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

const FAULT_STORE_NAME: &str = "object-log-fault-store";

/// One object-store operation measured by [`FaultStore`].
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Operation {
    /// A single-part object write.
    Put,
    /// Multipart upload creation.
    MultipartCreate,
    /// One multipart upload part.
    MultipartPart,
    /// Multipart upload completion.
    MultipartComplete,
    /// Multipart upload abort.
    MultipartAbort,
    /// One object read.
    Get,
    /// One ranged object read request.
    GetRanges,
    /// One object deletion.
    Delete,
    /// One recursive list request.
    List,
    /// One delimiter-based list request.
    ListWithDelimiter,
    /// One object copy.
    Copy,
    /// One object rename.
    Rename,
}

/// The position of an injected failure relative to a storage mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailurePhase {
    /// Return an error without calling the wrapped operation.
    Before,
    /// Call the wrapped operation, then hide its successful response.
    ///
    /// A mutation is visible when this failure occurs.
    After,
}

/// A deterministic one-shot fault on one operation occurrence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Failure {
    /// Operation that will fail.
    pub operation: Operation,
    /// The one-based occurrence of this operation after the last metric reset.
    pub occurrence: u64,
    /// Whether failure occurs before or after the wrapped operation.
    pub phase: FailurePhase,
}

/// The result recorded for one completed request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestOutcome {
    /// The wrapped request succeeded.
    Succeeded,
    /// The wrapped backend returned an error.
    BackendError,
    /// The wrapper failed before the backend request.
    InjectedBefore,
    /// The wrapper hid a successful backend response.
    InjectedAfter,
}

/// One request in the exact order in which the wrapper accepted it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestEvent {
    /// Monotonic request order within this wrapper.
    pub sequence: u64,
    /// Operation type.
    pub operation: Operation,
    /// One-based count for this operation type.
    pub occurrence: u64,
    /// Object path.
    pub path: String,
    /// Bytes sent to the backend.
    pub uploaded_bytes: u64,
    /// Bytes received from the backend.
    pub downloaded_bytes: u64,
    /// Observed request result.
    pub outcome: RequestOutcome,
}

/// Per-operation counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OperationMetrics {
    /// Total accepted requests.
    pub requests: u64,
    /// Requests for which the caller received success.
    pub succeeded: u64,
    /// Mutations that became visible in the wrapped backend.
    pub visible_mutations: u64,
    /// Errors returned by the wrapped backend.
    pub backend_errors: u64,
    /// Failures injected before the wrapped request.
    pub injected_before: u64,
    /// Failures injected after a successful wrapped request.
    pub injected_after: u64,
    /// Total uploaded bytes.
    pub uploaded_bytes: u64,
    /// Total downloaded bytes.
    pub downloaded_bytes: u64,
}

/// A consistent snapshot of all wrapper metrics and completed request events.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Metrics {
    /// Counters grouped by operation type.
    pub operations: BTreeMap<Operation, OperationMetrics>,
    /// Ordered request events when event recording is enabled.
    pub events: Vec<RequestEvent>,
}

impl Metrics {
    /// Returns counters for one operation type.
    #[must_use]
    pub fn operation(&self, operation: Operation) -> OperationMetrics {
        self.operations.get(&operation).copied().unwrap_or_default()
    }

    /// Returns the total request count.
    #[must_use]
    pub fn total_requests(&self) -> u64 {
        self.operations
            .values()
            .map(|metrics| metrics.requests)
            .sum()
    }

    /// Returns total bytes sent to the backend.
    #[must_use]
    pub fn uploaded_bytes(&self) -> u64 {
        self.operations
            .values()
            .map(|metrics| metrics.uploaded_bytes)
            .sum()
    }

    /// Returns total bytes received from the backend.
    #[must_use]
    pub fn downloaded_bytes(&self) -> u64 {
        self.operations
            .values()
            .map(|metrics| metrics.downloaded_bytes)
            .sum()
    }
}

#[derive(Debug)]
struct State {
    next_sequence: u64,
    metrics: Metrics,
    failures: Vec<Failure>,
    record_events: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            next_sequence: 0,
            metrics: Metrics::default(),
            failures: Vec::new(),
            record_events: true,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RequestTicket {
    sequence: u64,
    operation: Operation,
    occurrence: u64,
    uploaded_bytes: u64,
}

/// An [`ObjectStore`] wrapper with deterministic one-shot failures and counters.
///
/// A failure in [`FailurePhase::After`] is ambiguous by design. The wrapped
/// mutation succeeded and became visible, but the caller receives an error.
#[derive(Clone)]
pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    state: Arc<Mutex<State>>,
}

impl FaultStore {
    /// Wraps one owned object store.
    #[must_use]
    pub fn new(store: impl ObjectStore) -> Self {
        Self::from_arc(Arc::new(store))
    }

    /// Wraps one shared object store.
    #[must_use]
    pub fn from_arc(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner: store,
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// Adds a deterministic one-shot failure.
    ///
    /// A failure occurrence is relative to the last call to [`Self::reset`].
    pub fn schedule(&self, failure: Failure) {
        let mut state = lock(&self.state);
        state.failures.retain(|scheduled| {
            scheduled.operation != failure.operation || scheduled.occurrence != failure.occurrence
        });
        state.failures.push(failure);
    }

    /// Fails the next request for `operation` at `phase`.
    pub fn fail_next(&self, operation: Operation, phase: FailurePhase) {
        let mut state = lock(&self.state);
        let occurrence = state
            .metrics
            .operation(operation)
            .requests
            .saturating_add(1);
        state.failures.retain(|scheduled| {
            scheduled.operation != operation || scheduled.occurrence != occurrence
        });
        state.failures.push(Failure {
            operation,
            occurrence,
            phase,
        });
    }

    /// Removes faults that have not fired. Existing metrics remain unchanged.
    pub fn clear_failures(&self) {
        lock(&self.state).failures.clear();
    }

    /// Returns the faults that have not fired.
    #[must_use]
    pub fn pending_failures(&self) -> Vec<Failure> {
        lock(&self.state).failures.clone()
    }

    /// Returns a consistent copy of all metrics.
    #[must_use]
    pub fn metrics(&self) -> Metrics {
        let mut metrics = lock(&self.state).metrics.clone();
        metrics.events.sort_unstable_by_key(|event| event.sequence);
        metrics
    }

    /// Enables or disables detailed request events.
    ///
    /// Counters remain enabled. Benchmarks should disable events to avoid an
    /// allocation for each measured request.
    pub fn record_events(&self, enabled: bool) {
        lock(&self.state).record_events = enabled;
    }

    /// Clears metrics and pending faults.
    pub fn reset(&self) {
        *lock(&self.state) = State::default();
    }

    /// Returns `true` when this wrapper created `error`.
    #[must_use]
    pub fn is_injected(error: &Error) -> bool {
        matches!(error, Error::Generic { store, .. } if *store == FAULT_STORE_NAME)
    }

    fn start(&self, operation: Operation, uploaded_bytes: u64) -> RequestTicket {
        let mut state = lock(&self.state);
        state.next_sequence = state.next_sequence.saturating_add(1);
        let sequence = state.next_sequence;
        let metrics = state.metrics.operations.entry(operation).or_default();
        metrics.requests = metrics.requests.saturating_add(1);
        RequestTicket {
            sequence,
            operation,
            occurrence: metrics.requests,
            uploaded_bytes,
        }
    }

    fn take_failure(&self, ticket: RequestTicket, phase: FailurePhase) -> bool {
        let mut state = lock(&self.state);
        let Some(index) = state.failures.iter().position(|failure| {
            failure.operation == ticket.operation
                && failure.occurrence == ticket.occurrence
                && failure.phase == phase
        }) else {
            return false;
        };
        state.failures.remove(index);
        true
    }

    fn finish(
        &self,
        ticket: RequestTicket,
        path: &Path,
        downloaded_bytes: u64,
        outcome: RequestOutcome,
    ) {
        let mut state = lock(&self.state);
        let metrics = state
            .metrics
            .operations
            .entry(ticket.operation)
            .or_default();
        let uploaded_bytes = if outcome == RequestOutcome::InjectedBefore {
            0
        } else {
            ticket.uploaded_bytes
        };
        metrics.uploaded_bytes = metrics.uploaded_bytes.saturating_add(uploaded_bytes);
        metrics.downloaded_bytes = metrics.downloaded_bytes.saturating_add(downloaded_bytes);
        if is_mutation(ticket.operation)
            && matches!(
                outcome,
                RequestOutcome::Succeeded | RequestOutcome::InjectedAfter
            )
        {
            metrics.visible_mutations = metrics.visible_mutations.saturating_add(1);
        }
        match outcome {
            RequestOutcome::Succeeded => metrics.succeeded = metrics.succeeded.saturating_add(1),
            RequestOutcome::BackendError => {
                metrics.backend_errors = metrics.backend_errors.saturating_add(1);
            }
            RequestOutcome::InjectedBefore => {
                metrics.injected_before = metrics.injected_before.saturating_add(1);
            }
            RequestOutcome::InjectedAfter => {
                metrics.injected_after = metrics.injected_after.saturating_add(1);
            }
        }
        if state.record_events {
            state.metrics.events.push(RequestEvent {
                sequence: ticket.sequence,
                operation: ticket.operation,
                occurrence: ticket.occurrence,
                path: path.to_string(),
                uploaded_bytes,
                downloaded_bytes,
                outcome,
            });
        }
    }

    fn injected_error(ticket: RequestTicket, phase: FailurePhase, path: &Path) -> Error {
        Error::Generic {
            store: FAULT_STORE_NAME,
            source: Box::new(InjectedFailure {
                operation: ticket.operation,
                occurrence: ticket.occurrence,
                phase,
                path: path.to_string(),
            }),
        }
    }

    async fn delete_one(&self, location: Path) -> Result<Path> {
        let ticket = self.start(Operation::Delete, 0);
        if self.take_failure(ticket, FailurePhase::Before) {
            self.finish(ticket, &location, 0, RequestOutcome::InjectedBefore);
            return Err(Self::injected_error(
                ticket,
                FailurePhase::Before,
                &location,
            ));
        }

        let fail_after = self.take_failure(ticket, FailurePhase::After);
        match self.inner.delete(&location).await {
            Ok(()) if fail_after => {
                self.finish(ticket, &location, 0, RequestOutcome::InjectedAfter);
                Err(Self::injected_error(ticket, FailurePhase::After, &location))
            }
            Ok(()) => {
                self.finish(ticket, &location, 0, RequestOutcome::Succeeded);
                Ok(location)
            }
            Err(error) => {
                self.finish(ticket, &location, 0, RequestOutcome::BackendError);
                Err(error)
            }
        }
    }
}

impl fmt::Debug for FaultStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FaultStore")
            .field("inner", &self.inner)
            .field("metrics", &self.metrics())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for FaultStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "fault-injecting({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        let ticket = self.start(Operation::Put, usize_to_u64(payload.content_length()));
        if self.take_failure(ticket, FailurePhase::Before) {
            self.finish(ticket, location, 0, RequestOutcome::InjectedBefore);
            return Err(Self::injected_error(ticket, FailurePhase::Before, location));
        }

        let fail_after = self.take_failure(ticket, FailurePhase::After);
        match self.inner.put_opts(location, payload, options).await {
            Ok(_) if fail_after => {
                self.finish(ticket, location, 0, RequestOutcome::InjectedAfter);
                Err(Self::injected_error(ticket, FailurePhase::After, location))
            }
            Ok(result) => {
                self.finish(ticket, location, 0, RequestOutcome::Succeeded);
                Ok(result)
            }
            Err(error) => {
                self.finish(ticket, location, 0, RequestOutcome::BackendError);
                Err(error)
            }
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let ticket = self.start(Operation::MultipartCreate, 0);
        if self.take_failure(ticket, FailurePhase::Before) {
            self.finish(ticket, location, 0, RequestOutcome::InjectedBefore);
            return Err(Self::injected_error(ticket, FailurePhase::Before, location));
        }
        let fail_after = self.take_failure(ticket, FailurePhase::After);
        match self.inner.put_multipart_opts(location, options).await {
            Ok(_) if fail_after => {
                self.finish(ticket, location, 0, RequestOutcome::InjectedAfter);
                Err(Self::injected_error(ticket, FailurePhase::After, location))
            }
            Ok(upload) => {
                self.finish(ticket, location, 0, RequestOutcome::Succeeded);
                Ok(Box::new(FaultMultipartUpload {
                    inner: upload,
                    store: self.clone(),
                    location: location.clone(),
                }))
            }
            Err(error) => {
                self.finish(ticket, location, 0, RequestOutcome::BackendError);
                Err(error)
            }
        }
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let ticket = self.start(Operation::Get, 0);
        let returns_body = !options.head;
        if self.take_failure(ticket, FailurePhase::Before) {
            self.finish(ticket, location, 0, RequestOutcome::InjectedBefore);
            return Err(Self::injected_error(ticket, FailurePhase::Before, location));
        }
        let fail_after = self.take_failure(ticket, FailurePhase::After);
        match self.inner.get_opts(location, options).await {
            Ok(result) if fail_after => {
                let bytes = if returns_body {
                    result.range.end.saturating_sub(result.range.start)
                } else {
                    0
                };
                self.finish(ticket, location, bytes, RequestOutcome::InjectedAfter);
                Err(Self::injected_error(ticket, FailurePhase::After, location))
            }
            Ok(result) => {
                let bytes = if returns_body {
                    result.range.end.saturating_sub(result.range.start)
                } else {
                    0
                };
                self.finish(ticket, location, bytes, RequestOutcome::Succeeded);
                Ok(result)
            }
            Err(error) => {
                self.finish(ticket, location, 0, RequestOutcome::BackendError);
                Err(error)
            }
        }
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let ticket = self.start(Operation::GetRanges, 0);
        if self.take_failure(ticket, FailurePhase::Before) {
            self.finish(ticket, location, 0, RequestOutcome::InjectedBefore);
            return Err(Self::injected_error(ticket, FailurePhase::Before, location));
        }
        let fail_after = self.take_failure(ticket, FailurePhase::After);
        match self.inner.get_ranges(location, ranges).await {
            Ok(bytes) if fail_after => {
                let downloaded = bytes.iter().fold(0_u64, |total, bytes| {
                    total.saturating_add(usize_to_u64(bytes.len()))
                });
                self.finish(ticket, location, downloaded, RequestOutcome::InjectedAfter);
                Err(Self::injected_error(ticket, FailurePhase::After, location))
            }
            Ok(bytes) => {
                let downloaded = bytes.iter().fold(0_u64, |total, bytes| {
                    total.saturating_add(usize_to_u64(bytes.len()))
                });
                self.finish(ticket, location, downloaded, RequestOutcome::Succeeded);
                Ok(bytes)
            }
            Err(error) => {
                self.finish(ticket, location, 0, RequestOutcome::BackendError);
                Err(error)
            }
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.clone();
        locations
            .then(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    store.delete_one(location).await
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let ticket = self.start(Operation::List, 0);
        let event_path = prefix.cloned().unwrap_or_default();
        if self.take_failure(ticket, FailurePhase::Before) {
            self.finish(ticket, &event_path, 0, RequestOutcome::InjectedBefore);
            let error = Self::injected_error(ticket, FailurePhase::Before, &event_path);
            return futures::stream::once(async move { Err(error) }).boxed();
        }
        let fail_after = self.take_failure(ticket, FailurePhase::After);
        let store = self.clone();
        let inner = self.inner.list(prefix);
        futures::stream::unfold(
            (inner, store, ticket, event_path, false),
            move |(mut inner, store, ticket, event_path, recorded)| async move {
                if let Some(result) = inner.next().await {
                    let recorded = if !recorded && result.is_err() {
                        store.finish(ticket, &event_path, 0, RequestOutcome::BackendError);
                        true
                    } else {
                        recorded
                    };
                    Some((result, (inner, store, ticket, event_path, recorded)))
                } else if recorded {
                    None
                } else if fail_after {
                    store.finish(ticket, &event_path, 0, RequestOutcome::InjectedAfter);
                    let error = Self::injected_error(ticket, FailurePhase::After, &event_path);
                    Some((Err(error), (inner, store, ticket, event_path, true)))
                } else {
                    store.finish(ticket, &event_path, 0, RequestOutcome::Succeeded);
                    None
                }
            },
        )
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let ticket = self.start(Operation::ListWithDelimiter, 0);
        let event_path = prefix.cloned().unwrap_or_default();
        if self.take_failure(ticket, FailurePhase::Before) {
            self.finish(ticket, &event_path, 0, RequestOutcome::InjectedBefore);
            return Err(Self::injected_error(
                ticket,
                FailurePhase::Before,
                &event_path,
            ));
        }
        let fail_after = self.take_failure(ticket, FailurePhase::After);
        match self.inner.list_with_delimiter(prefix).await {
            Ok(_) if fail_after => {
                self.finish(ticket, &event_path, 0, RequestOutcome::InjectedAfter);
                Err(Self::injected_error(
                    ticket,
                    FailurePhase::After,
                    &event_path,
                ))
            }
            Ok(result) => {
                self.finish(ticket, &event_path, 0, RequestOutcome::Succeeded);
                Ok(result)
            }
            Err(error) => {
                self.finish(ticket, &event_path, 0, RequestOutcome::BackendError);
                Err(error)
            }
        }
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let ticket = self.start(Operation::Copy, 0);
        if self.take_failure(ticket, FailurePhase::Before) {
            self.finish(ticket, to, 0, RequestOutcome::InjectedBefore);
            return Err(Self::injected_error(ticket, FailurePhase::Before, to));
        }
        let fail_after = self.take_failure(ticket, FailurePhase::After);
        match self.inner.copy_opts(from, to, options).await {
            Ok(()) if fail_after => {
                self.finish(ticket, to, 0, RequestOutcome::InjectedAfter);
                Err(Self::injected_error(ticket, FailurePhase::After, to))
            }
            Ok(()) => {
                self.finish(ticket, to, 0, RequestOutcome::Succeeded);
                Ok(())
            }
            Err(error) => {
                self.finish(ticket, to, 0, RequestOutcome::BackendError);
                Err(error)
            }
        }
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let ticket = self.start(Operation::Rename, 0);
        if self.take_failure(ticket, FailurePhase::Before) {
            self.finish(ticket, to, 0, RequestOutcome::InjectedBefore);
            return Err(Self::injected_error(ticket, FailurePhase::Before, to));
        }
        let fail_after = self.take_failure(ticket, FailurePhase::After);
        match self.inner.rename_opts(from, to, options).await {
            Ok(()) if fail_after => {
                self.finish(ticket, to, 0, RequestOutcome::InjectedAfter);
                Err(Self::injected_error(ticket, FailurePhase::After, to))
            }
            Ok(()) => {
                self.finish(ticket, to, 0, RequestOutcome::Succeeded);
                Ok(())
            }
            Err(error) => {
                self.finish(ticket, to, 0, RequestOutcome::BackendError);
                Err(error)
            }
        }
    }
}

#[derive(Debug)]
struct FaultMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    store: FaultStore,
    location: Path,
}

#[async_trait]
impl MultipartUpload for FaultMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let ticket = self.store.start(
            Operation::MultipartPart,
            usize_to_u64(data.content_length()),
        );
        let store = self.store.clone();
        let location = self.location.clone();
        if store.take_failure(ticket, FailurePhase::Before) {
            store.finish(ticket, &location, 0, RequestOutcome::InjectedBefore);
            let error = FaultStore::injected_error(ticket, FailurePhase::Before, &location);
            return Box::pin(async move { Err(error) });
        }
        let fail_after = store.take_failure(ticket, FailurePhase::After);
        let future = self.inner.put_part(data);
        Box::pin(async move {
            match future.await {
                Ok(()) if fail_after => {
                    store.finish(ticket, &location, 0, RequestOutcome::InjectedAfter);
                    Err(FaultStore::injected_error(
                        ticket,
                        FailurePhase::After,
                        &location,
                    ))
                }
                Ok(()) => {
                    store.finish(ticket, &location, 0, RequestOutcome::Succeeded);
                    Ok(())
                }
                Err(error) => {
                    store.finish(ticket, &location, 0, RequestOutcome::BackendError);
                    Err(error)
                }
            }
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let ticket = self.store.start(Operation::MultipartComplete, 0);
        if self.store.take_failure(ticket, FailurePhase::Before) {
            self.store
                .finish(ticket, &self.location, 0, RequestOutcome::InjectedBefore);
            return Err(FaultStore::injected_error(
                ticket,
                FailurePhase::Before,
                &self.location,
            ));
        }
        let fail_after = self.store.take_failure(ticket, FailurePhase::After);
        match self.inner.complete().await {
            Ok(_) if fail_after => {
                self.store
                    .finish(ticket, &self.location, 0, RequestOutcome::InjectedAfter);
                Err(FaultStore::injected_error(
                    ticket,
                    FailurePhase::After,
                    &self.location,
                ))
            }
            Ok(result) => {
                self.store
                    .finish(ticket, &self.location, 0, RequestOutcome::Succeeded);
                Ok(result)
            }
            Err(error) => {
                self.store
                    .finish(ticket, &self.location, 0, RequestOutcome::BackendError);
                Err(error)
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        let ticket = self.store.start(Operation::MultipartAbort, 0);
        if self.store.take_failure(ticket, FailurePhase::Before) {
            self.store
                .finish(ticket, &self.location, 0, RequestOutcome::InjectedBefore);
            return Err(FaultStore::injected_error(
                ticket,
                FailurePhase::Before,
                &self.location,
            ));
        }
        let fail_after = self.store.take_failure(ticket, FailurePhase::After);
        match self.inner.abort().await {
            Ok(()) if fail_after => {
                self.store
                    .finish(ticket, &self.location, 0, RequestOutcome::InjectedAfter);
                Err(FaultStore::injected_error(
                    ticket,
                    FailurePhase::After,
                    &self.location,
                ))
            }
            Ok(()) => {
                self.store
                    .finish(ticket, &self.location, 0, RequestOutcome::Succeeded);
                Ok(())
            }
            Err(error) => {
                self.store
                    .finish(ticket, &self.location, 0, RequestOutcome::BackendError);
                Err(error)
            }
        }
    }
}

#[derive(Debug)]
struct InjectedFailure {
    operation: Operation,
    occurrence: u64,
    phase: FailurePhase,
    path: String,
}

impl fmt::Display for InjectedFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "injected {:?} failure for {:?} occurrence {} at {}",
            self.phase, self.operation, self.occurrence, self.path
        )
    }
}

impl StdError for InjectedFailure {}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

const fn is_mutation(operation: Operation) -> bool {
    matches!(
        operation,
        Operation::Put
            | Operation::MultipartComplete
            | Operation::Delete
            | Operation::Copy
            | Operation::Rename
    )
}
