use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_log::{
    CommitStatus, Log, LogId, Options, Refresh, Resolution, TransactionId, ValidatedBackend,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tokio::sync::Notify;

const FAIL_NONE: u8 = 0;
const FAIL_BEFORE_UPDATE: u8 = 1;
const FAIL_AFTER_UPDATE: u8 = 2;

#[test]
fn log_ids_reject_unsafe_namespace_forms() {
    for invalid in ["", ".", "..", "a/b", "a\\b", "white space", "🦀"] {
        assert!(LogId::new(invalid).is_err(), "accepted {invalid:?}");
    }
    assert!(LogId::new("a".repeat(129)).is_err());
    assert!(LogId::new("tenant.A_1-2").is_ok());
}

#[tokio::test]
async fn concurrent_open_creates_one_head_and_existing_open_does_not_rewrite_it()
-> Result<(), Box<dyn std::error::Error>> {
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let log_id = LogId::new("concurrent-open")?;
    let backend = ValidatedBackend::new(backend, Path::from("protocol-tests")).await?;
    let first_store = backend.scope(&log_id);
    let second_store = backend.scope(&log_id);
    let (first, second) = tokio::join!(
        Log::open(first_store, Options::default()),
        Log::open(second_store, Options::default())
    );
    let first = first?;
    let second = second?;
    let first_view = first.load().await?;
    let second_view = second.load().await?;
    assert_eq!(first_view.cursor().generation(), 0);
    assert!(matches!(
        first.refresh(second_view.cursor()).await?,
        Refresh::NotModified
    ));

    let third_store = backend.scope(&log_id);
    let third = Log::open(third_store, Options::default()).await?;
    assert_eq!(third.load().await?.cursor().generation(), 0);
    assert!(matches!(
        first.refresh(first_view.cursor()).await?,
        Refresh::NotModified
    ));
    Ok(())
}

#[tokio::test]
async fn refresh_distinguishes_current_and_changed_heads() -> Result<(), Box<dyn std::error::Error>>
{
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = open(Arc::clone(&backend), "refresh").await?;
    let second = open(backend, "refresh").await?;
    let stale = first.load().await?;
    assert!(matches!(
        first.refresh(stale.cursor()).await?,
        Refresh::NotModified
    ));

    let prepared = second.prepare(
        stale.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"change"),
        Bytes::new(),
        Vec::new(),
    )?;
    let CommitStatus::Committed(committed) = second.commit(prepared).await? else {
        return Err("the refresh test candidate did not commit".into());
    };
    let Refresh::Updated(updated) = first.refresh(stale.cursor()).await? else {
        return Err("a changed head was reported as not modified".into());
    };
    assert_eq!(updated.cursor().tip(), committed.cursor().tip());
    assert!(matches!(
        first.refresh(updated.cursor()).await?,
        Refresh::NotModified
    ));
    Ok(())
}

#[tokio::test]
async fn capability_probe_rejects_false_not_modified_responses()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(InstrumentedStore::new());
    backend.lie_about_conditional_reads();
    let store: Arc<dyn ObjectStore> = backend;
    assert!(matches!(
        ValidatedBackend::new(store, Path::from("protocol-tests")).await,
        Err(object_log::Error::UnsupportedBackend("conditional read"))
    ));
    Ok(())
}

#[tokio::test]
async fn encoded_commit_limit_fails_before_publication() -> Result<(), Box<dyn std::error::Error>> {
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let log_id = LogId::new("commit-limit")?;
    let backend = ValidatedBackend::new(backend, Path::from("protocol-tests")).await?;
    let scoped = backend.scope(&log_id);
    let options = Options {
        max_commit_bytes: 1,
        ..Options::default()
    };
    let log = Log::open(scoped, options).await?;
    let before = log.load().await?;
    let candidate = log.prepare(
        before.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"larger than one encoded byte"),
        Bytes::new(),
        Vec::new(),
    )?;
    assert!(matches!(
        log.commit(candidate).await,
        Err(object_log::Error::LimitExceeded("encoded commit bytes"))
    ));
    assert!(log.load().await?.tail().is_empty());
    Ok(())
}

#[tokio::test]
async fn two_writers_publish_one_order_and_require_explicit_reprepare()
-> Result<(), Box<dyn std::error::Error>> {
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = open(Arc::clone(&backend), "two-writers").await?;
    let second = open(backend, "two-writers").await?;
    let first_view = first.load().await?;
    let second_view = second.load().await?;

    let first_transaction = TransactionId::new();
    let second_transaction = TransactionId::new();
    let first_candidate = first.prepare(
        first_view.cursor(),
        first_transaction,
        Bytes::from_static(b"first"),
        Bytes::from_static(b"first-result"),
        Vec::new(),
    )?;
    let second_candidate = second.prepare(
        second_view.cursor(),
        second_transaction,
        Bytes::from_static(b"second"),
        Bytes::from_static(b"second-result"),
        Vec::new(),
    )?;

    let (first_status, second_status) = tokio::join!(
        first.commit(first_candidate),
        second.commit(second_candidate)
    );
    let (winner, conflict, losing_transaction, losing_operation) =
        match (first_status?, second_status?) {
            (CommitStatus::Committed(winner), CommitStatus::Conflict(conflict)) => (
                winner,
                conflict,
                second_transaction,
                Bytes::from_static(b"second"),
            ),
            (CommitStatus::Conflict(conflict), CommitStatus::Committed(winner)) => (
                winner,
                conflict,
                first_transaction,
                Bytes::from_static(b"first"),
            ),
            _ => return Err("two writers did not produce one winner and one conflict".into()),
        };

    assert_eq!(winner.cursor().generation(), 1);
    assert_eq!(winner.cursor().next_sequence(), 1);
    assert_eq!(conflict.cursor().generation(), 1);
    let tail = first.read_tail(&winner).await?;
    assert_eq!(tail.len(), 1);
    assert_ne!(tail[0].operation(), &losing_operation);

    let retried = first.prepare(
        conflict.cursor(),
        losing_transaction,
        losing_operation.clone(),
        Bytes::new(),
        Vec::new(),
    )?;
    let CommitStatus::Committed(after_retry) = first.commit(retried).await? else {
        return Err("an explicitly reprepared candidate did not commit".into());
    };
    let tail = first.read_tail(&after_retry).await?;
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[1].operation(), &losing_operation);
    assert_eq!(tail[1].expected_tip(), Some(tail[0].reference().digest()));
    Ok(())
}

#[tokio::test]
async fn duplicate_exact_candidates_both_report_committed() -> Result<(), Box<dyn std::error::Error>>
{
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let log = open(backend, "duplicate-candidate").await?;
    let view = log.load().await?;
    let prepared = log.prepare(
        view.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"same candidate"),
        Bytes::from_static(b"same result"),
        Vec::new(),
    )?;

    let (first, second) = tokio::join!(log.commit(prepared.clone()), log.commit(prepared));
    assert!(matches!(first?, CommitStatus::Committed(_)));
    assert!(matches!(second?, CommitStatus::Committed(_)));
    assert_eq!(log.read_tail(&log.load().await?).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn cursor_is_bound_to_one_durable_log_incarnation() -> Result<(), Box<dyn std::error::Error>>
{
    let first_backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let second_backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = open(first_backend, "same-name").await?;
    let second = open(second_backend, "same-name").await?;
    let foreign = first.load().await?;

    assert!(matches!(
        second.prepare(
            foreign.cursor(),
            TransactionId::new(),
            Bytes::from_static(b"must not cross stores"),
            Bytes::new(),
            Vec::new(),
        ),
        Err(object_log::Error::InvalidFormat(_))
    ));
    Ok(())
}

#[tokio::test]
async fn open_rejects_options_that_differ_from_the_durable_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = open(Arc::clone(&backend), "durable-options").await?;
    let log_id = LogId::new("durable-options")?;
    let backend = ValidatedBackend::new(backend, Path::from("protocol-tests")).await?;
    let scoped = backend.scope(&log_id);
    let changed = Options {
        resolution_window: 0,
        ..Options::default()
    };

    assert!(matches!(
        Log::open(scoped, changed).await,
        Err(object_log::Error::ConfigurationMismatch("options"))
    ));
    assert!(first.load().await?.tail().is_empty());
    Ok(())
}

#[tokio::test]
async fn stale_cursor_is_rejected_without_publishing_its_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let log = open(backend, "stale-cursor").await?;
    let stale = log.load().await?;
    let first = log.prepare(
        stale.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"winner"),
        Bytes::new(),
        Vec::new(),
    )?;
    let stale_candidate = log.prepare(
        stale.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"stale"),
        Bytes::new(),
        Vec::new(),
    )?;

    let CommitStatus::Committed(current) = log.commit(first).await? else {
        return Err("the first candidate did not commit".into());
    };
    let CommitStatus::Conflict(conflict) = log.commit(stale_candidate).await? else {
        return Err("the stale candidate did not return a conflict".into());
    };
    assert_eq!(conflict.cursor().tip(), current.cursor().tip());
    let tail = log.read_tail(&conflict).await?;
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].operation(), &Bytes::from_static(b"winner"));
    Ok(())
}

#[tokio::test]
async fn referenced_objects_are_durable_before_head_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let observed = Arc::new(InstrumentedStore::new());
    let backend: Arc<dyn ObjectStore> = observed.clone();
    let log = open(backend, "object-order").await?;
    observed.arm_order_check();

    let payload = Bytes::from_static(b"immutable payload");
    let object = log.put_object(payload.clone()).await?;
    assert_eq!(log.read_object(&object).await?, payload);
    let before = log.load().await?;
    assert!(before.tail().is_empty());

    let prepared = log.prepare(
        before.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"references payload"),
        Bytes::new(),
        vec![object.clone()],
    )?;
    let CommitStatus::Committed(after) = log.commit(prepared).await? else {
        return Err("the object-referencing candidate did not commit".into());
    };
    assert!(observed.object_existed_before_update());
    let tail = log.read_tail(&after).await?;
    assert_eq!(tail[0].objects(), &[object]);
    Ok(())
}

#[tokio::test]
async fn tail_replay_leaves_referenced_objects_lazy() -> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(InMemory::new());
    let erased: Arc<dyn ObjectStore> = backend.clone();
    let log = open(erased, "missing-object").await?;
    let object = log.put_object(Bytes::from_static(b"payload")).await?;
    let view = log.load().await?;
    let prepared = log.prepare(
        view.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"operation"),
        Bytes::new(),
        vec![object.clone()],
    )?;
    let CommitStatus::Committed(committed) = log.commit(prepared).await? else {
        return Err("object commit did not publish".into());
    };
    backend
        .delete(&Path::from(format!(
            "protocol-tests/v1/logs/missing-object/objects/{}",
            object.digest()
        )))
        .await?;

    let tail = log.read_tail(&committed).await?;
    assert_eq!(tail[0].objects(), std::slice::from_ref(&object));
    assert!(matches!(
        log.read_object(&object).await,
        Err(object_log::Error::InvalidFormat(_))
    ));
    Ok(())
}

#[tokio::test]
async fn object_read_rejects_a_changed_referenced_object() -> Result<(), Box<dyn std::error::Error>>
{
    let backend = Arc::new(InMemory::new());
    let erased: Arc<dyn ObjectStore> = backend.clone();
    let log = open(erased, "changed-object").await?;
    let object = log.put_object(Bytes::from_static(b"payload")).await?;
    let view = log.load().await?;
    let prepared = log.prepare(
        view.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"operation"),
        Bytes::new(),
        vec![object.clone()],
    )?;
    let CommitStatus::Committed(committed) = log.commit(prepared).await? else {
        return Err("object commit did not publish".into());
    };
    backend
        .put(
            &Path::from(format!(
                "protocol-tests/v1/logs/changed-object/objects/{}",
                object.digest()
            )),
            Bytes::from_static(b"changed").into(),
        )
        .await?;

    assert_eq!(
        log.read_tail(&committed).await?[0].objects(),
        std::slice::from_ref(&object)
    );
    assert!(matches!(
        log.read_object(&object).await,
        Err(object_log::Error::CorruptObject)
    ));
    Ok(())
}

#[tokio::test]
async fn lost_success_response_resolves_to_the_original_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let observed = Arc::new(InstrumentedStore::new());
    let backend: Arc<dyn ObjectStore> = observed.clone();
    let log = open(backend, "lost-success").await?;
    let view = log.load().await?;
    let transaction_id = TransactionId::new();
    let prepared = log.prepare(
        view.cursor(),
        transaction_id,
        Bytes::from_static(b"uncertain"),
        Bytes::from_static(b"accepted"),
        Vec::new(),
    )?;

    observed.fail_next_update_after_success();
    let CommitStatus::Pending(pending) = log.commit(prepared).await? else {
        return Err("a lost success response was not classified as pending".into());
    };
    assert_eq!(pending.transaction_id(), transaction_id);
    let Resolution::Committed(resolved) = log.resolve(pending).await? else {
        return Err("the pending commit did not resolve as committed".into());
    };
    let tail = log.read_tail(&resolved).await?;
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].result(), &Bytes::from_static(b"accepted"));
    Ok(())
}

#[tokio::test]
async fn cancelled_head_update_resumes_after_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let observed = Arc::new(InstrumentedStore::new());
    let store: Arc<dyn ObjectStore> = observed.clone();
    let backend = ValidatedBackend::new(store, Path::from("protocol-tests")).await?;
    let log_id = LogId::new("cancelled-update")?;
    let log = Log::open(backend.scope(&log_id), Options::default()).await?;
    let prepared = log.prepare(
        log.load().await?.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"cancelled request"),
        Bytes::from_static(b"recorded result"),
        Vec::new(),
    )?;
    let token = prepared.recovery_token()?;
    observed.pause_next_update_after_success();
    let publish = tokio::spawn({
        let log = log.clone();
        async move { log.commit(prepared).await }
    });
    observed.wait_for_visible_update().await;
    publish.abort();
    let cancelled = publish
        .await
        .err()
        .ok_or("aborted publication returned normally")?;
    assert!(cancelled.is_cancelled());

    drop(log);
    let reopened = Log::open(backend.scope(&log_id), Options::default()).await?;
    let Resolution::Committed(view) = reopened.resume(&token).await? else {
        return Err("cancelled publication did not resolve as committed".into());
    };
    assert_eq!(reopened.read_tail(&view).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn tail_order_survives_out_of_order_read_completion() -> Result<(), Box<dyn std::error::Error>>
{
    let observed = Arc::new(InstrumentedStore::new());
    let store: Arc<dyn ObjectStore> = observed.clone();
    let backend = ValidatedBackend::new(store, Path::from("protocol-tests")).await?;
    let log = Log::open(
        backend.scope(&LogId::new("out-of-order-reads")?),
        Options::default(),
    )
    .await?;
    let mut view = log.load().await?;
    for operation in [b"first".as_slice(), b"second".as_slice()] {
        let prepared = log.prepare(
            view.cursor(),
            TransactionId::new(),
            Bytes::copy_from_slice(operation),
            Bytes::new(),
            Vec::new(),
        )?;
        let CommitStatus::Committed(next) = log.commit(prepared).await? else {
            return Err("test commit did not publish".into());
        };
        view = next;
    }

    observed.complete_second_wal_read_first();
    let tail =
        tokio::time::timeout(std::time::Duration::from_secs(1), log.read_tail(&view)).await??;
    assert_eq!(tail[0].operation(), b"first".as_slice());
    assert_eq!(tail[1].operation(), b"second".as_slice());
    Ok(())
}

#[tokio::test]
async fn pending_candidate_resolves_not_committed_after_another_writer_wins()
-> Result<(), Box<dyn std::error::Error>> {
    let observed = Arc::new(InstrumentedStore::new());
    let backend: Arc<dyn ObjectStore> = observed.clone();
    let first = open(Arc::clone(&backend), "pending-loser").await?;
    let second = open(backend, "pending-loser").await?;
    let source = first.load().await?;
    let pending_candidate = first.prepare(
        source.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"pending loser"),
        Bytes::new(),
        Vec::new(),
    )?;
    let winning_candidate = second.prepare(
        source.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"winner"),
        Bytes::new(),
        Vec::new(),
    )?;

    observed.fail_next_update_before_mutation();
    let CommitStatus::Pending(pending) = first.commit(pending_candidate).await? else {
        return Err("a hidden update failure was not classified as pending".into());
    };
    let CommitStatus::Committed(winner) = second.commit(winning_candidate).await? else {
        return Err("the second writer did not commit".into());
    };
    let Resolution::NotCommitted(current) = first.resolve(pending).await? else {
        return Err("the losing pending candidate did not resolve as not committed".into());
    };
    assert_eq!(current.cursor().tip(), winner.cursor().tip());
    let tail = first.read_tail(&current).await?;
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].operation(), &Bytes::from_static(b"winner"));
    Ok(())
}

#[tokio::test]
async fn rejected_candidate_remains_pending_when_the_winner_read_fails()
-> Result<(), Box<dyn std::error::Error>> {
    let observed = Arc::new(InstrumentedStore::new());
    let backend: Arc<dyn ObjectStore> = observed.clone();
    let first = open(Arc::clone(&backend), "rejected-without-view").await?;
    let second = open(backend, "rejected-without-view").await?;
    let source = first.load().await?;
    let loser = first.prepare(
        source.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"loser"),
        Bytes::new(),
        Vec::new(),
    )?;
    let winner = second.prepare(
        source.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"winner"),
        Bytes::new(),
        Vec::new(),
    )?;
    assert!(matches!(
        second.commit(winner).await?,
        CommitStatus::Committed(_)
    ));

    observed.fail_next_head_get();
    let CommitStatus::Pending(pending) = first.commit(loser).await? else {
        return Err("a rejected candidate without its winning view was not pending".into());
    };
    assert!(matches!(
        first.resolve(pending).await?,
        Resolution::NotCommitted(_)
    ));
    Ok(())
}

async fn open(store: Arc<dyn ObjectStore>, id: &str) -> Result<Log, object_log::Error> {
    let log_id = LogId::new(id)?;
    let backend = ValidatedBackend::new(store, Path::from("protocol-tests")).await?;
    let scoped = backend.scope(&log_id);
    Log::open(scoped, Options::default()).await
}

#[derive(Debug)]
struct InstrumentedStore {
    inner: Arc<InMemory>,
    failure: AtomicU8,
    order_check_armed: AtomicBool,
    object_created: AtomicBool,
    object_before_update: AtomicBool,
    lie_conditional_read: AtomicBool,
    fail_head_get: AtomicBool,
    pause_after_update: AtomicBool,
    visible_update: Notify,
    reorder_wal_reads: AtomicBool,
    wal_read_count: AtomicU8,
    second_wal_read: Notify,
}

impl InstrumentedStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            failure: AtomicU8::new(FAIL_NONE),
            order_check_armed: AtomicBool::new(false),
            object_created: AtomicBool::new(false),
            object_before_update: AtomicBool::new(false),
            lie_conditional_read: AtomicBool::new(false),
            fail_head_get: AtomicBool::new(false),
            pause_after_update: AtomicBool::new(false),
            visible_update: Notify::new(),
            reorder_wal_reads: AtomicBool::new(false),
            wal_read_count: AtomicU8::new(0),
            second_wal_read: Notify::new(),
        }
    }

    fn arm_order_check(&self) {
        self.object_created.store(false, Ordering::SeqCst);
        self.object_before_update.store(false, Ordering::SeqCst);
        self.order_check_armed.store(true, Ordering::SeqCst);
    }

    fn object_existed_before_update(&self) -> bool {
        self.object_before_update.load(Ordering::SeqCst)
    }

    fn fail_next_update_before_mutation(&self) {
        self.failure.store(FAIL_BEFORE_UPDATE, Ordering::SeqCst);
    }

    fn fail_next_update_after_success(&self) {
        self.failure.store(FAIL_AFTER_UPDATE, Ordering::SeqCst);
    }

    fn lost_ack_error() -> object_store::Error {
        object_store::Error::Generic {
            store: "instrumented",
            source: Box::new(std::io::Error::other("injected lost acknowledgement")),
        }
    }

    fn lie_about_conditional_reads(&self) {
        self.lie_conditional_read.store(true, Ordering::SeqCst);
    }

    fn fail_next_head_get(&self) {
        self.fail_head_get.store(true, Ordering::SeqCst);
    }

    fn pause_next_update_after_success(&self) {
        self.pause_after_update.store(true, Ordering::SeqCst);
    }

    async fn wait_for_visible_update(&self) {
        self.visible_update.notified().await;
    }

    fn complete_second_wal_read_first(&self) {
        self.wal_read_count.store(0, Ordering::SeqCst);
        self.reorder_wal_reads.store(true, Ordering::SeqCst);
    }
}

impl fmt::Display for InstrumentedStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("instrumented memory store")
    }
}

#[async_trait]
impl ObjectStore for InstrumentedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let is_update = matches!(&options.mode, object_store::PutMode::Update(_));
        let is_object = location.to_string().contains("/objects/");
        if is_update && self.order_check_armed.load(Ordering::SeqCst) {
            self.object_before_update
                .store(self.object_created.load(Ordering::SeqCst), Ordering::SeqCst);
        }
        if is_update
            && self
                .failure
                .compare_exchange(
                    FAIL_BEFORE_UPDATE,
                    FAIL_NONE,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
        {
            return Err(Self::lost_ack_error());
        }

        let result = self.inner.put_opts(location, payload, options).await;
        if result.is_ok() && is_object && self.order_check_armed.load(Ordering::SeqCst) {
            self.object_created.store(true, Ordering::SeqCst);
        }
        if is_update
            && result.is_ok()
            && self
                .failure
                .compare_exchange(
                    FAIL_AFTER_UPDATE,
                    FAIL_NONE,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
        {
            return Err(Self::lost_ack_error());
        }
        if is_update && result.is_ok() && self.pause_after_update.swap(false, Ordering::SeqCst) {
            self.visible_update.notify_one();
            std::future::pending::<()>().await;
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if location.to_string().contains("/wal/") && self.reorder_wal_reads.load(Ordering::SeqCst) {
            match self.wal_read_count.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    self.second_wal_read.notified().await;
                    return self.inner.get_opts(location, options).await;
                }
                1 => {
                    let result = self.inner.get_opts(location, options).await;
                    self.second_wal_read.notify_one();
                    return result;
                }
                _ => {}
            }
        }
        if location.to_string().ends_with("/index.cbor")
            && self.fail_head_get.swap(false, Ordering::SeqCst)
        {
            return Err(Self::lost_ack_error());
        }
        if options.if_none_match.is_some() && self.lie_conditional_read.load(Ordering::SeqCst) {
            return Err(object_store::Error::NotModified {
                path: location.to_string(),
                source: "injected false not-modified response".into(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
