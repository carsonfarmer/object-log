use std::{
    env,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use object_log::TransactionId;
use object_store::{
    ClientOptions, ObjectStore,
    aws::AmazonS3Builder,
    client::{
        HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService,
        ReqwestConnector,
    },
    path::Path,
    prefix::PrefixStore,
};

// Counts below the provider's retry/pagination layer. No request bodies, URLs,
// credentials, or per-request history are retained.
#[derive(Debug, Default)]
pub struct HttpCounts {
    pub attempts: AtomicU64,
    pub upload_body_bytes: AtomicU64,
    pub conflicts: AtomicU64,
    pub errors: AtomicU64,
}

impl HttpCounts {
    pub fn snapshot(&self) -> [u64; 4] {
        [
            self.attempts.load(Ordering::Relaxed),
            self.upload_body_bytes.load(Ordering::Relaxed),
            self.conflicts.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
        ]
    }
}

#[derive(Debug)]
struct CountedConnector(Arc<HttpCounts>);

impl HttpConnector for CountedConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(CountedClient {
            inner: ReqwestConnector::default().connect(options)?,
            counts: Arc::clone(&self.0),
        }))
    }
}

#[derive(Debug)]
struct CountedClient {
    inner: HttpClient,
    counts: Arc<HttpCounts>,
}

#[async_trait]
impl HttpService for CountedClient {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.counts.attempts.fetch_add(1, Ordering::Relaxed);
        self.counts
            .upload_body_bytes
            .fetch_add(request.body().content_length() as u64, Ordering::Relaxed);
        let response = self.inner.execute(request).await;
        match &response {
            Ok(response) if matches!(response.status().as_u16(), 409 | 412) => {
                self.counts.conflicts.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.counts.errors.fetch_add(1, Ordering::Relaxed);
            }
            Ok(response) if response.status().is_server_error() => {
                self.counts.errors.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        response
    }
}

type MeasuredStore = (Arc<dyn ObjectStore>, Arc<HttpCounts>);

pub fn minio() -> Result<MeasuredStore, Box<dyn Error>> {
    let endpoint = env::var("OBJECT_LOG_MINIO_ENDPOINT")?;
    // Qualification must stay local even if a shell has remote AWS settings.
    let authority = endpoint
        .strip_prefix("http://")
        .ok_or("KV qualification requires an HTTP loopback MinIO endpoint")?;
    let port = authority
        .strip_prefix("127.0.0.1:")
        .or_else(|| authority.strip_prefix("localhost:"))
        .ok_or("KV qualification requires a loopback MinIO endpoint")?;
    let _: u16 = port.parse()?;
    let counts = Arc::new(HttpCounts::default());
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_access_key_id(env::var("OBJECT_LOG_MINIO_ACCESS_KEY")?)
        .with_secret_access_key(env::var("OBJECT_LOG_MINIO_SECRET_KEY")?)
        .with_bucket_name(env::var("OBJECT_LOG_MINIO_BUCKET")?)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_disable_bulk_delete(false)
        .with_http_connector(CountedConnector(Arc::clone(&counts)))
        .build()?;
    Ok((
        Arc::new(PrefixStore::new(
            store,
            Path::from(format!("kv-qualification-{}", TransactionId::new())),
        )),
        counts,
    ))
}

pub fn aws() -> Result<MeasuredStore, Box<dyn Error>> {
    let bucket = env::var("OBJECT_LOG_AWS_BUCKET")?;
    let region = env::var("OBJECT_LOG_AWS_REGION")?;
    let prefix = Path::parse(env::var("OBJECT_LOG_AWS_PREFIX")?)?;
    if prefix.as_ref().is_empty() {
        return Err("AWS qualification requires a nonempty object prefix".into());
    }
    let counts = Arc::new(HttpCounts::default());
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_disable_bulk_delete(false)
        .with_http_connector(CountedConnector(Arc::clone(&counts)));
    if let Ok(access_key) = env::var("AWS_ACCESS_KEY_ID") {
        builder = builder.with_access_key_id(access_key);
    }
    if let Ok(secret_key) = env::var("AWS_SECRET_ACCESS_KEY") {
        builder = builder.with_secret_access_key(secret_key);
    }
    if let Ok(token) = env::var("AWS_SESSION_TOKEN") {
        builder = builder.with_token(token);
    }
    let store = builder.build()?;
    Ok((
        Arc::new(PrefixStore::new(
            store,
            prefix.join(format!("kv-qualification-{}", TransactionId::new())),
        )),
        counts,
    ))
}
