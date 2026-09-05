//! Local signed transport fixture, not the application entry point.
#![cfg_attr(
    not(target_arch = "wasm32"),
    allow(dead_code, reason = "The fixture entry requires WASI.")
)]
#[path = "../src/transport.rs"]
mod transport;
use bytes::Bytes;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, RetryConfig, UpdateVersion, aws::AmazonS3Builder,
    path::Path,
};
use spin_sdk::http::{IntoResponse, Request, Response};
use std::sync::Arc;
use transport::{Crypto, Transport};
async fn connection_case(store: &dyn ObjectStore, uri: &str) -> anyhow::Result<Option<Response>> {
    if uri.ends_with("/retry-close") {
        store.get(&Path::from("retry-close")).await?.bytes().await?;
        return Ok(Some(Response::new(200, "read retry passed\n")));
    }
    if uri.ends_with("/repeat-close") {
        anyhow::ensure!(store.get(&Path::from("repeat-close")).await.is_err());
        return Ok(Some(Response::new(200, "bounded retry failure passed\n")));
    }
    if uri.ends_with("/write-close") {
        anyhow::ensure!(
            store
                .put(&Path::from("write-close"), Bytes::new().into())
                .await
                .is_err()
        );
        return Ok(Some(Response::new(200, "write uncertainty preserved\n")));
    }
    Ok(None)
}

// SDK-generated ABI glue requires unsafe exports; application code remains safe.
#[allow(unsafe_code, clippy::same_length_and_capacity)]
mod entry {
    use super::{
        AmazonS3Builder, Arc, Bytes, Crypto, IntoResponse, ObjectStore, ObjectStoreExt, Path,
        PutMode, Request, Response, RetryConfig, Transport, UpdateVersion,
    };
    #[cfg_attr(target_arch = "wasm32", spin_sdk::http_component)]
    async fn handle(request: Request) -> anyhow::Result<impl IntoResponse> {
        let store = AmazonS3Builder::new()
            .with_bucket_name("probe")
            .with_region("us-east-1")
            .with_access_key_id("probe-access")
            .with_secret_access_key("probe-secret")
            .with_endpoint("http://127.0.0.1:19171")
            .with_client_options(
                object_store::client::ClientOptions::new()
                    .with_allow_http(true)
                    .with_timeout_disabled()
                    .with_connect_timeout(std::time::Duration::from_secs(5))
                    .with_read_timeout(std::time::Duration::from_secs(30)),
            )
            .with_http_connector(Transport::default())
            .with_crypto_provider(Arc::new(Crypto))
            .with_retry(RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            })
            .build()?;
        if let Some(response) = super::connection_case(&store, request.uri()).await? {
            return Ok(response);
        }
        if request.uri().ends_with("/redirect") {
            let result = store
                .put(
                    &Path::from("redirect"),
                    Bytes::from(vec![0; 8 * 1024 * 1024]).into(),
                )
                .await;
            anyhow::ensure!(
                result.is_err_and(|error| error.to_string().contains("307")),
                "early redirect must propagate before upload completion"
            );
            return Ok(Response::new(200, "early redirect cancellation passed\n"));
        }
        if request.uri().ends_with("/held") {
            store.get(&Path::from("held")).await?.bytes().await?;
            return Ok(Response::new(200, "held request released\n"));
        }
        if request.uri().ends_with("/failure") {
            let result = store.get(&Path::from("failure")).await;
            anyhow::ensure!(result.is_err(), "503 must propagate as a store error");
            return Ok(Response::new(200, "bounded failure passed\n"));
        }
        let path = Path::from("probe-object");
        let created = store
            .put_opts(
                &path,
                Bytes::from_static(b"transport-probe").into(),
                PutMode::Create.into(),
            )
            .await?;
        let conflict = store
            .put_opts(
                &path,
                Bytes::from_static(b"transport-probe").into(),
                PutMode::Create.into(),
            )
            .await;
        anyhow::ensure!(
            matches!(conflict, Err(object_store::Error::AlreadyExists { .. })),
            "create conflict must propagate"
        );
        store
            .put_opts(
                &path,
                Bytes::from_static(b"transport-probe").into(),
                PutMode::Update(UpdateVersion {
                    e_tag: created.e_tag,
                    version: created.version,
                })
                .into(),
            )
            .await?;
        let bytes = store.get(&path).await?.bytes().await?;
        anyhow::ensure!(bytes == b"transport-probe"[..], "body mismatch");
        let range = store.get_range(&path, 2..7).await?;
        anyhow::ensure!(range == b"anspo"[..], "range mismatch");
        let listed = store.list_with_delimiter(None).await?;
        anyhow::ensure!(
            listed.objects.len() == 1 && listed.objects[0].location == path,
            "list mismatch"
        );
        store.delete(&path).await?;
        Ok(Response::new(
            200,
            "signed conditional put/get/range/list/delete transport passed\n",
        ))
    }
}
