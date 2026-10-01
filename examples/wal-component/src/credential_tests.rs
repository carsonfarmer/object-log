use super::*;
use async_trait::async_trait;
use http_body_util::{BodyExt, Full};
use object_store::aws::AmazonS3ConfigKey;
use object_store::client::{
    ClientOptions, HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse,
    HttpResponseBody, HttpService,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn settings() -> Config {
    Config {
        endpoint: "https://s3.us-west-2.amazonaws.com".into(),
        bucket: "qualification".into(),
        region: "us-west-2".into(),
        credential_mode: CredentialMode::StaticCredentials,
        access_key: "temporary-access-key".into(),
        secret_key: "temporary-secret-key".into(),
        session_token: Some("temporary-session-token".into()),
        prefix: "isolated-prefix".into(),
        log_id: "repo".into(),
        log_options: b"{}".to_vec(),
        transport_limits: TransportLimits {
            max_calls: 10,
            max_bytes: 1024,
        },
    }
}

#[test]
fn every_durable_limit_maps_to_the_matching_core_option() {
    let mapped = log_options(
        br#"{
        "max_tail_entries":1,
        "resolution_window":2,
        "max_inline_operation_bytes":3,
        "max_inline_result_bytes":4,
        "max_object_refs":5,
        "max_object_bytes":6,
        "max_commit_bytes":7,
        "max_head_bytes":8,
        "max_checkpoint_bytes":9,
        "max_retention_ids":10,
        "max_collection_objects":11,
        "max_collection_plan_bytes":12
    }"#,
    )
    .unwrap();
    assert_eq!(
        [
            mapped.max_tail_entries,
            mapped.resolution_window,
            mapped.max_inline_operation_bytes,
            mapped.max_inline_result_bytes,
            mapped.max_object_refs,
            mapped.max_object_bytes,
            mapped.max_commit_bytes,
            mapped.max_head_bytes,
            mapped.max_checkpoint_bytes,
            mapped.max_retention_ids,
            mapped.max_collection_objects,
            mapped.max_collection_plan_bytes,
        ],
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    );
}

#[test]
fn byte_options_preserve_defaults_and_exact_git_geometry() {
    assert_eq!(log_options(b"{}").unwrap(), object_log::Options::default());
    assert_eq!(
        log_options(br#"{"max_object_bytes":2097152,"max_collection_objects":100000}"#).unwrap(),
        object_log::Options {
            max_object_bytes: 2 << 20,
            ..object_log::Options::default()
        }
    );
}

#[test]
fn byte_options_reject_unknown_duplicate_invalid_and_oversized_values() {
    for value in [
        b"".as_slice(),
        b"[]",
        b"null",
        b"{}{}",
        br#"{"unknown":1}"#,
        br#"{"maxObjectBytes":1}"#,
        br#"{"max_object_bytes":1,"max_object_bytes":2}"#,
        br#"{"max_object_bytes":-1}"#,
        br#"{"max_object_bytes":1.5}"#,
        br#"{"max_object_bytes":"1"}"#,
        br#"{"max_object_bytes":18446744073709551616}"#,
    ] {
        assert!(log_options(value).is_err(), "accepted {value:?}");
    }
    let mut padded = b"{}".to_vec();
    padded.resize(2048, b' ');
    assert!(log_options(&padded).is_ok());
    padded.push(b' ');
    assert!(matches!(log_options(&padded), Err(Failure::Limit(_))));
    #[cfg(target_pointer_width = "32")]
    assert!(log_options(br#"{"max_object_bytes":4294967296}"#).is_err());
}

#[test]
fn invalid_options_are_rejected_before_backend_validation_or_open() {
    for options in [b"[]".to_vec(), vec![b' '; 2049]] {
        let mut config = settings();
        config.log_options = options;
        assert!(Component::validate_backend(config.clone()).is_err());
        assert!(Component::open(config.clone()).is_err());
        assert!(Component::open_existing(config).is_err());
    }
}

fn instance_settings() -> Config {
    Config {
        credential_mode: CredentialMode::InstanceRole,
        access_key: String::new(),
        secret_key: String::new(),
        session_token: None,
        ..settings()
    }
}

#[test]
fn s3_defaults_keep_path_style_bulk_delete_and_imdsv2() {
    for config in [settings(), instance_settings()] {
        let builder = s3_builder(&config, Metadata::default()).unwrap();
        for key in [
            AmazonS3ConfigKey::VirtualHostedStyleRequest,
            AmazonS3ConfigKey::DisableBulkDelete,
            AmazonS3ConfigKey::ImdsV1Fallback,
        ] {
            assert_eq!(builder.get_config_value(&key).as_deref(), Some("false"));
        }
    }
}

#[tokio::test]
async fn static_credentials_preserve_optional_session_token_without_metadata() {
    for session_token in [None, Some("temporary-session-token".into())] {
        let fixture = Metadata::default();
        let mut config = settings();
        config.session_token = session_token.clone();
        let store = s3_builder(&config, fixture.clone())
            .unwrap()
            .build()
            .unwrap();
        let credential = store.credentials().get_credential().await.unwrap();
        assert_eq!(credential.key_id, config.access_key);
        assert_eq!(credential.secret_key, config.secret_key);
        assert_eq!(credential.token, session_token);
        assert_eq!(fixture.0.calls.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn ambiguous_or_incomplete_credentials_fail_before_network_access() {
    for mode in [
        CredentialMode::StaticCredentials,
        CredentialMode::InstanceRole,
    ] {
        for field in 0..4 {
            let fixture = Metadata::default();
            let mut config = match mode {
                CredentialMode::StaticCredentials => settings(),
                CredentialMode::InstanceRole => instance_settings(),
            };
            match field {
                0 => config.region.clear(),
                1 => {
                    config.access_key = if matches!(mode, CredentialMode::InstanceRole) {
                        "unexpected".into()
                    } else {
                        String::new()
                    }
                }
                2 => {
                    config.secret_key = if matches!(mode, CredentialMode::InstanceRole) {
                        "unexpected".into()
                    } else {
                        String::new()
                    }
                }
                _ => config.session_token = Some(String::new()),
            }
            assert!(s3_builder(&config, fixture.clone()).is_err());
            assert_eq!(fixture.0.calls.load(Ordering::Relaxed), 0);
        }
    }
}

#[derive(Debug, Default)]
struct MetadataState {
    calls: AtomicUsize,
    issued: AtomicUsize,
    unavailable: AtomicBool,
}

#[derive(Clone, Debug, Default)]
struct Metadata(Arc<MetadataState>);

impl HttpConnector for Metadata {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.clone()))
    }
}

#[async_trait]
impl HttpService for Metadata {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.0.calls.fetch_add(1, Ordering::Relaxed);
        assert!(request.headers().get("authorization").is_none());
        let mut status = 200;
        let body = if self.0.unavailable.load(Ordering::Relaxed) {
            // A 403 token failure must not fall back to IMDSv1.
            assert_eq!(request.method(), http::Method::PUT);
            status = 403;
            "unavailable".to_string()
        } else if request.method() == http::Method::PUT {
            assert_eq!(request.uri().path(), "/latest/api/token");
            assert_eq!(
                request.headers()["x-aws-ec2-metadata-token-ttl-seconds"],
                "600"
            );
            "imds-token".into()
        } else {
            assert_eq!(request.method(), http::Method::GET);
            assert_eq!(request.headers()["x-aws-ec2-metadata-token"], "imds-token");
            match request.uri().path() {
                "/latest/meta-data/iam/security-credentials/" => "test-role".into(),
                "/latest/meta-data/iam/security-credentials/test-role" => {
                    let issued = self.0.issued.fetch_add(1, Ordering::Relaxed) + 1;
                    // First credentials are expired. A second lookup must renew;
                    // subsequent lookups retain the unexpired replacement.
                    let expiration = if issued == 1 {
                        "2000-01-01T00:00:00Z"
                    } else {
                        "2100-01-01T00:00:00Z"
                    };
                    format!(
                        r#"{{"AccessKeyId":"key-{issued}","SecretAccessKey":"secret-{issued}","Token":"session-{issued}","Expiration":"{expiration}"}}"#
                    )
                }
                path => panic!("unexpected metadata path {path}"),
            }
        };
        Ok(http::Response::builder()
            .status(status)
            .body(HttpResponseBody::new(
                Full::new(Bytes::from(body)).map_err(|never| match never {}),
            ))
            .unwrap())
    }
}

#[tokio::test]
async fn instance_provider_obtains_renews_and_caches_credentials_through_connector() {
    let fixture = Metadata::default();
    let builder = s3_builder(&instance_settings(), fixture.clone()).unwrap();
    assert_eq!(
        builder.get_config_value(&AmazonS3ConfigKey::AccessKeyId),
        None
    );
    assert_eq!(
        builder.get_config_value(&AmazonS3ConfigKey::ImdsV1Fallback),
        Some("false".into())
    );
    assert_eq!(
        builder.get_config_value(&AmazonS3ConfigKey::MetadataEndpoint),
        Some(INSTANCE_METADATA_ENDPOINT.into())
    );
    let store = builder.build().unwrap();
    let first = store.credentials().get_credential().await.unwrap();
    assert_eq!(
        (&*first.key_id, first.token.as_deref()),
        ("key-1", Some("session-1"))
    );
    let renewed = store.credentials().get_credential().await.unwrap();
    assert_eq!(
        (&*renewed.key_id, renewed.token.as_deref()),
        ("key-2", Some("session-2"))
    );
    let cached = store.credentials().get_credential().await.unwrap();
    assert!(Arc::ptr_eq(&renewed, &cached));
    assert_eq!(fixture.0.calls.load(Ordering::Relaxed), 6);
}

#[tokio::test]
async fn unavailable_metadata_rejects_initial_credentials_and_expiry_renewal() {
    for obtain_first in [false, true] {
        let fixture = Metadata::default();
        let store = s3_builder(&instance_settings(), fixture.clone())
            .unwrap()
            .build()
            .unwrap();
        if obtain_first {
            store.credentials().get_credential().await.unwrap();
        }
        fixture.0.unavailable.store(true, Ordering::Relaxed);
        let before = fixture.0.calls.load(Ordering::Relaxed);
        assert!(store.credentials().get_credential().await.is_err());
        assert_eq!(fixture.0.calls.load(Ordering::Relaxed), before + 1);
    }
}

#[test]
fn missing_head_has_a_distinct_failure() {
    assert!(matches!(
        Failure::from(object_log::Error::LogNotFound),
        Failure::Missing
    ));
    assert!(matches!(
        Failure::from(object_log::Error::InvalidFormat("bad head".into())),
        Failure::Other(_)
    ));
    assert!(matches!(
        Failure::from(object_log::Error::ViewExpired),
        Failure::Expired
    ));
    assert!(matches!(
        Failure::from(object_log::Error::LimitExceeded("read bytes")),
        Failure::Limit(message) if message == "read bytes"
    ));
}
