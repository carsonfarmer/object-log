//! Test-only Spin embedding: execute upstream's unmodified key-value guest.
//! The production deliverable is a provider library, not a runtime executable.
use std::{sync::Arc, time::Duration};

use clap::Parser;
use http_body_util::BodyExt;
use object_log_spin_key_value::ObjectLogKeyValueStore;
use spin_factor_key_value::{KeyValueFactor, runtime_config::spin::RuntimeConfigResolver};
use spin_factor_outbound_http::OutboundHttpFactor;
use spin_factor_outbound_networking::OutboundNetworkingFactor;
use spin_factor_variables::VariablesFactor;
use spin_factor_wasi::{WasiFactor, spin::SpinFilesMounter};
use spin_factors::RuntimeFactors;
use spin_factors_executor::FactorsExecutor;
use spin_trigger::Trigger;
use spin_trigger_http::HttpTrigger;

#[derive(RuntimeFactors)]
struct TestFactors {
    wasi: WasiFactor,
    variables: VariablesFactor,
    networking: OutboundNetworkingFactor,
    http: OutboundHttpFactor,
    key_value: KeyValueFactor,
}

#[derive(Parser)]
struct TestArgs {
    #[command(flatten)]
    http: spin_trigger_http::CliArgs,
}

#[tokio::test]
#[ignore = "run ./test-guest.sh to build the pinned, unmodified upstream guest first"]
async fn unchanged_spin_guest() -> anyhow::Result<()> {
    spin_tls::install_default_crypto_provider();
    let wasm = std::path::PathBuf::from(std::env::var("SPIN_KV_TEST_GUEST")?).canonicalize()?;
    let temp = tempfile::tempdir()?;
    std::fs::copy(&wasm, temp.path().join("guest.wasm"))?;
    let manifest = temp.path().join("spin.toml");
    std::fs::write(
        &manifest,
        r#"
spin_manifest_version = 2
[application]
name = "unchanged-kv-guest"
[[trigger.http]]
route = "/..."
component = "guest"
[component.guest]
source = "guest.wasm"
key_value_stores = ["default"]
allowed_outbound_hosts = []
"#,
    )?;
    let app = spin_app::App::new(
        "kv-test",
        spin_loader::from_file(
            &manifest,
            spin_loader::FilesMountStrategy::Direct,
            None,
            None,
        )
        .await?,
    );
    let factors = TestFactors {
        wasi: WasiFactor::new(SpinFilesMounter::new(temp.path(), false)),
        variables: VariablesFactor::default(),
        networking: OutboundNetworkingFactor::default(),
        http: OutboundHttpFactor::default(),
        key_value: KeyValueFactor::new(),
    };
    let mut resolver = RuntimeConfigResolver::new();
    resolver.register_store_type(ObjectLogKeyValueStore)?;
    let table = toml::toml! {
        [key_value_store.default]
        type = "object-log"
        memory = true
        prefix = "guest-test"
    };
    let config = TestFactorsRuntimeConfig {
        key_value: Some(resolver.resolve(Some(&table))?),
        ..Default::default()
    };
    let args = TestArgs::parse_from(["test", "--listen", "127.0.0.1:0"]);
    let mut trigger = <HttpTrigger as Trigger<TestFactors>>::new(args.http, &app)?;
    let mut engine_config = spin_core::Config::default();
    <HttpTrigger as Trigger<TestFactors>>::update_core_config(&mut trigger, &mut engine_config)?;
    let mut engine = spin_core::Engine::builder(&engine_config)?;
    <HttpTrigger as Trigger<TestFactors>>::add_to_linker(&mut trigger, engine.linker())?;
    let executor = Arc::new(FactorsExecutor::new(engine, factors)?);
    let loaded = executor
        .load_app(
            app,
            config,
            &spin_trigger::loader::ComponentLoader::new(),
            Some("http"),
            <HttpTrigger as Trigger<TestFactors>>::trigger_dependencies_composer(),
        )
        .await?;
    let server = trigger.into_server(loaded)?;
    let request = http::Request::builder().uri("http://localhost/").body(
        http_body_util::Empty::<bytes::Bytes>::new()
            .map_err(|never| match never {})
            .boxed_unsync(),
    )?;
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        server.handle(request, http::uri::Scheme::HTTP, "127.0.0.1:12345".parse()?),
    )
    .await??;
    let status = response.status();
    let body = response.into_body().collect().await?.to_bytes();
    anyhow::ensure!(
        status.is_success(),
        "upstream guest failed: {status}: {}",
        String::from_utf8_lossy(&body)
    );
    Ok(())
}
