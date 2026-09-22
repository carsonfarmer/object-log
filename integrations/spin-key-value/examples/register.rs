//! Run with `cargo run --example register -- runtime-config.toml`.
//! A custom Spin runtime uses the same resolver before configuring KeyValueFactor.
use anyhow::Context;
use object_log_spin_key_value::ObjectLogKeyValueStore;
use spin_factor_key_value::runtime_config::spin::RuntimeConfigResolver;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file = std::env::args()
        .nth(1)
        .context("expected runtime-config.toml")?;
    let table: toml::Table = toml::from_str(&std::fs::read_to_string(file)?)?;
    let mut resolver = RuntimeConfigResolver::new();
    resolver.register_store_type(ObjectLogKeyValueStore)?;
    let config = resolver.resolve(Some(&table))?;
    // Install `config` as KeyValueFactor's RuntimeConfig in your runtime.
    // A direct smoke check, without a guest:
    let manager = config
        .get_store_manager("default")
        .context("no default store")?;
    let store = manager.get("default").await?;
    println!(
        "Configured default store: {} keys",
        store.get_keys(usize::MAX).await?.len()
    );
    Ok(())
}
