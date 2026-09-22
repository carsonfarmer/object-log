use std::sync::Arc;

use anyhow::Context;
use object_log_spin_key_value::{Config, Maintenance, ObjectLogKeyValueStore};
use spin_factor_key_value::{StoreManager, runtime_config::spin::MakeKeyValueStore};

pub async fn qualify(config: Config) -> anyhow::Result<()> {
    let host = ObjectLogKeyValueStore.make_store(config.clone())?;
    let other_host = ObjectLogKeyValueStore.make_store(config.clone())?;
    let store = host.get("default").await.context("open default")?;
    store
        .set("saved", b"survives host restart")
        .await
        .context("initial set")?;
    let boundary = vec![0xa5; config.limits.value_bytes];
    store
        .set("boundary", &boundary)
        .await
        .context("set maximum-size value")?;
    assert_eq!(
        host.get("audit").await?.get("saved", usize::MAX).await?,
        None
    );
    let other = other_host.get("default").await.context("open default")?;
    let mut tasks = Vec::new();
    for writer in [Arc::clone(&store), other] {
        tasks.push(tokio::spawn(async move {
            for _ in 0..8 {
                writer
                    .increment("counter".into(), 1)
                    .await
                    .context("concurrent increment")?;
            }
            Ok::<_, anyhow::Error>(())
        }));
    }
    for task in tasks {
        task.await??;
    }
    drop(store);
    drop(host);
    drop(other_host);
    let restarted = ObjectLogKeyValueStore.make_store(config)?;
    let store = restarted.get("default").await?;
    assert_eq!(
        store.get("saved", usize::MAX).await?,
        Some(b"survives host restart".to_vec())
    );
    assert_eq!(
        store.get("counter", usize::MAX).await?,
        Some(16i64.to_le_bytes().to_vec())
    );
    assert_eq!(store.get("boundary", usize::MAX).await?, Some(boundary));
    assert!(store.set("oversize", &[0; 65537]).await.is_err());
    let mut complete = false;
    for _ in 0..20 {
        if restarted.maintain("default").await.context("maintenance")? == Maintenance::Complete {
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert_eq!(
        store.get("counter", usize::MAX).await?,
        Some(16i64.to_le_bytes().to_vec())
    );
    Ok(())
}

#[allow(dead_code)] // Shared test helper; the MinIO test crate uses only `qualify`.
pub async fn qualify_large_values(config: Config) -> anyhow::Result<()> {
    let maximum = config.limits.value_bytes;
    let host = ObjectLogKeyValueStore.make_store(config.clone())?;
    let store = host.get("large").await?;
    eprintln!("provider=aws value_sizes=[65536,262144,1048576,4194304,{maximum}]");
    for (sample, size) in [64 << 10, 256 << 10, 1 << 20, 4 << 20, maximum]
        .into_iter()
        .enumerate()
    {
        let value = vec![u8::try_from(sample + 1)?; size];
        store.set("payload", &value).await?;
        let read = store
            .get("payload", usize::MAX)
            .await?
            .context("large value missing after publication")?;
        assert_eq!(read, value);
    }

    let gate = Arc::new(tokio::sync::Barrier::new(4));
    let mut tasks = Vec::new();
    for index in 0..3 {
        let gate = Arc::clone(&gate);
        let store = Arc::clone(&store);
        tasks.push(tokio::spawn(async move {
            gate.wait().await;
            store
                .set(&format!("concurrent/{index}"), &vec![0x5a; maximum])
                .await
        }));
    }
    gate.wait().await;
    let mut committed = 0;
    let mut busy = 0;
    for task in tasks {
        match task.await? {
            Ok(()) => committed += 1,
            Err(error) if error.to_string().contains("concurrent operation limit") => busy += 1,
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::ensure!(
        committed == 2 && busy == 1,
        "large-value admission was not bounded"
    );
    drop(store);
    drop(host);
    let restarted = ObjectLogKeyValueStore.make_store(config)?;
    let store = restarted.get("large").await?;
    assert_eq!(
        store
            .get("payload", usize::MAX)
            .await?
            .context("large value missing after cold restart")?
            .len(),
        maximum,
    );
    store.delete("payload").await?;
    for index in 0..3 {
        store.delete(&format!("concurrent/{index}")).await?;
    }
    for _ in 0..20 {
        if restarted.maintain("large").await? == Maintenance::Complete {
            return Ok(());
        }
    }
    anyhow::bail!("large-value maintenance did not complete")
}
