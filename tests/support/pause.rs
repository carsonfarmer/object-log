use std::future::Future;
use std::time::Duration;

pub(crate) async fn entered(
    wait: impl Future<Output = bool>,
) -> Result<(), tokio::time::error::Elapsed> {
    assert!(tokio::time::timeout(Duration::from_secs(5), wait).await?);
    Ok(())
}
