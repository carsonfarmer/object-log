//! Opt-in unchanged Git protocol clients against the shared local provider fixture.
#![cfg(all(unix, not(target_arch = "wasm32")))]

fn run(script: &str) -> Result<(), Box<dyn std::error::Error>> {
    let status = std::process::Command::new("python3")
        .arg(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests")
                .join(script),
        )
        .status()?;
    assert!(status.success(), "protocol client fixture failed");
    Ok(())
}

#[test]
#[ignore = "requires local MinIO, Spin 4, AWS CLI, Python and release WASIp2 component"]
fn shallow_minio_clients() -> Result<(), Box<dyn std::error::Error>> {
    run("check_shallow.py")
}

#[test]
#[ignore = "requires local MinIO, Spin 4, AWS CLI, Python and release WASIp2 component"]
fn partial_minio_clients() -> Result<(), Box<dyn std::error::Error>> {
    run("check_partial.py")
}

#[test]
#[ignore = "requires local MinIO, Spin 4, AWS CLI, Python and release WASIp2 component"]
fn uri_minio_clients() -> Result<(), Box<dyn std::error::Error>> {
    run("check_uri.py")
}
