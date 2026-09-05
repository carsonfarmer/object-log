//! Opt-in unchanged Git clients with actual credential helper pipes.
#![cfg(not(target_arch = "wasm32"))]

#[test]
#[ignore = "requires local MinIO, Spin 4, AWS CLI, Python, and release WASIp2 component"]
fn auth_minio_credential_helper_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let status = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/check_auth_git.py"
        ))
        .status()?;
    assert!(status.success(), "credential-helper lifecycle failed");
    Ok(())
}

#[test]
#[ignore = "requires Spin 4, Python, and release WASIp2 component"]
fn auth_minio_preflight_without_backend_or_body() -> Result<(), Box<dyn std::error::Error>> {
    for script in ["check_fixture_processes.py", "check_auth.py"] {
        let status = std::process::Command::new("python3")
            .arg(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests")
                    .join(script),
            )
            .status()?;
        assert!(status.success(), "authentication preflight failed");
    }
    Ok(())
}
