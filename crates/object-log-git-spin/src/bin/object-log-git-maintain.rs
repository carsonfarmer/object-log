//! One-shot, local operator access to an existing WAL. No HTTP listener.

#[path = "../auth.rs"]
#[cfg(all(unix, not(target_arch = "wasm32")))]
#[allow(
    dead_code,
    reason = "the local operator shares config validation but serves no HTTP requests"
)]
mod auth;

#[path = "../operator.rs"]
#[cfg(all(unix, not(target_arch = "wasm32")))]
mod operator;

#[cfg(all(unix, not(target_arch = "wasm32")))]
fn main() -> std::process::ExitCode {
    let report = operator::run(std::env::args_os());
    let exit = report.exit();
    if report.write(std::io::stdout().lock()).is_err() {
        // A lost output does not establish that a resume failed to publish.
        return std::process::ExitCode::from(4);
    }
    std::process::ExitCode::from(exit)
}

#[cfg(any(not(unix), target_arch = "wasm32"))]
fn main() -> std::process::ExitCode {
    println!("{{\"operation\":\"input\",\"outcome\":\"unsupported_platform\"}}");
    std::process::ExitCode::from(2)
}
