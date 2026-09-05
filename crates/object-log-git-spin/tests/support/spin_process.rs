//! Test-only shutdown for the Spin launcher and its HTTP trigger descendants.
use std::{
    io,
    net::{SocketAddr, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

fn signal_group(id: u32, signal: &str) -> io::Result<bool> {
    Ok(Command::new("/bin/kill")
        .args([signal, "--", &format!("-{id}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?
        .success())
}

fn group_gone(result: rustix::io::Result<()>) -> bool {
    // A permission error (including transient macOS EPERM) is not evidence
    // that descendants exited. Only ESRCH proves the private group is absent.
    matches!(result, Err(rustix::io::Errno::SRCH))
}

pub fn stop(child: &mut Child, address: &str, signal: &str) -> io::Result<()> {
    let address: SocketAddr = address.parse().map_err(io::Error::other)?;
    let group =
        rustix::process::Pid::from_raw(i32::try_from(child.id()).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("invalid fixture process group"))?;
    signal_group(child.id(), signal)?;
    let start = Instant::now();
    let mut forced = false;
    while start.elapsed() < Duration::from_secs(4) {
        let reaped = child.try_wait()?.is_some();
        let group_absent = group_gone(rustix::process::test_kill_process_group(group));
        let listener_closed = matches!(
            TcpStream::connect_timeout(&address, Duration::from_millis(20)),
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused
        );
        if reaped && group_absent && listener_closed {
            return Ok(());
        }
        if !forced && start.elapsed() >= Duration::from_secs(2) {
            signal_group(child.id(), "-KILL")?;
            forced = true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(io::Error::other(
        "Spin process group or listener remained after shutdown",
    ))
}

#[test]
fn process_group_probe_requires_no_such_process() {
    assert!(group_gone(Err(rustix::io::Errno::SRCH)));
    assert!(!group_gone(Ok(())));
    assert!(!group_gone(Err(rustix::io::Errno::PERM)));
    assert!(!group_gone(Err(rustix::io::Errno::IO)));
}
