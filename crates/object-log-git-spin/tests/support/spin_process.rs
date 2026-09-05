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

pub fn stop(child: &mut Child, address: &str, signal: &str) -> io::Result<()> {
    let address: SocketAddr = address.parse().map_err(io::Error::other)?;
    signal_group(child.id(), signal)?;
    let start = Instant::now();
    let mut forced = false;
    while start.elapsed() < Duration::from_secs(4) {
        let reaped = child.try_wait()?.is_some();
        let group_alive = signal_group(child.id(), "-0")?;
        let listener_closed = matches!(
            TcpStream::connect_timeout(&address, Duration::from_millis(20)),
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused
        );
        if reaped && !group_alive && listener_closed {
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
