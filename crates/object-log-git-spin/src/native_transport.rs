//! Native operator HTTP reads tolerate one pre-response connection failure.

use async_trait::async_trait;
use object_store::client::{
    ClientOptions, HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse,
    HttpService, ReqwestConnector,
};

#[path = "read_retry.rs"]
mod read_retry;

#[derive(Debug)]
pub(super) struct Connector;

impl HttpConnector for Connector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(Service(
            ReqwestConnector::default().connect(options)?,
        )))
    }
}

#[derive(Debug)]
struct Service(HttpClient);

#[async_trait]
impl HttpService for Service {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        // object_store's reqwest connector classifies incomplete/closed hyper
        // requests as Request, and broken pipes/resets/EOF as Interrupted.
        // Request also covers other pre-response request failures: the shared
        // policy restricts replay to a bodyless GET/HEAD and one extra attempt.
        // This bounds HttpClient executions, not wire transmissions: reqwest
        // retains its own safe protocol-NACK retry behavior. Logical core
        // requests and backend HTTP attempts remain distinct.
        read_retry::retry_read(
            request,
            |request| self.0.execute(request),
            |error| {
                matches!(
                    error.kind(),
                    HttpErrorKind::Request | HttpErrorKind::Interrupted
                )
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use object_store::client::HttpRequestBody;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    };

    #[tokio::test]
    async fn native_connector_retries_only_pre_response_reads()
    -> Result<(), Box<dyn std::error::Error>> {
        for (method, close_count, status, truncated, expected) in [
            (http::Method::GET, 1, 200, false, 2),
            (http::Method::HEAD, 1, 200, false, 2),
            (http::Method::PUT, 1, 200, false, 1),
            (http::Method::POST, 1, 200, false, 1),
            (http::Method::GET, 2, 200, false, 2),
            (http::Method::GET, 0, 503, false, 1),
            (http::Method::GET, 0, 200, true, 1),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let address = listener.local_addr()?;
            listener.set_nonblocking(true)?;
            let done = Arc::new(AtomicBool::new(false));
            let server_done = Arc::clone(&done);
            let server = std::thread::spawn(move || -> std::io::Result<Vec<String>> {
                let deadline = Instant::now() + Duration::from_secs(5);
                let mut requests = Vec::new();
                while !server_done.load(Ordering::Relaxed) && Instant::now() < deadline {
                    let (mut socket, _) = match listener.accept() {
                        Ok(socket) => socket,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
                    let mut header = Vec::new();
                    while !header.ends_with(b"\r\n\r\n") && header.len() < 8192 {
                        let mut byte = [0];
                        match socket.read(&mut byte)? {
                            0 => break,
                            _ => header.push(byte[0]),
                        }
                    }
                    if header.is_empty() {
                        continue;
                    }
                    requests.push(String::from_utf8_lossy(&header).into_owned());
                    if requests.len() > close_count {
                        write!(
                            socket,
                            "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            if truncated { 4 } else { 0 }
                        )?;
                        if truncated {
                            socket.write_all(b"x")?;
                        }
                    }
                }
                Ok(requests)
            });
            let client = Connector.connect(
                &ClientOptions::new()
                    .with_allow_http(true)
                    .with_timeout(Duration::from_secs(2)),
            )?;
            let request = http::Request::builder()
                .method(method.clone())
                .uri(format!("http://{address}/object?versionId=7"))
                .header("range", "bytes=3-5")
                .header("if-match", "version")
                .body(HttpRequestBody::empty())?;
            let response = client.execute(request).await;
            if truncated {
                assert!(response?.into_body().collect().await.is_err());
            } else if close_count == 0 || (close_count == 1 && expected == 2) {
                assert_eq!(response?.status().as_u16(), status);
            } else {
                assert!(response.is_err());
            }
            done.store(true, Ordering::Relaxed);
            let requests = server.join().map_err(|_| "HTTP fixture thread failed")??;
            assert_eq!(
                requests.len(),
                expected,
                "method={method}, closes={close_count}"
            );
            for request in &requests {
                assert!(request.starts_with(&format!("{method} /object?versionId=7 HTTP/1.1\r\n")));
                assert!(request.contains("range: bytes=3-5\r\n"));
                assert!(request.contains("if-match: version\r\n"));
            }
        }
        Ok(())
    }
}
