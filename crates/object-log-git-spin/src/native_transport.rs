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

    // Reqwest may abandon a speculative connection without sending HTTP.
    // Only a reset/EOF before the first byte is not an attempted request.
    fn request_header(mut input: impl Read) -> std::io::Result<Option<String>> {
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            match input.read(&mut byte) {
                Ok(0) if header.is_empty() => return Ok(None),
                Err(error)
                    if header.is_empty()
                        && matches!(
                            error.kind(),
                            std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::ConnectionAborted
                                | std::io::ErrorKind::UnexpectedEof
                        ) =>
                {
                    return Ok(None);
                }
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "partial HTTP request",
                    ));
                }
                Ok(_) => header.push(byte[0]),
                Err(error) => return Err(error),
            }
            if header.len() > 8192 {
                return Err(std::io::Error::other("fixture header exceeds limit"));
            }
        }
        Ok(Some(String::from_utf8_lossy(&header).into_owned()))
    }

    #[test]
    fn empty_connection_resets_are_not_http_requests() -> Result<(), Box<dyn std::error::Error>> {
        struct Reset;
        impl Read for Reset {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::ConnectionReset.into())
            }
        }
        assert!(request_header(Reset)?.is_none());
        assert!(request_header(std::io::empty())?.is_none());
        assert_eq!(
            request_header(std::io::Cursor::new(b"GET / HTTP/1.1\r\n\r\n"))?,
            Some("GET / HTTP/1.1\r\n\r\n".into())
        );
        assert_eq!(
            request_header(std::io::Cursor::new(b"G").chain(Reset))
                .err()
                .ok_or("partial reset accepted")?
                .kind(),
            std::io::ErrorKind::ConnectionReset
        );
        Ok(())
    }

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
                    let Some(header) = request_header(&mut socket)? else {
                        continue;
                    };
                    requests.push(header);
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
            let response = match client.execute(request).await {
                Ok(response) => {
                    let status = response.status().as_u16();
                    let body_failed = response.into_body().collect().await.is_err();
                    Ok((status, body_failed))
                }
                Err(error) => Err(error),
            };
            // Always stop and join before inspecting the client outcome, so a
            // server error cannot be hidden behind a secondary connect failure.
            done.store(true, Ordering::Relaxed);
            let requests = server.join().map_err(|_| "HTTP fixture thread failed")??;
            if truncated {
                assert!(response?.1);
            } else if close_count == 0 || (close_count == 1 && expected == 2) {
                assert_eq!(response?, (status, false));
            } else {
                assert!(response.is_err());
            }
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
