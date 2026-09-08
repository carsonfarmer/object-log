use object_store::client::{HttpError, HttpRequest, HttpResponse};

// WAL heads never restore an old version; immutable creates never overwrite.
// Replaying either exact condition cannot overwrite an intervening publication.
// A failed replay cannot disprove success of the first attempt: keep its error.
pub(super) async fn retry_request<F, Fut>(
    request: HttpRequest,
    mut attempt: F,
    retryable_error: fn(&HttpError) -> bool,
) -> Result<HttpResponse, HttpError>
where
    F: FnMut(HttpRequest) -> Fut,
    Fut: std::future::Future<Output = Result<HttpResponse, HttpError>>,
{
    let read = matches!(*request.method(), http::Method::GET | http::Method::HEAD)
        && request.body().content_length() == 0;
    let condition = |name: http::header::HeaderName| {
        let mut values = request.headers().get_all(name).iter();
        let value = values.next()?;
        values.next().is_none().then_some(value)
    };
    let create = condition(http::header::IF_NONE_MATCH).is_some_and(|v| v == "*");
    let update = condition(http::header::IF_MATCH).is_some_and(|v| {
        let value = v.as_bytes();
        value.len() >= 2
            && value[0] == b'"'
            && value[value.len() - 1] == b'"'
            && !value[1..value.len() - 1]
                .iter()
                .any(|b| matches!(b, b'"' | b','))
    });
    let conditional_put = *request.method() == http::Method::PUT && (create || update);
    let retry = (read || conditional_put).then(|| request.clone());
    match (retry, attempt(request).await) {
        (Some(request), Err(first)) if retryable_error(&first) => {
            let response = attempt(request).await;
            if read || response.as_ref().is_ok_and(|r| r.status().is_success()) {
                response
            } else {
                Err(first)
            }
        }
        (_, result) => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{BodyExt, Full};
    use object_store::client::{HttpErrorKind, HttpRequestBody, HttpResponseBody};

    fn lost_response() -> HttpError {
        HttpError::new(
            HttpErrorKind::Unknown,
            std::io::Error::other("lost response"),
        )
    }

    fn response(status: u16) -> HttpResponse {
        http::Response::builder()
            .status(status)
            .body(HttpResponseBody::new(
                Full::new(bytes::Bytes::new()).map_err(|never| match never {}),
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn conditional_replay_preserves_bytes_and_uncertainty() {
        for header in [http::header::IF_MATCH, http::header::IF_NONE_MATCH] {
            for status in [200, 403, 404, 409, 412, 500] {
                let condition = if header == http::header::IF_MATCH {
                    "\"revision-1\""
                } else {
                    "*"
                };
                let request = http::Request::builder()
                    .method("PUT")
                    .uri("http://store/key")
                    .header(header.clone(), condition)
                    .body(HttpRequestBody::from(object_store::PutPayload::from_iter(
                        [
                            bytes::Bytes::from_static(b"candidate"),
                            bytes::Bytes::from_static(b" body"),
                        ],
                    )))
                    .unwrap();
                let mut attempts = 0;
                let result = retry_request(
                    request,
                    |request| {
                        attempts += 1;
                        let number = attempts;
                        let header = header.clone();
                        async move {
                            assert_eq!(request.method(), "PUT");
                            assert_eq!(request.uri(), "http://store/key");
                            assert_eq!(request.headers()[header], condition);
                            let bytes = request.into_body().collect().await.unwrap().to_bytes();
                            assert_eq!(bytes.as_ref(), b"candidate body");
                            if number == 1 {
                                Err(lost_response())
                            } else {
                                Ok(response(status))
                            }
                        }
                    },
                    |_| true,
                )
                .await;
                assert_eq!(attempts, 2);
                if status == 200 {
                    assert_eq!(result.unwrap().status(), status);
                } else {
                    assert!(result.unwrap_err().to_string().contains("lost response"));
                }
            }
        }
    }

    #[tokio::test]
    async fn retries_are_bounded_and_fail_closed() {
        for (method, header, value, retryable, calls) in [
            ("PUT", "if-match", "*", true, 1),
            ("PUT", "if-match", "\"one\", \"two\"", true, 1),
            ("PUT", "if-match", "", true, 1),
            ("PUT", "if-none-match", "\"one\"", true, 1),
            ("POST", "if-none-match", "*", true, 1),
            ("PUT", "if-none-match", "*", false, 1),
            ("PUT", "if-match", "\"one\"", true, 2),
            ("GET", "x-test", "value", true, 2),
            ("HEAD", "x-test", "value", true, 2),
        ] {
            let request = http::Request::builder()
                .method(method)
                .uri("http://store/key")
                .header(header, value)
                .body(HttpRequestBody::empty())
                .unwrap();
            let mut attempts = 0;
            let result = retry_request(
                request,
                |_| {
                    attempts += 1;
                    std::future::ready(if attempts == 1 {
                        Err(lost_response())
                    } else {
                        Err(HttpError::new(
                            HttpErrorKind::Unknown,
                            std::io::Error::other("second error"),
                        ))
                    })
                },
                if retryable { |_| true } else { |_| false },
            )
            .await;
            assert_eq!(attempts, calls, "{method} {header}: {value}");
            if method == "PUT" {
                assert!(result.unwrap_err().to_string().contains("lost response"));
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[tokio::test]
    async fn repeated_condition_fields_are_not_replayed() {
        for header in [http::header::IF_MATCH, http::header::IF_NONE_MATCH] {
            let value = if header == http::header::IF_MATCH {
                "\"old\""
            } else {
                "*"
            };
            let request = http::Request::builder()
                .method("PUT")
                .uri("http://store/key")
                .header(header.clone(), value)
                .header(header, "\"new\"")
                .body(HttpRequestBody::empty())
                .unwrap();
            let mut attempts = 0;
            let result = retry_request(
                request,
                |_| {
                    attempts += 1;
                    std::future::ready(Err(lost_response()))
                },
                |_| true,
            )
            .await;
            assert!(result.is_err());
            assert_eq!(attempts, 1);
        }
    }
}
