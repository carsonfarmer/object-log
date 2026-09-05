use object_store::client::{HttpError, HttpRequest, HttpRequestBody, HttpResponse};

// A pooled connection can close before a response arrives. Retry only a bodyless
// read, once, before exposing response bytes. Writes and streaming-body failures
// retain their uncertain-result semantics. The caller owns per-attempt accounting.
pub(super) async fn retry_read<F, Fut>(
    request: HttpRequest,
    mut attempt: F,
    retryable_error: fn(&HttpError) -> bool,
) -> Result<HttpResponse, HttpError>
where
    F: FnMut(HttpRequest) -> Fut,
    Fut: std::future::Future<Output = Result<HttpResponse, HttpError>>,
{
    let retryable = matches!(*request.method(), http::Method::GET | http::Method::HEAD)
        && request.body().content_length() == 0;
    let (parts, body) = request.into_parts();
    let retry =
        retryable.then(|| http::Request::from_parts(parts.clone(), HttpRequestBody::empty()));
    let result = attempt(http::Request::from_parts(parts, body)).await;
    if let (Some(request), Err(error)) = (retry, &result)
        && retryable_error(error)
    {
        return attempt(request).await;
    }
    result
}
