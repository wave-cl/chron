//! The metrics endpoint: a plain HTTP/1 listener serving `GET /metrics`.
//! No token and no TLS: bind it to a private interface.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::metrics::Metrics;

/// Accepts connections until `cancel` fires; each is served on its own
/// task and dropped at shutdown.
pub async fn serve(listener: TcpListener, metrics: Arc<Metrics>, cancel: CancellationToken) {
    let mut tasks = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            _ = cancel.cancelled() => break,
            a = listener.accept() => a,
        };
        match accepted {
            Ok((stream, _)) => {
                let metrics = metrics.clone();
                tasks.spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req| handle(req, metrics.clone()));
                    if let Err(e) = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, svc)
                        .await
                    {
                        tracing::debug!(error = %e, "metrics connection");
                    }
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "metrics accept");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    tasks.abort_all();
}

fn text(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("valid response")
}

async fn handle(
    req: Request<Incoming>,
    metrics: Arc<Metrics>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let head_only = req.method() == Method::HEAD;
    if req.method() != Method::GET && !head_only {
        let mut r = text(StatusCode::METHOD_NOT_ALLOWED, "GET or HEAD\n");
        r.headers_mut()
            .insert("allow", "GET, HEAD".parse().expect("valid header"));
        return Ok(r);
    }
    let mut r = match req.uri().path() {
        "/metrics" => {
            let rendered = tokio::task::spawn_blocking(move || metrics.render())
                .await
                .unwrap_or_default();
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                .body(Full::new(Bytes::from(rendered)))
                .expect("valid response")
        }
        "/" => text(StatusCode::OK, "chrond metrics at /metrics\n"),
        _ => text(StatusCode::NOT_FOUND, "not found\n"),
    };
    if head_only {
        *r.body_mut() = Full::new(Bytes::new());
    }
    Ok(r)
}
