//! A tower layer counting every gRPC call by method and status code,
//! timing it to the end of its stream, and tracking open streams.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use http::{HeaderMap, Request, Response};
use http_body::{Body, Frame};
use tower::{Layer, Service};

use crate::metrics::{Metrics, code_label, method_label};
use crate::node::Node;

#[derive(Clone)]
pub struct RpcMetricsLayer {
    metrics: Arc<Metrics>,
    node: Arc<Node>,
}

impl RpcMetricsLayer {
    pub fn new(metrics: Arc<Metrics>, node: Arc<Node>) -> Self {
        RpcMetricsLayer { metrics, node }
    }
}

impl<S> Layer<S> for RpcMetricsLayer {
    type Service = RpcMetrics<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RpcMetrics {
            inner,
            metrics: self.metrics.clone(),
            node: self.node.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RpcMetrics<S> {
    inner: S,
    metrics: Arc<Metrics>,
    node: Arc<Node>,
}

/// The `grpc-status` in headers or trailers, when present.
fn grpc_status(headers: &HeaderMap) -> Option<i32> {
    headers.get("grpc-status")?.to_str().ok()?.parse().ok()
}

/// The `db` label: the database the request names when the daemon has
/// it, `default` without a header, `unknown` otherwise.
fn db_label(node: &Node, headers: &HeaderMap) -> String {
    match headers.get(chron_proto::DATABASE_HEADER) {
        None => chron_proto::DEFAULT_DATABASE.to_string(),
        Some(v) => match v.to_str() {
            Ok(name) if node.get(name).is_some() => name.to_string(),
            _ => "unknown".to_string(),
        },
    }
}

/// One call in flight; records once, at the end.
struct Recording {
    metrics: Arc<Metrics>,
    db: String,
    method: &'static str,
    streaming: bool,
    start: Instant,
}

impl Recording {
    fn finish(self, code: i32) {
        self.metrics
            .rpc_calls
            .with_label_values(&[&self.db, self.method, code_label(code)])
            .inc();
        self.metrics
            .rpc_duration
            .with_label_values(&[&self.db, self.method])
            .observe(self.start.elapsed().as_secs_f64());
        if self.streaming {
            self.metrics
                .streams_open
                .with_label_values(&[&self.db, self.method])
                .dec();
        }
    }
}

impl<S, ReqB, ResB> Service<Request<ReqB>> for RpcMetrics<S>
where
    S: Service<Request<ReqB>, Response = Response<ResB>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ResB: Body<Data = Bytes> + Send + 'static,
{
    type Response = Response<MetricsBody<ResB>>;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqB>) -> Self::Future {
        let (method, streaming) = method_label(req.uri().path());
        let rec = Recording {
            metrics: self.metrics.clone(),
            db: db_label(&self.node, req.headers()),
            method,
            streaming,
            start: Instant::now(),
        };
        let fut = self.inner.call(req);
        Box::pin(async move {
            let res = fut.await?;
            let (parts, body) = res.into_parts();
            // Trailers-only: the status is in the headers and the call is
            // over. Otherwise it ends with the body's trailers.
            let rec = match grpc_status(&parts.headers) {
                Some(code) => {
                    rec.finish(code);
                    None
                }
                None => {
                    if rec.streaming {
                        rec.metrics
                            .streams_open
                            .with_label_values(&[&rec.db, rec.method])
                            .inc();
                    }
                    Some(rec)
                }
            };
            Ok(Response::from_parts(
                parts,
                MetricsBody { inner: body, rec },
            ))
        })
    }
}

pin_project_lite::pin_project! {
    /// The response body, recording the call when its trailers arrive or
    /// when it is dropped unfinished (a client that went away).
    pub struct MetricsBody<B> {
        #[pin]
        inner: B,
        rec: Option<Recording>,
    }

    impl<B> PinnedDrop for MetricsBody<B> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let Some(rec) = this.rec.take() {
                rec.finish(1); // Canceled
            }
        }
    }
}

impl<B> Body for MetricsBody<B>
where
    B: Body<Data = Bytes>,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let polled = this.inner.poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref()
                    && let Some(rec) = this.rec.take()
                {
                    rec.finish(grpc_status(trailers).unwrap_or(0));
                }
            }
            Poll::Ready(Some(Err(_))) => {
                if let Some(rec) = this.rec.take() {
                    rec.finish(2); // Unknown
                }
            }
            Poll::Ready(None) => {
                if let Some(rec) = this.rec.take() {
                    rec.finish(0);
                }
            }
            Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}
