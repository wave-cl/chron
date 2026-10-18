//! One log line per call: the method, the gRPC status and how long it
//! took, read from the response trailers (so streams are logged when they
//! end).

use std::time::Duration;

use http::HeaderMap;
use tower_http::classify::{GrpcErrorsAsFailures, GrpcFailureClass, SharedClassifier};
use tower_http::trace::{OnEos, OnFailure, TraceLayer};
use tracing::Span;

#[derive(Clone, Copy)]
pub struct Log;

impl OnEos for Log {
    fn on_eos(self, trailers: Option<&HeaderMap>, stream_duration: Duration, span: &Span) {
        let status: i32 = trailers
            .and_then(|t| t.get("grpc-status"))
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        tracing::info!(parent: span, status, duration_ms = stream_duration.as_millis() as u64, "done");
    }
}

impl OnFailure<GrpcFailureClass> for Log {
    fn on_failure(&mut self, class: GrpcFailureClass, latency: Duration, span: &Span) {
        match class {
            GrpcFailureClass::Code(code) => {
                tracing::info!(parent: span, status = code.get(), duration_ms = latency.as_millis() as u64, "refused")
            }
            GrpcFailureClass::Error(e) => {
                tracing::warn!(parent: span, error = %e, duration_ms = latency.as_millis() as u64, "failed")
            }
        }
    }
}

type Layer = TraceLayer<
    SharedClassifier<GrpcErrorsAsFailures>,
    fn(&http::Request<tonic::body::Body>) -> Span,
    (),
    (),
    (),
    Log,
    Log,
>;

fn span(req: &http::Request<tonic::body::Body>) -> Span {
    let peer = req
        .extensions()
        .get::<tonic::transport::server::TcpConnectInfo>()
        .and_then(|i| i.remote_addr())
        .map(|a| a.to_string())
        .unwrap_or_default();
    tracing::info_span!("rpc", method = %req.uri().path(), %peer)
}

pub fn layer() -> Layer {
    TraceLayer::new_for_grpc()
        .make_span_with(span as fn(&http::Request<tonic::body::Body>) -> Span)
        .on_request(())
        .on_response(())
        .on_body_chunk(())
        .on_eos(Log)
        .on_failure(Log)
}
