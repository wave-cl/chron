//! The HTTP/JSON gateway: the same calls as the gRPC service for clients
//! without gRPC, on `--http-listen`, with the same tokens and TLS.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, MatchedPath, Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use bytes::Bytes;
use chron::Uuid;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tower::ServiceExt;

use crate::auth::BearerAuth;
use crate::http_json::{
    ApiError, TxnJson, appends_from_json, committed_json, database_json, error_json, event_json,
    info_json, retained_json, role_name,
};
use crate::metrics::Metrics;
use crate::node::{DbNode, Node};
use crate::ops::{self, PAGE, Read};

/// Every route the gateway serves; `openapi.json` must list exactly these
/// (a unit test checks both directions).
#[cfg_attr(not(test), allow(dead_code))]
pub const ROUTES: &[(&str, &str)] = &[
    ("POST", "/v1/db/{db}/commit"),
    ("GET", "/v1/db/{db}/events"),
    ("GET", "/v1/db/{db}/aggregates/{uuid}/events"),
    ("GET", "/v1/db/{db}/aggregates/{uuid}/len"),
    ("GET", "/v1/db/{db}/tags/{tag}/events"),
    ("GET", "/v1/db/{db}/head"),
    ("GET", "/v1/db/{db}/info"),
    ("GET", "/v1/db/{db}/tail"),
    ("POST", "/v1/db/{db}/flush"),
    ("POST", "/v1/db/{db}/retain"),
    ("GET", "/v1/databases"),
    ("POST", "/v1/databases"),
    ("DELETE", "/v1/databases/{name}"),
    ("GET", "/v1/status"),
    ("GET", "/openapi.json"),
    ("GET", "/"),
];

/// The OpenAPI document, served at `/openapi.json`.
pub const OPENAPI: &str = include_str!("../openapi.json");

const MAX_LIMIT: u64 = 4096;

#[derive(Clone)]
struct HttpState {
    node: Arc<Node>,
    metrics: Arc<Metrics>,
    auth: BearerAuth,
}

/// The connection's peer address, for the log line.
#[derive(Clone, Copy)]
struct Peer(SocketAddr);

type Result<T> = std::result::Result<T, ApiError>;

/// Builds the router: `/v1/...` behind the bearer token, `/openapi.json`
/// and `/` open.
pub fn router(
    node: Arc<Node>,
    metrics: Arc<Metrics>,
    auth: BearerAuth,
    request_timeout: Option<Duration>,
) -> Router {
    let state = HttpState {
        node: node.clone(),
        metrics,
        auth,
    };
    let mut v1 = Router::new()
        .route("/db/{db}/commit", post(commit))
        .route("/db/{db}/events", get(read_all))
        .route("/db/{db}/aggregates/{uuid}/events", get(read_aggregate))
        .route("/db/{db}/aggregates/{uuid}/len", get(aggregate_len))
        .route("/db/{db}/tags/{tag}/events", get(read_tag))
        .route("/db/{db}/head", get(head))
        .route("/db/{db}/info", get(info))
        .route("/db/{db}/tail", get(tail))
        .route("/db/{db}/flush", post(flush))
        .route("/db/{db}/retain", post(retain))
        .route("/databases", get(list_databases).post(create_database))
        .route("/databases/{name}", delete(drop_database))
        .route("/status", get(status))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(middleware::from_fn_with_state(state.clone(), require_token))
        // JSON carries binary as base64, a third larger than the wire.
        .layer(DefaultBodyLimit::max(node.max_message_bytes / 3 * 4 + 1024));
    if let Some(t) = request_timeout {
        v1 = v1.layer(tower_http::timeout::TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            t,
        ));
    }
    Router::new()
        .nest("/v1", v1)
        .route("/openapi.json", get(openapi))
        .route("/", get(index))
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(state.clone(), observe))
        .with_state(state)
}

// ---- middleware

/// One log line per request and the request counter by route and status.
async fn observe(State(st): State<HttpState>, req: Request, next: Next) -> Response {
    let start = Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());
    let peer = req
        .extensions()
        .get::<Peer>()
        .map(|p| p.0.to_string())
        .unwrap_or_default();
    let span = tracing::info_span!("http", %method, %path, %peer);
    let _g = span.enter();
    let res = next.run(req).await;
    let status = res.status();
    st.metrics
        .http_requests
        .with_label_values(&[&route, status.as_str()])
        .inc();
    tracing::info!(
        status = status.as_u16(),
        duration_ms = start.elapsed().as_millis() as u64,
        "done"
    );
    res
}

async fn require_token(State(st): State<HttpState>, req: Request, next: Next) -> Response {
    match st.auth.check(req.headers()) {
        Ok(()) => next.run(req).await,
        Err(s) => ApiError(s).into_response(),
    }
}

async fn not_found() -> ApiError {
    ApiError(Status::not_found("no such route; see /openapi.json"))
}

async fn method_not_allowed() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({ "error": "method not allowed for this route; see /openapi.json", "code": "Unimplemented" })),
    )
        .into_response()
}

// ---- helpers

fn db(st: &HttpState, name: &str) -> Result<Arc<DbNode>> {
    Ok(st.node.resolve_name(name)?)
}

fn admin(st: &HttpState, headers: &HeaderMap) -> Result<()> {
    Ok(st
        .node
        .require_admin_token(crate::auth::bearer_headers(headers))?)
}

fn uuid(s: &str) -> Result<Uuid> {
    Uuid::parse_str(s)
        .map_err(|_| ApiError(Status::invalid_argument(format!("{s:?} is not a UUID"))))
}

fn bad(e: impl std::fmt::Display) -> ApiError {
    ApiError(Status::invalid_argument(e.to_string()))
}

/// `?from&limit&backward` of a finite read.
#[derive(Deserialize, Default)]
struct ReadQuery {
    from: Option<u64>,
    limit: Option<u64>,
    backward: Option<bool>,
    /// A binary tag, base64; the path's tag is then `-`.
    tag_base64: Option<String>,
}

async fn page(dbn: &DbNode, read: Read, q: ReadQuery) -> Result<Json<Value>> {
    let backward = q.backward.unwrap_or(false);
    let from = q.from.unwrap_or(if backward { u64::MAX } else { 0 });
    // 0 means "as many as a page holds", as the gRPC calls' 0 means all.
    let limit = match q.limit {
        None => PAGE as u64,
        Some(0) => MAX_LIMIT,
        Some(n) => n.min(MAX_LIMIT),
    } as usize;
    let read = Arc::new(read);
    let events = ops::read_page(dbn, read.clone(), from, limit).await?;
    let next = events.last().and_then(|l| read.next_cursor(l));
    Ok(Json(json!({
        "events": events.iter().map(event_json).collect::<Vec<_>>(),
        "next": next,
    })))
}

// ---- handlers

#[derive(Deserialize, Default)]
struct CommitQuery {
    epoch: Option<u64>,
}

async fn commit(
    State(st): State<HttpState>,
    Path(name): Path<String>,
    q: std::result::Result<Query<CommitQuery>, axum::extract::rejection::QueryRejection>,
    body: std::result::Result<Json<TxnJson>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<Value>> {
    let dbn = db(&st, &name)?;
    let Query(q) = q.map_err(bad)?;
    let Json(txn) = body.map_err(json_rejection)?;
    let appends = appends_from_json(txn)?;
    let node = st.node.clone();
    let name = dbn.name.clone();
    let start = Instant::now();
    let result = tokio::task::spawn_blocking(move || node.commit_guarded(&dbn, appends, q.epoch))
        .await
        .map_err(|e| ApiError(crate::codec::join("commit", e)))?;
    st.node.metrics.commit(&name, start, &result);
    Ok(Json(committed_json(&result?)))
}

fn json_rejection(r: axum::extract::rejection::JsonRejection) -> ApiError {
    use axum::extract::rejection::JsonRejection;
    match &r {
        JsonRejection::BytesRejection(_) => ApiError(Status::resource_exhausted(format!(
            "the body could not be read in full: {r}"
        ))),
        _ => bad(r),
    }
}

async fn read_all(
    State(st): State<HttpState>,
    Path(name): Path<String>,
    q: std::result::Result<Query<ReadQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<Value>> {
    let dbn = db(&st, &name)?;
    let Query(q) = q.map_err(bad)?;
    let backward = q.backward.unwrap_or(false);
    page(&dbn, Read::All { backward }, q).await
}

async fn read_aggregate(
    State(st): State<HttpState>,
    Path((name, id)): Path<(String, String)>,
    q: std::result::Result<Query<ReadQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<Value>> {
    let dbn = db(&st, &name)?;
    let id = uuid(&id)?;
    let Query(q) = q.map_err(bad)?;
    let backward = q.backward.unwrap_or(false);
    page(&dbn, Read::Aggregate { id, backward }, q).await
}

async fn read_tag(
    State(st): State<HttpState>,
    Path((name, tag)): Path<(String, String)>,
    q: std::result::Result<Query<ReadQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<Value>> {
    let dbn = db(&st, &name)?;
    let Query(q) = q.map_err(bad)?;
    let backward = q.backward.unwrap_or(false);
    let tag = match &q.tag_base64 {
        Some(b) => Bytes::from(crate::http_json::decode_base64(b, "tag_base64").map_err(ApiError)?),
        None => Bytes::from(tag),
    };
    page(&dbn, Read::Tag { tag, backward }, q).await
}

async fn aggregate_len(
    State(st): State<HttpState>,
    Path((name, id)): Path<(String, String)>,
) -> Result<Json<Value>> {
    let dbn = db(&st, &name)?;
    let id = uuid(&id)?;
    let len = ops::blocking(&dbn.db, "aggregate_len", move |db| db.aggregate_len(id)).await?;
    Ok(Json(json!({ "len": len })))
}

async fn head(State(st): State<HttpState>, Path(name): Path<String>) -> Result<Json<Value>> {
    let dbn = db(&st, &name)?;
    Ok(Json(json!({ "head": dbn.db.head() })))
}

async fn info(State(st): State<HttpState>, Path(name): Path<String>) -> Result<Json<Value>> {
    let dbn = db(&st, &name)?;
    Ok(Json(info_json(&ops::info(&st.node, &dbn).await?)))
}

#[derive(Deserialize, Default)]
struct TailQuery {
    from: Option<u64>,
    tag: Option<String>,
    tag_base64: Option<String>,
}

async fn tail(
    State(st): State<HttpState>,
    Path(name): Path<String>,
    q: std::result::Result<Query<TailQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Sse<impl tokio_stream::Stream<Item = std::result::Result<SseEvent, Infallible>>>> {
    let dbn = db(&st, &name)?;
    let Query(q) = q.map_err(bad)?;
    let tag = match (&q.tag_base64, q.tag) {
        (Some(b), _) => Some(crate::http_json::decode_base64(b, "tag_base64").map_err(ApiError)?),
        (None, t) => t.map(String::into_bytes),
    };
    let rx = ops::subscribe(st.node.clone(), dbn, q.from.unwrap_or(0), tag);
    let stream = rx.map(|item| {
        Ok(match item {
            Ok(ev) => SseEvent::default().data(event_json(&ev).to_string()),
            Err(s) => SseEvent::default()
                .event("error")
                .data(error_json(&s).to_string()),
        })
    });
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("heartbeat"),
    ))
}

async fn flush(State(st): State<HttpState>, Path(name): Path<String>) -> Result<StatusCode> {
    let dbn = db(&st, &name)?;
    ops::blocking(&dbn.db, "flush", |db| db.flush()).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct RetainBody {
    floor: u64,
}

async fn retain(
    State(st): State<HttpState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: std::result::Result<Json<RetainBody>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<Value>> {
    admin(&st, &headers)?;
    let dbn = db(&st, &name)?;
    let Json(b) = body.map_err(json_rejection)?;
    let floor = b.floor;
    let r = ops::blocking(&dbn.db, "retain", move |db| db.retain_from(floor)).await?;
    Ok(Json(retained_json(&r)))
}

async fn list_databases(State(st): State<HttpState>, headers: HeaderMap) -> Result<Json<Value>> {
    admin(&st, &headers)?;
    let all: Vec<Value> = st
        .node
        .databases()
        .iter()
        .map(|d| database_json(&d.info()))
        .collect();
    Ok(Json(json!({ "databases": all })))
}

#[derive(Deserialize)]
struct NameBody {
    name: String,
}

async fn create_database(
    State(st): State<HttpState>,
    headers: HeaderMap,
    body: std::result::Result<Json<NameBody>, axum::extract::rejection::JsonRejection>,
) -> Result<(StatusCode, Json<Value>)> {
    admin(&st, &headers)?;
    let Json(b) = body.map_err(json_rejection)?;
    let dbn = ops::create_database(st.node.clone(), b.name).await?;
    Ok((StatusCode::CREATED, Json(database_json(&dbn.info()))))
}

async fn drop_database(
    State(st): State<HttpState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode> {
    admin(&st, &headers)?;
    ops::drop_database(st.node.clone(), name).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn status(State(st): State<HttpState>) -> Json<Value> {
    let node = &st.node;
    let role = node.role();
    Json(json!({
        "role": role_name(role.wire()),
        "epoch": node.epoch(),
        "head": node.default_db().db.head(),
        "fenced_by": node.fenced_by(),
        "replicating_from": node.leader_addr(),
        "databases": node.databases().len(),
    }))
}

async fn openapi() -> Response {
    ([(header::CONTENT_TYPE, "application/json")], OPENAPI).into_response()
}

async fn index() -> &'static str {
    "chrond HTTP gateway: see /openapi.json\n"
}

// ---- serving

/// The HTTP twin of `Tls::server_config`: a rustls server config from the
/// daemon's PEM files, with HTTP/1.1 as the only protocol.
pub fn rustls_config(tls: &crate::Tls) -> anyhow::Result<Arc<tokio_rustls::rustls::ServerConfig>> {
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio_rustls::rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier};
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&tls.cert_pem)
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| anyhow::anyhow!("TLS certificate: {e}"))?;
    let key =
        PrivateKeyDer::from_pem_slice(&tls.key_pem).map_err(|e| anyhow::anyhow!("TLS key: {e}"))?;
    let builder = ServerConfig::builder();
    let mut cfg = match &tls.client_ca_pem {
        Some(ca) => {
            let mut roots = RootCertStore::empty();
            for c in CertificateDer::pem_slice_iter(ca) {
                roots
                    .add(c.map_err(|e| anyhow::anyhow!("client CA: {e}"))?)
                    .map_err(|e| anyhow::anyhow!("client CA: {e}"))?;
            }
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| anyhow::anyhow!("client CA: {e}"))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)?
        }
        None => builder.with_no_client_auth().with_single_cert(certs, key)?,
    };
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(cfg))
}

/// Accepts connections until `cancel` fires; plaintext, or TLS when a
/// config is given.
pub async fn serve(
    listener: TcpListener,
    router: Router,
    tls: Option<Arc<tokio_rustls::rustls::ServerConfig>>,
    cancel: CancellationToken,
) {
    let mut tasks = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            _ = cancel.cancelled() => break,
            a = listener.accept() => a,
        };
        let (stream, peer) = match accepted {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(error = %e, "http accept");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let router = router.clone();
        let tls = tls.clone();
        tasks.spawn(async move {
            let svc = hyper::service::service_fn(
                move |mut req: hyper::Request<hyper::body::Incoming>| {
                    req.extensions_mut().insert(Peer(peer));
                    let router = router.clone();
                    async move {
                        let req = req.map(Body::new);
                        router.oneshot(req).await
                    }
                },
            );
            match tls {
                Some(cfg) => {
                    let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
                    let handshake =
                        tokio::time::timeout(Duration::from_secs(10), acceptor.accept(stream))
                            .await;
                    match handshake {
                        Ok(Ok(tls_stream)) => {
                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(TokioIo::new(tls_stream), svc)
                                .await;
                        }
                        Ok(Err(e)) => tracing::debug!(%peer, error = %e, "http tls handshake"),
                        Err(_) => tracing::debug!(%peer, "http tls handshake timed out"),
                    }
                }
                None => {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                }
            }
        });
    }
    tasks.abort_all();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The document lists exactly the routes the router serves.
    #[test]
    fn openapi_matches_the_route_table() {
        let doc: Value = serde_json::from_str(OPENAPI).expect("openapi.json parses");
        let paths = doc["paths"].as_object().expect("paths");
        for (method, path) in ROUTES {
            let ops = paths
                .get(*path)
                .unwrap_or_else(|| panic!("{path} missing from openapi.json"));
            assert!(
                ops.get(method.to_lowercase()).is_some(),
                "{method} {path} missing from openapi.json"
            );
        }
        for (path, ops) in paths {
            for method in ops.as_object().unwrap().keys() {
                let m = method.to_uppercase();
                assert!(
                    ROUTES.iter().any(|(rm, rp)| *rm == m && rp == path),
                    "{m} {path} is in openapi.json but not served"
                );
            }
        }
    }
}
