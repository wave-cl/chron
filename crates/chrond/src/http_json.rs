//! The gateway's JSON: the same shapes as the CLI's `--json` output and
//! `commit -f` input, and the error object every failure becomes.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use chron::{Committed, Event, Expected, NewEvent, Retained, Uuid};
use chron_proto::convert;
use chron_proto::v1 as pb;
use serde::Deserialize;
use serde_json::{Value, json};
use tonic::{Code, Status};

use crate::metrics::code_label;

/// A transaction as `chron commit -f` takes it.
#[derive(Deserialize)]
pub struct TxnJson {
    pub appends: Vec<AppendJson>,
    /// A commit id as 32 hex digits or a hyphenated UUID; absent for none.
    #[serde(default)]
    pub commit_id: Option<String>,
}

/// The commit id a JSON transaction names, parsed.
pub fn commit_id_from_json(txn: &TxnJson) -> Result<Option<[u8; 16]>, Status> {
    txn.commit_id
        .as_deref()
        .map(|s| {
            Uuid::parse_str(s).map(Uuid::into_bytes).map_err(|_| {
                Status::invalid_argument(format!("commit_id {s:?} is not 32 hex digits or a UUID"))
            })
        })
        .transpose()
}

#[derive(Deserialize)]
pub struct AppendJson {
    /// A UUID, `"free"`, or absent for free events.
    #[serde(default)]
    pub aggregate: Option<String>,
    #[serde(default)]
    pub expect: Option<ExpectJson>,
    pub events: Vec<EventJson>,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum ExpectJson {
    Word(String),
    Exact(u64),
}

#[derive(Deserialize)]
pub struct EventJson {
    #[serde(default)]
    pub payload: Option<String>,
    #[serde(default)]
    pub payload_base64: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub tags_base64: Vec<String>,
    #[serde(default)]
    pub metadata: Option<String>,
    #[serde(default)]
    pub metadata_base64: Option<String>,
}

pub fn decode_base64(b: &str, what: &str) -> Result<Vec<u8>, Status> {
    base64::engine::general_purpose::STANDARD
        .decode(b)
        .map_err(|e| Status::invalid_argument(format!("{what} is not base64: {e}")))
}

fn bytes(text: Option<String>, b64: Option<String>, what: &str) -> Result<Vec<u8>, Status> {
    match (text, b64) {
        (Some(t), None) => Ok(t.into_bytes()),
        (None, Some(b)) => decode_base64(&b, &format!("{what}_base64")),
        (None, None) => Ok(Vec::new()),
        (Some(_), Some(_)) => Err(Status::invalid_argument(format!(
            "give {what} or {what}_base64, not both"
        ))),
    }
}

/// The wire appends of a JSON transaction, so a gateway commit takes the
/// same path (and gets the same refusals) as a gRPC one.
pub fn appends_from_json(txn: TxnJson) -> Result<Vec<pb::Append>, Status> {
    let mut out = Vec::with_capacity(txn.appends.len());
    for a in txn.appends {
        let aggregate = match a.aggregate.as_deref() {
            None | Some("free") => None,
            Some(s) => Some(Uuid::parse_str(s).map_err(|_| {
                Status::invalid_argument(format!("{s:?} is neither a UUID nor \"free\""))
            })?),
        };
        let expected = match a.expect {
            None => Expected::Any,
            Some(ExpectJson::Exact(n)) => Expected::Exact(n),
            // A number in quotes is accepted too, as the CLI's commit -f does.
            Some(ExpectJson::Word(w)) => match w.as_str() {
                "any" => Expected::Any,
                "new" => Expected::New,
                other => match other.parse::<u64>() {
                    Ok(n) => Expected::Exact(n),
                    Err(_) => {
                        return Err(Status::invalid_argument(format!(
                            "expect takes any, new or a number, not {other:?}"
                        )));
                    }
                },
            },
        };
        if aggregate.is_none() && expected != Expected::Any {
            return Err(Status::invalid_argument(
                "free events have no length to expect; use \"any\"",
            ));
        }
        let mut events = Vec::with_capacity(a.events.len());
        for e in a.events {
            let mut tags: Vec<bytes::Bytes> = e.tags.into_iter().map(bytes::Bytes::from).collect();
            for t in e.tags_base64 {
                tags.push(decode_base64(&t, "tags_base64")?.into());
            }
            let mut event = NewEvent::new(bytes(e.payload, e.payload_base64, "payload")?)
                .metadata(bytes(e.metadata, e.metadata_base64, "metadata")?);
            for t in tags {
                event = event.tag(t);
            }
            events.push(event);
        }
        out.push(convert::append_to_wire(aggregate, expected, events));
    }
    Ok(out)
}

fn text_or_base64(obj: &mut serde_json::Map<String, Value>, key: &str, bytes: &[u8]) {
    if bytes.is_empty() {
        obj.insert(key.to_string(), Value::Null);
    } else if let Ok(s) = std::str::from_utf8(bytes) {
        obj.insert(key.to_string(), Value::String(s.to_string()));
    } else {
        obj.insert(
            format!("{key}_base64"),
            Value::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
        );
    }
}

/// An event as the CLI prints it with `--json`.
pub fn event_json(e: &Event) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("position".into(), json!(e.position));
    obj.insert(
        "aggregate".into(),
        match e.aggregate {
            Some(a) => Value::String(a.to_string()),
            None => Value::Null,
        },
    );
    obj.insert("index".into(), json!(e.index));
    obj.insert("recorded_at_unix_nanos".into(), json!(e.recorded_at));
    obj.insert("first_in_txn".into(), json!(e.is_first_in_txn()));
    obj.insert("last_in_txn".into(), json!(e.is_last_in_txn()));
    if let Some(id) = e.commit_id {
        obj.insert(
            "commit_id".into(),
            Value::String(Uuid::from_bytes(id).to_string()),
        );
    }
    let (text, binary): (Vec<&[u8]>, Vec<&[u8]>) = e
        .tags
        .iter()
        .map(|t| t.as_ref())
        .partition(|t| std::str::from_utf8(t).is_ok());
    obj.insert(
        "tags".into(),
        Value::Array(
            text.iter()
                .map(|t| Value::String(String::from_utf8_lossy(t).into_owned()))
                .collect(),
        ),
    );
    if !binary.is_empty() {
        obj.insert(
            "tags_base64".into(),
            Value::Array(
                binary
                    .iter()
                    .map(|t| Value::String(base64::engine::general_purpose::STANDARD.encode(t)))
                    .collect(),
            ),
        );
    }
    text_or_base64(&mut obj, "metadata", &e.metadata);
    text_or_base64(&mut obj, "payload", &e.payload);
    Value::Object(obj)
}

pub fn committed_json(c: &Committed) -> Value {
    let appends: Vec<Value> = c
        .appends
        .iter()
        .map(|a| {
            json!({
                "aggregate": a.aggregate.map(|u| u.to_string()),
                "first_position": a.first_position,
                "first_index": a.first_index,
                "count": a.count,
                "new_len": a.new_len,
            })
        })
        .collect();
    json!({ "first": c.first, "last": c.last, "next": c.next, "appends": appends, "duplicate": c.duplicate })
}

pub fn role_name(role: pb::Role) -> &'static str {
    match role {
        pb::Role::Leader => "leader",
        pb::Role::Replica => "replica",
        pb::Role::Fenced => "fenced",
        pb::Role::Candidate => "candidate",
        pb::Role::Removed => "removed",
        pb::Role::Unspecified => "unknown",
    }
}

pub fn members_json(m: &pb::GetMembersResponse) -> Value {
    let members = m.members.clone().unwrap_or_default();
    json!({
        "members": { "epoch": members.epoch, "version": members.version, "addrs": members.addrs },
        "committed": m.committed, "member": m.member, "leader_addr": m.leader_addr,
    })
}

pub fn info_json(i: &pb::InfoResponse) -> Value {
    let o = i.options.unwrap_or_default();
    json!({
        "database": i.database, "path": i.path,
        "db_id": Uuid::from_slice(&i.db_id).map(|u| u.to_string()).ok(),
        "role": role_name(i.role()), "epoch": i.epoch, "head": i.head, "floor": i.floor,
        "segments": i.segments, "bytes": i.bytes,
        "fsync": if i.fsync { "always" } else { "never" },
        "options": { "verify_all_segments": o.verify_all_segments,
                     "segment_max_bytes": o.segment_max_bytes, "max_record_bytes": o.max_record_bytes },
        "replicating_from": i.replicating_from, "replica_connected": i.replica_connected,
        "leader_head": i.leader_head, "last_replication_error": i.last_replication_error,
        "fenced_by": i.fenced_by,
        "leader_addr": i.leader_addr, "lease_remaining_ms": i.lease_remaining_ms,
        "peers": i.peers, "voted_epoch": i.voted_epoch,
        "majority_applied": i.majority_applied, "sync_commit": i.sync_commit,
        "confirmed": i.confirmed, "read_consistency": consistency_name(i.read_consistency),
        "followers": i.followers.iter().map(|f| json!({
            "peer": f.peer, "applied": f.applied, "durable": f.durable, "counted": f.counted,
        })).collect::<Vec<_>>(),
        "members_version": i.members_version, "members_epoch": i.members_epoch,
        "members_committed": i.members_committed, "member": i.member,
    })
}

pub fn database_json(d: &pb::DatabaseInfo) -> Value {
    json!({
        "name": d.name, "head": d.head, "floor": d.floor, "epoch": d.epoch,
        "db_id": Uuid::from_slice(&d.db_id).map(|u| u.to_string()).ok(),
    })
}

pub fn retained_json(r: &Retained) -> Value {
    json!({ "floor": r.floor, "segments_removed": r.segments_removed, "bytes_freed": r.bytes_freed })
}

/// The HTTP status a gRPC code becomes.
pub fn http_status(code: Code) -> StatusCode {
    match code {
        Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        Code::InvalidArgument => StatusCode::BAD_REQUEST,
        Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        Code::PermissionDenied => StatusCode::FORBIDDEN,
        Code::NotFound => StatusCode::NOT_FOUND,
        Code::AlreadyExists | Code::FailedPrecondition => StatusCode::CONFLICT,
        Code::OutOfRange => StatusCode::RANGE_NOT_SATISFIABLE,
        Code::ResourceExhausted => StatusCode::PAYLOAD_TOO_LARGE,
        Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// The error object: the message, the gRPC code's name, and whatever
/// metadata the status carried.
pub fn error_json(s: &Status) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("error".into(), Value::String(s.message().to_string()));
    obj.insert(
        "code".into(),
        Value::String(code_label(s.code() as i32).to_string()),
    );
    let num = |key: &str| -> Option<u64> { s.metadata().get(key)?.to_str().ok()?.parse().ok() };
    if let Some(n) = num(chron_proto::CONFLICT_ACTUAL_LENGTH_HEADER) {
        obj.insert("actual_length".into(), json!(n));
    }
    if let Some(a) = s
        .metadata()
        .get(chron_proto::CONFLICT_AGGREGATE_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        obj.insert("aggregate".into(), Value::String(a.to_string()));
    }
    for (key, name) in [
        (chron_proto::EPOCH_HEADER, "epoch"),
        (chron_proto::POSITION_HEADER, "position"),
        (chron_proto::FLOOR_HEADER, "floor"),
        (chron_proto::HEAD_HEADER, "head"),
        (chron_proto::COMMITTED_FIRST_HEADER, "committed_first"),
        (chron_proto::COMMITTED_NEXT_HEADER, "committed_next"),
    ] {
        if let Some(n) = num(key) {
            obj.insert(name.into(), json!(n));
        }
    }
    // The two 409s told apart without parsing the message.
    if s.code() == Code::FailedPrecondition {
        let reason = if obj.contains_key("actual_length") {
            Some("expectation")
        } else if obj.contains_key("epoch") {
            Some("epoch")
        } else {
            None
        };
        if let Some(r) = reason {
            obj.insert("reason".into(), Value::String(r.into()));
        }
    }
    Value::Object(obj)
}

/// A failed call: a gRPC status rendered as the error object.
pub struct ApiError(pub Status);

impl From<Status> for ApiError {
    fn from(s: Status) -> Self {
        ApiError(s)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (http_status(self.0.code()), Json(error_json(&self.0))).into_response()
    }
}

/// The JSON name of a wire consistency.
pub fn consistency_name(c: i32) -> &'static str {
    match pb::Consistency::try_from(c).unwrap_or(pb::Consistency::Unspecified) {
        pb::Consistency::Local => "local",
        pb::Consistency::Confirmed => "confirmed",
        pb::Consistency::Unspecified => "default",
    }
}

/// A `?consistency=` value: `local`, `confirmed`, or absent for the
/// daemon's default.
pub fn consistency_from_query(s: Option<&str>) -> Result<pb::Consistency, Status> {
    Ok(match s {
        None => pb::Consistency::Unspecified,
        Some("local") => pb::Consistency::Local,
        Some("confirmed") => pb::Consistency::Confirmed,
        Some(other) => {
            return Err(Status::invalid_argument(format!(
                "consistency {other:?}: expected local or confirmed"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_transaction_round_trips_text_base64_and_free() {
        let txn: TxnJson = serde_json::from_str(
            r#"{"appends":[
                {"aggregate":"00000000-0000-0000-0000-000000000001","expect":"new","events":[{"payload":"a","tags":["t"]}]},
                {"aggregate":"free","events":[{"payload_base64":"/w==","tags_base64":["/w=="]}]},
                {"events":[{"payload":"c"}]}
            ]}"#,
        )
        .unwrap();
        let appends = appends_from_json(txn).unwrap();
        assert_eq!(appends.len(), 3);
        assert!(appends[0].aggregate.is_some() && appends[1].aggregate.is_none());
        assert_eq!(appends[1].events[0].payload, vec![0xff]);
        assert_eq!(appends[1].events[0].tags, vec![vec![0xffu8]]);
        let bad: TxnJson =
            serde_json::from_str(r#"{"appends":[{"events":[{"payload_base64":"nope!"}]}]}"#)
                .unwrap();
        assert_eq!(
            appends_from_json(bad).unwrap_err().code(),
            Code::InvalidArgument
        );
        let free_expect: TxnJson =
            serde_json::from_str(r#"{"appends":[{"expect":3,"events":[{"payload":"c"}]}]}"#)
                .unwrap();
        let err = appends_from_json(free_expect).unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("free events"), "{err}");
    }

    #[test]
    fn binary_payloads_render_as_base64_and_codes_map_to_statuses() {
        let ev = chron::Event {
            position: 3,
            aggregate: None,
            index: 0,
            recorded_at: 1,
            flags: 0,
            commit_id: None,
            tags: vec![],
            metadata: bytes::Bytes::new(),
            payload: bytes::Bytes::from_static(&[0xff, 0xfe]),
        };
        let v = event_json(&ev);
        assert_eq!(v["payload_base64"], "//4=");
        assert!(v.get("payload").is_none());
        assert_eq!(v["metadata"], Value::Null);
        assert_eq!(http_status(Code::InvalidArgument), StatusCode::BAD_REQUEST);
        assert_eq!(http_status(Code::Unauthenticated), StatusCode::UNAUTHORIZED);
        assert_eq!(http_status(Code::PermissionDenied), StatusCode::FORBIDDEN);
        assert_eq!(http_status(Code::NotFound), StatusCode::NOT_FOUND);
        assert_eq!(http_status(Code::FailedPrecondition), StatusCode::CONFLICT);
        assert_eq!(http_status(Code::AlreadyExists), StatusCode::CONFLICT);
        assert_eq!(
            http_status(Code::OutOfRange),
            StatusCode::RANGE_NOT_SATISFIABLE
        );
        assert_eq!(
            http_status(Code::ResourceExhausted),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            http_status(Code::Unavailable),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            http_status(Code::Internal),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
