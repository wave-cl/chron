use base64::Engine;
use chron::{Committed, Event};
use chron_client::ClientError;
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Human,
    Json,
}

/// A mistake in the arguments that clap could not catch (exit code 2).
#[derive(Debug)]
pub struct Usage(pub String);

impl std::fmt::Display for Usage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Usage {}

fn text_or_base64(
    obj: &mut serde_json::Map<String, Value>,
    key: &str,
    bytes: &[u8],
    empty_is_null: bool,
) {
    if bytes.is_empty() && empty_is_null {
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
    if let Some(id) = e.commit_id {
        obj.insert(
            "commit_id".into(),
            Value::String(chron::Uuid::from_bytes(id).to_string()),
        );
    }
    obj.insert("last_in_txn".into(), json!(e.is_last_in_txn()));
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
    text_or_base64(&mut obj, "metadata", &e.metadata, true);
    text_or_base64(&mut obj, "payload", &e.payload, true);
    Value::Object(obj)
}

pub fn print_event(format: Format, e: &Event) {
    match format {
        Format::Json => println!("{}", event_json(e)),
        Format::Human => {
            let who = match e.aggregate {
                Some(a) => format!("{a}#{}", e.index),
                None => "free".to_string(),
            };
            let tags: Vec<String> = e
                .tags
                .iter()
                .map(|t| String::from_utf8_lossy(t).into_owned())
                .collect();
            let payload = match std::str::from_utf8(&e.payload) {
                Ok(s) => s.to_string(),
                Err(_) => format!("<{} bytes>", e.payload.len()),
            };
            println!(
                "{:>8}  {}  [{}]  {}",
                e.position,
                who,
                tags.join(","),
                payload
            );
        }
    }
}

pub fn print_commit(format: Format, c: &Committed) {
    match format {
        Format::Json => {
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
            println!(
                "{}",
                json!({ "first": c.first, "last": c.last, "next": c.next, "appends": appends, "duplicate": c.duplicate })
            );
        }
        Format::Human => {
            if c.duplicate {
                println!(
                    "already committed {}..{} (next {}); nothing written",
                    c.first, c.last, c.next
                );
            } else {
                println!("committed {}..{} (next {})", c.first, c.last, c.next);
            }
            for a in &c.appends {
                println!(
                    "  {}  first {}  index {}  count {}  len {}",
                    a.aggregate.map_or("free".to_string(), |u| u.to_string()),
                    a.first_position,
                    a.first_index,
                    a.count,
                    a.new_len
                );
            }
        }
    }
}

pub fn print_value(format: Format, human: String, value: Value) {
    match format {
        Format::Json => println!("{value}"),
        Format::Human => println!("{human}"),
    }
}

pub fn render_error(e: &anyhow::Error) -> String {
    if let Some(c) = e.downcast_ref::<ClientError>() {
        return match c {
            ClientError::Conflict {
                message, actual, ..
            } => {
                format!("FailedPrecondition: {message} (actual length {actual})")
            }
            ClientError::Fenced { message, epoch } => {
                format!("FailedPrecondition: {message} (daemon epoch {epoch})")
            }
            ClientError::Unconfirmed {
                first,
                next,
                message,
            } => format!(
                "Unconfirmed: committed locally at {first}..{} but {message}",
                next - 1
            ),
            ClientError::Unauthenticated(m) => {
                format!("Unauthenticated: {m}; set --token or CHRON_TOKEN")
            }
            ClientError::InvalidArgument(m) => format!("InvalidArgument: {m}"),
            ClientError::OutOfRange(m) => format!("OutOfRange: {m}"),
            ClientError::PositionOutOfRange { .. } | ClientError::BelowFloor { .. } => {
                format!("OutOfRange: {c}")
            }
            ClientError::PermissionDenied(m) => {
                format!("PermissionDenied: {m}; set --admin-token")
            }
            ClientError::NotFound(m) if m.contains("stream") => format!("NotFound: {m}"),
            ClientError::NotFound(m) => format!("NotFound: {m}; see chron db list"),
            ClientError::AlreadyExists(m) => format!("AlreadyExists: {m}"),
            ClientError::FailedPrecondition(m) => format!("FailedPrecondition: {m}"),
            ClientError::Status(s) => format!("{:?}: {}", s.code(), s.message()),
            other => other.to_string(),
        };
    }
    if let Some(chron::Error::Locked { path }) = e.downcast_ref::<chron::Error>() {
        return format!(
            "{} is open by another process; stop the daemon first",
            path.display()
        );
    }
    format!("{e:#}")
}

pub fn exit_code(e: &anyhow::Error) -> i32 {
    if e.downcast_ref::<crate::client::Unreachable>().is_some()
        || e.downcast_ref::<Usage>().is_some()
    {
        return 2;
    }
    if let Some(
        ClientError::Unavailable(_) | ClientError::Transport(_) | ClientError::NoLeader(_),
    ) = e.downcast_ref::<ClientError>()
    {
        return 2;
    }
    1
}
