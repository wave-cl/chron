use base64::Engine;
use chron_proto::v1 as pb;
use serde_json::{Value, json};
use uuid::Uuid;

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

pub fn aggregate_text(bytes: Option<&[u8]>) -> String {
    match bytes.and_then(|b| <[u8; 16]>::try_from(b).ok()) {
        Some(arr) => Uuid::from_bytes(arr).to_string(),
        None => "free".to_string(),
    }
}

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

pub fn event_json(e: &pb::Event) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("position".into(), json!(e.position));
    obj.insert(
        "aggregate".into(),
        match &e.aggregate {
            Some(_) => Value::String(aggregate_text(e.aggregate.as_deref())),
            None => Value::Null,
        },
    );
    obj.insert("index".into(), json!(e.index));
    obj.insert(
        "recorded_at_unix_nanos".into(),
        json!(e.recorded_at_unix_nanos),
    );
    obj.insert("first_in_txn".into(), json!(e.first_in_txn));
    obj.insert("last_in_txn".into(), json!(e.last_in_txn));
    obj.insert(
        "tags".into(),
        Value::Array(
            e.tags
                .iter()
                .map(|t| Value::String(String::from_utf8_lossy(t).into_owned()))
                .collect(),
        ),
    );
    text_or_base64(&mut obj, "metadata", &e.metadata, true);
    text_or_base64(&mut obj, "payload", &e.payload, false);
    Value::Object(obj)
}

pub fn print_event(format: Format, e: &pb::Event) {
    match format {
        Format::Json => println!("{}", event_json(e)),
        Format::Human => {
            let who = match &e.aggregate {
                Some(a) => format!("{}#{}", aggregate_text(Some(a)), e.index),
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

pub fn print_commit(format: Format, c: &pb::CommitResponse) {
    match format {
        Format::Json => {
            let appends: Vec<Value> = c
                .appends
                .iter()
                .map(|a| {
                    json!({
                        "aggregate": a.aggregate.as_ref().map(|b| aggregate_text(Some(b))),
                        "first_position": a.first_position,
                        "first_index": a.first_index,
                        "count": a.count,
                        "new_len": a.new_len,
                    })
                })
                .collect();
            println!(
                "{}",
                json!({ "first": c.first, "last": c.last, "next": c.next, "appends": appends })
            );
        }
        Format::Human => {
            println!("committed {}..{} (next {})", c.first, c.last, c.next);
            for a in &c.appends {
                println!(
                    "  {}  first {}  index {}  count {}  len {}",
                    aggregate_text(a.aggregate.as_deref()),
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
    if let Some(status) = e.downcast_ref::<tonic::Status>() {
        let mut s = format!("{:?}: {}", status.code(), status.message());
        if let Some(actual) = status
            .metadata()
            .get(chron_proto::CONFLICT_ACTUAL_LENGTH_HEADER)
            .and_then(|v| v.to_str().ok())
        {
            s.push_str(&format!(" (actual length {actual})"));
        }
        return s;
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
    if let Some(status) = e.downcast_ref::<tonic::Status>()
        && status.code() == tonic::Code::Unavailable
        && status.message().contains("connect")
    {
        return 2;
    }
    1
}
