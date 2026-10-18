use std::io::Read;

use anyhow::Context;
use base64::Engine;
use chron::{Expected, NewEvent};
use chron_client::Transaction;
use clap::Args;
use serde::Deserialize;

use crate::client::{self, Conn};
use crate::cmd::{parse_aggregate, parse_expect};
use crate::output::{Format, Usage, print_commit};

#[derive(Args, Debug)]
pub struct AppendArgs {
    /// The aggregate's UUID, or `free`.
    aggregate: String,
    /// any, new, or the exact length the aggregate must have.
    #[arg(long, default_value = "any")]
    expect: String,
    /// A tag; repeatable.
    #[arg(long = "tag")]
    tags: Vec<String>,
    /// Metadata bytes.
    #[arg(long)]
    metadata: Option<String>,
    /// A payload; repeatable: every -d is one event, all in one
    /// transaction, in order.
    #[arg(short = 'd', long = "data", conflicts_with = "stdin")]
    data: Vec<String>,
    /// Read the payloads from stdin, one event per line.
    #[arg(long)]
    stdin: bool,
    /// The epoch the daemon must be at (a fencing token).
    #[arg(long)]
    epoch: Option<u64>,
}

async fn send(
    conn: &Conn,
    format: Format,
    txn: Transaction,
    epoch: Option<u64>,
) -> anyhow::Result<()> {
    let c = client::connect(conn).await?;
    let committed = match epoch {
        Some(e) => c.commit_with_epoch(txn, e).await?,
        None => c.commit(txn).await?,
    };
    print_commit(format, &committed);
    Ok(())
}

pub async fn append(conn: &Conn, format: Format, a: AppendArgs) -> anyhow::Result<()> {
    let aggregate = parse_aggregate(&a.aggregate)?;
    let expected = parse_expect(&a.expect)?;
    if aggregate.is_none() && expected != Expected::Any {
        return Err(Usage("free events have no length to expect; use --expect any".into()).into());
    }
    let payloads: Vec<Vec<u8>> = if a.stdin {
        let mut text = String::new();
        std::io::stdin()
            .read_to_string(&mut text)
            .context("read stdin")?;
        text.lines().map(|l| l.as_bytes().to_vec()).collect()
    } else {
        a.data.into_iter().map(String::into_bytes).collect()
    };
    if payloads.is_empty() {
        return Err(Usage("give at least one payload with -d, or lines on --stdin".into()).into());
    }
    let events: Vec<NewEvent> = payloads
        .into_iter()
        .map(|p| {
            let mut event = NewEvent::new(p).tags(a.tags.clone());
            if let Some(m) = &a.metadata {
                event = event.metadata(m.clone());
            }
            event
        })
        .collect();
    let mut txn = Transaction::new();
    match aggregate {
        Some(id) => txn.append(id, expected, events),
        None => txn.append_free(events),
    };
    send(conn, format, txn, a.epoch).await
}

#[derive(Args, Debug)]
pub struct CommitArgs {
    /// A JSON file: `{"appends":[{"aggregate":"UUID"|null,"expect":"any"|"new"|N,
    /// "events":[{"payload":"…","tags":["…"],"metadata":"…"}]}]}`; `payload_base64`,
    /// `metadata_base64` and `tags_base64` carry binary values.
    #[arg(short = 'f', long, conflicts_with = "stdin")]
    file: Option<std::path::PathBuf>,
    /// Read the JSON from stdin.
    #[arg(long)]
    stdin: bool,
    /// The epoch the daemon must be at (a fencing token).
    #[arg(long)]
    epoch: Option<u64>,
}

#[derive(Deserialize)]
struct TxnJson {
    appends: Vec<AppendJson>,
}

#[derive(Deserialize)]
struct AppendJson {
    #[serde(default)]
    aggregate: Option<String>,
    #[serde(default)]
    expect: Option<ExpectJson>,
    events: Vec<EventJson>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ExpectJson {
    Word(String),
    Exact(u64),
}

#[derive(Deserialize)]
struct EventJson {
    #[serde(default)]
    payload: Option<String>,
    #[serde(default)]
    payload_base64: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    tags_base64: Vec<String>,
    #[serde(default)]
    metadata: Option<String>,
    #[serde(default)]
    metadata_base64: Option<String>,
}

fn decode_base64(b: &str, what: &str) -> anyhow::Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(b)
        .map_err(|e| Usage(format!("{what} is not base64: {e}")).into())
}

fn bytes(text: Option<String>, b64: Option<String>, what: &str) -> anyhow::Result<Vec<u8>> {
    match (text, b64) {
        (Some(t), None) => Ok(t.into_bytes()),
        (None, Some(b)) => decode_base64(&b, &format!("{what}_base64")),
        (None, None) => Ok(Vec::new()),
        (Some(_), Some(_)) => Err(Usage(format!("give {what} or {what}_base64, not both")).into()),
    }
}

pub async fn commit(conn: &Conn, format: Format, a: CommitArgs) -> anyhow::Result<()> {
    let text = match (a.file, a.stdin) {
        (Some(f), _) => {
            std::fs::read_to_string(&f).with_context(|| format!("read {}", f.display()))?
        }
        (None, true) => {
            let mut s = String::new();
            std::io::stdin()
                .read_to_string(&mut s)
                .context("read stdin")?;
            s
        }
        (None, false) => return Err(Usage("give the transaction with -f or --stdin".into()).into()),
    };
    let parsed: TxnJson =
        serde_json::from_str(&text).map_err(|e| Usage(format!("bad transaction JSON: {e}")))?;
    let mut txn = Transaction::new();
    for a in parsed.appends {
        let aggregate = match a.aggregate {
            Some(s) => parse_aggregate(&s)?,
            None => None,
        };
        let expected = match a.expect {
            None => Expected::Any,
            Some(ExpectJson::Word(w)) => parse_expect(&w)?,
            Some(ExpectJson::Exact(n)) => Expected::Exact(n),
        };
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
        match aggregate {
            Some(id) => txn.append(id, expected, events),
            None => txn.append_free(events),
        };
    }
    send(conn, format, txn, a.epoch).await
}
