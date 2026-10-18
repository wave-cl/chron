use chron::BackupMeta;
use clap::Args;
use serde_json::json;

use crate::client::{self, Conn};
use crate::cmd::aggregate_id;
use crate::output::{Format, print_value};

pub async fn len(conn: &Conn, format: Format, aggregate: &str) -> anyhow::Result<()> {
    let id = aggregate_id(aggregate)?;
    let len = client::connect(conn).await?.aggregate_len(id).await?;
    print_value(format, len.to_string(), json!({ "len": len }));
    Ok(())
}

pub async fn head(conn: &Conn, format: Format) -> anyhow::Result<()> {
    let head = client::connect(conn).await?.head().await?;
    print_value(format, head.to_string(), json!({ "head": head }));
    Ok(())
}

pub async fn info(conn: &Conn, format: Format) -> anyhow::Result<()> {
    let i = client::connect(conn).await?.info().await?;
    let o = &i.options;
    let mut human = format!(
        "path {}\ndb_id {}\nrole {}\nepoch {}\nhead {}\nfloor {}\nsegments {} ({} bytes)\nfsync {}  verify_all_segments {}  segment_max_bytes {}  max_record_bytes {}",
        i.path,
        i.db_id.map_or("?".to_string(), |u| u.to_string()),
        i.role,
        i.epoch,
        i.head,
        i.floor,
        i.segments,
        i.bytes,
        o.fsync == chron::Fsync::Always,
        o.verify_all_segments,
        o.segment_max_bytes,
        o.max_record_bytes
    );
    if let Some(from) = &i.replicating_from {
        human.push_str(&format!(
            "\nreplicating_from {from}  connected {}  leader_head {}  last_error {}",
            i.replica_connected,
            i.leader_head.map_or("?".to_string(), |h| h.to_string()),
            i.last_replication_error.as_deref().unwrap_or("none")
        ));
    }
    if let Some(by) = i.fenced_by {
        human.push_str(&format!("\nfenced_by {by}"));
    }
    print_value(
        format,
        human,
        json!({
            "path": i.path, "db_id": i.db_id.map(|u| u.to_string()), "role": i.role.to_string(), "epoch": i.epoch,
            "head": i.head, "floor": i.floor, "segments": i.segments, "bytes": i.bytes,
            "options": { "fsync": o.fsync == chron::Fsync::Always, "verify_all_segments": o.verify_all_segments,
                         "segment_max_bytes": o.segment_max_bytes, "max_record_bytes": o.max_record_bytes },
            "replicating_from": i.replicating_from, "replica_connected": i.replica_connected,
            "leader_head": i.leader_head, "last_replication_error": i.last_replication_error,
            "fenced_by": i.fenced_by,
        }),
    );
    Ok(())
}

pub async fn flush(conn: &Conn, format: Format) -> anyhow::Result<()> {
    client::connect(conn).await?.flush().await?;
    print_value(format, "flushed".into(), json!({ "flushed": true }));
    Ok(())
}

#[derive(Args, Debug)]
pub struct BackupArgs {
    /// Where the daemon writes the archive, on its own filesystem.
    archive: String,
    /// Incremental: the events from this position on.
    #[arg(long)]
    since: Option<u64>,
}

pub fn meta_json(m: &BackupMeta) -> serde_json::Value {
    json!({
        "kind": if m.kind == chron::BackupKind::Incremental { "incremental" } else { "full" },
        "base_head": m.base_head, "head": m.head, "floor": m.floor,
        "db_id": m.db_id.map(|u| u.to_string()),
        "created_at_unix_nanos": m.created_at_unix_nanos, "files": m.files, "bytes": m.bytes,
    })
}

pub fn meta_human(m: &BackupMeta) -> String {
    let id = m
        .db_id
        .map_or(String::new(), |u| format!(" of database {u}"));
    match m.base_head {
        Some(b) => format!(
            "incremental backup {}..{}{id} ({} bytes)",
            b, m.head, m.bytes
        ),
        None => format!(
            "full backup to head {} floor {}{id} ({} files, {} bytes)",
            m.head, m.floor, m.files, m.bytes
        ),
    }
}

pub async fn backup(conn: &Conn, format: Format, a: BackupArgs) -> anyhow::Result<()> {
    let m = client::connect(conn)
        .await?
        .backup(&a.archive, a.since)
        .await?;
    print_value(format, meta_human(&m), meta_json(&m));
    Ok(())
}

pub async fn apply(conn: &Conn, format: Format, archive: String) -> anyhow::Result<()> {
    let m = client::connect(conn).await?.apply_backup(&archive).await?;
    print_value(format, format!("applied; head {}", m.head), meta_json(&m));
    Ok(())
}

pub async fn retain(conn: &Conn, format: Format, floor: u64) -> anyhow::Result<()> {
    let r = client::connect(conn).await?.retain(floor).await?;
    print_value(
        format,
        format!(
            "floor {} ({} segments, {} bytes freed)",
            r.floor, r.segments_removed, r.bytes_freed
        ),
        json!({ "floor": r.floor, "segments_removed": r.segments_removed, "bytes_freed": r.bytes_freed }),
    );
    Ok(())
}

pub async fn health(conn: &Conn, format: Format, service: &str) -> anyhow::Result<()> {
    let serving = client::connect(conn).await?.health(service).await?;
    print_value(
        format,
        if serving {
            "SERVING".into()
        } else {
            "NOT_SERVING".into()
        },
        json!({ "service": service, "serving": serving }),
    );
    if !serving {
        anyhow::bail!("{service:?} is not serving");
    }
    Ok(())
}

pub async fn promote(conn: &Conn, format: Format) -> anyhow::Result<()> {
    let (epoch, head) = client::connect(conn).await?.promote().await?;
    print_value(
        format,
        format!("promoted; epoch {epoch} head {head}"),
        json!({ "epoch": epoch, "head": head }),
    );
    Ok(())
}

pub async fn fence(conn: &Conn, format: Format, epoch: u64) -> anyhow::Result<()> {
    let own = client::connect(conn).await?.fence(epoch).await?;
    print_value(
        format,
        format!("fenced; epoch {own}"),
        json!({ "epoch": own }),
    );
    Ok(())
}
