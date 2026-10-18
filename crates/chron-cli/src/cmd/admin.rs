use chron_proto::v1 as pb;
use clap::Args;
use serde_json::json;

use crate::client;
use crate::cmd::aggregate_bytes;
use crate::output::{Format, print_value};

pub async fn len(addr: &str, format: Format, aggregate: &str) -> anyhow::Result<()> {
    let aggregate = aggregate_bytes(aggregate)?;
    let mut c = client::connect(addr).await?;
    let len = c
        .aggregate_len(pb::AggregateLenRequest { aggregate })
        .await?
        .into_inner()
        .len;
    print_value(format, len.to_string(), json!({ "len": len }));
    Ok(())
}

pub async fn head(addr: &str, format: Format) -> anyhow::Result<()> {
    let mut c = client::connect(addr).await?;
    let head = c.head(pb::HeadRequest {}).await?.into_inner().head;
    print_value(format, head.to_string(), json!({ "head": head }));
    Ok(())
}

pub async fn info(addr: &str, format: Format) -> anyhow::Result<()> {
    let mut c = client::connect(addr).await?;
    let i = c.info(pb::InfoRequest {}).await?.into_inner();
    let o = i.options.unwrap_or_default();
    let human = format!(
        "path {}\nhead {}\nfloor {}\nsegments {} ({} bytes)\nfsync {}  verify_all_segments {}  segment_max_bytes {}  max_record_bytes {}",
        i.path,
        i.head,
        i.floor,
        i.segments,
        i.bytes,
        o.fsync,
        o.verify_all_segments,
        o.segment_max_bytes,
        o.max_record_bytes
    );
    print_value(
        format,
        human,
        json!({
            "path": i.path, "head": i.head, "floor": i.floor, "segments": i.segments, "bytes": i.bytes,
            "options": { "fsync": o.fsync, "verify_all_segments": o.verify_all_segments,
                         "segment_max_bytes": o.segment_max_bytes, "max_record_bytes": o.max_record_bytes }
        }),
    );
    Ok(())
}

pub async fn flush(addr: &str, format: Format) -> anyhow::Result<()> {
    let mut c = client::connect(addr).await?;
    c.flush(pb::FlushRequest {}).await?;
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

pub fn meta_json(m: &pb::BackupMeta) -> serde_json::Value {
    json!({
        "kind": if m.incremental { "incremental" } else { "full" },
        "base_head": m.base_head, "head": m.head, "floor": m.floor,
        "created_at_unix_nanos": m.created_at_unix_nanos, "files": m.files, "bytes": m.bytes,
    })
}

pub fn meta_human(m: &pb::BackupMeta) -> String {
    match m.base_head {
        Some(b) => format!("incremental backup {}..{} ({} bytes)", b, m.head, m.bytes),
        None => format!(
            "full backup to head {} floor {} ({} files, {} bytes)",
            m.head, m.floor, m.files, m.bytes
        ),
    }
}

pub async fn backup(addr: &str, format: Format, a: BackupArgs) -> anyhow::Result<()> {
    let mut c = client::connect(addr).await?;
    let m = c
        .backup(pb::BackupRequest {
            archive: a.archive,
            since: a.since,
        })
        .await?
        .into_inner();
    print_value(format, meta_human(&m), meta_json(&m));
    Ok(())
}

pub async fn apply(addr: &str, format: Format, archive: String) -> anyhow::Result<()> {
    let mut c = client::connect(addr).await?;
    let m = c
        .apply_backup(pb::ApplyBackupRequest { archive })
        .await?
        .into_inner();
    print_value(format, format!("applied; head {}", m.head), meta_json(&m));
    Ok(())
}

pub async fn retain(addr: &str, format: Format, floor: u64) -> anyhow::Result<()> {
    let mut c = client::connect(addr).await?;
    let r = c.retain(pb::RetainRequest { floor }).await?.into_inner();
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
