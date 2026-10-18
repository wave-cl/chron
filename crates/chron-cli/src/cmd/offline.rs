use std::path::Path;

use chron::{BackupKind, Db, Options};
use serde_json::json;

use crate::output::{Format, print_value};

pub fn rebuild_index(format: Format, path: &Path) -> anyhow::Result<()> {
    let r = Db::rebuild_index(path, &Options::new())?;
    print_value(
        format,
        format!(
            "rebuilt; head {} floor {} ({} segments, {} dropped)",
            r.head, r.floor, r.segments, r.dropped
        ),
        json!({ "head": r.head, "floor": r.floor, "segments": r.segments, "dropped": r.dropped }),
    );
    Ok(())
}

pub fn verify(format: Format, path: &Path) -> anyhow::Result<()> {
    let db = Db::open(path, Options::new().verify_all_segments(true))?;
    let info = db.info()?;
    print_value(
        format,
        format!(
            "ok; head {} floor {} ({} segments)",
            info.head, info.floor, info.segments
        ),
        json!({ "ok": true, "head": info.head, "floor": info.floor, "segments": info.segments }),
    );
    Ok(())
}

fn meta_json(m: &chron::BackupMeta) -> serde_json::Value {
    json!({
        "kind": if m.kind == BackupKind::Incremental { "incremental" } else { "full" },
        "base_head": m.base_head, "head": m.head, "floor": m.floor,
        "created_at_unix_nanos": m.created_at_unix_nanos, "files": m.files, "bytes": m.bytes,
    })
}

pub fn restore(format: Format, archive: &Path, dir: &Path) -> anyhow::Result<()> {
    let m = chron::restore_backup(archive, dir, &Options::new())?;
    print_value(
        format,
        format!(
            "restored {} to head {} floor {}",
            dir.display(),
            m.head,
            m.floor
        ),
        meta_json(&m),
    );
    Ok(())
}

pub fn inspect(format: Format, archive: &Path) -> anyhow::Result<()> {
    let m = chron::inspect_backup(archive)?;
    let human = match m.base_head {
        Some(b) => format!("incremental backup {}..{} ({} bytes)", b, m.head, m.bytes),
        None => format!(
            "full backup to head {} floor {} ({} files, {} bytes)",
            m.head, m.floor, m.files, m.bytes
        ),
    };
    print_value(format, human, meta_json(&m));
    Ok(())
}
