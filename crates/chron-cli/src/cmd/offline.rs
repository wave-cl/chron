use std::path::Path;

use chron::{Db, Options};
use clap::Args;
use serde_json::json;

use crate::cmd::admin::{meta_human, meta_json};
use crate::output::{Format, Usage, print_value};

/// Options an offline command opens the database with.
#[derive(Args, Debug, Clone)]
pub struct OfflineOpts {
    /// The record limit the database was written with, when not the
    /// default.
    #[arg(long)]
    max_record_bytes: Option<usize>,
}

impl OfflineOpts {
    fn options(&self) -> Options {
        let mut o = Options::new();
        if let Some(n) = self.max_record_bytes {
            o = o.max_record_bytes(n);
        }
        o
    }
}

/// A directory holding a database: a `segments` directory or an index.
/// Opening anything else would create a new, empty database.
fn ensure_database(path: &Path) -> anyhow::Result<()> {
    if !path.is_dir() {
        return Err(Usage(format!("{} is not a directory", path.display())).into());
    }
    let has_data = path.join("segments").is_dir() || path.join("index.redb").is_file();
    if !has_data {
        return Err(Usage(format!("{} holds no chron database", path.display())).into());
    }
    Ok(())
}

pub fn rebuild_index(format: Format, path: &Path, opts: &OfflineOpts) -> anyhow::Result<()> {
    ensure_database(path)?;
    let r = Db::rebuild_index(path, &opts.options())?;
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

pub fn verify(format: Format, path: &Path, opts: &OfflineOpts) -> anyhow::Result<()> {
    ensure_database(path)?;
    let db = Db::open(path, opts.options().verify_all_segments(true))?;
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

pub fn restore(
    format: Format,
    archive: &Path,
    dir: &Path,
    opts: &OfflineOpts,
) -> anyhow::Result<()> {
    let m = chron::restore_backup(archive, dir, &opts.options())?;
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
    print_value(format, meta_human(&m), meta_json(&m));
    Ok(())
}
