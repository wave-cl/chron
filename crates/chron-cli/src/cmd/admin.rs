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

pub async fn head(conn: &Conn, format: Format, a: HeadArgs) -> anyhow::Result<()> {
    let h = client::connect(conn)
        .await?
        .head_with(crate::cmd::read::read_options(a.at_least, a.wait))
        .await?;
    // One number for scripts; the JSON form carries both bounds.
    print_value(
        format,
        h.head.to_string(),
        json!({ "head": h.head, "confirmed": h.confirmed, "local": h.local }),
    );
    Ok(())
}

#[derive(clap::Args, Debug)]
pub struct HeadArgs {
    /// Wait until the bound reaches this position first (a commit's
    /// `next`).
    #[arg(long)]
    pub at_least: Option<u64>,
    /// Seconds --at-least may wait (default: the daemon's sync timeout).
    #[arg(long, requires = "at_least")]
    pub wait: Option<f64>,
}

pub async fn info(conn: &Conn, format: Format) -> anyhow::Result<()> {
    let i = client::connect(conn).await?.info().await?;
    let o = &i.options;
    let mut human = format!(
        "database {}\npath {}\ndb_id {}\nrole {}\nepoch {}\nhead {}\nfloor {}\nsegments {} ({} bytes)\nfsync {}  verify_all_segments {}  segment_max_bytes {}  max_record_bytes {}",
        i.database,
        i.path,
        i.db_id,
        i.role,
        i.epoch,
        i.head,
        i.floor,
        i.segments,
        i.bytes,
        if i.fsync { "always" } else { "never" },
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
    if !i.peers.is_empty() || i.role == chron_client::Role::Removed {
        human.push_str(&format!(
            "\nleader {}  lease_remaining {}  voted_epoch {}\npeers {}\nmembers_version {} (epoch {})  committed {}  member {}\nsync_commit {}  majority_applied {}",
            i.leader_addr.as_deref().unwrap_or("none"),
            i.lease_remaining
                .map_or("-".to_string(), |d| format!("{} ms", d.as_millis())),
            i.voted_epoch.map_or("-".to_string(), |e| e.to_string()),
            i.peers.join(","),
            i.members_version,
            i.members_epoch,
            i.members_committed,
            i.member,
            if i.sync_commit { "majority" } else { "off" },
            i.majority_applied
                .map_or("-".to_string(), |m| m.to_string())
        ));
        for f in &i.followers {
            human.push_str(&format!(
                "\nfollower {}  applied {}  durable {}  counted {}",
                f.peer, f.applied, f.durable, f.counted
            ));
        }
    }
    print_value(
        format,
        human,
        json!({
            "database": i.database, "path": i.path, "db_id": i.db_id.to_string(), "role": i.role.to_string(), "epoch": i.epoch,
            "head": i.head, "floor": i.floor, "segments": i.segments, "bytes": i.bytes,
            "fsync": if i.fsync { "always" } else { "never" },
            "options": { "verify_all_segments": o.verify_all_segments,
                         "segment_max_bytes": o.segment_max_bytes, "max_record_bytes": o.max_record_bytes },
            "replicating_from": i.replicating_from, "replica_connected": i.replica_connected,
            "leader_head": i.leader_head, "last_replication_error": i.last_replication_error,
            "fenced_by": i.fenced_by,
            "leader_addr": i.leader_addr, "lease_remaining_ms": i.lease_remaining.map(|d| d.as_millis() as u64),
            "peers": i.peers, "voted_epoch": i.voted_epoch,
            "sync_commit": i.sync_commit, "majority_applied": i.majority_applied,
            "followers": i.followers.iter().map(|f| json!({
                "peer": f.peer, "applied": f.applied, "durable": f.durable, "counted": f.counted,
            })).collect::<Vec<_>>(),
            "members_version": i.members_version, "members_epoch": i.members_epoch,
            "members_committed": i.members_committed, "member": i.member,
        }),
    );
    Ok(())
}

/// Asks every address of `--addr` who leads, one line (or JSON object)
/// per node; a node that cannot be reached is reported, not fatal. Exit
/// 2 when none answered.
pub async fn discover(conn: &Conn, format: Format) -> anyhow::Result<()> {
    let addrs = conn.addrs();
    let mut reached = 0;
    for addr in &addrs {
        let answer = match client::connect_one(conn, addr).await {
            Ok(c) => c.discover().await.map_err(anyhow::Error::from),
            Err(e) => Err(e),
        };
        match answer {
            Ok(d) => {
                reached += 1;
                print_value(
                    format,
                    format!(
                        "{addr}: leader {}  role {}  epoch {}  lease_remaining {}  peers {}  members_version {}",
                        d.leader_addr.as_deref().unwrap_or("none"),
                        d.role,
                        d.epoch,
                        d.lease_remaining
                            .map_or("-".to_string(), |l| format!("{} ms", l.as_millis())),
                        if d.peers.is_empty() {
                            "none".to_string()
                        } else {
                            d.peers.join(",")
                        },
                        d.members_version
                    ),
                    json!({
                        "node": addr, "leader_addr": d.leader_addr, "role": d.role.to_string(),
                        "epoch": d.epoch, "lease_remaining_ms": d.lease_remaining.map(|l| l.as_millis() as u64),
                        "peers": d.peers, "members_version": d.members_version, "members_epoch": d.members_epoch,
                    }),
                );
            }
            Err(e) => print_value(
                format,
                format!("{addr}: unreachable: {e}"),
                json!({ "node": addr, "error": e.to_string() }),
            ),
        }
    }
    if reached == 0 {
        anyhow::bail!(crate::client::Unreachable {
            addr: conn.addr.clone(),
            source: chron_client::ClientError::NoLeader("no node answered".into()),
        });
    }
    Ok(())
}

pub async fn flush(conn: &Conn, format: Format) -> anyhow::Result<()> {
    client::connect(conn).await?.flush().await?;
    print_value(format, "flushed".into(), json!({ "flushed": true }));
    Ok(())
}

#[derive(Args, Debug)]
pub struct BackupArgs {
    /// File name under the daemon's backup directory (--backup-dir).
    #[arg(required_unless_present = "fetch", conflicts_with = "fetch")]
    archive: Option<String>,
    /// Incremental: the events from this position on.
    #[arg(long, conflicts_with = "fetch")]
    since: Option<u64>,
    /// Download a full backup to this local file instead (the daemon
    /// needs no --backup-dir; any node of a group serves it).
    #[arg(long, value_name = "FILE")]
    fetch: Option<std::path::PathBuf>,
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
    if let Some(path) = &a.fetch {
        let m = client::connect(conn).await?.fetch_backup_to(path).await?;
        let mut j = meta_json(&m);
        j["path"] = json!(path.display().to_string());
        print_value(
            format,
            format!("{} -> {}", meta_human(&m), path.display()),
            j,
        );
        return Ok(());
    }
    let archive = a.archive.expect("clap requires one of archive and --fetch");
    let m = client::connect(conn)
        .await?
        .backup(&archive, a.since)
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
    let p = client::connect(conn).await?.promote().await?;
    print_value(
        format,
        format!("promoted; epoch {} head {}", p.epoch, p.head),
        json!({ "epoch": p.epoch, "head": p.head }),
    );
    Ok(())
}

fn members_human(
    m: &chron_client::Members,
    committed: Option<bool>,
    member: Option<bool>,
) -> String {
    let mut s = format!("version {} (epoch {})", m.version, m.epoch);
    if let Some(c) = committed {
        s.push_str(&format!("  committed {}", if c { "yes" } else { "no" }));
    }
    if let Some(mem) = member {
        s.push_str(&format!("  member {}", if mem { "yes" } else { "no" }));
    }
    for a in &m.addrs {
        s.push_str(&format!("\nmember {a}"));
    }
    s
}

fn members_value(m: &chron_client::Members) -> serde_json::Value {
    json!({ "epoch": m.epoch, "version": m.version, "addrs": m.addrs })
}

pub async fn members(conn: &Conn, format: Format) -> anyhow::Result<()> {
    let m = client::connect_one(conn, &conn.addrs()[0])
        .await?
        .members()
        .await?;
    print_value(
        format,
        members_human(&m.members, Some(m.committed), Some(m.member)),
        json!({ "members": members_value(&m.members), "committed": m.committed, "member": m.member, "leader_addr": m.leader_addr }),
    );
    Ok(())
}

pub async fn members_add(
    conn: &Conn,
    format: Format,
    addr: &str,
    force: bool,
) -> anyhow::Result<()> {
    let (m, committed) = client::connect(conn).await?.add_member(addr, force).await?;
    print_value(
        format,
        format!(
            "{}\n{}",
            if committed {
                "committed"
            } else {
                "pending: `chron members` says when it is committed"
            },
            members_human(&m, Some(committed), None)
        ),
        json!({ "members": members_value(&m), "committed": committed }),
    );
    Ok(())
}

pub async fn members_remove(conn: &Conn, format: Format, addr: &str) -> anyhow::Result<()> {
    let (m, committed) = client::connect(conn).await?.remove_member(addr).await?;
    print_value(
        format,
        format!(
            "{}\n{}",
            if committed {
                "committed"
            } else {
                "pending: `chron members` says when it is committed"
            },
            members_human(&m, Some(committed), None)
        ),
        json!({ "members": members_value(&m), "committed": committed }),
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

pub async fn db_create(conn: &Conn, format: Format, name: &str) -> anyhow::Result<()> {
    let d = client::connect(conn).await?.create_database(name).await?;
    print_value(
        format,
        format!("created {}", d.name),
        json!({ "name": d.name, "head": d.head, "floor": d.floor, "epoch": d.epoch, "db_id": d.db_id.to_string() }),
    );
    Ok(())
}

pub async fn db_list(conn: &Conn, format: Format) -> anyhow::Result<()> {
    let all = client::connect(conn).await?.list_databases().await?;
    let human = all
        .iter()
        .map(|d| {
            format!(
                "{}  head {}  floor {}  epoch {}  db_id {}",
                d.name, d.head, d.floor, d.epoch, d.db_id
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let value: Vec<serde_json::Value> = all
        .iter()
        .map(|d| json!({ "name": d.name, "head": d.head, "floor": d.floor, "epoch": d.epoch, "db_id": d.db_id.to_string() }))
        .collect();
    print_value(format, human, json!({ "databases": value }));
    Ok(())
}

pub async fn db_drop(conn: &Conn, format: Format, name: &str) -> anyhow::Result<()> {
    client::connect(conn).await?.drop_database(name).await?;
    print_value(
        format,
        format!("dropped {name}"),
        json!({ "dropped": name }),
    );
    Ok(())
}

#[derive(Args, Debug)]
pub struct StatusArgs {
    /// Degraded when a replica is this many events or more behind its
    /// leader. Ignored on a leader, and when the lag is not yet known.
    #[arg(long)]
    max_lag: Option<u64>,
}

/// One screen of health; exit 1 when degraded.
pub async fn status(conn: &Conn, format: Format, a: StatusArgs) -> anyhow::Result<()> {
    let i = client::connect(conn).await?.info().await?;
    let lag = i.leader_head.map(|l| l.saturating_sub(i.head));
    let mut reasons: Vec<String> = Vec::new();
    if i.role == chron_client::Role::Replica && !i.replica_connected {
        reasons.push(format!(
            "replica disconnected{}",
            i.last_replication_error
                .as_deref()
                .map(|e| format!(": {e}"))
                .unwrap_or_default()
        ));
    }
    if i.role == chron_client::Role::Fenced {
        reasons.push(format!("fenced by epoch {}", i.fenced_by.unwrap_or(0)));
    }
    if i.role == chron_client::Role::Candidate {
        reasons.push(format!(
            "candidate: no leader{}",
            i.leader_addr
                .as_deref()
                .map(|a| format!(" (last heard {a})"))
                .unwrap_or_default()
        ));
    }
    if i.role == chron_client::Role::Removed {
        reasons.push(format!(
            "removed from the group (membership version {}); add it with `chron members add`, or stop it",
            i.members_version
        ));
    }
    if i.role == chron_client::Role::Leader && i.lease_remaining == Some(std::time::Duration::ZERO)
    {
        reasons.push("the majority lease has expired; writes are refused".to_string());
    }
    if let (Some(max), Some(lag)) = (a.max_lag, lag)
        && i.role == chron_client::Role::Replica
        && lag > max
    {
        reasons.push(format!("lag {lag} above {max}"));
    }
    let healthy = reasons.is_empty();
    let mut human = format!(
        "database {}\nrole {}{}\ndb_id {}  epoch {}\nhead {}  floor {}  lag {}",
        i.database,
        i.role,
        i.replicating_from
            .as_deref()
            .map(|f| format!(" (of {f})"))
            .unwrap_or_default(),
        i.db_id,
        i.epoch,
        i.head,
        i.floor,
        lag.map_or("-".to_string(), |l| l.to_string())
    );
    if i.role == chron_client::Role::Replica {
        human.push_str(&format!(
            "\nreplica {}  last_error {}",
            if i.replica_connected {
                "connected"
            } else {
                "disconnected"
            },
            i.last_replication_error.as_deref().unwrap_or("none")
        ));
    }
    if let Some(by) = i.fenced_by {
        human.push_str(&format!("\nfenced_by {by}"));
    }
    if !i.peers.is_empty() || i.role == chron_client::Role::Removed {
        human.push_str(&format!(
            "\nleader {}  lease_remaining {}  members {} (version {})",
            i.leader_addr.as_deref().unwrap_or("none"),
            i.lease_remaining
                .map_or("-".to_string(), |d| format!("{} ms", d.as_millis())),
            i.peers.len() + usize::from(i.member),
            i.members_version
        ));
    }
    human.push_str(&format!(
        "\nstatus {}",
        if healthy {
            "healthy".to_string()
        } else {
            format!("degraded: {}", reasons.join("; "))
        }
    ));
    print_value(
        format,
        human,
        json!({
            "database": i.database, "role": i.role.to_string(), "replicating_from": i.replicating_from,
            "db_id": i.db_id.to_string(), "epoch": i.epoch, "head": i.head, "floor": i.floor,
            "leader_head": i.leader_head, "lag": lag, "replica_connected": i.replica_connected,
            "last_replication_error": i.last_replication_error, "fenced_by": i.fenced_by,
            "leader_addr": i.leader_addr, "lease_remaining_ms": i.lease_remaining.map(|d| d.as_millis() as u64),
            "peers": i.peers, "voted_epoch": i.voted_epoch,
            "members_version": i.members_version, "member": i.member,
            "healthy": healthy, "reasons": reasons,
        }),
    );
    if !healthy {
        anyhow::bail!("degraded: {}", reasons.join("; "));
    }
    Ok(())
}
