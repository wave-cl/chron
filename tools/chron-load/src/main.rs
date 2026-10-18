use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

/// Load against a chrond leader (and a replica), checking invariants.
#[derive(Parser, Debug)]
#[command(name = "chron-load", version)]
struct Cli {
    /// The leader's address, or a comma list of a failover group's nodes.
    #[arg(long, env = "CHRON_ADDR", default_value = "http://127.0.0.1:4242")]
    addr: String,
    /// A replica to compare with the leader at the end.
    #[arg(long)]
    replica: Option<String>,
    /// Bearer token (also used as the admin token).
    #[arg(long, env = "CHRON_TOKEN", hide_env_values = true)]
    token: Option<String>,
    /// The database to load; the daemon's default when unset.
    #[arg(long, env = "CHRON_DB")]
    db: Option<String>,
    /// How long to run, in seconds.
    #[arg(long, default_value_t = 60)]
    duration: u64,
    #[arg(long, default_value_t = 4)]
    writers: usize,
    #[arg(long, default_value_t = 2)]
    readers: usize,
    #[arg(long, default_value_t = 1)]
    subscribers: usize,
    /// How many aggregates the writers share.
    #[arg(long, default_value_t = 16)]
    aggregates: u64,
    #[arg(long, default_value_t = 2)]
    events_per_txn: usize,
    #[arg(long, default_value_t = 200)]
    payload_bytes: usize,
    /// The daemon's --backup-dir on this host; enables periodic backups
    /// that are restored and checked offline.
    #[arg(long)]
    backup_dir: Option<PathBuf>,
    /// Seconds between backups (needs --backup-dir).
    #[arg(long)]
    backup_every: Option<u64>,
    /// Seconds between retentions to half the head.
    #[arg(long)]
    retain_every: Option<u64>,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Kill the leader (SIGKILL to the pid --pid gives for its address)
    /// every this many seconds; something else must restart it.
    #[arg(long, requires = "pid")]
    kill_leader_every: Option<u64>,
    /// ADDR=PID, or ADDR=FILE holding the pid (read at each kill, so a
    /// restarted daemon's new pid is found), for each node of the group.
    #[arg(long = "pid", value_name = "ADDR=PID|FILE")]
    pid: Vec<String>,
    /// Print the report as JSON.
    #[arg(long)]
    json: bool,
}

/// A killer that sends SIGKILL to the pid given for an address (a
/// number, or a file holding one, read at each kill).
fn killer(pairs: &[String]) -> anyhow::Result<chron_load::Killer> {
    let mut pids: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for p in pairs {
        let (addr, pid) = p
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--pid takes ADDR=PID or ADDR=FILE, not {p:?}"))?;
        pids.insert(addr.trim().to_string(), pid.trim().to_string());
    }
    let pids = std::sync::Arc::new(pids);
    Ok(std::sync::Arc::new(move |addr: String| {
        let pids = pids.clone();
        Box::pin(async move {
            let Some(spec) = pids.get(&addr) else {
                eprintln!("no pid known for the leader {addr}; not killed");
                return;
            };
            let pid: Option<u32> = match spec.parse() {
                Ok(n) => Some(n),
                Err(_) => std::fs::read_to_string(spec)
                    .ok()
                    .and_then(|t| t.trim().parse().ok()),
            };
            match pid {
                Some(pid) => {
                    eprintln!("killing the leader {addr} (pid {pid})");
                    let _ = std::process::Command::new("kill")
                        .args(["-9", &pid.to_string()])
                        .status();
                }
                None => eprintln!("no pid in {spec} for the leader {addr}; not killed"),
            }
        })
    }))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let report = chron_load::run(chron_load::Config {
        addr: cli.addr,
        replica: cli.replica,
        token: cli.token,
        database: cli.db,
        duration: Duration::from_secs(cli.duration),
        writers: cli.writers,
        readers: cli.readers,
        subscribers: cli.subscribers,
        aggregates: cli.aggregates.max(1),
        events_per_txn: cli.events_per_txn.max(1),
        payload_bytes: cli.payload_bytes,
        backup_dir: cli.backup_dir,
        backup_every: cli.backup_every.map(Duration::from_secs),
        retain_every: cli.retain_every.map(Duration::from_secs),
        seed: cli.seed,
        kill_every: cli.kill_leader_every.map(Duration::from_secs),
        killer: if cli.pid.is_empty() {
            None
        } else {
            Some(killer(&cli.pid)?)
        },
    })
    .await?;
    if cli.json {
        println!("{}", report.to_json());
    } else {
        println!(
            "transactions {}  events {}  conflicts {}  errors {}\nreads {} ({} events)  subscribed {}  backups {}  retentions {}\ncommit p50 {:.2} ms  p99 {:.2} ms\nfinal head {}  replica head {:?}\nkills {}  lost acks {}  failover errors {}",
            report.transactions,
            report.events,
            report.conflicts,
            report.errors,
            report.reads,
            report.events_read,
            report.subscribed_events,
            report.backups,
            report.retentions,
            report.commit_p50_ms,
            report.commit_p99_ms,
            report.final_head,
            report.replica_head,
            report.kills,
            report.lost_acks,
            report.failover_errors
        );
        for v in &report.violations {
            println!("VIOLATION: {v}");
        }
        println!("{}", if report.ok() { "ok" } else { "FAILED" });
    }
    if !report.ok() {
        std::process::exit(1);
    }
    Ok(())
}
