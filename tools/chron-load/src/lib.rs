//! A load generator for chron daemons that checks invariants while it
//! runs: writers, readers and subscribers against a leader, periodic
//! backups restored and checked offline, periodic retention, and a
//! replica compared at the end.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chron_client::{Client, ClientConfig, ClientError, Event, Expected, NewEvent, Uuid};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

/// What one run does.
#[derive(Debug, Clone)]
pub struct Config {
    pub addr: String,
    pub replica: Option<String>,
    pub token: Option<String>,
    pub database: Option<String>,
    pub duration: Duration,
    pub writers: usize,
    pub readers: usize,
    pub subscribers: usize,
    pub aggregates: u64,
    pub events_per_txn: usize,
    pub payload_bytes: usize,
    /// The daemon's backup directory on this host, for restoring what
    /// the daemon wrote; `None` skips backups.
    pub backup_dir: Option<std::path::PathBuf>,
    pub backup_every: Option<Duration>,
    pub retain_every: Option<Duration>,
    pub seed: u64,
}

/// Counts and timings of a run.
#[derive(Debug, Default, Clone)]
pub struct Report {
    pub transactions: u64,
    pub events: u64,
    pub conflicts: u64,
    pub errors: u64,
    pub reads: u64,
    pub events_read: u64,
    pub subscribed_events: u64,
    pub backups: u64,
    pub retentions: u64,
    pub commit_p50_ms: f64,
    pub commit_p99_ms: f64,
    pub final_head: u64,
    pub replica_head: Option<u64>,
    /// Every invariant that failed, with where and what.
    pub violations: Vec<String>,
}

impl Report {
    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "transactions": self.transactions, "events": self.events, "conflicts": self.conflicts,
            "errors": self.errors, "reads": self.reads, "events_read": self.events_read,
            "subscribed_events": self.subscribed_events, "backups": self.backups,
            "retentions": self.retentions, "commit_p50_ms": self.commit_p50_ms,
            "commit_p99_ms": self.commit_p99_ms, "final_head": self.final_head,
            "replica_head": self.replica_head, "violations": self.violations, "ok": self.ok(),
        })
    }
}

/// A small deterministic generator (xorshift64*), one per worker.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed.max(1))
    }

    pub fn draw(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.draw() % n }
    }
}

struct Shared {
    stop: AtomicBool,
    transactions: AtomicU64,
    events: AtomicU64,
    conflicts: AtomicU64,
    errors: AtomicU64,
    reads: AtomicU64,
    events_read: AtomicU64,
    subscribed: AtomicU64,
    backups: AtomicU64,
    retentions: AtomicU64,
    floor: AtomicU64,
    latencies: Mutex<Vec<u64>>,
    violations: Mutex<Vec<String>>,
}

impl Shared {
    fn violation(&self, what: String) {
        if let Ok(mut v) = self.violations.try_lock()
            && v.len() < 100
        {
            v.push(what);
        }
    }

    fn error(&self, what: &str, e: &ClientError) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        self.violation(format!("{what}: unexpected error: {e}"));
    }
}

pub fn aggregate(n: u64) -> Uuid {
    Uuid::from_u128(0x1000 + u128::from(n))
}

/// `w<writer>:s<seq>:a<agg>:i<index>` and filler to `payload_bytes`.
fn payload(writer: usize, seq: u64, agg: u64, index: u64, bytes: usize) -> Vec<u8> {
    let mut p = format!("w{writer}:s{seq}:a{agg}:i{index}:").into_bytes();
    while p.len() < bytes {
        p.push(b'x');
    }
    p
}

/// The aggregate number and index the payload claims.
fn claimed(payload: &[u8]) -> Option<(u64, u64)> {
    let text = std::str::from_utf8(payload).ok()?;
    let mut agg = None;
    let mut index = None;
    for part in text.split(':') {
        if let Some(a) = part.strip_prefix('a') {
            agg = a.parse().ok();
        } else if let Some(i) = part.strip_prefix('i') {
            index = i.parse().ok();
        }
    }
    Some((agg?, index?))
}

/// Checks one event against what its payload claims and its position.
fn check_event(shared: &Shared, where_: &str, ev: &Event, expect_position: Option<u64>) {
    if let Some(p) = expect_position
        && ev.position != p
    {
        shared.violation(format!(
            "{where_}: position {} where {p} was expected",
            ev.position
        ));
    }
    let Some((agg, index)) = claimed(&ev.payload) else {
        shared.violation(format!("{where_}: payload at {} is not ours", ev.position));
        return;
    };
    if ev.aggregate != Some(aggregate(agg)) || ev.index != index {
        shared.violation(format!(
            "{where_}: event at {} claims aggregate {agg} index {index}, the record says {:?} #{}",
            ev.position, ev.aggregate, ev.index
        ));
    }
}

async fn connect(cfg: &Config, addr: &str) -> Result<Client, ClientError> {
    Client::connect(
        addr,
        ClientConfig {
            token: cfg.token.clone(),
            admin_token: cfg.token.clone(),
            database: cfg.database.clone(),
            ..ClientConfig::default()
        },
    )
    .await
}

async fn writer(cfg: Config, client: Client, shared: Arc<Shared>, w: usize) {
    let mut rng = Rng::new(cfg.seed ^ ((w as u64 + 1) * 0x9E37));
    let mut lens: HashMap<u64, u64> = HashMap::new();
    let mut seq = 0u64;
    while !shared.stop.load(Ordering::Relaxed) {
        let agg = rng.below(cfg.aggregates);
        let id = aggregate(agg);
        let len = match lens.get(&agg) {
            Some(l) => *l,
            None => match client.aggregate_len(id).await {
                Ok(l) => l,
                Err(e) => {
                    shared.error("writer len", &e);
                    continue;
                }
            },
        };
        let events: Vec<NewEvent> = (0..cfg.events_per_txn)
            .map(|j| {
                NewEvent::new(payload(w, seq, agg, len + j as u64, cfg.payload_bytes)).tag("load")
            })
            .collect();
        let n = events.len() as u64;
        let start = Instant::now();
        match client.append(id, Expected::Exact(len), events).await {
            Ok(c) => {
                shared
                    .latencies
                    .lock()
                    .await
                    .push(start.elapsed().as_nanos() as u64);
                shared.transactions.fetch_add(1, Ordering::Relaxed);
                shared.events.fetch_add(n, Ordering::Relaxed);
                if c.appends[0].first_index != len || c.appends[0].new_len != len + n {
                    shared.violation(format!(
                        "writer {w}: expected first index {len} and length {}, got {} and {}",
                        len + n,
                        c.appends[0].first_index,
                        c.appends[0].new_len
                    ));
                }
                lens.insert(agg, len + n);
                seq += 1;
            }
            Err(ClientError::Conflict { actual, .. }) => {
                shared.conflicts.fetch_add(1, Ordering::Relaxed);
                if actual < len {
                    shared.violation(format!(
                        "writer {w}: aggregate {agg} shrank from {len} to {actual}"
                    ));
                }
                lens.insert(agg, actual);
            }
            Err(e) => shared.error("writer commit", &e),
        }
    }
}

async fn reader(client: Client, shared: Arc<Shared>, r: usize, seed: u64) {
    let mut rng = Rng::new(seed ^ ((r as u64 + 100) * 0x7F4A));
    while !shared.stop.load(Ordering::Relaxed) {
        let head = match client.head().await {
            Ok(h) => h,
            Err(e) => {
                shared.error("reader head", &e);
                continue;
            }
        };
        let floor = shared.floor.load(Ordering::Relaxed);
        if head <= floor {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        let from = floor + rng.below(head - floor);
        let limit = 1 + rng.below(256) as usize;
        let backward = rng.below(4) == 0;
        let result = if backward {
            client.read_all_backward(from, limit).await
        } else {
            client.read_all(from, limit).await
        };
        match result {
            Ok(events) => {
                shared.reads.fetch_add(1, Ordering::Relaxed);
                shared
                    .events_read
                    .fetch_add(events.len() as u64, Ordering::Relaxed);
                if !backward && events.is_empty() {
                    shared.violation(format!("reader {r}: nothing at {from} below head {head}"));
                }
                for (i, ev) in events.iter().enumerate() {
                    let expect = if backward {
                        from.checked_sub(i as u64)
                    } else {
                        Some(from + i as u64)
                    };
                    check_event(&shared, &format!("reader {r}"), ev, expect);
                }
            }
            Err(ClientError::BelowFloor { floor: f, .. }) => {
                // Retention moved the floor since we looked; remember it.
                shared.floor.fetch_max(f, Ordering::Relaxed);
            }
            Err(e) => shared.error("reader", &e),
        }
    }
}

async fn subscriber(client: Client, shared: Arc<Shared>, s: usize) {
    let from = shared.floor.load(Ordering::Relaxed);
    let mut stream = match client.subscribe(from).await {
        Ok(s) => s,
        Err(e) => {
            shared.error("subscribe", &e);
            return;
        }
    };
    let mut next = from;
    while !shared.stop.load(Ordering::Relaxed) {
        let item = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
        match item {
            Ok(Some(Ok(ev))) => {
                check_event(&shared, &format!("subscriber {s}"), &ev, Some(next));
                next = ev.position + 1;
                shared.subscribed.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Some(Err(e))) => {
                shared.error("subscriber", &e);
                return;
            }
            Ok(None) => {
                shared.violation(format!("subscriber {s}: the stream ended"));
                return;
            }
            Err(_) => {}
        }
    }
}

/// Backups restored and checked offline, and retention, on their timers.
async fn admin(cfg: Config, client: Client, shared: Arc<Shared>) {
    let mut last_backup = Instant::now();
    let mut last_retain = Instant::now();
    let mut n = 0u64;
    while !shared.stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let (Some(every), Some(dir)) = (cfg.backup_every, &cfg.backup_dir)
            && last_backup.elapsed() >= every
        {
            last_backup = Instant::now();
            n += 1;
            let name = format!("load-{n}.chronbak");
            match client.backup(&name, None).await {
                Ok(meta) => {
                    shared.backups.fetch_add(1, Ordering::Relaxed);
                    let archive = dir
                        .join(cfg.database.as_deref().unwrap_or("default"))
                        .join(&name);
                    let lens: Vec<(u64, u64)> = {
                        let mut v = Vec::new();
                        for a in 0..cfg.aggregates {
                            match client.aggregate_len(aggregate(a)).await {
                                Ok(l) => v.push((a, l)),
                                Err(e) => shared.error("admin len", &e),
                            }
                        }
                        v
                    };
                    let shared2 = shared.clone();
                    let check = tokio::task::spawn_blocking(move || {
                        check_backup(&shared2, &archive, meta.head, &lens)
                    })
                    .await;
                    if let Err(e) = check {
                        shared.violation(format!("backup check task: {e}"));
                    }
                    let _ = std::fs::remove_file(
                        dir.join(cfg.database.as_deref().unwrap_or("default"))
                            .join(&name),
                    );
                }
                Err(e) => shared.error("backup", &e),
            }
        }
        if let Some(every) = cfg.retain_every
            && last_retain.elapsed() >= every
        {
            last_retain = Instant::now();
            match client.head().await {
                Ok(head) => match client.retain(head / 2).await {
                    Ok(r) => {
                        shared.retentions.fetch_add(1, Ordering::Relaxed);
                        shared.floor.fetch_max(r.floor, Ordering::Relaxed);
                        if r.floor > head / 2 {
                            shared.violation(format!(
                                "retain to {} moved the floor to {}",
                                head / 2,
                                r.floor
                            ));
                        }
                    }
                    Err(e) => shared.error("retain", &e),
                },
                Err(e) => shared.error("admin head", &e),
            }
        }
    }
}

/// Restores a full backup into a temp dir and checks it is a consistent
/// prefix: head as the archive says, dense positions, payloads agreeing
/// with their records, every aggregate's length at most the live one.
fn check_backup(shared: &Shared, archive: &std::path::Path, head: u64, lens: &[(u64, u64)]) {
    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(e) => {
            shared.violation(format!("backup tempdir: {e}"));
            return;
        }
    };
    let target = dir.path().join("restored");
    if let Err(e) = chron::restore_backup(archive, &target, &chron::Options::new()) {
        shared.violation(format!("restore {}: {e}", archive.display()));
        return;
    }
    let db = match chron::Db::open(
        &target,
        chron::Options::new()
            .fsync(chron::Fsync::Never)
            .verify_all_segments(true),
    ) {
        Ok(db) => db,
        Err(e) => {
            shared.violation(format!("open restored backup: {e}"));
            return;
        }
    };
    if db.head() != head {
        shared.violation(format!(
            "restored head {} differs from the archive's {head}",
            db.head()
        ));
    }
    let floor = db.floor();
    let mut expect = floor;
    for ev in db.iter_all(floor) {
        match ev {
            Ok(ev) => {
                check_event(shared, "restored backup", &ev, Some(expect));
                expect += 1;
            }
            Err(e) => {
                shared.violation(format!("restored backup read: {e}"));
                break;
            }
        }
    }
    if expect != head {
        shared.violation(format!(
            "restored backup holds {} events from {floor}, head {head}",
            expect - floor
        ));
    }
    for (a, live) in lens {
        match db.aggregate_len(aggregate(*a)) {
            Ok(l) if l > *live => shared.violation(format!(
                "restored aggregate {a} has {l} events, the live one {live}"
            )),
            Ok(_) => {}
            Err(e) => shared.violation(format!("restored len {a}: {e}")),
        }
    }
}

/// Runs the configured load for its duration and reports.
pub async fn run(cfg: Config) -> anyhow::Result<Report> {
    let leader = connect(&cfg, &cfg.addr).await?;
    let shared = Arc::new(Shared {
        stop: AtomicBool::new(false),
        transactions: AtomicU64::new(0),
        events: AtomicU64::new(0),
        conflicts: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        reads: AtomicU64::new(0),
        events_read: AtomicU64::new(0),
        subscribed: AtomicU64::new(0),
        backups: AtomicU64::new(0),
        retentions: AtomicU64::new(0),
        floor: AtomicU64::new(leader.info().await?.floor),
        latencies: Mutex::new(Vec::new()),
        violations: Mutex::new(Vec::new()),
    });
    let mut tasks = Vec::new();
    for w in 0..cfg.writers {
        tasks.push(tokio::spawn(writer(
            cfg.clone(),
            leader.clone(),
            shared.clone(),
            w,
        )));
    }
    for r in 0..cfg.readers {
        tasks.push(tokio::spawn(reader(
            leader.clone(),
            shared.clone(),
            r,
            cfg.seed,
        )));
    }
    for s in 0..cfg.subscribers {
        tasks.push(tokio::spawn(subscriber(leader.clone(), shared.clone(), s)));
    }
    tasks.push(tokio::spawn(admin(
        cfg.clone(),
        leader.clone(),
        shared.clone(),
    )));
    tokio::time::sleep(cfg.duration).await;
    shared.stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(30), t).await;
    }

    let final_head = leader.head().await?;
    let mut replica_head = None;
    if let Some(addr) = &cfg.replica {
        let replica = connect(&cfg, addr).await?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let h = replica.head().await?;
            if h >= final_head {
                replica_head = Some(h);
                break;
            }
            if Instant::now() > deadline {
                shared.violation(format!(
                    "replica stuck at {h} while the leader is at {final_head}"
                ));
                replica_head = Some(h);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if replica_head == Some(final_head) {
            for a in 0..cfg.aggregates {
                let (l, r) = (
                    leader.aggregate_len(aggregate(a)).await?,
                    replica.aggregate_len(aggregate(a)).await?,
                );
                if l != r {
                    shared.violation(format!("aggregate {a}: leader {l}, replica {r}"));
                }
            }
            let floor = shared
                .floor
                .load(Ordering::Relaxed)
                .max(replica.info().await?.floor);
            let mut rng = Rng::new(cfg.seed ^ 0xABCD);
            for _ in 0..10 {
                if final_head <= floor {
                    break;
                }
                let from = floor + rng.below(final_head - floor);
                let (a, b) = (
                    leader.read_all(from, 100).await?,
                    replica.read_all(from, 100).await?,
                );
                let pa: Vec<_> = a.iter().map(|e| (e.position, e.payload.clone())).collect();
                let pb: Vec<_> = b.iter().map(|e| (e.position, e.payload.clone())).collect();
                if pa != pb {
                    shared.violation(format!("page at {from} differs between leader and replica"));
                }
            }
        }
    }

    let mut lat = shared.latencies.lock().await.clone();
    lat.sort_unstable();
    let pct = |p: f64| -> f64 {
        if lat.is_empty() {
            0.0
        } else {
            let i = ((lat.len() - 1) as f64 * p) as usize;
            lat[i] as f64 / 1e6
        }
    };
    Ok(Report {
        transactions: shared.transactions.load(Ordering::Relaxed),
        events: shared.events.load(Ordering::Relaxed),
        conflicts: shared.conflicts.load(Ordering::Relaxed),
        errors: shared.errors.load(Ordering::Relaxed),
        reads: shared.reads.load(Ordering::Relaxed),
        events_read: shared.events_read.load(Ordering::Relaxed),
        subscribed_events: shared.subscribed.load(Ordering::Relaxed),
        backups: shared.backups.load(Ordering::Relaxed),
        retentions: shared.retentions.load(Ordering::Relaxed),
        commit_p50_ms: pct(0.5),
        commit_p99_ms: pct(0.99),
        final_head,
        replica_head,
        violations: shared.violations.lock().await.clone(),
    })
}
