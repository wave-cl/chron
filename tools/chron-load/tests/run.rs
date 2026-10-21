//! A short run against in-process daemons: a leader with a backup
//! directory and a replica, with backups and retention on tight timers.

use std::time::Duration;

/// `CHRON_LOAD_FSYNC=1` runs the daemons with fsync on (off by default:
/// the tests are about invariants, not disks).
fn fsync() -> bool {
    std::env::var("CHRON_LOAD_FSYNC").is_ok_and(|v| v == "1")
}

/// `CHRON_LOAD_IN_FLIGHT`: commits per writer in flight (default 1).
fn in_flight() -> usize {
    std::env::var("CHRON_LOAD_IN_FLIGHT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_short_run_finds_no_violation() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = chrond::Options::new(dir.path().join("leader"), "127.0.0.1:0".parse().unwrap());
    opts.fsync = fsync();
    opts.backup_dir = Some(dir.path().join("backups"));
    let leader = chrond::start(opts).await.unwrap();
    let mut ropts =
        chrond::Options::new(dir.path().join("replica"), "127.0.0.1:0".parse().unwrap());
    ropts.fsync = fsync();
    ropts.replicate_from = Some(format!("http://{}", leader.local_addr));
    let replica = chrond::start(ropts).await.unwrap();

    let report = chron_load::run(chron_load::Config {
        addr: format!("http://{}", leader.local_addr),
        replica: Some(format!("http://{}", replica.local_addr)),
        token: None,
        database: None,
        duration: Duration::from_secs(if std::env::var("CHRON_LOAD_SECS").is_ok() {
            std::env::var("CHRON_LOAD_SECS").unwrap().parse().unwrap()
        } else {
            20
        }),
        writers: 4,
        readers: 2,
        subscribers: 1,
        aggregates: 8,
        events_per_txn: 2,
        payload_bytes: 120,
        backup_dir: Some(dir.path().join("backups")),
        backup_every: Some(Duration::from_secs(5)),
        retain_every: Some(Duration::from_secs(7)),
        seed: 7,
        kill_every: None,
        killer: None,
        sync: None,
        in_flight: in_flight(),
        local: false,
    })
    .await
    .unwrap();
    eprintln!("{}", report.to_json());
    assert!(report.transactions > 100, "{report:?}");
    assert!(report.transactions_per_s > 0.0 && report.wall_s > 0.0);
    assert_eq!(report.fsync, Some(fsync()));
    assert!(
        report.reads > 10 && report.subscribed_events > 100,
        "{report:?}"
    );
    assert!(report.backups >= 2 && report.retentions >= 1, "{report:?}");
    assert_eq!(report.replica_head, Some(report.final_head));
    assert!(report.ok(), "{:#?}", report.violations);
    replica.shutdown().await.unwrap();
    leader.shutdown().await.unwrap();
}

/// A three-node group whose leader is killed every few seconds and
/// restarted two seconds later, in both commit modes: no violation, and
/// in sync mode no acknowledged write is lost at all.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_with_the_leader_killed_finds_no_violation() {
    use chrond::testing::Cluster;
    let secs: u64 = std::env::var("CHRON_LOAD_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    for (mode, sync) in [("sync", Some(true)), ("async", Some(false))] {
        let cluster = std::sync::Arc::new(tokio::sync::Mutex::new(
            Cluster::start_fsync(3, Duration::from_secs(1), fsync()).await,
        ));
        let addrs: Vec<String> = {
            let c = cluster.lock().await;
            c.wait_for_leader(Duration::from_secs(6)).await;
            (0..3).map(|i| c.addr(i)).collect()
        };
        let killer: chron_load::Killer = {
            let cluster = cluster.clone();
            std::sync::Arc::new(move |addr: String| {
                let cluster = cluster.clone();
                Box::pin(async move {
                    let i = {
                        let mut c = cluster.lock().await;
                        let Some(i) = (0..3).find(|i| c.addr(*i) == addr) else {
                            return false;
                        };
                        c.stop(i).await;
                        i
                    };
                    let cluster = cluster.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        cluster.lock().await.restart(i).await;
                    });
                    true
                })
            })
        };
        let report = chron_load::run(chron_load::Config {
            addr: addrs.join(","),
            replica: None,
            token: None,
            database: None,
            duration: Duration::from_secs(secs),
            writers: 4,
            readers: 2,
            subscribers: 1,
            aggregates: 8,
            events_per_txn: 2,
            payload_bytes: 120,
            backup_dir: None,
            backup_every: None,
            retain_every: None,
            seed: 11,
            kill_every: Some(Duration::from_secs(6)),
            killer: Some(killer),
            sync,
            in_flight: in_flight(),
            local: false,
        })
        .await
        .unwrap();
        eprintln!("{mode}: {}", report.to_json());
        assert!(report.kills >= 2, "{mode}: {report:?}");
        assert!(report.transactions > 100, "{mode}: {report:?}");
        assert!(report.ok(), "{mode}: {:#?}", report.violations);
        if mode == "sync" {
            assert_eq!(
                report.lost_acks, 0,
                "{mode}: an acknowledged write was lost"
            );
        }
        // Wait for the last restart before tearing down.
        tokio::time::sleep(Duration::from_secs(3)).await;
        cluster.lock().await.stop_all().await;
    }
}

/// Several commits in flight per writer on distinct aggregates: the
/// expectations stay exact, cross-writer conflicts happen, nothing is
/// violated, and the report carries throughput.
#[tokio::test(flavor = "multi_thread")]
async fn a_short_pipelined_run_finds_no_violation() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = chrond::Options::new(dir.path().join("leader"), "127.0.0.1:0".parse().unwrap());
    opts.fsync = fsync();
    let leader = chrond::start(opts).await.unwrap();
    let report = chron_load::run(chron_load::Config {
        addr: format!("http://{}", leader.local_addr),
        replica: None,
        token: None,
        database: None,
        duration: Duration::from_secs(10),
        writers: 4,
        readers: 1,
        subscribers: 1,
        aggregates: 8,
        events_per_txn: 2,
        payload_bytes: 120,
        backup_dir: None,
        backup_every: None,
        retain_every: None,
        seed: 13,
        kill_every: None,
        killer: None,
        sync: None,
        in_flight: 8,
        local: false,
    })
    .await
    .unwrap();
    eprintln!("{}", report.to_json());
    assert_eq!(report.in_flight, 8);
    assert!(report.conflicts > 0, "writers share aggregates: {report:?}");
    assert!(report.transactions > 100 && report.transactions_per_s > 0.0);
    assert!(report.ok(), "{:#?}", report.violations);
    leader.shutdown().await.unwrap();
}
