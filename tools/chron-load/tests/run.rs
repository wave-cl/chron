//! A short run against in-process daemons: a leader with a backup
//! directory and a replica, with backups and retention on tight timers.

use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn a_short_run_finds_no_violation() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = chrond::Options::new(dir.path().join("leader"), "127.0.0.1:0".parse().unwrap());
    opts.fsync = false;
    opts.backup_dir = Some(dir.path().join("backups"));
    let leader = chrond::start(opts).await.unwrap();
    let mut ropts =
        chrond::Options::new(dir.path().join("replica"), "127.0.0.1:0".parse().unwrap());
    ropts.fsync = false;
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
    })
    .await
    .unwrap();
    eprintln!("{}", report.to_json());
    assert!(report.transactions > 100, "{report:?}");
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
