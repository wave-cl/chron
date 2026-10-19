use chron::{Db, Error, Expected, Fsync, Options};

use crate::common::*;

#[test]
fn reopen_preserves_everything() {
    let d = tmp();
    let snapshot = |db: &Db| {
        (
            db.head(),
            db.read_all(0, 1000).unwrap(),
            db.read_aggregate(agg(1), 0, 1000).unwrap(),
            db.read_aggregate(agg(2), 0, 1000).unwrap(),
            db.read_tag("t", 0, 1000).unwrap(),
            db.aggregate_len(agg(1)).unwrap(),
        )
    };
    let before = {
        let db = open(d.path());
        for i in 0..20 {
            let mut t = db.transaction();
            t.append_one(
                agg(1 + i % 2),
                Expected::Any,
                tagged(&format!("e{i}"), if i % 3 == 0 { &["t"] } else { &[] }),
            );
            t.commit().unwrap();
        }
        snapshot(&db)
    };
    let db = open(d.path());
    assert_eq!(snapshot(&db), before);
    assert_eq!(db.head(), 20);
    let mut t = db.transaction();
    t.append_one(agg(1), Expected::Exact(10), ev("more"));
    assert_eq!(t.commit().unwrap().first, 20);
    drop(db);
    let db = open(d.path());
    assert_eq!(db.head(), 21);
    assert_eq!(payloads(&db.read_all(20, 10).unwrap()), ["more"]);
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 11);
}

#[test]
fn lock_refuses_a_second_open() {
    let d = tmp();
    let db = open(d.path());
    assert!(matches!(
        Db::open(d.path(), opts()),
        Err(Error::Locked { .. })
    ));
    drop(db);
    open(d.path());
}

#[test]
fn fsync_never_flush_and_drop_persist() {
    let d = tmp();
    let o = Options::new().fsync(Fsync::Never);
    let db = open_with(d.path(), o.clone());
    fill(&db, agg(1), 2);
    db.flush().unwrap();
    fill(&db, agg(1), 1);
    drop(db);
    let db = open_with(d.path(), o);
    assert_eq!(db.head(), 3);
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 3);
}

#[test]
fn wait_for_sees_a_commit_from_another_thread() {
    let d = tmp();
    let db = open(d.path());
    assert_eq!(db.wait_for(1, std::time::Duration::from_millis(50)), 0);
    let writer = db.clone();
    let h = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(50));
        fill(&writer, agg(1), 1);
    });
    assert_eq!(db.wait_for(1, std::time::Duration::from_secs(10)), 1);
    h.join().unwrap();
}

/// Under `Fsync::Always` with checkpoints held back, a clean close still
/// makes the index durable: the file on disk is at the head.
#[test]
fn a_clean_close_under_always_checkpoints() {
    let d = tmp();
    let o = Options::new()
        .fsync(Fsync::Always)
        .index_checkpoint_batches(usize::MAX)
        .index_checkpoint_interval(std::time::Duration::from_secs(3600));
    let db = open_with(d.path(), o);
    fill(&db, agg(1), 7);
    assert_eq!(db.stats().index_checkpoints, 0);
    drop(db);
    use redb::ReadableDatabase;
    let index = redb::Database::open(d.path().join("index.redb")).unwrap();
    let txn = index.begin_read().unwrap();
    let meta: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("meta");
    let head = txn
        .open_table(meta)
        .unwrap()
        .get("head")
        .unwrap()
        .map(|v| v.value());
    assert_eq!(head, Some(7));
}

/// What a crash leaves and what the tests copy: while commits are
/// pending a durable commit, the index file on disk is at the last one
/// (redb flips its header only durably); a clean close persists them.
#[test]
fn the_index_file_on_disk_is_at_the_last_durable_commit_while_open() {
    let d = tmp();
    let o = Options::new()
        .fsync(Fsync::Always)
        .index_checkpoint_batches(usize::MAX)
        .index_checkpoint_interval(std::time::Duration::from_secs(3600));
    let db = open_with(d.path(), o);
    fill(&db, agg(1), 3);
    db.flush().unwrap();
    fill(&db, agg(1), 5);
    assert_eq!(db.head(), 8);
    let image = snapshot_index(d.path());
    let copy = tmp();
    std::fs::write(copy.path().join("index.redb"), &image).unwrap();
    use redb::ReadableDatabase;
    let meta: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("meta");
    let head_of = |path: std::path::PathBuf| {
        let index = redb::Database::open(path).unwrap();
        let txn = index.begin_read().unwrap();
        txn.open_table(meta)
            .unwrap()
            .get("head")
            .unwrap()
            .map(|v| v.value())
    };
    assert_eq!(
        head_of(copy.path().join("index.redb")),
        Some(3),
        "the image lags"
    );
    drop(db);
    assert_eq!(
        head_of(d.path().join("index.redb")),
        Some(8),
        "the close persisted"
    );
}
