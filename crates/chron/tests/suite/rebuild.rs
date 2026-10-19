use std::fs;

use chron::{Db, Error, Expected, Fsync, Options};

use crate::common::*;

fn durable() -> Options {
    Options::new().fsync(Fsync::Always)
}

type Snapshot = (
    u64,
    Vec<chron::Event>,
    Vec<chron::Event>,
    Vec<chron::Event>,
    Vec<chron::Event>,
    u64,
    u64,
);

fn snapshot(db: &Db) -> Snapshot {
    (
        db.head(),
        db.read_all(0, 10_000).unwrap(),
        db.read_aggregate(agg(1), 0, 10_000).unwrap(),
        db.read_aggregate(agg(2), 0, 10_000).unwrap(),
        db.read_tag("t", 0, 10_000).unwrap(),
        db.aggregate_len(agg(1)).unwrap(),
        db.aggregate_len(agg(2)).unwrap(),
    )
}

#[test]
fn rebuild_after_deleting_the_index_restores_the_snapshot() {
    let d = tmp();
    let o = opts().segment_max_bytes(500);
    let before = {
        let db = open_with(d.path(), o.clone());
        for i in 0..60u64 {
            let mut t = db.transaction();
            let tags: &[&str] = if i % 3 == 0 { &["t"] } else { &[] };
            t.append_one(
                agg(1 + i as u128 % 2),
                Expected::Any,
                tagged(&format!("e{i}"), tags),
            );
            if i % 5 == 0 {
                t.append(agg(1), Expected::Any, vec![ev("x"), tagged("y", &["t"])]);
                t.append_free(vec![tagged("free", &["t"])]);
            }
            t.commit().unwrap();
        }
        snapshot(&db)
    };
    assert!(segments(d.path()).len() > 3);
    fs::remove_file(d.path().join("index.redb")).unwrap();
    assert!(matches!(
        Db::open(d.path(), o.clone()),
        Err(Error::Corrupt { .. })
    ));

    let r = Db::rebuild_index(d.path(), &o).unwrap();
    assert_eq!(r.head, before.0);
    assert_eq!(r.segments, segments(d.path()).len());
    assert_eq!(r.dropped, 0);
    assert!(!d.path().join("index.redb.rebuild").exists());

    let db = open_with(d.path(), o.clone().verify_all_segments(true));
    assert_eq!(snapshot(&db), before);
    let len = db.aggregate_len(agg(1)).unwrap();
    let mut t = db.transaction();
    t.append_one(agg(1), Expected::Exact(len), ev("after"));
    let c = t.commit().unwrap();
    assert_eq!(c.first, before.0);
    assert_eq!(c.appends[0].first_index, len);
}

#[test]
fn rebuild_drops_a_torn_trailing_transaction() {
    let d = tmp();
    let db = open_with(d.path(), durable());
    fill(&db, agg(1), 3);
    let seg = last_segment(d.path());
    let committed = file_len(&seg);
    let mut t = db.transaction();
    t.append(agg(1), Expected::Any, vec![ev("g0"), ev("g1"), ev("g2")]);
    db.debug_write_without_commit(t).unwrap();
    drop(db);
    let ghost_bytes = file_len(&seg) - committed;
    chop(&seg, ghost_bytes / 3 / 2);
    fs::remove_file(d.path().join("index.redb")).unwrap();

    let r = Db::rebuild_index(d.path(), &durable()).unwrap();
    assert_eq!((r.head, r.floor, r.segments, r.dropped), (3, 0, 1, 2));
    let db = open_with(d.path(), durable());
    assert_eq!(db.head(), 3);
    assert_eq!(
        file_len(&seg),
        committed,
        "open cut the torn transaction's bytes"
    );
    assert_eq!(payloads(&db.read_all(0, 10).unwrap()), ["e0", "e1", "e2"]);
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 3);
}

#[test]
fn rebuild_keeps_a_complete_unacknowledged_transaction() {
    let d = tmp();
    let db = open_with(d.path(), durable());
    fill(&db, agg(1), 3);
    let mut t = db.transaction();
    t.append(
        agg(1),
        Expected::Any,
        vec![tagged("g0", &["t"]), ev("g1"), ev("g2")],
    );
    db.debug_write_without_commit(t).unwrap();
    drop(db);
    fs::remove_file(d.path().join("index.redb")).unwrap();
    let r = Db::rebuild_index(d.path(), &durable()).unwrap();
    assert_eq!((r.head, r.floor, r.segments, r.dropped), (6, 0, 1, 0));
    let db = open_with(d.path(), durable().verify_all_segments(true));
    assert_eq!(payloads(&db.read_all(3, 10).unwrap()), ["g0", "g1", "g2"]);
    assert_eq!(payloads(&db.read_tag("t", 0, 10).unwrap()), ["g0"]);
    assert_eq!(
        indices(&db.read_aggregate(agg(1), 0, 10).unwrap()),
        [0, 1, 2, 3, 4, 5]
    );
}

#[test]
fn rebuild_refuses_a_bad_header_and_leaves_the_old_index_alone() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 2);
    drop(db);
    let index = d.path().join("index.redb");
    let before = fs::read(&index).unwrap();
    flip(&segments(d.path())[0], 20);
    assert!(matches!(
        Db::rebuild_index(d.path(), &opts()),
        Err(Error::Corrupt { offset: 0, .. })
    ));
    assert!(!d.path().join("index.redb.rebuild").exists());
    assert_eq!(fs::read(&index).unwrap(), before);
}

#[test]
fn rebuild_refuses_damage_inside_an_earlier_segment() {
    let d = tmp();
    let o = opts().segment_max_bytes(200);
    let db = open_with(d.path(), o.clone());
    fill(&db, agg(1), 6);
    let (_, offset, _) = db.debug_locate(0).unwrap().unwrap();
    drop(db);
    flip(&segments(d.path())[0], offset + 20);
    match Db::rebuild_index(d.path(), &o) {
        Err(Error::Corrupt { offset: at, .. }) => assert_eq!(at, offset),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn rebuild_refuses_while_open_and_is_idempotent() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 2);
    assert!(matches!(
        Db::rebuild_index(d.path(), &opts()),
        Err(Error::Locked { .. })
    ));
    let before = snapshot(&db);
    drop(db);
    let r = Db::rebuild_index(d.path(), &opts()).unwrap();
    assert_eq!((r.head, r.floor, r.segments, r.dropped), (2, 0, 1, 0));
    assert_eq!(snapshot(&open(d.path())), before);
}

#[test]
fn rebuild_of_an_empty_directory_gives_head_zero() {
    let d = tmp();
    let r = Db::rebuild_index(d.path().join("fresh"), &opts()).unwrap();
    assert_eq!((r.head, r.floor, r.segments, r.dropped), (0, 0, 0, 0));
    let db = open(d.path().join("fresh"));
    assert_eq!(db.head(), 0);
    fill(&db, agg(1), 1);
    assert_eq!(db.head(), 1);
}

/// A database whose index lags its data: rebuilding from the segments
/// and recovering on open agree.
#[test]
fn rebuild_of_a_lagging_index_db_matches_recovery() {
    let d = tmp();
    let db = open_with(d.path(), durable());
    fill(&db, agg(1), 4);
    db.flush().unwrap();
    let stale = snapshot_index(d.path());
    for i in 0..6 {
        let mut t = db.transaction();
        t.append(
            agg(1 + i % 2),
            Expected::Any,
            vec![tagged("x", &["t"]), ev("y")],
        );
        t.commit().unwrap();
    }
    drop(db);
    restore_index(d.path(), &stale);
    let recovered = {
        let db = open_with(d.path(), durable().verify_all_segments(true));
        snapshot(&db)
    };
    fs::remove_file(d.path().join("index.redb")).unwrap();
    let r = Db::rebuild_index(d.path(), &durable()).unwrap();
    assert_eq!((r.head, r.dropped), (16, 0));
    let rebuilt = {
        let db = open_with(d.path(), durable().verify_all_segments(true));
        snapshot(&db)
    };
    assert_eq!(rebuilt, recovered);
}
