//! Regressions for what the round-5 review found.

use std::fs;

use chron::{Db, Error, Expected, Fsync, Options};

use crate::common::*;

fn multi_segment(dir: &std::path::Path) -> Db {
    let db = open_with(dir, opts().segment_max_bytes(300));
    for i in 0..60u64 {
        let mut t = db.transaction();
        t.append_one(
            agg(1 + i as u128 % 2),
            Expected::Any,
            tagged(&format!("e{i}"), &["t"]),
        );
        t.commit().unwrap();
    }
    assert!(segments(dir).len() > 3);
    db
}

#[test]
fn a_retention_between_the_head_check_and_the_read_snapshot_is_seen() {
    let d = tmp();
    let db = multi_segment(d.path());
    let target = db.head() / 2;
    let writer = db.clone();
    let result = db.read_all_with(0, 10, move || {
        writer.retain_from(target).unwrap();
    });
    assert!(
        matches!(result, Err(Error::BelowFloor { position: 0, .. })),
        "{result:?}"
    );
    let floor = db.floor();
    assert!(floor > 0);
    assert_eq!(db.read_all(floor, 1).unwrap()[0].position, floor);
}

#[test]
fn an_aggregate_wholly_below_the_floor_keeps_its_length_across_restore_and_rebuild() {
    let d = tmp();
    let src = open_with(d.path().join("src"), opts().segment_max_bytes(300));
    let early = agg(9);
    fill(&src, early, 5);
    for i in 0..60u64 {
        let mut t = src.transaction();
        t.append_one(agg(1 + i as u128 % 2), Expected::Any, ev(&format!("e{i}")));
        t.commit().unwrap();
    }
    let floor = src.retain_from(src.head() / 2).unwrap().floor;
    assert!(floor > 5);
    assert!(
        src.read_aggregate(early, 0, 10).unwrap().is_empty(),
        "all of its events are gone"
    );
    assert_eq!(src.aggregate_len(early).unwrap(), 5);

    let archive = d.path().join("full.chronbak");
    src.backup(&archive).unwrap();
    chron::restore_backup(&archive, d.path().join("dst"), &opts()).unwrap();
    let dst = open(d.path().join("dst"));
    assert_eq!(
        dst.aggregate_len(early).unwrap(),
        5,
        "restore kept the length"
    );
    let mut t = dst.transaction();
    t.append_one(early, Expected::New, ev("x"));
    assert!(matches!(
        t.commit(),
        Err(Error::ExpectedLength { actual: 5, .. })
    ));
    let mut t = dst.transaction();
    t.append_one(early, Expected::Exact(5), ev("sixth"));
    assert_eq!(t.commit().unwrap().appends[0].first_index, 5);
    drop(dst);

    drop(src);
    let r = Db::rebuild_index(d.path().join("src"), &opts()).unwrap();
    assert!(r.identity_kept);
    let src = open(d.path().join("src"));
    assert_eq!(
        src.aggregate_len(early).unwrap(),
        5,
        "rebuild kept the length"
    );
    assert_eq!(src.aggregate_len(agg(1)).unwrap(), 30);
}

#[test]
fn rebuild_refuses_damage_inside_the_last_segment() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 1000);
    let (_, offset, _) = db.debug_locate(10).unwrap().unwrap();
    drop(db);
    flip(&last_segment(d.path()), offset + 20);
    fs::remove_file(d.path().join("index.redb")).unwrap();
    match Db::rebuild_index(d.path(), &opts()) {
        Err(Error::Corrupt { reason, .. }) => {
            assert!(reason.contains("committed records follow"), "{reason}")
        }
        other => panic!("unexpected {other:?}"),
    }
    assert!(!d.path().join("index.redb").exists(), "nothing was built");
}

#[test]
fn rebuild_reports_a_torn_tail_and_a_lowered_record_limit_is_not_damage() {
    let d = tmp();
    let db = open_with(d.path(), Options::new().fsync(Fsync::Always));
    fill(&db, agg(1), 3);
    let mut t = db.transaction();
    t.append(agg(1), Expected::Any, vec![ev("g0"), ev("g1"), ev("g2")]);
    db.debug_write_without_commit(t).unwrap();
    drop(db);
    chop(&last_segment(d.path()), 5);
    fs::remove_file(d.path().join("index.redb")).unwrap();
    let r = Db::rebuild_index(d.path(), &opts().max_record_bytes(64)).unwrap();
    assert_eq!((r.head, r.dropped), (3, 2));
    assert!(r.tail_bytes_dropped > 0);
    assert!(r.tail_reason.is_some());
    assert!(!r.identity_kept, "no index was there to keep it from");
}

#[test]
fn a_failed_commit_leaves_no_leftovers_for_retention_or_verification() {
    let d = tmp();
    // A small segment and a large failed commit, so its bytes would outlive
    // the smaller commits that follow and the roll after them.
    let db = open_with(d.path(), opts().segment_max_bytes(200));
    fill(&db, agg(1), 2);
    db.debug_fail_next_commit();
    let big = "g".repeat(100);
    let mut t = db.transaction();
    t.append(agg(1), Expected::Any, vec![ev(&big), ev(&big), ev(&big)]);
    assert!(matches!(t.commit(), Err(Error::Io { .. })));
    assert_eq!(db.head(), 2);
    fill(&db, agg(1), 30);
    assert!(
        segments(d.path()).len() > 2,
        "rolled past the segment with the failed commit"
    );
    let before = db.read_all(0, 1000).unwrap();
    assert_eq!(before.len(), 32);
    assert!(payloads(&before).iter().all(|p| !p.starts_with("ggg")));
    let floor = db.retain_from(db.head() / 2).unwrap().floor;
    assert_eq!(db.read_all(floor, 1000).unwrap(), before[floor as usize..]);
    drop(db);
    let db = open_with(
        d.path(),
        opts().segment_max_bytes(200).verify_all_segments(true),
    );
    assert_eq!(db.read_all(floor, 1000).unwrap(), before[floor as usize..]);
}

#[test]
fn set_epoch_never_goes_backwards_and_wait_for_accepts_the_longest_timeout() {
    let d = tmp();
    let db = open(d.path());
    db.set_epoch(3).unwrap();
    db.set_epoch(3).unwrap();
    assert!(matches!(
        db.set_epoch(2),
        Err(Error::EpochNotNewer {
            epoch: 2,
            current: 3
        })
    ));
    assert_eq!(db.epoch(), 3);
    assert_eq!(db.wait_for(0, std::time::Duration::MAX), 0);
    assert!(
        !d.path().join("index.redb.new").exists(),
        "the index was renamed into place"
    );
}
