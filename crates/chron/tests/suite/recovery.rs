use std::fs;
use std::time::Duration;

use chron::{Db, Error, Expected, Fsync, Options};

use crate::common::*;

/// A valid header for a segment whose first record has `base`.
fn header(base: u64) -> [u8; 64] {
    let mut h = [0u8; 64];
    h[0..8].copy_from_slice(b"CHRONSG\0");
    h[8..12].copy_from_slice(&1u32.to_be_bytes());
    h[16..24].copy_from_slice(&base.to_be_bytes());
    let crc = crc32fast::hash(&h);
    h[12..16].copy_from_slice(&crc.to_be_bytes());
    h
}

fn durable() -> Options {
    Options::new().fsync(Fsync::Always)
}

/// A complete transaction the segment holds past the index head was
/// written and synced before the index commit that a crash cut: it is
/// replayed on open, as `rebuild_index` has always kept it.
#[test]
fn a_complete_unindexed_transaction_is_replayed_on_open() {
    let d = tmp();
    let db = open_with(d.path(), durable());
    fill(&db, agg(1), 10);
    let seg = last_segment(d.path());
    let committed = file_len(&seg);
    let mut t = db.transaction();
    t.append(
        agg(1),
        Expected::Any,
        vec![tagged("ghost0", &["g"]), ev("ghost1"), ev("ghost2")],
    );
    db.debug_write_without_commit(t).unwrap();
    drop(db);
    let written = file_len(&seg);
    assert!(written > committed, "the ghost bytes reached the file");

    let db = open_with(d.path(), durable().verify_all_segments(true));
    assert_eq!(db.head(), 13);
    assert_eq!(file_len(&seg), written, "nothing was cut");
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 13);
    let all = db.read_all(0, 100).unwrap();
    assert_eq!(all.len(), 13);
    assert_eq!(payloads(&all[10..]), ["ghost0", "ghost1", "ghost2"]);
    assert_eq!(positions(&db.read_tag("g", 0, 0).unwrap()), [10]);
    assert_eq!(
        indices(&db.read_aggregate(agg(1), 9, 10).unwrap()),
        [9, 10, 11, 12]
    );
    let mut t = db.transaction();
    t.append_one(agg(1), Expected::Exact(13), ev("real"));
    let c = t.commit().unwrap();
    assert_eq!(c.first, 13);
    assert_eq!(c.appends[0].first_index, 13);
    drop(db);
    let db = open_with(d.path(), durable().verify_all_segments(true));
    assert_eq!(payloads(&db.read_all(13, 10).unwrap()), ["real"]);
    assert_eq!(db.head(), 14);
}

#[test]
fn torn_tail_matrix() {
    let d = tmp();
    let db = open_with(d.path(), durable());
    fill(&db, agg(1), 3);
    let seg = last_segment(d.path());
    let committed = file_len(&seg);
    let one_record = {
        let a = db.debug_locate(1).unwrap().unwrap();
        let b = db.debug_locate(2).unwrap().unwrap();
        b.1 - a.1
    };
    let mut t = db.transaction();
    t.append(agg(1), Expected::Any, vec![ev("g0"), ev("g1")]);
    db.debug_write_without_commit(t).unwrap();
    drop(db);
    let ghosts = file_len(&seg) - committed;
    assert_eq!(ghosts, 2 * one_record);
    // Uncut, the whole transaction is there and is replayed.
    {
        let db = open_with(d.path(), durable());
        assert_eq!(db.head(), 5, "uncut");
        assert_eq!(payloads(&db.read_all(3, 10).unwrap()), ["g0", "g1"]);
        db.truncate_to(3).unwrap();
        drop(db);
        assert_eq!(file_len(&seg), committed);
    }

    let cuts = [
        1,
        3,
        one_record / 2,
        one_record - 8,
        one_record + 1,
        ghosts - 1,
    ];
    for cut in cuts {
        let db = open_with(d.path(), durable());
        let mut t = db.transaction();
        t.append(agg(1), Expected::Any, vec![ev("g0"), ev("g1")]);
        db.debug_write_without_commit(t).unwrap();
        drop(db);
        assert_eq!(file_len(&seg), committed + ghosts);
        chop(&seg, cut);
        let db = open_with(d.path(), durable());
        assert_eq!(db.head(), 3, "cut {cut}");
        assert_eq!(file_len(&seg), committed, "cut {cut}");
        assert_eq!(
            payloads(&db.read_all(0, 10).unwrap()),
            ["e0", "e1", "e2"],
            "cut {cut}"
        );
        drop(db);
    }

    // Two transactions past the index, the second torn: the first is
    // replayed, the second cut.
    {
        // The first committed with its index commit held back (the
        // index image copied before it), the second written without one.
        let db = open_with(
            d.path(),
            durable()
                .index_checkpoint_batches(usize::MAX)
                .index_checkpoint_interval(Duration::from_secs(3600)),
        );
        let crashed = snapshot_index(d.path());
        let mut t = db.transaction();
        t.append(agg(1), Expected::Any, vec![ev("h0"), ev("h1")]);
        t.commit().unwrap();
        let mut t = db.transaction();
        t.append(agg(1), Expected::Any, vec![ev("h2"), ev("h3")]);
        db.debug_write_without_commit(t).unwrap();
        drop(db);
        restore_index(d.path(), &crashed);
        assert_eq!(file_len(&seg), committed + 2 * ghosts);
        chop(&seg, 5);
        let db = open_with(d.path(), durable());
        assert_eq!(db.head(), 5, "two transactions, the second torn");
        assert_eq!(payloads(&db.read_all(3, 10).unwrap()), ["h0", "h1"]);
        assert_eq!(file_len(&seg), committed + ghosts);
        db.truncate_to(3).unwrap();
        drop(db);
    }

    chop(&seg, 1);
    assert!(matches!(
        Db::open(d.path(), durable()),
        Err(Error::Corrupt { .. })
    ));
}

/// Checkpoints held back: the index file (its image as a crash leaves
/// it, copied while open) lags the data across a segment roll, and open
/// replays through the roll.
#[test]
fn replay_crosses_a_segment_roll() {
    let d = tmp();
    let o = durable()
        .segment_max_bytes(300)
        .index_checkpoint_batches(usize::MAX)
        .index_checkpoint_interval(Duration::from_secs(3600));
    let db = open_with(d.path(), o.clone());
    fill(&db, agg(1), 2);
    db.flush().unwrap();
    let durable_head = db.head();
    for i in 0..30 {
        let mut t = db.transaction();
        t.append_one(
            agg(1 + i % 2),
            Expected::Any,
            tagged(&format!("r{i}"), &["t"]),
        );
        t.commit().unwrap();
    }
    let head = db.head();
    let before = db.read_all(0, 0).unwrap();
    let tag_before = db.read_tag("t", 0, 0).unwrap();
    let segs = segments(d.path()).len();
    assert!(segs > 3, "rolled");
    assert_eq!(db.stats().index_checkpoints, 1, "only the flush");
    // The index file as a crash leaves it: copied while open.
    let crashed = snapshot_index(d.path());
    drop(db);
    restore_index(d.path(), &crashed);

    let db = open_with(d.path(), o.verify_all_segments(true));
    assert_eq!(db.head(), head);
    assert!(head > durable_head);
    assert_eq!(db.read_all(0, 0).unwrap(), before);
    assert_eq!(db.read_tag("t", 0, 0).unwrap(), tag_before);
    assert_eq!(db.aggregate_len(agg(1)).unwrap(), 2 + 15);
    assert_eq!(db.aggregate_len(agg(2)).unwrap(), 15);
    assert_eq!(segments(d.path()).len(), segs, "no segment lost");
    fill(&db, agg(1), 1);
    assert_eq!(db.head(), head + 1);
}

/// The index file put back from an earlier copy: everything committed
/// since is replayed from the segments.
#[test]
fn a_stale_index_copy_is_rolled_forward() {
    let d = tmp();
    let db = open_with(d.path(), durable());
    fill(&db, agg(1), 3);
    db.flush().unwrap();
    let stale = snapshot_index(d.path());
    for i in 0..5 {
        let mut t = db.transaction();
        t.append(
            agg(1),
            Expected::Exact(3 + 2 * i),
            vec![tagged("a", &["t"]), ev("b")],
        );
        t.append_free(vec![ev("f")]);
        t.commit().unwrap();
    }
    let before = db.read_all(0, 0).unwrap();
    let lens = (db.aggregate_len(agg(1)).unwrap(), db.head());
    drop(db);
    restore_index(d.path(), &stale);
    let db = open_with(d.path(), durable().verify_all_segments(true));
    assert_eq!(db.head(), 18);
    assert_eq!((db.aggregate_len(agg(1)).unwrap(), db.head()), lens);
    assert_eq!(db.read_all(0, 0).unwrap(), before);
    assert_eq!(db.read_tag("t", 0, 0).unwrap().len(), 5);
    assert_eq!(
        indices(&db.read_aggregate(agg(1), 0, 0).unwrap()),
        (0..13).collect::<Vec<_>>()
    );
    drop(db);
    // The replay was checkpointed: a second open has nothing to do and
    // the index on disk is at the head.
    let db = open_with(d.path(), durable());
    assert_eq!(db.head(), 18);
}

/// Damage inside the replayed tail with a valid record after it is
/// corruption, not a torn tail.
#[test]
fn damage_in_the_replayed_tail_followed_by_a_valid_record_is_corrupt() {
    let d = tmp();
    let db = open_with(d.path(), durable());
    fill(&db, agg(1), 2);
    db.flush().unwrap();
    let stale = snapshot_index(d.path());
    fill(&db, agg(1), 4);
    let (_, offset, _) = db.debug_locate(3).unwrap().unwrap();
    drop(db);
    let seg = last_segment(d.path());
    restore_index(d.path(), &stale);
    flip(&seg, offset + 20);
    match Db::open(d.path(), durable()) {
        Err(Error::Corrupt { reason, .. }) => {
            assert!(reason.contains("committed records follow"), "{reason}")
        }
        other => panic!("unexpected {:?}", other.map(|_| ())),
    }
}

#[test]
fn a_flipped_committed_byte_is_corrupt() {
    let d = tmp();
    let db = open_with(d.path(), durable());
    fill(&db, agg(1), 3);
    let seg = last_segment(d.path());
    let (_, offset, _) = db.debug_locate(2).unwrap().unwrap();
    drop(db);
    flip(&seg, offset + 20);
    assert!(matches!(
        Db::open(d.path(), durable()),
        Err(Error::Corrupt { .. })
    ));
}

#[test]
fn a_flipped_byte_in_an_earlier_segment_fails_the_read() {
    let d = tmp();
    let o = durable().segment_max_bytes(200);
    let db = open_with(d.path(), o.clone());
    fill(&db, agg(1), 6);
    assert!(segments(d.path()).len() >= 3);
    let (base, offset, _) = db.debug_locate(0).unwrap().unwrap();
    assert_eq!(base, 0);
    drop(db);
    flip(&segments(d.path())[0], offset + 20);
    let db = open_with(d.path(), o);
    assert!(matches!(db.read_all(0, 1), Err(Error::Corrupt { .. })));
    match db.read_all(0, 100) {
        Err(Error::Corrupt { offset: at, .. }) => {
            assert_eq!(at, offset, "the flipped record is blamed")
        }
        other => panic!("unexpected {other:?}"),
    }
    assert!(matches!(
        db.read_aggregate(agg(1), 0, 1),
        Err(Error::Corrupt { .. })
    ));
    assert!(db.read_all(5, 1).is_ok(), "the tail segment still reads");
}

#[test]
fn verify_all_segments_catches_a_flipped_byte_in_an_early_segment() {
    let d = tmp();
    let o = durable().segment_max_bytes(200);
    let db = open_with(d.path(), o.clone());
    fill(&db, agg(1), 6);
    assert!(segments(d.path()).len() >= 3);
    let (_, offset, _) = db.debug_locate(0).unwrap().unwrap();
    drop(db);
    flip(&segments(d.path())[0], offset + 20);
    assert!(
        Db::open(d.path(), o.clone()).is_ok(),
        "without the option only the tail is checked"
    );
    match Db::open(d.path(), o.verify_all_segments(true)) {
        Err(Error::Corrupt { offset: at, .. }) => assert_eq!(at, offset),
        other => panic!("unexpected {:?}", other.map(|_| ())),
    }
}

#[test]
fn verify_all_segments_passes_on_a_healthy_multi_segment_db() {
    let d = tmp();
    let o = opts().segment_max_bytes(300);
    let before = {
        let db = open_with(d.path(), o.clone());
        for i in 0..40u64 {
            let mut t = db.transaction();
            t.append_one(
                agg(1 + i as u128 % 2),
                Expected::Any,
                tagged(&format!("e{i}"), &["t"]),
            );
            t.append_free(vec![ev("free")]);
            t.commit().unwrap();
        }
        assert!(segments(d.path()).len() > 3);
        db.read_all(0, 1000).unwrap()
    };
    let db = open_with(d.path(), o.verify_all_segments(true));
    assert_eq!(db.read_all(0, 1000).unwrap(), before);
    assert_eq!(db.head(), 80);
    fill(&db, agg(1), 1);
    assert_eq!(db.head(), 81);
}

#[test]
fn verify_all_segments_catches_a_shifted_index_entry() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 3);
    drop(db);
    {
        use redb::ReadableTable;
        const POS: redb::TableDefinition<u64, (u64, u64, u32)> = redb::TableDefinition::new("pos");
        let index = redb::Database::open(d.path().join("index.redb")).unwrap();
        let txn = index.begin_write().unwrap();
        {
            let mut t = txn.open_table(POS).unwrap();
            let (base, offset, len) = t.get(1).unwrap().unwrap().value();
            t.insert(1, (base, offset + 1, len)).unwrap();
        }
        txn.commit().unwrap();
    }
    assert!(
        Db::open(d.path(), opts()).is_ok(),
        "the tail check looks at the head only"
    );
    assert!(matches!(
        Db::open(d.path(), opts().verify_all_segments(true)),
        Err(Error::Corrupt { .. })
    ));
}

#[test]
fn torn_roll() {
    let d = tmp();
    let db = open_with(d.path(), durable());
    fill(&db, agg(1), 2);
    drop(db);
    let dir = d.path().join("segments");

    // A roll that died before its header was complete.
    let short = dir.join(format!("{:020}.seg", 2));
    fs::write(&short, [1u8; 12]).unwrap();
    let db = open_with(d.path(), durable());
    assert!(!short.exists());
    assert_eq!(db.head(), 2);
    drop(db);

    // A complete header-only segment at the head is kept as the tail.
    let o = durable().segment_max_bytes(64);
    let db = open_with(d.path(), o.clone());
    fill(&db, agg(1), 1);
    drop(db);
    let tail = last_segment(d.path());
    assert_eq!(
        tail.file_name().unwrap().to_str().unwrap(),
        format!("{:020}.seg", 3)
    );
    assert_eq!(file_len(&tail), 64);
    let db = open_with(d.path(), o.clone());
    assert!(tail.exists());
    fill(&db, agg(1), 1);
    drop(db);
    assert!(file_len(&tail) > 64);

    // A segment past the head is deleted.
    let future = dir.join(format!("{:020}.seg", 99));
    fs::write(&future, header(99)).unwrap();
    let db = open_with(d.path(), o.clone());
    assert!(!future.exists());
    assert_eq!(db.head(), 4);
    drop(db);

    // A header whose base disagrees with the name is corrupt.
    let liar = dir.join(format!("{:020}.seg", 5));
    fs::copy(&tail, &liar).unwrap();
    assert!(matches!(Db::open(d.path(), o), Err(Error::Corrupt { .. })));
}

#[test]
fn segments_without_an_index_are_corrupt() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 1);
    drop(db);
    fs::remove_file(d.path().join("index.redb")).unwrap();
    assert!(matches!(
        Db::open(d.path(), opts()),
        Err(Error::Corrupt { .. })
    ));
}
