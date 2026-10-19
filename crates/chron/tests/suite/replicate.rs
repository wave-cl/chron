use std::fs;

use chron::{Db, Error, Expected, Options};

use crate::common::*;

fn small() -> Options {
    opts().segment_max_bytes(300)
}

fn write_some(db: &Db, from: u64, n: u64) {
    for i in from..from + n {
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
}

type Snapshot = (
    u64,
    Vec<chron::Event>,
    Vec<chron::Event>,
    Vec<chron::Event>,
    u64,
    u64,
);

fn snapshot(db: &Db) -> Snapshot {
    (
        db.head(),
        db.read_all(0, 100_000).unwrap(),
        db.read_aggregate(agg(1), 0, 100_000).unwrap(),
        db.read_tag("t", 0, 100_000).unwrap(),
        db.aggregate_len(agg(1)).unwrap(),
        db.aggregate_len(agg(2)).unwrap(),
    )
}

#[test]
fn chunks_end_on_transaction_boundaries_and_chain_to_an_identical_snapshot() {
    let d = tmp();
    let src = open_with(d.path().join("src"), small());
    write_some(&src, 0, 60);
    assert!(segments(&d.path().join("src")).len() > 3);
    let dst = open_with(d.path().join("dst"), small());
    dst.adopt_id(src.id()).unwrap();
    let mut from = 0;
    let mut chunks = 0;
    while let Some(c) = src.replication_chunk(from, 7, usize::MAX).unwrap() {
        assert_eq!(c.from, from);
        assert!(c.to > c.from && c.to <= src.head());
        let events = src.read_all(c.from, (c.to - c.from) as usize).unwrap();
        assert!(
            events[0].is_first_in_txn(),
            "chunk {chunks} starts a transaction"
        );
        assert!(
            events.last().unwrap().is_last_in_txn(),
            "chunk {chunks} ends one"
        );
        assert_eq!(dst.apply_chunk(&c).unwrap(), c.to);
        from = c.to;
        chunks += 1;
    }
    assert!(chunks > 10);
    assert_eq!(from, src.head());
    assert_eq!(snapshot(&dst), snapshot(&src));
    assert_eq!(dst.id(), src.id());
    drop(dst);
    let dst = open_with(d.path().join("dst"), small().verify_all_segments(true));
    assert_eq!(snapshot(&dst), snapshot(&src));
    assert!(
        segments(&d.path().join("dst")).len() > 3,
        "the destination rolled too"
    );
}

#[test]
fn chunks_respect_the_byte_cap_but_take_one_transaction_whole() {
    let d = tmp();
    let src = open(d.path().join("src"));
    for i in 0..20 {
        let mut t = src.transaction();
        t.append(
            agg(1),
            Expected::Any,
            vec![ev(&"x".repeat(1000)), ev(&format!("e{i}"))],
        );
        t.commit().unwrap();
    }
    let c = src.replication_chunk(0, 1000, 2500).unwrap().unwrap();
    assert_eq!(
        c.to, 4,
        "two ~1 KiB transactions fit under 2500 bytes, a third does not"
    );
    assert!(c.frames.len() <= 2500);
    let c = src.replication_chunk(0, 1000, 10).unwrap().unwrap();
    assert_eq!(c.to, 2, "the first transaction is always whole");
    assert!(c.frames.len() > 10);
}

#[test]
fn a_long_transaction_is_sent_whole() {
    let d = tmp();
    let src = open(d.path().join("src"));
    let mut t = src.transaction();
    t.append(
        agg(1),
        Expected::New,
        (0..2000).map(|i| ev(&format!("big{i}"))).collect(),
    );
    t.commit().unwrap();
    fill(&src, agg(2), 1);
    let first = src.replication_chunk(0, 100, usize::MAX).unwrap().unwrap();
    assert_eq!((first.from, first.to), (0, 2000));
    let second = src
        .replication_chunk(first.to, 100, usize::MAX)
        .unwrap()
        .unwrap();
    assert_eq!((second.from, second.to), (2000, 2001));
    assert!(
        src.replication_chunk(2001, 100, usize::MAX)
            .unwrap()
            .is_none()
    );
    let dst = open(d.path().join("dst"));
    dst.apply_chunk(&first).unwrap();
    dst.apply_chunk(&second).unwrap();
    assert_eq!(
        dst.read_all(0, 10_000).unwrap(),
        src.read_all(0, 10_000).unwrap()
    );
}

#[test]
fn chunk_positions_past_head_or_below_floor_are_refused() {
    let d = tmp();
    let db = open_with(d.path(), small());
    write_some(&db, 0, 40);
    assert!(
        db.replication_chunk(db.head(), 10, usize::MAX)
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        db.replication_chunk(db.head() + 1, 10, usize::MAX),
        Err(Error::PositionOutOfRange { .. })
    ));
    let floor = db.retain_from(db.head() / 2).unwrap().floor;
    assert!(matches!(
        db.replication_chunk(floor - 1, 10, usize::MAX),
        Err(Error::BelowFloor { .. })
    ));
    let c = db
        .replication_chunk(floor, 10, usize::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(c.from, floor);
}

#[test]
fn apply_chunk_refuses_a_gap_and_a_torn_chunk() {
    let d = tmp();
    let src = open(d.path().join("src"));
    write_some(&src, 0, 6);
    let c = src.replication_chunk(4, 100, usize::MAX).unwrap().unwrap();
    assert!(
        matches!(
            src.replication_chunk(2, 100, usize::MAX),
            Err(Error::Inconsistent { position: 2, .. })
        ),
        "position 2 is inside the first transaction"
    );
    let dst = open(d.path().join("dst"));
    assert!(matches!(
        dst.apply_chunk(&c),
        Err(Error::ReplicationGap { from: 4, head: 0 })
    ));
    let whole = src.replication_chunk(0, 100, usize::MAX).unwrap().unwrap();
    let torn = &whole.frames[..whole.frames.len() - 10];
    assert!(matches!(
        dst.apply_chunk(&chron::Chunk {
            from: 0,
            to: whole.to,
            frames: torn.to_vec()
        }),
        Err(Error::Corrupt { .. })
    ));
    assert_eq!(dst.head(), 0, "nothing written");
    dst.apply_chunk(&whole).unwrap();
    assert_eq!(dst.head(), src.head());
}

#[test]
fn db_id_persists_across_reopen_rebuild_and_restore_and_old_databases_get_one() {
    let d = tmp();
    let src = open(d.path().join("src"));
    let id = src.id();
    write_some(&src, 0, 5);
    assert_eq!(src.info().unwrap().db_id, id);
    drop(src);
    assert_eq!(open(d.path().join("src")).id(), id, "reopen");
    Db::rebuild_index(d.path().join("src"), &opts()).unwrap();
    let src = open(d.path().join("src"));
    assert_eq!(src.id(), id, "rebuild keeps the identity");

    let archive = d.path().join("full.chronbak");
    let meta = src.backup(&archive).unwrap();
    assert_eq!(meta.db_id, Some(id));
    assert_eq!(chron::inspect_backup(&archive).unwrap().db_id, Some(id));
    chron::restore_backup(&archive, d.path().join("dst"), &opts()).unwrap();
    let dst = open(d.path().join("dst"));
    assert_eq!(dst.id(), id, "a restored backup is the same database");

    let other = open(d.path().join("other"));
    write_some(&other, 0, 2);
    let inc = d.path().join("other.chronbak");
    other.backup_since(&inc, 0).unwrap();
    assert!(matches!(
        src.apply_backup(&inc),
        Err(Error::WrongDatabase { theirs, ours, .. }) if theirs == other.id() && ours == id
    ));

    src.debug_forget_id().unwrap();
    drop(src);
    let src = open(d.path().join("src"));
    let minted = src.id();
    assert_ne!(minted, id, "an old database gets a fresh identity");
    drop(src);
    assert_eq!(
        open(d.path().join("src")).id(),
        minted,
        "which then persists"
    );
    drop(dst);
    fs::remove_file(d.path().join("dst").join("index.redb")).unwrap();
    Db::rebuild_index(d.path().join("dst"), &opts()).unwrap();
    assert_ne!(
        open(d.path().join("dst")).id(),
        id,
        "a rebuild without an index mints anew"
    );
}

#[test]
fn adopt_id_only_on_an_empty_database_and_epoch_is_durable() {
    let d = tmp();
    let db = open(d.path());
    let other = chron::Uuid::new_v4();
    db.adopt_id(other).unwrap();
    assert_eq!(db.id(), other);
    fill(&db, agg(1), 1);
    assert!(matches!(
        db.adopt_id(chron::Uuid::new_v4()),
        Err(Error::NotEmpty { head: 1 })
    ));
    assert_eq!(db.epoch(), 0);
    db.set_epoch(3).unwrap();
    assert_eq!(db.epoch(), 3);
    drop(db);
    let db = open(d.path());
    assert_eq!(
        (db.id(), db.epoch(), db.info().unwrap().epoch),
        (other, 3, 3)
    );
}

#[test]
fn fingerprint_matches_a_restored_backup_and_differs_after_a_divergent_write() {
    let d = tmp();
    let src = open(d.path().join("src"));
    write_some(&src, 0, 5);
    let head = src.head();
    let archive = d.path().join("full.chronbak");
    src.backup(&archive).unwrap();
    chron::restore_backup(&archive, d.path().join("dst"), &opts()).unwrap();
    let dst = open(d.path().join("dst"));
    for p in 0..head {
        assert_eq!(
            src.fingerprint(p).unwrap(),
            dst.fingerprint(p).unwrap(),
            "position {p}"
        );
    }
    assert!(src.fingerprint(head).unwrap().is_none());
    let mut t = src.transaction();
    t.append_free(vec![ev("on the source")]);
    t.commit().unwrap();
    let mut t = dst.transaction();
    t.append_free(vec![ev("on the copy")]);
    t.commit().unwrap();
    assert_ne!(
        src.fingerprint(head).unwrap(),
        dst.fingerprint(head).unwrap()
    );
    assert!(src.fingerprint(head).unwrap().is_some());
}

/// A chunk lands as one batch on the destination: one segment sync and
/// one index commit for every transaction it carries.
#[test]
fn a_chunk_lands_as_one_batch() {
    let d = tmp();
    let src = open(d.path().join("src"));
    write_some(&src, 0, 20);
    let dst = open_with(
        d.path().join("dst"),
        opts()
            .fsync(chron::Fsync::Always)
            .index_checkpoint_batches(1),
    );
    dst.adopt_id(src.id()).unwrap();
    let chunk = src.replication_chunk(0, 1000, usize::MAX).unwrap().unwrap();
    let before = dst.stats();
    assert_eq!(dst.apply_chunk(&chunk).unwrap(), src.head());
    let after = dst.stats();
    assert_eq!(after.batches - before.batches, 1, "one batch");
    assert_eq!(after.fsyncs - before.fsyncs, 1, "one segment sync");
    assert_eq!(after.index_checkpoints - before.index_checkpoints, 1);
    assert_eq!(after.transactions - before.transactions, 20);
    assert_eq!(after.events - before.events, src.head());
    assert_eq!(snapshot(&dst), snapshot(&src));
    drop(dst);
    let dst = open_with(d.path().join("dst"), opts().verify_all_segments(true));
    assert_eq!(snapshot(&dst), snapshot(&src));
}

/// A destination with a small segment limit still rolls inside a chunk:
/// several batches, never more than one transaction over the limit.
#[test]
fn a_chunk_still_rolls_at_the_segment_limit() {
    let d = tmp();
    let src = open(d.path().join("src"));
    write_some(&src, 0, 40);
    let dst = open_with(d.path().join("dst"), small());
    dst.adopt_id(src.id()).unwrap();
    let chunk = src.replication_chunk(0, 1000, usize::MAX).unwrap().unwrap();
    dst.apply_chunk(&chunk).unwrap();
    assert!(dst.stats().batches > 1, "the segment limit split the chunk");
    let segs = segments(&d.path().join("dst"));
    assert!(segs.len() > 3);
    for s in &segs[..segs.len() - 1] {
        assert!(
            file_len(s) < 300 + 400,
            "{s:?} overshoots by at most one transaction"
        );
    }
    assert_eq!(snapshot(&dst), snapshot(&src));
}
