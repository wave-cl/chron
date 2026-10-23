//! Streams: a named subset of the global order over a set of tags, filled
//! from what is there and kept up as records land.

use std::time::Duration;

use chron::{Error, Event, Expected};

use crate::common::*;

/// Positions of the events carrying any of `tags`, by merging the tag
/// reads: what a stream over `tags` must equal.
fn merged(db: &chron::Db, tags: &[&str]) -> Vec<u64> {
    let mut all: Vec<u64> = tags
        .iter()
        .flat_map(|t| positions(&db.read_tag(t, 0, 0).unwrap()))
        .collect();
    all.sort_unstable();
    all.dedup();
    all
}

/// Commits `n` events whose tags cycle through `pattern` (an empty slice
/// means no tag), one per transaction, on aggregate 1.
fn write_pattern(db: &chron::Db, n: usize, pattern: &[&[&str]]) {
    for i in 0..n {
        let tags = pattern[i % pattern.len()];
        let mut t = db.transaction();
        t.append_one(agg(1), Expected::Any, tagged(&format!("e{i}"), tags));
        t.commit().unwrap();
    }
}

const PATTERN: &[&[&str]] = &[&["a"], &["b"], &["a", "b"], &["c"], &[], &["b", "c"]];

#[test]
fn a_stream_collects_its_tags_in_global_order_as_records_land() {
    let d = tmp();
    let db = open(d.path());
    let info = db.create_stream("ab", ["a", "b"]).unwrap();
    assert_eq!(
        (info.name.as_str(), info.tags.clone(), info.ready),
        (
            "ab",
            vec![chron::Bytes::from("a"), chron::Bytes::from("b")],
            true
        )
    );
    write_pattern(&db, 30, PATTERN);
    let got = db.read_stream("ab", 0, 0).unwrap();
    assert_eq!(positions(&got), merged(&db, &["a", "b"]));
    assert!(got.iter().all(|e| e.has_tag(b"a") || e.has_tag(b"b")));
    // An event carrying both tags appears once; one with `c` only never.
    let both: Vec<u64> = (0..30).filter(|i| i % 6 == 2).map(|i| i as u64).collect();
    assert!(both.iter().all(|p| positions(&got).contains(p)));
    assert_eq!(positions(&got).len(), merged(&db, &["a", "b"]).len());
    assert!(!positions(&got).contains(&3), "c alone is not a member");
    // Slicing and the backward read.
    assert_eq!(
        positions(&db.read_stream("ab", 10, 3).unwrap()),
        merged(&db, &["a", "b"])
            .into_iter()
            .filter(|p| *p >= 10)
            .take(3)
            .collect::<Vec<_>>()
    );
    let back = positions(&db.read_stream_backward("ab", u64::MAX, 4).unwrap());
    let mut want = merged(&db, &["a", "b"]);
    want.reverse();
    assert_eq!(back, want[..4]);
    assert_eq!(
        positions(&db.read_stream_backward("ab", 7, 0).unwrap()),
        want.into_iter().filter(|p| *p <= 7).collect::<Vec<_>>()
    );
    // The paged iterator and a subscription deliver the same.
    let iter: Vec<Event> = db
        .iter_stream("ab", 0)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(positions(&iter), positions(&got));
    let mut sub = db.subscribe_stream("ab", 0).unwrap();
    let mut seen = Vec::new();
    while let Some(ev) = sub.next_timeout(Duration::from_millis(50)).unwrap() {
        seen.push(ev.position);
    }
    assert_eq!(seen, positions(&got));
    // Live: a new member and a non-member.
    let mut t = db.transaction();
    t.append_free(vec![tagged("x", &["c"]), tagged("y", &["b"])]);
    let c = t.commit().unwrap();
    let ev = sub.next_timeout(Duration::from_secs(5)).unwrap().unwrap();
    assert_eq!(ev.position, c.first + 1);
    assert!(
        sub.next_timeout(Duration::from_millis(50))
            .unwrap()
            .is_none()
    );
    assert_eq!(db.info().unwrap().streams, 1);
}

#[test]
fn a_stream_created_later_covers_what_was_already_there() {
    let d = tmp();
    let db = open(d.path());
    write_pattern(&db, 40, PATTERN);
    db.create_stream("bc", ["c", "b", "c"]).unwrap();
    assert_eq!(
        db.stream("bc").unwrap().unwrap().tags,
        vec![chron::Bytes::from("b"), chron::Bytes::from("c")],
        "sorted, deduplicated"
    );
    assert_eq!(
        positions(&db.read_stream("bc", 0, 0).unwrap()),
        merged(&db, &["b", "c"])
    );
    write_pattern(&db, 12, PATTERN);
    assert_eq!(
        positions(&db.read_stream("bc", 0, 0).unwrap()),
        merged(&db, &["b", "c"]),
        "kept up after the fill"
    );
    drop(db);
    let db = open(d.path());
    assert_eq!(db.streams().unwrap().len(), 1, "a reopen keeps it");
    assert_eq!(
        positions(&db.read_stream("bc", 0, 0).unwrap()),
        merged(&db, &["b", "c"])
    );
}

#[test]
fn names_tags_and_absence_are_checked() {
    let d = tmp();
    let db = open(d.path());
    assert!(matches!(db.read_stream("none", 0, 0), Err(Error::NoStream(n)) if n == "none"));
    assert!(matches!(
        db.subscribe_stream("none", 0),
        Err(Error::NoStream(_))
    ));
    assert!(matches!(db.iter_stream("none", 0), Err(Error::NoStream(_))));
    assert_eq!(db.stream("none").unwrap(), None);
    assert!(db.streams().unwrap().is_empty());
    assert!(matches!(
        db.create_stream("", ["a"]),
        Err(Error::InvalidStream(_))
    ));
    assert!(matches!(
        db.create_stream("s", Vec::<&str>::new()),
        Err(Error::InvalidStream(_))
    ));
    let many: Vec<String> = (0..33).map(|i| format!("t{i}")).collect();
    assert!(matches!(
        db.create_stream("s", many),
        Err(Error::InvalidStream(_))
    ));
    assert!(matches!(
        db.create_stream("s", [vec![0u8; 256]]),
        Err(Error::InvalidStream(_))
    ));
    db.create_stream("s", ["a"]).unwrap();
    assert!(matches!(
        db.create_stream("s", ["b"]),
        Err(Error::StreamExists(n)) if n == "s"
    ));
    assert!(db.delete_stream("s").unwrap());
    assert!(!db.delete_stream("s").unwrap());
    assert!(matches!(db.read_stream("s", 0, 0), Err(Error::NoStream(_))));
}

/// An index from before streams existed reads as having none; one can
/// be created on it.
#[test]
fn an_old_index_without_the_tables_reads_as_empty() {
    let d = tmp();
    let db = open(d.path());
    write_pattern(&db, 6, PATTERN);
    db.create_stream("ab", ["a", "b"]).unwrap();
    db.debug_drop_stream_tables().unwrap();
    assert!(db.streams().unwrap().is_empty());
    assert!(matches!(
        db.read_stream("ab", 0, 0),
        Err(Error::NoStream(_))
    ));
    write_pattern(&db, 6, PATTERN);
    db.create_stream("ab", ["a", "b"]).unwrap();
    assert_eq!(
        positions(&db.read_stream("ab", 0, 0).unwrap()),
        merged(&db, &["a", "b"])
    );
}

/// Deleting a stream removes its entries; a recreate with other tags
/// starts clean.
#[test]
fn delete_removes_the_entries_and_a_recreate_starts_clean() {
    let d = tmp();
    let db = open(d.path());
    write_pattern(&db, 24, PATTERN);
    db.create_stream("s", ["a"]).unwrap();
    assert_eq!(
        positions(&db.read_stream("s", 0, 0).unwrap()),
        merged(&db, &["a"])
    );
    assert!(db.delete_stream("s").unwrap());
    db.create_stream("s", ["c"]).unwrap();
    assert_eq!(
        positions(&db.read_stream("s", 0, 0).unwrap()),
        merged(&db, &["c"])
    );
    write_pattern(&db, 6, PATTERN);
    assert_eq!(
        positions(&db.read_stream("s", 0, 0).unwrap()),
        merged(&db, &["c"])
    );
}

/// A stream begun while a batch waits behind the writer lock misses
/// none of that batch's members and doubles none: whichever of the two
/// takes the lock first, the batch lands through the cache or lies below
/// the head the definition records.
#[test]
fn a_create_racing_a_held_batch_misses_nothing() {
    for round in 0..10 {
        let d = tmp();
        let db = open(d.path());
        write_pattern(&db, 6, PATTERN);
        let hold = db.debug_hold_writer();
        let db1 = db.clone();
        let writer = std::thread::spawn(move || {
            for i in 0..5 {
                let mut t = db1.transaction();
                t.append_free(vec![tagged(&format!("w{i}"), &["a", "x"])]);
                t.commit().unwrap();
            }
        });
        let db2 = db.clone();
        let creator = std::thread::spawn(move || db2.create_stream("s", ["a"]).unwrap());
        // Both threads queue behind the hold; the order they take the
        // lock in once it drops is theirs.
        std::thread::sleep(Duration::from_millis(20 + round * 5));
        drop(hold);
        writer.join().unwrap();
        creator.join().unwrap();
        assert_eq!(
            positions(&db.read_stream("s", 0, 0).unwrap()),
            merged(&db, &["a"]),
            "round {round}"
        );
    }
}

/// A truncation drops the stream entries from the cut on; records landing
/// at the reused positions are indexed afresh. A cut at the head changes
/// nothing.
#[test]
fn truncate_drops_entries_from_the_cut_and_reused_positions_are_indexed_again() {
    let d = tmp();
    let db = open(d.path());
    write_pattern(&db, 24, PATTERN);
    db.create_stream("ab", ["a", "b"]).unwrap();
    let before = positions(&db.read_stream("ab", 0, 0).unwrap());
    db.truncate_to(db.head()).unwrap();
    assert_eq!(positions(&db.read_stream("ab", 0, 0).unwrap()), before);
    db.truncate_to(10).unwrap();
    assert_eq!(
        positions(&db.read_stream("ab", 0, 0).unwrap()),
        merged(&db, &["a", "b"])
    );
    assert!(
        positions(&db.read_stream("ab", 0, 0).unwrap())
            .iter()
            .all(|p| *p < 10)
    );
    // Reused positions with other tags: only the members come back.
    for i in 0..10 {
        let mut t = db.transaction();
        let tags: &[&str] = if i % 2 == 0 { &["b"] } else { &["c"] };
        t.append_free(vec![tagged(&format!("r{i}"), tags)]);
        t.commit().unwrap();
    }
    assert_eq!(
        positions(&db.read_stream("ab", 0, 0).unwrap()),
        merged(&db, &["a", "b"])
    );
    assert_eq!(
        positions(&db.read_stream("ab", 10, 0).unwrap()),
        [10, 12, 14, 16, 18]
    );
}

/// Retention drops the stream entries below the floor, a read from 0
/// starts at the floor, and a retention landing between two chunks of a
/// fill leaves no entry below the new floor.
#[test]
fn retention_drops_entries_below_the_floor_even_during_a_fill() {
    let d = tmp();
    let db = open_with(d.path(), opts().segment_max_bytes(300));
    write_pattern(&db, 60, PATTERN);
    assert!(segments(d.path()).len() > 3);
    db.create_stream("ab", ["a", "b"]).unwrap();
    let head = db.head();
    db.retain_from(db.floor()).unwrap();
    assert_eq!(
        positions(&db.read_stream("ab", 0, 0).unwrap()),
        merged(&db, &["a", "b"]),
        "a retention at the floor changes nothing"
    );
    let r = db.retain_from(head / 2).unwrap();
    assert!(r.floor > 0);
    let got = positions(&db.read_stream("ab", 0, 0).unwrap());
    assert_eq!(got, merged(&db, &["a", "b"]));
    assert!(got.iter().all(|p| *p >= r.floor));
    assert_eq!(
        db.debug_stream_entries("ab").unwrap(),
        got,
        "the table holds nothing below the floor"
    );
    let mut sub = db.subscribe_stream("ab", 0).unwrap();
    assert_eq!(
        sub.next_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .position,
        got[0],
        "a subscription from below the floor starts at it"
    );
    // A second stream, filled with a retention between its chunks: the
    // first chunk's entries below the new floor are gone with it.
    db.begin_stream("c", ["b", "c"]).unwrap();
    let floor_before = db.floor();
    let db2 = db.clone();
    let mut cut = false;
    let done = db
        .debug_fill_stream_with("c", || {
            if !cut {
                cut = true;
                let h = db2.head();
                db2.retain_from(h - h / 4).unwrap();
            }
        })
        .unwrap();
    assert!(done);
    assert!(cut, "the hook ran");
    assert!(db.floor() > floor_before);
    let got = positions(&db.read_stream("c", 0, 0).unwrap());
    assert_eq!(got, merged(&db, &["b", "c"]));
    assert!(got.iter().all(|p| *p >= db.floor()));
    assert_eq!(
        db.debug_stream_entries("c").unwrap(),
        got,
        "the chunk written before the retention left nothing below the floor"
    );
    assert!(
        positions(&db.read_stream_backward("c", u64::MAX, 0).unwrap())
            .iter()
            .all(|p| *p >= db.floor())
    );
}

/// A fill of several chunks while another thread commits members: the
/// result equals the merge of the tag reads.
#[test]
fn a_fill_of_several_chunks_interleaved_with_commits_is_complete() {
    let d = tmp();
    let db = open(d.path());
    // 60 000 tagged events: more than one fill chunk.
    for i in 0..300 {
        let mut t = db.transaction();
        let tags: &[&str] = if i % 2 == 0 { &["a"] } else { &["b"] };
        t.append_free(
            (0..200)
                .map(|j| tagged(&format!("{i}-{j}"), tags))
                .collect(),
        );
        t.commit().unwrap();
    }
    let db1 = db.clone();
    let writer = std::thread::spawn(move || {
        for i in 0..200 {
            let mut t = db1.transaction();
            t.append_free(vec![
                tagged(&format!("live{i}"), &["a"]),
                tagged("n", &["c"]),
            ]);
            t.commit().unwrap();
        }
    });
    db.create_stream("a", ["a"]).unwrap();
    writer.join().unwrap();
    assert_eq!(
        positions(&db.read_stream("a", 0, 0).unwrap()),
        merged(&db, &["a"])
    );
    assert_eq!(db.read_stream("a", 0, 0).unwrap().len(), 30_000 + 200);
}

/// Records written but not committed to the index before a crash are
/// replayed at the next open, into the stream too.
#[test]
fn a_crash_before_the_index_commit_is_replayed_into_the_stream() {
    let d = tmp();
    let db = open(d.path());
    write_pattern(&db, 12, PATTERN);
    db.create_stream("ab", ["a", "b"]).unwrap();
    let mut t = db.transaction();
    t.append_free(vec![
        tagged("lost-a", &["a"]),
        tagged("lost-c", &["c"]),
        tagged("lost-b", &["b", "c"]),
    ]);
    let c = db.debug_write_without_commit(t).unwrap();
    drop(db);
    let db = open(d.path());
    assert_eq!(db.head(), c.next, "replayed");
    let got = positions(&db.read_stream("ab", 0, 0).unwrap());
    assert_eq!(got, merged(&db, &["a", "b"]));
    assert!(got.contains(&c.first) && got.contains(&(c.first + 2)));
    assert!(!got.contains(&(c.first + 1)));
}

/// A creation interrupted before its fill finished leaves nothing: the
/// next open removes the definition and whatever entries it had.
#[test]
fn an_interrupted_create_leaves_nothing_after_a_reopen() {
    let d = tmp();
    let db = open(d.path());
    write_pattern(&db, 12, PATTERN);
    db.begin_stream("s", ["a"]).unwrap();
    // Records landing after the definition are indexed for it already.
    write_pattern(&db, 6, PATTERN);
    assert!(!db.debug_stream_entries("s").unwrap().is_empty());
    assert!(matches!(
        db.read_stream("s", 0, 0),
        Err(Error::StreamNotReady(_))
    ));
    assert!(!db.stream("s").unwrap().unwrap().ready);
    drop(db);
    let db = open(d.path());
    assert!(db.streams().unwrap().is_empty(), "gone with its entries");
    assert!(matches!(db.read_stream("s", 0, 0), Err(Error::NoStream(_))));
    db.create_stream("s", ["a"]).unwrap();
    assert_eq!(
        positions(&db.read_stream("s", 0, 0).unwrap()),
        merged(&db, &["a"])
    );
}

/// An index rebuild keeps the definitions and recomputes the entries.
#[test]
fn a_rebuild_keeps_the_streams() {
    let d = tmp();
    let db = open(d.path());
    write_pattern(&db, 24, PATTERN);
    db.create_stream("ab", ["a", "b"]).unwrap();
    db.create_stream("c", ["c"]).unwrap();
    let want_ab = positions(&db.read_stream("ab", 0, 0).unwrap());
    let want_c = positions(&db.read_stream("c", 0, 0).unwrap());
    drop(db);
    chron::Db::rebuild_index(d.path(), &opts()).unwrap();
    let db = open(d.path());
    let names: Vec<String> = db.streams().unwrap().into_iter().map(|s| s.name).collect();
    assert_eq!(names, ["ab", "c"]);
    assert!(db.streams().unwrap().iter().all(|s| s.ready));
    assert_eq!(positions(&db.read_stream("ab", 0, 0).unwrap()), want_ab);
    assert_eq!(positions(&db.read_stream("c", 0, 0).unwrap()), want_c);
    write_pattern(&db, 6, PATTERN);
    assert_eq!(
        positions(&db.read_stream("ab", 0, 0).unwrap()),
        merged(&db, &["a", "b"])
    );
}

/// A full backup carries the definitions (format 3); a restore rebuilds
/// their entries. An archive from a database without streams keeps the
/// earlier format.
#[test]
fn a_backup_carries_the_definitions_and_a_restore_rebuilds_the_entries() {
    let d = tmp();
    let db = open(d.path());
    write_pattern(&db, 24, PATTERN);
    let plain = d.path().join("plain.chronbak");
    assert_eq!(db.backup(&plain).unwrap().format, 1);
    db.create_stream("ab", ["a", "b"]).unwrap();
    db.create_stream("c", ["c"]).unwrap();
    let archive = d.path().join("full.chronbak");
    let meta = db.backup(&archive).unwrap();
    assert_eq!(meta.format, 3);
    assert_eq!(chron::inspect_backup(&archive).unwrap().format, 3);
    assert_eq!(
        meta.files as usize,
        segments(d.path()).len() + 2,
        "lengths and streams"
    );
    let dst = d.path().join("restored");
    chron::restore_backup(&archive, &dst, &opts()).unwrap();
    let restored = open(&dst);
    assert_eq!(restored.streams().unwrap(), db.streams().unwrap());
    assert_eq!(
        positions(&restored.read_stream("ab", 0, 0).unwrap()),
        positions(&db.read_stream("ab", 0, 0).unwrap())
    );
    assert_eq!(
        positions(&restored.read_stream("c", 0, 0).unwrap()),
        positions(&db.read_stream("c", 0, 0).unwrap())
    );
    let plain_dst = d.path().join("plain");
    chron::restore_backup(&plain, &plain_dst, &opts()).unwrap();
    assert!(open(&plain_dst).streams().unwrap().is_empty());
}

#[cfg(feature = "tokio")]
mod tokio_api {
    use super::*;

    const LIVE: Duration = Duration::from_secs(10);

    /// A delete ends an async waiter on the stream at once; a subscription
    /// on another stream goes on.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_delete_ends_a_waiting_subscription() {
        let d = tmp();
        let db = open(d.path());
        write_pattern(&db, 6, PATTERN);
        db.create_stream("ab", ["a", "b"]).unwrap();
        db.create_stream("c", ["c"]).unwrap();
        let mut sub = db.subscribe_stream("ab", 0).unwrap();
        let mut other = db.subscribe_stream("c", 0).unwrap();
        while sub.position() < db.head() {
            sub.next_async().await.unwrap();
        }
        let db2 = db.clone();
        tokio::task::spawn_blocking(move || {
            std::thread::sleep(Duration::from_millis(100));
            assert!(db2.delete_stream("ab").unwrap());
        });
        match tokio::time::timeout(LIVE, sub.next_async()).await.unwrap() {
            Err(Error::NoStream(n)) => assert_eq!(n, "ab"),
            other => panic!("expected NoStream, got {other:?}"),
        }
        assert!(db.stream("ab").unwrap().is_none());
        let mut t = db.transaction();
        t.append_free(vec![tagged("z", &["c"])]);
        let c = t.commit().unwrap();
        let mut last = 0;
        while last < c.first {
            last = tokio::time::timeout(LIVE, other.next_async())
                .await
                .unwrap()
                .unwrap()
                .position;
        }
        assert_eq!(last, c.first, "the other stream still delivers");
    }
}
