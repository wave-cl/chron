//! Idempotent commits: a transaction with a commit id is answered with
//! its first landing when resent, and the id follows its records
//! through replication, backups, rebuilds, truncation and retention.

use std::time::Duration;

use chron::{AppendResult, Committed, Db, Error, Expected, Fsync, NewEvent, Options, Uuid};

use crate::common::*;

fn id(n: u8) -> [u8; 16] {
    [n; 16]
}

/// Two aggregates, the first of them twice, and a free append.
fn mixed(db: &Db, cid: [u8; 16], expected: Expected) -> chron::Result<Committed> {
    let mut t = db.transaction();
    t.append(agg(1), expected, vec![ev("a0"), ev("a1")]);
    t.append_one(agg(2), Expected::Any, tagged("b0", &["t"]));
    t.append_one(agg(1), Expected::Any, ev("a2"));
    t.append_free(vec![ev("f0"), ev("f1")]);
    t.commit_id(cid);
    t.commit()
}

fn one(
    db: &Db,
    cid: [u8; 16],
    aggregate: Uuid,
    expected: Expected,
    payload: &str,
) -> chron::Result<Committed> {
    let mut t = db.transaction();
    t.append_one(aggregate, expected, ev(payload));
    t.commit_id(cid);
    t.commit()
}

#[test]
fn a_resent_commit_id_answers_the_original_positions() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 3);
    let first = mixed(&db, id(7), Expected::Exact(3)).unwrap();
    assert!(!first.duplicate);
    assert_eq!((first.first, first.last, first.next), (3, 8, 9));
    assert_eq!(
        first.appends,
        [
            AppendResult {
                aggregate: Some(agg(1)),
                first_position: 3,
                first_index: 3,
                count: 2,
                new_len: 5
            },
            AppendResult {
                aggregate: Some(agg(2)),
                first_position: 5,
                first_index: 0,
                count: 1,
                new_len: 1
            },
            AppendResult {
                aggregate: Some(agg(1)),
                first_position: 6,
                first_index: 5,
                count: 1,
                new_len: 6
            },
            AppendResult {
                aggregate: None,
                first_position: 7,
                first_index: 0,
                count: 2,
                new_len: 0
            },
        ]
    );
    let events = db.read_all(3, 10).unwrap();
    assert_eq!(events[0].commit_id, Some(id(7)));
    assert!(events[0].flags & chron::FLAG_COMMIT_ID != 0);
    assert!(events[1..].iter().all(|e| e.commit_id.is_none()));
    assert_eq!(db.lookup_commit(id(7)).unwrap(), Some((3, 9)));

    // Resent with the expectation it had (still true by chance or not),
    // and with other payloads: the shape is what counts.
    let again = mixed(&db, id(7), Expected::Exact(3)).unwrap();
    assert!(again.duplicate);
    assert_eq!(
        Committed {
            duplicate: false,
            ..again.clone()
        },
        first
    );
    assert_eq!(db.head(), 9, "nothing was written");
    assert_eq!(db.read_all(3, 10).unwrap(), events);
    assert_eq!(db.stats().duplicates, 1);
    assert_eq!(db.stats().transactions, 4);

    // A stale expectation on a resend is not a conflict.
    let stale = mixed(&db, id(7), Expected::Exact(0)).unwrap();
    assert!(stale.duplicate);
    assert_eq!(stale.first, 3);
    assert_eq!(db.head(), 9);

    // The resend survives a reopen: the id is in the index.
    drop(db);
    let db = open(d.path());
    let after = mixed(&db, id(7), Expected::Exact(3)).unwrap();
    assert!(after.duplicate);
    assert_eq!(after.first, 3);
    assert_eq!(db.head(), 9);
    assert_eq!(db.lookup_commit(id(8)).unwrap(), None);
}

#[test]
fn a_reused_id_with_another_shape_is_refused() {
    let d = tmp();
    let db = open(d.path());
    let c = mixed(&db, id(1), Expected::Any).unwrap();
    let seg_len = file_len(&last_segment(d.path()));
    type Build = Box<dyn Fn(&mut chron::Transaction)>;
    let cases: Vec<(&str, Build)> = vec![
        (
            "fewer events",
            Box::new(|t| {
                t.append(agg(1), Expected::Any, vec![ev("a0")]);
            }),
        ),
        (
            "another aggregate",
            Box::new(|t| {
                t.append(agg(9), Expected::Any, vec![ev("a0"), ev("a1")]);
                t.append_one(agg(2), Expected::Any, ev("b0"));
                t.append_one(agg(1), Expected::Any, ev("a2"));
                t.append_free(vec![ev("f0"), ev("f1")]);
            }),
        ),
        (
            "free where owned",
            Box::new(|t| {
                t.append(agg(1), Expected::Any, vec![ev("a0"), ev("a1")]);
                t.append_free(vec![ev("b0")]);
                t.append_one(agg(1), Expected::Any, ev("a2"));
                t.append_free(vec![ev("f0"), ev("f1")]);
            }),
        ),
    ];
    for (name, build) in cases {
        let mut t = db.transaction();
        build(&mut t);
        t.commit_id(id(1));
        match t.commit() {
            Err(Error::CommitIdReused {
                id: got,
                first,
                next,
                ..
            }) => {
                assert_eq!((got, first, next), (id(1), c.first, c.next), "{name}");
            }
            other => panic!("{name}: {other:?}"),
        }
    }
    assert_eq!(db.head(), c.next, "nothing was written");
    assert_eq!(file_len(&last_segment(d.path())), seg_len);
    assert_eq!(db.stats().duplicates, 0);

    // The same events split into other appends are the same records, so
    // that is a duplicate, answered per the resend's appends.
    let mut t = db.transaction();
    t.append(agg(1), Expected::Any, vec![ev("a0")]);
    t.append(agg(1), Expected::Any, vec![ev("a1")]);
    t.append_one(agg(2), Expected::Any, ev("b0"));
    t.append_one(agg(1), Expected::Any, ev("a2"));
    t.append_free(vec![ev("f0"), ev("f1")]);
    t.commit_id(id(1));
    let split = t.commit().unwrap();
    assert!(split.duplicate);
    assert_eq!((split.first, split.next), (c.first, c.next));
    assert_eq!(split.appends.len(), 5);
    assert_eq!(
        (
            split.appends[1].first_position,
            split.appends[1].first_index,
            split.appends[1].new_len
        ),
        (1, 1, 2)
    );
    assert_eq!(db.head(), c.next);
}

#[test]
fn two_commits_with_one_id_in_one_batch_land_once() {
    let d = tmp();
    let db = open_with(d.path(), opts().fsync(Fsync::Always));
    fill(&db, agg(1), 2);
    let before = db.stats();
    let handles = {
        let hold = db.debug_hold_writer();
        let handles: Vec<_> = (0..3u64)
            .map(|i| {
                let dbc = db.clone();
                let h = std::thread::spawn(move || {
                    // Two resends of one transaction and an unrelated one.
                    let cid = if i < 2 { id(5) } else { id(6) };
                    let mut t = dbc.transaction();
                    t.append(agg(1), Expected::Exact(2), vec![ev("x"), ev("y")]);
                    t.commit_id(cid);
                    t.commit()
                });
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                while db.debug_pending_commits() < i as usize + 1 {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::sleep(Duration::from_millis(2));
                }
                h
            })
            .collect();
        drop(hold);
        handles
    };
    let results: Vec<chron::Result<Committed>> =
        handles.into_iter().map(|h| h.join().unwrap()).collect();
    let a = results[0].as_ref().unwrap();
    let b = results[1].as_ref().unwrap();
    assert_eq!((a.first, a.next), (2, 4));
    assert_eq!((b.first, b.next), (2, 4));
    assert_ne!(a.duplicate, b.duplicate, "exactly one of the pair landed");
    assert_eq!(a.appends, b.appends);
    // The third carries Exact(2) too and is refused: the aggregate grew.
    assert!(matches!(
        results[2],
        Err(Error::ExpectedLength { actual: 4, .. })
    ));
    assert_eq!(db.head(), 4, "one transaction landed for the pair");
    let st = db.stats();
    assert_eq!(st.batches - before.batches, 1);
    assert_eq!(st.transactions - before.transactions, 1);
    assert_eq!(st.duplicates, 1);
    assert_eq!(db.lookup_commit(id(5)).unwrap(), Some((2, 4)));
    assert_eq!(db.lookup_commit(id(6)).unwrap(), None);
}

#[test]
fn an_in_batch_reuse_with_another_shape_is_refused() {
    let d = tmp();
    let db = open(d.path());
    let handles = {
        let hold = db.debug_hold_writer();
        let handles: Vec<_> = (0..2u64)
            .map(|i| {
                let dbc = db.clone();
                let h = std::thread::spawn(move || {
                    let mut t = dbc.transaction();
                    if i == 0 {
                        t.append(agg(1), Expected::Any, vec![ev("x"), ev("y")]);
                    } else {
                        t.append(agg(1), Expected::Any, vec![ev("x")]);
                    }
                    t.commit_id(id(3));
                    t.commit()
                });
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                while db.debug_pending_commits() < i as usize + 1 {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::sleep(Duration::from_millis(2));
                }
                h
            })
            .collect();
        drop(hold);
        handles
    };
    let results: Vec<chron::Result<Committed>> =
        handles.into_iter().map(|h| h.join().unwrap()).collect();
    let landed = results[0].as_ref().unwrap();
    assert_eq!((landed.first, landed.next, landed.duplicate), (0, 2, false));
    assert!(
        matches!(
            results[1],
            Err(Error::CommitIdReused {
                first: 0,
                next: 0,
                ..
            })
        ),
        "{:?}",
        results[1]
    );
    assert_eq!(db.head(), 2);
}

#[test]
fn a_chunk_carries_the_id_and_a_copy_deduplicates() {
    let d = tmp();
    let src = open(d.path().join("src"));
    let dst = open(d.path().join("dst"));
    dst.adopt_id(src.id()).unwrap();
    let c = mixed(&src, id(4), Expected::Any).unwrap();
    one(&src, id(5), agg(3), Expected::New, "solo").unwrap();
    let chunk = src.replication_chunk(0, 100, usize::MAX).unwrap().unwrap();
    assert_eq!(dst.apply_chunk(&chunk).unwrap(), src.head());
    assert_eq!(dst.read_all(0, 1).unwrap()[0].commit_id, Some(id(4)));
    assert_eq!(dst.lookup_commit(id(4)).unwrap(), Some((c.first, c.next)));
    assert_eq!(
        dst.lookup_commit(id(5)).unwrap(),
        Some((c.next, c.next + 1))
    );
    // The copy, promoted, answers a resend with the original positions.
    let again = mixed(&dst, id(4), Expected::Exact(0)).unwrap();
    assert!(again.duplicate);
    assert_eq!((again.first, again.next), (c.first, c.next));
    assert_eq!(again.appends, c.appends);
    assert_eq!(dst.head(), src.head());
    let solo = one(&dst, id(5), agg(3), Expected::New, "other").unwrap();
    assert!(solo.duplicate);
    assert_eq!(solo.first, c.next);
    assert_eq!(dst.head(), src.head());
}

#[test]
fn truncate_forgets_the_ids_of_the_cut_tail() {
    let d = tmp();
    let db = open(d.path());
    let x = {
        let mut t = db.transaction();
        t.append(agg(1), Expected::Any, vec![ev("x0"), ev("x1")]);
        t.commit_id(id(1));
        t.commit().unwrap()
    };
    let y = {
        let mut t = db.transaction();
        t.append(agg(1), Expected::Any, vec![ev("y0"), ev("y1")]);
        t.commit_id(id(2));
        t.commit().unwrap()
    };
    assert_eq!((x.first, y.first), (0, 2));
    db.truncate_to(2).unwrap();
    assert_eq!(db.lookup_commit(id(1)).unwrap(), Some((0, 2)));
    assert_eq!(db.lookup_commit(id(2)).unwrap(), None);
    let again = {
        let mut t = db.transaction();
        t.append(agg(1), Expected::Any, vec![ev("y0"), ev("y1")]);
        t.commit_id(id(2));
        t.commit().unwrap()
    };
    assert!(!again.duplicate, "the cut transaction lands again");
    assert_eq!(again.first, 2);
    assert_eq!(db.head(), 4);
    let kept = {
        let mut t = db.transaction();
        t.append(agg(1), Expected::Any, vec![ev("x0"), ev("x1")]);
        t.commit_id(id(1));
        t.commit().unwrap()
    };
    assert!(kept.duplicate);
    assert_eq!(kept.first, 0);
}

#[test]
fn retention_forgets_ids_below_the_floor_and_keeps_the_rest() {
    let d = tmp();
    let db = open_with(d.path(), opts().segment_max_bytes(300));
    let mut landed = Vec::new();
    for i in 0..60u8 {
        let c = one(&db, id(i), agg(1), Expected::Any, &format!("e{i}")).unwrap();
        landed.push(c.first);
    }
    assert!(segments(d.path()).len() > 3);
    let head = db.head();
    let r = db.retain_from(head / 2).unwrap();
    assert!(r.floor > 0);
    let mut below = 0;
    let mut above = 0;
    for i in 0..60u8 {
        let first = landed[i as usize];
        let known = db.lookup_commit(id(i)).unwrap();
        let again = one(&db, id(i), agg(1), Expected::Any, "resend").unwrap();
        if first < r.floor {
            assert_eq!(known, None, "id {i}");
            assert!(!again.duplicate, "id {i} below the floor lands again");
            below += 1;
        } else {
            assert_eq!(known, Some((first, first + 1)));
            assert!(again.duplicate, "id {i} above the floor is a duplicate");
            assert_eq!(again.first, first);
            above += 1;
        }
    }
    assert!(below > 0 && above > 0);
    assert_eq!(db.head(), head + below);
    drop(db);
    let db = open_with(
        d.path(),
        opts().segment_max_bytes(300).verify_all_segments(true),
    );
    assert_eq!(db.head(), head + below);
}

#[test]
fn rebuild_restore_and_applied_increments_keep_the_ids() {
    let d = tmp();
    let src = d.path().join("src");
    let db = open_with(&src, opts().segment_max_bytes(300));
    let c1 = mixed(&db, id(1), Expected::Any).unwrap();
    let full = d.path().join("full.chronbak");
    let h1 = db.backup(&full).unwrap().head;
    let c2 = one(&db, id(2), agg(5), Expected::New, "later").unwrap();
    let inc = d.path().join("inc.chronbak");
    db.backup_since(&inc, h1).unwrap();
    drop(db);

    // Rebuild from the segments alone.
    std::fs::remove_file(src.join("index.redb")).unwrap();
    Db::rebuild_index(&src, &opts()).unwrap();
    let db = open(&src);
    assert_eq!(db.lookup_commit(id(1)).unwrap(), Some((c1.first, c1.next)));
    assert_eq!(db.lookup_commit(id(2)).unwrap(), Some((c2.first, c2.next)));
    let again = mixed(&db, id(1), Expected::Exact(0)).unwrap();
    assert!(again.duplicate);
    assert_eq!(again.appends, c1.appends);
    drop(db);

    // Restore the full backup, then apply the increment.
    let dst = d.path().join("dst");
    chron::restore_backup(&full, &dst, &opts()).unwrap();
    let r = open(&dst);
    assert_eq!(r.lookup_commit(id(1)).unwrap(), Some((c1.first, c1.next)));
    assert_eq!(r.lookup_commit(id(2)).unwrap(), None);
    let again = mixed(&r, id(1), Expected::Exact(0)).unwrap();
    assert!(again.duplicate);
    r.apply_backup(&inc).unwrap();
    assert_eq!(r.lookup_commit(id(2)).unwrap(), Some((c2.first, c2.next)));
    let later = one(&r, id(2), agg(5), Expected::New, "x").unwrap();
    assert!(later.duplicate);
    assert_eq!(later.first, c2.first);
    assert_eq!(r.head(), c2.next);
}

#[test]
fn a_write_without_commit_leaves_no_id_behind() {
    let d = tmp();
    let db = open_with(d.path(), opts().fsync(Fsync::Always));
    fill(&db, agg(1), 2);
    let mut t = db.transaction();
    t.append(agg(1), Expected::Any, vec![ev("ghost")]);
    t.commit_id(id(9));
    db.debug_write_without_commit(t).unwrap();
    drop(db);
    let db = open_with(d.path(), opts().fsync(Fsync::Always));
    assert_eq!(db.lookup_commit(id(9)).unwrap(), None);
    let c = one(&db, id(9), agg(1), Expected::Exact(2), "real").unwrap();
    assert!(!c.duplicate);
    assert_eq!(c.first, 2);
}

#[test]
fn an_id_counts_toward_the_record_limit() {
    let d = tmp();
    let max = 8 + 1 + 1 + 8 + 16 + 8 + 8 + 1 + 4 + 4 + 4; // frame + fixed + "abcd"
    let db = open_with(
        d.path(),
        Options::new().fsync(Fsync::Never).max_record_bytes(max),
    );
    let mut t = db.transaction();
    t.append_one(agg(1), Expected::Any, NewEvent::new("abcd"));
    t.commit().unwrap();
    let mut t = db.transaction();
    t.append_one(agg(1), Expected::Any, NewEvent::new("abcd"));
    t.commit_id(id(1));
    assert!(matches!(t.commit(), Err(Error::RecordTooLarge { .. })));
}
