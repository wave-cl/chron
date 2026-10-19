//! Group commit: concurrent transactions land together, each still
//! checked and acknowledged on its own.

use std::time::Duration;

use chron::{Db, Error, Expected, Fsync, Options, Stats, Uuid};

use crate::common::*;

fn always() -> Options {
    opts().fsync(Fsync::Always)
}

fn wait_pending(db: &Db, n: usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while db.debug_pending_commits() < n {
        assert!(
            std::time::Instant::now() < deadline,
            "waiting for {n} queued commits"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn delta(after: Stats, before: Stats) -> (u64, u64, u64, u64) {
    (
        after.batches - before.batches,
        after.transactions - before.transactions,
        after.events - before.events,
        after.fsyncs - before.fsyncs,
    )
}

fn commit_one(
    db: &Db,
    id: Uuid,
    expected: Expected,
    payload: &str,
) -> chron::Result<chron::Committed> {
    let mut t = db.transaction();
    t.append_one(id, expected, ev(payload));
    t.commit()
}

#[test]
fn contended_commits_share_fsyncs() {
    const THREADS: u64 = 8;
    const PER_THREAD: usize = 100;
    let d = tmp();
    let db = open_with(d.path(), always());
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let db = db.clone();
            s.spawn(move || {
                for i in 0..PER_THREAD {
                    commit_one(
                        &db,
                        agg(u128::from(t) + 1),
                        Expected::Any,
                        &format!("t{t}e{i}"),
                    )
                    .unwrap();
                }
            });
        }
    });
    let st = db.stats();
    assert_eq!((st.transactions, st.events), (800, 800));
    assert_eq!(st.batches, st.fsyncs, "one fsync per batch");
    assert!(
        st.fsyncs < st.transactions,
        "contended commits were grouped: {} fsyncs for {} transactions",
        st.fsyncs,
        st.transactions
    );
    assert!(st.fsync_nanos > 0);
    assert_eq!(db.head(), 800);
    let all = db.read_all(0, 0).unwrap();
    assert_eq!(positions(&all), (0..800).collect::<Vec<_>>());
    for t in 0..THREADS {
        assert_eq!(db.aggregate_len(agg(u128::from(t) + 1)).unwrap(), 100);
    }
}

#[test]
fn batching_disabled_syncs_every_commit() {
    let d = tmp();
    let db = open_with(d.path(), always().group_commit_max_txns(1));
    std::thread::scope(|s| {
        for t in 0..8u64 {
            let db = db.clone();
            s.spawn(move || {
                for i in 0..100 {
                    commit_one(
                        &db,
                        agg(u128::from(t) + 1),
                        Expected::Any,
                        &format!("t{t}e{i}"),
                    )
                    .unwrap();
                }
            });
        }
    });
    let st = db.stats();
    assert_eq!((st.transactions, st.batches, st.fsyncs), (800, 800, 800));
}

#[test]
fn a_failed_expectation_in_the_middle_lands_the_others() {
    let d = tmp();
    let db = open_with(d.path(), always());
    let before = db.stats();
    let a = agg(1);
    let (t0, t1, t2) = {
        let hold = db.debug_hold_writer();
        let db0 = db.clone();
        let t0 = std::thread::spawn(move || commit_one(&db0, a, Expected::Exact(0), "first"));
        wait_pending(&db, 1);
        let db1 = db.clone();
        let t1 = std::thread::spawn(move || commit_one(&db1, a, Expected::Exact(5), "wrong"));
        wait_pending(&db, 2);
        let db2 = db.clone();
        let t2 = std::thread::spawn(move || commit_one(&db2, a, Expected::Exact(1), "second"));
        wait_pending(&db, 3);
        drop(hold);
        (t0, t1, t2)
    };
    let r0 = t0.join().unwrap().unwrap();
    assert_eq!(r0.first, 0);
    match t1.join().unwrap() {
        Err(Error::ExpectedLength {
            aggregate,
            expected,
            actual,
        }) => assert_eq!((aggregate, expected, actual), (a, Expected::Exact(5), 1)),
        other => panic!("expected a refused expectation, got {other:?}"),
    }
    let r2 = t2.join().unwrap().unwrap();
    assert_eq!((r2.first, r2.appends[0].first_index), (1, 1));
    assert_eq!(db.head(), 2);
    assert_eq!(delta(db.stats(), before), (1, 2, 2, 1), "one batch of two");
    assert_eq!(indices(&db.read_aggregate(a, 0, 0).unwrap()), [0, 1]);
    assert_eq!(payloads(&db.read_all(0, 0).unwrap()), ["first", "second"]);
}

#[test]
fn the_fault_hook_fails_the_whole_batch_and_leaves_nothing_behind() {
    let d = tmp();
    let db = open_with(d.path(), always());
    fill(&db, agg(1), 2);
    let before = db.stats();
    let len_before = file_len(&last_segment(d.path()));
    let results: Vec<chron::Result<chron::Committed>> = {
        let hold = db.debug_hold_writer();
        let handles: Vec<_> = (0..8u64)
            .map(|t| {
                let dbc = db.clone();
                let h = std::thread::spawn(move || {
                    commit_one(
                        &dbc,
                        agg(10 + u128::from(t)),
                        Expected::Any,
                        &"g".repeat(100),
                    )
                });
                wait_pending(&db, t as usize + 1);
                h
            })
            .collect();
        db.debug_fail_next_commit();
        drop(hold);
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    };
    for r in &results {
        assert!(matches!(r, Err(Error::Io { op: "commit", .. })), "{r:?}");
    }
    assert_eq!(db.head(), 2);
    assert_eq!(delta(db.stats(), before).0, 0, "no batch landed");
    assert_eq!(
        file_len(&last_segment(d.path())),
        len_before,
        "no leftovers"
    );
    fill(&db, agg(1), 30);
    let all = db.read_all(0, 0).unwrap();
    assert_eq!(all.len(), 32);
    assert!(payloads(&all).iter().all(|p| !p.starts_with("ggg")));
    drop(db);
    let db = open_with(d.path(), always().verify_all_segments(true));
    assert_eq!(db.head(), 32);
}

#[test]
fn an_uncontended_commit_is_one_batch() {
    for (fsync, want_fsyncs) in [(Fsync::Always, 1), (Fsync::Never, 0)] {
        let d = tmp();
        let db = open_with(d.path(), opts().fsync(fsync));
        for i in 0..3u64 {
            let before = db.stats();
            commit_one(&db, agg(1), Expected::Any, "x").unwrap();
            assert_eq!(
                delta(db.stats(), before),
                (1, 1, 1, want_fsyncs),
                "commit {i}"
            );
        }
    }
}

#[test]
fn caps_split_a_long_queue() {
    for (opts, queued, want_batches) in [
        (always().group_commit_max_txns(2), 5, 3),
        (always().group_commit_max_bytes(1), 5, 5),
        (always(), 5, 1),
    ] {
        let d = tmp();
        let db = open_with(d.path(), opts);
        let before = db.stats();
        let handles = {
            let hold = db.debug_hold_writer();
            let handles: Vec<_> = (0..queued)
                .map(|i| {
                    let dbc = db.clone();
                    let h = std::thread::spawn(move || {
                        commit_one(&dbc, agg(1 + i as u128), Expected::Any, "x")
                    });
                    wait_pending(&db, i + 1);
                    h
                })
                .collect();
            drop(hold);
            handles
        };
        for h in handles {
            h.join().unwrap().unwrap();
        }
        assert_eq!(
            delta(db.stats(), before),
            (want_batches, 5, 5, want_batches)
        );
    }
}

#[test]
fn a_batch_stops_where_the_segment_is_full() {
    let d = tmp();
    let db = open_with(d.path(), always().segment_max_bytes(300));
    let handles = {
        let hold = db.debug_hold_writer();
        let handles: Vec<_> = (0..20u64)
            .map(|i| {
                let dbc = db.clone();
                let h = std::thread::spawn(move || {
                    let mut t = dbc.transaction();
                    t.append(
                        agg(1),
                        Expected::Any,
                        vec![ev("aaaaaaaaaa"), ev("bbbbbbbbbb"), ev("cccccccccc")],
                    );
                    t.commit()
                });
                wait_pending(&db, i as usize + 1);
                h
            })
            .collect();
        drop(hold);
        handles
    };
    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(db.head(), 60);
    assert_eq!(
        positions(&db.read_all(0, 0).unwrap()),
        (0..60).collect::<Vec<_>>()
    );
    let segs = segments(d.path());
    assert!(
        segs.len() >= 3,
        "the queue spanned several segments: {segs:?}"
    );
    // One three-event transaction is well under 300 bytes, so a segment
    // may exceed the limit by that much and no more.
    for s in &segs {
        assert!(
            file_len(s) < 300 + 300,
            "{} is {} bytes: a batch ran past the segment limit",
            s.display(),
            file_len(s)
        );
    }
    drop(db);
    let db = open_with(
        d.path(),
        always().segment_max_bytes(300).verify_all_segments(true),
    );
    assert_eq!(db.head(), 60);
}

#[test]
fn flags_and_recorded_at_are_per_transaction_inside_a_batch() {
    let d = tmp();
    let db = open_with(d.path(), always());
    let sub_db = db.clone();
    let subscriber = std::thread::spawn(move || {
        sub_db
            .subscribe(0)
            .take(6)
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
    });
    let handles = {
        let hold = db.debug_hold_writer();
        let handles: Vec<_> = (0..3u64)
            .map(|i| {
                let dbc = db.clone();
                let h = std::thread::spawn(move || {
                    let mut t = dbc.transaction();
                    t.append(agg(1 + i as u128), Expected::Any, vec![ev("a"), ev("b")]);
                    t.commit()
                });
                wait_pending(&db, i as usize + 1);
                h
            })
            .collect();
        drop(hold);
        handles
    };
    for h in handles {
        h.join().unwrap().unwrap();
    }
    let got = subscriber.join().unwrap();
    let flags: Vec<(bool, bool)> = got
        .iter()
        .map(|e| (e.is_first_in_txn(), e.is_last_in_txn()))
        .collect();
    assert_eq!(
        flags,
        [
            (true, false),
            (false, true),
            (true, false),
            (false, true),
            (true, false),
            (false, true)
        ]
    );
    for pair in got.chunks(2) {
        assert_eq!(pair[0].recorded_at, pair[1].recorded_at);
    }
    assert_eq!(db.stats().batches, 1);
}

#[test]
fn checkpoints_every_n_batches() {
    let d = tmp();
    let db = open_with(
        d.path(),
        always()
            .index_checkpoint_batches(4)
            .index_checkpoint_interval(Duration::from_secs(3600)),
    );
    for i in 0..10 {
        commit_one(&db, agg(1), Expected::Any, &format!("c{i}")).unwrap();
    }
    let st = db.stats();
    assert_eq!(
        (st.batches, st.fsyncs),
        (10, 10),
        "one segment sync per batch"
    );
    assert_eq!(st.index_checkpoints, 2, "the 4th and 8th batch");
    let total: u64 = st.fsync_buckets.iter().sum();
    assert_eq!(total, st.fsyncs, "every sync is in a bucket");
    // The interval forces one too.
    let db2 = open_with(
        tmp().path(),
        always()
            .index_checkpoint_batches(usize::MAX)
            .index_checkpoint_interval(Duration::from_millis(50)),
    );
    commit_one(&db2, agg(1), Expected::Any, "a").unwrap();
    std::thread::sleep(Duration::from_millis(80));
    commit_one(&db2, agg(1), Expected::Any, "b").unwrap();
    assert_eq!(db2.stats().index_checkpoints, 1);
}

#[test]
fn checkpoint_batches_1_is_every_batch() {
    let d = tmp();
    let db = open_with(d.path(), always().index_checkpoint_batches(1));
    for i in 0..5 {
        commit_one(&db, agg(1), Expected::Any, &format!("c{i}")).unwrap();
    }
    let st = db.stats();
    assert_eq!(st.index_checkpoints, st.batches);
    assert_eq!(st.batches, 5);
}
