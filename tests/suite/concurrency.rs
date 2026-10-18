use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;

use chron::{Error, Expected};

use crate::common::*;

const THREADS: usize = 8;
const PER_THREAD: usize = 100;

#[test]
fn concurrent_any_appends_are_dense() {
    let d = tmp();
    let db = open(d.path());
    let shared = agg(1);
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                let mut positions = Vec::new();
                for i in 0..PER_THREAD {
                    let mut txn = db.transaction();
                    txn.append_one(shared, Expected::Any, ev(&format!("s{t}-{i}")));
                    txn.append_one(
                        agg(100 + t as u128),
                        Expected::Any,
                        ev(&format!("p{t}-{i}")),
                    );
                    let c = txn.commit().unwrap();
                    positions.push(c.first);
                    positions.push(c.last);
                }
                positions
            })
        })
        .collect();
    let all: BTreeSet<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    let n = (THREADS * PER_THREAD * 2) as u64;
    assert_eq!(all, (0..n).collect());
    assert_eq!(db.head(), n);
    assert_eq!(
        indices(&db.read_aggregate(shared, 0, 10_000).unwrap()),
        (0..(THREADS * PER_THREAD) as u64).collect::<Vec<_>>()
    );
    for t in 0..THREADS {
        assert_eq!(
            db.aggregate_len(agg(100 + t as u128)).unwrap(),
            PER_THREAD as u64
        );
    }
}

#[test]
fn exact_contention_has_one_winner_per_index() {
    let d = tmp();
    let db = open(d.path());
    let a = agg(1);
    let wins = Arc::new(AtomicU64::new(0));
    let losses = Arc::new(AtomicU64::new(0));
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let db = db.clone();
            let (wins, losses) = (wins.clone(), losses.clone());
            thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    let len = db.aggregate_len(a).unwrap();
                    let mut txn = db.transaction();
                    txn.append_one(a, Expected::Exact(len), ev("x"));
                    match txn.commit() {
                        Ok(c) => {
                            assert_eq!(c.appends[0].first_index, len);
                            wins.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(Error::ExpectedLength { actual, .. }) => {
                            assert!(actual > len);
                            losses.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => panic!("{e}"),
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let wins = wins.load(Ordering::Relaxed);
    assert_eq!(
        wins + losses.load(Ordering::Relaxed),
        (THREADS * PER_THREAD) as u64
    );
    assert_eq!(db.aggregate_len(a).unwrap(), wins);
    assert_eq!(db.head(), wins);
    assert_eq!(
        indices(&db.read_aggregate(a, 0, 10_000).unwrap()),
        (0..wins).collect::<Vec<_>>()
    );
}

#[test]
fn readers_alongside_a_writer_see_dense_prefixes() {
    let d = tmp();
    let db = open(d.path());
    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|r| {
            let db = db.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                let mut reads = 0;
                while !stop.load(Ordering::Relaxed) {
                    let head = db.head();
                    let all = db.read_all(0, 10_000).unwrap();
                    assert!(all.len() as u64 >= head);
                    assert_eq!(positions(&all), (0..all.len() as u64).collect::<Vec<_>>());
                    let tagged = db.read_tag("t", 0, 10_000).unwrap();
                    assert!(tagged.iter().all(|e| e.has_tag(b"t")));
                    let from = r as u64 * 7;
                    let own = db.read_aggregate(agg(1), from, 10_000).unwrap();
                    assert_eq!(
                        indices(&own),
                        (from..from + own.len() as u64).collect::<Vec<_>>()
                    );
                    reads += 1;
                }
                reads
            })
        })
        .collect();
    for i in 0..300u64 {
        let mut t = db.transaction();
        t.append_one(
            agg(1),
            Expected::Exact(i),
            tagged(&format!("e{i}"), if i % 2 == 0 { &["t"] } else { &[] }),
        );
        t.commit().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        assert!(r.join().unwrap() > 0);
    }
    assert_eq!(db.read_tag("t", 0, 10_000).unwrap().len(), 150);
}

#[test]
fn parallel_aggregate_reads_at_different_offsets() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 200);
    let handles: Vec<_> = (0..8u64)
        .map(|r| {
            let db = db.clone();
            thread::spawn(move || {
                for _ in 0..50 {
                    let from = r * 13;
                    let got = db.read_aggregate(agg(1), from, 25).unwrap();
                    assert_eq!(indices(&got), (from..from + 25).collect::<Vec<_>>());
                    assert_eq!(payloads(&got)[0], format!("e{from}"));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}
