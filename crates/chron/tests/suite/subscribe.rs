use std::thread;
use std::time::{Duration, Instant};

use chron::{Event, Expected};

use crate::common::*;

const LIVE: Duration = Duration::from_secs(10);
const QUIET: Duration = Duration::from_millis(50);

fn take(sub: &mut chron::Subscription, n: usize) -> Vec<Event> {
    (0..n)
        .map(|_| {
            sub.next_timeout(LIVE)
                .unwrap()
                .expect("an event within the live bound")
        })
        .collect()
}

#[test]
fn from_zero_sees_existing_then_live_in_order() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 5);
    let writer = db.clone();
    let h = thread::spawn(move || {
        thread::sleep(QUIET);
        for i in 5..10 {
            let mut t = writer.transaction();
            t.append_one(agg(1), Expected::Exact(i), ev(&format!("e{i}")));
            t.commit().unwrap();
        }
    });
    let got: Vec<Event> = db.subscribe(0).take(10).map(|r| r.unwrap()).collect();
    h.join().unwrap();
    assert_eq!(positions(&got), (0..10).collect::<Vec<_>>());
    assert_eq!(
        payloads(&got),
        (0..10).map(|i| format!("e{i}")).collect::<Vec<_>>()
    );
}

#[test]
fn from_head_sees_only_new() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 3);
    let mut sub = db.subscribe(db.head());
    assert_eq!(sub.position(), 3);
    assert!(sub.next_timeout(QUIET).unwrap().is_none());
    fill(&db, agg(1), 1);
    let got = sub.next_timeout(LIVE).unwrap().unwrap();
    assert_eq!(got.position, 3);
    assert_eq!(sub.position(), 4);
}

#[test]
fn past_head_waits_for_that_position() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 2);
    let mut sub = db.subscribe(db.head() + 2);
    assert!(sub.next_timeout(QUIET).unwrap().is_none());
    fill(&db, agg(1), 3);
    assert_eq!(sub.next_timeout(LIVE).unwrap().unwrap().position, 4);
}

#[test]
fn tag_subscription_filters_and_checkpoints() {
    let d = tmp();
    let db = open(d.path());
    for i in 0..6u64 {
        let mut t = db.transaction();
        let tags: &[&str] = if i % 2 == 0 { &["t"] } else { &[] };
        t.append_one(agg(1), Expected::Any, tagged(&format!("e{i}"), tags));
        t.commit().unwrap();
    }
    let mut sub = db.subscribe_tag("t", 0);
    let got = take(&mut sub, 3);
    assert_eq!(positions(&got), [0, 2, 4]);
    assert!(got.iter().all(|e| e.has_tag(b"t")));
    assert!(sub.next_timeout(QUIET).unwrap().is_none());
    assert_eq!(
        sub.position(),
        db.head(),
        "a drained tag subscription sits at the head"
    );

    let mut t = db.transaction();
    t.append_one(agg(1), Expected::Any, ev("untagged"));
    t.commit().unwrap();
    assert!(sub.next_timeout(QUIET).unwrap().is_none());
    assert_eq!(sub.position(), db.head());

    let mut t = db.transaction();
    t.append_free(vec![tagged("free-tagged", &["t"])]);
    t.commit().unwrap();
    let got = sub.next_timeout(LIVE).unwrap().unwrap();
    assert_eq!(got.position, 7);
    assert!(got.is_free());

    let mut resumed = db.subscribe_tag("t", sub.position());
    assert!(resumed.next_timeout(QUIET).unwrap().is_none());
}

#[test]
fn next_timeout_returns_none_when_quiet() {
    let d = tmp();
    let db = open(d.path());
    let mut sub = db.subscribe(0);
    let started = Instant::now();
    assert!(
        sub.next_timeout(Duration::from_millis(100))
            .unwrap()
            .is_none()
    );
    assert!(started.elapsed() >= Duration::from_millis(100));
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[test]
fn subscriber_thread_sees_a_transaction_contiguously() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(9), 1);
    let reader = db.clone();
    let h = thread::spawn(move || {
        let mut sub = reader.subscribe(1);
        take(&mut sub, 6)
    });
    thread::sleep(QUIET);
    let mut t = db.transaction();
    t.append(agg(1), Expected::New, vec![ev("a0"), ev("a1")]);
    t.append_one(agg(2), Expected::New, ev("b0"));
    t.append(agg(3), Expected::New, vec![ev("c0"), ev("c1"), ev("c2")]);
    t.commit().unwrap();
    let got = h.join().unwrap();
    assert_eq!(positions(&got), [1, 2, 3, 4, 5, 6]);
    let firsts: Vec<bool> = got.iter().map(|e| e.is_first_in_txn()).collect();
    let lasts: Vec<bool> = got.iter().map(|e| e.is_last_in_txn()).collect();
    assert_eq!(firsts, [true, false, false, false, false, false]);
    assert_eq!(lasts, [false, false, false, false, false, true]);
    assert!(got.iter().all(|e| e.recorded_at == got[0].recorded_at));
}

#[test]
fn subscription_across_a_segment_roll() {
    let d = tmp();
    let db = open_with(d.path(), opts().segment_max_bytes(200));
    let reader = db.clone();
    let h = thread::spawn(move || {
        let mut sub = reader.subscribe(0).batch(7);
        take(&mut sub, 60)
    });
    for i in 0..60u64 {
        let mut t = db.transaction();
        t.append_one(agg(1), Expected::Exact(i), ev(&format!("e{i}")));
        t.commit().unwrap();
    }
    let got = h.join().unwrap();
    assert_eq!(positions(&got), (0..60).collect::<Vec<_>>());
    assert!(segments(d.path()).len() > 3);
}

#[test]
fn small_batches_stay_dense_and_position_counts_the_buffer() {
    let d = tmp();
    let db = open(d.path());
    fill(&db, agg(1), 10);
    let got: Vec<Event> = db
        .subscribe(0)
        .batch(3)
        .take(10)
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(positions(&got), (0..10).collect::<Vec<_>>());

    let mut sub = db.subscribe(0).batch(4);
    assert_eq!(sub.position(), 0);
    assert_eq!(sub.next_timeout(LIVE).unwrap().unwrap().position, 0);
    assert_eq!(sub.position(), 1, "the buffered next event, not the cursor");
    let rest = take(&mut sub, 9);
    assert_eq!(positions(&rest), (1..10).collect::<Vec<_>>());
    assert_eq!(sub.position(), 10);
    assert_eq!(
        db.subscribe(0)
            .batch(0)
            .next_timeout(LIVE)
            .unwrap()
            .unwrap()
            .position,
        0
    );
}
