//! A model of the public semantics, checked against random transaction
//! sequences, reopens and retentions.

use std::collections::HashMap;

use bytes::Bytes;
use chron::{Db, Error, Expected, NewEvent, Uuid};
use proptest::prelude::*;

use crate::common::*;

const TAGS: [&str; 3] = ["a", "b", "c"];

#[derive(Debug, Clone)]
enum Exp {
    Any,
    New,
    Exact(u8),
    /// Resolved to the aggregate's length at that point in the transaction.
    Current,
}

#[derive(Debug, Clone)]
struct Append {
    aggregate: Option<u8>,
    expected: Exp,
    events: Vec<(u8, Vec<u8>)>,
}

#[derive(Debug, Clone)]
enum Op {
    Txn(Vec<Append>),
    Reopen,
    /// Retain from this percentage of the head.
    Retain(u8),
    /// Resend the transaction op `k` sent, with its id and the
    /// expectations it carried then. A no-op when op `k` lies ahead or
    /// is not a transaction.
    Resend(u8),
    /// A crash: the index file as it is on disk while open (at its last
    /// durable commit) is put back after the close, the segments hold
    /// everything, open replays.
    Crash,
}

fn arb_append() -> impl Strategy<Value = Append> {
    let aggregate = prop_oneof![1 => Just(None), 4 => (0..4u8).prop_map(Some)];
    let expected = prop_oneof![
        Just(Exp::Any),
        Just(Exp::New),
        (0..8u8).prop_map(Exp::Exact),
        Just(Exp::Current),
    ];
    let events = prop::collection::vec((0..16u8, prop::collection::vec(0..3u8, 0..=2)), 1..=3);
    (aggregate, expected, events).prop_map(|(aggregate, expected, events)| Append {
        aggregate,
        expected,
        events,
    })
}

fn arb_ops() -> impl Strategy<Value = Vec<Op>> {
    let op = prop_oneof![
        10 => prop::collection::vec(arb_append(), 1..=3).prop_map(Op::Txn),
        1 => Just(Op::Reopen),
        1 => Just(Op::Crash),
        1 => (0..=100u8).prop_map(Op::Retain),
        2 => (0..40u8).prop_map(Op::Resend),
    ];
    prop::collection::vec(op, 1..=40)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Projected {
    position: u64,
    aggregate: Option<Uuid>,
    index: u64,
    tags: Vec<Bytes>,
    payload: String,
    first: bool,
    last: bool,
}

fn project(ev: &chron::Event) -> Projected {
    Projected {
        position: ev.position,
        aggregate: ev.aggregate,
        index: ev.index,
        tags: ev.tags.clone(),
        payload: String::from_utf8(ev.payload.to_vec()).unwrap(),
        first: ev.is_first_in_txn(),
        last: ev.is_last_in_txn(),
    }
}

#[derive(Default)]
struct Model {
    events: Vec<Projected>,
    lens: HashMap<Uuid, u64>,
    floor: u64,
}

fn id(n: u8) -> Uuid {
    Uuid::from_u128(1000 + n as u128)
}

fn new_event(payload: u8, tags: &[u8]) -> NewEvent {
    NewEvent::new(format!("p{payload}")).tags(tags.iter().map(|t| TAGS[*t as usize].to_string()))
}

/// An append with `Current` resolved: what is sent to the database.
#[derive(Debug, Clone)]
struct Resolved {
    aggregate: Option<u8>,
    expected: Expected,
    events: Vec<(u8, Vec<u8>)>,
}

impl Model {
    /// Resolves `Current` against the model's running lengths, so the
    /// database and the model are asked the same question.
    fn resolve(&self, appends: &[Append]) -> Vec<Resolved> {
        let mut lens = self.lens.clone();
        appends
            .iter()
            .map(|a| {
                let expected = match a.aggregate {
                    None => Expected::Any,
                    Some(n) => {
                        let cur = lens.get(&id(n)).copied().unwrap_or(0);
                        lens.insert(id(n), cur + a.events.len() as u64);
                        real_expected(&a.expected, cur)
                    }
                };
                Resolved {
                    aggregate: a.aggregate,
                    expected,
                    events: a.events.clone(),
                }
            })
            .collect()
    }

    /// Predicts a transaction's outcome, and applies it when it commits.
    fn txn(&mut self, appends: &[Resolved]) -> Result<(u64, u64), (Uuid, Expected, u64)> {
        let mut lens = self.lens.clone();
        let mut planned: Vec<Projected> = Vec::new();
        let total: usize = appends.iter().map(|a| a.events.len()).sum();
        let mut position = self.events.len() as u64;
        let first = position;
        for a in appends {
            let aggregate = a.aggregate.map(id);
            let mut index = 0;
            if let Some(agg) = aggregate {
                let cur = lens.get(&agg).copied().unwrap_or(0);
                let expected = a.expected;
                let ok = match expected {
                    Expected::Any => true,
                    Expected::New => cur == 0,
                    Expected::Exact(n) => cur == n,
                };
                if !ok {
                    return Err((agg, expected, cur));
                }
                index = cur;
                lens.insert(agg, cur + a.events.len() as u64);
            }
            for (j, (payload, tags)) in a.events.iter().enumerate() {
                let mut tags: Vec<Bytes> = tags
                    .iter()
                    .map(|t| Bytes::from(TAGS[*t as usize]))
                    .collect();
                tags.sort();
                tags.dedup();
                let seq = planned.len();
                planned.push(Projected {
                    position,
                    aggregate,
                    index: if aggregate.is_some() {
                        index + j as u64
                    } else {
                        0
                    },
                    tags,
                    payload: format!("p{payload}"),
                    first: seq == 0,
                    last: seq + 1 == total,
                });
                position += 1;
            }
        }
        self.events.extend(planned);
        self.lens = lens;
        Ok((first, position - 1))
    }

    fn visible(&self) -> impl Iterator<Item = &Projected> {
        self.events.iter().filter(move |e| e.position >= self.floor)
    }
}

/// The expectation of a real transaction, built from the model's
/// resolution so `Current` means the same thing on both sides.
fn real_expected(e: &Exp, cur: u64) -> Expected {
    match e {
        Exp::Any => Expected::Any,
        Exp::New => Expected::New,
        Exp::Exact(n) => Expected::Exact(*n as u64),
        Exp::Current => Expected::Exact(cur),
    }
}

fn check(db: &Db, model: &Model) -> Result<(), TestCaseError> {
    prop_assert_eq!(db.head(), model.events.len() as u64);
    prop_assert_eq!(db.floor(), model.floor);
    let visible: Vec<Projected> = model.visible().cloned().collect();
    let all: Vec<Projected> = db
        .read_all(model.floor, usize::MAX)
        .unwrap()
        .iter()
        .map(project)
        .collect();
    prop_assert_eq!(&all, &visible);
    let mut back: Vec<Projected> = db
        .read_all_backward(u64::MAX, usize::MAX)
        .unwrap()
        .iter()
        .map(project)
        .collect();
    back.reverse();
    prop_assert_eq!(&back, &visible);
    for (k, e) in visible.iter().enumerate().step_by(7) {
        let one = db.read_all(e.position, 1).unwrap();
        prop_assert_eq!(project(&one[0]), visible[k].clone());
    }
    for t in TAGS {
        let want: Vec<Projected> = visible
            .iter()
            .filter(|e| e.tags.iter().any(|x| x.as_ref() == t.as_bytes()))
            .cloned()
            .collect();
        let got: Vec<Projected> = db
            .read_tag(t, 0, usize::MAX)
            .unwrap()
            .iter()
            .map(project)
            .collect();
        prop_assert_eq!(&got, &want);
        let mut back: Vec<Projected> = db
            .read_tag_backward(t, u64::MAX, usize::MAX)
            .unwrap()
            .iter()
            .map(project)
            .collect();
        back.reverse();
        prop_assert_eq!(&back, &want);
    }
    for n in 0..4u8 {
        let agg = id(n);
        let want: Vec<Projected> = visible
            .iter()
            .filter(|e| e.aggregate == Some(agg))
            .cloned()
            .collect();
        let got: Vec<Projected> = db
            .read_aggregate(agg, 0, usize::MAX)
            .unwrap()
            .iter()
            .map(project)
            .collect();
        prop_assert_eq!(&got, &want);
        let mut back: Vec<Projected> = db
            .read_aggregate_backward(agg, u64::MAX, usize::MAX)
            .unwrap()
            .iter()
            .map(project)
            .collect();
        back.reverse();
        prop_assert_eq!(&back, &want);
        prop_assert_eq!(
            db.aggregate_len(agg).unwrap(),
            model.lens.get(&agg).copied().unwrap_or(0)
        );
    }
    if model.floor > 0 {
        let below = matches!(
            db.read_all(model.floor - 1, 1),
            Err(Error::BelowFloor { .. })
        );
        prop_assert!(below, "a read below the floor is BelowFloor");
    }
    Ok(())
}

fn segment_bases(dir: &std::path::Path) -> Vec<u64> {
    segments(dir)
        .iter()
        .map(|p| p.file_stem().unwrap().to_str().unwrap().parse().unwrap())
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, .. ProptestConfig::default() })]

    #[test]
    fn the_database_matches_the_model(ops in arb_ops()) {
        let d = tmp();
        let o = opts().segment_max_bytes(300);
        let mut db = Some(Db::open(d.path(), o.clone()).unwrap());
        let mut model = Model::default();
        // Per op: what was sent (resolved appends) and where it landed.
        let mut sent: Vec<Option<Sent>> = Vec::new();
        for (k, op) in ops.iter().enumerate() {
            let handle = db.as_ref().unwrap();
            let mut record = None;
            match op {
                Op::Txn(appends) => {
                    let resolved = model.resolve(appends);
                    let mut txn = handle.transaction();
                    send(&mut txn, &resolved);
                    txn.commit_id(commit_id(k));
                    let predicted = model.txn(&resolved);
                    let landed = match (txn.commit(), predicted) {
                        (Ok(c), Ok((first, last))) => {
                            prop_assert_eq!((c.first, c.last, c.next, c.duplicate), (first, last, last + 1, false));
                            Some((first, last))
                        }
                        (Err(Error::ExpectedLength { aggregate, expected, actual }), Err((a, e, cur))) => {
                            prop_assert_eq!((aggregate, expected, actual), (a, e, cur));
                            None
                        }
                        (got, want) => { prop_assert!(false, "db {:?} vs model {:?}", got.map(|c| c.next), want); None }
                    };
                    record = Some((resolved, landed));
                }
                Op::Resend(j) => {
                    let j = *j as usize;
                    if let Some(Some((resolved, landed))) = sent.get(j).cloned() {
                        let mut txn = handle.transaction();
                        send(&mut txn, &resolved);
                        txn.commit_id(commit_id(j));
                        let kept = landed.filter(|(first, _)| *first >= model.floor);
                        match kept {
                            // Landed and still kept: a duplicate, nothing new.
                            Some((first, last)) => {
                                let c = txn.commit().unwrap();
                                prop_assert_eq!((c.first, c.last, c.duplicate), (first, last, true));
                            }
                            // Never landed, or forgotten below the floor: a
                            // fresh transaction with the old expectations;
                            // once it lands, the id is landed for the next
                            // resend.
                            None => {
                                let predicted = model.txn(&resolved);
                                match (txn.commit(), predicted) {
                                    (Ok(c), Ok((first, last))) => {
                                        prop_assert_eq!((c.first, c.last, c.duplicate), (first, last, false));
                                        sent[j] = Some((resolved, Some((first, last))));
                                    }
                                    (Err(Error::ExpectedLength { aggregate, expected, actual }), Err((a, e, cur))) => {
                                        prop_assert_eq!((aggregate, expected, actual), (a, e, cur));
                                    }
                                    (got, want) => prop_assert!(false, "resend: db {:?} vs model {:?}", got.map(|c| c.next), want),
                                }
                            }
                        }
                    }
                }
                Op::Reopen => {
                    drop(db.take());
                    db = Some(Db::open(d.path(), o.clone()).unwrap());
                }
                Op::Crash => {
                    let image = snapshot_index(d.path());
                    drop(db.take());
                    restore_index(d.path(), &image);
                    db = Some(Db::open(d.path(), o.clone()).unwrap());
                }
                Op::Retain(pct) => {
                    let head = handle.head();
                    let target = head * (*pct as u64) / 100;
                    let want = segment_bases(d.path()).into_iter().filter(|b| *b <= target).max().unwrap_or(0).max(model.floor);
                    let r = handle.retain_from(target).unwrap();
                    prop_assert_eq!(r.floor, want);
                    model.floor = want;
                }
            }
            sent.push(record);
            check(db.as_ref().unwrap(), &model)?;
        }
    }
}

/// What a transaction op sent, and where it landed if it did.
type Sent = (Vec<Resolved>, Option<(u64, u64)>);

fn commit_id(k: usize) -> [u8; 16] {
    (k as u128 + 1).to_be_bytes()
}

fn send(txn: &mut chron::Transaction, resolved: &[Resolved]) {
    for a in resolved {
        let events: Vec<NewEvent> = a.events.iter().map(|(p, t)| new_event(*p, t)).collect();
        match a.aggregate {
            None => {
                txn.append_free(events);
            }
            Some(n) => {
                txn.append(id(n), a.expected, events);
            }
        }
    }
}
