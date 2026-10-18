use chron::{Error, Expected, MAX_TAG_BYTES, MAX_TAGS_PER_EVENT, NewEvent};

use crate::common::*;

#[test]
fn payload_at_the_limit_round_trips_and_one_more_is_refused() {
    let d = tmp();
    let max = 1024 * 1024;
    let db = open_with(d.path(), opts().max_record_bytes(max));
    // frame 8 + fixed body 43 + metadata len 4 + payload len 4
    let overhead = 8 + 43 + 4 + 4;
    let payload = vec![0xabu8; max - overhead];
    let mut t = db.transaction();
    t.append_one(agg(1), Expected::New, NewEvent::new(payload.clone()));
    t.commit().unwrap();
    let got = db.read_aggregate(agg(1), 0, 1).unwrap();
    assert_eq!(got[0].payload.as_ref(), payload.as_slice());

    let mut t = db.transaction();
    t.append_one(
        agg(1),
        Expected::Any,
        NewEvent::new(vec![0u8; max - overhead + 1]),
    );
    assert!(matches!(
        t.commit(),
        Err(Error::RecordTooLarge { size, max: m }) if size == max + 1 && m == max
    ));
    assert_eq!(db.head(), 1);
}

#[test]
fn records_over_the_coalesce_cap_read_alone() {
    let d = tmp();
    let db = open_with(d.path(), opts().max_record_bytes(4 * 1024 * 1024));
    let payloads: Vec<Vec<u8>> = (0..6u8).map(|i| vec![i; 1536 * 1024]).collect();
    for p in &payloads {
        let mut t = db.transaction();
        t.append_one(agg(1), Expected::Any, NewEvent::new(p.clone()));
        t.commit().unwrap();
    }
    let got = db.read_all(0, 10).unwrap();
    assert_eq!(got.len(), 6);
    for (ev, p) in got.iter().zip(&payloads) {
        assert_eq!(ev.payload.as_ref(), p.as_slice());
    }
    assert_eq!(
        db.read_all_backward(u64::MAX, 2).unwrap()[0]
            .payload
            .as_ref(),
        payloads[5].as_slice()
    );
}

#[test]
fn tag_limits() {
    let d = tmp();
    let db = open(d.path());
    let mut t = db.transaction();
    t.append_one(
        agg(1),
        Expected::Any,
        NewEvent::new("p").tag(vec![b'x'; MAX_TAG_BYTES + 1]),
    );
    assert!(matches!(t.commit(), Err(Error::InvalidTag(_))));
    let mut t = db.transaction();
    t.append_one(
        agg(1),
        Expected::Any,
        NewEvent::new("p").tags((0..=MAX_TAGS_PER_EVENT).map(|i| i.to_string())),
    );
    assert!(matches!(t.commit(), Err(Error::InvalidTag(_))));
    let mut t = db.transaction();
    t.append_one(
        agg(1),
        Expected::Any,
        NewEvent::new("p")
            .tag(vec![b'x'; MAX_TAG_BYTES])
            .tags((1..MAX_TAGS_PER_EVENT).map(|i| i.to_string())),
    );
    t.commit().unwrap();
    assert_eq!(db.head(), 1);
    assert_eq!(
        db.read_tag(vec![b'x'; MAX_TAG_BYTES], 0, 1).unwrap().len(),
        1
    );
}
