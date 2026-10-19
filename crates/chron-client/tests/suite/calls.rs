use std::time::Duration;

use chron_client::{Client, ClientConfig, ClientError, Expected, NewEvent, Role, Transaction};
use tokio_stream::StreamExt;

use crate::common::*;

#[tokio::test(flavor = "multi_thread")]
async fn commit_is_typed_and_a_conflict_is_a_conflict() {
    let f = Fixture::start().await;
    let mut txn = Transaction::new();
    txn.append(
        agg(1),
        Expected::New,
        vec![NewEvent::new("a0").tag("t"), NewEvent::new("a1")],
    );
    txn.append_free(vec![NewEvent::new("free").tag("t")]);
    txn.append_one(agg(2), Expected::Exact(0), NewEvent::new("b0"));
    let c = f.client.commit(txn).await.unwrap();
    assert_eq!((c.first, c.last, c.next), (0, 3, 4));
    assert_eq!(c.appends[0].aggregate, Some(agg(1)));
    assert_eq!(c.appends[1].aggregate, None);
    assert_eq!(c.appends[2].new_len, 1);

    let err = f
        .client
        .append(agg(1), Expected::Exact(7), vec![NewEvent::new("x")])
        .await
        .unwrap_err();
    match err {
        ClientError::Conflict {
            aggregate,
            actual,
            expected,
            ..
        } => assert_eq!(
            (aggregate, actual, expected),
            (agg(1), 2, Some(Expected::Exact(7)))
        ),
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(f.client.head().await.unwrap(), 4);
    assert!(matches!(
        f.client.commit(Transaction::new()).await,
        Err(ClientError::InvalidArgument(_))
    ));
    assert!(matches!(
        f.client.read_all(99, 1).await,
        Err(ClientError::PositionOutOfRange {
            position: 99,
            head: 4
        })
    ));
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_both_ways_across_pages() {
    let f = Fixture::start().await;
    for i in 0..600u64 {
        let tag: &[&str] = if i % 2 == 0 { &["even"] } else { &[] };
        f.client
            .append(
                agg(1),
                Expected::Any,
                vec![NewEvent::new(format!("e{i}")).tags(tag.iter().map(|t| t.to_string()))],
            )
            .await
            .unwrap();
    }
    let all = f.client.read_aggregate(agg(1), 0, 0).await.unwrap();
    assert_eq!(all.len(), 600);
    assert_eq!(all[599].index, 599);
    assert!(all[0].is_first_in_txn() && all[0].is_last_in_txn());
    let back = f.client.read_all_backward(u64::MAX, 3).await.unwrap();
    assert_eq!(
        back.iter().map(|e| e.position).collect::<Vec<_>>(),
        [599, 598, 597]
    );
    let back = f
        .client
        .read_aggregate_backward(agg(1), 1, 5)
        .await
        .unwrap();
    assert_eq!(back.iter().map(|e| e.index).collect::<Vec<_>>(), [1, 0]);
    let even = f.client.read_tag("even", 590, 0).await.unwrap();
    assert_eq!(payloads(&even), ["e590", "e592", "e594", "e596", "e598"]);
    let even_back = f.client.read_tag_backward("even", 5, 0).await.unwrap();
    assert_eq!(payloads(&even_back), ["e4", "e2", "e0"]);
    let mut streamed = f.client.read_all_stream(0, 0).await.unwrap();
    let mut n = 0;
    while let Some(e) = streamed.next().await {
        assert_eq!(e.unwrap().position, n);
        n += 1;
    }
    assert_eq!(n, 600);
    assert_eq!(f.client.aggregate_len(agg(1)).await.unwrap(), 600);
    assert_eq!(f.client.head().await.unwrap(), 600);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribe_rebuilds_library_events() {
    let f = Fixture::start().await;
    let mut txn = Transaction::new();
    txn.append_one(agg(1), Expected::New, NewEvent::new("a").tag("t"));
    txn.append_free(vec![NewEvent::new("f")]);
    f.client.commit(txn).await.unwrap();
    let mut stream = f.client.subscribe(0).await.unwrap();
    let a = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let free = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(a.is_first_in_txn() && !a.is_last_in_txn() && a.has_tag(b"t"));
    assert!(free.is_free() && free.is_last_in_txn());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), stream.next())
            .await
            .is_err()
    );
    f.client
        .append(agg(1), Expected::Exact(1), vec![NewEvent::new("live")])
        .await
        .unwrap();
    let live = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!((live.position, live.index), (2, 1));
    let mut tagged = f.client.subscribe_tag("t", 0).await.unwrap();
    assert_eq!(tagged.next().await.unwrap().unwrap().position, 0);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn info_flush_backup_apply_retain_promote_fence_typed() {
    let f = Fixture::start_with(
        |o| o.backup_dir = Some(o.data_dir.parent().unwrap().join("backups")),
        ClientConfig::default(),
    )
    .await;
    f.client
        .append(agg(1), Expected::New, vec![NewEvent::new("one")])
        .await
        .unwrap();
    f.client.flush().await.unwrap();
    let info = f.client.info().await.unwrap();
    assert_eq!(
        (info.head, info.floor, info.segments, info.role, info.epoch),
        (1, 0, 1, Role::Leader, 0)
    );
    assert!(!info.fsync, "the fixture runs without fsync");
    assert!(!info.options.verify_all_segments);
    let meta = f.client.backup("full.chronbak", None).await.unwrap();
    assert!(f.dir.path().join("backups/default/full.chronbak").exists());
    assert_eq!(
        (meta.head, meta.format, meta.db_id),
        (1, 1, Some(info.db_id))
    );
    f.client
        .append(agg(1), Expected::Exact(1), vec![NewEvent::new("two")])
        .await
        .unwrap();
    let meta = f.client.backup("inc.chronbak", Some(1)).await.unwrap();
    assert_eq!((meta.base_head, meta.head), (Some(1), 2));
    assert!(matches!(
        f.client.apply_backup("inc.chronbak").await,
        Err(ClientError::FailedPrecondition(_))
    ));
    let r = f.client.retain(2).await.unwrap();
    assert_eq!(r.segments_removed, 0);
    assert!(matches!(
        f.client.promote().await,
        Err(ClientError::Fenced { epoch: 0, .. })
    ));
    assert!(matches!(
        f.client.fence(0).await,
        Err(ClientError::Fenced { .. })
    ));
    assert_eq!(f.client.fence(5).await.unwrap(), 0);
    let err = f
        .client
        .append(agg(1), Expected::Any, vec![NewEvent::new("late")])
        .await
        .unwrap_err();
    assert!(
        matches!(err, ClientError::Fenced { epoch: 0, .. }),
        "{err:?}"
    );
    assert_eq!(f.client.info().await.unwrap().role, Role::Fenced);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_with_epoch_checks_the_token() {
    let f = Fixture::start().await;
    f.running.as_ref().unwrap().db.set_epoch(2).unwrap();
    let mut txn = Transaction::new();
    txn.append_one(agg(1), Expected::Any, NewEvent::new("x"));
    assert!(matches!(
        f.client.commit_with_epoch(txn.clone(), 1).await,
        Err(ClientError::Fenced { epoch: 2, .. })
    ));
    f.client.commit_with_epoch(txn, 2).await.unwrap();
    assert!(f.client.health("chron.v1.Chron").await.unwrap());
    assert!(f.client.health("").await.unwrap());
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn database_config_and_rebinding_route_calls() {
    let f = Fixture::start().await;
    f.client
        .append(agg(1), Expected::New, vec![NewEvent::new("d0")])
        .await
        .unwrap();
    let created = f.client.create_database("orders").await.unwrap();
    assert_eq!((created.name.as_str(), created.head), ("orders", 0));
    assert!(matches!(
        f.client.create_database("orders").await,
        Err(ClientError::AlreadyExists(_))
    ));
    let orders = f.client.database("orders");
    assert_eq!(orders.database_name(), Some("orders"));
    assert_eq!(orders.head().await.unwrap(), 0);
    orders
        .append(agg(1), Expected::New, vec![NewEvent::new("o0")])
        .await
        .unwrap();
    assert_eq!(
        (orders.head().await.unwrap(), f.client.head().await.unwrap()),
        (1, 1)
    );
    assert_eq!(orders.info().await.unwrap().database, "orders");
    assert_eq!(f.client.info().await.unwrap().database, "default");
    let configured = chron_client::Client::connect(
        format!("http://{}", f.addr()),
        ClientConfig {
            database: Some("orders".into()),
            ..ClientConfig::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(payloads(&configured.read_all(0, 0).await.unwrap()), ["o0"]);
    let listed: Vec<String> = f
        .client
        .list_databases()
        .await
        .unwrap()
        .into_iter()
        .map(|d| d.name)
        .collect();
    assert_eq!(listed, ["default", "orders"]);
    f.client.drop_database("orders").await.unwrap();
    assert!(matches!(orders.head().await, Err(ClientError::NotFound(_))));
    assert!(matches!(
        f.client.database("Bad Name").head().await,
        Err(ClientError::InvalidArgument(_))
    ));
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_backup_to_writes_a_restorable_archive() {
    let f = Fixture::start().await;
    for i in 0..3 {
        let mut t = Transaction::new();
        t.append_free(vec![NewEvent::new(format!("e{i}"))]);
        f.client.commit(t).await.unwrap();
    }
    let path = f.dir.path().join("fetched.chronbak");
    let meta = f.client.fetch_backup_to(&path).await.unwrap();
    let info = f.client.info().await.unwrap();
    assert_eq!((meta.head, meta.db_id), (3, Some(info.db_id)));
    assert_eq!(meta.bytes, std::fs::metadata(&path).unwrap().len());
    assert!(!f.dir.path().join("fetched.chronbak.tmp").exists());
    let restored = f.dir.path().join("restored");
    chron::restore_backup(&path, &restored, &chron::Options::new()).unwrap();
    let db = chron::Db::open(&restored, chron::Options::new().fsync(chron::Fsync::Never)).unwrap();
    assert_eq!(db.head(), 3);
    // A second fetch replaces the file through its .tmp and rename.
    let again = f.client.fetch_backup_to(&path).await.unwrap();
    assert_eq!(again.head, 3);
    assert!(!f.dir.path().join("fetched.chronbak.tmp").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_commit_id_is_sent_and_a_resend_is_a_duplicate() {
    let f = Fixture::start().await;
    let txn = || {
        let mut t = Transaction::new();
        t.append(agg(1), Expected::New, vec![NewEvent::new("a0")]);
        t.append_free(vec![NewEvent::new("f")]);
        t
    };
    let options = chron_client::CommitOptions {
        commit_id: Some([2; 16]),
        ..Default::default()
    };
    let c = f.client.commit_with(txn(), options).await.unwrap();
    assert_eq!((c.first, c.next, c.duplicate), (0, 2, false));
    let again = f.client.commit_with(txn(), options).await.unwrap();
    assert_eq!((again.first, again.next, again.duplicate), (0, 2, true));
    assert_eq!(again.appends, c.appends);
    assert_eq!(f.client.head().await.unwrap(), 2);
    let events = f.client.read_all(0, 0).await.unwrap();
    assert_eq!(events[0].commit_id, Some([2; 16]));
    assert_eq!(events[1].commit_id, None);
    // A client-chosen id is on by default and visible on the record.
    let c2 = f.client.commit(txn_free("auto")).await.unwrap();
    let events = f.client.read_all(c2.first, 1).await.unwrap();
    assert!(events[0].commit_id.is_some());
    // Off: no id on the record.
    let plain = Client::connect(
        format!("http://{}", f.addr()),
        ClientConfig {
            commit_ids: false,
            ..ClientConfig::default()
        },
    )
    .await
    .unwrap();
    let c3 = plain.commit(txn_free("plain")).await.unwrap();
    let events = plain.read_all(c3.first, 1).await.unwrap();
    assert_eq!(events[0].commit_id, None);
    f.stop().await;
}

fn txn_free(payload: &str) -> Transaction {
    let mut t = Transaction::new();
    t.append_free(vec![NewEvent::new(payload.to_string())]);
    t
}
