//! Several named databases per daemon: routing by the `chron-db`
//! metadata, create/list/drop, isolation, backups, migration, promotion
//! and replication across databases.

use std::time::Duration;

use chron_proto::v1 as pb;
use tokio_stream::StreamExt;
use tonic::Code;

use crate::common::*;

async fn commit_in(c: &mut DbClient, payload: &str) -> u64 {
    c.commit(pb::CommitRequest {
        appends: vec![append(
            Some(agg(1)),
            any(),
            vec![new_event(payload, &["t"])],
        )],
        epoch: None,
        sync: None,
    })
    .await
    .unwrap()
    .into_inner()
    .first
}

async fn head_in(c: &mut DbClient) -> u64 {
    c.head(pb::HeadRequest {}).await.unwrap().into_inner().head
}

async fn create(s: &mut Server, name: &str) -> pb::DatabaseInfo {
    s.client
        .create_database(pb::CreateDatabaseRequest { name: name.into() })
        .await
        .unwrap()
        .into_inner()
        .database
        .unwrap()
}

async fn names(s: &mut Server) -> Vec<String> {
    s.client
        .list_databases(pb::ListDatabasesRequest {})
        .await
        .unwrap()
        .into_inner()
        .databases
        .into_iter()
        .map(|d| d.name)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_header_routes_and_absent_means_default() {
    let mut s = Server::start().await;
    let addr = s.running.as_ref().unwrap().local_addr;
    // A 0.1.0-style client sends no header.
    fill(&mut s.client, agg(1), 2).await;
    let mut explicit = with_db(plain_channel(addr).await, "default");
    assert_eq!(head_in(&mut explicit).await, 2);
    commit_in(&mut explicit, "third").await;
    assert_eq!(
        read_all_payloads(&mut s.client).await,
        ["e0", "e1", "third"],
        "both ways land in the same database"
    );
    let info = s.info().await;
    assert_eq!(info.database, "default");
    assert_eq!(names(&mut s).await, ["default"]);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_and_bad_names_are_refused() {
    let mut s = Server::start().await;
    let addr = s.running.as_ref().unwrap().local_addr;
    let mut orders = with_db(plain_channel(addr).await, "orders");
    let err = orders.head(pb::HeadRequest {}).await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    assert!(
        err.message().contains("chron db create orders"),
        "{}",
        err.message()
    );
    for bad in ["Orders", "a/b", "..", "", "déf", &"a".repeat(65), "a b"] {
        let mut c = with_db(plain_channel(addr).await, bad);
        let err = c.head(pb::HeadRequest {}).await.unwrap_err();
        assert_eq!(
            err.code(),
            Code::InvalidArgument,
            "{bad:?}: {}",
            err.message()
        );
        let err = s
            .client
            .create_database(pb::CreateDatabaseRequest { name: bad.into() })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "create {bad:?}");
    }
    // Control: once created, the same header is served.
    create(&mut s, "orders").await;
    assert_eq!(head_in(&mut orders).await, 0);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn create_list_drop_over_rpc() {
    let mut s = Server::start().await;
    let created = create(&mut s, "orders").await;
    assert_eq!(
        (created.name.as_str(), created.head, created.epoch),
        ("orders", 0, 0)
    );
    assert!(s.data_dir().join("orders/index.redb").exists());
    assert_eq!(names(&mut s).await, ["default", "orders"]);
    let err = s
        .client
        .create_database(pb::CreateDatabaseRequest {
            name: "orders".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
    let addr = s.running.as_ref().unwrap().local_addr;
    let mut orders = with_db(plain_channel(addr).await, "orders");
    commit_in(&mut orders, "o1").await;
    let old_id = created.db_id.clone();

    s.client
        .drop_database(pb::DropDatabaseRequest {
            name: "orders".into(),
        })
        .await
        .unwrap();
    assert!(
        !s.data_dir().join("orders").exists(),
        "the directory is gone"
    );
    assert_eq!(names(&mut s).await, ["default"]);
    assert_eq!(
        orders.head(pb::HeadRequest {}).await.unwrap_err().code(),
        Code::NotFound
    );
    let err = s
        .client
        .drop_database(pb::DropDatabaseRequest {
            name: "orders".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    let again = create(&mut s, "orders").await;
    assert_eq!(again.head, 0, "recreated empty");
    assert_ne!(again.db_id, old_id, "with a fresh identity");
    let err = s
        .client
        .drop_database(pb::DropDatabaseRequest {
            name: "default".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn databases_are_isolated_and_dropping_ends_only_its_streams() {
    let mut s = Server::start().await;
    create(&mut s, "orders").await;
    let addr = s.running.as_ref().unwrap().local_addr;
    let mut orders = with_db(plain_channel(addr).await, "orders");
    fill(&mut s.client, agg(1), 3).await;
    assert_eq!(
        commit_in(&mut orders, "o0").await,
        0,
        "positions restart per database"
    );
    commit_in(&mut orders, "o1").await;
    assert_eq!(s.info().await.head, 3);
    assert_eq!(head_in(&mut orders).await, 2);
    assert_eq!(read_all_payloads(&mut s.client).await, ["e0", "e1", "e2"]);
    let got = collect(
        orders
            .read_all(pb::ReadAllRequest {
                from: 0,
                limit: 0,
                backward: false,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    assert_eq!(payloads(&got), ["o0", "o1"]);

    let mut on_orders = orders
        .subscribe(pb::SubscribeRequest { from: 2, tag: None })
        .await
        .unwrap()
        .into_inner();
    let mut on_default = s
        .client
        .subscribe(pb::SubscribeRequest { from: 3, tag: None })
        .await
        .unwrap()
        .into_inner();
    fill(&mut s.client, agg(1), 1).await;
    assert_eq!(on_default.next().await.unwrap().unwrap().position, 3);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), on_orders.next())
            .await
            .is_err(),
        "a commit on default is not seen on orders"
    );
    s.client
        .drop_database(pb::DropDatabaseRequest {
            name: "orders".into(),
        })
        .await
        .unwrap();
    let end = tokio::time::timeout(Duration::from_secs(5), on_orders.next())
        .await
        .expect("the orders stream ends")
        .expect("with a status")
        .unwrap_err();
    assert_eq!(end.code(), Code::Unavailable);
    assert!(end.message().contains("dropped"), "{}", end.message());
    fill(&mut s.client, agg(1), 1).await;
    assert_eq!(
        on_default.next().await.unwrap().unwrap().position,
        4,
        "the default stream is unaffected"
    );
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn create_and_drop_are_admin_and_leader_only() {
    let s = Server::start_with_no_client(|o| {
        o.token = Some("user".into());
        o.admin_token = Some("admin".into());
    })
    .await;
    let addr = s.running.as_ref().unwrap().local_addr;
    let channel = plain_channel(addr).await;
    let mut user = with_token(channel.clone(), "user");
    let req = pb::CreateDatabaseRequest {
        name: "orders".into(),
    };
    assert_eq!(
        user.create_database(req.clone()).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        user.list_databases(pb::ListDatabasesRequest {})
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut admin = with_token(channel, "admin");
    admin.create_database(req).await.unwrap();
    s.stop().await;

    let mut leader = Server::start().await;
    fill(&mut leader.client, agg(1), 1).await;
    let mut replica = Server::start_replica_of(&leader).await;
    let err = replica
        .client
        .create_database(pb::CreateDatabaseRequest {
            name: "orders".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("leader"), "{}", err.message());
    assert_eq!(names(&mut replica).await, ["default"], "a replica lists");
    replica.stop().await;
    leader.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn backups_live_under_per_database_subdirs() {
    let mut s = Server::start_with(with_backup_dir).await;
    create(&mut s, "orders").await;
    let addr = s.running.as_ref().unwrap().local_addr;
    let mut orders = with_db(plain_channel(addr).await, "orders");
    fill(&mut s.client, agg(1), 2).await;
    commit_in(&mut orders, "o0").await;
    let req = pb::BackupRequest {
        archive: "x.chronbak".into(),
        since: None,
    };
    let a = s.client.backup(req.clone()).await.unwrap().into_inner();
    let b = orders.backup(req).await.unwrap().into_inner();
    assert_ne!(a.db_id, b.db_id);
    assert_eq!((a.head, b.head), (2, 1));
    assert!(s.dir.path().join("backups/default/x.chronbak").exists());
    assert!(s.dir.path().join("backups/orders/x.chronbak").exists());
    let err = orders
        .backup(pb::BackupRequest {
            archive: "../escape".into(),
            since: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_0_1_0_layout_is_migrated_into_default() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    {
        let db = chron::Db::open(&data, chron::Options::new().fsync(chron::Fsync::Never)).unwrap();
        let mut t = db.transaction();
        t.append_free(vec![
            chron::NewEvent::new("a"),
            chron::NewEvent::new("b"),
            chron::NewEvent::new("c"),
        ]);
        t.commit().unwrap();
    }
    std::fs::write(data.join("fenced"), "epoch 1\n").unwrap();
    assert!(data.join("index.redb").exists() && data.join("segments").is_dir());
    let mut s = Server::start_in(dir, "127.0.0.1:0".parse().unwrap(), |_| {}).await;
    let data = s.data_dir();
    assert!(data.join("default/index.redb").exists());
    assert!(data.join("default/segments").is_dir());
    assert!(!data.join("index.redb").exists() && !data.join("segments").exists());
    assert!(!data.join("LOCK").exists(), "the root LOCK is gone");
    assert!(data.join("fenced").exists(), "markers stay at the root");
    let info = s.info().await;
    assert_eq!((info.head, info.role()), (3, pb::Role::Fenced));
    let dir = s.stop_keep_dir().await;
    // Idempotent: a second start finds nothing to move.
    let mut again = Server::start_in(dir, "127.0.0.1:0".parse().unwrap(), |_| {}).await;
    assert_eq!(again.info().await.head, 3);
    let dir = again.stop_keep_dir().await;
    let db = chron::Db::open(
        dir.path().join("data/default"),
        chron::Options::new().verify_all_segments(true),
    )
    .unwrap();
    assert_eq!(db.head(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn promotion_raises_every_database() {
    let mut leader = Server::start().await;
    leader.running.as_ref().unwrap().db.set_epoch(2).unwrap();
    let created = create(&mut leader, "orders").await;
    assert_eq!(created.epoch, 2, "a new database takes the daemon's epoch");
    fill(&mut leader.client, agg(1), 1).await;
    let laddr = leader.running.as_ref().unwrap().local_addr;
    let mut lorders = with_db(plain_channel(laddr).await, "orders");
    commit_in(&mut lorders, "o0").await;
    let mut replica = Server::start_replica_of(&leader).await;
    let raddr = replica.running.as_ref().unwrap().local_addr;
    let rorders = with_db(plain_channel(raddr).await, "orders");
    wait_until("the replica to mirror both", || {
        let mut d = replica.client.clone();
        let mut o = rorders.clone();
        async move {
            d.head(pb::HeadRequest {}).await.unwrap().into_inner().head == 1
                && o.head(pb::HeadRequest {})
                    .await
                    .map(|h| h.into_inner().head)
                    .ok()
                    == Some(1)
        }
    })
    .await;
    let p = replica
        .client
        .promote(pb::PromoteRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(p.epoch, 3);
    let epochs: Vec<(String, u64)> = replica
        .client
        .list_databases(pb::ListDatabasesRequest {})
        .await
        .unwrap()
        .into_inner()
        .databases
        .into_iter()
        .map(|d| (d.name, d.epoch))
        .collect();
    assert_eq!(epochs, [("default".into(), 3), ("orders".into(), 3)]);
    let mut rorders = rorders;
    let err = rorders
        .commit(pb::CommitRequest {
            appends: vec![append(Some(agg(1)), any(), vec![new_event("stale", &[])])],
            epoch: Some(2),
            sync: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(err.metadata().get(chron_proto::EPOCH_HEADER).unwrap(), "3");
    rorders
        .commit(pb::CommitRequest {
            appends: vec![append(Some(agg(1)), any(), vec![new_event("fresh", &[])])],
            epoch: Some(3),
            sync: None,
        })
        .await
        .unwrap();
    let lc = leader.client.clone();
    wait_until("the old leader to be fenced", || {
        let mut c = lc.clone();
        async move {
            c.info(pb::InfoRequest {})
                .await
                .unwrap()
                .into_inner()
                .role()
                == pb::Role::Fenced
        }
    })
    .await;
    let err = lorders
        .commit(pb::CommitRequest {
            appends: vec![append(Some(agg(1)), any(), vec![new_event("late", &[])])],
            epoch: None,
            sync: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(err.metadata().get(chron_proto::EPOCH_HEADER).unwrap(), "2");
    replica.stop().await;
    leader.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_mirrors_every_database_and_discovers_new_ones() {
    let mut leader = Server::start().await;
    create(&mut leader, "orders").await;
    fill(&mut leader.client, agg(1), 2).await;
    let laddr = leader.running.as_ref().unwrap().local_addr;
    let mut lorders = with_db(plain_channel(laddr).await, "orders");
    commit_in(&mut lorders, "o0").await;
    let addr = leader.addr();
    let mut replica = Server::start_with(move |o| {
        o.replicate_from = Some(addr);
        o.discover_interval = Duration::from_secs(1);
    })
    .await;
    let raddr = replica.running.as_ref().unwrap().local_addr;
    let rorders = with_db(plain_channel(raddr).await, "orders");
    wait_until("both databases to catch up", || {
        let mut d = replica.client.clone();
        let mut o = rorders.clone();
        async move {
            d.head(pb::HeadRequest {}).await.unwrap().into_inner().head == 2
                && o.head(pb::HeadRequest {})
                    .await
                    .map(|h| h.into_inner().head)
                    .ok()
                    == Some(1)
        }
    })
    .await;
    assert_eq!(names(&mut replica).await, ["default", "orders"]);

    // Created after the replica connected: discovered within the interval.
    create(&mut leader, "billing").await;
    let mut lbilling = with_db(plain_channel(laddr).await, "billing");
    commit_in(&mut lbilling, "b0").await;
    commit_in(&mut lbilling, "b1").await;
    let rbilling = with_db(plain_channel(raddr).await, "billing");
    wait_until("billing to appear and catch up on the replica", || {
        let mut b = rbilling.clone();
        async move {
            b.head(pb::HeadRequest {})
                .await
                .map(|h| h.into_inner().head)
                .ok()
                == Some(2)
        }
    })
    .await;
    assert_eq!(names(&mut replica).await, ["billing", "default", "orders"]);
    let got = collect(
        rbilling
            .clone()
            .read_all(pb::ReadAllRequest {
                from: 0,
                limit: 0,
                backward: false,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    assert_eq!(payloads(&got), ["b0", "b1"]);

    // Dropped on the leader: the replica keeps its copy, read-only.
    leader
        .client
        .drop_database(pb::DropDatabaseRequest {
            name: "orders".into(),
        })
        .await
        .unwrap();
    let mut rorders2 = rorders.clone();
    wait_until("the replica to notice the drop", || {
        let mut o = rorders.clone();
        async move {
            let i = o.info(pb::InfoRequest {}).await.unwrap().into_inner();
            !i.replica_connected
                && i.last_replication_error
                    .as_deref()
                    .is_some_and(|e| e.contains("dropped on the leader"))
        }
    })
    .await;
    assert_eq!(
        head_in(&mut rorders2).await,
        1,
        "the copy is still readable"
    );
    assert_eq!(
        rorders2
            .commit(pb::CommitRequest {
                appends: vec![append(Some(agg(1)), any(), vec![new_event("x", &[])])],
                epoch: None,
                sync: None
            })
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    replica.stop().await;
    leader.stop().await;
}
