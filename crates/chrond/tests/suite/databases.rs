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
        commit_id: Vec::new(),
    })
    .await
    .unwrap()
    .into_inner()
    .first
}

async fn head_in(c: &mut DbClient) -> u64 {
    c.head(pb::HeadRequest::default())
        .await
        .unwrap()
        .into_inner()
        .head
}

async fn create(s: &mut Server, name: &str) -> pb::DatabaseInfo {
    s.client
        .create_database(pb::CreateDatabaseRequest {
            name: name.into(),
            options: None,
        })
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
    let err = orders.head(pb::HeadRequest::default()).await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    assert!(
        err.message().contains("chron db create orders"),
        "{}",
        err.message()
    );
    for bad in ["Orders", "a/b", "..", "", "déf", &"a".repeat(65), "a b"] {
        let mut c = with_db(plain_channel(addr).await, bad);
        let err = c.head(pb::HeadRequest::default()).await.unwrap_err();
        assert_eq!(
            err.code(),
            Code::InvalidArgument,
            "{bad:?}: {}",
            err.message()
        );
        let err = s
            .client
            .create_database(pb::CreateDatabaseRequest {
                name: bad.into(),
                options: None,
            })
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
            options: None,
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
        orders
            .head(pb::HeadRequest::default())
            .await
            .unwrap_err()
            .code(),
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
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    assert_eq!(payloads(&got), ["o0", "o1"]);

    let mut on_orders = orders
        .subscribe(pb::SubscribeRequest {
            from: 2,
            tag: None,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let mut on_default = s
        .client
        .subscribe(pb::SubscribeRequest {
            from: 3,
            tag: None,
            ..Default::default()
        })
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
        options: None,
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
            options: None,
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
            d.head(pb::HeadRequest::default())
                .await
                .unwrap()
                .into_inner()
                .head
                == 1
                && o.head(pb::HeadRequest::default())
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
            commit_id: Vec::new(),
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
            commit_id: Vec::new(),
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
            commit_id: Vec::new(),
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
            d.head(pb::HeadRequest::default())
                .await
                .unwrap()
                .into_inner()
                .head
                == 2
                && o.head(pb::HeadRequest::default())
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
            b.head(pb::HeadRequest::default())
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
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    assert_eq!(payloads(&got), ["b0", "b1"]);

    // Dropped on the leader: the replica removes its copy too (by the
    // tombstone's identity), and the name answers NOT_FOUND there.
    leader
        .client
        .drop_database(pb::DropDatabaseRequest {
            name: "orders".into(),
        })
        .await
        .unwrap();
    let mut rorders2 = rorders.clone();
    let orders_dir = replica.dir.path().join("data").join("orders");
    wait_until("the replica to remove the dropped database", || {
        let mut o = rorders.clone();
        let dir = orders_dir.clone();
        async move {
            o.info(pb::InfoRequest {})
                .await
                .is_err_and(|s| s.code() == Code::NotFound)
                && !dir.exists()
        }
    })
    .await;
    assert_eq!(
        rorders2
            .commit(pb::CommitRequest {
                appends: vec![append(Some(agg(1)), any(), vec![new_event("x", &[])])],
                epoch: None,
                sync: None,
                commit_id: Vec::new(),
            })
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    replica.stop().await;
    leader.stop().await;
}

async fn typed(s: &Server) -> chron_client::Client {
    chron_client::Client::connect(s.addr(), Default::default())
        .await
        .unwrap()
}

/// A database created with its own fsync setting keeps it: Info says so,
/// its commits sync nothing while the default's do, a restart reads the
/// options file, and a replica creates its mirror with the same setting.
#[tokio::test(flavor = "multi_thread")]
async fn a_database_created_without_fsync_keeps_it_across_a_restart_and_a_mirror() {
    let s = Server::start_with(|o| o.fsync = true).await;
    let client = typed(&s).await;
    let fast = client
        .create_database_with(
            "fast",
            chron_client::DatabaseOptions {
                fsync: Some(false),
                max_record_bytes: Some(1 << 20),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        (fast.fsync, fast.max_record_bytes),
        (Some(false), Some(1 << 20))
    );
    let fast_client = client.database("fast");
    for i in 0..10 {
        fast_client
            .append(
                agg(1),
                chron_client::Expected::Exact(i),
                vec![chron_client::NewEvent::new("e".to_string())],
            )
            .await
            .unwrap();
    }
    client
        .append(
            agg(1),
            chron_client::Expected::New,
            vec![chron_client::NewEvent::new("d".to_string())],
        )
        .await
        .unwrap();
    let node = s.running.as_ref().unwrap().node.clone();
    assert_eq!(
        node.get("fast").unwrap().db.stats().fsyncs,
        0,
        "no sync on fast"
    );
    assert!(
        node.get("default").unwrap().db.stats().fsyncs > 0,
        "default syncs"
    );
    assert!(!fast_client.info().await.unwrap().fsync);
    let listed = client.list_databases_full().await.unwrap();
    let f = listed.databases.iter().find(|d| d.name == "fast").unwrap();
    assert_eq!(f.fsync, Some(false));
    // The node handle keeps the databases open: let it go before the
    // restart, which takes their locks again.
    drop(node);
    let addr = s.running.as_ref().unwrap().local_addr;
    let dir = s.stop_keep_dir().await;
    assert!(dir.path().join("data").join("fast.options").exists());
    let s = Server::start_in(dir, addr, |o| o.fsync = true).await;
    let client = typed(&s).await;
    let info = client.database("fast").info().await.unwrap();
    assert!(!info.fsync, "the options file was read at the restart");
    assert_eq!(info.options.max_record_bytes, 1 << 20);
    // A replica mirrors it with the same setting.
    let laddr = s.addr();
    let replica = Server::start_with(move |o| {
        o.fsync = true;
        o.replicate_from = Some(laddr);
        o.discover_interval = Duration::from_secs(1);
    })
    .await;
    let rnode = replica.running.as_ref().unwrap().node.clone();
    wait_until("the mirror of fast", || {
        let rnode = rnode.clone();
        async move { rnode.get("fast").is_some() }
    })
    .await;
    assert_eq!(
        rnode.get("fast").unwrap().db.options().fsync,
        chron::Fsync::Never,
        "the mirror took the leader's setting"
    );
    assert!(
        replica
            .dir
            .path()
            .join("data")
            .join("fast.options")
            .exists()
    );
    replica.stop().await;
    s.stop().await;
}

/// A drop on the leader removes the replica's copy within a discovery
/// interval, and a database recreated under the dropped name is
/// mirrored afresh, since the tombstone names the old identity.
#[tokio::test(flavor = "multi_thread")]
async fn a_replica_removes_a_dropped_database_by_identity() {
    let mut leader = Server::start().await;
    create(&mut leader, "orders").await;
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
    wait_until("orders to catch up on the replica", || {
        let mut o = rorders.clone();
        async move {
            o.head(pb::HeadRequest::default())
                .await
                .map(|h| h.into_inner().head)
                .ok()
                == Some(1)
        }
    })
    .await;
    assert_eq!(names(&mut replica).await, ["default", "orders"]);
    let old_id = replica
        .running
        .as_ref()
        .unwrap()
        .node
        .get("orders")
        .unwrap()
        .db
        .id();
    leader
        .client
        .drop_database(pb::DropDatabaseRequest {
            name: "orders".into(),
        })
        .await
        .unwrap();
    let rnode = replica.running.as_ref().unwrap().node.clone();
    let orders_dir = replica.dir.path().join("data").join("orders");
    wait_until("the replica to remove orders", || {
        let rnode = rnode.clone();
        let dir = orders_dir.clone();
        async move { rnode.get("orders").is_none() && !dir.exists() }
    })
    .await;
    assert!(
        rnode.tombstones().iter().any(|t| t.db_id == old_id),
        "the tombstone was kept"
    );
    let code = rorders
        .clone()
        .head(pb::HeadRequest::default())
        .await
        .unwrap_err()
        .code();
    assert_eq!(code, Code::NotFound);
    // Recreated under the name: a new identity, mirrored afresh and kept.
    create(&mut leader, "orders").await;
    let mut lorders = with_db(plain_channel(laddr).await, "orders");
    commit_in(&mut lorders, "o1").await;
    commit_in(&mut lorders, "o2").await;
    wait_until("the recreated orders to catch up", || {
        let mut o = rorders.clone();
        async move {
            o.head(pb::HeadRequest::default())
                .await
                .map(|h| h.into_inner().head)
                .ok()
                == Some(2)
        }
    })
    .await;
    let fresh = rnode.get("orders").unwrap();
    assert_ne!(fresh.db.id(), old_id);
    // Three more discovery passes: the tombstone names the old identity,
    // so the fresh mirror stays the same one (a match by name would
    // delete and recreate it on every pass).
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(names(&mut replica).await, ["default", "orders"]);
    assert!(
        std::sync::Arc::ptr_eq(&fresh, &rnode.get("orders").unwrap()),
        "the recreated mirror was replaced"
    );
    replica.stop().await;
    leader.stop().await;
}
