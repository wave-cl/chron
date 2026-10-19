use chron_proto::v1 as pb;
use tonic::Code;

use crate::common::*;

async fn commit_big(db: &chron::Db, n: usize) {
    let db = db.clone();
    tokio::task::spawn_blocking(move || {
        let mut t = db.transaction();
        t.append(
            chron::Uuid::from_u128(7),
            chron::Expected::Any,
            (0..n)
                .map(|i| chron::NewEvent::new(format!("big{i}")).tag("t"))
                .collect(),
        );
        t.commit().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn replica_tails_existing_data_and_live_commits_including_a_long_transaction() {
    let mut leader = Server::start().await;
    fill(&mut leader.client, agg(1), 5).await;
    let mut replica = Server::start_replica_of(&leader).await;
    let replica_client = replica.client.clone();
    wait_until("the replica to reach 5", || {
        let mut c = replica_client.clone();
        async move { c.head(pb::HeadRequest {}).await.unwrap().into_inner().head == 5 }
    })
    .await;
    commit_big(&leader.running.as_ref().unwrap().db, 2000).await;
    fill(&mut leader.client, agg(2), 3).await;
    let want = leader.info().await.head;
    let replica_client = replica.client.clone();
    wait_until("the replica to catch up", || {
        let mut c = replica_client.clone();
        async move { c.head(pb::HeadRequest {}).await.unwrap().into_inner().head == want }
    })
    .await;
    assert_eq!(
        read_all_payloads(&mut replica.client).await,
        read_all_payloads(&mut leader.client).await
    );
    let info = replica.info().await;
    assert_eq!(info.role(), pb::Role::Replica);
    assert!(info.replica_connected);
    assert_eq!(info.leader_head, Some(want));
    assert_eq!(info.replicating_from, Some(leader.addr()));
    assert_eq!(info.db_id, leader.info().await.db_id);
    assert_eq!(leader.info().await.role(), pb::Role::Leader);
    replica.stop().await;
    leader.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn replica_refuses_writes_and_apply_but_backs_up_and_retains() {
    let mut leader = Server::start().await;
    fill(&mut leader.client, agg(1), 2).await;
    let addr = leader.addr();
    let mut replica = Server::start_with(move |o| {
        with_backup_dir(o);
        o.replicate_from = Some(addr);
    })
    .await;
    let err = replica
        .client
        .commit(pb::CommitRequest {
            appends: vec![append(Some(agg(1)), any(), vec![new_event("x", &[])])],
            epoch: None,
            sync: None,
            commit_id: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("read-only replica of"),
        "{}",
        err.message()
    );
    assert_eq!(err.metadata().get(chron_proto::EPOCH_HEADER).unwrap(), "0");
    let err = replica
        .client
        .apply_backup(pb::ApplyBackupRequest {
            archive: "nonexistent.chronbak".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    replica
        .client
        .backup(pb::BackupRequest {
            archive: "b.chronbak".into(),
            since: None,
        })
        .await
        .unwrap();
    assert!(
        replica
            .dir
            .path()
            .join("backups/default/b.chronbak")
            .exists()
    );
    replica
        .client
        .retain(pb::RetainRequest { floor: 0 })
        .await
        .unwrap();
    replica.stop().await;
    leader.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn promotion_bumps_the_epoch_fences_the_old_leader_and_the_old_leader_rejoins() {
    let port = free_port();
    let mut leader = Server::start_in(
        tempfile::tempdir().unwrap(),
        format!("127.0.0.1:{port}").parse().unwrap(),
        |_| {},
    )
    .await;
    leader.running.as_ref().unwrap().db.set_epoch(2).unwrap();
    fill(&mut leader.client, agg(1), 3).await;
    let mut replica = Server::start_replica_of(&leader).await;
    let want = leader.info().await.head;
    let rc = replica.client.clone();
    wait_until("catch-up", || {
        let mut c = rc.clone();
        async move { c.head(pb::HeadRequest {}).await.unwrap().into_inner().head == want }
    })
    .await;
    assert_eq!(
        replica.info().await.epoch,
        2,
        "the replica adopted the leader's epoch"
    );

    let promoted = replica
        .client
        .promote(pb::PromoteRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!((promoted.epoch, promoted.head), (3, want));
    assert!(replica.data_dir().join("promoted").exists());
    let info = replica.info().await;
    assert_eq!((info.role(), info.epoch), (pb::Role::Leader, 3));
    fill(&mut replica.client, agg(2), 1).await;
    assert!(matches!(
        replica
            .client
            .promote(pb::PromoteRequest {})
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    ));

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
    let info = leader.info().await;
    assert_eq!((info.fenced_by, info.epoch), (Some(3), 2));
    let err = leader
        .client
        .commit(pb::CommitRequest {
            appends: vec![append(Some(agg(1)), any(), vec![new_event("late", &[])])],
            epoch: None,
            sync: None,
            commit_id: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("fenced: a newer leader (epoch 3)"),
        "{}",
        err.message()
    );
    assert_eq!(err.metadata().get(chron_proto::EPOCH_HEADER).unwrap(), "2");
    assert!(leader.data_dir().join("fenced").exists());

    // The old leader rejoins as a replica of the new one.
    let old_dir = leader.stop_keep_dir().await;
    let new_addr = replica.addr();
    let mut rejoined = Server::start_in(old_dir, "127.0.0.1:0".parse().unwrap(), move |o| {
        o.replicate_from = Some(new_addr)
    })
    .await;
    fill(&mut replica.client, agg(2), 2).await;
    let want = replica.info().await.head;
    let rc = rejoined.client.clone();
    wait_until("the rejoined node to catch up", || {
        let mut c = rc.clone();
        async move { c.head(pb::HeadRequest {}).await.unwrap().into_inner().head == want }
    })
    .await;
    assert!(!rejoined.data_dir().join("fenced").exists());
    let info = rejoined.info().await;
    assert_eq!((info.role(), info.epoch), (pb::Role::Replica, 3));
    assert_eq!(
        read_all_payloads(&mut rejoined.client).await,
        read_all_payloads(&mut replica.client).await
    );
    rejoined.stop().await;
    replica.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_of_a_different_database_and_a_diverged_replica_are_refused() {
    let mut leader = Server::start_with(with_backup_dir).await;
    fill(&mut leader.client, agg(1), 3).await;
    let other = tempfile::tempdir().unwrap();
    {
        let db = chron::Db::open(
            other.path().join("data"),
            chron::Options::new().fsync(chron::Fsync::Never),
        )
        .unwrap();
        let mut t = db.transaction();
        t.append_free(vec![chron::NewEvent::new("mine")]);
        t.commit().unwrap();
    }
    let addr = leader.addr();
    let err = Server::try_start_in(other, "127.0.0.1:0".parse().unwrap(), move |o| {
        o.replicate_from = Some(addr)
    })
    .await
    .err()
    .expect("refused");
    assert!(err.to_string().contains("is database"), "{err}");

    let archive = leader.dir.path().join("backups/default/full.chronbak");
    leader
        .client
        .backup(pb::BackupRequest {
            archive: "full.chronbak".into(),
            since: None,
        })
        .await
        .unwrap();
    let diverged = tempfile::tempdir().unwrap();
    chron::restore_backup(
        &archive,
        diverged.path().join("data"),
        &chron::Options::new(),
    )
    .unwrap();
    {
        let db = chron::Db::open(
            diverged.path().join("data"),
            chron::Options::new().fsync(chron::Fsync::Never),
        )
        .unwrap();
        let mut t = db.transaction();
        t.append_free(vec![chron::NewEvent::new("diverging")]);
        t.commit().unwrap();
    }
    fill(&mut leader.client, agg(1), 1).await;
    let addr = leader.addr();
    let err = Server::try_start_in(diverged, "127.0.0.1:0".parse().unwrap(), move |o| {
        o.replicate_from = Some(addr)
    })
    .await
    .err()
    .expect("refused");
    assert!(err.to_string().contains("diverged"), "{err}");
    leader.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fencing_token_on_commit() {
    let mut s = Server::start().await;
    let req = |epoch: Option<u64>| pb::CommitRequest {
        appends: vec![append(Some(agg(1)), any(), vec![new_event("x", &[])])],
        epoch,
        sync: None,
        commit_id: Vec::new(),
    };
    s.client.commit(req(None)).await.unwrap();
    s.client.commit(req(Some(0))).await.unwrap();
    s.running.as_ref().unwrap().db.set_epoch(2).unwrap();
    let err = s.client.commit(req(Some(1))).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("stale epoch 1"), "{}", err.message());
    assert_eq!(err.metadata().get(chron_proto::EPOCH_HEADER).unwrap(), "2");
    s.client.commit(req(Some(2))).await.unwrap();
    let not_newer = s
        .client
        .fence(pb::FenceRequest { epoch: 2 })
        .await
        .unwrap_err();
    assert_eq!(not_newer.code(), Code::FailedPrecondition);

    // A jump of more than one is a confused client, not a new leader.
    let err = s.client.commit(req(Some(7))).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("more than one above"),
        "{}",
        err.message()
    );
    let info = s.info().await;
    assert_eq!((info.role(), info.epoch), (pb::Role::Leader, 2));
    // Exactly one above: a leader promoted from this one exists.
    let err = s.client.commit(req(Some(3))).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("fenced"), "{}", err.message());
    let info = s.info().await;
    assert_eq!(
        (info.role(), info.fenced_by, info.epoch),
        (pb::Role::Fenced, Some(3), 2)
    );
    assert_eq!(
        s.client.commit(req(None)).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        s.client
            .head(pb::HeadRequest {})
            .await
            .unwrap()
            .into_inner()
            .head,
        3
    );
    let dir = s.stop_keep_dir().await;
    let mut again = Server::start_in(dir, "127.0.0.1:0".parse().unwrap(), |_| {}).await;
    assert_eq!(
        again.info().await.role(),
        pb::Role::Fenced,
        "the marker survives a restart"
    );
    again.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_promoted_dir_refuses_to_start_as_a_replica_until_the_marker_is_removed() {
    let mut leader = Server::start().await;
    fill(&mut leader.client, agg(1), 1).await;
    let mut replica = Server::start_replica_of(&leader).await;
    let rc = replica.client.clone();
    wait_until("catch-up", || {
        let mut c = rc.clone();
        async move { c.head(pb::HeadRequest {}).await.unwrap().into_inner().head == 1 }
    })
    .await;
    replica.client.promote(pb::PromoteRequest {}).await.unwrap();
    let dir = replica.stop_keep_dir().await;
    let addr = leader.addr();
    let err = Server::try_start_in(dir, "127.0.0.1:0".parse().unwrap(), move |o| {
        o.replicate_from = Some(addr)
    })
    .await
    .err()
    .expect("refused");
    assert!(err.to_string().contains("was promoted"), "{err}");
    leader.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn replica_reconnects_after_the_leader_restarts_on_the_same_port() {
    let port = free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut leader = Server::start_in(tempfile::tempdir().unwrap(), addr, |_| {}).await;
    fill(&mut leader.client, agg(1), 2).await;
    let mut replica = Server::start_replica_of(&leader).await;
    let rc = replica.client.clone();
    wait_until("catch-up", || {
        let mut c = rc.clone();
        async move { c.head(pb::HeadRequest {}).await.unwrap().into_inner().head == 2 }
    })
    .await;
    let dir = leader.stop_keep_dir().await;
    let rc = replica.client.clone();
    wait_until("the replica to notice", || {
        let mut c = rc.clone();
        async move {
            let i = c.info(pb::InfoRequest {}).await.unwrap().into_inner();
            !i.replica_connected && i.last_replication_error.is_some()
        }
    })
    .await;
    let mut leader = Server::start_in(dir, addr, |_| {}).await;
    fill(&mut leader.client, agg(1), 3).await;
    let rc = replica.client.clone();
    wait_until("the replica to reconnect and catch up", || {
        let mut c = rc.clone();
        async move {
            let i = c.info(pb::InfoRequest {}).await.unwrap().into_inner();
            i.replica_connected && i.head == 5
        }
    })
    .await;
    assert_eq!(
        read_all_payloads(&mut replica.client).await,
        read_all_payloads(&mut leader.client).await
    );
    replica.stop().await;
    leader.stop().await;
}
