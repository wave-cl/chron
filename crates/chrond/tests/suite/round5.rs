//! The daemon findings of the review round: message limits, stream
//! back-pressure, the replica marker, streams that end on a fence, and the
//! leader health service.

use std::time::Duration;

use chron_proto::v1 as pb;
use tokio_stream::StreamExt;
use tonic::Code;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

use crate::common::*;

async fn commit_payloads(db: &chron::Db, payloads: Vec<Vec<u8>>) {
    let db = db.clone();
    tokio::task::spawn_blocking(move || {
        let mut t = db.transaction();
        t.append(
            chron::Uuid::from_u128(9),
            chron::Expected::Any,
            payloads.into_iter().map(chron::NewEvent::new).collect(),
        );
        t.commit().unwrap();
    })
    .await
    .unwrap();
}

async fn head_reaches(client: &tonic::transport::Channel, n: u64) {
    let c = pb::chron_client::ChronClient::new(client.clone());
    wait_until(&format!("the head to reach {n}"), || {
        let mut c = c.clone();
        async move { c.head(pb::HeadRequest {}).await.unwrap().into_inner().head == n }
    })
    .await;
}

async fn leader_health(addr: std::net::SocketAddr) -> ServingStatus {
    HealthClient::new(plain_channel(addr).await)
        .check(HealthCheckRequest {
            service: chrond::LEADER_HEALTH_SERVICE.into(),
        })
        .await
        .unwrap()
        .into_inner()
        .status()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_of_several_megabytes_replicates_with_the_default_limits() {
    let mut leader = Server::start().await;
    fill(&mut leader.client, agg(1), 1).await;
    let replica = Server::start_replica_of(&leader).await;
    commit_payloads(
        &leader.running.as_ref().unwrap().db,
        (0..3).map(|i| vec![i as u8; 2 * 1024 * 1024]).collect(),
    )
    .await;
    head_reaches(
        &plain_channel(replica.running.as_ref().unwrap().local_addr).await,
        4,
    )
    .await;
    replica.stop().await;
    leader.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_above_the_message_limit_is_reported_not_retried_silently() {
    let limits = |o: &mut chrond::Options| {
        o.max_record_bytes = Some(512 * 1024);
        o.max_message_bytes = 1024 * 1024;
    };
    let mut leader = Server::start_with(limits).await;
    fill(&mut leader.client, agg(1), 1).await;
    let addr = leader.addr();
    let mut replica = Server::start_with(move |o| {
        limits(o);
        o.replicate_from = Some(addr);
    })
    .await;
    head_reaches(
        &plain_channel(replica.running.as_ref().unwrap().local_addr).await,
        1,
    )
    .await;
    // Three records of 400 KiB: each fits, the transaction does not.
    commit_payloads(
        &leader.running.as_ref().unwrap().db,
        (0..3).map(|_| vec![7u8; 400 * 1024]).collect(),
    )
    .await;
    let rc = replica.client.clone();
    wait_until("the replica to report the oversize transaction", || {
        let mut c = rc.clone();
        async move {
            let i = c.info(pb::InfoRequest {}).await.unwrap().into_inner();
            !i.replica_connected
                && i.last_replication_error
                    .as_deref()
                    .is_some_and(|e| e.contains("raise --max-message-bytes"))
        }
    })
    .await;
    assert_eq!(replica.info().await.head, 1, "nothing partial landed");
    // The leader itself keeps serving.
    fill(&mut leader.client, agg(1), 1).await;
    replica.stop().await;
    leader.stop().await;
}

/// Stalled streams must not hold blocking threads: on a runtime with 8 of
/// them, 64 streams nobody reads would otherwise queue every commit and
/// info call behind them.
#[test]
fn stalled_streams_do_not_starve_commits() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(8)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut s = Server::start().await;
        // 4 MiB per stream: far more than the HTTP/2 windows and the
        // channel hold, so every stream stalls with data left to send.
        commit_payloads(
            &s.running.as_ref().unwrap().db,
            (0..2000).map(|_| vec![1u8; 2048]).collect(),
        )
        .await;
        let addr = s.running.as_ref().unwrap().local_addr;
        let mut streams = Vec::new();
        for _ in 0..2 {
            let channel = plain_channel(addr).await;
            for _ in 0..32 {
                let mut c = pb::chron_client::ChronClient::new(channel.clone());
                streams.push(
                    c.read_all(pb::ReadAllRequest {
                        from: 0,
                        limit: 0,
                        backward: false,
                    })
                    .await
                    .unwrap()
                    .into_inner(),
                );
            }
        }
        // Let every stream fill its buffers and stall.
        tokio::time::sleep(Duration::from_millis(300)).await;
        tokio::time::timeout(Duration::from_secs(1), fill(&mut s.client, agg(2), 1))
            .await
            .expect("a commit answers within a second while 64 streams wait unread");
        tokio::time::timeout(Duration::from_secs(1), s.info())
            .await
            .expect("info answers within a second");
        // One stream is still live and completes once its unread siblings
        // (which held the connection's HTTP/2 window) are gone.
        let first = streams.pop().unwrap();
        drop(streams);
        let n = tokio::time::timeout(Duration::from_secs(30), collect(first))
            .await
            .expect("the remaining stream completes")
            .unwrap()
            .len();
        assert!(
            n == 2000 || n == 2001,
            "the stream reads to the head it sees: {n}"
        );
        s.stop().await;
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_dir_refuses_to_start_as_a_leader_and_promotes_with_the_flag() {
    let mut leader = Server::start().await;
    leader.running.as_ref().unwrap().db.set_epoch(4).unwrap();
    fill(&mut leader.client, agg(1), 2).await;
    let replica = Server::start_replica_of(&leader).await;
    assert!(replica.data_dir().join("replica").exists());
    let replica_addr = replica.running.as_ref().unwrap().local_addr;
    assert_eq!(leader_health(replica_addr).await, ServingStatus::NotServing);
    assert_eq!(
        leader_health(leader.running.as_ref().unwrap().local_addr).await,
        ServingStatus::Serving
    );
    head_reaches(&plain_channel(replica_addr).await, 2).await;
    let dir = replica.stop_keep_dir().await;
    let marker = std::fs::read_to_string(dir.path().join("data/replica")).unwrap();
    assert_eq!(marker.trim(), format!("leader {}", leader.addr()));

    let err = Server::try_start_in(dir, "127.0.0.1:0".parse().unwrap(), |_| {})
        .await
        .err()
        .expect("a replica directory does not start as a leader");
    assert!(err.to_string().contains("is a replica of"), "{err}");
    leader.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn promote_at_start_bumps_the_epoch_and_writes_the_marker() {
    let mut leader = Server::start().await;
    leader.running.as_ref().unwrap().db.set_epoch(4).unwrap();
    fill(&mut leader.client, agg(1), 2).await;
    let replica = Server::start_replica_of(&leader).await;
    head_reaches(
        &plain_channel(replica.running.as_ref().unwrap().local_addr).await,
        2,
    )
    .await;
    let dir = replica.stop_keep_dir().await;
    let err = Server::try_start_in(dir, "127.0.0.1:0".parse().unwrap(), |_| {})
        .await
        .err()
        .expect("refused");
    assert!(err.to_string().contains("--promote"), "{err}");
    let dir = tempfile::tempdir().unwrap();
    // A leader directory has nothing to promote.
    let err = Server::try_start_in(dir, "127.0.0.1:0".parse().unwrap(), |o| o.promote = true)
        .await
        .err()
        .expect("refused");
    assert!(err.to_string().contains("not a replica"), "{err}");

    // The same replica directory, promoted.
    let replica = Server::start_replica_of(&leader).await;
    let addr = replica.running.as_ref().unwrap().local_addr;
    head_reaches(&plain_channel(addr).await, 2).await;
    let dir = replica.stop_keep_dir().await;
    let mut promoted =
        Server::start_in(dir, "127.0.0.1:0".parse().unwrap(), |o| o.promote = true).await;
    let info = promoted.info().await;
    assert_eq!(
        (info.role(), info.epoch, info.head),
        (pb::Role::Leader, 5, 2)
    );
    assert!(promoted.data_dir().join("promoted").exists());
    assert!(!promoted.data_dir().join("replica").exists());
    assert_eq!(
        leader_health(promoted.running.as_ref().unwrap().local_addr).await,
        ServingStatus::Serving
    );
    fill(&mut promoted.client, agg(1), 1).await;
    let dir = promoted.stop_keep_dir().await;
    let mut again = Server::start_in(dir, "127.0.0.1:0".parse().unwrap(), |_| {}).await;
    assert_eq!(again.info().await.role(), pb::Role::Leader);
    again.stop().await;
    leader.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_starts_disconnected_when_its_leader_is_down_and_connects_later() {
    let port = free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut leader = Server::start_in(tempfile::tempdir().unwrap(), addr, |_| {}).await;
    fill(&mut leader.client, agg(1), 2).await;
    let replica = Server::start_replica_of(&leader).await;
    head_reaches(
        &plain_channel(replica.running.as_ref().unwrap().local_addr).await,
        2,
    )
    .await;
    let replica_dir = replica.stop_keep_dir().await;
    let leader_dir = leader.stop_keep_dir().await;

    let leader_addr = format!("http://{addr}");
    let mut replica = Server::start_in(replica_dir, "127.0.0.1:0".parse().unwrap(), move |o| {
        o.replicate_from = Some(leader_addr)
    })
    .await;
    let info = replica.info().await;
    assert_eq!(info.role(), pb::Role::Replica);
    assert!(!info.replica_connected);
    assert!(
        info.last_replication_error
            .as_deref()
            .is_some_and(|e| e.contains("cannot reach the leader")),
        "{:?}",
        info.last_replication_error
    );
    let mut leader = Server::start_in(leader_dir, addr, |_| {}).await;
    fill(&mut leader.client, agg(1), 3).await;
    let rc = replica.client.clone();
    wait_until("the replica to connect and catch up", || {
        let mut c = rc.clone();
        async move {
            let i = c.info(pb::InfoRequest {}).await.unwrap().into_inner();
            i.replica_connected && i.head == 5 && i.last_replication_error.is_none()
        }
    })
    .await;
    replica.stop().await;
    leader.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribe_and_replication_streams_end_with_unavailable_when_the_node_is_fenced() {
    let mut leader = Server::start().await;
    fill(&mut leader.client, agg(1), 2).await;
    let mut replica = Server::start_replica_of(&leader).await;
    head_reaches(
        &plain_channel(replica.running.as_ref().unwrap().local_addr).await,
        2,
    )
    .await;
    let mut sub = leader
        .client
        .subscribe(pb::SubscribeRequest { from: 0, tag: None })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(sub.next().await.unwrap().unwrap().position, 0);
    assert_eq!(sub.next().await.unwrap().unwrap().position, 1);
    leader
        .client
        .fence(pb::FenceRequest { epoch: 1 })
        .await
        .unwrap();
    let end = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("the subscription ends")
        .expect("with a status")
        .unwrap_err();
    assert_eq!(end.code(), Code::Unavailable);
    assert!(end.message().contains("fenced"), "{}", end.message());
    assert!(sub.next().await.is_none());
    let rc = replica.client.clone();
    wait_until("the replica to lose its stream", || {
        let mut c = rc.clone();
        async move {
            let i = c.info(pb::InfoRequest {}).await.unwrap().into_inner();
            !i.replica_connected
                && i.last_replication_error
                    .as_deref()
                    .is_some_and(|e| e.contains("fenced"))
        }
    })
    .await;
    assert_eq!(replica.info().await.head, 2);
    replica.stop().await;
    leader.stop().await;
}

/// A leader retained up to its head cannot check the record before a
/// replica's head when the two coincide: the replica (which kept
/// everything) still sends the fingerprint, and the leader lets it pass.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_at_its_floor_accepts_a_replica_without_checking_the_fingerprint() {
    // Offline: tiny segments, so the head sits on a segment base; the
    // replica is cut from the leader before retention moves the floor up
    // to the head.
    let leader_dir = tempfile::tempdir().unwrap();
    let archive = leader_dir.path().join("full.chronbak");
    {
        let db = chron::Db::open(
            leader_dir.path().join("data"),
            chron::Options::new()
                .fsync(chron::Fsync::Never)
                .segment_max_bytes(1),
        )
        .unwrap();
        for i in 0..5 {
            let mut t = db.transaction();
            t.append(
                agg(1),
                chron::Expected::Any,
                vec![chron::NewEvent::new(format!("e{i}"))],
            );
            t.commit().unwrap();
        }
        db.backup(&archive).unwrap();
        let r = db.retain_from(5).unwrap();
        assert_eq!((r.floor, db.head()), (5, 5), "floor and head coincide");
    }
    let replica_dir = tempfile::tempdir().unwrap();
    chron::restore_backup(
        &archive,
        replica_dir.path().join("data"),
        &chron::Options::new(),
    )
    .unwrap();
    let mut leader = Server::start_in(leader_dir, "127.0.0.1:0".parse().unwrap(), |_| {}).await;
    assert_eq!(leader.info().await.floor, 5);
    let addr = leader.addr();
    let mut replica = Server::start_in(replica_dir, "127.0.0.1:0".parse().unwrap(), move |o| {
        o.replicate_from = Some(addr)
    })
    .await;
    let info = replica.info().await;
    assert_eq!(
        (info.head, info.floor),
        (5, 0),
        "the replica kept everything"
    );
    assert!(info.replica_connected, "{:?}", info.last_replication_error);
    fill(&mut leader.client, agg(1), 2).await;
    let rc = replica.client.clone();
    wait_until("the replica to follow from the leader's floor", || {
        let mut c = rc.clone();
        async move {
            let i = c.info(pb::InfoRequest {}).await.unwrap().into_inner();
            i.replica_connected && i.head == 7
        }
    })
    .await;
    replica.stop().await;
    leader.stop().await;
}

/// Fence takes the write gate's write side, ApplyBackup its read side
/// with the role check inside: a fence issued while an apply is in flight
/// waits for it, and nothing applies after the fence returns.
#[tokio::test(flavor = "multi_thread")]
async fn apply_backup_racing_fence_never_lands_after_the_fence() {
    let mut s = Server::start_with(with_backup_dir).await;
    fill(&mut s.client, agg(1), 3).await;
    s.client
        .backup(pb::BackupRequest {
            archive: "full.chronbak".into(),
            since: None,
        })
        .await
        .unwrap();
    // An incremental 3..5 from a restored copy, so this daemon (head 3)
    // can apply it.
    let backups = s.dir.path().join("backups");
    let copy = s.dir.path().join("copy");
    chron::restore_backup(backups.join("full.chronbak"), &copy, &chron::Options::new()).unwrap();
    {
        let db = chron::Db::open(&copy, chron::Options::new().fsync(chron::Fsync::Never)).unwrap();
        let mut t = db.transaction();
        t.append(
            agg(1),
            chron::Expected::Any,
            vec![chron::NewEvent::new("four"), chron::NewEvent::new("five")],
        );
        t.commit().unwrap();
        db.backup_since(backups.join("inc.chronbak"), 3).unwrap();
    }

    let pause = chrond::node::WritePause::new();
    *s.running.as_ref().unwrap().node.write_pause.lock().unwrap() = Some(pause.clone());
    // A failed assertion must not leave the apply parked on the barrier
    // (the runtime would wait for it forever): release it on unwind.
    struct Release(std::sync::Arc<chrond::node::WritePause>, bool);
    impl Drop for Release {
        fn drop(&mut self) {
            if !self.1 {
                let p = self.0.clone();
                std::thread::spawn(move || p.proceed.wait());
            }
        }
    }
    let mut release = Release(pause.clone(), false);
    let mut c = s.client.clone();
    let apply = tokio::spawn(async move {
        c.apply_backup(pb::ApplyBackupRequest {
            archive: "inc.chronbak".into(),
        })
        .await
    });
    let p = pause.clone();
    tokio::task::spawn_blocking(move || p.entered.wait())
        .await
        .unwrap();
    // The apply has passed its role check and holds the gate. A fence
    // issued now must wait for it.
    let mut c = s.client.clone();
    let fence = tokio::spawn(async move { c.fence(pb::FenceRequest { epoch: 1 }).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !fence.is_finished(),
        "the fence waited for the apply in flight"
    );
    assert_eq!(s.info().await.role(), pb::Role::Leader);
    release.1 = true;
    let p = pause.clone();
    tokio::task::spawn_blocking(move || p.proceed.wait())
        .await
        .unwrap();
    let applied = apply.await.unwrap().unwrap().into_inner();
    assert_eq!(applied.head, 5);
    fence.await.unwrap().unwrap();
    let info = s.info().await;
    assert_eq!((info.role(), info.head), (pb::Role::Fenced, 5));
    // After the fence nothing applies, in flight or new.
    let head_after_fence = info.head;
    let err = s
        .client
        .apply_backup(pb::ApplyBackupRequest {
            archive: "inc.chronbak".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("fenced"), "{}", err.message());
    assert_eq!(s.info().await.head, head_after_fence);
    s.stop().await;
}
