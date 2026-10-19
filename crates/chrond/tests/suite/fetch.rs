//! A backup fetched over the daemon: the archive's bytes as a stream.

use std::time::Duration;

use chron_client::{Client, ClientConfig, ClientError};
use chron_proto::v1 as pb;
use tokio_stream::StreamExt;

use crate::common::{Server, append, new_event, with_backup_dir};

async fn fill_big(s: &Server, n: usize) {
    fill_bytes(s, n, 1024).await
}

async fn fill_bytes(s: &Server, n: usize, bytes: usize) {
    let mut c = s.client.clone();
    for i in 0..n {
        c.commit(pb::CommitRequest {
            appends: vec![append(None, None, vec![new_event(&"x".repeat(bytes), &[])])],
            epoch: None,
            sync: None,
            commit_id: Vec::new(),
        })
        .await
        .unwrap_or_else(|e| panic!("event {i}: {e}"));
    }
}

fn after_header(b: &[u8]) -> &[u8] {
    let len = 12 + u32::from_be_bytes(b[8..12].try_into().unwrap()) as usize;
    &b[len..b.len() - 4]
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_backup_streams_the_same_archive_as_a_path_backup() {
    let s = Server::start_with(with_backup_dir).await;
    fill_big(&s, 300).await;
    let client = Client::connect(s.addr(), ClientConfig::default())
        .await
        .unwrap();
    let on_disk_meta = client.backup("full.chronbak", None).await.unwrap();
    let on_disk = std::fs::read(
        s.dir
            .path()
            .join("backups")
            .join("default")
            .join("full.chronbak"),
    )
    .unwrap();
    let fetched_to = s.dir.path().join("fetched.chronbak");
    let meta = client.fetch_backup_to(&fetched_to).await.unwrap();
    let fetched = std::fs::read(&fetched_to).unwrap();
    assert_eq!(after_header(&on_disk), after_header(&fetched));
    assert_eq!(
        (meta.head, meta.floor, meta.db_id, meta.bytes),
        (
            on_disk_meta.head,
            on_disk_meta.floor,
            on_disk_meta.db_id,
            fetched.len() as u64
        )
    );
    // The chunks are bounded.
    let mut stream = client.fetch_backup().await.unwrap();
    let mut chunks = 0;
    while let Some(c) = stream.next().await {
        assert!(c.unwrap().len() <= 1 << 20);
        chunks += 1;
    }
    assert!(chunks >= 1);
    let node = &s.running.as_ref().unwrap().node;
    assert_eq!(
        node.metrics
            .backup_fetches
            .with_label_values(&["default", "ok"])
            .get(),
        2
    );
    // Restorable offline, like a path backup.
    let restored = s.dir.path().join("restored");
    chron::restore_backup(&fetched_to, &restored, &chron::Options::new()).unwrap();
    let db = chron::Db::open(&restored, chron::Options::new().fsync(chron::Fsync::Never)).unwrap();
    assert_eq!(db.head(), 300);
    s.stop().await;

    // No --backup-dir: Backup is refused, FetchBackup works.
    let s = Server::start().await;
    let client = Client::connect(s.addr(), ClientConfig::default())
        .await
        .unwrap();
    let r = client.backup("x.chronbak", None).await;
    assert!(
        matches!(r, Err(ClientError::FailedPrecondition(_))),
        "{r:?}"
    );
    let meta = client
        .fetch_backup_to(s.dir.path().join("f.chronbak"))
        .await
        .unwrap();
    assert_eq!(meta.head, 0);
    // An unknown database leaves neither the file nor its .tmp behind.
    let r = client
        .database("nope")
        .fetch_backup_to(s.dir.path().join("none.chronbak"))
        .await;
    assert!(matches!(r, Err(ClientError::NotFound(_))), "{r:?}");
    assert!(!s.dir.path().join("none.chronbak").exists());
    assert!(!s.dir.path().join("none.chronbak.tmp").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fetch_dropped_midway_releases_the_leader() {
    let s = Server::start().await;
    // More chunks than the stream buffers (four of a mebibyte), so the
    // writer is still at work when the client goes away.
    fill_bytes(&s, 2500, 4096).await;
    let client = Client::connect(s.addr(), ClientConfig::default())
        .await
        .unwrap();
    let mut stream = client.fetch_backup().await.unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert!(!first.is_empty());
    drop(stream);
    // Retention needs the maintenance lock the backup held.
    let r = tokio::time::timeout(Duration::from_secs(5), client.retain(1500)).await;
    assert!(
        matches!(r, Ok(Ok(_))),
        "retention waited on a dropped fetch: {r:?}"
    );
    let node = s.running.as_ref().unwrap().node.clone();
    crate::common::wait_until("the fetch counted as cancelled", || {
        let node = node.clone();
        async move {
            node.metrics
                .backup_fetches
                .with_label_values(&["default", "cancelled"])
                .get()
                == 1
        }
    })
    .await;
    s.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_backup_needs_the_admin_token() {
    let s = Server::start_with_no_client(|o| {
        o.token = Some("t".into());
        o.admin_token = Some("a".into());
    })
    .await;
    let plain = Client::connect(
        s.addr(),
        ClientConfig {
            token: Some("t".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let r = plain.fetch_backup().await.err();
    assert!(matches!(r, Some(ClientError::PermissionDenied(_))), "{r:?}");
    let admin = Client::connect(
        s.addr(),
        ClientConfig {
            token: Some("t".into()),
            admin_token: Some("a".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let stream = admin.fetch_backup().await.unwrap();
    drop(stream);
    s.stop().await;
}
