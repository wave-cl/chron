use chron_proto::v1 as pb;
use tonic::Code;

use crate::common::*;

#[tokio::test(flavor = "multi_thread")]
async fn info_flush_shutdown_and_lock() {
    let mut s = Server::start().await;
    fill(&mut s.client, agg(1), 3).await;
    let info = s
        .client
        .info(pb::InfoRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!((info.head, info.floor, info.segments), (3, 0, 1));
    assert!(info.bytes > 64);
    assert_eq!(info.database, "default");
    assert_eq!(
        info.path,
        s.data_dir().join("default").display().to_string()
    );
    assert!(!info.options.unwrap().fsync);
    s.client.flush(pb::FlushRequest {}).await.unwrap();

    let dir = s.data_dir();
    let mut opts = chrond::Options::new(&dir, "127.0.0.1:0".parse().unwrap());
    opts.fsync = false;
    let err = match chrond::start(opts).await {
        Ok(_) => panic!("started on a locked directory"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("cannot open"), "{err}");

    let _keep = s.stop().await;
    let db = chron::Db::open(
        dir.join("default"),
        chron::Options::new().fsync(chron::Fsync::Never),
    )
    .unwrap();
    assert_eq!(db.head(), 3, "shutdown flushed");
}

#[tokio::test(flavor = "multi_thread")]
async fn backup_over_rpc_restores_offline_and_retain_moves_the_floor() {
    let mut s = Server::start_with(with_backup_dir).await;
    fill(&mut s.client, agg(1), 5).await;
    let archive = s.dir.path().join("backups/default/full.chronbak");
    let meta = s
        .client
        .backup(pb::BackupRequest {
            archive: "full.chronbak".into(),
            since: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!((meta.incremental, meta.head), (false, 5));
    fill(&mut s.client, agg(1), 2).await;
    let inc = s.dir.path().join("backups/default/inc.chronbak");
    let meta = s
        .client
        .backup(pb::BackupRequest {
            archive: "inc.chronbak".into(),
            since: Some(5),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (meta.incremental, meta.base_head, meta.head),
        (true, Some(5), 7)
    );
    let wrong = s
        .client
        .apply_backup(pb::ApplyBackupRequest {
            archive: "inc.chronbak".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(wrong.code(), Code::FailedPrecondition);

    let restored = s.dir.path().join("restored");
    chron::restore_backup(&archive, &restored, &chron::Options::new()).unwrap();
    let r = chron::Db::open(&restored, chron::Options::new()).unwrap();
    assert_eq!(r.head(), 5);
    assert_eq!(r.apply_backup(&inc).unwrap().head, 7);
    assert_eq!(r.head(), 7);
    drop(r);

    let retained = s
        .client
        .retain(pb::RetainRequest { floor: 7 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        retained.segments_removed, 0,
        "one segment: nothing below it to drop"
    );
    let info = s
        .client
        .info(pb::InfoRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.floor, retained.floor);
    let past = s
        .client
        .retain(pb::RetainRequest { floor: 99 })
        .await
        .unwrap_err();
    assert_eq!(past.code(), Code::OutOfRange);
    s.stop().await;
}
