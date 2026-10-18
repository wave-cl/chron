use bank::{Balances, Embedded, commands};
use chron::{Fsync, Options};

use crate::common::*;

#[tokio::test(flavor = "multi_thread")]
async fn scenarios_embedded() {
    let (s, _d) = embedded();
    open_deposit_withdraw_history(&s).await;
    overdraft_is_refused_and_nothing_written(&s).await;
    closed_refuses_and_open_is_guarded(&s).await;
    concurrent_withdrawals_never_overdraw(&s).await;
    balances_projection_equals_replayed_states(&s).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_keeps_everything() {
    let (s, d) = embedded();
    let id = commands::open(&s, "erin").await.unwrap();
    commands::deposit(&s, id, 42).await.unwrap();
    let mut b = Balances::default();
    b.catch_up(&s).await.unwrap();
    drop(s);
    let s = Embedded(
        chron::Db::open(d.path().join("bank"), Options::new().fsync(Fsync::Never)).unwrap(),
    );
    assert_eq!(commands::load(&s, id).await.unwrap().balance, 42);
    let mut again = Balances::default();
    again.catch_up(&s).await.unwrap();
    assert_eq!(again, b);
}
