use crate::common::*;

#[tokio::test(flavor = "multi_thread")]
async fn scenarios_through_a_daemon() {
    let (running, client, _d) = remote().await;
    open_deposit_withdraw_history(&client).await;
    overdraft_is_refused_and_nothing_written(&client).await;
    closed_refuses_and_open_is_guarded(&client).await;
    concurrent_withdrawals_never_overdraw(&client).await;
    balances_projection_equals_replayed_states(&client).await;
    running.shutdown().await.unwrap();
}
