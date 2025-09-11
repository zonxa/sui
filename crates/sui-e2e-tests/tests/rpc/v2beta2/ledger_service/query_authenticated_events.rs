// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use sui_macros::sim_test;
use sui_protocol_config::ProtocolConfig;
use sui_rpc::proto::sui::rpc::v2beta2::ledger_service_client::LedgerServiceClient;
use sui_rpc::proto::sui::rpc::v2beta2::{Event, QueryAuthenticatedEventsRequest};
use sui_keys::keystore::AccountKeystore;
use serde::{Deserialize, Serialize};
use sui_types::programmable_transaction_builder::ProgrammableTransactionBuilder;
use sui_types::transaction::TransactionData;
use test_cluster::TestClusterBuilder;

#[derive(Deserialize, Serialize)]
struct AuthEventPayload {
    value: u64,
}

#[sim_test]
async fn query_authenticated_events_end_to_end() {
    let _guard: sui_protocol_config::OverrideGuard = ProtocolConfig::apply_overrides_for_testing(|_, mut cfg| {
        cfg.enable_accumulators_for_testing();
        cfg
    });

    let mut test_cluster = TestClusterBuilder::new().build().await;
    let rgp = test_cluster.get_reference_gas_price().await;

    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/rpc/data/auth_event");
    let (package_id, _, _) = {
        sui_test_transaction_builder::publish_package(&mut test_cluster.wallet, path).await
    };

    // Submit 10 transactions, one authenticated event per transaction
    let sender = test_cluster.wallet.config.keystore.addresses()[0];
    for i in 1..=10u64 {
        let emit_value = 100 + i;
        let mut ptb_i = ProgrammableTransactionBuilder::new();
        let val_i = ptb_i.pure(emit_value).unwrap();
        ptb_i.programmable_move_call(
            package_id,
            move_core_types::identifier::Identifier::new("events").unwrap(),
            move_core_types::identifier::Identifier::new("emit").unwrap(),
            vec![],
            vec![val_i],
        );
        let tx_data_i = TransactionData::new(
            sui_types::transaction::TransactionKind::ProgrammableTransaction(ptb_i.finish()),
            sender,
            {
                let wallet = &mut test_cluster.wallet;
                wallet
                    .gas_objects(sender)
                    .await
                    .unwrap()
                    .pop()
                    .unwrap()
                    .1
                    .object_ref()
            },
            10_000_000,
            rgp,
        );
        test_cluster.sign_and_execute_transaction(&tx_data_i).await;
    }

    let mut client = LedgerServiceClient::connect(test_cluster.rpc_url().to_owned())
        .await
        .unwrap();

    let mut req = QueryAuthenticatedEventsRequest::default();
    req.stream_id = Some(package_id.to_string());
    req.start_checkpoint = Some(0);
    req.end_checkpoint = Some(u64::MAX);
    let resp = client
        .query_authenticated_events(req)
        .await
        .unwrap()
        .into_inner();
    let count = resp.events.len();
    assert_eq!(count, 10, "expected 10 authenticated events, got {count}");
    let found = resp.events.iter().any(|e| match &e.event {
        Some(Event { contents: Some(bcs), .. }) => {
            !bcs.value.clone().unwrap_or_default().is_empty()
        }
        _ => false,
    });
    assert!(found, "expected authenticated event for the stream");
}



