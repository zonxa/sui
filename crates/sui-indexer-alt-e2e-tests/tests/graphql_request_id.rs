// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sui_indexer_alt::config::{IndexerConfig, PipelineLayer};
use sui_indexer_alt::BootstrapGenesis;
use sui_indexer_alt_e2e_tests::{
    local_ingestion_client_args, OffchainCluster, OffchainClusterConfig,
};
use sui_indexer_alt_schema::{checkpoints::StoredGenesis, epochs::StoredEpochStart};
use sui_types::sui_system_state::{mock, SuiSystemState};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn request_id() -> Result<(), anyhow::Error> {
    let request_id: String = test_graphql().await?;
    // value is random
    assert_eq!(request_id.len(), 36);
    Ok(())
}

async fn test_graphql<T: DeserializeOwned>() -> anyhow::Result<T> {
    telemetry_subscribers::init_for_testing();
    let (client_args, _temp_dir) = local_ingestion_client_args();
    let offchain = OffchainCluster::new(
        client_args,
        OffchainClusterConfig {
            indexer_config: IndexerConfig {
                pipeline: PipelineLayer {
                    // need at least one pipeline to start OffchainCluster
                    cp_sequence_numbers: Some(Default::default()),
                    ..Default::default()
                },
                ..Default::default()
            },
            bootstrap_genesis: Some(BootstrapGenesis {
                stored_genesis: StoredGenesis {
                    genesis_digest: [1u8; 32].to_vec(),
                    initial_protocol_version: 0,
                },
                stored_epoch_start: StoredEpochStart {
                    epoch: 0,
                    protocol_version: 0,
                    cp_lo: 0,
                    start_timestamp_ms: 0,
                    reference_gas_price: 0,
                    system_state: bcs::to_bytes(&SuiSystemState::V1(
                        mock::sui_system_state_inner_v1(),
                    ))?,
                },
            }),
            ..Default::default()
        },
        &prometheus::Registry::new(),
        CancellationToken::new(),
    )
    .await?;

    let query = json!({"query": "query { requestId }"});
    let client = Client::new();

    let request = client.post(offchain.graphql_url()).json(&query);
    let response = request.send().await?;

    offchain.stopped().await;

    let value: Value = response.json().await?;
    let Some(request_id) = value.pointer("/data/requestId") else {
        return Err(anyhow::Error::msg("requestId not found"));
    };
    Ok(serde_json::from_value(request_id.clone())?)
}
