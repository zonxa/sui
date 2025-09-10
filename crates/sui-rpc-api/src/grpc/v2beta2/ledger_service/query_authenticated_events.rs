// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::RpcError;
use crate::RpcService;
use bytes::Bytes;
use move_core_types::language_storage::{StructTag, TypeTag};
use std::str::FromStr;
use std::sync::Arc;
use sui_rpc::proto::sui::rpc::v2beta2::{
    AuthenticatedEvent, EventStreamHead, Proof, QueryAuthenticatedEventsRequest,
    QueryAuthenticatedEventsResponse,
};
use sui_types::effects::TransactionEffectsAPI;
use sui_types::MoveTypeTagTraitGeneric;

fn load_event_stream_head(
    reader: &Arc<dyn sui_types::storage::RpcStateReader>,
    stream_id: &str,
    at_checkpoint: u64,
) -> Option<EventStreamHead> {
    #[derive(serde::Deserialize)]
    struct MoveEventStreamHead {
        mmr: Vec<Vec<u8>>,
        checkpoint_seq: u64,
        num_events: u64,
    }
    let stream_address = sui_types::base_types::SuiAddress::from_str(stream_id).ok()?;
    let key_type_tag = sui_types::accumulator_root::event_stream_head_key_type_tag();
    let id = sui_types::accumulator_root::event_stream_head_dynamic_field_id(stream_address)?;

    // Find the dynamic field object's version written in `at_checkpoint` only.
    let contents = reader.get_checkpoint_contents_by_sequence_number(at_checkpoint)?;
    let mut version: Option<sui_types::base_types::SequenceNumber> = None;
    for exec in contents.iter() {
        let tx = exec.transaction;
        if let Some(effects) = reader.get_transaction_effects(&tx) {
            for (obj_id, ver, _dig) in effects.written() {
                if obj_id == id {
                    version = Some(ver);
                    break;
                }
            }
            if version.is_some() {
                break;
            }
        }
    }

    let version = version?;
    let obj = reader.get_object_by_key(&id, version)?;
    let mo = obj.data.try_as_move()?;
    let field = mo.to_rust::<sui_types::dynamic_field::Field<
        sui_types::accumulator_root::AccumulatorKey,
        MoveEventStreamHead,
    >>()?;

    let mut out = EventStreamHead::default();
    out.mmr = field.value.mmr.into_iter().map(Bytes::from).collect();
    out.checkpoint_seq = Some(field.value.checkpoint_seq);
    out.num_events = Some(field.value.num_events);
    Some(out)
}

#[tracing::instrument(skip(_service))]
pub fn query_authenticated_events(
    _service: &RpcService,
    request: QueryAuthenticatedEventsRequest,
) -> Result<QueryAuthenticatedEventsResponse, RpcError> {
    let stream_id = request.stream_id.unwrap_or_default();
    let start = request.start_checkpoint.unwrap_or(0);
    let end = request.end_checkpoint.unwrap_or(u64::MAX);

    if stream_id.is_empty() {
        return Ok(QueryAuthenticatedEventsResponse::default());
    }
    if end < start {
        return Ok(QueryAuthenticatedEventsResponse::default());
    }

    let reader = _service.reader.inner();
    let indexes = match reader.indexes() {
        Some(ix) => ix,
        None => return Ok(QueryAuthenticatedEventsResponse::default()),
    };

    let stream_addr = match sui_types::base_types::SuiAddress::from_str(&stream_id) {
        Ok(addr) => addr,
        Err(_) => return Ok(QueryAuthenticatedEventsResponse::default()),
    };

    let mut events = Vec::new();
    let mut last_checkpoint_with_events: Option<u64> = None;
    let mut iter = indexes
        .authenticated_event_iter(stream_addr, start, end)
        .map_err(|e| RpcError::new(tonic::Code::Internal, e.to_string()))?;
    while let Some(item) = iter
        .next()
        .transpose()
        .map_err(|e| RpcError::new(tonic::Code::Internal, e.to_string()))?
    {
        let (cp, txd, idx, bytes) = item;
        last_checkpoint_with_events = Some(cp);
        let mut ae = AuthenticatedEvent::default();
        ae.checkpoint = Some(cp);
        ae.tx_digest = Some(txd.to_string());
        ae.event_index = Some(idx);
        ae.move_event = Some(Bytes::from(bytes));
        ae.stream_id = Some(stream_id.clone());
        events.push(ae);
    }

    // Load EventStreamHead from the accumulator dynamic field, if available.
    let event_stream_head = last_checkpoint_with_events
        .and_then(|cp| load_event_stream_head(reader, &stream_id, cp));
    let mut resp = QueryAuthenticatedEventsResponse::default();
    resp.events = events;
    resp.proof = event_stream_head.map(|esh| {
        let mut p = Proof::default();
        p.event_stream_head = Some(esh);
        p
    });
    Ok(resp)
}
