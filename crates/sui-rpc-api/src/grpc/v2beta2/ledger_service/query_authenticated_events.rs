// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::RpcError;
use crate::RpcService;
use bytes::Bytes;
use move_core_types::language_storage::{StructTag, TypeTag};
use sui_types::MoveTypeTagTraitGeneric;
use sui_types::accumulator_root as ar;
use std::str::FromStr;
use std::sync::Arc;
use sui_rpc::proto::sui::rpc::v2beta2::{
    AuthenticatedEvent, EventStreamHead, Proof, QueryAuthenticatedEventsRequest,
    QueryAuthenticatedEventsResponse, Event,
};
use sui_types::effects::TransactionEffectsAPI;

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
    let event_stream_head_object_id = {
        let module = ar::ACCUMULATOR_SETTLEMENT_MODULE.to_owned();
        let name = ar::ACCUMULATOR_SETTLEMENT_EVENT_STREAM_HEAD.to_owned();
        let tag = StructTag {
            address: sui_types::SUI_FRAMEWORK_ADDRESS,
            module,
            name,
            type_params: vec![],
        };
        let key_type_tag = ar::AccumulatorKey::get_type_tag(&[TypeTag::Struct(Box::new(tag))]);
        let df_key = sui_types::dynamic_field::DynamicFieldKey(
            sui_types::SUI_ACCUMULATOR_ROOT_OBJECT_ID,
            ar::AccumulatorKey { owner: stream_address },
            key_type_tag,
        );
        df_key.into_unbounded_id().ok()?.as_object_id()
    };

    let contents = reader.get_checkpoint_contents_by_sequence_number(at_checkpoint)?;
    let mut version: Option<sui_types::base_types::SequenceNumber> = None;
    for exec in contents.iter() {
        let tx = exec.transaction;
        if let Some(effects) = reader.get_transaction_effects(&tx) {
            for (obj_id, ver, _dig) in effects.written() {
                if obj_id == event_stream_head_object_id {
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
    let obj = reader.get_object_by_key(&event_stream_head_object_id, version)?;
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

    fn to_grpc_event(ev: &sui_types::event::Event) -> Event {
        let mut out = Event::default();
        out.package_id = Some(ev.package_id.to_canonical_string(true));
        out.module = Some(ev.transaction_module.to_string());
        out.sender = Some(ev.sender.to_string());
        out.event_type = Some(ev.type_.to_canonical_string(true));
        let mut bcs = sui_rpc::proto::sui::rpc::v2beta2::Bcs::default();
        bcs.value = Some(Bytes::from(ev.contents.clone()));
        out.contents = Some(bcs);
        out
    }

    fn to_authenticated_event(
        stream_id: &str,
        cp: u64,
        txd: &sui_types::base_types::TransactionDigest,
        idx: u32,
        ev: &sui_types::event::Event,
    ) -> AuthenticatedEvent {
        let mut ae = AuthenticatedEvent::default();
        ae.checkpoint = Some(cp);
        ae.tx_digest = Some(txd.to_string());
        ae.event_index = Some(idx);
        ae.event = Some(to_grpc_event(ev));
        ae.stream_id = Some(stream_id.to_string());
        ae
    }

    let iter = indexes
        .authenticated_event_iter(stream_addr, start, end)
        .map_err(|e| RpcError::new(tonic::Code::Internal, e.to_string()))?;
    let events: Vec<AuthenticatedEvent> = iter
        .map(|res| res.map(|(cp, txd, idx, ev)| to_authenticated_event(&stream_id, cp, &txd, idx, &ev)))
        .collect::<Result<_, _>>()
        .map_err(|e| RpcError::new(tonic::Code::Internal, e.to_string()))?;
    let last_checkpoint_with_events = events.last().and_then(|e| e.checkpoint);

    let event_stream_head = last_checkpoint_with_events
        .and_then(|last_checkpoint| load_event_stream_head(reader, &stream_id, last_checkpoint));
    let mut resp = QueryAuthenticatedEventsResponse::default();
    resp.events = events;
    resp.proof = event_stream_head.map(|esh| {
        let mut p = Proof::default();
        p.event_stream_head = Some(esh);
        p
    });
    Ok(resp)
}
