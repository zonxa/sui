// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::RpcError;
use crate::RpcService;
use bytes::Bytes;
use move_core_types::identifier::Identifier;
use move_core_types::language_storage::{StructTag, TypeTag};
use std::str::FromStr;
use std::sync::Arc;
use sui_rpc::proto::sui::rpc::v2beta2::{
    AuthenticatedEvent, EventStreamHead, Proof, QueryAuthenticatedEventsRequest,
    QueryAuthenticatedEventsResponse,
};
use sui_types::effects::TransactionEffectsAPI;
use sui_types::MoveTypeTagTraitGeneric;

// Extract the stream ID address from an accumulator event if it targets sui::accumulator_settlement::EventStreamHead
fn stream_id_from_accumulator_event(
    ev: &sui_types::accumulator_event::AccumulatorEvent,
) -> Option<sui_types::base_types::SuiAddress> {
    if let TypeTag::Struct(tag) = &ev.write.address.ty {
        if tag.address == sui_types::SUI_FRAMEWORK_ADDRESS
            && tag.module.as_ident_str() == move_core_types::ident_str!("accumulator_settlement")
            && tag.name.as_ident_str() == move_core_types::ident_str!("EventStreamHead")
        {
            return Some(ev.write.address.address);
        }
    }
    None
}

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
    let module = Identifier::new("accumulator_settlement").ok()?;
    let name = Identifier::new("EventStreamHead").ok()?;
    let tag = StructTag {
        address: sui_types::SUI_FRAMEWORK_ADDRESS,
        module,
        name,
        type_params: vec![],
    };
    let key_type_tag =
        sui_types::accumulator_root::AccumulatorKey::get_type_tag(&[TypeTag::Struct(Box::new(
            tag,
        ))]);
    let df_key = sui_types::dynamic_field::DynamicFieldKey(
        sui_types::SUI_ACCUMULATOR_ROOT_OBJECT_ID,
        sui_types::accumulator_root::AccumulatorKey { owner: stream_address },
        key_type_tag,
    );
    let id = df_key.into_unbounded_id().ok()?.as_object_id();

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

    // Scan checkpoints and, for each tx, pick only events that were committed to the
    // accumulator stream for this `stream_id` using effects.accumulator_events().
    let reader = _service.reader.inner();
    let mut events = Vec::new();
    let mut last_checkpoint_with_events: Option<u64> = None;
    let requested_stream_address = match sui_types::base_types::SuiAddress::from_str(&stream_id) {
        Ok(addr) => addr,
        Err(_) => return Ok(QueryAuthenticatedEventsResponse::default()),
    };

    for cp in start..=end {
        if let Some(contents) = reader.get_checkpoint_contents_by_sequence_number(cp) {
            for exec in contents.iter() {
                let tx = exec.transaction;
                if let Some(effects) = reader.get_transaction_effects(&tx) {
                    // Gather accumulator events that target our stream head by matching stream ID/address
                    let mut indices: Vec<u64> = Vec::new();
                    for acc in effects.accumulator_events() {
                        if let Some(event_stream_address) = stream_id_from_accumulator_event(&acc) {
                            if event_stream_address == requested_stream_address {
                                if let sui_types::effects::AccumulatorValue::EventDigest(idx, _d) = acc.write.value {
                                    indices.push(idx);
                                }
                            }
                        }
                    }
                    if !indices.is_empty() {
                        last_checkpoint_with_events = Some(cp);
                        if let Some(tx_events) = reader.get_events(effects.transaction_digest()) {
                            for idx in indices {
                                let ui = idx as usize;
                                if ui < tx_events.data.len() {
                                    let ev = &tx_events.data[ui];
                                    let mut ae = AuthenticatedEvent::default();
                                    ae.checkpoint = Some(cp);
                                    ae.tx_digest = Some(tx.to_string());
                                    ae.event_index = Some(idx as u32);
                                    ae.move_event = Some(Bytes::from(ev.contents.clone()));
                                    ae.stream_id = Some(stream_id.clone());
                                    events.push(ae);
                                }
                            }
                        }
                    }
                }
            }
        }
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
