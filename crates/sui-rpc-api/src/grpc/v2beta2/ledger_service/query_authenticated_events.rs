// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::RpcError;
use crate::RpcService;
use bytes::Bytes;
use std::collections::BTreeMap;
use sui_rpc::proto::sui::rpc::v2beta2::AuthenticatedEvent;
use sui_rpc::proto::sui::rpc::v2beta2::EventStreamHead;
use sui_rpc::proto::sui::rpc::v2beta2::QueryAuthenticatedEventsRequest;
use sui_rpc::proto::sui::rpc::v2beta2::QueryAuthenticatedEventsResponse;

// Minimal in-memory mock dataset: (stream_id, checkpoint) -> Vec<AuthenticatedEvent>
// For MVP/demo only. Replace with index-backed query later.
fn mock_events() -> BTreeMap<(String, u64), Vec<AuthenticatedEvent>> {
    let mut m = BTreeMap::new();

    let s1 = "0x1111".to_string();
    let s2 = "0x2222".to_string();

    {
        let mut e = AuthenticatedEvent::default();
        e.checkpoint = Some(10);
        e.tx_digest = Some("txa".to_string());
        e.event_index = Some(0);
        e.move_event = Some(Bytes::from(vec![1u8, 2, 3]));
        e.stream_id = Some(s1.clone());
        m.insert((s1.clone(), 10), vec![e]);
    }
    {
        let mut e = AuthenticatedEvent::default();
        e.checkpoint = Some(11);
        e.tx_digest = Some("txb".to_string());
        e.event_index = Some(0);
        e.move_event = Some(Bytes::from(vec![4u8, 5]));
        e.stream_id = Some(s1.clone());
        m.insert((s1.clone(), 11), vec![e]);
    }

    {
        let mut e = AuthenticatedEvent::default();
        e.checkpoint = Some(10);
        e.tx_digest = Some("txc".to_string());
        e.event_index = Some(0);
        e.move_event = Some(Bytes::from(vec![9u8]));
        e.stream_id = Some(s2.clone());
        m.insert((s2.clone(), 10), vec![e]);
    }

    m
}

fn query_from_mock(stream_id: &str, start: u64, end: u64) -> (Vec<AuthenticatedEvent>, Option<EventStreamHead>) {
    let data = mock_events();
    let mut events = Vec::new();
    let mut last_cp = None;
    let mut total_events = 0u64;

    for cp in start..=end {
        if let Some(v) = data.get(&(stream_id.to_string(), cp)) {
            last_cp = Some(cp);
            total_events += v.len() as u64;
            events.extend(v.clone());
        }
    }

    let event_stream_head = last_cp.map(|cp| {
        let mut h = EventStreamHead::default();
        // Minimal mock values to satisfy shape equivalence with Move's EventStreamHead
        h.mmr = vec![Bytes::from_static(&[0u8; 32])];
        h.checkpoint_seq = Some(cp);
        h.num_events = Some(total_events);
        h
    });
    (events, event_stream_head)
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

    let (events, event_stream_head) = query_from_mock(&stream_id, start, end);
    let mut resp = QueryAuthenticatedEventsResponse::default();
    resp.events = events;
    resp.event_stream_head = event_stream_head;
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_events_in_range_and_head() {
        let (events, head) = query_from_mock("0x1111", 10, 11);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].checkpoint, Some(10));
        assert_eq!(events[1].checkpoint, Some(11));
        let head = head.unwrap();
        assert_eq!(head.checkpoint_seq, Some(11));
        assert_eq!(head.num_events, Some(2));
        assert!(!head.mmr.is_empty());
    }
}


