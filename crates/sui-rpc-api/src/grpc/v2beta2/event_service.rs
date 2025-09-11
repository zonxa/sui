// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::RpcService;
use sui_rpc::proto::sui::rpc::v2beta2::event_service_server::EventService;
use sui_rpc::proto::sui::rpc::v2beta2::{
    QueryAuthenticatedEventsRequest, QueryAuthenticatedEventsResponse,
};
use crate::grpc::v2beta2::query_authenticated_events;

#[tonic::async_trait]
impl EventService for RpcService {
    async fn query_authenticated_events(
        &self,
        request: tonic::Request<QueryAuthenticatedEventsRequest>,
    ) -> Result<tonic::Response<QueryAuthenticatedEventsResponse>, tonic::Status> {
        let req = request.into_inner();
        let resp: QueryAuthenticatedEventsResponse =
            query_authenticated_events::query_authenticated_events(self, req)
                .map_err(tonic::Status::from)?;
        Ok(tonic::Response::new(resp))
    }
}


