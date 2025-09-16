// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use sui_types::{
    accumulator_root::AccumulatorValue,
    error::{SuiError, SuiResult, UserInputError},
};

use crate::execution_cache::ObjectCacheRead;

/// Checks if balances are available in the latest versions of the referenced acccumulator
/// objects. This does un-sequenced reads and can only be used on the signing/voting path
/// where deterministic results are not required.
pub fn check_balances_available(
    object_cache_read: &dyn ChildObjectResolver,
    requested_balances: &BTreeMap<AccumulatorObjId, u64>,
) -> SuiResult<()> {
    for (object_id, balance) in requested_balances {
        let accum_value = AccumulatorValue::load_by_id(child_object_resolver, None, *object_id)?;

        if balance == 0 {
            return Err(SuiError::UserInputError {
                user_input_error: UserInputError::InvalidWithdrawReservation {
                    error: format!("balance for object id {} is 0", object_id),
                },
            });
        }
    }
}
