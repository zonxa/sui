// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use moka::sync::Cache as MokaCache;
use sui_types::{
    base_types::{ObjectID, ObjectRef},
    coin::CoinMetadata,
    coin_reservation::{self, CoinReservationResolverTrait},
    error::{SuiError, UserInputError},
    transaction::FundsWithdrawalArg,
    type_input::TypeInput,
    TypeTag,
};

use crate::execution_cache::ObjectCacheRead;

pub struct CoinReservationResolver {
    object_cache_read: Arc<dyn ObjectCacheRead>,
    object_id_to_type_cache: MokaCache<ObjectID, TypeInput>,
}

impl CoinReservationResolver {
    pub fn new(object_cache_read: Arc<dyn ObjectCacheRead>) -> Self {
        Self {
            object_cache_read,
            object_id_to_type_cache: MokaCache::builder().max_capacity(1000).build(),
        }
    }

    fn get_type_input_for_object(&self, object: &ObjectID) -> SuiResult<TypeInput> {
        let type_input = self.object_id_to_type_cache.get(object);
        if type_input.is_some() {
            return type_input;
        }

        let object = self
            .object_cache_read
            .get_object(object)
            .ok_or(SuiError::UserInputError {
                user_input_error: UserInputError::InvalidWithdrawReservation {
                    error: format!("object id {} not found", object.id()),
                },
            })?;

        // Ensure that transaction is referencing an object that can never be deleted or wrapped
        match object.owner() {
            sui_types::object::Owner::Shared { .. } | sui_types::object::Owner::Immutable => (),
            _ => {
                return Err(SuiError::UserInputError {
                    user_input_error: UserInputError::InvalidWithdrawReservation {
                        error: format!("object id {} must be shared or immutable", object.id()),
                    },
                })
            }
        }

        let object_type: TypeTag = object.type_()?.clone().into();
        let TypeTag::Struct(object_struct) = object_type else {
            return Err(SuiError::UserInputError {
                user_input_error: UserInputError::InvalidWithdrawReservation {
                    error: format!("object id {} is not a coin metadata object", object.id()),
                },
            });
        };
        let coin_struct = CoinMetadata::is_coin_metadata_with_coin_type(&object_struct).ok_or(
            SuiError::UserInputError {
                user_input_error: UserInputError::InvalidWithdrawReservation {
                    error: format!("object id {} is not a coin metadata", object.id()),
                },
            },
        )?;
        let coin_type = TypeTag::Struct(coin_struct.clone().into());
        let coin_type_input = TypeInput::from(coin_type);
        self.object_id_to_type_cache
            .insert(object.id(), coin_type_input.clone());
        Ok(coin_type_input)
    }

    pub fn resolve_funds_withdrawal(
        &self,
        coin_reservation: ObjectRef,
    ) -> SuiResult<FundsWithdrawalArg> {
        // Should only be called on valid coin reservation object refs
        let parsed = coin_reservation::parse_object_ref(&coin_reservation)
            .expect("invalid coin reservation object ref");

        // object existence must be checked earlier
        let type_input = self.get_type_input_for_object(&parsed.unmasked_object_id)?;

        Ok(FundsWithdrawalArg::balance_from_sender(
            parsed.reservation_amount,
            type_input,
        ))
    }
}

impl CoinReservationResolverTrait for CoinReservationResolver {
    fn resolve_funds_withdrawal(
        &self,
        coin_reservation: ObjectRef,
    ) -> SuiResult<FundsWithdrawalArg> {
        self.resolve_funds_withdrawal(coin_reservation)
    }
}
