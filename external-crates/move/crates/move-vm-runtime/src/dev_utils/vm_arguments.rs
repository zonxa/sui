// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0
use crate::{
    execution::{
        TypeSubst as _,
        dispatch_tables::VMDispatchTables,
        interpreter::locals::{BaseHeap, BaseHeapId},
        values::{PrimVec, Struct, Value, Variant, Vector, VectorSpecialization},
        vm::MoveVM,
    },
    jit::execution::ast::Type,
};
use move_binary_format::errors::{Location, PartialVMError, PartialVMResult, VMResult};
use move_core_types::{
    identifier::IdentStr,
    language_storage::ModuleId,
    runtime_value::{MoveStruct, MoveValue, MoveVariant},
    vm_status::StatusCode,
};
use std::{borrow::Borrow, cell::RefCell, collections::BTreeMap, rc::Rc};
use tracing::warn;

// -------------------------------------------------------------------------------------------------
// Types
// -------------------------------------------------------------------------------------------------

/// Serialized return values from function/script execution
/// Simple struct is designed just to convey meaning behind serialized values
#[derive(Debug)]
pub struct ContextualizedValues {
    pub context: CallContext,
    /// The value of any arguments that were mutably borrowed.
    /// Non-mut borrowed values are not included
    /// Mapping is from argument index to the heap location
    pub heap_refs: BTreeMap<u16, BaseHeapId>,
    /// The values passed in with any references taken.
    pub values: Vec<Value>,
}

/// Rc and RefCell wrapper around CallContext_ to make it a bit easier and ergonomic to pass around
/// and mutate for testing purposes.
pub type CallContext = Rc<RefCell<BaseHeap>>;

// -------------------------------------------------------------------------------------------------
// Value Serialization and Deserialization
// -------------------------------------------------------------------------------------------------

pub fn new_call_context() -> CallContext {
    Rc::new(RefCell::new(BaseHeap::new()))
}

impl ContextualizedValues {
    pub fn no_refs(values: Vec<Value>) -> Self {
        Self {
            context: new_call_context(),
            heap_refs: BTreeMap::new(),
            values,
        }
    }
}

pub fn allocate_args_for_call_with_context(
    vm: &MoveVM<'_>,
    runtime_id: &ModuleId,
    function_name: &IdentStr,
    ty_args: &[Type],
    serialized_args: Vec<impl Borrow<[u8]>>,
    call_context: &CallContext,
) -> VMResult<ContextualizedValues> {
    let f = vm.find_function(runtime_id, function_name, ty_args)?;
    let arg_types = f
        .parameters
        .into_iter()
        .map(|ty| ty.subst(ty_args))
        .collect::<PartialVMResult<Vec<_>>>()
        .map_err(|err| err.finish(Location::Undefined))?;
    deserialize_args(&vm.virtual_tables, call_context, arg_types, serialized_args)
        .map_err(|e| e.finish(Location::Undefined))
}

pub fn allocate_args_for_call(
    vm: &MoveVM<'_>,
    runtime_id: &ModuleId,
    function_name: &IdentStr,
    ty_args: &[Type],
    serialized_args: Vec<impl Borrow<[u8]>>,
) -> VMResult<ContextualizedValues> {
    allocate_args_for_call_with_context(
        vm,
        runtime_id,
        function_name,
        ty_args,
        serialized_args,
        &new_call_context(),
    )
}

fn deserialize_value(
    vtables: &VMDispatchTables,
    ty: &Type,
    arg: impl Borrow<[u8]>,
) -> PartialVMResult<Value> {
    let layout = match vtables.type_to_type_layout(ty) {
        Ok(layout) => layout,
        Err(_err) => {
            warn!("[VM] failed to get layout from type");
            return Err(PartialVMError::new(
                StatusCode::INVALID_PARAM_TYPE_FOR_DESERIALIZATION,
            ));
        }
    };

    match Value::simple_deserialize(arg.borrow(), &layout) {
        Some(val) => Ok(val),
        None => {
            warn!("[VM] failed to deserialize argument");
            Err(PartialVMError::new(
                StatusCode::FAILED_TO_DESERIALIZE_ARGUMENT,
            ))
        }
    }
}

/// Returns the list of mutable references plus the vector of values.
pub fn deserialize_args(
    vtables: &VMDispatchTables,
    context: &CallContext,
    arg_tys: Vec<Type>,
    serialized_args: Vec<impl Borrow<[u8]>>,
) -> PartialVMResult<ContextualizedValues> {
    if arg_tys.len() != serialized_args.len() {
        return Err(
            PartialVMError::new(StatusCode::NUMBER_OF_ARGUMENTS_MISMATCH).with_message(format!(
                "argument length mismatch: expected {} got {}",
                arg_tys.len(),
                serialized_args.len()
            )),
        );
    }

    let mut heap_refs = BTreeMap::new();
    // Arguments for the invoked function. These can be owned values or references
    let deserialized_args = arg_tys
        .into_iter()
        .zip(serialized_args)
        .enumerate()
        .map(|(idx, (arg_ty, arg_bytes))| match &arg_ty {
            Type::MutableReference(inner_t) | Type::Reference(inner_t) => {
                // Each ref-arg value stored on the base heap, borrowed, and passed by
                // reference to the invoked function.
                let (ndx, value) = context
                    .borrow_mut()
                    .allocate_and_borrow_loc(deserialize_value(vtables, inner_t, arg_bytes)?)?;
                heap_refs.insert(idx as u16, ndx);
                Ok(value)
            }
            _ => deserialize_value(vtables, &arg_ty, arg_bytes),
        })
        .collect::<PartialVMResult<Vec<_>>>()?;
    Ok(ContextualizedValues {
        context: context.clone(),
        heap_refs,
        values: deserialized_args,
    })
}

pub fn vm_value_to_move_value(v: &Value) -> Option<MoveValue> {
    use PrimVec as PV;

    Some(match v {
        Value::U8(x) => MoveValue::U8(*x),
        Value::U16(x) => MoveValue::U16(*x),
        Value::U32(x) => MoveValue::U32(*x),
        Value::U64(x) => MoveValue::U64(*x),
        Value::U128(x) => MoveValue::U128(**x),
        Value::U256(x) => MoveValue::U256(**x),
        Value::Bool(x) => MoveValue::Bool(*x),
        Value::Address(x) => MoveValue::Address(**x),
        Value::Variant(entry) => {
            let (tag, values) = entry.as_ref();
            let tag = *tag; // Simply copy the u16 value, no need for dereferencing
            let fields = values
                .iter()
                .map(|v| vm_value_to_move_value(&v.borrow()))
                .collect::<Option<Vec<_>>>()?;
            MoveValue::Variant(MoveVariant { tag, fields })
        }

        // Struct case with direct access to Box
        Value::Struct(values) => {
            let fields = values
                .iter()
                .map(|v| vm_value_to_move_value(&v.borrow()))
                .collect::<Option<Vec<_>>>()?;
            MoveValue::Struct(MoveStruct::new(fields))
        }

        // Vector case with handling different container types
        Value::Vec(values) => MoveValue::Vector(
            values
                .iter()
                .map(|v| vm_value_to_move_value(&v.borrow()))
                .collect::<Option<_>>()?,
        ),
        Value::PrimVec(values) => {
            use MoveValue as MV;
            macro_rules! make_vec {
                ($xs:expr, $ctor:ident) => {
                    MV::Vector($xs.iter().map(|x| MV::$ctor(*x)).collect())
                };
            }
            match values {
                PV::VecU8(xs) => make_vec!(xs, U8),
                PV::VecU16(xs) => make_vec!(xs, U16),
                PV::VecU32(xs) => make_vec!(xs, U32),
                PV::VecU64(xs) => make_vec!(xs, U64),
                PV::VecU128(xs) => make_vec!(xs, U128),
                PV::VecU256(xs) => make_vec!(xs, U256),
                PV::VecBool(xs) => make_vec!(xs, Bool),
                PV::VecAddress(xs) => make_vec!(xs, Address),
            }
        }

        val => panic!("Cannot convert value {:?}", val),
    })
}

pub fn vm_value_from_move_value(v: &MoveValue) -> Option<Value> {
    use MoveValue as RV;
    Some(match v {
        RV::U8(x) => Value::u8(*x),
        RV::U16(x) => Value::u16(*x),
        RV::U32(x) => Value::u32(*x),
        RV::U64(x) => Value::u64(*x),
        RV::U128(x) => Value::u128(*x),
        RV::U256(x) => Value::u256(*x),
        RV::Bool(x) => Value::bool(*x),
        RV::Address(x) => Value::address(*x),
        RV::Signer(x) => Value::signer(*x),
        RV::Vector(xs) => {
            if xs.is_empty() {
                return Some(Value::Vec(vec![]));
            }
            let specialization = match xs[0] {
                RV::U8(_) => VectorSpecialization::U8,
                RV::U64(_) => VectorSpecialization::U64,
                RV::U128(_) => VectorSpecialization::U128,
                RV::Bool(_) => VectorSpecialization::Bool,
                RV::Address(_) => VectorSpecialization::Address,
                RV::U16(_) => VectorSpecialization::U16,
                RV::U32(_) => VectorSpecialization::U32,
                RV::U256(_) => VectorSpecialization::U256,
                RV::Variant(_) | RV::Vector(_) | RV::Struct(_) => VectorSpecialization::Container,
                RV::Signer(_) => return None,
            };
            Vector::pack(
                specialization,
                xs.iter()
                    .map(vm_value_from_move_value)
                    .collect::<Option<Vec<_>>>()?,
            )
            .ok()?
        }
        RV::Struct(s) => {
            let vals = s
                .fields()
                .iter()
                .map(vm_value_from_move_value)
                .collect::<Option<Vec<_>>>()?;
            Value::struct_(Struct::pack(vals))
        }
        RV::Variant(v) => {
            let vals = v
                .fields()
                .iter()
                .map(vm_value_from_move_value)
                .collect::<Option<Vec<_>>>()?;
            Value::variant(Variant::pack(v.tag, vals))
        }
    })
}
