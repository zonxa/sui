// Copyright (c) The Diem Core Contributors
// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, thread};

use crate::{
    dev_utils::{
        compilation_utils::{as_module, compile_units},
        in_memory_test_adapter::InMemoryTestAdapter,
        storage::StoredPackage,
        vm_test_adapter::VMTestAdapter,
    },
    execution::vm::MoveVM,
    runtime::telemetry::MoveRuntimeTelemetry,
    shared::gas::UnmeteredGasMeter,
};
use move_binary_format::errors::VMResult;
use move_core_types::{
    account_address::AccountAddress,
    identifier::Identifier,
    language_storage::{ModuleId, TypeTag},
    runtime_value::{MoveStruct, MoveValue},
    u256::U256,
    vm_status::StatusCode,
};

const TEST_ADDR: AccountAddress = AccountAddress::new([42; AccountAddress::LENGTH]);

fn make_adapter() -> InMemoryTestAdapter {
    let code = format!(
        r#"
        module 0x{}::M {{
            public struct Foo has copy, drop {{ x: u64 }}
            public struct Bar<T> has copy, drop {{ x: T }}

            fun foo() {{ }}

            fun bar(): u64 {{
                let mut x = 0;
                while (x < 1000) {{
                    x = x + 1;
                }};
                x
            }}
        }}
    "#,
        TEST_ADDR
    );

    let mut units = compile_units(&code).unwrap();
    let m = as_module(units.pop().unwrap());

    let mut adapter = InMemoryTestAdapter::new();
    let pkg = StoredPackage::from_modules_for_testing(TEST_ADDR, vec![m.clone()]).unwrap();
    adapter.insert_package_into_storage(pkg);
    adapter
}

fn make_vm(adapter: &InMemoryTestAdapter) -> MoveVM {
    let linkage = adapter.get_linkage_context(TEST_ADDR).unwrap();
    adapter.make_vm(linkage).unwrap()
}

fn call_foo(vm: &mut MoveVM) -> VMResult<()> {
    let module_id = ModuleId::new(TEST_ADDR, Identifier::new("M").unwrap());
    let fun_name = Identifier::new("foo").unwrap();
    vm.execute_function_bypass_visibility(
        &module_id,
        &fun_name,
        vec![],
        Vec::<Vec<u8>>::new(),
        &mut UnmeteredGasMeter,
        None,
    )?;
    Ok(())
}

fn call_bar(vm: &mut MoveVM) -> VMResult<()> {
    let module_id = ModuleId::new(TEST_ADDR, Identifier::new("M").unwrap());
    let fun_name = Identifier::new("foo").unwrap();
    vm.execute_function_bypass_visibility(
        &module_id,
        &fun_name,
        vec![],
        Vec::<Vec<u8>>::new(),
        &mut UnmeteredGasMeter,
        None,
    )?;
    Ok(())
}
#[test]
fn basic_telemetry() {
    let adapter = make_adapter();
    let mut vm = make_vm(&adapter);

    let telemetry = adapter.get_telemetry_report();
    // Test that we can get telemetry, and it recorded reasonable things.
    assert_eq!(telemetry.package_cache_count, 1);
    assert_eq!(telemetry.total_arena_size, 3392);
    assert_eq!(telemetry.module_count, 1);
    assert_eq!(telemetry.function_count, 2);
    assert_eq!(telemetry.type_count, 2);
    assert_eq!(telemetry.interner_size, 4096);
    assert_eq!(telemetry.load_count, 1);
    assert_eq!(telemetry.validation_count, 1);
    assert_eq!(telemetry.jit_count, 1);
    assert_eq!(telemetry.execution_count, 0);
    assert_eq!(telemetry.interpreter_count, 0);
    assert_eq!(telemetry.total_count, 1);

    let _ = call_foo(&mut vm);

    // === After call_foo ===
    let telemetry = adapter.get_telemetry_report();
    assert_eq!(telemetry.package_cache_count, 1);
    assert_eq!(telemetry.total_arena_size, 3392);
    assert_eq!(telemetry.module_count, 1);
    assert_eq!(telemetry.function_count, 2);
    assert_eq!(telemetry.type_count, 2);
    assert_eq!(telemetry.interner_size, 4096);
    assert_eq!(telemetry.load_count, 1); // unchanged
    assert_eq!(telemetry.validation_count, 1); // unchanged
    assert_eq!(telemetry.jit_count, 1); // unchanged
    assert_eq!(telemetry.execution_count, 1); // 0 -> 1 after call_foo
    assert_eq!(telemetry.interpreter_count, 1); // 0 -> 1 after call_foo
    assert_eq!(telemetry.total_count, 2); // increased by 1

    let _ = call_bar(&mut vm);

    // === After call_bar ===
    let telemetry = adapter.get_telemetry_report();
    assert_eq!(telemetry.package_cache_count, 1);
    assert_eq!(telemetry.total_arena_size, 3392);
    assert_eq!(telemetry.module_count, 1);
    assert_eq!(telemetry.function_count, 2);
    assert_eq!(telemetry.type_count, 2);
    assert_eq!(telemetry.interner_size, 4096);
    assert_eq!(telemetry.load_count, 1); // unchanged
    assert_eq!(telemetry.validation_count, 1); // unchanged
    assert_eq!(telemetry.jit_count, 1); // unchanged
    assert_eq!(telemetry.execution_count, 2); // 1 -> 2 after call_bar
    assert_eq!(telemetry.interpreter_count, 2); // 1 -> 2 after call_bar
    assert_eq!(telemetry.total_count, 3); // increased by 1
}

#[test]
fn parallel_telemetry() {
    // Create the shared adapter.
    let adapter = Arc::new(make_adapter());
    let num_threads = 100;
    let mut handles = Vec::with_capacity(num_threads);

    // Spawn 10 threads.
    for i in 0..num_threads {
        let adapter = adapter.clone();
        // Each thread will create its own VM.
        handles.push(thread::spawn(move || {
            let mut vm = make_vm(&adapter);
            // Alternate between call_foo and call_bar based on the thread index.
            if i % 2 == 0 {
                call_foo(&mut vm).expect("call_foo failed");
            } else {
                call_bar(&mut vm).expect("call_bar failed");
            }
        }));
    }

    // Wait for all threads to complete.
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Get the telemetry report after all parallel calls.
    let telemetry = adapter.get_telemetry_report();

    // In our basic setup, before any calls:
    //   package_cache_count:   1
    //   total_arena_size:      3392
    //   module_count:          1
    //   function_count:        2
    //   type_count:            2
    //   interner_size:         4096
    //   load_count:            1
    //   validation_count:      1
    //   jit_count:             1
    //   execution_count:       0
    //   interpreter_count:     0
    //   total_count:           1
    //
    // Each call (via call_foo or call_bar) records a transaction that increments:
    //   execution_count, interpreter_count, and total_count (+1 each per call).
    // With 10 calls running in parallel we expect:
    //   execution_count:   10
    //   interpreter_count: 10
    //   total_count:       11 (the initial value 1 plus 10 calls)
    //
    // All other fields remain unchanged.
    assert_eq!(telemetry.package_cache_count, 1);
    assert_eq!(telemetry.total_arena_size, 3392);
    assert_eq!(telemetry.module_count, 1);
    assert_eq!(telemetry.function_count, 2);
    assert_eq!(telemetry.type_count, 2);
    assert_eq!(telemetry.interner_size, 4096);
    assert_eq!(telemetry.load_count, 1);
    assert_eq!(telemetry.validation_count, 1);
    assert_eq!(telemetry.jit_count, 1);
    assert_eq!(telemetry.execution_count, num_threads as u64);   // 10 calls executed
    assert_eq!(telemetry.interpreter_count, num_threads as u64);   // 10 calls executed
    assert_eq!(telemetry.total_count, num_threads as u64 + 1);         // initial count (1) + 10 calls
}
