module counter::simple {
    public fun add(a: u64, b: u64): u64 {
        a + b
    }

    public fun multiply(a: u64, b: u64): u64 {
        a * b
    }
}