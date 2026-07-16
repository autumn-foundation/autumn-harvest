// echo.ts — a Harvest WASM activity guest written in AssemblyScript (issue #965).
//
// ILLUSTRATIVE, not compiled in CI. It is here to show that the JSON-over-linear-
// memory ABI (see README.md) is genuinely language-agnostic: this AssemblyScript
// guest implements the SAME `alloc` / `run` contract as the hand-written
// `echo.wat` that CI actually executes. Build it to `.wasm` with the `asc`
// command in README.md and it can be published for an activity exactly like the
// WAT guest.
//
// The ABI a guest must satisfy:
//   * export `memory`            — its linear memory.
//   * export `alloc(len) -> ptr` — return `len` writable bytes; the host writes
//                                  the serialized JSON input there once.
//   * export `run(in_ptr, in_len) -> i64` — execute; return the output location
//                                  packed as `(out_ptr << 32) | out_len`. The
//                                  host reads `out_len` JSON bytes at `out_ptr`.

// Bump allocator over a fixed heap offset. A real guest would deserialize the
// input JSON at `in_ptr..in_ptr+in_len`, do its work, serialize a JSON result
// into freshly `alloc`ed bytes, and return that (out_ptr, out_len).
let bump: i32 = 1024;

export function alloc(len: i32): i32 {
  const ptr = bump;
  bump += len;
  return ptr;
}

// Echo: the output IS the input, so return the input location packed into the
// low/high halves of the i64. `<u32>` casts keep the pointer/length unsigned so
// a high pointer bit never sign-extends (the host reinterprets the i64 as u64
// before shifting, mirroring this).
export function run(inPtr: i32, inLen: i32): i64 {
  return (<i64>(<u32>inPtr) << 32) | <i64>(<u32>inLen);
}
