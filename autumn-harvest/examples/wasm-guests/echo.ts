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

// Bytes per wasm page (64 KiB).
const PAGE_BYTES: i32 = 65536;

// `alloc` MUST guarantee the returned range is actually backed by linear memory.
//
// This is not boilerplate: `asc --runtime stub` emits a module whose memory has
// **zero initial pages** when nothing statically requires more, so a naive bump
// pointer hands back an address that is out of bounds. The host bounds-checks
// every write, so that surfaces as a `WasmTrap` ("alloc returned an
// out-of-bounds pointer for the input") on the very first attempt rather than as
// memory corruption — but the guest is still the thing that has to grow.
//
// A hand-written `.wat` guest that declares `(memory 1)` never hits this, which
// is exactly why a guest built by a real toolchain is worth testing.
export function alloc(len: i32): i32 {
  const ptr = bump;
  const need = ptr + len;
  const have = memory.size() * PAGE_BYTES;
  if (need > have) {
    // Round the shortfall up to whole pages.
    const pages = (need - have + PAGE_BYTES - 1) / PAGE_BYTES;
    if (memory.grow(pages) < 0) {
      // Out of memory: return a null pointer. The host's bounds check rejects
      // the subsequent write as a trap rather than writing out of bounds.
      return 0;
    }
  }
  bump = need;
  return ptr;
}

// Echo: the output IS the input, so return the input location packed into the
// low/high halves of the i64. `<u32>` casts keep the pointer/length unsigned so
// a high pointer bit never sign-extends (the host reinterprets the i64 as u64
// before shifting, mirroring this).
export function run(inPtr: i32, inLen: i32): i64 {
  return (<i64>(<u32>inPtr) << 32) | <i64>(<u32>inLen);
}
