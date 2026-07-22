;; echo.wat — a Harvest WASM activity guest (issue #965).
;;
;; This is the guest CI actually executes: the `wasm-activities` integration
;; tests and the `wasm_activity` example assemble this exact text with
;; `wat::parse_str(...)` and run it through the worker's WASM dispatch seam.
;;
;; It implements the JSON-over-linear-memory ABI (see README.md): it exports
;; `memory`, a bump-allocator `alloc`, and `run`. `run` echoes its input by
;; returning the input location packed as `(in_ptr << 32) | in_len`, so the host
;; reads back the exact JSON bytes it wrote.
(module
  (memory (export "memory") 1)
  (global $bump (mut i32) (i32.const 1024))

  ;; alloc(len) -> ptr : hand back `len` writable bytes from a bump pointer.
  (func (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $bump))
    (global.set $bump (i32.add (global.get $bump) (local.get $len)))
    (local.get $ptr))

  ;; run(in_ptr, in_len) -> i64 : echo — return packed (in_ptr << 32) | in_len.
  (func (export "run") (param $in_ptr i32) (param $in_len i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $in_ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $in_len)))))
