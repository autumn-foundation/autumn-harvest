;; pipeline_v1.wat — a Harvest hot-swappable WORKFLOW module (issue #967).
;;
;; This is the guest CI actually executes: the `hot-code-swap` integration
;; tests assemble this exact text with `wat::parse_str(...)` and run it through
;; the module trampoline.
;;
;; Logic (v1):  step 0 -> await activity "charge"
;;              step 1 -> complete with "v1-done"
;;
;; It reads its step index from byte 8 of the request (see README.md) and
;; returns one of two pre-baked JSON responses from its data segments.
(module
  (memory (export "memory") 1)

  ;; Response for step 0: ask the host to run the `charge` activity.
  (data (i32.const 1024) "{\"kind\":\"await\",\"activity\":\"charge\",\"input\":{\"amount\":100}}")
  ;; Response for step 1 (and any later step): terminal success.
  (data (i32.const 1280) "{\"kind\":\"complete\",\"output\":\"v1-done\"}")

  ;; Bump allocator starts past both data segments so host-written input can
  ;; never overwrite a response.
  (global $bump (mut i32) (i32.const 4096))

  ;; alloc(len) -> ptr : hand back `len` writable bytes from a bump pointer.
  (func (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $bump))
    (global.set $bump (i32.add (global.get $bump) (local.get $len)))
    (local.get $ptr))

  ;; pack(ptr, len) -> i64 : the ABI's return encoding.
  (func $pack (param $ptr i32) (param $len i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $len))))

  ;; run(in_ptr, in_len) -> i64
  (func (export "run") (param $in_ptr i32) (param $in_len i32) (result i64)
    (local $step i32)
    ;; step digit lives at a fixed offset 8 of `{"step":N,...`.
    (local.set $step
      (i32.sub
        (i32.load8_u (i32.add (local.get $in_ptr) (i32.const 8)))
        (i32.const 48)))
    (if (result i64) (i32.eqz (local.get $step))
      (then (call $pack (i32.const 1024) (i32.const 59)))
      (else (call $pack (i32.const 1280) (i32.const 38))))))
