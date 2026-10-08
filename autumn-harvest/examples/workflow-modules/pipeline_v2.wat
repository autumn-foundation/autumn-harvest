;; pipeline_v2.wat — the v2 hot-swap target for issue #967.
;;
;; Same ABI and same shape as `pipeline_v1.wat`; the logic gains a step:
;;
;;   step 0 -> await activity "charge"
;;   step 1 -> await activity "notify"     <- new in v2
;;   step 2 -> complete with "v2-done"
;;
;; Publishing this under a NEW build id and ramping to it is the whole hot
;; swap: v1-assigned in-flight executions keep running `pipeline_v1.wat`,
;; because the trampoline routes on the execution's `assigned_build_id`.
(module
  (memory (export "memory") 1)

  (data (i32.const 1024) "{\"kind\":\"await\",\"activity\":\"charge\",\"input\":{\"amount\":100}}")
  (data (i32.const 1280) "{\"kind\":\"await\",\"activity\":\"notify\",\"input\":{\"channel\":\"email\"}}")
  (data (i32.const 1536) "{\"kind\":\"complete\",\"output\":\"v2-done\"}")

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
      (else
        (if (result i64) (i32.eq (local.get $step) (i32.const 1))
          (then (call $pack (i32.const 1280) (i32.const 64)))
          (else (call $pack (i32.const 1536) (i32.const 38))))))))
