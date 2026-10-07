;; Core module of the artifact probe component (`probe.wasm`), used by
;; runner-host's `ext_artifact_port` dispatch tests. Rebuild, from this dir,
;; with wasm-tools 1.255.0:
;;   wasm-tools component embed wit --world probe probe.core.wat -o /tmp/probe.embed.wasm
;;   wasm-tools component new /tmp/probe.embed.wasm -o probe.wasm
;; invoke-tool(name, args) calls artifact.put(bytes=[1,2,3], "image/png",
;; "a.png") and returns Ok(<id>) on success or Ok("err:<case>") on failure.
(module
  (import "greentic:extension-host/artifact@0.1.0" "put"
    (func $put (param i32 i32 i32 i32 i32 i32 i32)))
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 1024))
  (data (i32.const 100) "image/png")
  (data (i32.const 120) "a.png")
  (data (i32.const 200) "\01\02\03")
  (data (i32.const 300) "err:?")
  (func (export "cabi_realloc") (param i32 i32 i32 i32) (result i32)
    (local $p i32)
    ;; align the bump pointer to 8, hand out `new_size` bytes
    (local.set $p (i32.and (i32.add (global.get $heap) (i32.const 7)) (i32.const -8)))
    (global.set $heap (i32.add (local.get $p) (local.get 3)))
    (local.get $p))
  (func (export "greentic:extension-design/tools@0.2.0#invoke-tool")
    (param i32 i32 i32 i32) (result i32)
    (call $put (i32.const 200) (i32.const 3) (i32.const 100) (i32.const 9)
               (i32.const 120) (i32.const 5) (i32.const 64))
    (i32.store8 (i32.const 512) (i32.const 0))
    (if (i32.eqz (i32.load8_u (i32.const 64)))
      (then
        (i32.store (i32.const 516) (i32.load (i32.const 68)))
        (i32.store (i32.const 520) (i32.load (i32.const 72))))
      (else
        (i32.store8 (i32.const 304)
          (i32.add (i32.const 48) (i32.load8_u (i32.const 68))))
        (i32.store (i32.const 516) (i32.const 300))
        (i32.store (i32.const 520) (i32.const 5))))
    (i32.const 512))
)
