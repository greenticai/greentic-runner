//! Execution limits for guest components: a wall-clock deadline (wasmtime
//! epoch interruption) and a linear-memory / table / instance cap.
//!
//! * `GREENTIC_WASM_MAX_EXEC_SECS` — per-invocation deadline, default 300,
//!   `0` disables. Enforced to one-second granularity by a process-wide ticker
//!   thread that bumps every live engine's epoch once per second. The deadline
//!   is armed on each fresh `Store`, i.e. once per invocation. Time spent in
//!   host calls counts, but a host call itself is not interrupted: the trap
//!   fires when control returns to guest code.
//! * `GREENTIC_WASM_MAX_MEMORY_MB` — cap on any one linear memory AND on the
//!   total across every memory in the store (multi-memory is enabled, so a
//!   per-memory cap alone would allow N memories to reach N times the cap),
//!   default 1024, `0` disables both. Table-element, instance, memory and table counts are
//!   always capped at generous fixed values.
//!
//! A grow past a cap is DENIED, not trapped: `memory.grow` / `table.grow`
//! return `-1` to the guest, which decides what to do (a Rust guest typically
//! aborts on the failed allocation, and THAT surfaces as an ordinary invocation
//! error). Wasmtime logs nothing for a denied grow, so the host warns once per
//! store ([`GuestLimiter`]).
//!
//! An unparseable value falls back to the default (a malformed knob must not
//! silently remove a safety limit, nor break every invocation).

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Result;
use parking_lot::Mutex;
use wasmtime::{
    Config, Engine, EngineWeak, ResourceLimiter, Store, StoreLimits, StoreLimitsBuilder,
};

pub(crate) const EXEC_SECS_ENV: &str = "GREENTIC_WASM_MAX_EXEC_SECS";
pub(crate) const MEMORY_MB_ENV: &str = "GREENTIC_WASM_MAX_MEMORY_MB";
pub(crate) const DEFAULT_EXEC_SECS: u64 = 300;
pub(crate) const DEFAULT_MEMORY_MB: u64 = 1024;

const MAX_TABLE_ELEMENTS: usize = 10_000_000;
const MAX_INSTANCES: usize = 10_000;
const MAX_MEMORIES: usize = 16;
const MAX_TABLES: usize = 10_000;

/// Engine-config fingerprint: keeps compiled artifacts from engines without
/// epoch instrumentation out of the disk cache.
pub(crate) const ENGINE_FINGERPRINT: &str = "epoch-v1";

/// Resolved limits for one invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WasmLimits {
    /// Deadline in seconds; `0` = none.
    pub exec_secs: u64,
    /// Per-memory and total-per-store cap in MiB; `0` = none.
    pub memory_mb: u64,
}

fn parse_or(raw: Option<&str>, default: u64) -> u64 {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

impl WasmLimits {
    pub(crate) fn from_raw(exec: Option<&str>, memory: Option<&str>) -> Self {
        Self {
            exec_secs: parse_or(exec, DEFAULT_EXEC_SECS),
            memory_mb: parse_or(memory, DEFAULT_MEMORY_MB),
        }
    }

    pub(crate) fn from_env() -> Self {
        Self::from_raw(
            std::env::var(EXEC_SECS_ENV).ok().as_deref(),
            std::env::var(MEMORY_MB_ENV).ok().as_deref(),
        )
    }

    /// Epoch ticks (one per second) before the guest traps.
    pub(crate) fn deadline_ticks(&self) -> u64 {
        if self.exec_secs == 0 {
            // Effectively never; halved so the add to the current epoch
            // cannot overflow.
            u64::MAX / 2
        } else {
            // wasmtime computes `current_epoch + delta` unsaturated, so an
            // enormous configured value must be clamped as well.
            self.exec_secs.min(u64::MAX / 2)
        }
    }

    pub(crate) fn store_limits(&self) -> GuestLimiter {
        let mut builder = StoreLimitsBuilder::new()
            .table_elements(MAX_TABLE_ELEMENTS)
            .instances(MAX_INSTANCES)
            .memories(MAX_MEMORIES)
            .tables(MAX_TABLES);
        let mut total_cap = None;
        if self.memory_mb > 0 {
            let bytes =
                usize::try_from(self.memory_mb.saturating_mul(1024 * 1024)).unwrap_or(usize::MAX);
            builder = builder.memory_size(bytes);
            total_cap = Some(bytes);
        }
        GuestLimiter {
            inner: builder.build(),
            total_cap,
            total: 0,
            warned: false,
        }
    }
}

/// [`StoreLimits`] plus a cap on the TOTAL linear memory across every memory
/// in the store. Per-memory, table and instance limits are delegated.
pub(crate) struct GuestLimiter {
    inner: StoreLimits,
    total_cap: Option<usize>,
    /// Bytes granted so far across all memories (initial allocation counts).
    total: usize,
    /// Whether a denied grow was already logged for this store.
    warned: bool,
}

impl GuestLimiter {
    /// Log the first denied grow of this store (once, so a guest retrying a
    /// failing allocation cannot flood the log).
    fn note_denied(&mut self, what: &str, current: usize, desired: usize) {
        if !self.warned {
            self.warned = true;
            tracing::warn!(
                resource = what,
                current,
                desired,
                memory_cap_bytes = ?self.total_cap,
                "guest {what} grow denied by the wasm resource limits; the guest sees a failed grow (-1). \
                 Raise GREENTIC_WASM_MAX_MEMORY_MB if this is a legitimate workload (logged once per store)"
            );
        }
    }
}

impl ResourceLimiter for GuestLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if !self.inner.memory_growing(current, desired, maximum)? {
            self.note_denied("memory", current, desired);
            return Ok(false);
        }
        let delta = desired.saturating_sub(current);
        let new_total = self.total.saturating_add(delta);
        if let Some(cap) = self.total_cap
            && new_total > cap
        {
            self.note_denied("memory", current, desired);
            return Ok(false);
        }
        self.total = new_total;
        Ok(true)
    }

    fn memory_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.inner.memory_grow_failed(error)
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let allowed = self.inner.table_growing(current, desired, maximum)?;
        if !allowed {
            self.note_denied("table", current, desired);
        }
        Ok(allowed)
    }

    fn table_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.inner.table_grow_failed(error)
    }

    fn instances(&self) -> usize {
        self.inner.instances()
    }

    fn tables(&self) -> usize {
        self.inner.tables()
    }

    fn memories(&self) -> usize {
        self.inner.memories()
    }
}

pub(crate) const STATE_MAX_VALUE_MB_ENV: &str = "GREENTIC_STATE_MAX_VALUE_MB";
pub(crate) const DEFAULT_STATE_MAX_VALUE_MB: u64 = 16;
pub(crate) const HTTP_MAX_REQUEST_MB_ENV: &str = "GREENTIC_HTTP_MAX_REQUEST_MB";
pub(crate) const DEFAULT_HTTP_MAX_REQUEST_MB: u64 = 64;

/// Resolve a size cap in bytes from a MiB env value. `None` = disabled (`0`);
/// absent or unparseable falls back to `default_mb`.
pub(crate) fn mb_cap_from_raw(raw: Option<&str>, default_mb: u64) -> Option<usize> {
    let mb = parse_or(raw, default_mb);
    if mb == 0 {
        return None;
    }
    Some(usize::try_from(mb.saturating_mul(1024 * 1024)).unwrap_or(usize::MAX))
}

/// Cap on one guest `state.write` value.
pub(crate) fn state_max_value_bytes() -> Option<usize> {
    mb_cap_from_raw(
        std::env::var(STATE_MAX_VALUE_MB_ENV).ok().as_deref(),
        DEFAULT_STATE_MAX_VALUE_MB,
    )
}

/// Cap on one guest outbound HTTP request body.
pub(crate) fn http_max_request_bytes() -> Option<usize> {
    mb_cap_from_raw(
        std::env::var(HTTP_MAX_REQUEST_MB_ENV).ok().as_deref(),
        DEFAULT_HTTP_MAX_REQUEST_MB,
    )
}

/// `Err(message)` when `len` exceeds `cap` (`None` = unlimited).
pub(crate) fn check_size(what: &str, len: usize, cap: Option<usize>) -> Result<(), String> {
    match cap {
        Some(cap) if len > cap => Err(format!(
            "{what} of {len} bytes exceeds the {cap} byte limit"
        )),
        _ => Ok(()),
    }
}

/// Arm the epoch deadline on a fresh store. Must be called once per
/// invocation on any store whose engine came from [`build_engine`]: with epoch
/// interruption on and no deadline set, wasmtime traps immediately.
pub(crate) fn arm_deadline<T>(store: &mut Store<T>, limits: &WasmLimits) {
    store.set_epoch_deadline(limits.deadline_ticks());
}

static TICKED_ENGINES: Mutex<Vec<EngineWeak>> = Mutex::new(Vec::new());
static TICKER: OnceLock<bool> = OnceLock::new();

fn start_ticker() -> bool {
    *TICKER.get_or_init(|| {
        let spawned = std::thread::Builder::new()
            .name("greentic-wasm-epoch-ticker".into())
            .spawn(|| {
                loop {
                    std::thread::sleep(Duration::from_secs(1));
                    TICKED_ENGINES.lock().retain(|weak| match weak.upgrade() {
                        Some(engine) => {
                            engine.increment_epoch();
                            true
                        }
                        None => false,
                    });
                }
            });
        match spawned {
            Ok(_) => true,
            Err(error) => {
                tracing::warn!(%error, "failed to start wasm epoch ticker; execution deadline disabled");
                false
            }
        }
    })
}

/// Build an engine with epoch interruption on and register it with the
/// process-wide ticker (held weakly: a dropped engine is pruned).
pub(crate) fn build_engine() -> Result<Engine> {
    let mut config = Config::new();
    config.epoch_interruption(true);
    let engine = Engine::new(&config)
        .map_err(|err| anyhow::anyhow!("failed to build wasmtime engine: {err}"))?;
    if start_ticker() {
        TICKED_ENGINES.lock().push(engine.weak());
    }
    Ok(engine)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_overrides_parse() {
        let d = WasmLimits::from_raw(None, None);
        assert_eq!(d.exec_secs, 300);
        assert_eq!(d.memory_mb, 1024);
        let o = WasmLimits::from_raw(Some(" 7 "), Some("64"));
        assert_eq!((o.exec_secs, o.memory_mb), (7, 64));
        let off = WasmLimits::from_raw(Some("0"), Some("0"));
        assert_eq!((off.exec_secs, off.memory_mb), (0, 0));
        let bad = WasmLimits::from_raw(Some("abc"), Some("-1"));
        assert_eq!((bad.exec_secs, bad.memory_mb), (300, 1024));
    }

    #[test]
    fn deadline_ticks_zero_means_effectively_unbounded() {
        assert_eq!(WasmLimits::from_raw(Some("5"), None).deadline_ticks(), 5);
        assert!(WasmLimits::from_raw(Some("0"), None).deadline_ticks() > 1 << 40);
        assert_eq!(
            WasmLimits::from_raw(Some("18446744073709551615"), None).deadline_ticks(),
            u64::MAX / 2
        );
    }

    #[test]
    fn memory_cap_rejects_oversized_memory_and_zero_disables_it() {
        let engine = Engine::default();
        let module =
            wasmtime::Module::new(&engine, "(module (memory (export \"m\") 32))").expect("module");
        // 32 pages = 2 MiB.
        let capped = WasmLimits::from_raw(None, Some("1"));
        let mut store = Store::new(&engine, capped.store_limits());
        store.limiter(|l| l);
        assert!(wasmtime::Instance::new(&mut store, &module, &[]).is_err());

        let uncapped = WasmLimits::from_raw(None, Some("0"));
        let mut store = Store::new(&engine, uncapped.store_limits());
        store.limiter(|l| l);
        assert!(wasmtime::Instance::new(&mut store, &module, &[]).is_ok());
    }

    #[test]
    fn size_caps_resolve_and_enforce() {
        assert_eq!(mb_cap_from_raw(None, 16), Some(16 * 1024 * 1024));
        assert_eq!(mb_cap_from_raw(Some("0"), 16), None);
        assert_eq!(mb_cap_from_raw(Some("junk"), 16), Some(16 * 1024 * 1024));
        assert_eq!(mb_cap_from_raw(Some("1"), 16), Some(1024 * 1024));
        assert!(check_size("v", 10, Some(10)).is_ok());
        assert!(check_size("v", 11, Some(10)).is_err());
        assert!(check_size("v", usize::MAX, None).is_ok());
    }

    #[test]
    fn total_memory_across_memories_is_capped() {
        let engine = Engine::default();
        // Two 1 MiB memories: each fits the 1 MiB per-memory cap, together
        // they are 2 MiB.
        let module =
            wasmtime::Module::new(&engine, "(module (memory 16) (memory 16))").expect("module");
        let one_mib = WasmLimits::from_raw(None, Some("1"));
        let mut store = Store::new(&engine, one_mib.store_limits());
        store.limiter(|l| l);
        assert!(
            wasmtime::Instance::new(&mut store, &module, &[]).is_err(),
            "second memory must push the total over the cap"
        );

        let two_mib = WasmLimits::from_raw(None, Some("2"));
        let mut store = Store::new(&engine, two_mib.store_limits());
        store.limiter(|l| l);
        assert!(wasmtime::Instance::new(&mut store, &module, &[]).is_ok());

        let off = WasmLimits::from_raw(None, Some("0"));
        let mut store = Store::new(&engine, off.store_limits());
        store.limiter(|l| l);
        assert!(wasmtime::Instance::new(&mut store, &module, &[]).is_ok());
    }

    #[test]
    fn a_denied_grow_returns_minus_one_to_the_guest_and_is_flagged_once() {
        let engine = Engine::default();
        // 1 page now; the guest asks to grow by 32 pages (2 MiB) past a 1 MiB cap.
        let module = wasmtime::Module::new(
            &engine,
            "(module (memory 1) (func (export \"grow\") (result i32) (memory.grow (i32.const 32))))",
        )
        .expect("module");
        let limits = WasmLimits::from_raw(None, Some("1"));
        let mut store = Store::new(&engine, limits.store_limits());
        store.limiter(|l| l);
        let instance = wasmtime::Instance::new(&mut store, &module, &[]).expect("instance");
        let grow = instance
            .get_typed_func::<(), i32>(&mut store, "grow")
            .expect("grow");
        assert!(!store.data().warned);
        // Denied, not trapped: the guest sees -1.
        assert_eq!(grow.call(&mut store, ()).expect("no trap"), -1);
        assert!(store.data().warned, "the first denial is recorded");
        assert_eq!(grow.call(&mut store, ()).expect("no trap"), -1);
    }

    #[test]
    fn memory_count_is_capped() {
        let engine = Engine::default();
        let memories = "(memory 0)".repeat(MAX_MEMORIES + 1);
        let module =
            wasmtime::Module::new(&engine, format!("(module {memories})")).expect("module");
        let limits = WasmLimits::from_raw(None, Some("0"));
        let mut store = Store::new(&engine, limits.store_limits());
        store.limiter(|l| l);
        assert!(wasmtime::Instance::new(&mut store, &module, &[]).is_err());
    }

    #[test]
    fn looping_guest_traps_at_the_deadline_and_deadline_resets_per_store() {
        let engine = build_engine().expect("engine");
        let module = wasmtime::Module::new(
            &engine,
            "(module (func (export \"spin\") (loop (br 0))) (func (export \"ok\")))",
        )
        .expect("module");
        let limits = WasmLimits::from_raw(Some("1"), None);

        let mut store = Store::new(&engine, ());
        arm_deadline(&mut store, &limits);
        let instance = wasmtime::Instance::new(&mut store, &module, &[]).expect("instance");
        let spin = instance
            .get_typed_func::<(), ()>(&mut store, "spin")
            .expect("spin");
        let err = spin.call(&mut store, ()).expect_err("must trap");
        assert_eq!(
            err.downcast_ref::<wasmtime::Trap>(),
            Some(&wasmtime::Trap::Interrupt)
        );

        // A fresh store (a new invocation) gets a fresh deadline.
        let mut store = Store::new(&engine, ());
        arm_deadline(&mut store, &limits);
        let instance = wasmtime::Instance::new(&mut store, &module, &[]).expect("instance");
        let ok = instance
            .get_typed_func::<(), ()>(&mut store, "ok")
            .expect("ok");
        ok.call(&mut store, ()).expect("short call succeeds");
    }
}
