//! Execution limits for guest components: a wall-clock deadline (wasmtime
//! epoch interruption) and a linear-memory / table / instance cap.
//!
//! * `GREENTIC_WASM_MAX_EXEC_SECS` — per-invocation deadline, default 300,
//!   `0` disables. Enforced to one-second granularity by a process-wide ticker
//!   thread that bumps every live engine's epoch once per second. The deadline
//!   is armed on each fresh `Store`, i.e. once per invocation. Time spent in
//!   host calls counts, but a host call itself is not interrupted: the trap
//!   fires when control returns to guest code.
//! * `GREENTIC_WASM_MAX_MEMORY_MB` — cap on any one linear memory, default
//!   1024, `0` disables. Table-element, instance, memory and table counts are
//!   always capped at generous fixed values.
//!
//! An unparseable value falls back to the default (a malformed knob must not
//! silently remove a safety limit, nor break every invocation).

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Result;
use parking_lot::Mutex;
use wasmtime::{Config, Engine, EngineWeak, Store, StoreLimits, StoreLimitsBuilder};

pub(crate) const EXEC_SECS_ENV: &str = "GREENTIC_WASM_MAX_EXEC_SECS";
pub(crate) const MEMORY_MB_ENV: &str = "GREENTIC_WASM_MAX_MEMORY_MB";
pub(crate) const DEFAULT_EXEC_SECS: u64 = 300;
pub(crate) const DEFAULT_MEMORY_MB: u64 = 1024;

const MAX_TABLE_ELEMENTS: usize = 10_000_000;
const MAX_INSTANCES: usize = 10_000;
const MAX_MEMORIES: usize = 10_000;
const MAX_TABLES: usize = 10_000;

/// Engine-config fingerprint: keeps compiled artifacts from engines without
/// epoch instrumentation out of the disk cache.
pub(crate) const ENGINE_FINGERPRINT: &str = "epoch-v1";

/// Resolved limits for one invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WasmLimits {
    /// Deadline in seconds; `0` = none.
    pub exec_secs: u64,
    /// Per-memory cap in MiB; `0` = none.
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

    pub(crate) fn store_limits(&self) -> StoreLimits {
        let mut builder = StoreLimitsBuilder::new()
            .table_elements(MAX_TABLE_ELEMENTS)
            .instances(MAX_INSTANCES)
            .memories(MAX_MEMORIES)
            .tables(MAX_TABLES);
        if self.memory_mb > 0 {
            let bytes =
                usize::try_from(self.memory_mb.saturating_mul(1024 * 1024)).unwrap_or(usize::MAX);
            builder = builder.memory_size(bytes);
        }
        builder.build()
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
