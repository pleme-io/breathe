//! breathe-store — the durable-store seam (M0 of the Urdume-microservice
//! refactor; destination doc: `docs/BREATHE-MICROSERVICE.md`).
//!
//! Two `&dyn`-object-safe async traits sit between the reconcile loop and where
//! decision/sample state durably lives:
//!
//! - [`DecisionLog`] — the **single counter-accumulation point** (the
//!   `+1`-on-match fold that used to live inline in `breathe_runtime::status_for`,
//!   removing the dual-source-of-truth) **plus** the append-only decision feed
//!   (the seed of the M2 Postgres `decision_log` attestation chain).
//! - [`SampleCache`] — the predictive prior-sample store (write-through today;
//!   the authoritative predictive read flips to cache-first at M2 when Postgres,
//!   not the CRD status, is the durable home).
//!
//! Each trait has an in-memory tier ([`InMemDecisionLog`] / [`InMemSampleCache`])
//! that is **byte-identical** to today's CRD-status-backed behavior — the counter
//! is folded from the status seed every tick, so M0 changes no observable
//! behavior — and (M2/M3) a Postgres/Redis tier behind the SAME seam. The
//! controller holds `Arc<dyn DecisionLog>` / `Arc<dyn SampleCache>` and swaps
//! `InMem` ↔ `PgRedis` by shikumi config with zero change to the band law or loop.
//!
//! The traits use `#[async_trait]` for `&dyn` object-safety — the exact mechanism
//! `breathe_provider::Cluster` / `ResourceProvider` use. breathe-store depends on
//! NO other breathe crate: the classification of a `TickOutcome` into a
//! [`DecisionEntry`] is done by `breathe_runtime::entry_for` (the 4th consumer of
//! the `TickOutcome` keystone), keeping this crate a low-level leaf.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A stable per-band identity — `kind/namespace/name`
/// (e.g. `MemoryBand/pangea-system/pangea-database`). The key under which a
/// band's decisions + samples are stored, globally unique across band kinds.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BandRef(String);

impl BandRef {
    /// Build a key from the CRD kind, namespace, and name.
    #[must_use]
    pub fn new(kind: &str, namespace: &str, name: &str) -> Self {
        Self(format!("{kind}/{namespace}/{name}"))
    }

    /// The underlying `kind/namespace/name` string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BandRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The cumulative band counters projected onto `BandStatus`
/// (`carves_total` / `deferrals_total` / `conflicts_total`).
///
/// [`CumulativeCounters::fold`] is the ONE place the per-tick `+1` math lives —
/// the dual-source-of-truth the refactor removes. Every counter path folds here:
/// the in-memory tier, the caller's status-backed fallback, the golden
/// equivalence test, and (M2) the Postgres `band_registry` writer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CumulativeCounters {
    /// Cumulative `TickReceipt::Applied` carves.
    pub carves: i64,
    /// Cumulative deferred ceiling crossings (`TickReceipt::DeferredWouldRestart`).
    pub deferrals: i64,
    /// Cumulative single-writer yields (`TickReceipt::Conflict`).
    pub conflicts: i64,
}

impl CumulativeCounters {
    /// The empty count — the seed for a never-before-seen band.
    pub const ZERO: Self = Self {
        carves: 0,
        deferrals: 0,
        conflicts: 0,
    };

    /// Fold one decision's classification into the running cumulative — the
    /// `prior + 1-on-match` math that used to live inline in
    /// `breathe_runtime::status_for`. This is the single accumulation point;
    /// nothing else increments a band counter.
    #[must_use]
    pub fn fold(self, entry: &DecisionEntry) -> Self {
        let mut next = self;
        match entry.class {
            CounterClass::Carve => next.carves += 1,
            CounterClass::Deferral => next.deferrals += 1,
            CounterClass::Conflict => next.conflicts += 1,
            CounterClass::NoCount => {}
        }
        next
    }
}

/// Which cumulative counter a decision advances — **exactly one, or none.** A
/// `TickReceipt` is a sum type (one variant per tick), so a tick is never both a
/// carve and a conflict; making that an enum rather than three booleans means the
/// illegal "carve AND conflict" state has no representation
/// (★★ UNREPRESENTABILITY).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CounterClass {
    /// Advances `carves_total` — a real mutation was applied (`TickReceipt::Applied`).
    Carve,
    /// Advances `deferrals_total` — a refused ceiling crossing (`DeferredWouldRestart`).
    Deferral,
    /// Advances `conflicts_total` — a single-writer yield (`Conflict`).
    Conflict,
    /// Advances no counter (observe / stale / cooldown / dormant / error / shadow).
    NoCount,
}

/// One reconcile tick's classified decision — built from a `TickOutcome` by
/// `breathe_runtime::entry_for` (so breathe-store needs no breathe-core
/// dependency). [`CounterClass`] drives [`CumulativeCounters::fold`]; the typed
/// fields are the seed of the M2 append-only `decision_log` row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionEntry {
    /// A short, stable receipt-kind tag (`"Applied"`, `"Conflict"`, `"DryRunWouldApply"`, …).
    pub receipt_kind: String,
    /// Which cumulative counter (if any) this decision advances.
    pub class: CounterClass,
    /// The limit transition, when the receipt carried one (`Applied` / `DryRun` / `Deferred`).
    pub from_limit: Option<u64>,
    /// The carved-to limit, when the receipt carried one.
    pub to_limit: Option<u64>,
    /// Whether this tick was shadow-only (observe + attest, never mutate).
    pub dry_run: bool,
}

/// One observed working-set sample — the predictive prior + its wall-clock epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sample {
    /// The observed `used` scalar (bytes for memory/arc/cgroup; millicores for cpu).
    pub used: u64,
    /// Unix-epoch seconds at which the sample was observed.
    pub at_epoch: i64,
}

/// A store backend error. The in-memory tier never errors; the Postgres/Redis
/// tier (M2/M3) surfaces backend failures here so callers degrade to the
/// status-backed fallback rather than mutate on stale state.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The durable backend (Postgres/Redis) failed.
    #[error("store backend error: {0}")]
    Backend(String),
}

/// The append-only decision feed + the single counter-accumulation seam.
#[async_trait]
pub trait DecisionLog: Send + Sync {
    /// Append `entry` to `band`'s decision feed and return the new cumulative
    /// counters.
    ///
    /// `seed` is the band's prior cumulative — the in-memory tier folds the new
    /// decision onto it every tick (so the count stays durable via the CRD
    /// status, byte-identical to today). The M2 Postgres tier reads its
    /// authoritative `band_registry` row under lock and treats `seed` as
    /// advisory. Either way the `+1` math is [`CumulativeCounters::fold`], in one
    /// place.
    async fn append(
        &self,
        band: &BandRef,
        seed: CumulativeCounters,
        entry: DecisionEntry,
    ) -> Result<CumulativeCounters, StoreError>;
}

/// The predictive prior-sample cache.
#[async_trait]
pub trait SampleCache: Send + Sync {
    /// Record the latest observed sample for `band` (write-through; the next
    /// tick's predictive prior).
    async fn record(&self, band: &BandRef, sample: Sample) -> Result<(), StoreError>;

    /// The last recorded sample for `band`, if any. Cold this process ⇒ `None`
    /// (the caller falls back to the CRD status at M0).
    async fn prior(&self, band: &BandRef) -> Result<Option<Sample>, StoreError>;
}

/// Bounded recent-decision ring depth per band, in the in-memory tier.
const RECENT_CAP: usize = 64;

struct BandLog {
    counters: CumulativeCounters,
    recent: Vec<DecisionEntry>,
}

/// In-memory [`DecisionLog`] — the VERY-SMALL tier. Folds the new decision onto
/// the status-backed `seed` every tick (byte-identical to today's
/// `prior_n + matches!` accumulation, proven by the golden equivalence test) and
/// keeps a bounded recent feed per band (the M2 `decision_log` seed). Zero
/// external infra.
#[derive(Default)]
pub struct InMemDecisionLog {
    state: Mutex<HashMap<BandRef, BandLog>>,
}

impl InMemDecisionLog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inspection/test: the bounded recent decision feed for a band, oldest-first.
    #[must_use]
    pub fn recent(&self, band: &BandRef) -> Vec<DecisionEntry> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(band)
            .map(|b| b.recent.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl DecisionLog for InMemDecisionLog {
    async fn append(
        &self,
        band: &BandRef,
        seed: CumulativeCounters,
        entry: DecisionEntry,
    ) -> Result<CumulativeCounters, StoreError> {
        // Stateless fold from the status-backed seed — byte-identical to the old
        // `status_for` inline logic. The append-only feed is the value M0 adds.
        let counters = seed.fold(&entry);
        let mut map = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let log = map.entry(band.clone()).or_insert_with(|| BandLog {
            counters,
            recent: Vec::new(),
        });
        log.counters = counters;
        log.recent.push(entry);
        if log.recent.len() > RECENT_CAP {
            let excess = log.recent.len() - RECENT_CAP;
            log.recent.drain(0..excess);
        }
        Ok(counters)
    }
}

/// In-memory [`SampleCache`] — the VERY-SMALL tier. Holds the last sample per
/// band; cold after a restart (the caller falls back to the CRD status), warm
/// thereafter. Zero external infra.
#[derive(Default)]
pub struct InMemSampleCache {
    state: Mutex<HashMap<BandRef, Sample>>,
}

impl InMemSampleCache {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SampleCache for InMemSampleCache {
    async fn record(&self, band: &BandRef, sample: Sample) -> Result<(), StoreError> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(band.clone(), sample);
        Ok(())
    }

    async fn prior(&self, band: &BandRef) -> Result<Option<Sample>, StoreError> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(band)
            .copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The OLD inline logic from `status_for` (the deleted lines 311–316),
    /// re-expressed against the classified entry as an INDEPENDENT oracle (`==`
    /// comparisons, not [`CumulativeCounters::fold`]'s `match`) — the golden test
    /// proves `fold` reproduces it exactly.
    fn old_status_for_counters(prior: CumulativeCounters, e: &DecisionEntry) -> CumulativeCounters {
        CumulativeCounters {
            carves: prior.carves + i64::from(e.class == CounterClass::Carve),
            deferrals: prior.deferrals + i64::from(e.class == CounterClass::Deferral),
            conflicts: prior.conflicts + i64::from(e.class == CounterClass::Conflict),
        }
    }

    fn entry(kind: &str, class: CounterClass) -> DecisionEntry {
        let is_carve = matches!(class, CounterClass::Carve);
        DecisionEntry {
            receipt_kind: kind.to_string(),
            class,
            from_limit: is_carve.then_some(100),
            to_limit: is_carve.then_some(200),
            dry_run: false,
        }
    }

    fn mixed_sequence() -> Vec<DecisionEntry> {
        vec![
            entry("Applied", CounterClass::Carve),
            entry("Conflict", CounterClass::Conflict),
            entry("DeferredWouldRestart", CounterClass::Deferral),
            entry("Applied", CounterClass::Carve),
            entry("Observed", CounterClass::NoCount),
            entry("Stale", CounterClass::NoCount),
            entry("Applied", CounterClass::Carve),
            entry("Conflict", CounterClass::Conflict),
        ]
    }

    #[test]
    fn fold_matches_old_inline_logic_over_a_sequence() {
        // Seed from a non-zero prior (the post-restart re-hydration from status).
        let seed = CumulativeCounters {
            carves: 7,
            deferrals: 2,
            conflicts: 3,
        };
        let mut oracle = seed;
        let mut folded = seed;
        for e in &mixed_sequence() {
            oracle = old_status_for_counters(oracle, e);
            folded = folded.fold(e);
            assert_eq!(folded, oracle, "fold diverged from the old status_for logic");
        }
        // 3 Applied, 1 Deferred, 2 Conflict on top of the seed.
        assert_eq!(
            folded,
            CumulativeCounters {
                carves: 10,
                deferrals: 3,
                conflicts: 5
            }
        );
    }

    #[tokio::test]
    async fn inmem_decision_log_reproduces_the_status_backed_sequence() {
        let log = InMemDecisionLog::new();
        let band = BandRef::new("MemoryBand", "pangea-system", "pangea-database");
        // The controller passes prior_counters read from the CRD status each tick;
        // status mirrors the returned value, so the seed advances in lockstep.
        let mut status = CumulativeCounters {
            carves: 5,
            deferrals: 1,
            conflicts: 0,
        };
        let mut oracle = status;
        for e in &mixed_sequence() {
            let got = log.append(&band, status, e.clone()).await.unwrap();
            oracle = old_status_for_counters(oracle, e);
            assert_eq!(got, oracle, "InMem append diverged from status-backed fold");
            status = got; // status is patched with the returned value → next seed
        }
        // The append-only feed recorded every decision, bounded + ordered.
        assert_eq!(log.recent(&band).len(), mixed_sequence().len());
        assert_eq!(log.recent(&band)[0].receipt_kind, "Applied");
    }

    #[tokio::test]
    async fn recent_feed_is_bounded() {
        let log = InMemDecisionLog::new();
        let band = BandRef::new("CpuBand", "ns", "x");
        for _ in 0..(RECENT_CAP + 20) {
            log.append(&band, CumulativeCounters::ZERO, entry("Observed", CounterClass::NoCount))
                .await
                .unwrap();
        }
        assert_eq!(log.recent(&band).len(), RECENT_CAP);
    }

    #[tokio::test]
    async fn separate_bands_accumulate_independently() {
        let log = InMemDecisionLog::new();
        let a = BandRef::new("MemoryBand", "ns", "a");
        let b = BandRef::new("MemoryBand", "ns", "b");
        let ca = log
            .append(&a, CumulativeCounters::ZERO, entry("Applied", CounterClass::Carve))
            .await
            .unwrap();
        let cb = log
            .append(&b, CumulativeCounters::ZERO, entry("Conflict", CounterClass::Conflict))
            .await
            .unwrap();
        assert_eq!(ca.carves, 1);
        assert_eq!(ca.conflicts, 0);
        assert_eq!(cb.carves, 0);
        assert_eq!(cb.conflicts, 1);
    }

    #[tokio::test]
    async fn inmem_sample_cache_records_and_reads_prior() {
        let c = InMemSampleCache::new();
        let band = BandRef::new("MemoryBand", "ns", "db");
        // Cold ⇒ None (the caller falls back to the CRD status at M0).
        assert!(c.prior(&band).await.unwrap().is_none());
        c.record(
            &band,
            Sample {
                used: 2_000_000_000,
                at_epoch: 100,
            },
        )
        .await
        .unwrap();
        let got = c.prior(&band).await.unwrap().unwrap();
        assert_eq!(got.used, 2_000_000_000);
        assert_eq!(got.at_epoch, 100);
    }

    #[test]
    fn band_ref_is_stable_kind_ns_name() {
        assert_eq!(
            BandRef::new("MemoryBand", "pangea-system", "db").as_str(),
            "MemoryBand/pangea-system/db"
        );
    }
}
