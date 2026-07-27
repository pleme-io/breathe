//! The decision-log **state-change gate** — the write-volume half of the
//! architecture-of-record's attestation rule:
//!
//! > "Attest state-changes + a periodic in-band heartbeat, not every Hold —
//! > keeps signing load bounded (a flagged tradeoff, resolved toward bounded)."
//! > — `docs/BREATHE.md` § 8, row 8 (OutcomeChain attestation)
//!
//! [`DecisionLog::append`] was called UNCONDITIONALLY on every reconcile, so a
//! band that simply HOLDS wrote one row per tick forever. Its sibling
//! `breathe_runtime::patch_status_if_changed` has been diff-gated since task
//! #220; the decision log never was. At the live camelot-eks shape (~100 bands on
//! the 15s restart-free cooldown path, ~95 of them in an `Observed`-family phase)
//! that is ~576k rows/day; gated it is ~31k/day — the same ~18× the status
//! diff-gate already buys on the etcd side.
//!
//! [`GatedDecisionLog`] is a **decorator** over any [`DecisionLog`], not a patch
//! to one call site: the controller has two independent append paths
//! (`fold_counters` for the vertical kinds, `reconcile_replica_band` for the
//! horizontal one), and wrapping the `Arc<dyn DecisionLog>` gates both with one
//! definition of "changed".
//!
//! # What "materially changed" means here — and why it is NOT `BandStatus`
//!
//! The gate compares the [`DecisionEntry`] being appended against the last one
//! appended for that band. It deliberately does **not** reuse
//! `patch_status_if_changed`'s notion (full `BandStatus` equality): a
//! `BandStatus` carries live observations (`observed_used`, staleness, cooldown
//! remaining) that jitter on essentially every tick, so a status-equality gate
//! would still write ~every tick and buy nothing. `DecisionEntry` IS the row —
//! the exact tuple the chain hashes — so entry-equality is not a *second*
//! definition of equality, it is the log's own.
//!
//! A corollary worth stating plainly: the gate's resolution is exactly the log's
//! resolution. `entry_for` collapses `Observed { Hold }`, `Observed { AtCeiling }`
//! and `Observed { NoLimit }` into the same `{"Observed", NoCount, None, None}`
//! row, so a Holding→AtCeiling transition was never distinguishable in the log
//! and is not distinguishable now. The gate withholds nothing the log ever
//! recorded.
//!
//! # ★ What the gate does to the attestation chain
//!
//! Chain **integrity is untouched**: `seq` is still assigned by the inner tier
//! (`cur_seq + 1`), every appended row still links `prev_hash → content_hash`
//! from genesis, and `PgDecisionLog::verify_chain` still walks from genesis and
//! cross-checks the registry head. Gating removes candidate rows *before* they
//! enter the chain; it never skips a `seq`, never reorders, never rewrites.
//!
//! What DOES change is what a verifier may **conclude from a gap**. Before:
//! "there is no row for 14:03" implied *nothing happened at 14:03* only in the
//! sense that the controller did not tick. After: a gap means **nothing
//! CHANGED** — the band re-decided the same thing, and the heartbeat bounds how
//! long that may go unrecorded. A verifier reading this chain must interpret a
//! run of missing ticks as "the last recorded decision still stood", not as
//! "breathe was not running". Liveness is carried by the heartbeat: a band that
//! is reconciling at all writes at least one row per `heartbeat_secs`, so an
//! absence LONGER than the heartbeat is real evidence the controller stopped.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;

use crate::{BandRef, CounterClass, CumulativeCounters, DecisionEntry, DecisionLog, StoreError};

/// The wall-clock seam the heartbeat is measured against — injectable so the
/// heartbeat is provable without sleeping (the fleet's mockable-`Environment`
/// discipline; `Sample::at_epoch` already speaks the same unix-epoch-seconds
/// vocabulary).
pub trait Clock: Send + Sync {
    /// Unix-epoch seconds, now.
    fn now_epoch_secs(&self) -> i64;
}

/// The real clock — `SystemTime::now()` since the unix epoch.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_epoch_secs(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
    }
}

/// What was last appended for a band — the gate's whole state.
struct LastAppend {
    entry: DecisionEntry,
    at_epoch: i64,
    counters: CumulativeCounters,
}

/// A [`DecisionLog`] decorator that appends on a **material change** plus a
/// **periodic in-band heartbeat**, instead of on every tick.
///
/// A tick is appended when ANY of:
///
/// 1. it advances a counter (`class != NoCount` — a carve/deferral/conflict is
///    material by definition, and the counters are derived from the log, so one
///    is never withheld);
/// 2. this band has not been appended by this process yet (anchors the chain,
///    and re-anchors after a restart);
/// 3. the [`DecisionEntry`] differs from the last one appended;
/// 4. `heartbeat_secs` have elapsed since the last append (the in-band
///    heartbeat — `0` disables the gate entirely: every tick writes, the
///    pre-gate behavior, available as an explicit escape hatch).
///
/// Otherwise the append is skipped and the counters from the last append are
/// returned — correct by construction, because a skip is only ever reachable for
/// a `NoCount` entry, whose fold is the identity.
pub struct GatedDecisionLog {
    inner: Arc<dyn DecisionLog>,
    heartbeat_secs: i64,
    clock: Arc<dyn Clock>,
    state: Mutex<HashMap<BandRef, LastAppend>>,
}

impl GatedDecisionLog {
    /// Wrap `inner`, appending on change + every `heartbeat_secs` (`0` ⇒ every
    /// tick, i.e. gate off).
    #[must_use]
    pub fn new(inner: Arc<dyn DecisionLog>, heartbeat_secs: u64) -> Self {
        Self::with_clock(inner, heartbeat_secs, Arc::new(SystemClock))
    }

    /// [`GatedDecisionLog::new`] with an injected [`Clock`] — the test seam.
    #[must_use]
    pub fn with_clock(inner: Arc<dyn DecisionLog>, heartbeat_secs: u64, clock: Arc<dyn Clock>) -> Self {
        Self {
            inner,
            // A heartbeat past i64::MAX seconds is "never" either way.
            heartbeat_secs: i64::try_from(heartbeat_secs).unwrap_or(i64::MAX),
            clock,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// `Some(counters)` ⇒ withhold this append and report `counters`;
    /// `None` ⇒ append. Pure w.r.t. the gate's state (read-only).
    fn skip_verdict(&self, band: &BandRef, entry: &DecisionEntry, now: i64) -> Option<CumulativeCounters> {
        // (1) a counted decision is ALWAYS material — never withheld.
        if entry.class != CounterClass::NoCount {
            return None;
        }
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        // (2) first sight this process ⇒ append (anchors/re-anchors the chain).
        let last = state.get(band)?;
        // (3) the decision itself changed.
        if last.entry != *entry {
            return None;
        }
        // (4) the heartbeat is due. A NEGATIVE elapsed (wall clock stepped
        // backwards — NTP correction, a VM restore) fails OPEN toward writing:
        // the chain never loses information because a clock moved.
        let elapsed = now.saturating_sub(last.at_epoch);
        if elapsed < 0 || elapsed >= self.heartbeat_secs {
            return None;
        }
        Some(last.counters)
    }

    fn record(&self, band: &BandRef, entry: DecisionEntry, at_epoch: i64, counters: CumulativeCounters) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(band.clone(), LastAppend { entry, at_epoch, counters });
    }
}

#[async_trait]
impl DecisionLog for GatedDecisionLog {
    async fn append(
        &self,
        band: &BandRef,
        seed: CumulativeCounters,
        entry: DecisionEntry,
    ) -> Result<CumulativeCounters, StoreError> {
        let now = self.clock.now_epoch_secs();
        if let Some(held) = self.skip_verdict(band, &entry, now) {
            return Ok(held);
        }
        // The lock is NOT held across the await (an inner Postgres append is
        // real I/O). A given band is reconciled serially by kube-runtime, so the
        // read-decide-then-record window is not raced in practice; if it ever
        // were, the failure mode is a duplicate heartbeat row — never a withheld
        // counted decision (rule 1 is state-independent).
        let counters = self.inner.append(band, seed, entry.clone()).await?;
        self.record(band, entry, now, counters);
        Ok(counters)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{decision_content_hash, GENESIS_HASH};
    use std::sync::atomic::{AtomicI64, Ordering};

    /// A test clock the test steps by hand — no sleeping.
    struct FakeClock(AtomicI64);

    impl FakeClock {
        fn new(t: i64) -> Arc<Self> {
            Arc::new(Self(AtomicI64::new(t)))
        }
        fn advance(&self, secs: i64) {
            self.0.fetch_add(secs, Ordering::SeqCst);
        }
        fn set(&self, t: i64) {
            self.0.store(t, Ordering::SeqCst);
        }
    }

    impl Clock for FakeClock {
        fn now_epoch_secs(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    /// One row as the Postgres tier would write it — the chain fields included,
    /// so a test can verify linkage without a database.
    #[derive(Clone)]
    struct Row {
        band: BandRef,
        seq: i64,
        entry: DecisionEntry,
        content_hash: [u8; 32],
        prev_hash: [u8; 32],
    }

    /// A recording inner log that reproduces `PgDecisionLog::append`'s chain
    /// arithmetic exactly (`seq = cur + 1`, `prev = last content_hash`, counters
    /// folded from the durable value) — the mock the gate is proven against.
    #[derive(Default)]
    struct RecordingLog {
        rows: Mutex<Vec<Row>>,
        heads: Mutex<HashMap<BandRef, (i64, [u8; 32], CumulativeCounters)>>,
    }

    impl RecordingLog {
        fn rows_for(&self, band: &BandRef) -> Vec<Row> {
            self.rows
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.band == *band)
                .cloned()
                .collect()
        }
        fn count_for(&self, band: &BandRef) -> usize {
            self.rows_for(band).len()
        }
    }

    #[async_trait]
    impl DecisionLog for RecordingLog {
        async fn append(
            &self,
            band: &BandRef,
            seed: CumulativeCounters,
            entry: DecisionEntry,
        ) -> Result<CumulativeCounters, StoreError> {
            let mut heads = self.heads.lock().unwrap();
            let (cur_seq, prev, current) = heads
                .get(band)
                .copied()
                .unwrap_or((0, GENESIS_HASH, seed));
            let next = current.fold(&entry);
            let seq = cur_seq + 1;
            let content_hash = decision_content_hash(band, seq, &entry, &prev);
            self.rows.lock().unwrap().push(Row {
                band: band.clone(),
                seq,
                entry,
                content_hash,
                prev_hash: prev,
            });
            heads.insert(band.clone(), (seq, content_hash, next));
            Ok(next)
        }
    }

    /// `PgDecisionLog::verify_chain`'s walk, in memory: contiguous seq from 1,
    /// unbroken `prev_hash` linkage from genesis, every `content_hash` recomputes.
    fn chain_verifies(band: &BandRef, rows: &[Row]) -> bool {
        let mut expected_prev = GENESIS_HASH;
        for (i, r) in rows.iter().enumerate() {
            let expected_seq = i64::try_from(i).unwrap() + 1;
            if r.seq != expected_seq || r.prev_hash != expected_prev {
                return false;
            }
            let recomputed = decision_content_hash(band, r.seq, &r.entry, &expected_prev);
            if r.content_hash != recomputed {
                return false;
            }
            expected_prev = recomputed;
        }
        true
    }

    fn observed() -> DecisionEntry {
        DecisionEntry {
            receipt_kind: "Observed".into(),
            class: CounterClass::NoCount,
            from_limit: None,
            to_limit: None,
            dry_run: false,
        }
    }

    fn shadow(from: u64, to: u64) -> DecisionEntry {
        DecisionEntry {
            receipt_kind: "ShadowWouldApply".into(),
            class: CounterClass::NoCount,
            from_limit: Some(from),
            to_limit: Some(to),
            dry_run: true,
        }
    }

    fn applied(from: u64, to: u64) -> DecisionEntry {
        DecisionEntry {
            receipt_kind: "Applied".into(),
            class: CounterClass::Carve,
            from_limit: Some(from),
            to_limit: Some(to),
            dry_run: false,
        }
    }

    fn band() -> BandRef {
        BandRef::new("MemoryBand", "pangea-system", "db")
    }

    #[tokio::test]
    async fn a_steady_band_writes_once_plus_heartbeats_not_once_per_tick() {
        let inner = Arc::new(RecordingLog::default());
        let clock = FakeClock::new(1_000);
        // 15-minute heartbeat over the real 15s cooldown cadence.
        let gated = GatedDecisionLog::with_clock(inner.clone(), 900, clock.clone());
        let b = band();

        // 240 ticks at 15s = 1 hour of a band that just HOLDS.
        let mut counters = CumulativeCounters::ZERO;
        for _ in 0..240 {
            counters = gated.append(&b, counters, observed()).await.unwrap();
            clock.advance(15);
        }

        // 1 anchor + 3 heartbeats (t+900/1800/2700) = 4, not 240.
        assert_eq!(
            inner.count_for(&b),
            4,
            "a steady band must write the anchor + one row per heartbeat, not one per tick"
        );
        // 240 ungated writes → 4: past the ~18× reduction the volume math predicts.
        assert!(inner.count_for(&b) <= 240 / 18, "the gate must be at least the predicted ~18× reduction");
        assert_eq!(counters, CumulativeCounters::ZERO, "a held Observed advances no counter");
        assert!(chain_verifies(&b, &inner.rows_for(&b)), "the gated chain still verifies");
    }

    #[tokio::test]
    async fn a_changed_decision_writes_on_the_change() {
        let inner = Arc::new(RecordingLog::default());
        let clock = FakeClock::new(0);
        let gated = GatedDecisionLog::with_clock(inner.clone(), 900, clock.clone());
        let b = band();

        // Hold, hold, hold — one anchor row.
        for _ in 0..3 {
            gated.append(&b, CumulativeCounters::ZERO, observed()).await.unwrap();
            clock.advance(15);
        }
        assert_eq!(inner.count_for(&b), 1);

        // The decision CHANGES (the band starts wanting a carve) — written
        // immediately, nowhere near the heartbeat.
        gated.append(&b, CumulativeCounters::ZERO, shadow(100, 200)).await.unwrap();
        assert_eq!(inner.count_for(&b), 2, "a changed decision writes at once");
        clock.advance(15);

        // The SAME shadow decision repeats — gated again.
        gated.append(&b, CumulativeCounters::ZERO, shadow(100, 200)).await.unwrap();
        assert_eq!(inner.count_for(&b), 2, "an unchanged repeat is withheld");
        clock.advance(15);

        // A shadow whose target MOVED is a different decision — written.
        gated.append(&b, CumulativeCounters::ZERO, shadow(100, 250)).await.unwrap();
        assert_eq!(inner.count_for(&b), 3, "a moved target is a material change");

        assert!(chain_verifies(&b, &inner.rows_for(&b)));
    }

    #[tokio::test]
    async fn a_counted_decision_is_never_withheld_even_when_identical() {
        let inner = Arc::new(RecordingLog::default());
        let clock = FakeClock::new(0);
        let gated = GatedDecisionLog::with_clock(inner.clone(), 900, clock.clone());
        let b = band();

        // Two byte-identical carves back to back, inside the heartbeat window:
        // rule 1 is state-independent, so BOTH are written and BOTH are counted.
        let c1 = gated.append(&b, CumulativeCounters::ZERO, applied(100, 200)).await.unwrap();
        clock.advance(1);
        let c2 = gated.append(&b, c1, applied(100, 200)).await.unwrap();
        assert_eq!(c1.carves, 1);
        assert_eq!(c2.carves, 2, "a carve must never be gated away — the counters derive from the log");
        assert_eq!(inner.count_for(&b), 2);
        assert!(chain_verifies(&b, &inner.rows_for(&b)));
    }

    #[tokio::test]
    async fn the_heartbeat_interval_is_the_configured_one() {
        let inner = Arc::new(RecordingLog::default());
        let clock = FakeClock::new(0);
        let gated = GatedDecisionLog::with_clock(inner.clone(), 60, clock.clone());
        let b = band();

        gated.append(&b, CumulativeCounters::ZERO, observed()).await.unwrap(); // anchor
        clock.set(59);
        gated.append(&b, CumulativeCounters::ZERO, observed()).await.unwrap();
        assert_eq!(inner.count_for(&b), 1, "before the interval elapses: withheld");
        clock.set(60);
        gated.append(&b, CumulativeCounters::ZERO, observed()).await.unwrap();
        assert_eq!(inner.count_for(&b), 2, "at the interval: the heartbeat writes");
        clock.set(119);
        gated.append(&b, CumulativeCounters::ZERO, observed()).await.unwrap();
        assert_eq!(inner.count_for(&b), 2, "the window restarts from the last write");
    }

    #[tokio::test]
    async fn heartbeat_zero_disables_the_gate() {
        let inner = Arc::new(RecordingLog::default());
        let clock = FakeClock::new(0);
        let gated = GatedDecisionLog::with_clock(inner.clone(), 0, clock.clone());
        let b = band();
        for _ in 0..10 {
            gated.append(&b, CumulativeCounters::ZERO, observed()).await.unwrap();
        }
        assert_eq!(inner.count_for(&b), 10, "heartbeat 0 = the pre-gate escape hatch");
    }

    #[tokio::test]
    async fn a_backwards_clock_step_fails_open_toward_writing() {
        let inner = Arc::new(RecordingLog::default());
        let clock = FakeClock::new(10_000);
        let gated = GatedDecisionLog::with_clock(inner.clone(), 900, clock.clone());
        let b = band();
        gated.append(&b, CumulativeCounters::ZERO, observed()).await.unwrap();
        clock.set(5_000); // NTP correction / VM restore
        gated.append(&b, CumulativeCounters::ZERO, observed()).await.unwrap();
        assert_eq!(inner.count_for(&b), 2, "a backwards clock must never silence the log");
    }

    #[tokio::test]
    async fn withheld_ticks_report_the_counters_from_the_last_append() {
        let inner = Arc::new(RecordingLog::default());
        let clock = FakeClock::new(0);
        let gated = GatedDecisionLog::with_clock(inner.clone(), 900, clock.clone());
        let b = band();

        // Seed a real durable count via a carve, then hold.
        let after_carve = gated
            .append(&b, CumulativeCounters { carves: 4, deferrals: 1, conflicts: 0 }, applied(1, 2))
            .await
            .unwrap();
        assert_eq!(after_carve.carves, 5);
        gated.append(&b, after_carve, observed()).await.unwrap(); // anchors Observed
        for _ in 0..20 {
            clock.advance(15);
            let held = gated.append(&b, CumulativeCounters::ZERO, observed()).await.unwrap();
            assert_eq!(
                held, after_carve,
                "a withheld tick reports the last APPENDED counters, never the (possibly stale) seed"
            );
        }
    }

    #[tokio::test]
    async fn bands_are_gated_independently() {
        let inner = Arc::new(RecordingLog::default());
        let clock = FakeClock::new(0);
        let gated = GatedDecisionLog::with_clock(inner.clone(), 900, clock.clone());
        let a = BandRef::new("MemoryBand", "ns", "a");
        let b = BandRef::new("MemoryBand", "ns", "b");

        gated.append(&a, CumulativeCounters::ZERO, observed()).await.unwrap();
        gated.append(&b, CumulativeCounters::ZERO, observed()).await.unwrap();
        clock.advance(15);
        // `a` holds (withheld); `b` changes (written).
        gated.append(&a, CumulativeCounters::ZERO, observed()).await.unwrap();
        gated.append(&b, CumulativeCounters::ZERO, shadow(1, 2)).await.unwrap();

        assert_eq!(inner.count_for(&a), 1);
        assert_eq!(inner.count_for(&b), 2);
        assert!(chain_verifies(&a, &inner.rows_for(&a)));
        assert!(chain_verifies(&b, &inner.rows_for(&b)));
    }

    #[tokio::test]
    async fn a_gated_run_produces_a_contiguous_verifiable_chain() {
        // The ★ integrity property, over a long mixed sequence: gating removes
        // candidate rows BEFORE they enter the chain, so seq stays contiguous
        // from 1 and every link recomputes. No gaps in seq, ever.
        let inner = Arc::new(RecordingLog::default());
        let clock = FakeClock::new(0);
        let gated = GatedDecisionLog::with_clock(inner.clone(), 300, clock.clone());
        let b = band();

        let mut counters = CumulativeCounters::ZERO;
        for i in 0..400 {
            let e = match i % 97 {
                0 => applied(100, 200),
                7 => shadow(100, 150 + i),
                _ => observed(),
            };
            counters = gated.append(&b, counters, e).await.unwrap();
            clock.advance(15);
        }

        let rows = inner.rows_for(&b);
        assert!(rows.len() < 400, "the gate withheld ticks ({} rows of 400)", rows.len());
        assert!(!rows.is_empty());
        assert!(chain_verifies(&b, &rows), "a gated chain verifies from genesis, contiguously");
        // The counted decisions all survived: 400/97 → i = 0, 97, 194, 291, 388.
        assert_eq!(counters.carves, 5, "every carve reached the log");
    }
}
