//! `breathe-control` — the dimension-agnostic resource-balancing core.
//!
//! The proven heart of the `breathe` homeostasis substrate
//! ([`theory/BREATHE.md`](https://github.com/pleme-io/theory/blob/main/BREATHE.md) §2):
//! every "resident problem category" (memory, storage, cpu, …) projects into the
//! two scalars `(used, capacity)`, and the *same* band law holds it inside a
//! typed utilization band (default 80% used / 20% headroom) by gentle, bounded
//! steps that converge over many single-shot ticks. No I/O lives here — every
//! function is a pure mapping from observed state to a [`Decision`] / [`TickPlan`],
//! so the whole balancing algebra is unit-testable without a cluster. A provider
//! never sees `decide`/`BandConfig`; it receives a computed target value and
//! cannot re-decide, widen the band, or subvert the safety clamp.
//!
//! Responsibilities (all pure, all tested):
//!   1. [`decide`] — the bidirectional band law, with a shrink-safety clamp that
//!      makes a shrink provably unable to push live usage over the band.
//!   2. [`competing_field_manager`] — the field-granular single-writer invariant:
//!      yield to any *other* manager owning the same field path (breathe ⟂ KEDA,
//!      memory ⟂ cpu provable), never fight.
//!   3. [`clamp_to_directionality`] — `GrowOnly` / `ObserveOnly` policy, so a new
//!      directionality needs zero new band code.
//!   4. [`plan_tick`] — the pure reconcile heart: guard → decide → directionality
//!      → freshness → cooldown → a [`TickPlan`] the async loop executes.

/// `lapidar` — the self-optimization loop every CONTROLLED band runs by default
/// (analyze → suggest → apply → watch → test → accept/revert). Pure, like the
/// band law it refines.
pub mod lapidar;

/// `replica` — the HORIZONTAL band law: how many replicas a workload should run
/// given a work-rate signal, held in a typed count band with asymmetric anti-flap,
/// an HA floor, and spot-reclaim-driven scale-OUT. The horizontal peer of the
/// vertical [`decide`] limit law; pure + mockable (the TYPED-SPEC triplet).
pub mod replica;

/// `lifecycle` — the LIFECYCLE BREATH: a workload breathes through a five-phase
/// lifecycle (Zero → Wake+expansion → Settle+shadow-tighten → Steady → Idle→Zero),
/// not just a held setpoint (theory/BREATHABILITY.md §II.5 default #6). The
/// ORCHESTRATOR that composes [`decide`]/[`safe_min`]/[`soft_min`]/[`lapidar`] into
/// the lifecycle, with the never-stuck invariant encoded in the type system where it
/// can be (a phantom-typestate FSM + a parse-don't-validate `Confirmed` witness) and
/// property-tested where it cannot. Pure + mockable (the TYPED-SPEC triplet).
pub mod lifecycle;

/// `carve_safety` — the STRUCTURAL storage-carve safety triad: footprint
/// ownership ([`carve_safety::OwnedPvc`] — a resident volume is unownable),
/// release witnesses ([`carve_safety::DurableVolume`] has no release method;
/// [`carve_safety::NotInUse`] gates a regenerable release), and the grow-only
/// autonomous typestate ([`carve_safety::AutonomousStorageCarve`] emits only a
/// bounded, atomic [`carve_safety::SmallAtomicGrow`]). Every witness is re-minted
/// from live state and re-verified per tick at the atomic commit. Pure, like the
/// band law it guards.
pub mod carve_safety;

/// Tunable band/step policy. Every knob is config-driven (a `MemoryBand` CR's
/// spec → the watcher's args). Defaults encode the 80/20 setpoint with a
/// What a band does when its working-set reading is UNTRUSTED — a `0` reading
/// from a running workload (almost always a broken/missing/lagging metric, not a
/// real "needs nothing"), or an explicitly missing scrape. The non-negotiable
/// invariant regardless of policy: **an untrusted reading may NEVER drive a
/// downward carve** — trusting it carves to floor and OOM-kills the pod when its
/// true working set returns (the fleet-wide split-brain). The policy only
/// chooses what to do ABOVE that floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MetricMissingPolicy {
    /// DEFAULT + safest. Restore headroom: if the limit currently sits below the
    /// durable demonstrated-peak safe floor ([`safe_min`] computed from the
    /// carried peak), grow back up to it; otherwise hold. A blind observer
    /// assumes the worst and provides the headroom it cannot prove is unneeded.
    #[default]
    RestoreHeadroom,
    /// Hold the current limit exactly — never shrink, never grow — until a
    /// trusted reading returns. For bands where a transient metric gap should not
    /// move the limit at all.
    Hold,
    /// Treat `0` as a REAL reading and run the band law normally — the gate never
    /// fires. For dimensions where zero is legitimate, not a broken metric:
    /// node-COUNT bands (a pool scaled to zero genuinely has 0 nodes), and any
    /// host/CR-field dimension whose `0` is a true value. Never use for memory/cpu
    /// of a running pod, where `0` is always a degraded metric.
    Trust,
}

/// ~15-point deadband (70–85%).
#[derive(Debug, Clone)]
pub struct BandConfig {
    /// Utilization strictly above this triggers a grow. Default `0.85`.
    pub grow_above: f64,
    /// Utilization strictly below this triggers a shrink. Default `0.70`.
    pub shrink_below: f64,
    /// Target utilization the shrink-safety clamp lands on. Default `0.80`.
    pub setpoint: f64,
    /// Multiplier applied to the limit on grow. Default `1.25`.
    pub grow_factor: f64,
    /// Multiplier applied to the limit on shrink (gentle). Default `0.90`.
    pub shrink_factor: f64,
    /// Never shrink the limit below this many bytes. Default 256Mi.
    pub floor_bytes: u64,
    /// Never grow the limit above this many bytes. Default 16Gi.
    pub ceiling_bytes: u64,
    /// The operator's DECLARED guaranteed working set — `resources.requests.<r>`
    /// for a k8s carve. A shrink can never drop the limit below this: requests is
    /// the scheduler's guarantee that the workload always has at least this much,
    /// so carving the LIMIT under the REQUEST is both nonsensical (limit < request
    /// is rejected by k8s) and unsafe (it removes the operator-declared headroom).
    /// `0` = no declared request floor (the unset default — behaviour-preserving).
    pub request_floor_bytes: u64,
    /// WARMUP HOLD — the minimum seconds a workload must be OBSERVED (since its last
    /// restart) before any SHRINK is permitted. A workload that (re)started less than
    /// `warmup_seconds` ago has not yet demonstrated a full duty cycle: its idle
    /// reading is not proof it is safe to carve. So a shrink proposal during warmup
    /// becomes a HOLD ([`Decision::Warmup`]) regardless of how low the observed
    /// utilization is. This closes the un-observed-boot-spike hole the demonstrated-
    /// peak floor alone cannot: the authentik worker's blueprint-discovery spike
    /// happens at boot, BEFORE the first scrape, so the peak floor only ever saw idle
    /// and carved to idle. Holding through warmup means the spike is observed (and
    /// folds into the peak) before any carve. Default `600` (10 min). A grow is NEVER
    /// held — buying headroom is always safe. `0` = warmup disabled.
    pub warmup_seconds: u64,
    /// What to do when the working-set reading is UNTRUSTED (a `0` from a running
    /// workload — a degraded metric, not a real zero). An untrusted reading NEVER
    /// shrinks regardless of this; the policy chooses hold vs restore-headroom
    /// above the floor. Default [`MetricMissingPolicy::RestoreHeadroom`].
    pub metric_missing_policy: MetricMissingPolicy,
}

impl Default for BandConfig {
    fn default() -> Self {
        Self {
            grow_above: 0.85,
            shrink_below: 0.70,
            setpoint: 0.80,
            grow_factor: 1.25,
            shrink_factor: 0.90,
            floor_bytes: 256 * (1 << 20),
            ceiling_bytes: 16 * (1 << 30),
            request_floor_bytes: 0,
            warmup_seconds: 600,
            metric_missing_policy: MetricMissingPolicy::RestoreHeadroom,
        }
    }
}

/// Why a [`BandConfig`] is rejected at the CRD→config boundary — the typed
/// parse-time gate that keeps a *malformed* band out of the loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BandConfigError {
    /// A threshold is outside `(0, 1]` or the deadband is not well-ordered
    /// (`shrink_below ≤ setpoint ≤ grow_above`).
    BadBand,
    /// `grow_factor ≤ 1` (a grow must raise) or `shrink_factor ∉ (0, 1)`.
    BadFactor,
    /// `floor_bytes > ceiling_bytes` (an empty operating range).
    EmptyRange,
}

impl std::fmt::Display for BandConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadBand => f.write_str("band thresholds must satisfy 0 < shrink_below ≤ setpoint ≤ grow_above ≤ 1"),
            Self::BadFactor => f.write_str("grow_factor must be > 1 and shrink_factor in (0, 1)"),
            Self::EmptyRange => f.write_str("floor_bytes must be ≤ ceiling_bytes"),
        }
    }
}

impl std::error::Error for BandConfigError {}

impl BandConfig {
    /// Reject a MALFORMED config at the CRD→config boundary (parse-time), before
    /// it drives a tick: a well-ordered deadband, a grow that raises, a shrink
    /// that lowers, a non-empty operating range. The harder *dead-time flap
    /// margin* (P4) is deliberately NOT asserted here — the naive "a single step
    /// must land inside the band" bound is wrong (it flags the shipped default,
    /// which provably converges grow→shrink→hold rather than limit-cycling);
    /// the true bound is the round-trip-gain analysis and is a typed follow-on.
    /// `safety_clamp` already proves the load-bearing *never-OOM* invariant for
    /// any config; this is the authoring-sanity complement.
    ///
    /// # Errors
    /// A typed [`BandConfigError`] naming the first violated invariant.
    pub fn validate(&self) -> Result<(), BandConfigError> {
        let in_unit = |x: f64| x > 0.0 && x <= 1.0;
        if !(in_unit(self.shrink_below) && in_unit(self.setpoint) && in_unit(self.grow_above)
            && self.shrink_below <= self.setpoint && self.setpoint <= self.grow_above)
        {
            return Err(BandConfigError::BadBand);
        }
        if self.grow_factor <= 1.0 || !(self.shrink_factor > 0.0 && self.shrink_factor < 1.0) {
            return Err(BandConfigError::BadFactor);
        }
        if self.floor_bytes > self.ceiling_bytes {
            return Err(BandConfigError::EmptyRange);
        }
        Ok(())
    }

    /// Apply a single `lapidar` trial override to this IN-MEMORY config (the CR
    /// spec is never touched — `self` was built from spec; this returns a new
    /// value the band law uses for this tick only).
    ///
    /// **Clamped-by-construction:** the one changed field is clamped into the
    /// interval bounded by its UNCHANGED neighbours, so the well-ordered-deadband
    /// invariant ([`Self::validate`]) holds for the result no matter how *stale*
    /// the stored override is (e.g. a later operator spec edit narrowed the band
    /// under the override). Because `lapidar` tunes exactly one ordering field at
    /// a time over an already-valid spec, clamping that single field preserves
    /// `shrink_below ≤ setpoint ≤ grow_above` — infallible, debug-asserted. A
    /// `NaN` override is inert.
    #[must_use]
    pub fn with_override(mut self, param: lapidar::TunedParam, value: f64) -> Self {
        if value.is_nan() {
            return self;
        }
        // The smallest sane positive utilization fraction for a shrink trigger.
        const MIN_FRAC: f64 = 0.01;
        match param {
            lapidar::TunedParam::Setpoint => {
                self.setpoint = value.clamp(self.shrink_below, self.grow_above);
            }
            lapidar::TunedParam::GrowAbove => {
                self.grow_above = value.clamp(self.setpoint, 1.0);
            }
            lapidar::TunedParam::ShrinkBelow => {
                self.shrink_below = value.clamp(MIN_FRAC, self.setpoint);
            }
            lapidar::TunedParam::WarmupSeconds => {
                // NaN already returned; negative → 0; huge → saturating cast.
                self.warmup_seconds = value.max(0.0) as u64;
            }
        }
        debug_assert!(self.validate().is_ok(), "with_override must preserve a valid band");
        self
    }
}

#[cfg(test)]
mod with_override_tests {
    use super::BandConfig;
    use super::lapidar::TunedParam;

    #[test]
    fn setpoint_override_applies_within_band() {
        let cfg = BandConfig::default().with_override(TunedParam::Setpoint, 0.75);
        assert!((cfg.setpoint - 0.75).abs() < 1e-9);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn stale_override_is_clamped_into_the_current_band() {
        // grow_above default 0.85; a stale setpoint override of 0.99 clamps to 0.85.
        let cfg = BandConfig::default().with_override(TunedParam::Setpoint, 0.99);
        assert!((cfg.setpoint - cfg.grow_above).abs() < 1e-9, "clamped to grow_above");
        assert!(cfg.validate().is_ok(), "clamp preserves validity");
        // a below-floor shrink_below override clamps up, not through 0.
        let cfg2 = BandConfig::default().with_override(TunedParam::ShrinkBelow, -5.0);
        assert!(cfg2.shrink_below > 0.0 && cfg2.validate().is_ok());
    }

    #[test]
    fn grow_above_override_stays_ordered() {
        let cfg = BandConfig::default().with_override(TunedParam::GrowAbove, 0.90);
        assert!(cfg.grow_above >= cfg.setpoint && cfg.grow_above <= 1.0);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn warmup_override_saturates_and_nan_is_inert() {
        let cfg = BandConfig::default().with_override(TunedParam::WarmupSeconds, 900.0);
        assert_eq!(cfg.warmup_seconds, 900);
        let neg = BandConfig::default().with_override(TunedParam::WarmupSeconds, -1.0);
        assert_eq!(neg.warmup_seconds, 0);
        let nan = BandConfig::default().with_override(TunedParam::Setpoint, f64::NAN);
        assert!((nan.setpoint - BandConfig::default().setpoint).abs() < 1e-9, "NaN inert");
    }

    #[test]
    fn every_param_override_keeps_a_valid_band() {
        for p in [TunedParam::Setpoint, TunedParam::GrowAbove, TunedParam::ShrinkBelow, TunedParam::WarmupSeconds] {
            for v in [-1.0, 0.0, 0.5, 0.83, 1.0, 5.0, 1e9] {
                assert!(BandConfig::default().with_override(p, v).validate().is_ok(), "{p:?} {v}");
            }
        }
    }
}

/// The outcome of one band evaluation for one target. Every non-`Hold` variant
/// is observable (the watcher emits a typed event) so a tick's behaviour is
/// fully legible in the logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Inside the deadband — do nothing.
    Hold,
    /// Grow the limit (low headroom).
    Grow { from: u64, to: u64 },
    /// Shrink the limit (excess headroom), gently + safely.
    Shrink { from: u64, to: u64 },
    /// Would grow but already at/over the ceiling.
    AtCeiling { current: u64 },
    /// Would shrink but cannot do so safely (floor / safe-min binds).
    NoSafeShrink { current: u64 },
    /// Container declares no memory limit — the controller refuses to reason
    /// about utilization without a denominator. Skip + surface.
    NoLimit,
    /// Would shrink, but the workload is still in its WARMUP window (it (re)started
    /// less than `warmup_seconds` ago and has not demonstrated a full duty cycle).
    /// A shrink is HELD — the idle reading is not yet proof the slack is safe to take
    /// (the un-observed-boot-spike hole). `current` is the held limit; `observed_for`
    /// the seconds the workload has been observed; `warmup` the configured window. A
    /// grow is never held (raising the limit is always safe).
    Warmup { current: u64, observed_for: u64, warmup: u64 },
    /// Would shrink, but the workload's SUPPRESSED DEMAND is non-blind: the resource
    /// is being actively THROTTLED right now (CFS throttling for cpu), or the
    /// workload recently (re)started / is crash-looping. The low observed `used` is
    /// therefore not proof the slack is safe to reclaim — for a hard-capped soft
    /// resource like CPU the observed usage can NEVER exceed the limit (CFS caps it),
    /// so a usage-keyed shrink would ratchet a bursty/idle workload to its floor and
    /// starve it. A shrink is HELD; a grow is never held (relieving the throttle is
    /// the safe direction). `current` is the held limit; `restarting` is `true` when
    /// the hold is driven by a recent restart / crash-loop (vs live throttling).
    Throttled { current: u64, restarting: bool },
}

/// A control law's RAW proposal for one tick — the target limit it wants,
/// BEFORE the shared safety gate makes it safe. `Hold` = utilization is in-band.
/// `Target(t)` = move toward `t` (grow if above the current limit, shrink if
/// below); the gate clamps `t` to `[safe_min/floor, ceiling]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Proposal {
    Hold,
    Target(u64),
}

/// Which CGROUP LIMIT a carve targets — the typed soft/hard distinction that makes
/// a carve-induced OOM impossible by construction.
///
/// A k8s `resources.limits.memory` IS the cgroup `memory.max` — a **HARD** limit;
/// exceeding it is an instant OOM-kill, with no warning and no chance for the
/// kernel to reclaim. By contrast cgroup v2 `memory.high` is a **SOFT** limit:
/// crossing it triggers reclaim + throttling, *never* a kill. So an efficiency
/// carve — "this workload is idle, take its slack back" — must target `memory.high`
/// (`Soft`): if a transient, un-observed spike then exceeds the soft target, the
/// kernel reclaims and throttles rather than OOM-killing. The HARD ceiling
/// (`memory.max` / the k8s limit) is governed by the peak-floor never-OOM ceiling
/// ONLY (see [`safe_min`]); it is never lowered for efficiency.
///
/// This is the structural fix for the authentik-worker OOM (2026-06): the worker
/// was carved toward its 40%-idle reading, but blueprint discovery is a transient
/// ~600 MB boot spike that OOM-killed the pod DURING the spike — before
/// metrics-server ever scraped it, so the demonstrated-peak floor only ever saw
/// idle. Had the efficiency carve targeted `memory.high` (reclaim) instead of
/// `memory.max` (kill), the spike would have throttled, not died.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CarveSemantics {
    /// `memory.high` (cgroup v2) — exceeding it RECLAIMS + THROTTLES, never kills.
    /// The efficiency-carve target. A `Soft` shrink can safely sit at the *working
    /// set* seat (it can only throttle a spike, never kill it), so its floor is the
    /// gentler [`soft_min`] (request/config floor), not the peak-over-window ceiling.
    Soft,
    /// `memory.max` (cgroup v2) == the k8s `resources.limits.memory` — a HARD cap;
    /// exceeding it OOM-KILLS. Only ever set to the never-OOM ceiling ([`safe_min`],
    /// keyed on the demonstrated peak + the declared request floor) and NEVER carved
    /// beneath it for efficiency. A grow always targets `Hard` (raising the kill
    /// ceiling buys headroom — always safe).
    Hard,
}

impl CarveSemantics {
    /// `true` iff exceeding this target can OOM-kill the workload (the `Hard`
    /// `memory.max` / k8s limit). A `Soft` target only ever reclaims + throttles.
    #[must_use]
    pub fn can_oom(self) -> bool {
        matches!(self, Self::Hard)
    }
}

/// The never-OOM HARD floor: the lowest `memory.max` (k8s limit) a carve may ever
/// set, keyed on the DEMONSTRATED peak working set (max over the trailing window) +
/// the declared request floor + the configured floor. Exceeding `memory.max` kills,
/// so this floor can never drop beneath what the workload has been observed to need:
/// `max( ceil(peak / setpoint), request_floor_bytes, floor_bytes )`. Pure; the
/// single source of truth for the hard floor both [`safety_clamp`] and the dual
/// soft/hard planner consume.
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn safe_min(peak_working_set: u64, working_set: u64, cfg: &BandConfig) -> u64 {
    let peak = peak_working_set.max(working_set);
    let setpoint = if cfg.setpoint <= 0.0 { 1.0 } else { cfg.setpoint };
    let from_peak = ((peak as f64) / setpoint).ceil() as u64;
    from_peak.max(cfg.request_floor_bytes).max(cfg.floor_bytes)
}

/// The SOFT floor: the lowest `memory.high` an efficiency carve may set. Because a
/// soft target only RECLAIMS + THROTTLES (never kills), it does NOT need the
/// peak-over-window ceiling — it may safely seat the soft limit at the live
/// working set's setpoint, throttling (not killing) a spike that exceeds it. It is
/// still bounded below by the operator's declared `request_floor_bytes` and the
/// configured `floor_bytes` (never push reclaim below the guaranteed working set).
/// The soft floor is therefore always `≤` the [`safe_min`] hard floor, which is the
/// whole point: `memory.high` can sit tighter than `memory.max`, so efficiency is
/// reclaimed without ever lowering the kill ceiling.
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn soft_min(working_set: u64, cfg: &BandConfig) -> u64 {
    let setpoint = if cfg.setpoint <= 0.0 { 1.0 } else { cfg.setpoint };
    let from_ws = ((working_set as f64) / setpoint).ceil() as u64;
    from_ws.max(cfg.request_floor_bytes).max(cfg.floor_bytes)
}

// ─────────────────────────────────────────────────────────────────────────────
// PROVISION-MINIMAL + GROW-ON-DEMAND — the storage carve, and the
// over-provisioning invariant it makes unrepresentable.
// ─────────────────────────────────────────────────────────────────────────────
//
// Storage is the `GrowOnly` dimension: a PVC / EBS volume expands ONLINE (CSI
// resize / EBS `ModifyVolume`) but can never shrink in place. The breathability
// law — "carve every resource dimension to its setpoint" — is a two-sided clamp
// for a `Bidirectional` dimension (memory / cpu) and a ONE-SIDED carve for
// storage: **provision-minimal + grow-on-demand**. A `StorageBand` starts a
// volume at a small floor and grows it online as demonstrated usage climbs toward
// the setpoint. The size breathe would ever set is exactly [`safe_min`] —
//
//   `target = max( ceil(peak_used / setpoint), request_floor_bytes, floor_bytes )`
//
// so a 50 GiB volume holding 890 MiB is a state breathe's OWN carve can never
// construct: it would have provisioned ~2 GiB and grown only with real data. That
// exact receipt — 155 GiB provisioned across camelot, ~5 GiB used — is the
// signature of the missing carve, not a leak to detect after the fact. The carve
// removes the whole waste class.

/// The provision-minimal + grow-on-demand carve TARGET — the size breathe would
/// set for a `GrowOnly` volume given its demonstrated peak usage. It is
/// [`safe_min`] under the storage-carve name: ONE dimension-agnostic setpoint
/// carve, two names (`safe_min` is the never-OOM hard floor a memory band never
/// drops below; `provision_target` is the grow-on-demand size a storage band
/// grows toward). The setpoint-carve `max(ceil(peak/setpoint), request_floor,
/// floor)` is identical because the band law is unit- and dimension-agnostic.
#[must_use]
pub fn provision_target(peak_used: u64, used: u64, cfg: &BandConfig) -> u64 {
    safe_min(peak_used, used, cfg)
}

/// The verdict of classifying a CURRENTLY-provisioned size against the
/// provision-minimal carve target. Total: every `(provisioned, target)` pair maps
/// to exactly one arm. The whole point is the codomain of breathe's own carve —
/// [`carve_output_verdict`]'s theorem proves the carve can only ever emit
/// `{RightSized, UnderProvisioned}`, so [`OverProvisioned`](Self::OverProvisioned)
/// is *unrepresentable in breathe's actuation output*.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvisionVerdict {
    /// `provisioned` sits within one grow-step of `target` — the size breathe's
    /// own carve converges to. The steady state.
    RightSized { provisioned: u64, target: u64 },
    /// `provisioned < target`: demonstrated usage has climbed past what the
    /// current size holds at the setpoint. Grow-on-demand raises it to `target`
    /// this / next tick — the NORMAL grow path, never waste.
    UnderProvisioned { provisioned: u64, target: u64, deficit: u64 },
    /// `provisioned ≫ target`: more capacity than the carve would EVER set for
    /// this usage — reclaimable waste. UNREACHABLE via breathe's grow-only
    /// actuation (breathe never sets a size above the carve target); only
    /// constructible by an EXTERNAL over-declaration (a chart's fixed `50Gi`, a
    /// hand-provisioned PVC). Grow-only cannot shrink it, so the reclaim is a
    /// one-time recreate — surfaced here so it is a typed, observable anomaly and
    /// never a silent 30×-idle volume.
    OverProvisioned { provisioned: u64, target: u64, waste: u64 },
}

impl ProvisionVerdict {
    /// `true` iff this is the reclaimable-waste arm — the only verdict breathe's
    /// own carve can never produce (proved by [`carve_output_verdict`]).
    #[must_use]
    pub fn is_over_provisioned(self) -> bool {
        matches!(self, Self::OverProvisioned { .. })
    }
    /// The reclaimable waste in bytes (`0` for the two non-waste arms).
    #[must_use]
    pub fn waste_bytes(self) -> u64 {
        match self {
            Self::OverProvisioned { waste, .. } => waste,
            _ => 0,
        }
    }
}

/// The largest size the grow-only carve would ever HOLD for a given target: the
/// carve target plus at most one transient grow-step of headroom (a grow lands at
/// `ceil(limit * grow_factor)`, then converges as the bigger denominator drops
/// utilization back inside the band). A provisioned size above this ceiling is
/// slack the carve does not create — the over-provisioning boundary.
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn carve_grow_ceiling(target: u64, cfg: &BandConfig) -> u64 {
    let factor = if cfg.grow_factor <= 1.0 { 1.0 } else { cfg.grow_factor };
    ((target as f64) * factor).ceil() as u64
}

/// Classify a CURRENTLY-provisioned size against the provision-minimal carve
/// target for its demonstrated usage. Pure + total. This is the typed detector
/// that turns "155 GiB provisioned / 5 GiB used" from an invisible waste into a
/// [`ProvisionVerdict::OverProvisioned`] with an exact `waste` figure a posture
/// controller can act on.
#[must_use]
pub fn classify_provision(
    provisioned: u64,
    peak_used: u64,
    used: u64,
    cfg: &BandConfig,
) -> ProvisionVerdict {
    let target = provision_target(peak_used, used, cfg);
    if provisioned < target {
        ProvisionVerdict::UnderProvisioned { provisioned, target, deficit: target - provisioned }
    } else if provisioned <= carve_grow_ceiling(target, cfg) {
        ProvisionVerdict::RightSized { provisioned, target }
    } else {
        ProvisionVerdict::OverProvisioned { provisioned, target, waste: provisioned - target }
    }
}

/// The FIXPOINT of the grow-only carve started from the band floor for a workload
/// whose demonstrated usage is `used`: repeatedly apply the grow decision under
/// the `GrowOnly` directionality clamp until it stops growing. This is the size
/// breathe's own actuation converges a volume to — the codomain sample the
/// over-provisioning theorem quantifies over. Pure; bounded (a grow strictly
/// raises the limit and the ceiling bounds it, so it converges).
#[must_use]
pub fn carve_fixpoint(used: u64, cfg: &BandConfig) -> u64 {
    let mut current = cfg.floor_bytes.max(1);
    // The ceiling bounds the number of grow steps; 128 is far above any realistic
    // floor→ceiling ratio for the default config, and the loop breaks on Hold.
    for _ in 0..128 {
        match clamp_to_directionality(decide(used, current, cfg), Directionality::GrowOnly) {
            Decision::Grow { to, .. } if to > current => current = to,
            // Hold / NoSafeShrink / AtCeiling / NoLimit / warmup / throttle → converged.
            _ => break,
        }
    }
    current
}

/// The verdict of the carve's OWN OUTPUT for a demonstrated usage — the size
/// breathe converges a volume to, classified against the target. The
/// over-provisioning theorem ([`breathe_carve_never_over_provisions`]) asserts
/// this is NEVER [`ProvisionVerdict::OverProvisioned`], for any usage — the
/// constructive proof that over-provisioning is unrepresentable in breathe's
/// actuation. Peak == usage (steady state) is the worst case for the bound.
#[must_use]
pub fn carve_output_verdict(used: u64, cfg: &BandConfig) -> ProvisionVerdict {
    let provisioned = carve_fixpoint(used, cfg);
    classify_provision(provisioned, used, used, cfg)
}

#[cfg(test)]
mod provision_tests {
    use super::{
        carve_fixpoint, carve_output_verdict, classify_provision, provision_target, BandConfig,
        ProvisionVerdict,
    };

    const MI: u64 = 1 << 20;
    const GI: u64 = 1 << 30;

    /// A provision-minimal storage band: a small 2 GiB floor, a large ceiling, the
    /// aggressive 80% setpoint — the fleet-default `StorageBand` shape.
    fn storage_cfg() -> BandConfig {
        BandConfig { floor_bytes: 2 * GI, ceiling_bytes: 200 * GI, ..BandConfig::default() }
    }

    /// The carve target IS the provision-minimal size: for tiny usage it floors at
    /// the (small) provision floor; for real usage it is `ceil(peak/setpoint)`.
    #[test]
    fn provision_target_is_floor_bounded_setpoint_carve() {
        let c = storage_cfg();
        // 890 MiB used → ceil(890/0.8) ≈ 1113 MiB, but the 2 GiB floor binds.
        assert_eq!(provision_target(890 * MI, 890 * MI, &c), 2 * GI);
        // 40 GiB used → ceil(40/0.8) = 50 GiB, above the floor.
        assert_eq!(provision_target(40 * GI, 40 * GI, &c), 50 * GI);
    }

    /// THE 155 GiB RECEIPT: a 50 GiB volume holding 890 MiB is `OverProvisioned`
    /// with an exact ~48 GiB waste — the typed detection of the class the missing
    /// carve produced. A posture controller reads `waste_bytes()` and acts.
    #[test]
    fn a_50gib_volume_holding_890mib_is_over_provisioned() {
        let c = storage_cfg();
        let v = classify_provision(50 * GI, 890 * MI, 890 * MI, &c);
        assert!(v.is_over_provisioned(), "50GiB/890MiB must be over-provisioned, got {v:?}");
        match v {
            ProvisionVerdict::OverProvisioned { target, waste, .. } => {
                assert_eq!(target, 2 * GI, "carve would have provisioned the 2 GiB floor");
                assert_eq!(waste, 50 * GI - 2 * GI, "≈48 GiB reclaimable");
            }
            other => panic!("expected OverProvisioned, got {other:?}"),
        }
    }

    /// A volume the carve itself grew (a full 2 GiB PVC → grows) classifies
    /// `RightSized` or `UnderProvisioned`, never waste — the steady-state carve is
    /// not mistaken for over-provisioning.
    #[test]
    fn a_carve_grown_volume_is_not_flagged_as_waste() {
        let c = storage_cfg();
        // 8 GiB used in a 10 GiB volume (80% — exactly the setpoint) → RightSized.
        let v = classify_provision(10 * GI, 8 * GI, 8 * GI, &c);
        assert!(!v.is_over_provisioned(), "an at-setpoint volume is not waste: {v:?}");
    }

    /// A volume that has FILLED past its setpoint is `UnderProvisioned` (grow path),
    /// not waste — the grow-on-demand direction.
    #[test]
    fn a_filling_volume_is_under_provisioned() {
        let c = storage_cfg();
        // 9.5 GiB used in a 10 GiB volume (95%) → target ceil(9.5/0.8) ≈ 11.9 GiB > 10.
        match classify_provision(10 * GI, 95 * GI / 10, 95 * GI / 10, &c) {
            ProvisionVerdict::UnderProvisioned { deficit, .. } => assert!(deficit > 0),
            other => panic!("a filling volume must be UnderProvisioned (grow), got {other:?}"),
        }
    }

    /// THE OVER-PROVISIONING THEOREM (the seal): for EVERY demonstrated usage, the
    /// size breathe's own grow-only carve converges to is `RightSized` or
    /// `UnderProvisioned` — NEVER `OverProvisioned`. The codomain of the carve
    /// excludes the waste arm, so over-provisioning is unrepresentable in breathe's
    /// actuation output (tier: mechanical CI forcing-function — Rust has no
    /// dependent-type quantifier — proven over a dense usage sweep; the residual
    /// over-provision is only ever an EXTERNAL over-declaration, which grow-only
    /// surfaces but cannot shrink).
    #[test]
    fn breathe_carve_never_over_provisions() {
        let c = storage_cfg();
        // A dense sweep from sub-floor to well above the floor, plus the awkward
        // just-crossed-a-grow-step usages.
        let mut usages: Vec<u64> = vec![0, MI, 100 * MI, 890 * MI, GI, 2 * GI, 3 * GI, 7 * GI, 40 * GI, 120 * GI];
        for step in 1..=64u64 {
            usages.push(step * 512 * MI);
        }
        for used in usages {
            let v = carve_output_verdict(used, &c);
            assert!(
                !v.is_over_provisioned(),
                "breathe's carve over-provisioned at used={used}: {v:?} — the carve codomain must exclude OverProvisioned",
            );
        }
    }

    /// The carve fixpoint never exceeds the ceiling and never sits below the floor —
    /// the provision-minimal bounds hold at both ends.
    #[test]
    fn carve_fixpoint_respects_floor_and_ceiling() {
        let c = storage_cfg();
        assert_eq!(carve_fixpoint(1, &c), c.floor_bytes, "tiny usage stays at the provision floor");
        assert!(carve_fixpoint(500 * GI, &c) <= c.ceiling_bytes, "huge usage is bounded by the ceiling");
    }
}

/// The typed two-target plan for ONE memory tick under the soft/hard split — the
/// OOM-impossible-by-construction shape. An efficiency shrink writes a SOFT target
/// (`memory.high`, reclaim) while the HARD ceiling (`memory.max` / the k8s limit)
/// is held at the never-OOM [`safe_min`] and NEVER lowered. Both halves funnel
/// through [`safety_clamp`] (via [`plan_dual_carve`]), so the proof is unforked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DualCarve {
    /// What to do to the HARD `memory.max` / k8s limit. A grow raises it (more
    /// headroom — safe); a shrink only ever clamps it UP to/holds it at
    /// [`safe_min`] (the kill ceiling never drops below the demonstrated peak).
    pub hard: Decision,
    /// What to do to the SOFT `memory.high` (reclaim) limit — the efficiency-carve
    /// target. `None` ⇒ the dimension has no soft plane (the soft target is not
    /// applicable, e.g. a non-memory dimension); the carve is hard-only, as before.
    pub soft: Option<Decision>,
}

/// Plan a memory tick as a SOFT/HARD split (the OOM-impossible-by-construction
/// shape). The single inner [`safety_clamp`] is reused for BOTH planes — the proof
/// is proven once, not forked:
///
/// - **Hard plane (`memory.max` / k8s limit):** the law's proposal is clamped by
///   [`safety_clamp`], but a SHRINK is additionally pinned to NEVER drop below
///   [`safe_min`] (already `safety_clamp`'s floor) — and, crucially, an efficiency
///   shrink does NOT lower the hard limit at all: the hard target only ever
///   *grows* (to cover a demonstrated peak) or *holds*. So the kill ceiling is
///   monotone-non-decreasing under efficiency pressure.
/// - **Soft plane (`memory.high`):** the efficiency carve lands here. A shrink
///   seats the soft limit at the working-set setpoint (down to [`soft_min`]); a
///   grow raises it; in-band holds. Exceeding it reclaims + throttles, never kills.
///
/// `hard_current` is the live `memory.max` (the k8s limit) and `soft_current` is
/// the live `memory.high` (or `None` when the dimension has no soft plane — then
/// the result is hard-only, byte-identical to the legacy single-limit path).
#[must_use]
pub fn plan_dual_carve<L: ControlLaw>(
    law: &L,
    working_set: u64,
    peak_working_set: u64,
    hard_current: u64,
    soft_current: Option<u64>,
    cfg: &BandConfig,
) -> DualCarve {
    // HARD plane: the kill ceiling. Grows when the demonstrated peak needs it; an
    // efficiency shrink NEVER lowers it (we suppress a hard shrink to NoSafeShrink
    // so the only way memory.max moves is UP). This is the load-bearing invariant:
    // the hard limit is governed by the peak-floor ceiling only.
    let raw_hard = decide_with(law, working_set, peak_working_set, hard_current, cfg);
    let hard = match raw_hard {
        // an efficiency (or any) shrink of the HARD limit is refused — memory.max is
        // never lowered for efficiency; it only rises to cover a peak (the grow arm)
        // or snaps down a genuinely over-ceiling limit (handled inside decide_with as
        // the hard-ceiling snap, which arrives here as a Shrink with from > ceiling).
        Decision::Shrink { from, to } if from <= cfg.ceiling_bytes => {
            // a normal efficiency shrink: hold the kill ceiling, never lower it.
            let _ = to;
            Decision::NoSafeShrink { current: from }
        }
        other => other,
    };
    // SOFT plane: the efficiency-carve target. Only present for a dimension that has
    // a memory.high lever. The soft floor is the gentler `soft_min` (reclaim is safe
    // below the peak), so the soft limit can sit tighter than the hard limit.
    let soft = soft_current.map(|sc| {
        let soft_cfg = BandConfig { floor_bytes: soft_min(working_set, cfg), ..cfg.clone() };
        // The soft plane uses the SAME law + the SAME safety_clamp, but with the
        // gentler soft floor: `safety_clamp`'s shrink floor becomes `soft_min`, so a
        // soft shrink can reclaim down toward the working set (never below the request
        // floor) without being pinned to the demonstrated-peak ceiling. A soft grow /
        // hold is unchanged. peak == working_set on the soft plane: reclaim is keyed on
        // the live working set, not the spike history (a spike throttles, not kills).
        decide_with(law, working_set, working_set, sc, &soft_cfg)
    });
    DualCarve { hard, soft }
}

/// The ROUTING decision for one k8s `MemoryBand` tick under the soft/hard split —
/// which actuator each plane goes to (`docs/OOM-VERIFICATION.md` § Part 1). It
/// answers the load-bearing routing question the controller asks every tick:
/// *"what must I write to `memory.max` (the k8s `limits.memory`, via the pod-resize
/// API) and what must I dispatch to the host-agent for `memory.high`?"* — and the
/// answer is OOM-impossible by construction because the [`hard_target`](Self::hard_target)
/// can NEVER be a value below the live `memory.max` (an efficiency shrink of the
/// kill ceiling is unrepresentable — there is no code path that produces it).
///
/// Built purely by [`plan_k8s_memory_carve`] from a [`DualCarve`]; the controller
/// applies `hard_target` via `KubeCluster`'s pod-resize (`limits.memory`) and
/// dispatches `soft_target` to the host-agent's `memory.high` writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct K8sMemoryCarve {
    /// The value the HARD `memory.max` (k8s `limits.memory`) must hold/grow to —
    /// the never-OOM kill ceiling. `None` ⇒ leave `memory.max` exactly as it is
    /// (hold). `Some(v)` is ALWAYS `≥` the live `memory.max` (only a grow to cover a
    /// demonstrated peak, or a snap down of a genuinely over-ceiling limit, is ever
    /// emitted) — an efficiency shrink of the kill ceiling has NO code path here.
    pub hard_target: Option<u64>,
    /// The value to dispatch to the host-agent for the SOFT `memory.high` (reclaim)
    /// cgroup file — the efficiency-carve target. `None` ⇒ no soft change this tick
    /// (in-band hold, or a refused soft shrink). Exceeding it reclaims + throttles,
    /// NEVER kills, so seating it tight is always OOM-safe.
    pub soft_target: Option<u64>,
}

impl K8sMemoryCarve {
    /// By construction, the routing decision can NEVER instruct the controller to
    /// LOWER the live `memory.max` (kill ceiling) for efficiency — `true` iff the
    /// hard target (when present) is `≥ live_hard`. The whole OOM-impossibility of
    /// the k8s plane funnels through this predicate: a `false` would mean an
    /// efficiency carve lowered the kill ceiling, and the planner has no branch that
    /// produces it (proven by `k8s_carve_never_lowers_the_kill_ceiling`).
    #[must_use]
    pub fn never_lowers_kill_ceiling(&self, live_hard: u64) -> bool {
        self.hard_target.map_or(true, |t| t >= live_hard)
    }
}

/// Plan ONE k8s `MemoryBand` tick as a typed actuator-routing decision — the pure
/// core of the SOFT-k8s-carve routing (`docs/OOM-VERIFICATION.md` § Part 1). Reuses
/// [`plan_dual_carve`] (so the never-OOM proof is unforked — both planes funnel
/// through the SAME [`safety_clamp`]) and projects its [`DualCarve`] onto the two
/// k8s actuators:
///
/// - **HARD (`memory.max` / k8s `limits.memory`, via pod-resize):** only a GROW or
///   an over-ceiling SNAP-DOWN is ever a target; an efficiency shrink is suppressed
///   to `None` (hold) by `plan_dual_carve`. So the kill ceiling is monotone-non-
///   decreasing under efficiency pressure — the OOM line never moves down.
/// - **SOFT (`memory.high`, dispatched to the host-agent):** the efficiency carve.
///   A shrink seats `memory.high` at the working-set setpoint; a grow raises it; an
///   in-band hold or a refused shrink is `None`.
///
/// `hard_current` is the live `memory.max` (k8s limit); `soft_current` is the live
/// pod `memory.high` (`u64::MAX` when unset — the host-agent's read maps an unset
/// cgroup file to `u64::MAX`, so the first tick snaps it down to the CRD ceiling).
#[must_use]
pub fn plan_k8s_memory_carve<L: ControlLaw>(
    law: &L,
    working_set: u64,
    peak_working_set: u64,
    hard_current: u64,
    soft_current: u64,
    cfg: &BandConfig,
) -> K8sMemoryCarve {
    let dual = plan_dual_carve(law, working_set, peak_working_set, hard_current, Some(soft_current), cfg);
    // HARD: ONLY a grow or an over-ceiling snap-down is a target. An efficiency
    // shrink arrives as NoSafeShrink (the kill ceiling is held) ⇒ None.
    let hard_target = match dual.hard {
        // `to >= from` for a grow; an over-ceiling snap-down arrives as a Shrink whose
        // `from > ceiling` (handled inside decide_with), so its `to` is the ceiling —
        // still NOT an efficiency lowering of an in-ceiling limit (those are NoSafeShrink).
        Decision::Grow { to, .. } => Some(to),
        Decision::Shrink { from, to } if from > cfg.ceiling_bytes => Some(to),
        _ => None,
    };
    // SOFT: the efficiency-carve target — a grow or a (reclaim) shrink.
    let soft_target = match dual.soft {
        Some(Decision::Grow { to, .. } | Decision::Shrink { to, .. }) => Some(to),
        _ => None,
    };
    K8sMemoryCarve { hard_target, soft_target }
}

/// A pluggable control law: the swap-in decision core of a breathe dimension.
/// The law decides DIRECTION + MAGNITUDE only; every law runs through the SAME
/// [`safety_clamp`] (floor/ceiling/safe-min), so the never-OOM + never-overshoot
/// safety is proven ONCE, not re-implemented per law. [`BandLaw`] is the default
/// and the conformance ORACLE — a new law (PID, AIMD, predictive) is property-
/// tested to never violate the invariants the gate enforces (see the tests).
///
/// `propose` is only ever called on an IN-RANGE limit (`floor ≤ limit ≤ ceiling`)
/// — [`decide_with`] handles the universal floor-seed / ceiling-snap first, which
/// also guards the law against a divide-by-zero on an unset (`0`) limit.
pub trait ControlLaw {
    fn propose(&self, working_set: u64, current_limit: u64, cfg: &BandConfig) -> Proposal;

    /// Rate-aware proposal — the feed-forward hook. `rate` is the signed rate of
    /// change of the working set in base-units per second (positive = rising), as
    /// the reconcile layer measures it across successive fresh samples (`0` when
    /// no history exists yet). The DEFAULT ignores the rate and delegates to
    /// [`propose`], so every existing law is byte-unchanged and the proven safety
    /// scaffolding is untouched; a *predictive* law (see [`PredictiveGrow`])
    /// overrides this to pre-grow for the burst the instantaneous `working_set`
    /// can't see yet. The rate-aware path is [`decide_with_rate`]; the proven
    /// default path ([`decide`]) calls [`propose`] with no rate. Prediction is a
    /// pure ADD — it can only ever raise a grow target (asymmetric), and the same
    /// [`safety_clamp`] still contains it (the never-OOM/never-overshoot proof
    /// holds for predictive laws too — see `safety_gate_contains_any_law`).
    fn propose_with_rate(&self, working_set: u64, current_limit: u64, cfg: &BandConfig, _rate: i64) -> Proposal {
        self.propose(working_set, current_limit, cfg)
    }
}

/// The SHARED safety gate: turn any law's raw proposal into a SAFE typed
/// [`Decision`]. A grow is clamped to the ceiling (→ `AtCeiling` if no room); a
/// shrink is clamped UP to the **never-OOM floor** — so live pages can never be
/// pushed over the band and a shrink never overshoots into grow territory
/// (→ `NoSafeShrink` if no safe room). Every control law funnels through here;
/// the proof holds for all of them.
///
/// # The never-OOM floor (peak-over-window, not instantaneous)
///
/// The floor a shrink can never breach is
/// `safe_min = max( ceil(peak_working_set / setpoint), request_floor_bytes, floor_bytes )`:
///
/// - **`peak_working_set`** — the MAX working set the workload has demonstrated
///   over a trailing window, NOT the instantaneous sample. This is the bug fix:
///   a single low-water sample (e.g. a Celery worker idle between blueprint
///   reconciliations) understates the real spiky working set, so a floor keyed on
///   the instantaneous reading carves the limit BELOW a known recent peak and the
///   next spike OOMKills. Keying on the demonstrated peak makes "shrink below what
///   the workload has been observed to need" structurally impossible.
/// - **`request_floor_bytes`** — the operator's declared `requests.<resource>`
///   guarantee; the limit can never drop under the guaranteed working set.
/// - **`floor_bytes`** — the band's hard configured floor.
///
/// # Honest tier (per `theory/UNREPRESENTABILITY.md`)
///
/// This is the **C2 "external-world observation" ceiling**: a FUTURE working set
/// is not knowable at compile time, so the strongest HONEST guarantee is
/// *"never below the DEMONSTRATED peak + the declared request floor"*, enforced
/// structurally because EVERY control law funnels through this one
/// `safety_clamp`. It is not (and cannot be) a compile-time proof that a workload
/// will never OOM — only that a carve can never lower the limit beneath what the
/// workload has already shown it needs in the trailing window. Callers raise the
/// floor by feeding a peak that holds a real spike for a meaningful window (see
/// [`update_peak`]).
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn safety_clamp(
    proposal: Proposal,
    working_set: u64,
    peak_working_set: u64,
    current_limit: u64,
    cfg: &BandConfig,
) -> Decision {
    match proposal {
        Proposal::Hold => Decision::Hold,
        Proposal::Target(raw) if raw > current_limit => {
            let to = raw.min(cfg.ceiling_bytes);
            if to <= current_limit {
                Decision::AtCeiling { current: current_limit }
            } else {
                Decision::Grow { from: current_limit, to }
            }
        }
        Proposal::Target(raw) if raw < current_limit => {
            // The never-OOM floor is keyed on the DEMONSTRATED PEAK working set
            // (max over the trailing window), never the instantaneous sample — a
            // low-water reading must not let a shrink carve under a recent spike.
            // `peak_working_set` is always ≥ the current `working_set` (the caller
            // folds the current sample into the peak), so this is monotone-safer:
            // it can only ever RAISE the safe minimum vs the old instantaneous form.
            // SINGLE SOURCE OF TRUTH: the hard floor lives in `safe_min` (consumed
            // by the soft/hard dual-carve planner too) so both planes prove never-OOM
            // through ONE computation.
            let safe_min = safe_min(peak_working_set, working_set, cfg);
            let to = raw.max(safe_min);
            if to >= current_limit {
                Decision::NoSafeShrink { current: current_limit }
            } else {
                Decision::Shrink { from: current_limit, to }
            }
        }
        Proposal::Target(_) => Decision::Hold, // raw == current_limit
    }
}

/// Fold one fresh working-set sample into the trailing-window PEAK that drives the
/// never-OOM shrink floor. The peak is an **EWMA-peak with slow decay**: a real
/// spike instantly raises it (`max` with the current sample), and it decays
/// geometrically by `decay ∈ [0,1)` per tick so a one-off spike still holds the
/// floor up for a meaningful window rather than evaporating on the very next
/// low-water sample. `decay = 0.0` ⇒ a pure single-tick max (no memory beyond the
/// current sample); a `decay` near 1 ⇒ the peak holds for many ticks. Pure +
/// dependency-free + testable; the reconcile layer persists the result in the
/// band status and feeds it back next tick.
///
/// Invariant: the returned peak is ALWAYS `≥ current` — folding the sample in can
/// never produce a value below the sample itself, so the shrink floor it feeds
/// can never drop under the live working set.
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn update_peak(prior_peak: u64, current: u64, decay: f64) -> u64 {
    let decay = decay.clamp(0.0, 0.999);
    let decayed = ((prior_peak as f64) * decay) as u64;
    decayed.max(current)
}

/// Run a control law through the universal safety scaffolding: floor-seed /
/// ceiling-snap (independent of the law; also the unset-limit guard) → the law's
/// proposal → [`safety_clamp`]. This is the one place a law's output becomes a
/// safe [`Decision`]. `peak_working_set` is the trailing-window peak the shrink
/// floor is keyed on (see [`safety_clamp`] / [`update_peak`]); pass `working_set`
/// for the no-history / instantaneous-only behaviour.
#[must_use]
pub fn decide_with<L: ControlLaw>(
    law: &L,
    working_set: u64,
    peak_working_set: u64,
    current_limit: u64,
    cfg: &BandConfig,
) -> Decision {
    // Hard-floor SEED/SNAP: an unset (0) or below-floor limit is grown straight
    // to the floor — independent of utilization, and the guard that keeps the
    // law from dividing by a zero limit. Lets breathe take over a freshly-ceded
    // field (CNPG/Flux relinquishes limits.memory → unset → seed to floor).
    if current_limit < cfg.floor_bytes {
        return Decision::Grow { from: current_limit, to: cfg.floor_bytes };
    }
    // Hard-ceiling SNAP: a limit above the ceiling is brought down to it (the
    // directionality clamp turns this into NoSafeShrink for grow-only dims).
    if current_limit > cfg.ceiling_bytes {
        return Decision::Shrink { from: current_limit, to: cfg.ceiling_bytes };
    }
    if let Some(d) = metric_untrusted_decision(working_set, peak_working_set, current_limit, cfg) {
        return d;
    }
    safety_clamp(law.propose(working_set, current_limit, cfg), working_set, peak_working_set, current_limit, cfg)
}

/// The METRIC-TRUST GATE (the split-brain fail-safe). A `working_set == 0`
/// reading from a running workload is almost always a broken/missing/lagging
/// metric, not a real "needs nothing" — trusting it carves to floor and
/// OOM-kills the pod when its true RSS returns (the fleet-wide split-brain where
/// a degraded metric pipeline reports `used=0` for many pods). The
/// non-negotiable invariant: an untrusted reading may NEVER drive a downward
/// carve. Returns `Some(safe_decision)` when the reading is untrusted (per the
/// configured [`MetricMissingPolicy`]), `None` when the reading is trusted and
/// the normal band law should run. A grow into this gate is impossible — a 0
/// reading can only ever Hold or restore headroom UP to the durable peak floor.
#[must_use]
pub fn metric_untrusted_decision(
    working_set: u64,
    peak_working_set: u64,
    current_limit: u64,
    cfg: &BandConfig,
) -> Option<Decision> {
    if working_set != 0 {
        return None; // trusted reading — run the law
    }
    match cfg.metric_missing_policy {
        // The dimension treats 0 as a real value (node-count scaled-to-zero, etc.)
        // — never gate; run the law as if the reading were trusted.
        MetricMissingPolicy::Trust => None,
        // Never shrink; if a prior carve (or a risen floor) left the limit below
        // the durable demonstrated-peak safe floor, grow back up to it.
        MetricMissingPolicy::RestoreHeadroom => {
            let safe = safe_min(peak_working_set, 0, cfg);
            if current_limit < safe {
                Some(Decision::Grow { from: current_limit, to: safe })
            } else {
                Some(Decision::Hold)
            }
        }
        // Freeze the limit exactly until a trusted reading returns.
        MetricMissingPolicy::Hold => Some(Decision::Hold),
    }
}

/// Rate-aware sibling of [`decide_with`]: runs a law's *feed-forward*
/// ([`ControlLaw::propose_with_rate`]) through the identical floor-seed /
/// ceiling-snap / [`safety_clamp`] scaffolding. The only difference from
/// [`decide_with`] is that the law sees the working-set `rate` (base-units/sec,
/// signed) — so a predictive law grows AHEAD of a rising burst. The safety gate
/// is the same, so the never-OOM/never-overshoot proof is unchanged. With
/// `rate == 0` this is identical to [`decide_with`] for every law.
#[must_use]
pub fn decide_with_rate<L: ControlLaw>(
    law: &L,
    working_set: u64,
    peak_working_set: u64,
    current_limit: u64,
    cfg: &BandConfig,
    rate: i64,
) -> Decision {
    if current_limit < cfg.floor_bytes {
        return Decision::Grow { from: current_limit, to: cfg.floor_bytes };
    }
    if current_limit > cfg.ceiling_bytes {
        return Decision::Shrink { from: current_limit, to: cfg.ceiling_bytes };
    }
    if let Some(d) = metric_untrusted_decision(working_set, peak_working_set, current_limit, cfg) {
        return d;
    }
    safety_clamp(
        law.propose_with_rate(working_set, current_limit, cfg, rate),
        working_set,
        peak_working_set,
        current_limit,
        cfg,
    )
}

/// The default control law + the conformance oracle: a deadband with gentle
/// multiplicative steps. Utilization above `grow_above` proposes a `grow_factor`
/// step; below `shrink_below` a `shrink_factor` step (the gate clamps it to the
/// safe minimum); in-band holds.
#[derive(Debug, Clone, Copy, Default)]
pub struct BandLaw;

impl ControlLaw for BandLaw {
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn propose(&self, working_set: u64, current_limit: u64, cfg: &BandConfig) -> Proposal {
        let util = working_set as f64 / current_limit as f64;
        if util > cfg.grow_above {
            Proposal::Target((current_limit as f64 * cfg.grow_factor).ceil() as u64)
        } else if util < cfg.shrink_below {
            // gentle step; safety_clamp lifts it to the safe minimum if needed
            Proposal::Target((current_limit as f64 * cfg.shrink_factor).floor() as u64)
        } else {
            Proposal::Hold
        }
    }
}

/// The bidirectional band law as a free function — `decide_with(&BandLaw, …)`.
/// Behaviour-preserving wrapper kept for the existing call sites: the proven
/// default. Shrink can never push a workload toward OOM by construction (the gate
/// clamps to the never-OOM floor — see [`safety_clamp`]). This instantaneous-only
/// form feeds `peak = working_set` (no trailing-window history); the rate-/peak-
/// aware path used by the reconcile loop is [`decide_with`] / [`decide_with_rate`]
/// fed a real [`update_peak`] value.
#[must_use]
pub fn decide(working_set: u64, current_limit: u64, cfg: &BandConfig) -> Decision {
    decide_with(&BandLaw, working_set, working_set, current_limit, cfg)
}

/// A PROPORTIONAL control law: the step size is proportional to the % deviance
/// from the setpoint (vs `BandLaw`'s fixed multiplicative factor). It aims at the
/// limit that would land utilization exactly at the setpoint (`working_set /
/// setpoint`) and moves `gain ∈ (0,1]` of the way there — `gain = 1.0` corrects
/// in one tick, `< 1.0` damps the move to reduce overshoot/oscillation (the
/// control-theoretic P-controller with a damping term). Outside the deadband
/// only; the shared safety gate still clamps every result. This is the
/// deviance-keyed graded response the band law approximates with a step.
#[derive(Debug, Clone, Copy)]
pub struct ProportionalLaw {
    /// Fraction of the gap to the setpoint-landing limit to traverse per tick.
    pub gain: f64,
}

impl Default for ProportionalLaw {
    fn default() -> Self {
        Self { gain: 0.7 }
    }
}

impl ControlLaw for ProportionalLaw {
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn propose(&self, working_set: u64, current_limit: u64, cfg: &BandConfig) -> Proposal {
        let util = working_set as f64 / current_limit as f64;
        if util > cfg.grow_above || util < cfg.shrink_below {
            let ideal = working_set as f64 / cfg.setpoint; // lands util at the setpoint
            let target = (current_limit as f64) + (ideal - current_limit as f64) * self.gain;
            Proposal::Target(target.round().max(0.0) as u64)
        } else {
            Proposal::Hold
        }
    }
}

/// A decorator that wraps ANY control law to cap the per-tick change to
/// `max_step_frac` of the current limit — a slew-rate limit that bounds jitter
/// and prevents an aggressive inner law from making a huge single jump
/// (control-theory anti-oscillation / the universal jitter damper). Composes:
/// `SlewLimited { inner: ProportionalLaw { gain: 1.0 }, max_step_frac: 0.25 }`.
#[derive(Debug, Clone, Copy)]
pub struct SlewLimited<L> {
    pub inner: L,
    pub max_step_frac: f64,
}

impl<L: ControlLaw> ControlLaw for SlewLimited<L> {
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn propose(&self, working_set: u64, current_limit: u64, cfg: &BandConfig) -> Proposal {
        match self.inner.propose(working_set, current_limit, cfg) {
            Proposal::Hold => Proposal::Hold,
            Proposal::Target(t) => {
                let max_delta = ((current_limit as f64) * self.max_step_frac).max(1.0);
                let lo = ((current_limit as f64) - max_delta).max(0.0) as u64;
                let hi = ((current_limit as f64) + max_delta) as u64;
                Proposal::Target(t.clamp(lo, hi))
            }
        }
    }
}

/// The ASYMMETRIC feed-forward decorator — the burst-OOM fix. Wraps any inner
/// law and adds a *predictive grow*: it projects the working set `lookahead_secs`
/// into the future at the observed `rate` (`ws + rate·lookahead`) and, if that
/// near-future working set would breach `grow_above`, grows NOW to seat the
/// *predicted* working set at the setpoint — even if the instantaneous
/// utilization is still in-band. The asymmetry is the whole point: the loop's
/// only fast, prediction-driven action is the one that BUYS headroom (grow), so
/// dead-time can only ever cost money, never the process. When the prediction is
/// benign it defers entirely to the inner law (including its shrink) — prediction
/// never shrinks and never overrides a shrink. With `rate == 0` (no history) the
/// prediction collapses to the present and the behaviour is exactly the inner
/// law's. The shared [`safety_clamp`] still caps the grow at the ceiling, so a
/// runaway rate cannot breach it (proven in `safety_gate_contains_any_law`).
///
/// Closes the L0-liveness category error from the breathability thesis:
/// averaging is fatal for OOM (a pointwise cliff); the fix is not a better
/// average but a one-sided predictive grow that pre-empts the cliff.
#[derive(Debug, Clone, Copy)]
pub struct PredictiveGrow<L> {
    /// The base control law (band/proportional/slew-limited).
    pub inner: L,
    /// How many seconds ahead to project the working set at the observed rate.
    /// Size it to the provision latency `θ` (refresh + cooldown) the grow must
    /// cover before the loop sees the next sample.
    pub lookahead_secs: f64,
}

impl<L: ControlLaw> ControlLaw for PredictiveGrow<L> {
    /// Rate-blind fallback: with no rate signal there is nothing to predict, so a
    /// predictive law is exactly its inner law (the proven default path uses this).
    fn propose(&self, working_set: u64, current_limit: u64, cfg: &BandConfig) -> Proposal {
        self.inner.propose(working_set, current_limit, cfg)
    }

    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn propose_with_rate(&self, working_set: u64, current_limit: u64, cfg: &BandConfig, rate: i64) -> Proposal {
        let inner = self.inner.propose_with_rate(working_set, current_limit, cfg, rate);
        // project the working set forward at the observed rate (never below 0).
        let predicted_ws = ((working_set as f64) + (rate as f64) * self.lookahead_secs).max(0.0);
        let predicted_util = predicted_ws / current_limit as f64;
        // ASYMMETRIC: only a PREDICTED breach of the grow edge triggers a
        // feed-forward grow; otherwise defer to the inner law verbatim (so its
        // shrink/hold are untouched — prediction never shrinks).
        if predicted_util > cfg.grow_above {
            let ff_target = (predicted_ws / cfg.setpoint).ceil() as u64;
            let base = match inner {
                Proposal::Target(t) => t,
                Proposal::Hold => current_limit,
            };
            // grow to whichever is larger — the inner law's grow or the
            // feed-forward seat — never smaller, never a shrink.
            return Proposal::Target(base.max(ff_target).max(current_limit));
        }
        inner
    }
}

/// **QuantizedSlice** — the discrete-geometry law (census hazard class
/// `count-based` / `discrete`). Wraps any inner law and SNAPS its continuous
/// target to a legal multiple of `slice`, rounding in the SAFE direction:
/// **always up**. Rounding a grow up over-provisions slightly (safe for
/// never-starve); rounding a shrink up makes it less aggressive (safe — the
/// gate turns an over-rounded shrink into `NoSafeShrink`). One law serves every
/// quantized count vector — pids/TasksMax (slice 1), cpuset cores, MIG profiles,
/// Kafka partitions, worker pools — by varying `slice`. Funnels through
/// `safety_clamp` unchanged, so it inherits never-starve/never-overshoot.
#[derive(Debug, Clone, Copy)]
pub struct QuantizedSlice<L> {
    pub inner: L,
    /// The discrete quantum the target snaps to (≥ 1; `1` = every integer legal).
    pub slice: u64,
}

impl<L: ControlLaw> ControlLaw for QuantizedSlice<L> {
    fn propose(&self, working_set: u64, current_limit: u64, cfg: &BandConfig) -> Proposal {
        snap_up(self.inner.propose(working_set, current_limit, cfg), self.slice)
    }
    fn propose_with_rate(&self, working_set: u64, current_limit: u64, cfg: &BandConfig, rate: i64) -> Proposal {
        snap_up(self.inner.propose_with_rate(working_set, current_limit, cfg, rate), self.slice)
    }
}

/// Round a proposal's target UP to the nearest multiple of `slice` (the safe
/// direction for both grow and shrink). `slice <= 1` is a no-op.
#[must_use]
fn snap_up(p: Proposal, slice: u64) -> Proposal {
    match p {
        Proposal::Hold => Proposal::Hold,
        Proposal::Target(t) if slice <= 1 => Proposal::Target(t),
        Proposal::Target(t) => {
            let snapped = t.div_ceil(slice).saturating_mul(slice);
            Proposal::Target(snapped)
        }
    }
}

/// **ThrottleAware** — the rate-shaped / soft-hazard anti-flap law (census
/// hazard class `cpu-soft` + `rate-shaped`; ~32 vectors). A soft cap (CPU quota,
/// io.max, network bandwidth, a rate limiter) signals saturation by THROTTLING,
/// not by a level — so a band keyed on the level alone will shrink the cap right
/// as the workload is being throttled, then grow it back: flap. ThrottleAware
/// wraps any inner level-law and reads a *throttle signal* through the
/// feed-forward `rate` channel (throttle events/sec, PSI stall %, dropped/sec —
/// whatever the descriptor's metric measures): **while the signal is positive
/// (live throttling), a shrink is suppressed to a Hold** (never tighten a cap
/// that is actively throttling); a grow or hold from the inner law passes
/// through. With no throttle signal it is exactly the inner law. The never-OOM
/// `safety_clamp` `ceil(working_set/setpoint)` carries over VERBATIM as
/// never-throttle-live (the offered-load rate seated at the setpoint), so this
/// law inherits the whole safety proof — it only ever makes a shrink LESS
/// aggressive, never escapes the envelope.
#[derive(Debug, Clone, Copy)]
pub struct ThrottleAware<L> {
    pub inner: L,
}

impl<L: ControlLaw> ControlLaw for ThrottleAware<L> {
    /// No throttle signal available → exactly the inner level-law.
    fn propose(&self, working_set: u64, current_limit: u64, cfg: &BandConfig) -> Proposal {
        self.inner.propose(working_set, current_limit, cfg)
    }

    /// `rate` here is the THROTTLE signal (≥ 0 = stalls/throttle-events per sec).
    /// While throttling, suppress any shrink to a Hold (anti-flap); otherwise the
    /// inner law verbatim. A grow is never suppressed — relieving throttle is the
    /// safe move.
    fn propose_with_rate(&self, working_set: u64, current_limit: u64, cfg: &BandConfig, rate: i64) -> Proposal {
        let inner = self.inner.propose(working_set, current_limit, cfg);
        if rate > 0 {
            match inner {
                // Actively throttling: do NOT tighten the cap. A proposed shrink
                // becomes a Hold; grow/hold pass through.
                Proposal::Target(t) if t < current_limit => Proposal::Hold,
                other => other,
            }
        } else {
            inner
        }
    }
}

/// **PercentileBand** — size the limit DIRECTLY to the setpoint-seat for the
/// observed working set, when outside the deadband (census: requests, retention
/// disk%, cache hit-rate, LimitRange — anywhere the metric is already a high
/// percentile / efficiency signal, so reacting to it once is right). Distinct
/// from `BandLaw`'s multiplicative deadband steps: it lands util AT the setpoint
/// in one move (like `ProportionalLaw` gain 1.0, but only outside the deadband).
/// Funnels through `safety_clamp`, so it inherits the whole safety proof.
#[derive(Debug, Clone, Copy, Default)]
pub struct PercentileBand;

impl ControlLaw for PercentileBand {
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn propose(&self, working_set: u64, current_limit: u64, cfg: &BandConfig) -> Proposal {
        let util = working_set as f64 / current_limit as f64;
        if util > cfg.grow_above || util < cfg.shrink_below {
            let setpoint = if cfg.setpoint <= 0.0 { 1.0 } else { cfg.setpoint };
            Proposal::Target((working_set as f64 / setpoint).ceil() as u64)
        } else {
            Proposal::Hold
        }
    }
}

/// **AIMD** — additive-increase / multiplicative-decrease, the TCP-congestion
/// shape for RATE LIMITERS (samba quotaPct, adaptive concurrency). With no
/// throttle signal it probes UP by a fixed `increment` (gently discover the
/// ceiling); on a positive throttle signal (the `rate` channel) it backs OFF
/// multiplicatively by `decrease_factor`. The proven gate still clamps both
/// directions, so AIMD can never breach the ceiling or shrink below the safe min.
#[derive(Debug, Clone, Copy)]
pub struct Aimd {
    /// Additive-increase step (base units) applied each clear tick.
    pub increment: u64,
    /// Multiplicative-decrease factor (0,1) applied on a throttle tick (e.g. 0.5).
    pub decrease_factor: f64,
}

impl ControlLaw for Aimd {
    fn propose(&self, _working_set: u64, current_limit: u64, _cfg: &BandConfig) -> Proposal {
        // no throttle info → additive increase (probe up).
        Proposal::Target(current_limit.saturating_add(self.increment))
    }
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn propose_with_rate(&self, _working_set: u64, current_limit: u64, _cfg: &BandConfig, rate: i64) -> Proposal {
        if rate > 0 {
            // throttle / loss detected → multiplicative back-off.
            Proposal::Target((current_limit as f64 * self.decrease_factor) as u64)
        } else {
            Proposal::Target(current_limit.saturating_add(self.increment))
        }
    }
}

/// **BurstBudget** — token-bucket depth sizing (CFS `cpu.max.burst`). GROW-BIASED:
/// it sizes UP readily to absorb the observed burst, but gives depth back only
/// HALF as fast as a normal band (a too-small burst budget throttles a latency-
/// sensitive workload, so reluctance to shrink is the safe bias). Funnels through
/// the gate like every law.
#[derive(Debug, Clone, Copy, Default)]
pub struct BurstBudget;

impl ControlLaw for BurstBudget {
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn propose(&self, working_set: u64, current_limit: u64, cfg: &BandConfig) -> Proposal {
        let util = working_set as f64 / current_limit as f64;
        if util > cfg.grow_above {
            let setpoint = if cfg.setpoint <= 0.0 { 1.0 } else { cfg.setpoint };
            Proposal::Target((working_set as f64 / setpoint).ceil() as u64)
        } else if util < cfg.shrink_below {
            // grow-bias: shrink at HALF the configured aggression.
            let gentle = 1.0 - (1.0 - cfg.shrink_factor) / 2.0;
            Proposal::Target((current_limit as f64 * gentle).floor() as u64)
        } else {
            Proposal::Hold
        }
    }
}

/// **SharedBudget** — fair-share under a SUMMED ceiling (NATS account-sum ≤
/// server-sum, Kafka tenant quotas, app-pool-sum ≤ DB max_connections). This is
/// breathe's L2 `nodeBudget` never-swap sum lifted to a logical resource pool:
/// wrap any inner law and CLAMP a grow so this member's new limit can't exceed
/// `current + available_headroom`, where `available_headroom = budget − Σsiblings`
/// is computed by the caller each tick. A shrink/hold passes through (giving back
/// is always safe). The gate still owns floor/ceiling/safe-min.
#[derive(Debug, Clone, Copy)]
pub struct SharedBudget<L> {
    pub inner: L,
    /// Budget remaining for THIS member's grow (`budget − Σ other members`), this tick.
    pub available_headroom: u64,
}

impl<L: ControlLaw> ControlLaw for SharedBudget<L> {
    fn propose(&self, working_set: u64, current_limit: u64, cfg: &BandConfig) -> Proposal {
        clamp_grow_to_headroom(self.inner.propose(working_set, current_limit, cfg), current_limit, self.available_headroom)
    }
    fn propose_with_rate(&self, working_set: u64, current_limit: u64, cfg: &BandConfig, rate: i64) -> Proposal {
        clamp_grow_to_headroom(
            self.inner.propose_with_rate(working_set, current_limit, cfg, rate),
            current_limit,
            self.available_headroom,
        )
    }
}

/// Clamp a grow proposal to `current + available_headroom`; pass shrink/hold through.
#[must_use]
fn clamp_grow_to_headroom(p: Proposal, current_limit: u64, available_headroom: u64) -> Proposal {
    match p {
        Proposal::Target(t) if t > current_limit => {
            Proposal::Target(t.min(current_limit.saturating_add(available_headroom)))
        }
        other => other,
    }
}

/// **BurnRateBand** (the COST class — census's only genuinely-new hazard). A cost
/// budget is a monotone-accumulating integral that resets per billing window, not
/// a `(used, capacity)` ratio — so it is not CARVED, it VETOES other carves' grows.
/// This pure forecast is the veto kernel: does the current burn rate breach the
/// budget before the window closes? The Forma/`Otimizador` layer (Step-14) consumes
/// it as a Meet-dependency edge into every grow. Pure + dependency-free + testable.
#[must_use]
pub fn burn_rate_breaches(spent_cents: u64, budget_cents: u64, burn_rate_cents_per_sec: f64, secs_remaining: u64) -> bool {
    #[allow(clippy::cast_precision_loss)]
    let forecast = spent_cents as f64 + burn_rate_cents_per_sec.max(0.0) * secs_remaining as f64;
    #[allow(clippy::cast_precision_loss)]
    let breaches = forecast > budget_cents as f64;
    breaches
}

/// The memory dimension's owned field path (the first consumer's constant).
/// Every provider declares the dotted `managedFields` path it owns; the guard
/// compares against *this exact path*, never a per-object flag.
pub const MEMORY_LIMIT_FIELD: &str = "resources.limits.memory";

/// One field-manager's claim on a *specific object field*, distilled from
/// `metadata.managedFields`. Field-granular by construction (review finding):
/// a flat per-object bool cannot tell a memory writer (`resources.limits.memory`)
/// apart from a replica writer (`spec.replicas`), so it cannot back the
/// disjoint-field composition contract. The dotted `field` path makes the
/// distinction provable by string equality.
#[derive(Debug, Clone)]
pub struct FieldOwner {
    pub manager: String,
    pub field: String,
}

/// The single-writer invariant, field-granular. Returns a *competing* manager
/// that owns the SAME `field` we intend to write, so the caller yields instead
/// of fighting. A manager owning a *different* field (KEDA on `spec.replicas`,
/// say) is not a competitor — this is the entire disjoint-field composition
/// contract (breathe ⟂ KEDA, memory ⟂ cpu), enforced by equality on the path.
/// Deterministic, fail-loud, never two writers oscillating one field.
#[must_use]
pub fn competing_field_manager(
    owners: &[FieldOwner],
    our_manager: &str,
    field: &str,
) -> Option<String> {
    owners
        .iter()
        .find(|o| o.field == field && o.manager != our_manager)
        .map(|o| o.manager.clone())
}

/// Memory-specialized alias retained for the existing memory call sites.
#[must_use]
pub fn competing_memory_manager(owners: &[FieldOwner], our_manager: &str) -> Option<String> {
    competing_field_manager(owners, our_manager, MEMORY_LIMIT_FIELD)
}

/// What a resident problem category may do. `GrowOnly` (storage) never shrinks
/// (data persists; online-resize is irreversible); `Bidirectional` (memory, cpu)
/// breathes both ways; `ObserveOnly` (KEDA-owned replicas) never mutates the
/// field at all. The loop enforces this — providers never carry band logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Directionality {
    Bidirectional,
    GrowOnly,
    ObserveOnly,
}

/// The dimension-agnostic projection a provider's `observe` yields: every
/// resident problem category reduces to `(used, capacity)` in its base unit
/// (bytes / bytes / milli-cores) plus the field-managers currently owning the
/// field and the age of the driving sample. Once a category projects to this
/// struct, the proven [`decide`] runs unchanged — the whole "dimension-agnostic
/// core" claim is made here. `staleness_secs` is load-bearing for safety: the
/// never-OOM proof holds only on a *fresh* sample (review finding), so a stale
/// read must never drive a mutation.
#[derive(Debug, Clone)]
pub struct Observation {
    pub used: u64,
    /// The PEAK working set demonstrated over a trailing window — the max RSS the
    /// reconcile layer has recently observed for this entity (an EWMA-peak with
    /// slow decay; see [`update_peak`]). This, NOT the instantaneous `used`, is
    /// what the shrink-safety floor in [`safety_clamp`] is keyed on: a low-water
    /// `used` sample must never let a carve drop the limit beneath a recent spike
    /// (the authentik-Celery-worker OOM). The reconcile layer carries it across
    /// ticks (persisted in the band status) and folds the current `used` in before
    /// each tick, so `peak_used ≥ used` always holds. On the first tick (no
    /// history) it equals `used` — behaviour-identical to the instantaneous form.
    pub peak_used: u64,
    pub capacity: u64,
    pub owners: Vec<FieldOwner>,
    /// Age of the metric sample driving `used`, in seconds. A scrape gap that
    /// returns a stale/zero `used` is indistinguishable from a real reading
    /// without this — so the loop refuses to mutate when it exceeds the bound.
    pub staleness_secs: u64,
    /// How many seconds the workload has been OBSERVED since its last (re)start —
    /// the warmup-gate input. The reconcile layer computes it from the target's pod
    /// `status.startTime` (or the band's first-observed epoch as a fallback). A
    /// workload observed for fewer than the band's `warmup_seconds` is still warming
    /// up, so a shrink is HELD ([`clamp_to_warmup`]) — its idle reading is not yet
    /// proof the slack is safe to reclaim (the un-observed-boot-spike OOM). `u64::MAX`
    /// = "no restart history / warmup not applicable" ⇒ always past warmup (the
    /// behaviour-preserving default for dimensions with no restart concept, e.g. host
    /// cgroup/ARC, and the first-tick fallback before a start time is known).
    pub observed_for_secs: u64,
    /// Restart-cost refinement for a memory in-place SHRINK: `true` iff the target
    /// is a pod whose `resizePolicy[<resource>]` is `NotRequired`, so the kubelet
    /// resizes it without restarting the container. Only meaningful for a memory
    /// `PodResize` carve; `false` (conservative — assume a shrink may restart)
    /// everywhere else, and never consulted unless the carve's base restart class
    /// is `RestartConditional`. This is what lets a `NotRequired` workload breathe
    /// DOWN on golden rails (Phase 2 of RIO-GOLDEN-UPDATE).
    pub memory_shrink_restart_free: bool,
    /// The target's LIVE declared `resources.requests.<resource>` (max across the pod
    /// group), in the band's base unit — the inviolable request floor a shrink can
    /// never carve beneath (requests is the scheduler's guaranteed working set; a
    /// limit below the request is invalid in k8s AND unsafe). The reconcile layer
    /// folds `max(cfg.request_floor_bytes, this)` into the effective `BandConfig`, so
    /// the DECLARED request is honored even when the operator omitted `requestFloor`
    /// from the band CR. `0` = no request declared / not applicable (host/node).
    pub request_floor: u64,
    /// The SUPPRESSED-DEMAND signal — the load-bearing fix for the CPU-blindness
    /// ratchet (the pangea-operator 2026-06 starve). For a hard-capped soft resource
    /// (CPU under a CFS quota), the observed `used` can NEVER exceed `capacity`: the
    /// cgroup throttles the workload at the limit, so metrics-server only ever sees
    /// usage ≤ limit. Each shrink lowers usage, which "justifies" the next shrink — a
    /// ratchet to the floor that starves the workload. The throttle is the demand the
    /// usage metric structurally cannot show: `> 0` means the resource is being
    /// throttled NOW (the descriptor reports CFS throttled-periods-per-second, a PSI
    /// stall %, or a throttle-ratio-derived value). When present, the reconcile layer
    /// (a) lifts the band-law inputs to a demand ABOVE the current limit (so the
    /// proven `safe_min` peak-floor refuses any shrink and the law grows) AND (b) the
    /// `plan_tick` throttle gate holds a shrink as a typed [`Decision::Throttled`] —
    /// belt-and-suspenders. `0` = no throttle / not applicable (memory, storage,
    /// observe-only). UNIT-AGNOSTIC: a presence/magnitude, never compared to
    /// `used`/`capacity`. See [`throttled_demand`].
    pub throttle_signal: u64,
    /// `true` iff the target recently (re)started or is crash-looping (an observed
    /// restart inside the warmup window, or a non-zero restart-count delta). Like a
    /// live throttle, a crash-loop means the current low `used` is a symptom, not
    /// proof of safe slack — so a shrink is HELD ([`Decision::Throttled`] with
    /// `restarting: true`). `false` = stable / not applicable. The reconcile layer
    /// sets it from the target's pod restart status.
    pub restarting: bool,
}

/// The WARMUP GATE: hold a SHRINK while the workload is still warming up. A
/// workload observed for fewer than `warmup_seconds` since its last (re)start has
/// not demonstrated a full duty cycle, so its low utilization is not yet proof the
/// slack is safe to reclaim — a shrink becomes a typed [`Decision::Warmup`] hold.
/// A GROW (and every non-shrink outcome) passes through untouched: buying headroom
/// is always safe, and refusing to grow during warmup would itself risk an OOM.
/// `warmup_seconds == 0` disables the gate (behaviour-preserving). Pure + testable.
#[must_use]
pub fn clamp_to_warmup(d: Decision, observed_for_secs: u64, warmup_seconds: u64) -> Decision {
    if warmup_seconds == 0 || observed_for_secs >= warmup_seconds {
        return d;
    }
    match d {
        Decision::Shrink { from, .. } => Decision::Warmup {
            current: from,
            observed_for: observed_for_secs,
            warmup: warmup_seconds,
        },
        other => other,
    }
}

/// THE NON-BLIND CPU INPUT — compute the demand a THROTTLED workload actually has,
/// which its (CFS-capped) usage metric structurally cannot reveal. When the
/// suppressed-demand signal is present (`throttle_signal > 0` and/or `restarting`),
/// the workload wanted MORE than its current limit — the throttle proves it. Report
/// a demand ABOVE the limit so the proven band law grows and, crucially, the proven
/// peak-keyed [`safe_min`] floor REFUSES any shrink (the carve can never drop the
/// limit below a value ≥ the limit). This is how the fix flows through the EXISTING
/// safety gate with ZERO change to `decide`/`safety_clamp`/`safe_min`: it only ever
/// RAISES the band law's `used`/`peak` inputs, exactly as `update_peak` does for a
/// memory spike. The lift is `ceil(current_limit * grow_factor)` — one grow step
/// above the cap, the same magnitude a normal grow uses — so a throttled workload
/// climbs out of the throttle at the band's own gentle rate. `None` ⇒ no throttle
/// signal (the demand is just the observed `used`, byte-identical to before).
/// Pure + dependency-free + testable.
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn throttled_demand(used: u64, current_limit: u64, throttle_signal: u64, restarting: bool, cfg: &BandConfig) -> Option<u64> {
    if throttle_signal == 0 && !restarting {
        return None;
    }
    // The throttle proves demand > limit; seat the demand one grow-step above the cap
    // (never below the observed `used`, which under throttle is ≈ the cap). A
    // restart-only signal (no live throttle) still lifts to just over the cap so the
    // peak-floor refuses a shrink while the workload re-establishes its duty cycle.
    let one_step = ((current_limit as f64) * cfg.grow_factor).ceil() as u64;
    Some(one_step.max(current_limit.saturating_add(1)).max(used))
}

/// THE NO-STARVE GATE: hold a SHRINK while the workload's suppressed demand is
/// non-blind — it is being actively THROTTLED (`throttle_signal > 0`) or recently
/// (re)started / crash-looping (`restarting`). This is the explicit, NAMED runtime
/// invariant the operator asked for: `throttled || restarting ⇒ proposal ∈ {Grow,
/// Hold}, never Shrink`. It is BELT-AND-SUSPENDERS over [`throttled_demand`] (which
/// already lifts the band-law inputs so the proven `safe_min` floor refuses the
/// shrink): even if the demand-lift somehow did not bind, this gate converts any
/// surviving shrink to a typed [`Decision::Throttled`] hold. A GROW (and every
/// non-shrink outcome) passes through untouched — relieving a throttle by buying
/// headroom is always the safe direction. No signal ⇒ no-op (behaviour-preserving).
/// Pure + testable. Mirrors [`clamp_to_warmup`]; runs in `plan_tick` AFTER
/// directionality and warmup, BEFORE freshness/cooldown.
#[must_use]
pub fn clamp_to_throttle(d: Decision, throttle_signal: u64, restarting: bool) -> Decision {
    if throttle_signal == 0 && !restarting {
        return d;
    }
    match d {
        Decision::Shrink { from, .. } => Decision::Throttled { current: from, restarting },
        other => other,
    }
}

/// Refuse an out-of-policy direction *before* it reaches the provider.
/// `GrowOnly` turns any `Shrink` into `NoSafeShrink` (storage = the band with
/// shrink disabled, zero storage-specific code); `ObserveOnly` turns any
/// mutation into `Hold` (the field is owned elsewhere, e.g. KEDA on replicas).
#[must_use]
pub fn clamp_to_directionality(d: Decision, dir: Directionality) -> Decision {
    match (dir, &d) {
        (Directionality::GrowOnly, Decision::Shrink { from, .. }) => {
            Decision::NoSafeShrink { current: *from }
        }
        (Directionality::ObserveOnly, Decision::Grow { from, .. })
        | (Directionality::ObserveOnly, Decision::Shrink { from, .. }) => {
            Decision::AtCeiling { current: *from } // observe-only: never write the field
        }
        _ => d,
    }
}

/// What a single tick resolves to *before any I/O* — the testable heart of the
/// reconcile loop. The async loop is a thin shell: `provider.observe` →
/// [`plan_tick`] → (maybe) `provider.assign`.
#[derive(Debug, PartialEq, Eq)]
pub enum TickPlan {
    /// Another field-manager owns the field — yield (single-writer invariant).
    Conflict { manager: String },
    /// The driving metric reports usage that EXCEEDS the entity's own capacity —
    /// physically impossible for a true per-entity gauge, so the metric is not
    /// measuring THIS entity (the classic local-path PVC case: `kubelet_volume_stats`
    /// reports the whole node filesystem, ~466G used / ~905G cap, for a 10Gi volume).
    /// Carving on that number would slam the limit to ceiling on a lie, so this is a
    /// typed, observable, NEVER-carves terminal — the metric mismatch is named, not
    /// silently acted on. Only reachable for `GrowOnly` hard-capped dimensions
    /// (storage), where `used ≤ capacity` is an invariant a real gauge always honours
    /// (reserved blocks keep even a 100%-full filesystem strictly below capacity).
    Unrepresentable { used: u64, capacity: u64 },
    /// A SHRINK is warranted but the workload is still in its warmup window — held
    /// + surfaced. The idle reading is not yet proof the slack is safe to reclaim
    /// (the un-observed-boot-spike OOM); the band waits until a full duty cycle has
    /// been observed (so a boot spike folds into the demonstrated peak) before any
    /// carve. A grow during warmup still acts (buying headroom is always safe).
    Warmup { observed_for: u64, warmup: u64, current: u64 },
    /// A SHRINK is warranted by the (CFS-capped) usage metric, but the workload's
    /// SUPPRESSED DEMAND is non-blind — it is being actively THROTTLED, or recently
    /// (re)started / is crash-looping — so the low usage is a symptom, not proof of
    /// safe slack. Held + surfaced as the typed no-starve outcome; the band waits for
    /// the throttle to clear before reconsidering any reclaim (and grows it out under
    /// sustained throttle, via the demand lift). A grow is never held. This closes the
    /// CPU-blindness ratchet that starved pangea-operator (2026-06). `restarting`
    /// distinguishes a crash-loop hold from a live-throttle hold.
    Throttled { current: u64, restarting: bool },
    /// A mutation is warranted but the driving sample is too old to trust —
    /// hold + surface (the never-OOM proof requires a fresh metric).
    Stale { staleness_secs: u64, decision: Decision },
    /// A mutation is warranted but the target is within its cooldown — skip.
    Cooldown { decision: Decision },
    /// A mutation to apply atomically via the provider.
    Act { decision: Decision },
    /// An observable, non-mutating outcome (Hold / AtCeiling / NoSafeShrink / NoLimit).
    Observe { decision: Decision },
}

/// The pure per-tick planner, embodying the Viggy beats in order: Observe (the
/// passed `obs`) → Diff/guard (field-granular single-writer, fail-loud) →
/// Classify/Decide (the proven band law) → directionality gate → **freshness
/// gate** → cooldown gate. No I/O, no clock, no cluster — fully unit-testable.
/// The single-writer guard runs FIRST so the controller never computes a
/// decision for a field it doesn't own; the freshness gate runs before any
/// mutation so a stale sample can never carve in the wrong direction.
#[must_use]
pub fn plan_tick(
    obs: &Observation,
    cfg: &BandConfig,
    dir: Directionality,
    in_cooldown: bool,
    our_manager: &str,
    our_field: &str,
    max_staleness_secs: u64,
    predictive: Option<(i64, f64)>,
) -> TickPlan {
    if let Some(manager) = competing_field_manager(&obs.owners, our_manager, our_field) {
        return TickPlan::Conflict { manager };
    }
    // PER-ENTITY METRIC INVARIANT (storage / any GrowOnly hard-capped dimension):
    // a real per-entity usage gauge can never exceed the entity's own capacity —
    // a full filesystem reports used STRICTLY below capacity (reserved blocks), a
    // file table can't exceed file-max, etc. `used > capacity` therefore PROVES the
    // metric is measuring something else (the local-path PVC reporting the whole
    // node fs). Refuse to derive a carve from a number that isn't about this entity;
    // surface it as a typed, observable, non-mutating outcome instead. Scoped to
    // GrowOnly so a Bidirectional memory/cpu band — where a transient over-limit
    // sample is a legitimate "grow hard" signal — is untouched.
    if dir == Directionality::GrowOnly && obs.used > obs.capacity {
        return TickPlan::Unrepresentable { used: obs.used, capacity: obs.capacity };
    }
    // Per-resource law selection (M0): predictive `Some((rate, lookahead))` carves
    // through the proven `PredictiveGrow<BandLaw>` — pre-grows for the burst the
    // instantaneous working-set misses, asymmetric (only ever raises a grow), still
    // contained by `safety_clamp` (the never-OOM oracle covers it via
    // `safety_gate_contains_the_predictive_law`). `None` = the plain reactive
    // `BandLaw`, byte-identical to before.
    // The shrink-safety floor is keyed on the DEMONSTRATED PEAK (`obs.peak_used`),
    // never the instantaneous `obs.used` — a low-water sample can't carve under a
    // recent spike. `peak_used ≥ used` is maintained by the reconcile layer.
    //
    // SUPPRESSED-DEMAND LIFT (the CPU-blindness fix): when the workload is being
    // throttled (CFS) or has recently restarted, its (CFS-capped) `used` cannot
    // exceed the limit, so a usage-keyed shrink would ratchet it to the floor. The
    // throttle proves the demand the usage metric can't show — so lift BOTH the
    // band-law `used` and the peak floor to a demand ABOVE the current limit. The
    // proven `decide`/`safety_clamp`/`safe_min` then (a) GROW (demand > limit) and
    // (b) structurally REFUSE any shrink (the peak-keyed floor is ≥ the limit). This
    // is the SAME mechanism as a memory spike folding into the peak — it only ever
    // RAISES the inputs, so the never-OOM oracle is untouched. `None` ⇒ no throttle,
    // byte-identical to before.
    let (law_used, law_peak) =
        match throttled_demand(obs.used, obs.capacity, obs.throttle_signal, obs.restarting, cfg) {
            Some(demand) => (obs.used.max(demand), obs.peak_used.max(demand)),
            None => (obs.used, obs.peak_used),
        };
    let raw = match predictive {
        Some((rate, lookahead_secs)) => decide_with_rate(
            &PredictiveGrow { inner: BandLaw, lookahead_secs },
            law_used,
            law_peak,
            obs.capacity,
            cfg,
            rate,
        ),
        None => decide_with(&BandLaw, law_used, law_peak, obs.capacity, cfg),
    };
    let decision = clamp_to_directionality(raw, dir);
    // WARMUP GATE: a SHRINK is held while the workload is still warming up (observed
    // for fewer than `warmup_seconds` since its last restart) — its idle reading is
    // not yet proof the slack is safe to reclaim (the un-observed-boot-spike OOM).
    // Runs AFTER directionality (so it only ever sees a real, in-policy shrink) and
    // BEFORE freshness/cooldown (a warmup hold is the strongest non-mutating reason).
    // A grow passes through untouched — buying headroom is always safe, even at boot.
    let decision = clamp_to_warmup(decision, obs.observed_for_secs, cfg.warmup_seconds);
    if let Decision::Warmup { observed_for, warmup, current } = decision {
        return TickPlan::Warmup { observed_for, warmup, current };
    }
    // NO-STARVE GATE (the explicit, NAMED runtime invariant): a throttled or
    // recently-restarted/crash-looping workload is NEVER shrunk — `throttled ||
    // restarting ⇒ proposal ∈ {Grow, Hold}`. Belt-and-suspenders over the demand
    // lift above (which already grows + refuses the shrink via the proven floor):
    // even if the lift didn't bind, this converts a surviving shrink to a typed hold.
    // Runs AFTER warmup, BEFORE freshness/cooldown — a throttle hold is a strong
    // non-mutating reason. A grow passes through (relieving throttle is always safe).
    let decision = clamp_to_throttle(decision, obs.throttle_signal, obs.restarting);
    if let Decision::Throttled { current, restarting } = decision {
        return TickPlan::Throttled { current, restarting };
    }
    let is_mutation = matches!(decision, Decision::Grow { .. } | Decision::Shrink { .. });
    if !is_mutation {
        return TickPlan::Observe { decision };
    }
    if obs.staleness_secs > max_staleness_secs {
        return TickPlan::Stale { staleness_secs: obs.staleness_secs, decision };
    }
    if in_cooldown {
        return TickPlan::Cooldown { decision };
    }
    TickPlan::Act { decision }
}

/// The base unit a dimension's scalars `(used, capacity, floor, ceiling)` live
/// in. [`decide`] is unit-agnostic — it operates on opaque `u64`s — so units
/// matter at exactly one boundary: parsing a k8s quantity string into the
/// scalar, and rendering the scalar back to a k8s-valid quantity. `Bytes`
/// (memory, storage) and `Millicores` (cpu) cover the fleet today; a new unit is
/// one arm here and nowhere else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    /// Memory / storage / ephemeral-storage — a k8s byte quantity
    /// (`2Gi`, `512Mi`, bare `2147483648`).
    Bytes,
    /// CPU — a k8s cpu quantity in millicores (`250m`, `2`, `0.5`;
    /// metrics-server `5m` / `123456n`).
    Millicores,
    /// A bare integer count — pids, connections, slots, file descriptors,
    /// conntrack entries, partitions, series, NODES. Decimal-SI tolerant on
    /// parse (`1k` = 1000, never binary `Ki`); renders as a bare integer. The
    /// band law is shape-blind, so it converges on a count exactly as on bytes.
    Count,
    /// A rate in BITS per second — network bandwidth (pod egress/ingress, NAT
    /// egress). Decimal-SI on parse (`50M` = 50_000_000 bits/s); renders bare.
    BitsPerSec,
    /// A rate in BYTES per second — `io.max` bandwidth caps, EBS/EFS throughput.
    /// Byte-quantity parse (binary `Mi` + decimal `M` + bare); renders bare.
    BytesPerSec,
    /// IO operations per second — `io.max` iops caps, EBS provisioned IOPS.
    /// Decimal-SI/bare parse; renders bare.
    Iops,
    /// Requests / queries per second — samba quotaPct, ingress rps, DNS qps,
    /// client-go QPS. Decimal-SI/bare integer rate; renders bare.
    Rps,
    /// Packets / connections per second — tc police, conntrack-establish rate.
    /// Decimal-SI/bare integer rate; renders bare.
    Pps,
    /// A retention / age DURATION in SECONDS — Kafka/NATS/VM/VLogs retention.
    /// Parses `d`/`h`/`m`/`s` suffixes (or bare seconds); renders bare seconds.
    Duration,
    /// A PERCENT (0–100) — HPA setpoint, disk%, PDB. Parses `80%`/`80`; renders bare.
    Percent,
    /// A cost in CENTS — Densa.cost_sla, commitments, egress-$. Parses `$5.00`/`500`
    /// (a `$` prefix is dollars→cents; bare is cents); renders bare cents.
    Cents,
    /// CFS burst budget in MICROSECONDS — `cpu.max.burst` token-bucket depth.
    /// Decimal-SI/bare; renders bare.
    BurstUsec,
}

impl Unit {
    /// The base unit for a k8s resource leaf key. `cpu` → millicores; every other
    /// resource (`memory`, `storage`, `ephemeral-storage`) → bytes.
    #[must_use]
    pub fn for_resource(resource: &str) -> Self {
        match resource {
            "cpu" => Self::Millicores,
            _ => Self::Bytes,
        }
    }

    /// Parse a k8s quantity string into this unit's base scalar (bytes for
    /// [`Unit::Bytes`], millicores for [`Unit::Millicores`]). `None` on malformed
    /// input — callers surface a typed error rather than guess a wrong limit.
    #[must_use]
    pub fn parse(self, q: &str) -> Option<u64> {
        match self {
            Self::Bytes => parse_bytes(q),
            Self::Millicores => parse_millicores(q),
            // Count + BitsPerSec share decimal-SI integer semantics (1k = 1000).
            Self::Count | Self::BitsPerSec => parse_count(q),
            // Byte-rate parses like bytes (binary + decimal SI + bare).
            Self::BytesPerSec => parse_bytes(q),
            // Integer rates (decimal-SI / bare).
            Self::Iops | Self::Rps | Self::Pps | Self::BurstUsec => parse_count(q),
            // Specialised parses.
            Self::Duration => parse_duration_secs(q),
            Self::Percent => parse_percent(q),
            Self::Cents => parse_cents(q),
        }
    }
}

/// A scalar + its [`Unit`], rendered to a k8s-valid quantity string via
/// `Display` — the typed emission surface for a limit value. (The `write!` lives
/// inside this `Display` impl; there is no bare `format!` of k8s syntax.) Bytes
/// render as a bare integer (`2147483648`, which k8s accepts and which
/// round-trips through [`Unit::parse`]); millicores render with the `m` suffix
/// so k8s never reads the integer as whole cores (`250` cores would be
/// catastrophic).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quantity {
    pub value: u64,
    pub unit: Unit,
}

impl std::fmt::Display for Quantity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.unit {
            Unit::Bytes => write!(f, "{}", self.value),
            Unit::Millicores => write!(f, "{}m", self.value),
            // Counts + rates + the specialised scalars all render as bare integers —
            // the actuator/k8s reads the raw scalar, and the bare form round-trips
            // through `parse` (Duration→seconds, Percent→0-100, Cents→cents).
            Unit::Count
            | Unit::BitsPerSec
            | Unit::BytesPerSec
            | Unit::Iops
            | Unit::Rps
            | Unit::Pps
            | Unit::Duration
            | Unit::Percent
            | Unit::Cents
            | Unit::BurstUsec => write!(f, "{}", self.value),
        }
    }
}

/// Parse a k8s cpu quantity to millicores: `4m`→4, `500000000n`(nano)→500,
/// `250u`(micro)→0, plain cores `1`→1000, `0.5`→500.
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn parse_millicores(q: &str) -> Option<u64> {
    let q = q.trim();
    if let Some(n) = q.strip_suffix('n') {
        n.parse::<f64>().ok().map(|v| (v / 1_000_000.0) as u64)
    } else if let Some(u) = q.strip_suffix('u') {
        u.parse::<f64>().ok().map(|v| (v / 1_000.0) as u64)
    } else if let Some(m) = q.strip_suffix('m') {
        m.parse::<f64>().ok().map(|v| v as u64)
    } else {
        q.parse::<f64>().ok().map(|v| (v * 1000.0) as u64)
    }
}

/// Parse a count quantity to a bare integer: a plain `u64` fast path, else a
/// DECIMAL-SI suffix (`1k`→1000, `2M`→2_000_000) — counts are decimal, never
/// binary (a conntrack/pids/file-max value is a base-10 magnitude, not bytes).
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
fn parse_count(q: &str) -> Option<u64> {
    let q = q.trim();
    if q.is_empty() {
        return None;
    }
    if let Ok(n) = q.parse::<u64>() {
        return Some(n);
    }
    let split = q.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(q.len());
    let (num, suffix) = q.split_at(split);
    let n: f64 = num.parse().ok()?;
    let mult: f64 = match suffix.trim() {
        "" => 1.0,
        "k" | "K" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        _ => return None,
    };
    Some((n * mult) as u64)
}

/// Parse a retention/age DURATION to SECONDS. Accepts `d`/`h`/`m`/`s` suffixes
/// (`10d`, `1h`, `30m`, `60s`) or a bare integer (seconds). `None` on malformed.
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
fn parse_duration_secs(q: &str) -> Option<u64> {
    let q = q.trim();
    if q.is_empty() {
        return None;
    }
    if let Ok(n) = q.parse::<u64>() {
        return Some(n); // bare seconds
    }
    let split = q.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(q.len());
    let (num, suffix) = q.split_at(split);
    let n: f64 = num.parse().ok()?;
    let mult: f64 = match suffix.trim() {
        "s" => 1.0,
        "m" => 60.0,
        "h" => 3_600.0,
        "d" => 86_400.0,
        _ => return None,
    };
    Some((n * mult) as u64)
}

/// Parse a PERCENT (`80%` / `80`) to an integer 0–100+. `None` on malformed.
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn parse_percent(q: &str) -> Option<u64> {
    let q = q.trim().strip_suffix('%').unwrap_or(q.trim());
    q.parse::<f64>().ok().map(|v| v.round() as u64)
}

/// Parse a COST to CENTS. A `$` prefix is DOLLARS (`$5.00` → 500, `$5` → 500);
/// otherwise the bare integer IS cents (`500` → 500). `None` on malformed.
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn parse_cents(q: &str) -> Option<u64> {
    let q = q.trim();
    if let Some(dollars) = q.strip_prefix('$') {
        dollars.parse::<f64>().ok().map(|d| (d * 100.0).round() as u64)
    } else {
        q.parse::<u64>().ok()
    }
}

/// Parse a k8s byte quantity (binary IEC `Ki/Mi/Gi/Ti/Pi/Ei`, decimal SI
/// `k/M/G/T/P/E`, or a bare number) to bytes. Hand-rolled to keep
/// `breathe-control` dependency-free: split the numeric prefix from the unit
/// suffix, multiply.
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
fn parse_bytes(q: &str) -> Option<u64> {
    let q = q.trim();
    if q.is_empty() {
        return None;
    }
    let split = q.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(q.len());
    let (num, suffix) = q.split_at(split);
    let n: f64 = num.parse().ok()?;
    let mult: f64 = match suffix.trim() {
        "" => 1.0,
        "Ki" => 1024.0,
        "Mi" => 1024.0 * 1024.0,
        "Gi" => 1024.0 * 1024.0 * 1024.0,
        "Ti" => 1024.0_f64.powi(4),
        "Pi" => 1024.0_f64.powi(5),
        "Ei" => 1024.0_f64.powi(6),
        "k" | "K" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        "T" => 1e12,
        "P" => 1e15,
        "E" => 1e18,
        _ => return None,
    };
    Some((n * mult) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MI: u64 = 1 << 20;
    const GI: u64 = 1 << 30;

    fn cfg() -> BandConfig {
        BandConfig::default()
    }

    // ── metric-trust gate (the split-brain fail-safe) ──────────────────────

    /// THE BUG (fleet-wide `dry-run: 0 -> floor`, live on vector → OOM): a `0`
    /// working-set reading from a running pod must NEVER drive a shrink. Before
    /// the gate, `util = 0/limit = 0 < shrink_below` proposed a shrink that
    /// `safety_clamp` lifted only to the floor — carving the pod to its floor on a
    /// broken metric, then OOM-killing it when real RSS returned.
    #[test]
    fn a_zero_reading_never_shrinks() {
        let c = cfg(); // RestoreHeadroom default
        // limit comfortably above floor; a trusted low reading would shrink, but a
        // 0 reading is untrusted ⇒ never shrink. peak 0 ⇒ safe_min == floor ≤ limit ⇒ hold.
        let limit = 1 * GI;
        assert_eq!(
            decide_with(&BandLaw, 0, 0, limit, &c),
            Decision::Hold,
            "a 0 (untrusted) reading must hold, never shrink to floor"
        );
    }

    /// RestoreHeadroom: if a prior carve stranded the limit below the durable
    /// demonstrated-peak safe floor, a 0 reading GROWS it back (restore headroom)
    /// — the vector fix: never leave a pod starved while the observer is blind.
    #[test]
    fn a_zero_reading_restores_headroom_to_the_durable_peak_floor() {
        let c = cfg();
        let peak = 800 * MI; // durable demonstrated peak (carried from status)
        let stranded = 300 * MI; // a prior carve left the limit here (above floor 256Mi)
        let safe = (peak as f64 / c.setpoint).ceil() as u64; // ceil(800Mi / 0.80) = 1000Mi
        match decide_with(&BandLaw, 0, peak, stranded, &c) {
            Decision::Grow { to, .. } => assert_eq!(to, safe, "restore to the durable-peak safe floor"),
            other => panic!("expected a headroom-restoring grow, got {other:?}"),
        }
    }

    /// The Hold policy freezes the limit exactly on an untrusted reading.
    #[test]
    fn hold_policy_freezes_on_a_zero_reading() {
        let c = BandConfig { metric_missing_policy: MetricMissingPolicy::Hold, ..cfg() };
        // even with a durable peak that would justify a restore, Hold freezes.
        assert_eq!(decide_with(&BandLaw, 0, 800 * MI, 300 * MI, &c), Decision::Hold);
    }

    /// `Trust` (node-count + any dimension where 0 is real) NEVER gates — a 0
    /// reading runs the band law normally (a pool scaled to zero stays at zero,
    /// not held/restored as if the metric were broken).
    #[test]
    fn trust_policy_treats_zero_as_a_real_reading() {
        let c = BandConfig { metric_missing_policy: MetricMissingPolicy::Trust, ..cfg() };
        assert!(metric_untrusted_decision(0, 0, 1 * GI, &c).is_none(), "Trust must not gate a 0 reading");
    }

    /// A TRUSTED reading is unaffected — the gate only fires on `used == 0`.
    #[test]
    fn a_nonzero_reading_runs_the_normal_law() {
        let c = cfg();
        // util = 0.50 < shrink_below → a normal (clamped) shrink, NOT gated.
        assert!(matches!(decide_with(&BandLaw, 500 * MI, 500 * MI, 1 * GI, &c), Decision::Shrink { .. }));
        assert!(metric_untrusted_decision(500 * MI, 500 * MI, 1 * GI, &c).is_none());
    }

    // ── band edges ─────────────────────────────────────────────────────────

    #[test]
    fn holds_inside_the_deadband() {
        let c = cfg();
        // util = 0.80 (setpoint) → hold
        assert_eq!(decide(800 * MI, 1000 * MI, &c), Decision::Hold);
        // exact lower edge 0.70 → hold (shrink is strict `<`)
        assert_eq!(decide(700 * MI, 1000 * MI, &c), Decision::Hold);
        // exact upper edge 0.85 → hold (grow is strict `>`)
        assert_eq!(decide(850 * MI, 1000 * MI, &c), Decision::Hold);
    }

    #[test]
    fn grows_above_upper_edge() {
        let c = cfg();
        // util = 0.95 at 1Gi → grow to ceil(1.25Gi)
        let from = GI;
        match decide(950 * MI, from, &c) {
            Decision::Grow { from: f, to } => {
                assert_eq!(f, from);
                assert_eq!(to, (from as f64 * 1.25).ceil() as u64);
                assert!(to > from);
            }
            d => panic!("expected Grow, got {d:?}"),
        }
    }

    #[test]
    fn shrinks_below_lower_edge_gently() {
        let c = cfg();
        // util = 0.20 at 2Gi → gentle 0.9× step (gentle wins over safe_min here)
        let from = 2 * GI;
        match decide(400 * MI, from, &c) {
            Decision::Shrink { from: f, to } => {
                assert_eq!(f, from);
                assert_eq!(to, (from as f64 * 0.90).floor() as u64);
                assert!(to < from);
                // post-shrink util is still well under the grow edge — no flap
                let new_util = (400 * MI) as f64 / to as f64;
                assert!(new_util < c.grow_above);
            }
            d => panic!("expected Shrink, got {d:?}"),
        }
    }

    // ── shrink safety: never OOM, never overshoot into grow territory ───────

    #[test]
    fn shrink_clamps_to_safe_min_when_step_too_aggressive() {
        // Contrived aggressive policy: shrink as soon as util < 0.85, by 50%.
        // safe_min must bind so the shrink can't push live pages over the band.
        let c = BandConfig {
            grow_above: 0.90,
            shrink_below: 0.85,
            setpoint: 0.80,
            shrink_factor: 0.50,
            ..BandConfig::default()
        };
        let from = GI;
        let ws = 800 * MI; // util 0.78 < 0.85 → shrink
        match decide(ws, from, &c) {
            Decision::Shrink { to, .. } => {
                let safe_min = (ws as f64 / 0.80).ceil() as u64;
                assert_eq!(to, safe_min, "must clamp to safe_min, not the 50% step");
                // after the clamped shrink, util == setpoint (≤ grow edge)
                let new_util = ws as f64 / to as f64;
                assert!(new_util <= 0.80 + 1e-9);
            }
            d => panic!("expected clamped Shrink, got {d:?}"),
        }
    }

    // ── ceiling / floor circuit breakers ────────────────────────────────────

    #[test]
    fn at_ceiling_does_not_grow() {
        let c = cfg(); // ceiling 16Gi
        assert_eq!(
            decide(16 * GI, 16 * GI, &c),
            Decision::AtCeiling { current: 16 * GI }
        );
    }

    #[test]
    fn at_floor_does_not_shrink() {
        let c = cfg(); // floor 256Mi
        // tiny working set at the floor → cannot shrink below floor
        assert_eq!(
            decide(10 * MI, 256 * MI, &c),
            Decision::NoSafeShrink { current: 256 * MI }
        );
    }

    #[test]
    fn unset_limit_seeds_to_floor() {
        // a freshly-ceded (unset = 0) limit is grown straight to the floor,
        // so breathe can take over the field. Independent of working-set.
        let c = cfg(); // floor 256Mi
        assert_eq!(decide(500 * MI, 0, &c), Decision::Grow { from: 0, to: c.floor_bytes });
    }

    #[test]
    fn below_floor_grows_to_floor() {
        let c = cfg();
        // current 1Gi but floor is set to 2Gi → snap up to 2Gi regardless of util
        let c2 = BandConfig { floor_bytes: 2 * GI, ..cfg() };
        assert_eq!(decide(80 * MI, GI, &c2), Decision::Grow { from: GI, to: 2 * GI });
        let _ = c;
    }

    #[test]
    fn above_ceiling_snaps_down() {
        let c = BandConfig { ceiling_bytes: 4 * GI, ..cfg() };
        // current 8Gi > ceiling 4Gi → snap down (a Shrink to the ceiling)
        assert_eq!(decide(GI, 8 * GI, &c), Decision::Shrink { from: 8 * GI, to: 4 * GI });
        // …but on a GrowOnly dim the directionality clamp forbids the snap-down
        assert_eq!(
            clamp_to_directionality(decide(GI, 8 * GI, &c), Directionality::GrowOnly),
            Decision::NoSafeShrink { current: 8 * GI }
        );
    }

    // ── convergence: repeated ticks settle into the band and stop ───────────

    #[test]
    fn repeated_shrink_ticks_converge_into_band_and_hold() {
        let c = cfg();
        let ws = 600 * MI;
        let mut limit = 4 * GI; // util 0.146 — way over-allotted
        for _ in 0..50 {
            match decide(ws, limit, &c) {
                Decision::Shrink { to, .. } => limit = to,
                Decision::Hold | Decision::NoSafeShrink { .. } => break,
                d => panic!("unexpected during converge: {d:?}"),
            }
        }
        let util = ws as f64 / limit as f64;
        assert!(
            util >= c.shrink_below && util <= c.grow_above,
            "converged util {util} must land inside the deadband"
        );
        // and it is stable: one more tick holds
        assert_eq!(decide(ws, limit, &c), Decision::Hold);
    }

    #[test]
    fn repeated_grow_ticks_converge_into_band() {
        let c = cfg();
        let ws = 950 * MI;
        let mut limit = GI; // util 0.927 — under-allotted
        for _ in 0..50 {
            match decide(ws, limit, &c) {
                Decision::Grow { to, .. } => limit = to,
                Decision::Hold | Decision::AtCeiling { .. } => break,
                d => panic!("unexpected during converge: {d:?}"),
            }
        }
        let util = ws as f64 / limit as f64;
        assert!(util <= c.grow_above, "converged util {util} must drop to/under the grow edge");
    }

    // ── single-writer invariant ─────────────────────────────────────────────

    fn owns(mgr: &str, field: &str) -> FieldOwner {
        FieldOwner { manager: mgr.into(), field: field.into() }
    }

    #[test]
    fn detects_competing_memory_manager() {
        let owners = vec![
            owns("helm", "metadata.labels"),
            owns("vpa-updater", MEMORY_LIMIT_FIELD),
        ];
        assert_eq!(
            competing_memory_manager(&owners, "pleme-memory-elastic"),
            Some("vpa-updater".into())
        );
    }

    #[test]
    fn no_conflict_when_only_we_own_the_limit() {
        let owners = vec![
            owns("pleme-memory-elastic", MEMORY_LIMIT_FIELD),
            owns("flux", "spec.template.spec.containers"),
        ];
        assert_eq!(competing_memory_manager(&owners, "pleme-memory-elastic"), None);
    }

    #[test]
    fn no_conflict_when_nobody_owns_the_limit() {
        let owners = vec![owns("flux", "metadata.annotations")];
        assert_eq!(competing_memory_manager(&owners, "pleme-memory-elastic"), None);
    }

    #[test]
    fn keda_on_replicas_is_not_a_memory_competitor() {
        // The disjoint-field composition contract: KEDA owns spec.replicas, a
        // memory band owns resources.limits.memory — different paths ⇒ no fight.
        let owners = vec![owns("keda-operator", "spec.replicas")];
        assert_eq!(
            competing_field_manager(&owners, "breathe-memory", MEMORY_LIMIT_FIELD),
            None
        );
        // …but a genuine same-field competitor (VPA) is still caught.
        let owners2 = vec![owns("keda-operator", "spec.replicas"), owns("vpa", MEMORY_LIMIT_FIELD)];
        assert_eq!(
            competing_field_manager(&owners2, "breathe-memory", MEMORY_LIMIT_FIELD),
            Some("vpa".into())
        );
    }

    // ── directionality gate: storage = band with shrink disabled, no special code ─

    #[test]
    fn growonly_converts_shrink_to_nosafeshrink() {
        assert_eq!(
            clamp_to_directionality(
                Decision::Shrink { from: 2 * GI, to: 1800 * MI },
                Directionality::GrowOnly
            ),
            Decision::NoSafeShrink { current: 2 * GI }
        );
    }

    #[test]
    fn growonly_passes_grow_through() {
        assert_eq!(
            clamp_to_directionality(Decision::Grow { from: GI, to: 2 * GI }, Directionality::GrowOnly),
            Decision::Grow { from: GI, to: 2 * GI }
        );
    }

    #[test]
    fn bidirectional_passes_shrink_through() {
        assert_eq!(
            clamp_to_directionality(
                Decision::Shrink { from: 2 * GI, to: 1800 * MI },
                Directionality::Bidirectional
            ),
            Decision::Shrink { from: 2 * GI, to: 1800 * MI }
        );
    }

    // ── plan_tick: the pure reconcile heart (single-writer FIRST) ────────────

    fn obs(used: u64, cap: u64, owners: Vec<FieldOwner>) -> Observation {
        // peak == used: no trailing-window history ⇒ instantaneous-equivalent.
        // observed_for u64::MAX ⇒ past warmup (warmup is exercised in its own tests).
        Observation { used, peak_used: used, capacity: cap, owners, staleness_secs: 0, memory_shrink_restart_free: false, observed_for_secs: u64::MAX, request_floor: 0, throttle_signal: 0, restarting: false }
    }
    /// An observation with an explicit trailing-window peak (peak ≥ used).
    #[allow(dead_code)] // a shared test helper retained for peak-keyed observations.
    fn obs_peak(used: u64, peak: u64, cap: u64, owners: Vec<FieldOwner>) -> Observation {
        Observation { used, peak_used: peak.max(used), capacity: cap, owners, staleness_secs: 0, memory_shrink_restart_free: false, observed_for_secs: u64::MAX, request_floor: 0, throttle_signal: 0, restarting: false }
    }
    fn ours() -> Vec<FieldOwner> {
        vec![owns("breathe-memory", MEMORY_LIMIT_FIELD)]
    }
    const FRESH: u64 = 60; // max acceptable sample age in these tests

    #[test]
    fn plan_yields_on_conflict_before_deciding() {
        // util 0.95 would Act, but a competing same-field owner must yield FIRST.
        let owners = vec![owns("vpa", MEMORY_LIMIT_FIELD)];
        assert_eq!(
            plan_tick(&obs(950 * MI, GI, owners), &cfg(), Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None),
            TickPlan::Conflict { manager: "vpa".into() }
        );
    }

    #[test]
    fn plan_acts_when_mutation_and_not_in_cooldown() {
        match plan_tick(&obs(950 * MI, GI, ours()), &cfg(), Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None) {
            TickPlan::Act { decision: Decision::Grow { .. } } => {}
            p => panic!("expected Act(Grow), got {p:?}"),
        }
    }

    #[test]
    fn plan_defers_mutation_in_cooldown() {
        match plan_tick(&obs(950 * MI, GI, ours()), &cfg(), Directionality::Bidirectional, true, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None) {
            TickPlan::Cooldown { decision: Decision::Grow { .. } } => {}
            p => panic!("expected Cooldown(Grow), got {p:?}"),
        }
    }

    #[test]
    fn plan_observes_hold_without_mutation() {
        assert_eq!(
            plan_tick(&obs(800 * MI, GI, ours()), &cfg(), Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None),
            TickPlan::Observe { decision: Decision::Hold }
        );
    }

    #[test]
    fn plan_observes_growonly_shrink_as_nosafeshrink() {
        // storage-like: util 0.20 would Shrink, but GrowOnly turns it into an
        // observable NoSafeShrink — one band law, no storage-specific path.
        match plan_tick(&obs(200 * MI, GI, ours()), &cfg(), Directionality::GrowOnly, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None) {
            TickPlan::Observe { decision: Decision::NoSafeShrink { .. } } => {}
            p => panic!("expected Observe(NoSafeShrink), got {p:?}"),
        }
    }

    #[test]
    fn plan_flags_growonly_used_over_capacity_as_unrepresentable() {
        // the local-path PVC case: kubelet_volume_stats reports the whole node fs
        // (used 2Gi) for a 1Gi volume (capacity). A per-volume gauge can NEVER do
        // this (reserved blocks keep a full fs strictly below capacity), so the
        // metric isn't about this entity — refuse to carve, surface it typed.
        match plan_tick(&obs(2 * GI, GI, ours()), &cfg(), Directionality::GrowOnly, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None) {
            TickPlan::Unrepresentable { used, capacity } => {
                assert_eq!((used, capacity), (2 * GI, GI));
            }
            p => panic!("expected Unrepresentable, got {p:?}"),
        }
    }

    #[test]
    fn plan_does_not_flag_bidirectional_over_limit_as_unrepresentable() {
        // a memory/cpu band reading momentarily ABOVE its limit is a legitimate
        // "grow hard" signal — the guard is scoped to GrowOnly and must not fire.
        match plan_tick(&obs(2 * GI, GI, ours()), &cfg(), Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None) {
            TickPlan::Act { decision: Decision::Grow { .. } } => {}
            p => panic!("expected Act(Grow) for an over-limit Bidirectional band, got {p:?}"),
        }
    }

    #[test]
    fn plan_does_not_flag_growonly_full_volume_as_unrepresentable() {
        // a genuinely 100%-full GrowOnly volume reads used == capacity (a valid
        // per-volume reading) — it must carve (grow), NOT be flagged unrepresentable.
        // Only used STRICTLY > capacity proves the wrong-entity metric.
        match plan_tick(&obs(GI, GI, ours()), &cfg(), Directionality::GrowOnly, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None) {
            TickPlan::Act { decision: Decision::Grow { .. } } => {}
            p => panic!("expected Act(Grow) for a full GrowOnly volume, got {p:?}"),
        }
    }

    #[test]
    fn plan_refuses_to_mutate_on_stale_metric() {
        // util 0.95 would Act(Grow), but a sample older than the bound must never
        // carve — the never-OOM proof holds only on a fresh metric.
        let stale = Observation { used: 950 * MI, peak_used: 950 * MI, capacity: GI, owners: ours(), staleness_secs: 120, memory_shrink_restart_free: false, observed_for_secs: u64::MAX, request_floor: 0, throttle_signal: 0, restarting: false };
        match plan_tick(&stale, &cfg(), Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None) {
            TickPlan::Stale { staleness_secs: 120, decision: Decision::Grow { .. } } => {}
            p => panic!("expected Stale(Grow), got {p:?}"),
        }
    }

    #[test]
    fn plan_observeonly_never_mutates() {
        // a replica-like ObserveOnly dim: even a strong grow signal yields no write.
        match plan_tick(&obs(950 * MI, GI, ours()), &cfg(), Directionality::ObserveOnly, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None) {
            TickPlan::Observe { .. } => {}
            p => panic!("expected Observe (no mutation), got {p:?}"),
        }
    }

    // ── unit codec: the only place dimensions stop being unit-agnostic ───────

    #[test]
    fn unit_for_resource_maps_cpu_to_millicores() {
        assert_eq!(Unit::for_resource("cpu"), Unit::Millicores);
        assert_eq!(Unit::for_resource("memory"), Unit::Bytes);
        assert_eq!(Unit::for_resource("storage"), Unit::Bytes);
        assert_eq!(Unit::for_resource("ephemeral-storage"), Unit::Bytes);
    }

    #[test]
    fn bytes_parse_binary_decimal_and_bare() {
        assert_eq!(Unit::Bytes.parse("2Gi"), Some(2 * GI));
        assert_eq!(Unit::Bytes.parse("512Mi"), Some(512 * MI));
        assert_eq!(Unit::Bytes.parse("256Mi"), Some(256 * MI));
        assert_eq!(Unit::Bytes.parse("80216Ki"), Some(80216 * 1024));
        // breathe's own written value round-trips (bare bytes).
        assert_eq!(Unit::Bytes.parse("2147483648"), Some(2 * GI));
        // decimal SI + fractional.
        assert_eq!(Unit::Bytes.parse("1G"), Some(1_000_000_000));
        assert_eq!(Unit::Bytes.parse("1.5Gi"), Some(1_610_612_736));
        // malformed → None (typed error upstream, never a wrong limit).
        assert_eq!(Unit::Bytes.parse("garbage"), None);
        assert_eq!(Unit::Bytes.parse(""), None);
    }

    #[test]
    fn millicores_parse_suffixes_and_bare_cores() {
        assert_eq!(Unit::Millicores.parse("250m"), Some(250));
        assert_eq!(Unit::Millicores.parse("2"), Some(2000)); // bare cores → millicores
        assert_eq!(Unit::Millicores.parse("0.5"), Some(500));
        assert_eq!(Unit::Millicores.parse("1"), Some(1000));
        assert_eq!(Unit::Millicores.parse("5m"), Some(5)); // metrics-server idle cpu
        assert_eq!(Unit::Millicores.parse("123456n"), Some(0)); // nanocores, sub-milli
        assert_eq!(Unit::Millicores.parse("500000000n"), Some(500));
        assert_eq!(Unit::Millicores.parse("nonsense"), None);
    }

    #[test]
    fn quantity_renders_unit_correct_k8s_strings() {
        // bytes: bare integer (round-trips through parse).
        let mem = Quantity { value: 2 * GI, unit: Unit::Bytes };
        assert_eq!(mem.to_string(), "2147483648");
        assert_eq!(Unit::Bytes.parse(&mem.to_string()), Some(2 * GI));
        // cpu: MUST carry the `m` suffix — "250" would be read as 250 CORES.
        let cpu = Quantity { value: 250, unit: Unit::Millicores };
        assert_eq!(cpu.to_string(), "250m");
        assert_eq!(Unit::Millicores.parse(&cpu.to_string()), Some(250));
    }

    // ── ControlLaw trait + shared safety gate (the conformance oracle) ───────

    #[test]
    fn decide_is_exactly_bandlaw_through_the_gate() {
        // The free `decide` == `decide_with(&BandLaw, …, …)`, so every band-edge
        // test above is also a behaviour-preservation test for the trait lift.
        let c = cfg();
        for (ws, lim) in [(800 * MI, GI), (950 * MI, GI), (200 * MI, GI), (0, 0), (16 * GI, 16 * GI)] {
            assert_eq!(decide(ws, lim, &c), decide_with(&BandLaw, ws, ws, lim, &c));
        }
    }

    #[test]
    fn safety_clamp_caps_grow_at_ceiling() {
        let c = BandConfig { ceiling_bytes: 4 * GI, ..cfg() };
        // a law proposing 100Gi is capped to the ceiling
        assert_eq!(safety_clamp(Proposal::Target(100 * GI), GI, GI, 2 * GI, &c), Decision::Grow { from: 2 * GI, to: 4 * GI });
        // growth with no room → AtCeiling, not an over-ceiling write
        assert_eq!(safety_clamp(Proposal::Target(100 * GI), GI, GI, 4 * GI, &c), Decision::AtCeiling { current: 4 * GI });
    }

    #[test]
    fn safety_clamp_lifts_shrink_to_safe_min() {
        let c = cfg();
        let ws = 800 * MI;
        let safe_min = (ws as f64 / c.setpoint).ceil() as u64;
        match safety_clamp(Proposal::Target(1), ws, ws, 2 * GI, &c) {
            Decision::Shrink { to, .. } => assert_eq!(to, safe_min.max(c.floor_bytes), "shrink lifted to the safe minimum"),
            d => panic!("expected clamped Shrink, got {d:?}"),
        }
    }

    /// THE CONFORMANCE ORACLE: the shared safety gate must contain ANY control
    /// law — including adversarial ones that propose extreme targets — within
    /// the floor / ceiling / safe-min invariants. Every future law (PID, AIMD,
    /// predictive, learned) is gated against exactly this.
    #[test]
    fn safety_gate_contains_any_law() {
        struct GrowToMax;
        impl ControlLaw for GrowToMax {
            fn propose(&self, _w: u64, _l: u64, _c: &BandConfig) -> Proposal { Proposal::Target(u64::MAX) }
        }
        struct ShrinkToZero;
        impl ControlLaw for ShrinkToZero {
            fn propose(&self, _w: u64, _l: u64, _c: &BandConfig) -> Proposal { Proposal::Target(0) }
        }
        let c = cfg();
        for &ws in &[0u64, 100 * MI, 800 * MI, 4 * GI, 16 * GI, 32 * GI] {
            for &limit in &[256 * MI, GI, 4 * GI, 16 * GI, 20 * GI /* > ceiling: snap */] {
                let safe_min = (ws as f64 / c.setpoint).ceil() as u64;
                for d in [
                    decide_with(&GrowToMax, ws, ws, limit, &c),
                    decide_with(&ShrinkToZero, ws, ws, limit, &c),
                    decide_with(&BandLaw, ws, ws, limit, &c),
                    decide_with(&ProportionalLaw { gain: 1.0 }, ws, ws, limit, &c),
                    decide_with(&ProportionalLaw { gain: 0.5 }, ws, ws, limit, &c),
                    decide_with(&SlewLimited { inner: GrowToMax, max_step_frac: 0.25 }, ws, ws, limit, &c),
                    decide_with(&SlewLimited { inner: ShrinkToZero, max_step_frac: 0.25 }, ws, ws, limit, &c),
                    // PR-1: QuantizedSlice — snapping a target to a quantum cannot
                    // escape the envelope (the gate still owns floor/ceiling/safe_min).
                    decide_with(&QuantizedSlice { inner: GrowToMax, slice: 64 }, ws, ws, limit, &c),
                    decide_with(&QuantizedSlice { inner: ShrinkToZero, slice: 64 }, ws, ws, limit, &c),
                    decide_with(&QuantizedSlice { inner: BandLaw, slice: 1 }, ws, ws, limit, &c),
                    // PR-3: ThrottleAware — no-rate path is the inner law verbatim.
                    decide_with(&ThrottleAware { inner: GrowToMax }, ws, ws, limit, &c),
                    decide_with(&ThrottleAware { inner: ShrinkToZero }, ws, ws, limit, &c),
                    // PR-3: ThrottleAware under an ACTIVE throttle signal — a shrink
                    // is suppressed to Hold, a grow still clamps to the ceiling.
                    decide_with_rate(&ThrottleAware { inner: GrowToMax }, ws, ws, limit, &c, 5),
                    decide_with_rate(&ThrottleAware { inner: ShrinkToZero }, ws, ws, limit, &c, 5),
                    // The Step-5..14 law families — each funnels through the SAME gate.
                    decide_with(&PercentileBand, ws, ws, limit, &c),
                    decide_with(&BurstBudget, ws, ws, limit, &c),
                    decide_with(&Aimd { increment: GI, decrease_factor: 0.5 }, ws, ws, limit, &c),
                    decide_with_rate(&Aimd { increment: GI, decrease_factor: 0.5 }, ws, ws, limit, &c, 7),
                    decide_with(&SharedBudget { inner: GrowToMax, available_headroom: GI }, ws, ws, limit, &c),
                    decide_with(&SharedBudget { inner: ShrinkToZero, available_headroom: GI }, ws, ws, limit, &c),
                ] {
                    match d {
                        Decision::Grow { from, to } => {
                            assert!(to <= c.ceiling_bytes, "ws={ws} limit={limit}: grew above ceiling to {to}");
                            assert!(to > from, "a Grow must raise the limit");
                        }
                        Decision::Shrink { from, to } => {
                            assert!(to >= c.floor_bytes || from > c.ceiling_bytes, "shrank below floor");
                            // never shrink below safe_min (would push live pages over the band) —
                            // the sole exception is the hard ceiling-snap (from > ceiling).
                            assert!(to >= safe_min || from > c.ceiling_bytes, "ws={ws} limit={limit}: shrank below safe_min to {to}");
                            assert!(to < from, "a Shrink must lower the limit");
                        }
                        _ => {} // Hold / AtCeiling / NoSafeShrink / NoLimit never mutate
                    }
                }
            }
        }
    }

    #[test]
    fn count_unit_parses_and_renders_bare_integers() {
        assert_eq!(Unit::Count.parse("42"), Some(42));
        assert_eq!(Unit::Count.parse("1k"), Some(1000)); // decimal-SI, not binary
        assert_eq!(Unit::Count.parse("2M"), Some(2_000_000));
        assert_eq!(Unit::Count.parse(""), None);
        assert_eq!(Unit::Count.parse("garbage"), None);
        assert_eq!(Quantity { value: 110, unit: Unit::Count }.to_string(), "110");
    }

    #[test]
    fn io_rate_units_parse_and_render_for_pr4() {
        // BitsPerSec — decimal-SI bits (50M = 50 megabits/s).
        assert_eq!(Unit::BitsPerSec.parse("50M"), Some(50_000_000));
        assert_eq!(Unit::BitsPerSec.parse("1G"), Some(1_000_000_000));
        // BytesPerSec — byte-quantity (binary + decimal + bare).
        assert_eq!(Unit::BytesPerSec.parse("100Mi"), Some(100 * 1024 * 1024));
        assert_eq!(Unit::BytesPerSec.parse("125000000"), Some(125_000_000));
        // Iops — bare/decimal-SI integer rate.
        assert_eq!(Unit::Iops.parse("3000"), Some(3000));
        assert_eq!(Unit::Iops.parse("16k"), Some(16_000));
        // all render bare + round-trip.
        for u in [Unit::BitsPerSec, Unit::BytesPerSec, Unit::Iops] {
            assert_eq!(Quantity { value: 4096, unit: u }.to_string(), "4096");
            assert_eq!(u.parse("4096"), Some(4096));
        }
    }

    #[test]
    fn the_remaining_carve_units_parse_and_render() {
        // Rps / Pps / BurstUsec — decimal-SI integer rates.
        assert_eq!(Unit::Rps.parse("5k"), Some(5000));
        assert_eq!(Unit::Pps.parse("100000"), Some(100_000));
        assert_eq!(Unit::BurstUsec.parse("2000"), Some(2000));
        // Duration → seconds.
        assert_eq!(Unit::Duration.parse("10d"), Some(864_000));
        assert_eq!(Unit::Duration.parse("1h"), Some(3600));
        assert_eq!(Unit::Duration.parse("30m"), Some(1800));
        assert_eq!(Unit::Duration.parse("90"), Some(90)); // bare seconds
        // Percent.
        assert_eq!(Unit::Percent.parse("80%"), Some(80));
        assert_eq!(Unit::Percent.parse("80"), Some(80));
        // Cents — $ = dollars→cents, bare = cents.
        assert_eq!(Unit::Cents.parse("$5.00"), Some(500));
        assert_eq!(Unit::Cents.parse("$5"), Some(500));
        assert_eq!(Unit::Cents.parse("500"), Some(500));
        // all render bare + round-trip through their base scalar.
        for u in [Unit::Rps, Unit::Pps, Unit::Duration, Unit::Percent, Unit::Cents, Unit::BurstUsec] {
            assert_eq!(Quantity { value: 42, unit: u }.to_string(), "42");
        }
    }

    #[test]
    fn quantized_slice_snaps_targets_up_to_the_quantum() {
        let c = cfg();
        // A band grow gets snapped UP to a multiple of `slice` (over-provision = safe).
        // slice=1 is a no-op; a wide slice rounds up.
        assert_eq!(snap_up(Proposal::Target(130), 64), Proposal::Target(192)); // 130→2*64
        assert_eq!(snap_up(Proposal::Target(128), 64), Proposal::Target(128)); // already aligned
        assert_eq!(snap_up(Proposal::Target(7), 1), Proposal::Target(7)); // slice 1 = no-op
        assert_eq!(snap_up(Proposal::Hold, 64), Proposal::Hold);
        // saturates rather than overflowing on a GrowToMax target.
        assert_eq!(snap_up(Proposal::Target(u64::MAX), 64), Proposal::Target(u64::MAX));
        // end-to-end: the snapped grow still clamps to the ceiling.
        let d = decide_with(&QuantizedSlice { inner: BandLaw, slice: 7 }, 950 * MI, 950 * MI, GI, &c);
        assert!(matches!(d, Decision::Grow { .. } | Decision::AtCeiling { .. }));
    }

    #[test]
    fn throttle_aware_suppresses_shrink_while_throttling_but_not_grow() {
        let c = cfg();
        // A low-util sample (would shrink) — but with a live throttle signal,
        // ThrottleAware holds instead of tightening the cap (anti-flap).
        let low_util_ws = 100 * MI; // util 0.1 at 1Gi → BandLaw would shrink
        let shrink = decide_with(&ThrottleAware { inner: BandLaw }, low_util_ws, low_util_ws, GI, &c);
        assert!(matches!(shrink, Decision::Shrink { .. }), "no throttle signal ⇒ inner shrink");
        let held = decide_with_rate(&ThrottleAware { inner: BandLaw }, low_util_ws, low_util_ws, GI, &c, 3);
        assert_eq!(held, Decision::Hold, "active throttle ⇒ shrink suppressed to Hold");
        // A grow is NEVER suppressed — relieving throttle is the safe move.
        let high_util_ws = 950 * MI;
        let grown = decide_with_rate(&ThrottleAware { inner: BandLaw }, high_util_ws, high_util_ws, GI, &c, 9);
        assert!(matches!(grown, Decision::Grow { .. }), "throttle + high util ⇒ still grows");
    }

    #[test]
    fn percentile_band_sizes_directly_to_the_setpoint_outside_the_deadband() {
        let c = cfg();
        // util 0.95 at 1Gi → size so the working set sits at the setpoint (0.80).
        let ws = (0.95 * GI as f64) as u64;
        match decide_with(&PercentileBand, ws, ws, GI, &c) {
            Decision::Grow { to, .. } => {
                let new_util = ws as f64 / to as f64;
                assert!((new_util - c.setpoint).abs() < 0.02, "lands util at setpoint, got {new_util}");
            }
            d => panic!("expected Grow, got {d:?}"),
        }
        // in-band → hold.
        assert_eq!(decide_with(&PercentileBand, (0.78 * GI as f64) as u64, (0.78 * GI as f64) as u64, GI, &c), Decision::Hold);
    }

    #[test]
    fn aimd_additively_increases_and_multiplicatively_decreases() {
        let c = cfg();
        let law = Aimd { increment: 256 * MI, decrease_factor: 0.5 };
        // no throttle → additive increase by `increment`.
        match decide_with(&law, 500 * MI, 500 * MI, GI, &c) {
            Decision::Grow { from, to } => { assert_eq!(from, GI); assert_eq!(to, GI + 256 * MI); }
            d => panic!("expected additive-increase Grow, got {d:?}"),
        }
        // throttle (rate>0) → multiplicative decrease to half (clamped to safe-min).
        let d = decide_with_rate(&law, 100 * MI, 100 * MI, 4 * GI, &c, 9);
        assert!(matches!(d, Decision::Shrink { to, .. } if to <= 2 * GI), "multiplicative back-off, got {d:?}");
    }

    #[test]
    fn shared_budget_clamps_a_grow_to_the_available_headroom() {
        let c = cfg();
        // GrowToMax wants u64::MAX, but only 512Mi of shared budget is free → cap there.
        let law = SharedBudget { inner: { struct G; impl ControlLaw for G { fn propose(&self,_:u64,l:u64,_:&BandConfig)->Proposal{Proposal::Target(l*100)} } G }, available_headroom: 512 * MI };
        match decide_with(&law, 950 * MI, 950 * MI, GI, &c) {
            Decision::Grow { to, .. } => assert_eq!(to, GI + 512 * MI, "grow clamped to current + headroom"),
            d => panic!("expected headroom-clamped Grow, got {d:?}"),
        }
    }

    #[test]
    fn burn_rate_band_vetoes_only_a_forecast_breach() {
        // spent $30 of a $50 budget, 100s left, burning 30¢/s → forecast $60 > $50 → breach.
        assert!(burn_rate_breaches(3000, 5000, 30.0, 100));
        // same spend, burning 10¢/s → forecast $40 < $50 → no breach.
        assert!(!burn_rate_breaches(3000, 5000, 10.0, 100));
        // already over budget regardless of rate.
        assert!(burn_rate_breaches(6000, 5000, 0.0, 0));
    }

    #[test]
    fn a_custom_law_plugs_in_without_touching_safety() {
        // A trivial alternative law (always grow one floor-step) proves a new law
        // is just a `propose` impl — the gate keeps it safe with zero new safety
        // code. This is the whole compounding point of the trait lift.
        struct StepUp;
        impl ControlLaw for StepUp {
            fn propose(&self, _w: u64, limit: u64, cfg: &BandConfig) -> Proposal {
                Proposal::Target(limit + cfg.floor_bytes)
            }
        }
        let c = cfg();
        // in-range: grows by a floor-step, capped at ceiling
        match decide_with(&StepUp, 800 * MI, 800 * MI, GI, &c) {
            Decision::Grow { from, to } => { assert_eq!(from, GI); assert_eq!(to, GI + c.floor_bytes); }
            d => panic!("expected Grow, got {d:?}"),
        }
        // and it STILL can't breach the ceiling — the shared gate owns that
        assert_eq!(decide_with(&StepUp, GI, GI, c.ceiling_bytes, &c), Decision::AtCeiling { current: c.ceiling_bytes });
    }

    #[test]
    fn proportional_law_lands_util_at_setpoint_in_one_tick_at_full_gain() {
        let c = cfg();
        let ws = 950 * MI; // util 0.927 at 1Gi → grow
        // gain 1.0 → target the limit that lands util exactly at the setpoint
        match decide_with(&ProportionalLaw { gain: 1.0 }, ws, ws, GI, &c) {
            Decision::Grow { to, .. } => {
                let new_util = ws as f64 / to as f64;
                assert!((new_util - c.setpoint).abs() < 0.02, "util {new_util} should land at setpoint");
            }
            d => panic!("expected Grow, got {d:?}"),
        }
    }

    #[test]
    fn proportional_law_step_scales_with_deviance() {
        let c = cfg();
        // further from the setpoint ⇒ bigger step (the deviance-proportional response)
        let near = match decide_with(&ProportionalLaw { gain: 1.0 }, 870 * MI, 870 * MI, GI, &c) {
            Decision::Grow { from, to } => to - from,
            _ => 0,
        };
        let far = match decide_with(&ProportionalLaw { gain: 1.0 }, 980 * MI, 980 * MI, GI, &c) {
            Decision::Grow { from, to } => to - from,
            _ => 0,
        };
        assert!(far > near, "a larger deviance must produce a larger corrective step ({far} vs {near})");
    }

    #[test]
    fn slew_limited_caps_an_aggressive_jump() {
        let c = cfg();
        // GrowToMax wants u64::MAX; the 25% slew cap limits the per-tick rise.
        struct GrowToMax;
        impl ControlLaw for GrowToMax {
            fn propose(&self, _w: u64, _l: u64, _c: &BandConfig) -> Proposal { Proposal::Target(u64::MAX) }
        }
        match decide_with(&SlewLimited { inner: GrowToMax, max_step_frac: 0.25 }, 950 * MI, 950 * MI, GI, &c) {
            Decision::Grow { from, to } => {
                let rise = (to - from) as f64 / from as f64;
                assert!(rise <= 0.26, "slew cap holds the per-tick rise near 25% (got {rise})");
            }
            d => panic!("expected a capped Grow, got {d:?}"),
        }
    }

    // ── PredictiveGrow: asymmetric feed-forward (the burst-OOM fix) ──────────

    #[test]
    fn predictive_grow_with_zero_rate_is_identical_to_inner() {
        // No history (rate 0) ⇒ nothing to predict ⇒ exactly the inner band law.
        let c = cfg();
        let law = PredictiveGrow { inner: BandLaw, lookahead_secs: 60.0 };
        for (ws, lim) in [(800 * MI, GI), (950 * MI, GI), (200 * MI, 2 * GI), (0, 0), (16 * GI, 16 * GI)] {
            assert_eq!(decide_with_rate(&law, ws, ws, lim, &c, 0), decide(ws, lim, &c), "ws={ws} lim={lim}");
        }
    }

    #[test]
    fn predictive_grow_preempts_a_rising_burst_while_in_band() {
        // util 0.78 (in-band → plain BandLaw HOLDS), but a +2Mi/s rate predicts
        // the working set crossing the grow edge within the lookahead → grow NOW.
        let c = cfg();
        let law = PredictiveGrow { inner: BandLaw, lookahead_secs: 60.0 };
        // plain law holds at this in-band utilization …
        assert_eq!(decide(800 * MI, GI, &c), Decision::Hold);
        // … but the predictive law grows ahead of the predicted breach.
        // predicted_ws = 800Mi + 2Mi/s·60s = 920Mi → seat at setpoint: 920Mi/0.8 = 1150Mi.
        match decide_with_rate(&law, 800 * MI, 800 * MI, GI, &c, (2 * MI) as i64) {
            Decision::Grow { from, to } => {
                assert_eq!(from, GI);
                assert_eq!(to, 1150 * MI);
                assert!(to > from, "a predictive grow raises the limit");
            }
            d => panic!("expected a predictive Grow, got {d:?}"),
        }
    }

    #[test]
    fn predictive_grow_is_still_ceiling_clamped() {
        // a runaway rate cannot breach the ceiling — the shared gate owns that.
        let c = BandConfig { ceiling_bytes: 4 * GI, ..cfg() };
        let law = PredictiveGrow { inner: BandLaw, lookahead_secs: 60.0 };
        match decide_with_rate(&law, 800 * MI, 800 * MI, GI, &c, GI as i64 /* 1Gi/s — absurd */) {
            Decision::Grow { from, to } => {
                assert_eq!(from, GI);
                assert_eq!(to, c.ceiling_bytes, "predictive grow capped at the ceiling");
            }
            d => panic!("expected a ceiling-capped Grow, got {d:?}"),
        }
    }

    #[test]
    fn predictive_grow_never_blocks_or_inverts_a_shrink() {
        // low util + a falling rate: the inner law shrinks; prediction (which only
        // ever grows) must pass the shrink through untouched.
        let c = cfg();
        let law = PredictiveGrow { inner: BandLaw, lookahead_secs: 60.0 };
        let with = decide_with_rate(&law, 200 * MI, 200 * MI, 2 * GI, &c, -(MI as i64));
        assert_eq!(with, decide(200 * MI, 2 * GI, &c));
        assert!(matches!(with, Decision::Shrink { .. }), "prediction must not block a shrink, got {with:?}");
    }

    #[test]
    fn band_config_validate_accepts_default_rejects_malformed() {
        // the shipped default is valid (it converges; it must never be rejected).
        assert!(BandConfig::default().validate().is_ok());
        // inverted band.
        assert_eq!(
            BandConfig { shrink_below: 0.90, grow_above: 0.70, ..BandConfig::default() }.validate(),
            Err(BandConfigError::BadBand)
        );
        // a grow that doesn't raise.
        assert_eq!(
            BandConfig { grow_factor: 1.0, ..BandConfig::default() }.validate(),
            Err(BandConfigError::BadFactor)
        );
        // a shrink that doesn't lower.
        assert_eq!(
            BandConfig { shrink_factor: 1.0, ..BandConfig::default() }.validate(),
            Err(BandConfigError::BadFactor)
        );
        // empty operating range.
        assert_eq!(
            BandConfig { floor_bytes: 8 << 30, ceiling_bytes: 1 << 30, ..BandConfig::default() }.validate(),
            Err(BandConfigError::EmptyRange)
        );
    }

    /// The conformance oracle EXTENDED to predictive laws: the shared safety gate
    /// must contain `PredictiveGrow` over adversarial rates (huge rising / falling)
    /// exactly as it contains every other law — never grow over the ceiling, never
    /// shrink below the safe minimum.
    #[test]
    fn safety_gate_contains_the_predictive_law() {
        let c = cfg();
        let band = PredictiveGrow { inner: BandLaw, lookahead_secs: 60.0 };
        let prop = PredictiveGrow { inner: ProportionalLaw { gain: 1.0 }, lookahead_secs: 30.0 };
        for &ws in &[0u64, 100 * MI, 800 * MI, 4 * GI, 16 * GI, 32 * GI] {
            for &limit in &[256 * MI, GI, 4 * GI, 16 * GI, 20 * GI] {
                let safe_min = (ws as f64 / c.setpoint).ceil() as u64;
                for &rate in &[i64::MIN / 2, -(GI as i64), 0, GI as i64, i64::MAX / 2] {
                    for d in [
                        decide_with_rate(&band, ws, ws, limit, &c, rate),
                        decide_with_rate(&prop, ws, ws, limit, &c, rate),
                    ] {
                        match d {
                            Decision::Grow { from, to } => {
                                assert!(to <= c.ceiling_bytes, "ws={ws} limit={limit} rate={rate}: grew over ceiling to {to}");
                                assert!(to > from, "a Grow must raise the limit");
                            }
                            Decision::Shrink { from, to } => {
                                assert!(to >= c.floor_bytes || from > c.ceiling_bytes, "shrank below floor");
                                assert!(to >= safe_min || from > c.ceiling_bytes, "ws={ws} limit={limit} rate={rate}: shrank below safe_min to {to}");
                                assert!(to < from, "a Shrink must lower the limit");
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    // ── THE NEVER-OOM-FROM-CARVE INVARIANT (the authentik-Celery fix) ──────────
    //
    // Honest tier (theory/UNREPRESENTABILITY.md): this is the C2 "external-world
    // observation" ceiling — a FUTURE working set is unknowable at compile time, so
    // the strongest HONEST guarantee is "a carve never drops the limit below the
    // DEMONSTRATED peak (max RSS over the trailing window) + the declared request
    // floor", enforced STRUCTURALLY because every control law funnels through the
    // single `safety_clamp`. These tests PROVE that much (and not more).

    /// Drive a controller's worth of ticks: fold each working-set sample into the
    /// trailing-window peak (exactly as the reconcile layer does) and run the proven
    /// `plan_tick`, applying any carve to the live limit. Returns, per tick, the
    /// `(limit, peak)` pair so callers can assert against the tick's CURRENT peak.
    fn run_sequence(samples: &[u64], mut limit: u64, c: &BandConfig, decay: f64) -> Vec<(u64, u64)> {
        let mut peak = 0u64;
        let mut trail = Vec::with_capacity(samples.len());
        for &used in samples {
            peak = update_peak(peak, used, decay);
            let o = Observation { used, peak_used: peak, capacity: limit, owners: ours(), staleness_secs: 0, memory_shrink_restart_free: false, observed_for_secs: u64::MAX, request_floor: 0, throttle_signal: 0, restarting: false };
            match plan_tick(&o, c, Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None) {
                TickPlan::Act { decision: Decision::Grow { to, .. } | Decision::Shrink { to, .. } } => limit = to,
                _ => {}
            }
            trail.push((limit, peak));
        }
        trail
    }

    /// THE NEVER-OOM INVARIANT, replicating the authentik Celery-worker failure:
    /// low, low, SPIKE, low, low. With the BUGGY instantaneous floor the limit
    /// collapsed on the post-spike low-water samples (350Mi → floor 446Mi) and the
    /// next 900Mi spike OOMed. With the peak-keyed floor + a slow decay, the limit
    /// HOLDS the demonstrated spike across the whole low-water window, so a re-spike
    /// to the same level always fits. (A near-1 decay = "the peak holds for the
    /// window"; the decay is the operator's knob for HOW LONG.)
    #[test]
    fn shrink_never_below_observed_peak_replicating_authentik() {
        let spike = 900 * MI;
        let idle = 350 * MI;
        let samples = [idle, idle, spike, idle, idle, idle, idle, idle];
        let c = cfg();
        // decay 1.0 (clamped to 0.999) = the peak effectively holds across the window:
        // the demonstrated spike never decays away within these few ticks.
        let trail = run_sequence(&samples, 2 * GI, &c, 1.0);

        // PER-TICK INVARIANT: after every tick the limit seats the CURRENT (slowly-
        // decaying) demonstrated peak — never the instantaneous low-water sample.
        // With the buggy instantaneous floor the limit would have collapsed to
        // 446Mi (=350Mi/0.8) on every idle tick; here it tracks ~900Mi/0.8.
        for (i, &(lim, peak)) in trail.iter().enumerate() {
            let floor = (peak as f64 / c.setpoint).ceil() as u64;
            assert!(lim >= floor, "tick {i}: limit {lim} < held-peak floor {floor} (the authentik OOM)");
        }
        // a subsequent re-spike to the SAME level never OOMs: it fits under the limit.
        let (final_limit, _) = *trail.last().unwrap();
        assert!(spike <= final_limit, "the re-spike {spike} must fit under the held limit {final_limit}");
        // the BUGGY behaviour is the counter-example: an instantaneous floor on the
        // idle sample would be 350Mi/0.8 = 446Mi, far below the 900Mi re-spike.
        let buggy_instantaneous_floor = (idle as f64 / c.setpoint).ceil() as u64;
        assert!(buggy_instantaneous_floor < spike, "the bug: instantaneous floor {buggy_instantaneous_floor} < re-spike {spike}");
        // the held limit is MILES above that buggy floor — proving the fix lifts it.
        assert!(final_limit > 2 * buggy_instantaneous_floor, "held limit {final_limit} must dwarf the buggy floor {buggy_instantaneous_floor}");
    }

    /// Exhaustive: for EVERY ordering of a small alphabet of working-set samples,
    /// NO shrink decision ever carves the limit below the never-OOM floor for the
    /// peak demonstrated so far (`max(ceil(peak/setpoint), floor)`). Drives the same
    /// reconcile→decide path the live loop uses, so this is a property of the
    /// SHIPPED control flow, not a unit poke. A GROW may still be climbing toward
    /// the band from below — the invariant is on the SHRINK direction (the carve
    /// that can starve a workload), which is exactly the OOM-from-carve class.
    #[test]
    fn no_shrink_ever_lands_below_the_running_demonstrated_peak_floor() {
        let c = cfg();
        let alphabet = [200 * MI, 500 * MI, 900 * MI, 1400 * MI];
        // all length-4 sequences over the alphabet (256 orderings).
        for a in alphabet {
            for b in alphabet {
                for d in alphabet {
                    for e in alphabet {
                        let samples = [a, b, d, e];
                        let mut peak = 0u64;
                        let mut limit = 2 * GI;
                        for &used in &samples {
                            // pure single-tick max (decay 0): the tightest honest claim
                            // — the floor must hold the peak at least the tick it is seen.
                            peak = update_peak(peak, used, 0.0); // = max(peak, used)
                            let floor = ((peak as f64 / c.setpoint).ceil() as u64).max(c.floor_bytes);
                            let o = Observation { used, peak_used: peak, capacity: limit, owners: ours(), staleness_secs: 0, memory_shrink_restart_free: false, observed_for_secs: u64::MAX, request_floor: 0, throttle_signal: 0, restarting: false };
                            match plan_tick(&o, &c, Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None) {
                                TickPlan::Act { decision: Decision::Shrink { to, .. } } => {
                                    assert!(
                                        to >= floor,
                                        "samples={samples:?} used={used} peak={peak}: SHRANK to {to} < demonstrated-peak floor {floor} (OOM-from-carve)"
                                    );
                                    limit = to;
                                }
                                TickPlan::Act { decision: Decision::Grow { to, .. } } => limit = to,
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    }

    /// A shrink can never carve the limit below the operator's declared
    /// `requests.<resource>` floor — requests is the scheduler's guarantee, and a
    /// limit under the request is both invalid in k8s and unsafe.
    #[test]
    fn shrink_never_below_request_floor() {
        // a low-util sample that WOULD shrink hard, but a 1Gi request floor binds.
        let c = BandConfig { request_floor_bytes: GI, ..cfg() };
        // util 0.05 @ 2Gi ⇒ BandLaw wants to shrink way down; request floor caps it.
        match decide(100 * MI, 2 * GI, &c) {
            Decision::Shrink { to, .. } => assert!(to >= GI, "shrink {to} dropped below the 1Gi request floor"),
            Decision::NoSafeShrink { .. } => {} // also acceptable (floor == limit cases)
            d => panic!("expected a request-floor-bound Shrink/NoSafeShrink, got {d:?}"),
        }
        // and the request floor composes with the peak floor (max of the two binds).
        let o = Observation { used: 100 * MI, peak_used: GI + 200 * MI, capacity: 4 * GI, owners: ours(), staleness_secs: 0, memory_shrink_restart_free: false, observed_for_secs: u64::MAX, request_floor: 0, throttle_signal: 0, restarting: false };
        if let TickPlan::Act { decision: Decision::Shrink { to, .. } } =
            plan_tick(&o, &c, Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None)
        {
            let peak_floor = ((GI + 200 * MI) as f64 / c.setpoint).ceil() as u64;
            assert!(to >= peak_floor.max(GI), "shrink {to} below max(peak_floor, request_floor)");
        }
    }

    /// Non-spiky workloads (garage / pangea-coming-soon) must STILL carve normally:
    /// the peak floor only ever RAISES the safe minimum, never lowers it, so a
    /// steadily-over-allotted band still shrinks toward the band as before.
    #[test]
    fn steady_workload_still_shrinks_into_band() {
        let c = cfg();
        // steady 600Mi working set, peak == used (no spikes), 4Gi limit ⇒ shrink.
        // Enough ticks for the gentle ×0.9 step to converge into the band; the peak
        // floor (== the steady working set's setpoint seat) never blocks this carve.
        let trail = run_sequence(&[600 * MI; 30], 4 * GI, &c, 0.97);
        let (final_limit, _) = *trail.last().unwrap();
        let util = (600 * MI) as f64 / final_limit as f64;
        assert!(util >= c.shrink_below, "a steady band must still converge into the band (util {util})");
        assert!(final_limit < 4 * GI, "a steady over-allotted band must shrink (final {final_limit})");
    }

    /// `update_peak` is monotone-safe: the folded peak is ALWAYS ≥ the current
    /// sample (so the shrink floor can never sit under the live working set) and a
    /// real spike raises it instantly + holds it across decay.
    #[test]
    fn update_peak_is_monotone_safe_and_holds_spikes() {
        // a spike instantly raises the peak.
        assert_eq!(update_peak(300 * MI, 900 * MI, 0.97), 900 * MI);
        // a subsequent low sample does NOT collapse the peak below itself; the
        // decayed prior peak still dominates for a meaningful window.
        let after_spike = update_peak(900 * MI, 350 * MI, 0.97);
        assert!(after_spike >= 350 * MI, "peak ≥ current sample");
        assert!(after_spike > 800 * MI, "a slow-decay peak holds the spike (got {after_spike})");
        // decay 0 ⇒ pure single-tick max (no memory beyond the sample).
        assert_eq!(update_peak(900 * MI, 350 * MI, 0.0), 350 * MI);
        // the peak is never below the current sample, for any decay.
        for &d in &[0.0, 0.5, 0.9, 0.999, 1.5 /* clamped */] {
            assert!(update_peak(0, 777 * MI, d) >= 777 * MI);
            assert!(update_peak(2 * GI, 100 * MI, d) >= 100 * MI);
        }
    }

    // ── PART 1: soft (memory.high) / hard (memory.max) carve semantics ─────────
    //
    // The OOM-impossible-by-construction shape: an efficiency shrink targets the
    // SOFT limit (memory.high → reclaim + throttle, never kill); the HARD limit
    // (memory.max == the k8s limit) is governed by the never-OOM peak floor ONLY
    // and is NEVER lowered for efficiency.

    /// `can_oom` is the bright line: only the HARD `memory.max` target can OOM-kill.
    #[test]
    fn carve_semantics_only_hard_can_oom() {
        assert!(CarveSemantics::Hard.can_oom());
        assert!(!CarveSemantics::Soft.can_oom());
    }

    /// The soft floor is ALWAYS ≤ the hard floor: a soft (reclaim) target may sit at
    /// the working-set setpoint, while the hard (kill) ceiling is pinned to the
    /// demonstrated PEAK setpoint. So memory.high can carve tighter than memory.max.
    #[test]
    fn soft_min_is_never_above_the_hard_safe_min() {
        let c = cfg();
        for &ws in &[100 * MI, 600 * MI, 2 * GI] {
            for &peak in &[ws, ws + 300 * MI, 4 * GI] {
                let soft = soft_min(ws, &c);
                let hard = safe_min(peak.max(ws), ws, &c);
                assert!(soft <= hard, "soft_min {soft} > hard safe_min {hard} (ws={ws} peak={peak})");
            }
        }
    }

    /// THE PART-1 INVARIANT: an efficiency shrink NEVER lowers the HARD memory.max
    /// (kill) limit — it only ever holds it (NoSafeShrink) or grows it to cover a
    /// peak — while the SOFT memory.high limit IS carved down for efficiency. This
    /// is exactly what makes a carve-induced OOM impossible: the kill ceiling is
    /// monotone-non-decreasing, so no carve can move the OOM line down under a spike.
    #[test]
    fn efficiency_shrink_carves_soft_never_lowers_hard() {
        let c = cfg();
        // util 0.20 @ 2Gi hard, 2Gi soft ⇒ BandLaw wants to shrink. Peak == used (no
        // spike history) so the hard floor is the idle setpoint, but the planner must
        // STILL refuse to lower memory.max for efficiency.
        let ws = 400 * MI;
        let plan = plan_dual_carve(&BandLaw, ws, ws, 2 * GI, Some(2 * GI), &c);
        // HARD: the kill ceiling is held — never lowered for efficiency.
        match plan.hard {
            Decision::NoSafeShrink { current } => assert_eq!(current, 2 * GI, "hard limit held"),
            d => panic!("efficiency pressure must NOT lower memory.max, got {d:?}"),
        }
        // SOFT: memory.high IS reclaimed toward the working-set setpoint.
        match plan.soft {
            Some(Decision::Shrink { from, to }) => {
                assert_eq!(from, 2 * GI);
                assert!(to < from, "memory.high reclaimed for efficiency");
                assert!(to >= soft_min(ws, &c), "soft shrink never below the soft floor");
            }
            other => panic!("efficiency pressure must reclaim memory.high, got {other:?}"),
        }
    }

    /// A GROW still raises the HARD memory.max (buying kill-ceiling headroom is the
    /// safe direction) AND the soft memory.high — neither plane is suppressed on grow.
    #[test]
    fn pressure_grows_both_planes() {
        let c = cfg();
        let ws = 950 * MI; // util 0.93 @ 1Gi ⇒ grow
        let plan = plan_dual_carve(&BandLaw, ws, ws, GI, Some(GI), &c);
        assert!(matches!(plan.hard, Decision::Grow { from: GI, .. }), "memory.max grows under pressure");
        assert!(matches!(plan.soft, Some(Decision::Grow { from: GI, .. })), "memory.high grows under pressure");
    }

    /// THE AUTHENTIK REPLAY at the soft/hard level: a worker carved on its 40%-idle
    /// reading. Under the soft/hard split, memory.max is HELD at its generous value
    /// (the kill ceiling never moves down), so an un-observed boot spike up to that
    /// ceiling cannot OOM — only memory.high (reclaim) was tightened. This is the
    /// structural guarantee the peak floor alone could not give (the spike is
    /// un-observed, so the peak is idle).
    #[test]
    fn authentik_efficiency_carve_cannot_lower_the_kill_ceiling() {
        let c = cfg();
        // the worker: idle 280Mi, but provisioned with a generous 662Mi memory.max
        // (the operator's headroom for the boot spike). metrics only ever sees idle.
        let idle = 280 * MI;
        let hard_limit = 662 * MI;
        let plan = plan_dual_carve(&BandLaw, idle, idle, hard_limit, Some(hard_limit), &c);
        // memory.max (the kill ceiling) is HELD at 662Mi — never carved to ~350Mi
        // (idle/0.8) the way the original single-limit carve did (→ the OOM).
        assert!(
            matches!(plan.hard, Decision::NoSafeShrink { current } if current == hard_limit),
            "the kill ceiling must stay at {hard_limit}, got {:?}",
            plan.hard
        );
        // a transient ~600Mi blueprint-discovery spike still fits UNDER the held
        // memory.max — so it reclaims/throttles at memory.high, never OOM-kills.
        let spike = 600 * MI;
        let held_hard = match plan.hard {
            Decision::NoSafeShrink { current } | Decision::Grow { to: current, .. } => current,
            _ => hard_limit,
        };
        assert!(spike < held_hard, "the un-observed spike {spike} must fit under the held kill ceiling {held_hard}");
    }

    /// A dimension with NO soft plane (`soft_current: None`) is hard-only — exactly
    /// the legacy single-limit behaviour, byte-identical to `decide_with`.
    #[test]
    fn no_soft_plane_is_legacy_hard_only() {
        let c = cfg();
        for (ws, lim) in [(950 * MI, GI), (800 * MI, GI), (16 * GI, 16 * GI)] {
            let plan = plan_dual_carve(&BandLaw, ws, ws, lim, None, &c);
            assert!(plan.soft.is_none(), "no soft plane ⇒ no soft decision");
            // hard is decide_with EXCEPT an efficiency shrink is suppressed (the kill
            // ceiling never lowers). For grow/hold/atceiling it is identical.
            let legacy = decide_with(&BandLaw, ws, ws, lim, &c);
            if !matches!(legacy, Decision::Shrink { .. }) {
                assert_eq!(plan.hard, legacy, "non-shrink hard plane == decide_with");
            }
        }
    }

    /// The dual planner funnels BOTH planes through the SAME `safety_clamp`: across
    /// adversarial laws + working sets, the soft target never drops below the soft
    /// floor and the hard target never drops below the safe_min (and a hard shrink
    /// is suppressed entirely for in-ceiling limits). The conformance oracle, lifted
    /// to the soft/hard split.
    #[test]
    fn dual_carve_both_planes_stay_within_their_floors() {
        struct ShrinkToZero;
        impl ControlLaw for ShrinkToZero {
            fn propose(&self, _w: u64, _l: u64, _c: &BandConfig) -> Proposal { Proposal::Target(0) }
        }
        let c = cfg();
        for &ws in &[0u64, 100 * MI, 800 * MI, 4 * GI] {
            for &hard in &[256 * MI, GI, 4 * GI] {
                for soft in [None, Some(hard)] {
                    let plan = plan_dual_carve(&ShrinkToZero, ws, ws, hard, soft, &c);
                    // hard plane: an efficiency shrink of an in-ceiling limit is refused.
                    assert!(
                        !matches!(plan.hard, Decision::Shrink { from, .. } if from <= c.ceiling_bytes),
                        "ws={ws} hard={hard}: memory.max must not shrink for efficiency, got {:?}", plan.hard
                    );
                    // soft plane (when present): a shrink never drops below the soft floor.
                    if let Some(Decision::Shrink { to, .. }) = plan.soft {
                        assert!(to >= soft_min(ws, &c), "ws={ws}: soft shrink {to} below soft floor");
                    }
                }
            }
        }
    }

    // ── PART 1 (k8s plane): the actuator-ROUTING decision ──────────────────────

    /// THE k8s-ROUTING INVARIANT: a `MemoryBand` efficiency carve routes the SHRINK
    /// to the SOFT `memory.high` (host-agent) and NEVER lowers the HARD `memory.max`
    /// (k8s `limits.memory`). The routing decision carries `hard_target = None` (hold
    /// the kill ceiling) + a soft target — the k8s-plane mirror of the dual-carve
    /// invariant, at the actuator-routing boundary the controller dispatches on.
    #[test]
    fn k8s_efficiency_carve_routes_soft_holds_hard() {
        let c = cfg();
        let ws = 400 * MI; // util 0.20 @ 2Gi ⇒ shrink pressure
        let carve = plan_k8s_memory_carve(&BandLaw, ws, ws, 2 * GI, 2 * GI, &c);
        // HARD memory.max: held — no pod-resize lowering of the kill ceiling.
        assert_eq!(carve.hard_target, None, "the kill ceiling must NOT be lowered for efficiency");
        // SOFT memory.high: reclaimed toward the working-set setpoint (host-agent write).
        match carve.soft_target {
            Some(t) => {
                assert!(t < 2 * GI, "memory.high reclaimed for efficiency");
                assert!(t >= soft_min(ws, &c), "soft target never below the soft floor");
            }
            None => panic!("an efficiency carve must dispatch a soft memory.high target"),
        }
    }

    /// A GROW routes to BOTH actuators: the HARD `memory.max` rises (buying kill-
    /// ceiling headroom is the safe direction, applied via pod-resize) AND the SOFT
    /// `memory.high` rises (dispatched to the host-agent).
    #[test]
    fn k8s_grow_routes_to_both_planes() {
        let c = cfg();
        let ws = 950 * MI; // util 0.93 @ 1Gi ⇒ grow
        let carve = plan_k8s_memory_carve(&BandLaw, ws, ws, GI, GI, &c);
        assert!(carve.hard_target.is_some_and(|t| t > GI), "memory.max grows");
        assert!(carve.soft_target.is_some_and(|t| t > GI), "memory.high grows");
    }

    /// The OOM-impossibility predicate: across every working set + live-limit pair,
    /// even under an adversarial shrink-to-zero law, the routed HARD target can NEVER
    /// be below the live `memory.max`. This is the structural never-OOM guarantee of
    /// the k8s plane — `never_lowers_kill_ceiling` holds for ALL inputs.
    #[test]
    fn k8s_carve_never_lowers_the_kill_ceiling() {
        struct ShrinkToZero;
        impl ControlLaw for ShrinkToZero {
            fn propose(&self, _w: u64, _l: u64, _c: &BandConfig) -> Proposal { Proposal::Target(0) }
        }
        let c = cfg();
        // exercise BOTH the proven default law and an adversarial shrink-to-zero law
        // through one closure, so the invariant is proven for any law (the gate owns it).
        let check = |law_name: &str, carve: K8sMemoryCarve, ws: u64, hard: u64, soft: u64| {
            assert!(
                carve.never_lowers_kill_ceiling(hard),
                "{law_name}: ws={ws} hard={hard} soft={soft} ⇒ routed hard {:?} would lower the kill ceiling",
                carve.hard_target
            );
        };
        for &ws in &[0u64, 50 * MI, 400 * MI, 950 * MI, 4 * GI, 32 * GI] {
            for &hard in &[256 * MI, GI, 2 * GI, 16 * GI] {
                for &soft in &[hard, u64::MAX] {
                    check("BandLaw", plan_k8s_memory_carve(&BandLaw, ws, ws, hard, soft, &c), ws, hard, soft);
                    check("ShrinkToZero", plan_k8s_memory_carve(&ShrinkToZero, ws, ws, hard, soft, &c), ws, hard, soft);
                }
            }
        }
    }

    /// The authentik replay at the routing boundary: an idle worker provisioned with
    /// a generous `memory.max` routes its efficiency carve to `memory.high` ONLY —
    /// the kill ceiling is held, so the un-observed boot spike fits under it and
    /// reclaims/throttles rather than OOM-killing.
    #[test]
    fn k8s_authentik_replay_holds_the_kill_ceiling() {
        let c = cfg();
        let idle = 280 * MI;
        let hard_limit = 662 * MI;
        let carve = plan_k8s_memory_carve(&BandLaw, idle, idle, hard_limit, hard_limit, &c);
        assert_eq!(carve.hard_target, None, "the kill ceiling stays at the provisioned value");
        assert!(carve.never_lowers_kill_ceiling(hard_limit));
        // an unset soft cgroup (u64::MAX) snaps down to a real soft target on tick 1.
        let unset = plan_k8s_memory_carve(&BandLaw, idle, idle, hard_limit, u64::MAX, &c);
        assert!(unset.soft_target.is_some(), "an unset memory.high snaps down to a soft target");
        assert_eq!(unset.hard_target, None, "still never lowers the kill ceiling");
    }

    /// An OVER-CEILING `memory.max` (e.g. a stale large limit above the band ceiling)
    /// IS routed a hard snap-DOWN to the ceiling — distinct from an efficiency shrink
    /// (which is suppressed). The snap-down is a correction, not an efficiency carve,
    /// and `never_lowers_kill_ceiling` still holds against the OVER-ceiling live value
    /// is false by design (a correction CAN lower an illegal over-ceiling limit) — but
    /// it never lowers an IN-ceiling limit, which is the case that matters for OOM.
    #[test]
    fn k8s_over_ceiling_limit_is_snapped_down_but_in_ceiling_is_never_lowered() {
        let c = cfg(); // ceiling 16Gi
        let over = 32 * GI;
        let carve = plan_k8s_memory_carve(&BandLaw, 8 * GI, 8 * GI, over, over, &c);
        assert_eq!(carve.hard_target, Some(c.ceiling_bytes), "over-ceiling limit snaps to the ceiling");
        // for an IN-ceiling limit the kill ceiling is never lowered (the OOM-safe case).
        let in_ceiling = plan_k8s_memory_carve(&BandLaw, 400 * MI, 400 * MI, 2 * GI, 2 * GI, &c);
        assert!(in_ceiling.never_lowers_kill_ceiling(2 * GI));
    }

    // ── PART 2: warmup-hold (closes the un-observed-boot-spike hole) ───────────

    /// The warmup gate holds a SHRINK while the workload is still warming up, and
    /// passes a GROW through untouched (buying headroom is always safe).
    #[test]
    fn warmup_holds_shrink_passes_grow() {
        let shrink = Decision::Shrink { from: 2 * GI, to: GI };
        assert_eq!(
            clamp_to_warmup(shrink, 60, 600),
            Decision::Warmup { current: 2 * GI, observed_for: 60, warmup: 600 }
        );
        // a grow during warmup is NEVER held.
        let grow = Decision::Grow { from: GI, to: 2 * GI };
        assert_eq!(clamp_to_warmup(grow, 60, 600), grow);
        // past warmup ⇒ the shrink passes through.
        assert_eq!(clamp_to_warmup(shrink, 700, 600), shrink);
        // warmup disabled (0) ⇒ the shrink passes through.
        assert_eq!(clamp_to_warmup(shrink, 1, 0), shrink);
        // exactly at the boundary ⇒ past warmup (>=).
        assert_eq!(clamp_to_warmup(shrink, 600, 600), shrink);
    }

    /// THE PART-2 PROPERTY (the authentik fix): a workload restarted LESS than
    /// `warmup_seconds` ago is NEVER shrunk, regardless of how low its observed
    /// utilization is. Drives the real `plan_tick`, so it is a property of the
    /// shipped control flow.
    #[test]
    fn warmup_workload_is_never_shrunk_no_matter_how_idle() {
        let c = cfg(); // warmup_seconds 600
        // TRUSTED idle readings (used ≥ 1). A `used == 0` reading is the
        // metric-trust gate's domain (untrusted → Hold/restore, also never
        // shrinks) — covered by `a_zero_reading_never_shrinks`, not warmup.
        for &used in &[1u64, 10 * MI, 100 * MI, 350 * MI] {
            for &observed_for in &[0u64, 1, 60, 300, 599] {
                let o = Observation {
                    used, peak_used: used, capacity: 2 * GI, owners: ours(),
                    staleness_secs: 0, memory_shrink_restart_free: false, observed_for_secs: observed_for, request_floor: 0, throttle_signal: 0, restarting: false,
                };
                let plan = plan_tick(&o, &c, Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None);
                assert!(
                    matches!(plan, TickPlan::Warmup { .. }),
                    "used={used} observed_for={observed_for}: a warming-up workload must HOLD (never shrink), got {plan:?}"
                );
                // and CRUCIALLY: never an Act/Shrink.
                assert!(!matches!(plan, TickPlan::Act { decision: Decision::Shrink { .. } }), "must never carve during warmup");
            }
        }
        // past warmup, the same idle workload DOES shrink (the gate only delays).
        let warm = Observation {
            used: 100 * MI, peak_used: 100 * MI, capacity: 2 * GI, owners: ours(),
            staleness_secs: 0, memory_shrink_restart_free: false, observed_for_secs: 601, request_floor: 0, throttle_signal: 0, restarting: false,
        };
        assert!(
            matches!(plan_tick(&warm, &c, Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None), TickPlan::Act { decision: Decision::Shrink { .. } }),
            "past warmup the idle workload shrinks normally"
        );
    }

    /// A GROW during warmup STILL acts — refusing to grow at boot would itself risk
    /// an OOM (the exact spike warmup is protecting against needs headroom NOW).
    #[test]
    fn warmup_never_blocks_a_grow() {
        let c = cfg();
        // util 0.93 @ 1Gi, restarted 5s ago (deep in warmup) ⇒ STILL grows.
        let booting = Observation {
            used: 950 * MI, peak_used: 950 * MI, capacity: GI, owners: ours(),
            staleness_secs: 0, memory_shrink_restart_free: false, observed_for_secs: 5, request_floor: 0, throttle_signal: 0, restarting: false,
        };
        assert!(
            matches!(plan_tick(&booting, &c, Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None), TickPlan::Act { decision: Decision::Grow { .. } }),
            "a grow at boot must still act (the spike needs headroom)"
        );
    }

    /// THE AUTHENTIK SEQUENCE end-to-end through warmup + the peak floor: the worker
    /// boots idle, a transient spike arrives DURING the warmup window, and the band
    /// must never have carved during the idle warmup phase — so when the spike lands
    /// it folds into the peak (raising the never-OOM floor) before any shrink is ever
    /// permitted. The combination (warmup-hold + peak-floor) closes the hole.
    #[test]
    fn authentik_warmup_never_carves_before_the_boot_spike_is_seen() {
        let c = cfg();
        // ticks every ~60s; the boot spike lands at t≈120s, still inside warmup(600).
        // samples: idle, idle, SPIKE(900Mi), idle, idle — observed_for grows 0,60,120,…
        let samples = [(0u64, 280 * MI), (60, 280 * MI), (120, 900 * MI), (180, 280 * MI), (240, 280 * MI)];
        let mut limit = 2 * GI;
        let mut peak = 0u64;
        let mut ever_shrank_before_spike = false;
        for (i, &(observed_for, used)) in samples.iter().enumerate() {
            peak = update_peak(peak, used, 0.99);
            let o = Observation {
                used, peak_used: peak, capacity: limit, owners: ours(),
                staleness_secs: 0, memory_shrink_restart_free: false, observed_for_secs: observed_for, request_floor: 0, throttle_signal: 0, restarting: false,
            };
            match plan_tick(&o, &c, Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None) {
                TickPlan::Act { decision: Decision::Shrink { to, .. } } => { if i < 2 { ever_shrank_before_spike = true; } limit = to; }
                TickPlan::Act { decision: Decision::Grow { to, .. } } => limit = to,
                TickPlan::Warmup { .. } => {} // held — the correct behaviour during warmup
                _ => {}
            }
        }
        // during the idle warmup phase (before the spike) the band NEVER shrank …
        assert!(!ever_shrank_before_spike, "must hold (never shrink) during the idle warmup phase");
        // … so the limit never dropped below the spike, and a re-spike fits.
        assert!(900 * MI <= limit, "the boot spike {} must fit under the held limit {limit}", 900 * MI);
    }

    // ── CPU-BLINDNESS / NO-STARVE INVARIANT (the pangea-operator 2026-06 fix) ─────

    /// An observation with a live throttle signal (and/or restarting), keyed on a
    /// peak == used (no history) — models a CFS-capped CPU workload whose usage can
    /// never exceed its limit.
    fn obs_throttled(used: u64, cap: u64, throttle: u64, restarting: bool) -> Observation {
        Observation {
            used, peak_used: used, capacity: cap, owners: ours(),
            staleness_secs: 0, memory_shrink_restart_free: false, observed_for_secs: u64::MAX,
            request_floor: 0, throttle_signal: throttle, restarting,
        }
    }

    /// THE CPU-BLINDNESS PROPERTY: for ANY `(limit, observed_usage ≤ limit,
    /// throttled = true)`, the carve is NEVER a `Shrink`. This is the structural
    /// no-starve invariant — a CFS-throttled workload's observed usage is hard-capped
    /// at its limit, so a usage-keyed shrink would ratchet a bursty/idle workload to
    /// the floor and starve it (exactly what happened to pangea-operator). Exhaustive
    /// over a wide grid of (usage, limit) with usage ≤ limit, the canonical capped case.
    #[test]
    fn throttled_workload_is_never_shrunk_for_any_usage_at_or_below_limit() {
        let c = cfg();
        let limits = [256 * MI, 500 * MI, GI, 2 * GI, 4 * GI, 8 * GI];
        for &limit in &limits {
            // every usage from 0 up to the limit in fine steps (CFS caps usage ≤ limit).
            for num in 0..=20u64 {
                let used = (limit / 20) * num; // 0%, 5%, …, 100% of the cap
                for &throttle in &[1u64, 5, 100] {
                    for &restarting in &[false, true] {
                        let o = obs_throttled(used, limit, throttle, restarting);
                        let plan = plan_tick(&o, &c, Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None);
                        // the carve must NEVER be a shrink — neither an Act(Shrink) nor a
                        // cooldown'd/stale Shrink decision; the only permitted outcomes are
                        // a hold (Throttled / Observe) or a GROW.
                        assert!(
                            !matches!(plan,
                                TickPlan::Act { decision: Decision::Shrink { .. } }
                                | TickPlan::Cooldown { decision: Decision::Shrink { .. } }
                                | TickPlan::Stale { decision: Decision::Shrink { .. }, .. }),
                            "throttled workload SHRANK (the CPU-starve ratchet): used={used} limit={limit} throttle={throttle} restarting={restarting} → {plan:?}"
                        );
                    }
                }
            }
        }
    }

    /// Even with the throttle gate DISABLED (`throttle_signal` 0) but `restarting`,
    /// a recently-restarted / crash-looping workload is never shrunk either — the
    /// no-starve invariant covers the crash-loop case the operator named.
    #[test]
    fn restarting_workload_is_never_shrunk_no_matter_how_idle() {
        let c = cfg();
        for &used in &[0u64, 10 * MI, 100 * MI, 400 * MI] {
            let o = obs_throttled(used, 2 * GI, 0, true); // idle, no live throttle, but restarting
            let plan = plan_tick(&o, &c, Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None);
            assert!(
                !matches!(plan, TickPlan::Act { decision: Decision::Shrink { .. } }),
                "a restarting workload must never be shrunk: used={used} → {plan:?}"
            );
        }
    }

    /// THE OPERATOR CASE, reproduced: an IDLE observed usage (1m ≈ the cap's noise
    /// floor) with ACTIVE CFS throttling — exactly pangea-operator's idle-between-
    /// bursts profile that breathe carved 1000m→283m. The band must (a) NEVER shrink
    /// below the throttle-derived demand floor, and (b) under SUSTAINED throttle,
    /// GROW (climb out of the cap). This is the regression test for the live incident.
    #[test]
    fn operator_case_idle_usage_with_throttle_grows_and_never_starves() {
        // cpu band semantics: scalars are millicores. limit 1000m, observed idle ~1m,
        // but the workload is being THROTTLED during its plan burst.
        let cpu_cfg = BandConfig { floor_bytes: 100, ceiling_bytes: 4000, ..BandConfig::default() };
        let limit = 1000u64; // 1000m, the pre-incident limit
        let o = obs_throttled(1, limit, /*throttle*/ 5, false); // ~1m idle, actively throttled
        match plan_tick(&o, &cpu_cfg, Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None) {
            // the throttle lifts demand above the cap ⇒ the band law GROWS (climb out).
            TickPlan::Act { decision: Decision::Grow { from, to } } => {
                assert_eq!(from, limit);
                assert!(to > limit, "sustained throttle must GROW the cpu limit, got {to}");
            }
            // a Throttled hold (if the demand-lift somehow didn't reach the grow edge)
            // is also acceptable — the invariant is "never shrink", and a hold honors it.
            TickPlan::Throttled { current, .. } => assert_eq!(current, limit),
            p => panic!("operator case must grow or hold (never shrink/starve), got {p:?}"),
        }

        // SUSTAINED throttle over many ticks: the limit must MONOTONICALLY climb out
        // of the cap, never ratchet down — the opposite of the incident's 1000→283.
        let mut lim = limit;
        for _ in 0..20 {
            let o = obs_throttled(1, lim, 5, false); // still idle-but-throttled at the new cap
            match plan_tick(&o, &cpu_cfg, Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None) {
                TickPlan::Act { decision: Decision::Grow { to, .. } } => { assert!(to >= lim); lim = to; }
                TickPlan::Observe { decision: Decision::AtCeiling { .. } } | TickPlan::Throttled { .. } => break,
                p => panic!("sustained throttle must only ever grow or hold, got {p:?}"),
            }
        }
        assert!(lim >= limit, "the limit must never drop below where it started under sustained throttle (got {lim})");
        assert!(lim > limit, "sustained throttle should have GROWN the limit out of the cap (got {lim})");
    }

    /// CONTRAST: the SAME idle reading WITHOUT a throttle signal DOES shrink (the
    /// legacy behaviour) — proving the throttle signal is exactly what holds the
    /// shrink. This is the before/after that pins the fix to the new input.
    #[test]
    fn idle_without_throttle_still_shrinks_proving_the_signal_is_load_bearing() {
        let c = cfg();
        // idle @ 2Gi, NO throttle, past warmup ⇒ the legacy idle shrink.
        let o = obs_throttled(100 * MI, 2 * GI, 0, false);
        assert!(
            matches!(plan_tick(&o, &c, Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None), TickPlan::Act { decision: Decision::Shrink { .. } }),
            "without a throttle signal an idle workload still shrinks (the signal is load-bearing)"
        );
    }

    /// A GROW under throttle ALWAYS acts — relieving a throttle by buying headroom is
    /// the safe direction; the gate only ever holds a SHRINK.
    #[test]
    fn throttle_never_blocks_a_grow() {
        let c = cfg();
        // high util @ 1Gi + throttle ⇒ a grow, never held.
        let o = obs_throttled(950 * MI, GI, 5, false);
        assert!(
            matches!(plan_tick(&o, &c, Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH, None), TickPlan::Act { decision: Decision::Grow { .. } }),
            "a grow under throttle must still act"
        );
    }

    // ── the pure throttle helpers ────────────────────────────────────────────

    #[test]
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn throttled_demand_lifts_above_the_limit_only_when_signalled() {
        let c = cfg();
        // no signal ⇒ None (byte-identical to before).
        assert_eq!(throttled_demand(100 * MI, GI, 0, false, &c), None);
        // a throttle signal ⇒ demand strictly above the current limit (one grow step).
        let d = throttled_demand(1, GI, 5, false, &c).expect("throttled ⇒ Some demand");
        assert!(d > GI, "throttled demand {d} must exceed the limit {GI} (so the floor refuses a shrink + the law grows)");
        assert_eq!(d, (GI as f64 * c.grow_factor).ceil() as u64, "one grow-step above the cap");
        // restarting alone (no live throttle) still lifts just above the cap.
        let r = throttled_demand(1, GI, 0, true, &c).expect("restarting ⇒ Some demand");
        assert!(r > GI);
    }

    #[test]
    fn clamp_to_throttle_holds_a_shrink_passes_grow_and_hold() {
        // a shrink under throttle ⇒ held as a typed Throttled.
        assert_eq!(
            clamp_to_throttle(Decision::Shrink { from: GI, to: 800 * MI }, 5, false),
            Decision::Throttled { current: GI, restarting: false }
        );
        // restarting flag is propagated.
        assert_eq!(
            clamp_to_throttle(Decision::Shrink { from: GI, to: 800 * MI }, 0, true),
            Decision::Throttled { current: GI, restarting: true }
        );
        // a grow passes through untouched.
        assert_eq!(
            clamp_to_throttle(Decision::Grow { from: GI, to: 2 * GI }, 5, false),
            Decision::Grow { from: GI, to: 2 * GI }
        );
        // no signal ⇒ no-op (behaviour-preserving).
        assert_eq!(
            clamp_to_throttle(Decision::Shrink { from: GI, to: 800 * MI }, 0, false),
            Decision::Shrink { from: GI, to: 800 * MI }
        );
        // a hold is untouched.
        assert_eq!(clamp_to_throttle(Decision::Hold, 5, false), Decision::Hold);
    }

    /// The throttle lift flows through the EXISTING `safe_min` floor: a throttled
    /// workload's effective demand raises `safe_min` ABOVE the limit, so the proven
    /// `safety_clamp` itself (not just the gate) refuses any shrink. Proves the fix
    /// is "a non-blind input to the proven gate", not a new safety path.
    #[test]
    fn throttle_lift_raises_safe_min_above_the_limit_so_the_proven_clamp_refuses_a_shrink() {
        let c = cfg();
        let limit = GI;
        let demand = throttled_demand(1, limit, 5, false, &c).unwrap();
        // safe_min keyed on the lifted demand-as-peak is ≥ the limit ⇒ no shrink room.
        let sm = safe_min(demand, demand, &c);
        assert!(sm >= limit, "lifted safe_min {sm} must be ≥ the limit {limit} (the floor itself refuses a shrink)");
        // and the proven safety_clamp turns an aggressive shrink proposal into a grow/hold.
        let d = safety_clamp(Proposal::Target(1), demand, demand, limit, &c);
        assert!(!matches!(d, Decision::Shrink { .. }), "the proven clamp must not produce a shrink under the lift, got {d:?}");
    }
}
