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

/// Tunable band/step policy. Every knob is config-driven (a `MemoryBand` CR's
/// spec → the watcher's args). Defaults encode the 80/20 setpoint with a
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
        }
    }
}

/// The outcome of one band evaluation for one target. Every non-`Hold` variant
/// is observable (the watcher emits a typed event) so a tick's behaviour is
/// fully legible in the logs.
#[derive(Debug, PartialEq, Eq)]
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
}

/// The SHARED safety gate: turn any law's raw proposal into a SAFE typed
/// [`Decision`]. A grow is clamped to the ceiling (→ `AtCeiling` if no room); a
/// shrink is clamped UP to `max(working_set/setpoint, floor)` — so live pages
/// can never be pushed over the band (the never-OOM proof) and a shrink never
/// overshoots into grow territory (→ `NoSafeShrink` if no safe room). Every
/// control law funnels through here; the proof holds for all of them.
#[must_use]
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn safety_clamp(
    proposal: Proposal,
    working_set: u64,
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
            let safe_min = (working_set as f64 / cfg.setpoint).ceil() as u64;
            let to = raw.max(safe_min).max(cfg.floor_bytes);
            if to >= current_limit {
                Decision::NoSafeShrink { current: current_limit }
            } else {
                Decision::Shrink { from: current_limit, to }
            }
        }
        Proposal::Target(_) => Decision::Hold, // raw == current_limit
    }
}

/// Run a control law through the universal safety scaffolding: floor-seed /
/// ceiling-snap (independent of the law; also the unset-limit guard) → the law's
/// proposal → [`safety_clamp`]. This is the one place a law's output becomes a
/// safe [`Decision`].
#[must_use]
pub fn decide_with<L: ControlLaw>(
    law: &L,
    working_set: u64,
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
    safety_clamp(law.propose(working_set, current_limit, cfg), working_set, current_limit, cfg)
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
/// default. Shrink can never push a workload toward OOM by construction (the
/// gate clamps to `working_set / setpoint`).
#[must_use]
pub fn decide(working_set: u64, current_limit: u64, cfg: &BandConfig) -> Decision {
    decide_with(&BandLaw, working_set, current_limit, cfg)
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
    pub capacity: u64,
    pub owners: Vec<FieldOwner>,
    /// Age of the metric sample driving `used`, in seconds. A scrape gap that
    /// returns a stale/zero `used` is indistinguishable from a real reading
    /// without this — so the loop refuses to mutate when it exceeds the bound.
    pub staleness_secs: u64,
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
) -> TickPlan {
    if let Some(manager) = competing_field_manager(&obs.owners, our_manager, our_field) {
        return TickPlan::Conflict { manager };
    }
    let decision = clamp_to_directionality(decide(obs.used, obs.capacity, cfg), dir);
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
        Observation { used, capacity: cap, owners, staleness_secs: 0 }
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
            plan_tick(&obs(950 * MI, GI, owners), &cfg(), Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH),
            TickPlan::Conflict { manager: "vpa".into() }
        );
    }

    #[test]
    fn plan_acts_when_mutation_and_not_in_cooldown() {
        match plan_tick(&obs(950 * MI, GI, ours()), &cfg(), Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH) {
            TickPlan::Act { decision: Decision::Grow { .. } } => {}
            p => panic!("expected Act(Grow), got {p:?}"),
        }
    }

    #[test]
    fn plan_defers_mutation_in_cooldown() {
        match plan_tick(&obs(950 * MI, GI, ours()), &cfg(), Directionality::Bidirectional, true, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH) {
            TickPlan::Cooldown { decision: Decision::Grow { .. } } => {}
            p => panic!("expected Cooldown(Grow), got {p:?}"),
        }
    }

    #[test]
    fn plan_observes_hold_without_mutation() {
        assert_eq!(
            plan_tick(&obs(800 * MI, GI, ours()), &cfg(), Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH),
            TickPlan::Observe { decision: Decision::Hold }
        );
    }

    #[test]
    fn plan_observes_growonly_shrink_as_nosafeshrink() {
        // storage-like: util 0.20 would Shrink, but GrowOnly turns it into an
        // observable NoSafeShrink — one band law, no storage-specific path.
        match plan_tick(&obs(200 * MI, GI, ours()), &cfg(), Directionality::GrowOnly, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH) {
            TickPlan::Observe { decision: Decision::NoSafeShrink { .. } } => {}
            p => panic!("expected Observe(NoSafeShrink), got {p:?}"),
        }
    }

    #[test]
    fn plan_refuses_to_mutate_on_stale_metric() {
        // util 0.95 would Act(Grow), but a sample older than the bound must never
        // carve — the never-OOM proof holds only on a fresh metric.
        let stale = Observation { used: 950 * MI, capacity: GI, owners: ours(), staleness_secs: 120 };
        match plan_tick(&stale, &cfg(), Directionality::Bidirectional, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH) {
            TickPlan::Stale { staleness_secs: 120, decision: Decision::Grow { .. } } => {}
            p => panic!("expected Stale(Grow), got {p:?}"),
        }
    }

    #[test]
    fn plan_observeonly_never_mutates() {
        // a replica-like ObserveOnly dim: even a strong grow signal yields no write.
        match plan_tick(&obs(950 * MI, GI, ours()), &cfg(), Directionality::ObserveOnly, false, "breathe-memory", MEMORY_LIMIT_FIELD, FRESH) {
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
        // The free `decide` == `decide_with(&BandLaw, …)`, so every band-edge
        // test above is also a behaviour-preservation test for the trait lift.
        let c = cfg();
        for (ws, lim) in [(800 * MI, GI), (950 * MI, GI), (200 * MI, GI), (0, 0), (16 * GI, 16 * GI)] {
            assert_eq!(decide(ws, lim, &c), decide_with(&BandLaw, ws, lim, &c));
        }
    }

    #[test]
    fn safety_clamp_caps_grow_at_ceiling() {
        let c = BandConfig { ceiling_bytes: 4 * GI, ..cfg() };
        // a law proposing 100Gi is capped to the ceiling
        assert_eq!(safety_clamp(Proposal::Target(100 * GI), GI, 2 * GI, &c), Decision::Grow { from: 2 * GI, to: 4 * GI });
        // growth with no room → AtCeiling, not an over-ceiling write
        assert_eq!(safety_clamp(Proposal::Target(100 * GI), GI, 4 * GI, &c), Decision::AtCeiling { current: 4 * GI });
    }

    #[test]
    fn safety_clamp_lifts_shrink_to_safe_min() {
        let c = cfg();
        let ws = 800 * MI;
        let safe_min = (ws as f64 / c.setpoint).ceil() as u64;
        match safety_clamp(Proposal::Target(1), ws, 2 * GI, &c) {
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
                    decide_with(&GrowToMax, ws, limit, &c),
                    decide_with(&ShrinkToZero, ws, limit, &c),
                    decide_with(&BandLaw, ws, limit, &c),
                    decide_with(&ProportionalLaw { gain: 1.0 }, ws, limit, &c),
                    decide_with(&ProportionalLaw { gain: 0.5 }, ws, limit, &c),
                    decide_with(&SlewLimited { inner: GrowToMax, max_step_frac: 0.25 }, ws, limit, &c),
                    decide_with(&SlewLimited { inner: ShrinkToZero, max_step_frac: 0.25 }, ws, limit, &c),
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
        match decide_with(&StepUp, 800 * MI, GI, &c) {
            Decision::Grow { from, to } => { assert_eq!(from, GI); assert_eq!(to, GI + c.floor_bytes); }
            d => panic!("expected Grow, got {d:?}"),
        }
        // and it STILL can't breach the ceiling — the shared gate owns that
        assert_eq!(decide_with(&StepUp, GI, c.ceiling_bytes, &c), Decision::AtCeiling { current: c.ceiling_bytes });
    }

    #[test]
    fn proportional_law_lands_util_at_setpoint_in_one_tick_at_full_gain() {
        let c = cfg();
        let ws = 950 * MI; // util 0.927 at 1Gi → grow
        // gain 1.0 → target the limit that lands util exactly at the setpoint
        match decide_with(&ProportionalLaw { gain: 1.0 }, ws, GI, &c) {
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
        let near = match decide_with(&ProportionalLaw { gain: 1.0 }, 870 * MI, GI, &c) {
            Decision::Grow { from, to } => to - from,
            _ => 0,
        };
        let far = match decide_with(&ProportionalLaw { gain: 1.0 }, 980 * MI, GI, &c) {
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
        match decide_with(&SlewLimited { inner: GrowToMax, max_step_frac: 0.25 }, 950 * MI, GI, &c) {
            Decision::Grow { from, to } => {
                let rise = (to - from) as f64 / from as f64;
                assert!(rise <= 0.26, "slew cap holds the per-tick rise near 25% (got {rise})");
            }
            d => panic!("expected a capped Grow, got {d:?}"),
        }
    }
}
