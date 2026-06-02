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

/// The bidirectional band law. Pure: `(working_set, current_limit, cfg) → Decision`.
///
/// Shrink can never push a workload toward OOM by construction: the target is
/// clamped to at least `working_set / setpoint`, so after a shrink the live
/// working set is ≤ `setpoint` of the new limit — i.e. the shrink only reclaims
/// *allocation headroom*, never live pages, and never lands above the
/// grow threshold (no shrink→grow flapping).
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub fn decide(working_set: u64, current_limit: u64, cfg: &BandConfig) -> Decision {
    // Hard-floor SEED/SNAP: an unset (0) or below-floor limit is grown straight
    // to the floor — independent of utilization. This is what lets breathe take
    // over a freshly-ceded field (the CNPG/Flux co-writer relinquishes
    // limits.memory → unset → breathe seeds it to the floor) and enforces the
    // floor as a hard minimum, not just a shrink clamp.
    if current_limit < cfg.floor_bytes {
        return Decision::Grow { from: current_limit, to: cfg.floor_bytes };
    }
    // Hard-ceiling SNAP: a limit above the ceiling is brought down to it (a
    // shrink — the directionality clamp turns this into NoSafeShrink for
    // grow-only dimensions, so storage never snaps down).
    if current_limit > cfg.ceiling_bytes {
        return Decision::Shrink { from: current_limit, to: cfg.ceiling_bytes };
    }
    let util = working_set as f64 / current_limit as f64;

    if util > cfg.grow_above {
        let target =
            ((current_limit as f64 * cfg.grow_factor).ceil() as u64).min(cfg.ceiling_bytes);
        if target <= current_limit {
            return Decision::AtCeiling { current: current_limit };
        }
        Decision::Grow { from: current_limit, to: target }
    } else if util < cfg.shrink_below {
        // Gentle step down, but never below the safe minimum (working_set /
        // setpoint, which lands util exactly at the setpoint) and never below
        // the floor. max() takes the *least aggressive* of the two lower
        // bounds vs the gentle step.
        let gentle = (current_limit as f64 * cfg.shrink_factor).floor() as u64;
        let safe_min = (working_set as f64 / cfg.setpoint).ceil() as u64;
        let target = gentle.max(safe_min).max(cfg.floor_bytes);
        if target >= current_limit {
            return Decision::NoSafeShrink { current: current_limit };
        }
        Decision::Shrink { from: current_limit, to: target }
    } else {
        Decision::Hold
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
}
