//! `breathe-control::replica` — the HORIZONTAL band law: how many replicas a
//! workload should run given a work-rate signal, held inside a typed count band
//! with asymmetric anti-flap, an HA floor, and spot-reclaim-driven scale-OUT.
//!
//! This is the horizontal peer of the vertical band law ([`crate::decide`]): the
//! vertical law holds a *limit* at a utilization setpoint; this law holds a
//! *replica count* at a work-rate setpoint. It owns NO I/O — every function is a
//! pure mapping from observed state to a [`ReplicaDecision`], so the whole
//! horizontal algebra is unit-testable without a cluster (the TYPED-SPEC +
//! INTERPRETER TRIPLET: typed border + pure decision + a mockable
//! [`ReplicaEnvironment`] the interpreter walks). A provider never sees this
//! config; it receives a computed target count and cannot re-decide.
//!
//! The load-bearing arithmetic is the Kubernetes HPA ratio law
//! `desiredReplicas = ceil(currentReplicas × currentMetric / targetMetric)`, with
//! the four stacked anti-flap mechanisms production HPAs layer (tolerance
//! dead-band → per-direction stabilization window → per-direction velocity cap →
//! and, above it all, the cooldown the reconcile layer applies). Two properties
//! are made structural rather than merely configured:
//!   * **memory is not a horizontal signal** — [`ReplicaSignal`] simply does not
//!     admit a memory-only arm (memory does not shed when replicas are added, the
//!     classic runaway-scale-out footgun), so the illegal signal is unrepresentable.
//!   * **a spot reclaim is a scale-OUT, not a scale-down** — a pending node
//!     reclaim (`reclaim_pending > 0`) forces [`ReplicaDecision::SpotScaleOut`]
//!     (provision the replacement set *before* the doomed pods drain, the
//!     `retirada` pre-drain) and can never resolve to a scale-in.

/// The signal a replica band scales on. Ordered by fidelity for horizontal
/// scaling (work-rate signals beat utilization). **There is no `Memory` arm on
/// purpose**: adding replicas does not reduce per-pod memory, so a memory-keyed
/// horizontal signal runs away — the illegal signal is made unrepresentable
/// rather than merely discouraged (★★ UNREPRESENTABILITY).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaSignal {
    /// A per-replica utilization RATIO already normalised against its own target
    /// basis (e.g. CPU% of request, in-flight/target concurrency). `value` is the
    /// current average per-replica utilization; `target` is the setpoint
    /// utilization. HPA ratio: `desired = ceil(current × value / target)`.
    Utilization,
    /// An ABSOLUTE total work RATE across the whole workload (requests/sec,
    /// messages/sec). `value` is the total rate; `target` is the target rate PER
    /// replica. Little's-Law sizing: `desired = ceil(value / target_per_replica)`.
    RequestRate,
    /// An ABSOLUTE backlog / queue DEPTH (pending items, lag). `value` is the total
    /// depth; `target` is the target depth PER replica. KEDA sizing:
    /// `desired = ceil(value / target_per_replica)`.
    QueueDepth,
}

impl ReplicaSignal {
    /// Stable label (catalog rendering / logging).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Utilization => "utilization",
            Self::RequestRate => "request-rate",
            Self::QueueDepth => "queue-depth",
        }
    }

    /// `true` for the ABSOLUTE-total signals (`value` is a fleet-wide total sized
    /// against a per-replica target); `false` for the per-replica RATIO signal.
    #[must_use]
    pub fn is_absolute(self) -> bool {
        matches!(self, Self::RequestRate | Self::QueueDepth)
    }

    /// The METRIC ratio `currentMetric / targetMetric` — the value the tolerance
    /// dead-band is applied to (exactly like the HPA, which skips any action when
    /// this is within `tolerance` of `1.0`). For the ratio signal it is
    /// `value / target`; for an absolute signal it is the per-replica load
    /// `(value / current) / target`. Gating on THIS (not the post-`ceil` replica
    /// ratio) is load-bearing: a 2.5% metric drift must not read as a 20% replica
    /// drift merely because `ceil` rounded the raw target up. `current == 0` with
    /// any work is `+∞` (must scale up); an absent denominator or empty workload
    /// is `1.0` (in-band → hold).
    #[must_use]
    pub fn metric_ratio(self, current: u32, value: f64, target: f64) -> f64 {
        if !value.is_finite() || value < 0.0 || target <= 0.0 {
            return 1.0;
        }
        match self {
            Self::Utilization => value / target,
            Self::RequestRate | Self::QueueDepth => {
                if current == 0 {
                    if value > 0.0 { f64::INFINITY } else { 1.0 }
                } else {
                    (value / f64::from(current)) / target
                }
            }
        }
    }

    /// The RAW HPA desired-replica count (before floor/ceiling/velocity clamps).
    /// For the ratio signal: `ceil(current × value / target)`. For the absolute
    /// signals: `ceil(value / target_per_replica)`. `target ≤ 0` (no denominator)
    /// or a non-finite `value` yields `current` (hold — the reconcile layer has
    /// already refused a band with no target at parse time). The result is capped
    /// at [`MAX_REPLICAS`] so a pathological signal can never overflow the clamp.
    #[must_use]
    pub fn desired_raw(self, current: u32, value: f64, target: f64) -> u32 {
        if !(value.is_finite()) || value < 0.0 || target <= 0.0 {
            return current;
        }
        let raw = match self {
            Self::Utilization => f64::from(current) * (value / target),
            Self::RequestRate | Self::QueueDepth => value / target,
        };
        if !raw.is_finite() || raw < 0.0 {
            return current;
        }
        let ceiled = raw.ceil();
        if ceiled >= f64::from(MAX_REPLICAS) {
            MAX_REPLICAS
        } else {
            // safe: 0 ≤ ceiled < MAX_REPLICAS ≤ u32::MAX.
            ceiled as u32
        }
    }
}

/// A hard upper backstop on any computed replica count — no real workload band
/// approaches it, and it keeps every `f64 → u32` conversion in range.
pub const MAX_REPLICAS: u32 = 100_000;

/// The default HA floor: 2 replicas. A single replica tolerates NO disruption
/// (a node drain / spot reclaim / rolling update = downtime, and a 1-replica
/// Deployment + PDB actively blocks node drains), so floor 1 is an availability
/// anti-pattern for any real service. For workloads that must stay HA *during*
/// maintenance too (survive a disruption while still serving with 2), set
/// [`ReplicaBandConfig::ha_floor`] to 3.
pub const DEFAULT_FLOOR: u32 = 2;

/// The typed HORIZONTAL band configuration — the replica peer of
/// [`crate::BandConfig`]. Every field is config-driven (a `ReplicaBand` CR's
/// spec). Defaults encode the fleet posture: HA floor 2, react fast up / hold
/// sticky down (asymmetric anti-flap).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReplicaBandConfig {
    /// The at-rest HA floor — never scale below this many replicas. Default 2.
    pub floor: u32,
    /// A stronger during-maintenance HA floor (e.g. 3), if the workload must
    /// survive one disruption while still serving. `None` ⇒ `floor` is the only
    /// floor. When `Some`, the effective floor is `max(floor, ha_floor)`.
    pub ha_floor: Option<u32>,
    /// Never scale above this many replicas (the L2 wall).
    pub ceiling: u32,
    /// Which signal drives scaling.
    pub signal: ReplicaSignal,
    /// The setpoint: target per-replica utilization (for [`ReplicaSignal::Utilization`])
    /// or target work PER replica (for the absolute signals).
    pub target: f64,
    /// SCALE-UP dead-band: scale up only when the metric ratio exceeds `1 +
    /// tolerance_up`. Small by default (react fast to spikes). Default 0.10.
    pub tolerance_up: f64,
    /// SCALE-DOWN dead-band: scale down only when the metric ratio drops below `1 -
    /// tolerance_down`. Large by default (resist churn on the way down). Default 0.20.
    pub tolerance_down: f64,
    /// Velocity cap UP: at most `max(max_scale_up_pods, current × max_scale_up_pct
    /// / 100)` replicas added per tick. Default 100% or 4 pods (the HPA default
    /// upper velocity).
    pub max_scale_up_pct: u32,
    pub max_scale_up_pods: u32,
    /// Velocity cap DOWN: at most `max(max_scale_down_pods, current ×
    /// max_scale_down_pct / 100)` replicas removed per tick. Default 10% (gentle,
    /// avoids a cliff). `max_scale_down_pods` defaults to 1.
    pub max_scale_down_pct: u32,
    pub max_scale_down_pods: u32,
}

impl Default for ReplicaBandConfig {
    fn default() -> Self {
        Self {
            floor: DEFAULT_FLOOR,
            ha_floor: None,
            ceiling: 10,
            signal: ReplicaSignal::Utilization,
            target: 0.80,
            // asymmetric: small up (react fast), large down (resist churn).
            tolerance_up: 0.10,
            tolerance_down: 0.20,
            max_scale_up_pct: 100,
            max_scale_up_pods: 4,
            max_scale_down_pct: 10,
            max_scale_down_pods: 1,
        }
    }
}

/// Why a [`ReplicaBandConfig`] / observation is rejected at the parse boundary —
/// the typed gate that keeps a malformed horizontal band out of the loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaError {
    /// `floor > ceiling` — an empty operating range.
    EmptyRange,
    /// `ceiling == 0` — a band that can never run.
    ZeroCeiling,
    /// `target ≤ 0` — no denominator for the ratio law.
    NoDenominator,
    /// The observed signal value is negative or non-finite (a broken metric).
    BadSignal,
    /// The environment could not read a required input (metric / replica count).
    Unreadable(&'static str),
}

impl std::fmt::Display for ReplicaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyRange => f.write_str("floor must be ≤ ceiling"),
            Self::ZeroCeiling => f.write_str("ceiling must be ≥ 1"),
            Self::NoDenominator => f.write_str("target must be > 0"),
            Self::BadSignal => f.write_str("signal value must be finite and ≥ 0"),
            Self::Unreadable(what) => write!(f, "environment could not read {what}"),
        }
    }
}

impl std::error::Error for ReplicaError {}

impl ReplicaBandConfig {
    /// The effective floor: the stronger of `floor` and `ha_floor` (if set).
    #[must_use]
    pub fn effective_floor(&self) -> u32 {
        match self.ha_floor {
            Some(h) => h.max(self.floor),
            None => self.floor,
        }
    }

    /// Parse-time validation — a malformed band is a typed error, never a silent
    /// wrong scale (★★ UNREPRESENTABILITY: parse-time-rejected).
    ///
    /// # Errors
    /// [`ReplicaError::ZeroCeiling`] / [`ReplicaError::EmptyRange`] /
    /// [`ReplicaError::NoDenominator`] when the respective invariant is violated.
    pub fn validate(&self) -> Result<(), ReplicaError> {
        if self.ceiling == 0 {
            return Err(ReplicaError::ZeroCeiling);
        }
        if self.effective_floor() > self.ceiling {
            return Err(ReplicaError::EmptyRange);
        }
        if self.target <= 0.0 || !self.target.is_finite() {
            return Err(ReplicaError::NoDenominator);
        }
        Ok(())
    }
}

/// One observed tick of horizontal state — the pure inputs to [`decide_replicas`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReplicaObservation {
    /// The workload's current `.spec.replicas`.
    pub current_replicas: u32,
    /// The current signal reading (a per-replica ratio, or an absolute total —
    /// per the config's [`ReplicaSignal`]).
    pub signal_value: f64,
    /// The MAX raw-desired count seen over the trailing scale-down stabilization
    /// window (the reconcile layer folds it forward). A scale-DOWN takes the
    /// highest recommendation over the window so a momentary dip cannot trigger a
    /// scale-in (the HPA `scaleDown.stabilizationWindowSeconds` mechanism). `None`
    /// ⇒ no window memory (act on the instantaneous value).
    pub window_max_desired: Option<u32>,
    /// A pending node/spot reclaim: this many of the workload's replicas are about
    /// to be lost when a reclaimed node drains. Non-zero forces a scale-OUT
    /// (provision the replacement set first — the `retirada` pre-drain) and
    /// suppresses any scale-down this tick.
    pub reclaim_pending: u32,
}

impl ReplicaObservation {
    /// A plain reactive observation (no window memory, no reclaim pending).
    #[must_use]
    pub fn reactive(current_replicas: u32, signal_value: f64) -> Self {
        Self { current_replicas, signal_value, window_max_desired: None, reclaim_pending: 0 }
    }
}

/// The typed outcome of one horizontal tick — the replica peer of
/// [`crate::Decision`]. Carries `from`/`to` so the actuator + status render the
/// exact transition; a `to == from` case is a `Hold`/`AtFloor`/`AtCeiling`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaDecision {
    /// Inside the dead-band (or window-held) — do nothing.
    Hold { current: u32 },
    /// Scale OUT to `to` replicas (react-fast direction).
    ScaleUp { from: u32, to: u32 },
    /// Scale IN to `to` replicas (churn-resistant direction; window-stabilised).
    ScaleDown { from: u32, to: u32 },
    /// Would scale in, but the effective HA floor binds.
    AtFloor { current: u32 },
    /// Would scale out, but the ceiling binds.
    AtCeiling { current: u32 },
    /// A node/spot reclaim is pending: pre-emptively scale OUT to `to` (covering
    /// the `reclaim` replicas about to be lost) BEFORE the doomed pods drain, so
    /// the shed load lands on already-warm capacity, never a cold-start hole. Never
    /// resolves to a scale-in while a reclaim is pending.
    SpotScaleOut { from: u32, to: u32, reclaim: u32 },
}

impl ReplicaDecision {
    /// The target replica count this decision wants applied (the value the
    /// actuator SSA-writes to `.spec.replicas`). For the no-op decisions it is the
    /// current count, so a caller can uniformly `assign(target())` and the write
    /// no-ops when nothing changes.
    #[must_use]
    pub fn target(self) -> u32 {
        match self {
            Self::Hold { current } | Self::AtFloor { current } | Self::AtCeiling { current } => current,
            Self::ScaleUp { to, .. } | Self::ScaleDown { to, .. } | Self::SpotScaleOut { to, .. } => to,
        }
    }

    /// The replica count this decision started FROM (the observed `.spec.replicas`).
    /// Uniform across every arm — the carve arms carry `from`, the no-op arms carry
    /// `current` — so a caller can render the `from -> to` transition without
    /// re-reading the observation.
    #[must_use]
    pub fn current(self) -> u32 {
        match self {
            Self::Hold { current } | Self::AtFloor { current } | Self::AtCeiling { current } => current,
            Self::ScaleUp { from, .. } | Self::ScaleDown { from, .. } | Self::SpotScaleOut { from, .. } => from,
        }
    }

    /// `true` when this decision mutates the replica count (a real carve).
    #[must_use]
    pub fn is_carve(self) -> bool {
        match self {
            Self::ScaleUp { from, to } | Self::ScaleDown { from, to } | Self::SpotScaleOut { from, to, .. } => {
                from != to
            }
            _ => false,
        }
    }

    /// Stable machine label (status `lastDecision` / logging).
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Hold { .. } => "Hold",
            Self::ScaleUp { .. } => "ScaleUp",
            Self::ScaleDown { .. } => "ScaleDown",
            Self::AtFloor { .. } => "AtFloor",
            Self::AtCeiling { .. } => "AtCeiling",
            Self::SpotScaleOut { .. } => "SpotScaleOut",
        }
    }
}

impl std::fmt::Display for ReplicaDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hold { current } => write!(f, "Hold@{current}"),
            Self::ScaleUp { from, to } => write!(f, "ScaleUp {from}→{to}"),
            Self::ScaleDown { from, to } => write!(f, "ScaleDown {from}→{to}"),
            Self::AtFloor { current } => write!(f, "AtFloor@{current}"),
            Self::AtCeiling { current } => write!(f, "AtCeiling@{current}"),
            Self::SpotScaleOut { from, to, reclaim } => write!(f, "SpotScaleOut {from}→{to} (reclaim {reclaim})"),
        }
    }
}

#[inline]
fn clamp(v: u32, lo: u32, hi: u32) -> u32 {
    v.max(lo).min(hi)
}

/// The pure horizontal band law: given the config + one observation, decide the
/// next replica count. The whole algorithm, unit-testable without a cluster.
///
/// Order (each step is load-bearing):
///   1. **spot reclaim first** — a pending reclaim forces a scale-OUT covering the
///      doomed replicas (never a scale-down) — the `retirada` pre-drain.
///   2. **HPA ratio** — `desired = ceil(current × metric/target)` (or the absolute
///      form) gives the raw target.
///   3. **asymmetric tolerance dead-band** — hold unless the metric ratio leaves
///      `[1 - tol_down, 1 + tol_up]` (react fast up, resist churn down).
///   4. **scale-down stabilization** — a scale-in takes `max(desired,
///      window_max_desired)` so a momentary dip cannot scale in.
///   5. **floor/ceiling clamp** — the effective HA floor and the ceiling bind.
///   6. **velocity cap** — bound the per-tick step in each direction.
#[must_use]
pub fn decide_replicas(cfg: &ReplicaBandConfig, obs: &ReplicaObservation) -> ReplicaDecision {
    let current = obs.current_replicas;
    let floor = cfg.effective_floor();
    let ceiling = cfg.ceiling.max(floor); // a mis-ordered range never inverts the clamp
    let raw = cfg.signal.desired_raw(current, obs.signal_value, cfg.target);

    // ── 1. Spot reclaim → scale-OUT, never scale-down (retirada pre-drain). ─────
    if obs.reclaim_pending > 0 {
        // Cover the replicas about to be lost, and honour a higher reactive want.
        let want = current.saturating_add(obs.reclaim_pending).max(raw);
        let to = clamp(want, floor, ceiling);
        return ReplicaDecision::SpotScaleOut { from: current, to, reclaim: obs.reclaim_pending };
    }

    // ── 2/3. Asymmetric tolerance dead-band on the METRIC ratio (pre-ceil). ─────
    // Gate on currentMetric/targetMetric exactly like the HPA — react fast up
    // (small `tolerance_up`), resist churn down (large `tolerance_down`).
    let ratio = cfg.signal.metric_ratio(current, obs.signal_value, cfg.target);
    let want_up = ratio > 1.0 + cfg.tolerance_up;
    let want_down = ratio < 1.0 - cfg.tolerance_down;

    if !want_up && !want_down {
        return ReplicaDecision::Hold { current };
    }

    if want_up {
        // ── 5. clamp, then 6. velocity cap up. ──
        let desired = clamp(raw, floor, ceiling);
        let step = cfg.max_scale_up_pods.max(current.saturating_mul(cfg.max_scale_up_pct) / 100).max(1);
        let to = desired.min(current.saturating_add(step));
        return if to > current {
            ReplicaDecision::ScaleUp { from: current, to }
        } else {
            // wanted to grow but the ceiling binds at the current count.
            ReplicaDecision::AtCeiling { current }
        };
    }

    // want_down: ── 4. stabilization (take the max over the window). ──
    let stabilized = obs.window_max_desired.map_or(raw, |w| w.max(raw));
    let desired = clamp(stabilized, floor, ceiling);
    // ── 6. velocity cap down. ──
    let step = cfg.max_scale_down_pods.max(current.saturating_mul(cfg.max_scale_down_pct) / 100).max(1);
    let to = desired.max(current.saturating_sub(step));
    if to < current {
        ReplicaDecision::ScaleDown { from: current, to }
    } else {
        // wanted to shrink but the floor (or the window/velocity) binds.
        ReplicaDecision::AtFloor { current }
    }
}

/// The side-effecting boundary the horizontal interpreter reads through — the
/// TYPED-SPEC triplet's Environment trait (the testability contract). Real impls
/// read the metric plane + the reclaim signal + the live `.spec.replicas`; tests
/// pass [`MockReplicaEnvironment`]. Sync + dependency-free, matching this crate's
/// pure-core discipline (the async k8s I/O adapter lives at the provider layer).
pub trait ReplicaEnvironment {
    /// The workload's current `.spec.replicas`.
    ///
    /// # Errors
    /// [`ReplicaError::Unreadable`] when the count cannot be read.
    fn current_replicas(&self) -> Result<u32, ReplicaError>;
    /// The current signal reading.
    ///
    /// # Errors
    /// [`ReplicaError::Unreadable`] when the metric cannot be read.
    fn signal_value(&self) -> Result<f64, ReplicaError>;
    /// The trailing scale-down window max desired (default: no window memory).
    fn window_max_desired(&self) -> Option<u32> {
        None
    }
    /// Replicas about to be lost to a pending node/spot reclaim (default: none).
    fn reclaim_pending(&self) -> u32 {
        0
    }
}

/// Walk the horizontal band's phases against a [`ReplicaEnvironment`]: validate →
/// observe → decide. The pure interpreter of the triplet — no panic, no
/// `unwrap`; every failure is a typed [`ReplicaError`] the caller surfaces.
///
/// # Errors
/// Propagates config validation errors, a bad/unreadable signal, or an unreadable
/// replica count.
pub fn interpret_replica<E: ReplicaEnvironment>(
    cfg: &ReplicaBandConfig,
    env: &E,
) -> Result<ReplicaDecision, ReplicaError> {
    // phase 1 — validate (parse-time gate).
    cfg.validate()?;
    // phase 2 — observe (through the mockable boundary).
    let current = env.current_replicas()?;
    let value = env.signal_value()?;
    if !value.is_finite() || value < 0.0 {
        return Err(ReplicaError::BadSignal);
    }
    let obs = ReplicaObservation {
        current_replicas: current,
        signal_value: value,
        window_max_desired: env.window_max_desired(),
        reclaim_pending: env.reclaim_pending(),
    };
    // phase 3 — decide (pure).
    Ok(decide_replicas(cfg, &obs))
}

/// The gate applied to a horizontal DECISION before it reaches the actuator — the
/// pure encoding of the shadow→confirm→effect lifecycle, the post-carve cooldown,
/// the `DisruptionPolicy` scale-in gate, and break-glass, with NO I/O. The async
/// controller resolves each field (`dry_run` via `Band::effective_dry_run`,
/// `scale_in_permitted` via `DisruptionPolicy::permits`, `force` via the CR's
/// break-glass) and hands it here; [`plan_replica_tick`] then decides whether — and
/// to what — `.spec.replicas` is written. Keeping the whole gate a pure input makes
/// "shadow observes but never writes / a scale-in is refused by policy" unit-testable
/// without a cluster.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReplicaGate {
    /// SHADOW: observe + attest, never write. The effective dry-run for this tick
    /// (a `ShadowConfirmEffect` band before its confirm window, an explicit
    /// `mode: shadow`, or a stale sample the caller refuses to act on).
    pub dry_run: bool,
    /// Within the post-carve cooldown window — a carve may be due but is held.
    pub in_cooldown: bool,
    /// Does the band's `DisruptionPolicy` permit a scale-IN? A scale-in sheds a pod
    /// (`RestartRequiring`); a scale-OUT is always `RestartFree` and is NEVER gated
    /// here. Under the default `restartFreeOnly` this is `false` (scale out freely,
    /// gate scale-in); set `allowRestart` to shed replicas.
    pub scale_in_permitted: bool,
    /// BREAK-GLASS: pin the count to exactly this (still floor/ceiling-clamped and
    /// still gated), bypassing the band law but not the safety envelope. `None` ⇒
    /// normal homeostasis via [`interpret_replica`].
    pub force: Option<u32>,
}

/// The pure outcome of planning one horizontal tick — the DECISION plus what the
/// actuator should do with it. The controller's async shell does the observe + the
/// SSA write; this value tells it whether (and to what) to write.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReplicaTickPlan {
    /// The band law's (or break-glass) decision for this tick.
    pub decision: ReplicaDecision,
    /// `Some(to)` ⇒ SSA-write `.spec.replicas = to`; `None` ⇒ observe only (a
    /// resting decision, or a carve withheld by shadow / cooldown / the scale-in gate).
    pub actuate: Option<u32>,
    /// A scale-IN the band law wanted but the `DisruptionPolicy` refused (a
    /// pod-shedding crossing) — surfaced as `DeferredWouldRestart`, never written.
    pub deferred: bool,
}

/// Plan one horizontal tick: run the band law (or the break-glass force), then apply
/// the shadow / cooldown / scale-in-policy gate. **Pure + I/O-free** — the caller's
/// async shell does the observe (through a real [`ReplicaEnvironment`]) and the SSA
/// write; the DECISION and the GATE live here so both are unit-testable without a
/// cluster (the TYPED-SPEC triplet's planning peer). A scale-OUT is `RestartFree` and
/// is never gated; only a scale-IN can be deferred by policy.
///
/// # Errors
/// Propagates every [`interpret_replica`] error (config invalid, bad/unreadable
/// signal, unreadable replica count) — never panics.
pub fn plan_replica_tick<E: ReplicaEnvironment>(
    cfg: &ReplicaBandConfig,
    env: &E,
    gate: ReplicaGate,
) -> Result<ReplicaTickPlan, ReplicaError> {
    // parse-time gate first — a malformed band never plans a carve.
    cfg.validate()?;
    let decision = match gate.force {
        Some(v) => {
            // break-glass: pin to the forced count, still floor/ceiling-clamped.
            let current = env.current_replicas()?;
            let floor = cfg.effective_floor();
            let ceiling = cfg.ceiling.max(floor);
            let to = v.clamp(floor, ceiling);
            if to > current {
                ReplicaDecision::ScaleUp { from: current, to }
            } else if to < current {
                ReplicaDecision::ScaleDown { from: current, to }
            } else {
                ReplicaDecision::Hold { current }
            }
        }
        None => interpret_replica(cfg, env)?,
    };

    // a scale-IN sheds a pod (RestartRequiring); it is DEFERRED when the policy
    // refuses it AND the tick is otherwise live (not shadow, not cooling down). A
    // scale-OUT / spot pre-drain never defers here.
    let is_scale_in = matches!(decision, ReplicaDecision::ScaleDown { .. });
    let deferred =
        decision.is_carve() && is_scale_in && !gate.scale_in_permitted && !gate.dry_run && !gate.in_cooldown;
    let actuate = if decision.is_carve() && !gate.dry_run && !gate.in_cooldown && !deferred {
        Some(decision.target())
    } else {
        None
    };
    Ok(ReplicaTickPlan { decision, actuate, deferred })
}

/// A canned [`ReplicaEnvironment`] for tests + shadow dry-runs — every input is a
/// field, so a test drives the interpreter with zero I/O.
#[derive(Debug, Clone, Copy, Default)]
pub struct MockReplicaEnvironment {
    pub current_replicas: u32,
    pub signal_value: f64,
    pub window_max_desired: Option<u32>,
    pub reclaim_pending: u32,
    /// Force a read failure to exercise the interpreter's typed-error path.
    pub replicas_unreadable: bool,
    pub signal_unreadable: bool,
}

impl ReplicaEnvironment for MockReplicaEnvironment {
    fn current_replicas(&self) -> Result<u32, ReplicaError> {
        if self.replicas_unreadable {
            Err(ReplicaError::Unreadable("current replicas"))
        } else {
            Ok(self.current_replicas)
        }
    }
    fn signal_value(&self) -> Result<f64, ReplicaError> {
        if self.signal_unreadable {
            Err(ReplicaError::Unreadable("signal metric"))
        } else {
            Ok(self.signal_value)
        }
    }
    fn window_max_desired(&self) -> Option<u32> {
        self.window_max_desired
    }
    fn reclaim_pending(&self) -> u32 {
        self.reclaim_pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ReplicaBandConfig {
        ReplicaBandConfig { ceiling: 50, ..Default::default() }
    }

    #[test]
    fn default_floor_is_two_for_ha() {
        assert_eq!(ReplicaBandConfig::default().floor, 2);
        assert_eq!(DEFAULT_FLOOR, 2);
    }

    #[test]
    fn hpa_ratio_scales_up_on_utilization() {
        // current 4 @ 0.9 util, target 0.8 → metric ratio 1.125 > 1.10 ⇒ grow.
        // raw = ceil(4 × 0.9/0.8) = ceil(4.5) = 5. velocity up = max(4, 4)=4 → 4+4=8,
        // so raw 5 (not the cap) wins. ScaleUp 4→5.
        let c = cfg();
        let d = decide_replicas(&c, &ReplicaObservation::reactive(4, 0.9));
        assert_eq!(d, ReplicaDecision::ScaleUp { from: 4, to: 5 });
        assert!(d.is_carve());
        assert_eq!(d.target(), 5);
    }

    #[test]
    fn tolerance_dead_band_holds_near_setpoint() {
        // current 5 @ 0.82 util, target 0.8 → ratio 1.025 ∈ [0.8, 1.1] ⇒ Hold.
        let c = cfg();
        let d = decide_replicas(&c, &ReplicaObservation::reactive(5, 0.82));
        assert_eq!(d, ReplicaDecision::Hold { current: 5 });
    }

    #[test]
    fn asymmetric_tolerance_reacts_fast_up_but_holds_small_dip() {
        // Small OVER-shoot (ratio just over 1.10) scales up…
        // current 10 @ 0.89 util, target 0.8 → raw = ceil(10×1.1125)=12, ratio 1.2 > 1.1.
        let c = cfg();
        let up = decide_replicas(&c, &ReplicaObservation::reactive(10, 0.89));
        assert!(matches!(up, ReplicaDecision::ScaleUp { from: 10, .. }));
        // Small UNDER-shoot within the 0.20 down-tolerance holds (resist churn):
        // current 10 @ 0.68, target 0.8 → raw = ceil(10×0.85)=9, ratio 0.9 > 0.8 ⇒ Hold.
        let hold = decide_replicas(&c, &ReplicaObservation::reactive(10, 0.68));
        assert_eq!(hold, ReplicaDecision::Hold { current: 10 });
    }

    #[test]
    fn scales_down_past_the_down_tolerance() {
        // current 10 @ 0.4, target 0.8 → raw = ceil(10×0.5)=5, ratio 0.5 < 0.8.
        // velocity down = max(1, 10×10%)=1 → 10-1 = 9. ScaleDown 10→9 (gentle).
        let c = cfg();
        let d = decide_replicas(&c, &ReplicaObservation::reactive(10, 0.4));
        assert_eq!(d, ReplicaDecision::ScaleDown { from: 10, to: 9 });
    }

    #[test]
    fn floor_binds_and_reports_at_floor() {
        // current 2 (at floor) @ 0.1 util → wants to shrink but floor 2 binds.
        let c = cfg();
        let d = decide_replicas(&c, &ReplicaObservation::reactive(2, 0.1));
        assert_eq!(d, ReplicaDecision::AtFloor { current: 2 });
    }

    #[test]
    fn ha_floor_overrides_base_floor() {
        // floor 2, ha_floor 3 → effective floor 3; a shrink from 3 reports AtFloor.
        let c = ReplicaBandConfig { ha_floor: Some(3), ..cfg() };
        assert_eq!(c.effective_floor(), 3);
        let d = decide_replicas(&c, &ReplicaObservation::reactive(3, 0.1));
        assert_eq!(d, ReplicaDecision::AtFloor { current: 3 });
    }

    #[test]
    fn ceiling_binds_and_reports_at_ceiling() {
        let c = ReplicaBandConfig { ceiling: 6, ..cfg() };
        // current 6 (at ceiling) @ 1.6 util → wants to grow but ceiling 6 binds.
        let d = decide_replicas(&c, &ReplicaObservation::reactive(6, 1.6));
        assert_eq!(d, ReplicaDecision::AtCeiling { current: 6 });
    }

    #[test]
    fn scale_down_stabilization_window_prevents_scale_in_on_a_dip() {
        // Instantaneous reading says shrink to 5, but the trailing window peaked at
        // 10 → stabilized max(5,10)=10 == current ⇒ AtFloor-style hold (no scale-in).
        let c = cfg();
        let obs = ReplicaObservation {
            current_replicas: 10,
            signal_value: 0.4, // raw = 5, would scale in without the window
            window_max_desired: Some(10),
            reclaim_pending: 0,
        };
        let d = decide_replicas(&c, &obs);
        assert_eq!(d, ReplicaDecision::AtFloor { current: 10 });
        // Without the window it WOULD scale in — proves the window is load-bearing.
        let no_window = decide_replicas(&c, &ReplicaObservation::reactive(10, 0.4));
        assert!(matches!(no_window, ReplicaDecision::ScaleDown { .. }));
    }

    #[test]
    fn queue_depth_sizes_by_backlog_over_target_per_replica() {
        // QueueDepth: value 100 total, target 10 per replica, current 3.
        // raw = ceil(100/10) = 10. ratio 10/3 > 1.1 → scale up (velocity: max(4,3)=4 → 3+4=7).
        let c = ReplicaBandConfig { signal: ReplicaSignal::QueueDepth, target: 10.0, ceiling: 50, ..Default::default() };
        let d = decide_replicas(&c, &ReplicaObservation::reactive(3, 100.0));
        assert_eq!(d, ReplicaDecision::ScaleUp { from: 3, to: 7 });
    }

    #[test]
    fn request_rate_is_an_absolute_total() {
        assert!(ReplicaSignal::RequestRate.is_absolute());
        assert!(ReplicaSignal::QueueDepth.is_absolute());
        assert!(!ReplicaSignal::Utilization.is_absolute());
        // 900 rps total, 100 rps/replica → raw 9.
        assert_eq!(ReplicaSignal::RequestRate.desired_raw(4, 900.0, 100.0), 9);
    }

    #[test]
    fn spot_reclaim_forces_scale_out_covering_the_doomed_replicas() {
        // current 3, 2 replicas about to be lost → provision 3+2 = 5 first.
        let c = cfg();
        let obs = ReplicaObservation { current_replicas: 3, signal_value: 0.8, window_max_desired: None, reclaim_pending: 2 };
        let d = decide_replicas(&c, &obs);
        assert_eq!(d, ReplicaDecision::SpotScaleOut { from: 3, to: 5, reclaim: 2 });
    }

    #[test]
    fn spot_reclaim_never_scales_down_even_when_idle() {
        // Idle signal (would normally scale in) but a reclaim is pending ⇒ scale OUT.
        let c = cfg();
        let obs = ReplicaObservation { current_replicas: 4, signal_value: 0.01, window_max_desired: None, reclaim_pending: 1 };
        let d = decide_replicas(&c, &obs);
        assert!(matches!(d, ReplicaDecision::SpotScaleOut { from: 4, to: 5, reclaim: 1 }));
        // never a scale-in while a reclaim is pending.
        assert!(!matches!(d, ReplicaDecision::ScaleDown { .. }));
    }

    #[test]
    fn spot_reclaim_respects_the_ceiling_best_effort() {
        // ceiling 4, current 4, reclaim 2 → cannot cover; best-effort holds at ceiling.
        let c = ReplicaBandConfig { ceiling: 4, ..cfg() };
        let obs = ReplicaObservation { current_replicas: 4, signal_value: 0.9, window_max_desired: None, reclaim_pending: 2 };
        let d = decide_replicas(&c, &obs);
        assert_eq!(d, ReplicaDecision::SpotScaleOut { from: 4, to: 4, reclaim: 2 });
        assert!(!d.is_carve()); // to == from, the actuator no-ops
    }

    #[test]
    fn velocity_cap_bounds_a_huge_scale_up_step() {
        // current 5, target-crushing signal → raw wants ~50, but velocity up =
        // max(4, 5×100%) = 5 → capped to 5+5 = 10 this tick.
        let c = cfg();
        let d = decide_replicas(&c, &ReplicaObservation::reactive(5, 8.0));
        assert_eq!(d, ReplicaDecision::ScaleUp { from: 5, to: 10 });
    }

    #[test]
    fn zero_replicas_with_work_scales_up_off_the_floor() {
        // current 0 (scaled to zero) with a backlog → scale up to the floor at least.
        let c = ReplicaBandConfig { signal: ReplicaSignal::QueueDepth, target: 10.0, floor: 2, ceiling: 50, ..Default::default() };
        let d = decide_replicas(&c, &ReplicaObservation::reactive(0, 30.0));
        // raw = 3, but velocity from 0 = max(4, 0) = 4 → min(3, 0+4)=3, floor 2 ⇒ 3.
        assert_eq!(d, ReplicaDecision::ScaleUp { from: 0, to: 3 });
    }

    // ── interpreter (the mockable Environment trait) ──────────────────────────

    #[test]
    fn interpreter_decides_through_the_mock_environment() {
        let c = cfg();
        let env = MockReplicaEnvironment { current_replicas: 4, signal_value: 0.9, ..Default::default() };
        let d = interpret_replica(&c, &env).expect("decides");
        assert_eq!(d, ReplicaDecision::ScaleUp { from: 4, to: 5 });
    }

    #[test]
    fn interpreter_surfaces_a_bad_signal_as_a_typed_error() {
        let c = cfg();
        let nan = MockReplicaEnvironment { current_replicas: 4, signal_value: f64::NAN, ..Default::default() };
        assert_eq!(interpret_replica(&c, &nan), Err(ReplicaError::BadSignal));
        let neg = MockReplicaEnvironment { current_replicas: 4, signal_value: -1.0, ..Default::default() };
        assert_eq!(interpret_replica(&c, &neg), Err(ReplicaError::BadSignal));
    }

    #[test]
    fn interpreter_surfaces_an_unreadable_metric() {
        let c = cfg();
        let env = MockReplicaEnvironment { current_replicas: 4, signal_unreadable: true, ..Default::default() };
        assert_eq!(interpret_replica(&c, &env), Err(ReplicaError::Unreadable("signal metric")));
    }

    #[test]
    fn interpreter_rejects_a_malformed_band_at_the_gate() {
        let empty = ReplicaBandConfig { floor: 10, ceiling: 3, ..Default::default() };
        assert_eq!(empty.validate(), Err(ReplicaError::EmptyRange));
        let zero = ReplicaBandConfig { ceiling: 0, ..Default::default() };
        assert_eq!(zero.validate(), Err(ReplicaError::ZeroCeiling));
        let no_denom = ReplicaBandConfig { target: 0.0, ..cfg() };
        assert_eq!(no_denom.validate(), Err(ReplicaError::NoDenominator));
        // and the interpreter propagates it (never panics):
        let env = MockReplicaEnvironment { current_replicas: 4, signal_value: 0.9, ..Default::default() };
        assert_eq!(interpret_replica(&empty, &env), Err(ReplicaError::EmptyRange));
    }

    #[test]
    fn desired_raw_is_overflow_safe_on_a_pathological_signal() {
        // enormous signal never overflows the u32 conversion — capped at MAX_REPLICAS.
        assert_eq!(ReplicaSignal::QueueDepth.desired_raw(1, 1e300, 1.0), MAX_REPLICAS);
        // no denominator ⇒ hold at current.
        assert_eq!(ReplicaSignal::Utilization.desired_raw(3, 0.9, 0.0), 3);
    }

    #[test]
    fn decision_target_and_label_are_consistent() {
        assert_eq!(ReplicaDecision::Hold { current: 5 }.target(), 5);
        assert_eq!(ReplicaDecision::ScaleUp { from: 2, to: 6 }.target(), 6);
        assert_eq!(ReplicaDecision::ScaleUp { from: 2, to: 6 }.current(), 2);
        assert_eq!(ReplicaDecision::Hold { current: 5 }.current(), 5);
        assert_eq!(ReplicaDecision::ScaleUp { from: 2, to: 6 }.label(), "ScaleUp");
        assert_eq!(ReplicaDecision::SpotScaleOut { from: 3, to: 5, reclaim: 2 }.label(), "SpotScaleOut");
    }

    // ── the pure tick planner (shadow / cooldown / scale-in-policy / force gate) ──

    fn gate(dry_run: bool, in_cooldown: bool, scale_in_permitted: bool) -> ReplicaGate {
        ReplicaGate { dry_run, in_cooldown, scale_in_permitted, force: None }
    }

    #[test]
    fn plan_holds_actuation_in_shadow_and_actuates_after_confirm() {
        // the TYPED-SPEC test the runtime wiring must satisfy: a band decides the
        // SAME thing in shadow and live, but only WRITES once confirmed (dry_run=false).
        let c = cfg();
        let env = MockReplicaEnvironment { current_replicas: 4, signal_value: 0.9, ..Default::default() };

        // SHADOW: decides ScaleUp 4→5 but actuate is None (nothing written).
        let shadow = plan_replica_tick(&c, &env, gate(true, false, true)).expect("plans");
        assert_eq!(shadow.decision, ReplicaDecision::ScaleUp { from: 4, to: 5 });
        assert_eq!(shadow.actuate, None, "shadow must never write");
        assert!(!shadow.deferred);

        // CONFIRMED (dry_run=false): the SAME decision now actuates to 5.
        let live = plan_replica_tick(&c, &env, gate(false, false, true)).expect("plans");
        assert_eq!(live.decision, ReplicaDecision::ScaleUp { from: 4, to: 5 });
        assert_eq!(live.actuate, Some(5), "confirmed must write the target");
    }

    #[test]
    fn plan_cooldown_suppresses_actuation_even_when_live() {
        let c = cfg();
        let env = MockReplicaEnvironment { current_replicas: 4, signal_value: 0.9, ..Default::default() };
        let cooling = plan_replica_tick(&c, &env, gate(false, true, true)).expect("plans");
        assert_eq!(cooling.decision, ReplicaDecision::ScaleUp { from: 4, to: 5 });
        assert_eq!(cooling.actuate, None, "a cooldown holds the write");
    }

    #[test]
    fn plan_defers_scale_in_under_restart_free_only_but_scales_out_freely() {
        let c = cfg();
        // idle signal → the law wants to scale IN (10 @ 0.4 → 9).
        let shrink_env = MockReplicaEnvironment { current_replicas: 10, signal_value: 0.4, ..Default::default() };
        // scale_in_permitted=false (the default restartFreeOnly posture): DEFERRED, no write.
        let deferred = plan_replica_tick(&c, &shrink_env, gate(false, false, false)).expect("plans");
        assert!(matches!(deferred.decision, ReplicaDecision::ScaleDown { from: 10, to: 9 }));
        assert!(deferred.deferred, "a scale-in is a pod-shedding crossing");
        assert_eq!(deferred.actuate, None, "restartFreeOnly refuses the scale-in");
        // scale_in_permitted=true (allowRestart): now it writes.
        let allowed = plan_replica_tick(&c, &shrink_env, gate(false, false, true)).expect("plans");
        assert_eq!(allowed.actuate, Some(9));
        assert!(!allowed.deferred);

        // a scale-OUT is RestartFree — never gated by the scale-in policy.
        let grow_env = MockReplicaEnvironment { current_replicas: 4, signal_value: 0.9, ..Default::default() };
        let grow = plan_replica_tick(&c, &grow_env, gate(false, false, false)).expect("plans");
        assert_eq!(grow.actuate, Some(5), "scale-out is never blocked by restartFreeOnly");
        assert!(!grow.deferred);
    }

    #[test]
    fn plan_break_glass_force_pins_the_count_clamped_and_gated() {
        let c = cfg(); // ceiling 50, floor 2
        let env = MockReplicaEnvironment { current_replicas: 4, signal_value: 0.05, ..Default::default() };
        // force 8 (would otherwise idle-shrink) — live pins to 8.
        let forced = plan_replica_tick(&c, &env, ReplicaGate { force: Some(8), ..gate(false, false, true) }).expect("plans");
        assert_eq!(forced.decision, ReplicaDecision::ScaleUp { from: 4, to: 8 });
        assert_eq!(forced.actuate, Some(8));
        // force still respects the ceiling clamp.
        let clamped = plan_replica_tick(&c, &env, ReplicaGate { force: Some(9999), ..gate(false, false, true) }).expect("plans");
        assert_eq!(clamped.actuate, Some(50));
        // force still honours shadow (no write).
        let shadow = plan_replica_tick(&c, &env, ReplicaGate { force: Some(8), ..gate(true, false, true) }).expect("plans");
        assert_eq!(shadow.actuate, None);
    }

    #[test]
    fn plan_propagates_a_bad_signal_error_never_panics() {
        let c = cfg();
        let nan = MockReplicaEnvironment { current_replicas: 4, signal_value: f64::NAN, ..Default::default() };
        assert_eq!(plan_replica_tick(&c, &nan, gate(false, false, true)), Err(ReplicaError::BadSignal));
        let bad_cfg = ReplicaBandConfig { ceiling: 0, ..Default::default() };
        let env = MockReplicaEnvironment { current_replicas: 4, signal_value: 0.9, ..Default::default() };
        assert_eq!(plan_replica_tick(&bad_cfg, &env, gate(false, false, true)), Err(ReplicaError::ZeroCeiling));
    }

    #[test]
    fn plan_resting_decision_never_actuates() {
        let c = cfg();
        // in-band (Hold) → no carve, no write, not deferred.
        let env = MockReplicaEnvironment { current_replicas: 5, signal_value: 0.82, ..Default::default() };
        let rest = plan_replica_tick(&c, &env, gate(false, false, true)).expect("plans");
        assert_eq!(rest.decision, ReplicaDecision::Hold { current: 5 });
        assert_eq!(rest.actuate, None);
        assert!(!rest.deferred);
    }
}
