//! `breathe-control::lifecycle` — the LIFECYCLE BREATH: a workload breathes
//! through a *lifecycle*, not just a held setpoint
//! ([`theory/BREATHABILITY.md`](https://github.com/pleme-io/theory/blob/main/BREATHABILITY.md)
//! §II.5 default #6 + "The lifecycle breath").
//!
//! Right-sizing toward a setpoint (the vertical [`crate::decide`] law) is only the
//! STEADY slice of breathing. A workload needs MORE at startup than at steady-state
//! (JIT warm-up, cache fill, migrations, connection pools); right-sizing to the
//! steady setpoint from `t=0` means it can never start. So breathe models the full
//! five-phase lifecycle:
//!
//! ```text
//!   Zero ──wake──▶ Wake ──settle──▶ Settle ──reach_setpoint──▶ Steady ──go_idle──▶ Idle
//!    ▲   (inhale: generous   (controlled exhale:     (cost floor)          │  (drain-ahead)
//!    │    startup band ≫      shadow-tighten                               │
//!    │    setpoint)           step-by-step)                               │
//!    └──────────────────────────── rest ◀──────────────────────────────── Idle
//!         Settle/Steady ──re_expand──▶ Wake   (a struggling workload always expands)
//!         Idle ──reload──▶ Steady               (load returned before zero)
//! ```
//!
//! This module owns NO I/O and adds NO new balancing algebra — it is the
//! ORCHESTRATOR that composes the shipped primitives (`crate::decide`,
//! [`crate::safe_min`]/[`crate::soft_min`], [`crate::lapidar`], [`crate::BandConfig`])
//! into the lifecycle, following the TYPED-SPEC + INTERPRETER triplet the
//! [`crate::replica`] module established: a typed border ([`LifecycleConfig`],
//! [`LifecycleDecision`]), a pure decision core ([`fuse`], [`plan_lifecycle_tick`]),
//! and a mockable [`NervousSystem`] environment the interpreter walks — so the whole
//! lifecycle is unit-testable without a cluster.
//!
//! # The never-stuck invariant (never stop breathing) — the centerpiece
//!
//! The system is designed so no reachable state traps or errors. The four rules of
//! §II.5, mapped to a mechanism, TIER-HONEST (★★ UNREPRESENTABILITY — a `Result::Err`
//! is *mitigation*; a compile error / absent method / parse boundary is
//! *unrepresentability*; never round up):
//!
//! | Rule | Mechanism | Tier |
//! |---|---|---|
//! | (a) uncertainty → **expand**, never starve | a [`Tighten`](LifecycleDecision::Tighten) carries a [`ShadowProvenStep`], whose only constructor takes a [`Confirmed`] metric; [`Confirmed::parse`] REJECTS a missing / zero / pre-warmup reading — so a tighten under uncertainty is *unconstructible*. The fusion then biases uncertainty to [`Expand`](LifecycleDecision::Expand). | **truly-unrep** (no `Tighten` value exists without a `Confirmed`) + **parse-time-rejected** (the `Confirmed` boundary); the expand-not-hold *bias* is **only-mitigated** + property-tested |
//! | (b) every state has an **exit** | the phantom-typestate FSM ([`Lifecycle<P>`]): a legal transition is a method, an illegal one is `E0599` (no method). Each [`Phase`] declares a non-empty `EXITS` set, asserted at COMPILE time (`const _: () = assert!(!EXITS.is_empty())`). | transition-legality: **truly-unrep** (library path); exit-non-emptiness: **truly-unrep** (const-assert); reaches-a-good-terminal: **only-mitigated** (a CI BFS forcing-function — Rust cannot prove a graph-reachability quantifier as a type) |
//! | (c) floor = **proven need** | every tighten step's floor is `max(`[`crate::safe_min`]`(peak, ws, steady), startup_observed_min)`; [`ShadowProvenStep::plan`] clamps the step `≥ floor`. | **only-mitigated** (a smart-constructor clamp, same tier as [`crate::soft_min`]) + property-tested |
//! | (d) shadow-gated tightening | a step is `shadow_only` until [`LifecycleCarry::shadow_confirmed_for`] marks it clean; a harmful one reverts via [`tighten_settled`] (reuses [`lapidar::ControlQuality::score`], the accept-if-improved-else-revert rule). | shadow-before-effect: **only-mitigated** (a runtime gate, reused from the shipped shadow→confirm→effect); the no-tighten-without-`Confirmed` half is **truly-unrep** as in (a) |
//!
//! The two convergence claims (every phase reaches BOTH good resting states
//! `{Zero, Steady}`; no phase is a dead-end) are MECHANICAL CI forcing-functions
//! ([`legal_edges`] BFS tests), never compile errors — Rust cannot prove the
//! reachability quantifier. Named honestly so the claim is falsifiable.

use crate::BandConfig;
use crate::lapidar;

// ===========================================================================
// Phase model — the phantom typestate FSM (rule (b): every state has an exit)
// ===========================================================================

/// The runtime mirror of a [`Phase`] — the tag persisted to `BandStatus`, driven
/// through the pure [`fuse`], and used by the [`legal_edges`] reachability tests.
/// The typed [`Lifecycle<P>`] proves transition legality at COMPILE time; this tag
/// carries the same state over the wire and through the pure decision core.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PhaseTag {
    /// Scaled to zero — the cheapest exhale.
    Zero,
    /// Just woken, cold-start in progress, holding the generous startup band.
    Wake,
    /// Metrics stabilized below the expansion; shadow-tightening toward the setpoint.
    Settle,
    /// Right-sized at the setpoint — the cost floor.
    Steady,
    /// Sustained idle; draining ahead toward zero (retirada's window).
    Idle,
}

impl PhaseTag {
    /// Stable label (status / logging).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Zero => "Zero",
            Self::Wake => "Wake",
            Self::Settle => "Settle",
            Self::Steady => "Steady",
            Self::Idle => "Idle",
        }
    }

    /// All five phases — the partition the never-stuck reachability tests iterate.
    #[must_use]
    pub fn all() -> [Self; 5] {
        [Self::Zero, Self::Wake, Self::Settle, Self::Steady, Self::Idle]
    }

    /// The legal outgoing edges of THIS phase (the runtime mirror of `Phase::EXITS`).
    /// The single source of truth is [`legal_edges`]; this is the per-phase view.
    #[must_use]
    pub fn exits(self) -> &'static [PhaseTag] {
        match self {
            Self::Zero => Zero::EXITS,
            Self::Wake => Wake::EXITS,
            Self::Settle => Settle::EXITS,
            Self::Steady => Steady::EXITS,
            Self::Idle => Idle::EXITS,
        }
    }

    /// `true` for the two GOOD RESTING states — the terminals the never-stuck
    /// convergence property requires every phase to be able to reach: `Steady`
    /// (load persists → right-sized) and `Zero` (idle persists → cheapest rest).
    #[must_use]
    pub fn is_good_resting(self) -> bool {
        matches!(self, Self::Zero | Self::Steady)
    }
}

mod sealed {
    pub trait Sealed {}
}

/// A lifecycle phase marker. Sealed (only the five in-crate types inhabit it) and
/// carries the phase's TAG plus its non-empty legal-exit set. The `EXITS` const is
/// the type-level witness of the never-stuck "every state has an exit" rule, asserted
/// non-empty at compile time below.
pub trait Phase: sealed::Sealed {
    /// The runtime tag for this phase.
    const TAG: PhaseTag;
    /// The phases this one may legally transition to. Non-empty by the const-assert.
    const EXITS: &'static [PhaseTag];
}

/// `Zero` — scaled to zero, resting. Only exit: wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Zero;
/// `Wake` — cold-start under the generous startup band. Only exit: settle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Wake;
/// `Settle` — shadow-tightening toward the setpoint. Exits: reach setpoint, or
/// re-expand (struggling).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settle;
/// `Steady` — right-sized at the setpoint. Exits: go idle, or re-expand (struggling).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Steady;
/// `Idle` — draining ahead toward zero. Exits: rest (to zero), or reload (to steady).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Idle;

impl sealed::Sealed for Zero {}
impl sealed::Sealed for Wake {}
impl sealed::Sealed for Settle {}
impl sealed::Sealed for Steady {}
impl sealed::Sealed for Idle {}

impl Phase for Zero {
    const TAG: PhaseTag = PhaseTag::Zero;
    const EXITS: &'static [PhaseTag] = &[PhaseTag::Wake];
}
impl Phase for Wake {
    const TAG: PhaseTag = PhaseTag::Wake;
    const EXITS: &'static [PhaseTag] = &[PhaseTag::Settle];
}
impl Phase for Settle {
    const TAG: PhaseTag = PhaseTag::Settle;
    const EXITS: &'static [PhaseTag] = &[PhaseTag::Steady, PhaseTag::Wake];
}
impl Phase for Steady {
    const TAG: PhaseTag = PhaseTag::Steady;
    const EXITS: &'static [PhaseTag] = &[PhaseTag::Idle, PhaseTag::Wake];
}
impl Phase for Idle {
    const TAG: PhaseTag = PhaseTag::Idle;
    const EXITS: &'static [PhaseTag] = &[PhaseTag::Zero, PhaseTag::Steady];
}

// COMPILE-TIME never-stuck: every phase has ≥1 legal exit. Adding a phase with an
// empty `EXITS` (a dead-end) fails the BUILD here, not a test — the "every state has
// an exit" rule is truly-unrepresentable on the exit-existence axis.
const _: () = assert!(!Zero::EXITS.is_empty());
const _: () = assert!(!Wake::EXITS.is_empty());
const _: () = assert!(!Settle::EXITS.is_empty());
const _: () = assert!(!Steady::EXITS.is_empty());
const _: () = assert!(!Idle::EXITS.is_empty());

/// The complete legal transition graph — the SINGLE source of truth the typed
/// [`Lifecycle<P>`] transition methods realize and the never-stuck reachability
/// tests walk. An edge `(a, b)` means "phase `a` may transition to phase `b`".
#[must_use]
pub fn legal_edges() -> Vec<(PhaseTag, PhaseTag)> {
    let mut edges = Vec::new();
    for from in PhaseTag::all() {
        for &to in from.exits() {
            edges.push((from, to));
        }
    }
    edges
}

/// A typed lifecycle position. The whole point is COMPILE-TIME transition legality:
/// a legal edge is a method (e.g. [`Lifecycle::<Zero>::wake`]); an illegal edge has
/// NO method, so writing it is `E0599` (method not found), not a `Result::Err`
/// caught at runtime (★★ UNREPRESENTABILITY: truly-unrepresentable on the
/// transition-legality axis, library path). The controller advances the runtime
/// [`PhaseTag`] but can hold this proof object to make an illegal advance
/// un-compilable.
///
/// ```
/// use breathe_control::lifecycle::{Lifecycle, Zero, PhaseTag, Phase};
/// let z = Lifecycle::<Zero>::start(0);
/// let w = z.wake(1_000, 0);          // Zero → Wake (legal)
/// let s = w.settle(2);               // Wake → Settle (legal)
/// let steady = s.reach_setpoint(3);  // Settle → Steady (legal)
/// assert_eq!(steady.tag(), PhaseTag::Steady);
/// ```
///
/// An illegal transition does not compile:
///
/// ```compile_fail
/// use breathe_control::lifecycle::{Lifecycle, Zero};
/// let z = Lifecycle::<Zero>::start(0);
/// let _ = z.go_idle(1);   // ERROR[E0599]: no method `go_idle` on Lifecycle<Zero>
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lifecycle<P: Phase> {
    carry: LifecycleCarry,
    _phase: core::marker::PhantomData<P>,
}

impl<P: Phase> Lifecycle<P> {
    /// The runtime tag for this typed position.
    #[must_use]
    pub fn tag(&self) -> PhaseTag {
        P::TAG
    }

    /// The carried durable state (limit, startup floor, timers).
    #[must_use]
    pub fn carry(&self) -> LifecycleCarry {
        self.carry
    }

    fn advance<Q: Phase>(self, carry: LifecycleCarry) -> Lifecycle<Q> {
        Lifecycle { carry, _phase: core::marker::PhantomData }
    }
}

impl Lifecycle<Zero> {
    /// Begin a lifecycle at rest (scaled to zero).
    #[must_use]
    pub fn start(now_epoch: i64) -> Self {
        Lifecycle {
            carry: LifecycleCarry { phase_entered_epoch: now_epoch, ..LifecycleCarry::default() },
            _phase: core::marker::PhantomData,
        }
    }

    /// Zero → Wake: a trigger woke the workload; seat the generous startup band and
    /// arm the startup-observed-minimum floor learner.
    #[must_use]
    pub fn wake(self, expansion_limit: u64, now_epoch: i64) -> Lifecycle<Wake> {
        self.advance(LifecycleCarry {
            phase_entered_epoch: now_epoch,
            current_limit: expansion_limit,
            startup_observed_min: 0,
            shadow_confirmed_for: None,
            ..LifecycleCarry::default()
        })
    }
}

impl Lifecycle<Wake> {
    /// Wake → Settle: cold-start stabilized; begin the shadow-tighten descent. The
    /// startup-observed minimum is frozen here as the durable floor (rule (c)).
    #[must_use]
    pub fn settle(self, now_epoch: i64) -> Lifecycle<Settle> {
        self.advance(LifecycleCarry { phase_entered_epoch: now_epoch, ..self.carry })
    }
}

impl Lifecycle<Settle> {
    /// Settle → Steady: the tightening trajectory reached the setpoint.
    #[must_use]
    pub fn reach_setpoint(self, now_epoch: i64) -> Lifecycle<Steady> {
        self.advance(LifecycleCarry { phase_entered_epoch: now_epoch, shadow_confirmed_for: None, ..self.carry })
    }

    /// Settle → Wake: a struggling workload always expands — a harmful tighten, an
    /// OOM, or a restart storm re-opens the generous band (the reversibility exit).
    #[must_use]
    pub fn re_expand(self, expansion_limit: u64, now_epoch: i64) -> Lifecycle<Wake> {
        self.advance(LifecycleCarry {
            phase_entered_epoch: now_epoch,
            current_limit: expansion_limit.max(self.carry.current_limit),
            shadow_confirmed_for: None,
            ..self.carry
        })
    }
}

impl Lifecycle<Steady> {
    /// Steady → Idle: sustained idle; enter the drain-ahead window before zero.
    #[must_use]
    pub fn go_idle(self, now_epoch: i64) -> Lifecycle<Idle> {
        self.advance(LifecycleCarry { phase_entered_epoch: now_epoch, ..self.carry })
    }

    /// Steady → Wake: a struggling steady workload always expands (rule (b) exit).
    #[must_use]
    pub fn re_expand(self, expansion_limit: u64, now_epoch: i64) -> Lifecycle<Wake> {
        self.advance(LifecycleCarry {
            phase_entered_epoch: now_epoch,
            current_limit: expansion_limit.max(self.carry.current_limit),
            shadow_confirmed_for: None,
            ..self.carry
        })
    }
}

impl Lifecycle<Idle> {
    /// Idle → Zero: the idle timer elapsed; scale to zero (the cheapest rest).
    #[must_use]
    pub fn rest(self, now_epoch: i64) -> Lifecycle<Zero> {
        self.advance(LifecycleCarry { phase_entered_epoch: now_epoch, current_limit: 0, ..self.carry })
    }

    /// Idle → Steady: load returned before the idle timer elapsed; re-load at the
    /// setpoint (the workload is already warm — no cold-start needed).
    #[must_use]
    pub fn reload(self, now_epoch: i64) -> Lifecycle<Steady> {
        self.advance(LifecycleCarry { phase_entered_epoch: now_epoch, ..self.carry })
    }
}

/// The durable per-target lifecycle state carried across ticks (the pure mirror of
/// what `BandStatus` persists). Everything the pure [`fuse`]/[`plan_lifecycle_tick`]
/// needs beyond the live signals — so a controller restart reconstructs the FSM from
/// the CR status with nothing lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LifecycleCarry {
    /// Wall-clock epoch when the CURRENT phase began (drives the timeout escapes +
    /// the idle-to-zero timer).
    pub phase_entered_epoch: i64,
    /// The limit currently seated (the expansion limit in Wake, descending in
    /// Settle, the steady target in Steady). `0` at Zero.
    pub current_limit: u64,
    /// The STARTUP-OBSERVED MINIMUM (rule (c) "floor = proven need"): the lowest the
    /// workload was seen to need during Wake, frozen at `settle`, and used as an
    /// additional floor for every tighten step so breathe never tightens below what
    /// the workload was *seen* to need to run. `0` = not yet learned.
    pub startup_observed_min: u64,
    /// SHADOW→CONFIRM→EFFECT (rule (d)): `Some(from)` marks that a tighten step FROM
    /// this limit has been shadow-observed for a clean window and may now EFFECT. A
    /// proposed step whose `from` differs is shadowed again first. `None` = nothing
    /// confirmed yet.
    pub shadow_confirmed_for: Option<u64>,
}

// ===========================================================================
// The reactive nervous system — typed signals + a mockable Environment
// ===========================================================================

/// A load reading — the work-rate afferent signal (RPS + queue depth). `rps == 0`
/// and `queue_depth == 0` is genuine quiescence (a wake trigger's absence, an
/// idle candidate); either non-zero is demand.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Load {
    /// Requests per second across the workload.
    pub rps: f64,
    /// Pending backlog / queue depth.
    pub queue_depth: u64,
}

impl Load {
    /// `true` iff there is any demand (either signal non-zero).
    #[must_use]
    pub fn is_active(self) -> bool {
        self.rps > 0.0 || self.queue_depth > 0
    }
}

/// A health reading — the restraint afferent signal. Any of these say "do NOT
/// tighten; if anything, expand": a crash / OOM / probe failure means the low
/// observed usage is a symptom, not proof of slack.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Health {
    /// Container restart count delta over the trailing window.
    pub restarts: u32,
    /// The last termination was an OOM-kill.
    pub oom: bool,
    /// Liveness/readiness probe failures over the trailing window.
    pub probe_failures: u32,
}

impl Health {
    /// `true` iff the workload is STRUGGLING (any restart / OOM / probe failure) —
    /// the signal that forces expansion over tightening.
    #[must_use]
    pub fn is_struggling(self) -> bool {
        self.restarts > 0 || self.oom || self.probe_failures > 0
    }
}

/// The utilization observation — the vertical metric plane. `used == None` is a
/// MISSING / broken reading (the uncertainty case, rule (a)); `used == Some(0)` from
/// a running workload is also treated as untrusted by [`Confirmed::parse`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Metrics {
    /// Working set in bytes. `None` = no fresh reading (uncertainty → expand).
    pub used: Option<u64>,
    /// The current limit / denominator (bytes). `0` = no limit to reason against.
    pub capacity: u64,
    /// Seconds the workload has been observed since its last (re)start — the warmup
    /// age. A tighten is refused until this clears the warmup window.
    pub observed_for_secs: u64,
    /// The demonstrated PEAK working set over the trailing window (bytes) — folds
    /// into the never-OOM floor via [`crate::safe_min`].
    pub peak_used: u64,
}

/// The side-effecting boundary the lifecycle interpreter reads through — the
/// TYPED-SPEC triplet's Environment trait (the testability contract). Real impls
/// (`breathe-kube` / `breathe-host`, LiveTODO) read the metric plane + readiness +
/// restart counts + the reclaim / schedule / dependency / cost signals; tests pass
/// [`MockNervousSystem`]. Sync + dependency-free, matching this crate's pure-core
/// discipline (the async k8s I/O adapter lives at the provider layer). Every source
/// is optional (`None` = "this signal was not read this tick") so the fusion can bias
/// missing signals to expansion (rule (a)) rather than assume quiescence.
pub trait NervousSystem {
    /// The vertical utilization observation (always present, even if `used: None`).
    fn metrics(&self) -> Metrics;
    /// Readiness / startup probe: `Some(true)` ready, `Some(false)` not ready yet,
    /// `None` unknown. Default: unknown.
    fn readiness(&self) -> Option<bool> {
        None
    }
    /// Work-rate load (RPS + queue depth): `None` = not read this tick. Default: none.
    fn load(&self) -> Option<Load> {
        None
    }
    /// Health (restarts / OOM / probe failures). Default: healthy.
    fn health(&self) -> Health {
        Health::default()
    }
    /// Whether the workload's blocking dependencies are ready: `Some(true/false)`,
    /// `None` unknown. Default: unknown.
    fn dependency_ready(&self) -> Option<bool> {
        None
    }
    /// A spot / node reclaim is pending (drain-ahead trigger). Default: false.
    fn spot_reclaim_pending(&self) -> bool {
        false
    }
    /// A schedule window is active (cron-driven pre-warm). `None` = unknown.
    /// Default: unknown.
    fn schedule_active(&self) -> Option<bool> {
        None
    }
    /// The workload is over its cost budget (cost-driven zero). Default: false.
    fn cost_over_budget(&self) -> bool {
        false
    }
    /// The current wall-clock epoch (drives the phase timers). Default: 0.
    fn now_epoch(&self) -> i64 {
        0
    }
}

/// The fused snapshot of all signals for one tick — the afferent nerve's reading,
/// collected once from the [`NervousSystem`] and handed to the pure [`fuse`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Signals {
    /// The utilization observation.
    pub metrics: Metrics,
    /// Readiness (`None` = unknown).
    pub readiness: Option<bool>,
    /// Load (`None` = not read).
    pub load: Option<Load>,
    /// Health.
    pub health: Health,
    /// Dependency readiness (`None` = unknown).
    pub dependency_ready: Option<bool>,
    /// Spot reclaim pending.
    pub spot_reclaim_pending: bool,
    /// Schedule active (`None` = unknown).
    pub schedule_active: Option<bool>,
    /// Over cost budget.
    pub cost_over_budget: bool,
    /// The tick's wall-clock epoch.
    pub now_epoch: i64,
}

/// Collect every signal from a [`NervousSystem`] into a [`Signals`] snapshot — the
/// one place the environment is read, so [`fuse`] stays pure.
#[must_use]
pub fn sense<E: NervousSystem>(env: &E) -> Signals {
    Signals {
        metrics: env.metrics(),
        readiness: env.readiness(),
        load: env.load(),
        health: env.health(),
        dependency_ready: env.dependency_ready(),
        spot_reclaim_pending: env.spot_reclaim_pending(),
        schedule_active: env.schedule_active(),
        cost_over_budget: env.cost_over_budget(),
        now_epoch: env.now_epoch(),
    }
}

impl Signals {
    /// Is there any WAKE trigger present (rule: any signal wakes, OR-fusion)? A wake
    /// fires on readiness-arming, active load, an active schedule, or dependencies
    /// coming ready. Cost-over-budget and a pending reclaim do NOT wake a resting
    /// workload (they only ever push toward zero).
    #[must_use]
    pub fn any_wake(self) -> Option<WakeReason> {
        if self.load.is_some_and(Load::is_active) {
            return Some(WakeReason::Load);
        }
        if self.schedule_active == Some(true) {
            return Some(WakeReason::Schedule);
        }
        if self.readiness == Some(true) {
            return Some(WakeReason::Readiness);
        }
        if self.dependency_ready == Some(true) {
            return Some(WakeReason::Dependency);
        }
        None
    }

    /// `true` iff the workload is STRUGGLING — the "always expand" trigger: a health
    /// regression (restart / OOM / probe failure) OR a not-ready readiness probe.
    #[must_use]
    pub fn is_struggling(self) -> bool {
        self.health.is_struggling() || self.readiness == Some(false)
    }

    /// The struggle reason (for the typed [`Expand`](LifecycleDecision::Expand) /
    /// [`ReExpand`](LifecycleDecision::ReExpand) receipt). Only meaningful when
    /// [`Self::is_struggling`] or metrics are missing.
    #[must_use]
    pub fn struggle_reason(self) -> StruggleReason {
        if self.health.oom {
            StruggleReason::Oom
        } else if self.health.restarts > 0 {
            StruggleReason::Restart
        } else if self.health.probe_failures > 0 || self.readiness == Some(false) {
            StruggleReason::ProbeFail
        } else {
            StruggleReason::Uncertainty
        }
    }
}

// ===========================================================================
// The Confirmed witness + the ShadowProvenStep (rules (a), (c), (d))
// ===========================================================================

/// Why a metric reading is UNCERTAIN — the typed rejection [`Confirmed::parse`]
/// returns. Every arm means "do not tighten; bias to expansion" (rule (a)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Uncertainty {
    /// No fresh working-set reading (`used: None`) — the classic uncertainty case.
    MetricMissing,
    /// A running workload reading `used == 0` — a degraded metric, not a real zero.
    ZeroUsed,
    /// No capacity/limit denominator (`capacity == 0`) — nothing to reason against.
    NoCapacity,
    /// Still inside the warmup window — the workload has not demonstrated a full duty
    /// cycle, so its low reading is not yet proof the slack is safe to take.
    WithinWarmup {
        /// Seconds observed so far.
        observed_for: u64,
        /// The configured warmup window.
        warmup: u64,
    },
}

impl std::fmt::Display for Uncertainty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MetricMissing => f.write_str("metric missing (no fresh working-set reading)"),
            Self::ZeroUsed => f.write_str("zero working-set from a running workload (degraded metric)"),
            Self::NoCapacity => f.write_str("no capacity denominator"),
            Self::WithinWarmup { observed_for, warmup } => {
                write!(f, "within warmup window ({observed_for}s of {warmup}s)")
            }
        }
    }
}

impl std::error::Error for Uncertainty {}

/// A metric reading PROVEN trustworthy enough to reason a SHRINK against — a
/// parse-don't-validate newtype (★★ UNREPRESENTABILITY: parse-time-rejected). The
/// field is private and the ONLY constructor is [`Self::parse`], which rejects a
/// missing / zero / no-denominator / pre-warmup reading. Because the only way to
/// build a [`ShadowProvenStep`] (and therefore the only way to name a
/// [`Tighten`](LifecycleDecision::Tighten)) is through a `&Confirmed`, a tighten
/// under uncertainty is *unconstructible* — the never-starve-under-uncertainty rule
/// (a), enforced by the type, not a runtime `if`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Confirmed {
    used: u64,
    capacity: u64,
    peak_used: u64,
}

impl Confirmed {
    /// Parse a [`Metrics`] reading into a `Confirmed` proof, or reject it with a
    /// typed [`Uncertainty`]. `warmup_seconds` is the configured warmup window (a
    /// reading younger than it is refused — the un-observed-boot-spike hole).
    ///
    /// # Errors
    /// A typed [`Uncertainty`] naming why the reading may not be tightened against.
    pub fn parse(m: Metrics, warmup_seconds: u64) -> Result<Self, Uncertainty> {
        let used = m.used.ok_or(Uncertainty::MetricMissing)?;
        if used == 0 {
            return Err(Uncertainty::ZeroUsed);
        }
        if m.capacity == 0 {
            return Err(Uncertainty::NoCapacity);
        }
        if m.observed_for_secs < warmup_seconds {
            return Err(Uncertainty::WithinWarmup { observed_for: m.observed_for_secs, warmup: warmup_seconds });
        }
        Ok(Self { used, capacity: m.capacity, peak_used: m.peak_used.max(used) })
    }

    /// The confirmed working set (bytes).
    #[must_use]
    pub fn used(self) -> u64 {
        self.used
    }

    /// The confirmed capacity / current limit (bytes).
    #[must_use]
    pub fn capacity(self) -> u64 {
        self.capacity
    }

    /// The demonstrated peak (bytes) — feeds the never-OOM floor.
    #[must_use]
    pub fn peak(self) -> u64 {
        self.peak_used
    }

    /// The confirmed utilization ratio.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn util(self) -> f64 {
        (self.used as f64) / (self.capacity as f64)
    }
}

/// ONE step of the shadow-tighten trajectory (rule (d)) — a limit move from `from`
/// DOWN toward the steady target, provably `≥ floor` and `≤ from` by construction.
/// Its ONLY constructor, [`Self::plan`], takes a `&`[`Confirmed`] — so a tightening
/// step cannot exist without a proven metric (rule (a), truly-unrepresentable at the
/// step level), and clamps the target to the proven-need floor (rule (c)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShadowProvenStep {
    from: u64,
    to: u64,
}

impl ShadowProvenStep {
    /// Plan the next tighten step from `from` toward `steady_target`, never below
    /// `floor`, gated by a `&Confirmed` proof. Returns `None` when there is no step
    /// left to take (`from` already at or below `max(steady_target, floor)`) — the
    /// caller then transitions Settle → Steady. Monotone: `floor ≤ to < from` for any
    /// returned step; `step_factor ∈ (0, 1)` multiplies the descent.
    ///
    /// The `_proof` parameter is load-bearing, not decorative: it makes a tighten
    /// step *unconstructible* without a [`Confirmed`] metric (rule (a)).
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn plan(from: u64, steady_target: u64, floor: u64, step_factor: f64, _proof: &Confirmed) -> Option<Self> {
        // The descent bottoms out at the higher of the steady target and the
        // proven-need floor — never tighten past either.
        let bottom = steady_target.max(floor);
        if from <= bottom {
            return None; // already tight — no step; caller reaches setpoint.
        }
        let factor = if step_factor > 0.0 && step_factor < 1.0 { step_factor } else { 0.9 };
        let stepped = ((from as f64) * factor).floor() as u64;
        // Clamp: never below the bottom, and always at least one byte of progress
        // (a factor that rounds to `from` still advances by snapping to `from - 1`
        // when there is room, else to the bottom).
        let to = stepped.max(bottom).min(from.saturating_sub(1)).max(bottom);
        Some(Self { from, to })
    }

    /// The limit this step moves away from.
    #[must_use]
    pub fn from(self) -> u64 {
        self.from
    }

    /// The (tighter) limit this step moves to — `floor ≤ to < from` by construction.
    #[must_use]
    pub fn to(self) -> u64 {
        self.to
    }
}

/// The outcome of a just-EFFECTED tighten step under lapidar's accept-if-improved-
/// else-revert rule (rule (d), the reversibility half).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TightenOutcome {
    /// The step improved (or held) control quality — keep it.
    Kept,
    /// The step regressed control quality — revert it (a tie reverts: never a change
    /// without a proven win).
    Reverted,
}

/// Decide whether a just-effected tighten step is KEPT or REVERTED, reusing
/// `lapidar`'s exact accept rule (`post.score() < pre.score()`, safety-first
/// weighting — a breach outweighs waste). The controller collects a pre-step and a
/// post-step [`lapidar::ControlQuality`] window and calls this; a `Reverted` outcome
/// drives Settle → Wake (re-expand) or restores the prior limit. Pure — no I/O.
#[must_use]
pub fn tighten_settled(pre_step: &lapidar::ControlQuality, post_step: &lapidar::ControlQuality) -> TightenOutcome {
    if post_step.score() < pre_step.score() {
        TightenOutcome::Kept
    } else {
        TightenOutcome::Reverted
    }
}

// ===========================================================================
// Config (parse-time gate) + the decision border
// ===========================================================================

/// The lifecycle-breath tuning border. Extends (does not replace) the vertical
/// [`BandConfig`] with the lifecycle knobs; the steady band IS a `BandConfig` so
/// every steady-phase carve reuses the shipped [`crate::decide`] law and floors
/// unchanged. Config precedence (shikumi) is applied upstream; this is the resolved
/// value one tick sees.
#[derive(Debug, Clone)]
pub struct LifecycleConfig {
    /// The steady-state band (the cost floor). Reused verbatim by [`crate::decide`]
    /// in the Steady phase; its `setpoint` + floors seed the tighten trajectory.
    pub steady: BandConfig,
    /// The startup EXPANSION factor (`≥ 1.0`, default `2.0`): the generous cold-start
    /// band is `≈ expansion_factor ×` the steady limit, so a workload gets headroom
    /// to start (JIT / cache / migrations / pools). `1.0` = no expansion (start at
    /// steady — the anti-pattern the whole phase exists to avoid).
    pub expansion_factor: f64,
    /// The per-step tighten multiplier (`∈ (0, 1)`, default `0.90`): each shadow-
    /// tighten step multiplies the limit down toward the setpoint.
    pub settle_step_factor: f64,
    /// Utilization strictly below this in Steady arms the Idle candidate (default =
    /// the steady band's `shrink_below`).
    pub idle_below: f64,
    /// Seconds a workload rests in Idle (draining ahead) before it scales to Zero
    /// (`> 0`, default `300`). Load returning inside the window reloads to Steady.
    pub idle_to_zero_secs: u64,
    /// Never-stuck TIMEOUT escape for Wake: force Settle after this long even if the
    /// workload never demonstrably stabilized (`> 0`, default `900`). Guarantees Wake
    /// is not a trap.
    pub expansion_max_secs: u64,
    /// Never-stuck TIMEOUT escape for Settle: force Steady after this long even if the
    /// tighten trajectory never confirmed the last step (`> 0`, default `900`) — lands
    /// at the current proven-need floor. Guarantees Settle is not a trap.
    pub settle_max_secs: u64,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        let steady = BandConfig::default();
        let idle_below = steady.shrink_below;
        Self {
            steady,
            expansion_factor: 2.0,
            settle_step_factor: 0.90,
            idle_below,
            idle_to_zero_secs: 300,
            expansion_max_secs: 900,
            settle_max_secs: 900,
        }
    }
}

/// Why a [`LifecycleConfig`] is rejected at the CRD→config boundary — the typed
/// parse-time gate that keeps a malformed lifecycle out of the loop (★★
/// UNREPRESENTABILITY: parse-time-rejected).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleConfigError {
    /// The steady [`BandConfig`] is itself malformed (its typed reason folded in).
    BadSteadyBand(crate::BandConfigError),
    /// `expansion_factor < 1.0` — a startup band that does not expand.
    BadExpansionFactor,
    /// `settle_step_factor ∉ (0, 1)` — a step that does not descend.
    BadStepFactor,
    /// `idle_below ∉ (0, 1]` — a nonsensical idle threshold.
    BadIdleThreshold,
    /// A required duration (`idle_to_zero_secs` / `expansion_max_secs` /
    /// `settle_max_secs`) is zero — a timer that never fires would trap a phase.
    ZeroDuration,
}

impl std::fmt::Display for LifecycleConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadSteadyBand(e) => write!(f, "steady band invalid: {e}"),
            Self::BadExpansionFactor => f.write_str("expansion_factor must be ≥ 1.0"),
            Self::BadStepFactor => f.write_str("settle_step_factor must be in (0, 1)"),
            Self::BadIdleThreshold => f.write_str("idle_below must be in (0, 1]"),
            Self::ZeroDuration => f.write_str("idle_to_zero / expansion_max / settle_max must be > 0"),
        }
    }
}

impl std::error::Error for LifecycleConfigError {}

impl LifecycleConfig {
    /// Reject a malformed lifecycle config at parse time — before it drives a tick.
    /// A well-formed steady band, a real expansion, a descending step, a sane idle
    /// threshold, and non-zero timers (so no phase timeout can be infinite — the
    /// never-stuck timers must actually fire).
    ///
    /// # Errors
    /// A typed [`LifecycleConfigError`] naming the first violated invariant.
    pub fn validate(&self) -> Result<(), LifecycleConfigError> {
        self.steady.validate().map_err(LifecycleConfigError::BadSteadyBand)?;
        if self.expansion_factor < 1.0 || !self.expansion_factor.is_finite() {
            return Err(LifecycleConfigError::BadExpansionFactor);
        }
        if !(self.settle_step_factor > 0.0 && self.settle_step_factor < 1.0) {
            return Err(LifecycleConfigError::BadStepFactor);
        }
        if !(self.idle_below > 0.0 && self.idle_below <= 1.0) {
            return Err(LifecycleConfigError::BadIdleThreshold);
        }
        if self.idle_to_zero_secs == 0 || self.expansion_max_secs == 0 || self.settle_max_secs == 0 {
            return Err(LifecycleConfigError::ZeroDuration);
        }
        Ok(())
    }

    /// The generous startup EXPANSION limit for a workload whose observed (or
    /// requested) startup working set is `startup_ws` bytes: `ceil(startup_ws ×
    /// expansion_factor / setpoint)`, clamped into the steady band's operating range.
    /// A workload with no reading yet gets `expansion_factor ×` the steady floor —
    /// never zero, never below the request floor (uncertainty → expand, rule (a)).
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn expansion_limit(&self, startup_ws: u64) -> u64 {
        let setpoint = if self.steady.setpoint <= 0.0 { 1.0 } else { self.steady.setpoint };
        let seed = startup_ws.max(self.steady.request_floor_bytes).max(self.steady.floor_bytes);
        let expanded = ((seed as f64) * self.expansion_factor / setpoint).ceil() as u64;
        expanded.clamp(self.steady.floor_bytes, self.steady.ceiling_bytes).max(1)
    }

    /// The steady-state TARGET limit the tighten trajectory descends toward, given the
    /// confirmed reading: `ceil(used / setpoint)`, floored by the never-OOM floor. This
    /// is exactly [`crate::soft_min`] on the confirmed working set — the tighten
    /// trajectory lands the vertical law's own setpoint target, no new algebra.
    #[must_use]
    pub fn steady_target(&self, proof: &Confirmed) -> u64 {
        crate::soft_min(proof.used(), &self.steady)
    }

    /// The proven-need FLOOR for a tighten step (rule (c)): the never-OOM floor
    /// ([`crate::safe_min`], keyed on the demonstrated peak + request + config floor)
    /// raised by the STARTUP-OBSERVED MINIMUM — breathe never tightens below either
    /// what the workload was *seen* to peak at OR what it was *seen* to need at
    /// startup.
    #[must_use]
    pub fn tighten_floor(&self, proof: &Confirmed, startup_observed_min: u64) -> u64 {
        crate::safe_min(proof.peak(), proof.used(), &self.steady).max(startup_observed_min)
    }
}

/// Why the workload struggled (drives the typed expand receipt).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StruggleReason {
    /// The last termination was an OOM-kill.
    Oom,
    /// The workload restarted / is crash-looping.
    Restart,
    /// A readiness/liveness probe is failing.
    ProbeFail,
    /// Metrics are missing / anomalous (the uncertainty case — expand, never starve).
    Uncertainty,
}

impl StruggleReason {
    /// Stable label (receipt / logging).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Oom => "oom",
            Self::Restart => "restart",
            Self::ProbeFail => "probe-fail",
            Self::Uncertainty => "uncertainty",
        }
    }
}

/// Why a resting workload woke (drives the typed wake receipt).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeReason {
    /// Work-rate arrived (RPS / queue depth).
    Load,
    /// A schedule window opened (cron / predictive).
    Schedule,
    /// The startup/readiness probe armed.
    Readiness,
    /// The workload's dependencies became ready.
    Dependency,
}

impl WakeReason {
    /// Stable label (receipt / logging).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Load => "load",
            Self::Schedule => "schedule",
            Self::Readiness => "readiness",
            Self::Dependency => "dependency",
        }
    }
}

/// The verdict of one lifecycle tick — every arm observable (a typed receipt), no
/// silent outcome. Mirrors the exhaustive-enumeration discipline of
/// [`crate::Decision`] and [`crate::replica::ReplicaDecision`]. A `Tighten` is the
/// ONLY arm carrying a mutation of the vertical limit downward, and it carries a
/// [`ShadowProvenStep`] — so a downward carve without a proven metric is
/// unrepresentable (rule (a)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleDecision {
    /// In-phase, nothing to do this tick (still cold-starting, still idle-debouncing,
    /// or genuinely at rest). Never returned when a phase timeout has elapsed — the
    /// never-stuck timers force progress.
    Hold,
    /// Zero → Wake: seat the generous startup band. `reason` is the wake trigger.
    Wake { expansion_limit: u64, reason: WakeReason },
    /// Grow headroom WITHIN Wake/Settle/Steady without changing phase — the
    /// uncertainty (rule (a)) / struggle (rule (b)) response. Never a downward move.
    Expand { to_limit: u64, reason: StruggleReason },
    /// Settle: one shadow-tighten step. `shadow_only` ⇒ observe + attest, do not
    /// write (rule (d)); `false` ⇒ the step was shadow-confirmed and now effects.
    Tighten { step: ShadowProvenStep, shadow_only: bool },
    /// Settle → Steady: the trajectory reached the setpoint. `limit` is the seated
    /// steady limit.
    ReachedSetpoint { limit: u64 },
    /// Settle/Steady → Wake: a struggling workload always expands (rule (b) exit).
    ReExpand { expansion_limit: u64, reason: StruggleReason },
    /// Steady → Idle: sustained idle; enter the drain-ahead window. `time_to_zero_secs`
    /// is how long Idle rests before Zero.
    Idle { time_to_zero_secs: u64 },
    /// Idle → Steady: load returned before the timer elapsed (the workload is warm).
    Reload,
    /// Idle → Zero: the idle timer elapsed (or cost / schedule forced it) — scale to
    /// zero.
    ScaleToZero,
}

impl LifecycleDecision {
    /// The phase this decision moves TO, given the CURRENT phase — the runtime mirror
    /// of the typed [`Lifecycle<P>`] transition. Every value here is a [`legal_edges`]
    /// edge or a same-phase hold; the `phase_edges_are_legal` test proves it.
    #[must_use]
    pub fn next_phase(self, current: PhaseTag) -> PhaseTag {
        match self {
            Self::Wake { .. } => PhaseTag::Wake,
            Self::Tighten { .. } | Self::Hold | Self::Expand { .. } => current,
            Self::ReachedSetpoint { .. } | Self::Reload => PhaseTag::Steady,
            Self::ReExpand { .. } => PhaseTag::Wake,
            Self::Idle { .. } => PhaseTag::Idle,
            Self::ScaleToZero => PhaseTag::Zero,
        }
    }

    /// `true` iff this decision moves the vertical limit DOWNWARD (a tighten that
    /// effects). The never-OOM audit: every `true` here carries a [`ShadowProvenStep`]
    /// clamped to the proven-need floor.
    #[must_use]
    pub fn is_downward_carve(self) -> bool {
        matches!(self, Self::Tighten { shadow_only: false, .. })
    }
}

// ===========================================================================
// The fusion + the tick planner (the pure interpreter)
// ===========================================================================

/// The reactive nervous system's fusion — the pure heart of the lifecycle breath. It
/// takes the current [`PhaseTag`], the fused [`Signals`], the config, and the carried
/// state, and emits a [`LifecycleDecision`]. NO I/O; the whole FSM is unit-testable.
///
/// The never-stuck guards run FIRST, before any phase-specific logic, so no phase can
/// starve or trap:
///   1. UNCERTAINTY (rule (a)): a missing/anomalous metric in any non-Zero phase →
///      [`Expand`](LifecycleDecision::Expand) (or [`ReExpand`](LifecycleDecision::ReExpand)
///      from Settle/Steady), never a tighten, never a hold-into-starve.
///   2. STRUGGLE (rule (b)): a health regression / not-ready probe → expand.
/// Only if neither fires does the phase logic run, and every phase's timeout escape
/// guarantees a non-`Hold` progressing decision eventually (rule (b): no trap).
#[must_use]
pub fn fuse(phase: PhaseTag, cfg: &LifecycleConfig, sig: &Signals, carry: &LifecycleCarry, gate: LifecycleGate) -> LifecycleDecision {
    // Zero rests until a wake trigger; uncertainty at Zero is fine (nothing to starve).
    if phase == PhaseTag::Zero {
        return match sig.any_wake() {
            Some(reason) => {
                let seed = sig.metrics.used.unwrap_or(0);
                LifecycleDecision::Wake { expansion_limit: cfg.expansion_limit(seed), reason }
            }
            None => LifecycleDecision::Hold,
        };
    }

    // ---- never-stuck guard 1 + 2: uncertainty / struggle → EXPAND, never starve ----
    // Try to CONFIRM the metric; a rejection means uncertainty (rule (a)). A struggle
    // signal (rule (b)) forces expansion even if the metric confirms.
    let confirmed = Confirmed::parse(sig.metrics, cfg.steady.warmup_seconds);
    let struggling = sig.is_struggling();
    if confirmed.is_err() || struggling {
        let reason = sig.struggle_reason();
        // Expand from the current limit; never below it. Grow by the expansion factor,
        // clamped to the steady ceiling. From Settle/Steady this is a phase-changing
        // ReExpand (→ Wake); from Wake it is an in-phase Expand.
        let grown = grow_headroom(carry.current_limit, cfg);
        return match phase {
            PhaseTag::Settle | PhaseTag::Steady => LifecycleDecision::ReExpand { expansion_limit: grown, reason },
            _ => LifecycleDecision::Expand { to_limit: grown, reason },
        };
    }
    // SAFETY: `confirmed` is `Ok` here (the `is_err()` arm returned above).
    let proof = confirmed.expect("confirmed metric past the uncertainty guard");

    match phase {
        PhaseTag::Zero => LifecycleDecision::Hold, // unreachable (handled above), exhaustive.
        PhaseTag::Wake => fuse_wake(cfg, sig, &proof, carry),
        PhaseTag::Settle => fuse_settle(cfg, sig, &proof, carry, gate),
        PhaseTag::Steady => fuse_steady(cfg, sig, &proof, carry),
        PhaseTag::Idle => fuse_idle(cfg, sig, carry),
    }
}

/// Grow the current limit by the expansion factor, clamped into the steady band's
/// operating range and never below the current limit (a grow only ever buys headroom).
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn grow_headroom(current: u64, cfg: &LifecycleConfig) -> u64 {
    let factor = if cfg.expansion_factor >= 1.0 { cfg.expansion_factor } else { 1.0 };
    let grown = ((current.max(cfg.steady.floor_bytes) as f64) * factor).ceil() as u64;
    grown.clamp(current.max(cfg.steady.floor_bytes), cfg.steady.ceiling_bytes).max(current)
}

/// Wake logic: hold the generous band until the workload STABILIZES (a confirmed
/// reading below the expansion setpoint, past warmup), then begin Settle. A never-
/// stuck TIMEOUT forces Settle if Wake has run longer than `expansion_max_secs` — Wake
/// can never trap.
fn fuse_wake(cfg: &LifecycleConfig, sig: &Signals, proof: &Confirmed, carry: &LifecycleCarry) -> LifecycleDecision {
    let age = sig.now_epoch.saturating_sub(carry.phase_entered_epoch);
    let timed_out = age >= 0 && (age as u64) >= cfg.expansion_max_secs;
    // Stabilized when utilization has settled comfortably below the steady setpoint
    // under the generous band (there is slack to reclaim) — the confirm the metric
    // already gives (past-warmup, non-zero) plus a below-setpoint reading.
    let stabilized = proof.util() < cfg.steady.setpoint;
    if stabilized || timed_out {
        // Settle → the caller advances the phase; the first Settle tick plans the
        // first tighten step. Signal readiness to move by ReachedSetpoint's sibling:
        // we model "begin settle" as a Tighten shadow of the first step so the wire is
        // uniform. Compute the first step here.
        let floor = cfg.tighten_floor(proof, carry.startup_observed_min);
        let steady_target = cfg.steady_target(proof);
        match ShadowProvenStep::plan(carry.current_limit, steady_target, floor, cfg.settle_step_factor, proof) {
            // There IS slack to tighten — begin the descent (shadow the first step).
            Some(step) => LifecycleDecision::Tighten { step, shadow_only: true },
            // Already at/under the setpoint target — nothing to tighten; go steady.
            None => LifecycleDecision::ReachedSetpoint { limit: carry.current_limit.max(floor) },
        }
    } else {
        LifecycleDecision::Hold // still cold-starting under the generous band.
    }
}

/// Settle logic: drive the shadow-tighten trajectory step by step. Each step is
/// SHADOWED first (`shadow_only`), and only EFFECTS once [`LifecycleCarry::shadow_confirmed_for`]
/// marks this `from` clean (rule (d)). When there is no step left, reach the setpoint.
/// A never-stuck TIMEOUT forces ReachedSetpoint at the current proven-need floor if
/// Settle has run longer than `settle_max_secs` — Settle can never trap.
fn fuse_settle(cfg: &LifecycleConfig, sig: &Signals, proof: &Confirmed, carry: &LifecycleCarry, gate: LifecycleGate) -> LifecycleDecision {
    let age = sig.now_epoch.saturating_sub(carry.phase_entered_epoch);
    let timed_out = age >= 0 && (age as u64) >= cfg.settle_max_secs;
    let floor = cfg.tighten_floor(proof, carry.startup_observed_min);
    let steady_target = cfg.steady_target(proof);

    match ShadowProvenStep::plan(carry.current_limit, steady_target, floor, cfg.settle_step_factor, proof) {
        None => LifecycleDecision::ReachedSetpoint { limit: carry.current_limit.max(floor) },
        Some(step) => {
            if timed_out {
                // Escape: stop stepping, land at the current floor-clamped limit.
                return LifecycleDecision::ReachedSetpoint { limit: carry.current_limit.max(floor) };
            }
            // Shadow unless this exact `from` has been confirmed clean AND the tick is
            // live (not a dry-run band). A dry-run band always shadows (never writes).
            let confirmed_clean = carry.shadow_confirmed_for == Some(step.from());
            let shadow_only = gate.dry_run || !confirmed_clean;
            LifecycleDecision::Tighten { step, shadow_only }
        }
    }
}

/// Steady logic: hold at the setpoint until sustained idle arms the Idle candidate. A
/// cost-over-budget or an off-schedule signal also arms Idle (cost-driven zero). The
/// vertical [`crate::decide`] law runs UNCHANGED in this phase for right-sizing — this
/// only governs the lifecycle transition.
fn fuse_steady(cfg: &LifecycleConfig, sig: &Signals, proof: &Confirmed, _carry: &LifecycleCarry) -> LifecycleDecision {
    let idle_by_util = proof.util() < cfg.idle_below && !sig.load.is_some_and(Load::is_active);
    let idle_by_cost = sig.cost_over_budget;
    let idle_by_schedule = sig.schedule_active == Some(false) && !sig.load.is_some_and(Load::is_active);
    if idle_by_util || idle_by_cost || idle_by_schedule {
        LifecycleDecision::Idle { time_to_zero_secs: cfg.idle_to_zero_secs }
    } else {
        LifecycleDecision::Hold
    }
}

/// Idle logic: the drain-ahead window. Load returning (or a spot reclaim needing the
/// workload back) RELOADS to Steady; the idle timer elapsing (or cost forcing it)
/// scales to Zero. Never a trap: either the timer fires (→ Zero) or load returns
/// (→ Steady).
fn fuse_idle(cfg: &LifecycleConfig, sig: &Signals, carry: &LifecycleCarry) -> LifecycleDecision {
    // Load returned before the timer → reload (the workload is warm; no cold-start).
    if sig.load.is_some_and(Load::is_active) || sig.schedule_active == Some(true) {
        return LifecycleDecision::Reload;
    }
    let age = sig.now_epoch.saturating_sub(carry.phase_entered_epoch);
    let elapsed = age >= 0 && (age as u64) >= cfg.idle_to_zero_secs;
    if elapsed || sig.cost_over_budget {
        LifecycleDecision::ScaleToZero
    } else {
        LifecycleDecision::Hold // still draining ahead.
    }
}

/// The pure encoding of the shadow / cooldown gate applied to a lifecycle DECISION
/// before it reaches the actuator — mirrors [`crate::replica::ReplicaGate`]. The async
/// controller resolves `dry_run` (a `ShadowConfirmEffect` band before its confirm
/// window, or an explicit shadow mode) + `in_cooldown` (the post-carve window) and
/// hands it here; [`plan_lifecycle_tick`] then decides whether — and to what — the
/// vertical limit is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleGate {
    /// SHADOW: a tighten observes + attests, never writes (the effective dry-run for
    /// this tick).
    pub dry_run: bool,
    /// Within the post-carve cooldown window — a tighten may be due but is held.
    pub in_cooldown: bool,
}

/// The pure outcome of planning one lifecycle tick — the decision, the phase it moves
/// to, what the actuator should write, and whether to route a scale-to-zero. The
/// controller's async shell does the observe + the SSA write; this value tells it what
/// to do. Mirrors [`crate::replica::ReplicaTickPlan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleTickPlan {
    /// The fused decision for this tick.
    pub decision: LifecycleDecision,
    /// The phase to persist to status after this tick.
    pub next_phase: PhaseTag,
    /// `Some(limit)` ⇒ SSA-write the vertical limit; `None` ⇒ observe only (a shadow
    /// tighten, a held tick, a resting decision, or a scale-to-zero routed elsewhere).
    pub actuate: Option<u64>,
    /// `true` ⇒ route a scale-to-zero (Idle → Zero) to the replica band / scaler.
    pub scale_to_zero: bool,
}

/// Why a lifecycle tick could not be planned — the typed error the caller surfaces
/// (never a panic). Mirrors [`crate::replica::ReplicaError`]'s discipline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleError {
    /// The [`LifecycleConfig`] is malformed — rejected at the parse gate.
    BadConfig(LifecycleConfigError),
}

impl std::fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadConfig(e) => write!(f, "lifecycle config invalid: {e}"),
        }
    }
}

impl std::error::Error for LifecycleError {}

/// Plan one lifecycle tick: validate → sense the nervous system → fuse → resolve the
/// gate into an actuation. PURE except the single [`NervousSystem::metrics`] +
/// signal reads through the (mockable) environment — the whole FSM + gate is unit-
/// testable without a cluster (the TYPED-SPEC triplet's planning peer).
///
/// A `Tighten { shadow_only }` actuates the vertical limit ONLY when it is confirmed
/// (`shadow_only == false`) AND not in cooldown; a shadow tighten, a hold, an expand,
/// or a resting decision never writes downward. An `Expand`/`Wake`/`ReExpand` always
/// actuates UPWARD (buying headroom is always safe — never gated).
///
/// # Errors
/// [`LifecycleError::BadConfig`] when the config fails its parse-time gate.
pub fn plan_lifecycle_tick<E: NervousSystem>(
    phase: PhaseTag,
    cfg: &LifecycleConfig,
    env: &E,
    gate: LifecycleGate,
    carry: &LifecycleCarry,
) -> Result<LifecycleTickPlan, LifecycleError> {
    cfg.validate().map_err(LifecycleError::BadConfig)?;
    let sig = sense(env);
    let decision = fuse(phase, cfg, &sig, carry, gate);
    let next_phase = decision.next_phase(phase);

    let (actuate, scale_to_zero) = match decision {
        // UPWARD moves are never gated — buying headroom is always safe.
        LifecycleDecision::Wake { expansion_limit, .. } => (Some(expansion_limit), false),
        LifecycleDecision::Expand { to_limit, .. } => (Some(to_limit), false),
        LifecycleDecision::ReExpand { expansion_limit, .. } => (Some(expansion_limit), false),
        // A DOWNWARD tighten writes only when confirmed + live + not cooling down.
        LifecycleDecision::Tighten { step, shadow_only } => {
            if shadow_only || gate.in_cooldown {
                (None, false)
            } else {
                (Some(step.to()), false)
            }
        }
        // Reaching the setpoint seats the steady limit.
        LifecycleDecision::ReachedSetpoint { limit } => (Some(limit), false),
        // Scale-to-zero routes to the scaler, not a vertical write.
        LifecycleDecision::ScaleToZero => (None, true),
        // Holds / idle-arming / reload change no limit this tick.
        LifecycleDecision::Hold | LifecycleDecision::Idle { .. } | LifecycleDecision::Reload => (None, false),
    };

    Ok(LifecycleTickPlan { decision, next_phase, actuate, scale_to_zero })
}

/// A canned [`NervousSystem`] for tests + shadow dry-runs — every input is a field, so
/// a test drives the interpreter with zero I/O. Mirrors
/// [`crate::replica::MockReplicaEnvironment`].
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MockNervousSystem {
    /// The utilization observation returned by [`NervousSystem::metrics`].
    pub metrics: Metrics,
    /// Readiness (`None` = unknown).
    pub readiness: Option<bool>,
    /// Load (`None` = not read).
    pub load: Option<Load>,
    /// Health.
    pub health: Health,
    /// Dependency readiness (`None` = unknown).
    pub dependency_ready: Option<bool>,
    /// Spot reclaim pending.
    pub spot_reclaim_pending: bool,
    /// Schedule active (`None` = unknown).
    pub schedule_active: Option<bool>,
    /// Over cost budget.
    pub cost_over_budget: bool,
    /// The tick's wall-clock epoch.
    pub now_epoch: i64,
}

impl NervousSystem for MockNervousSystem {
    fn metrics(&self) -> Metrics {
        self.metrics
    }
    fn readiness(&self) -> Option<bool> {
        self.readiness
    }
    fn load(&self) -> Option<Load> {
        self.load
    }
    fn health(&self) -> Health {
        self.health
    }
    fn dependency_ready(&self) -> Option<bool> {
        self.dependency_ready
    }
    fn spot_reclaim_pending(&self) -> bool {
        self.spot_reclaim_pending
    }
    fn schedule_active(&self) -> Option<bool> {
        self.schedule_active
    }
    fn cost_over_budget(&self) -> bool {
        self.cost_over_budget
    }
    fn now_epoch(&self) -> i64 {
        self.now_epoch
    }
}

// ===========================================================================
// LiveTODOs — this-delivery is the pure typed core + tests. The live-controller /
// cluster-integration parts are NAMED here, not faked (tier-honest per the
// LiveTODO honesty-gate). Each is a controller/provider wiring task, NOT new algebra:
//
//   LiveTODO(breathe-kube): implement the `NervousSystem` signal readers against the
//     live cluster — readiness from pod.status.conditions[Ready], health from
//     container restartCount deltas + lastState.terminated.reason==OOMKilled +
//     probe-failure events, load from a KEDA/ingress-nginx RPS+queue metric, schedule
//     from a cron evaluation, dependency from a HelmRelease/Application readiness,
//     cost_over_budget from a Viggy CostBudget promessa. `metrics` reuses the existing
//     MetricSource::PodMetricsMax path.
//   LiveTODO(breathe-crd): add `LifecycleSpec { enabled, expansion_factor,
//     idle_to_zero_secs, settle_step_factor, expansion_max_secs, settle_max_secs }` to
//     each Band spec + `BandStatus { lifecycle_phase: PhaseTag-as-string,
//     phase_entered_epoch, startup_observed_min, shadow_confirmed_for }` so the FSM is
//     durable across controller restarts (reconstruct LifecycleCarry from status).
//   LiveTODO(breathe-controller): call `plan_lifecycle_tick` when
//     band.spec.lifecycle.enabled, route `actuate` to the vertical carve, route
//     `scale_to_zero` to the ReplicaBand / KEDA scaler, apply the typed phantom
//     transition (`Lifecycle<P>::wake/settle/...`) to advance the persisted phase, and
//     run the per-step shadow→confirm window (set `shadow_confirmed_for` after a clean
//     window; call `tighten_settled` on the pre/post ControlQuality to revert a
//     harmful step → drive `re_expand`).
//   LiveTODO(breathe-controller): learn the STARTUP-OBSERVED MINIMUM during Wake
//     (fold the min confirmed `used` into `carry.startup_observed_min`) and freeze it
//     at `settle` — the durable proven-need floor (rule (c)); wire it as a third floor
//     source alongside the peak floor + config floor.
//   LiveTODO(breathe-catalog): add a `LifecycleOrchestrator` catalog row (a peer
//     SUBSYSTEM, NOT a new DimensionId — do not touch the DimensionId bijection) so the
//     lifecycle's required signal sources are audit-visible via CATALOG REFLECTION.
//   LiveTODO(breathe-config): a `LifecycleServiceConfig` (shikumi) resolving
//     expansion_factor / idle timings via override ← discovered ← prescribed_default,
//     alongside `ScaleConfig`.
//   LiveTODO(retirada): route `spot_reclaim_pending` into a proactive Idle→drain (a
//     reclaim during Steady/Idle pre-drains ahead of capacity leaving) once the
//     ReplicaBand SpotScaleOut peer is wired.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lapidar::ControlQuality;

    fn cfg() -> LifecycleConfig {
        LifecycleConfig::default()
    }

    // A confirmed-able metric: past warmup, non-zero used, real capacity.
    fn metrics(used: u64, capacity: u64) -> Metrics {
        Metrics { used: Some(used), capacity, observed_for_secs: 10_000, peak_used: used }
    }

    fn healthy_env(m: Metrics) -> MockNervousSystem {
        MockNervousSystem { metrics: m, now_epoch: 1_000_000, ..MockNervousSystem::default() }
    }

    // ---- config parse gate (parse-time-rejected tier) ----

    #[test]
    fn config_default_validates() {
        assert!(cfg().validate().is_ok());
    }

    #[test]
    fn config_rejects_non_expanding_factor() {
        let c = LifecycleConfig { expansion_factor: 0.9, ..cfg() };
        assert_eq!(c.validate(), Err(LifecycleConfigError::BadExpansionFactor));
    }

    #[test]
    fn config_rejects_non_descending_step() {
        for bad in [0.0, 1.0, 1.5, -0.1] {
            let c = LifecycleConfig { settle_step_factor: bad, ..cfg() };
            assert_eq!(c.validate(), Err(LifecycleConfigError::BadStepFactor), "step {bad}");
        }
    }

    #[test]
    fn config_rejects_zero_timers() {
        for c in [
            LifecycleConfig { idle_to_zero_secs: 0, ..cfg() },
            LifecycleConfig { expansion_max_secs: 0, ..cfg() },
            LifecycleConfig { settle_max_secs: 0, ..cfg() },
        ] {
            assert_eq!(c.validate(), Err(LifecycleConfigError::ZeroDuration));
        }
    }

    #[test]
    fn config_folds_in_bad_steady_band() {
        let steady = BandConfig { setpoint: 2.0, ..BandConfig::default() };
        let c = LifecycleConfig { steady, ..cfg() };
        assert!(matches!(c.validate(), Err(LifecycleConfigError::BadSteadyBand(_))));
    }

    #[test]
    fn plan_rejects_bad_config() {
        let c = LifecycleConfig { expansion_factor: 0.5, ..cfg() };
        let env = healthy_env(metrics(500 << 20, 1 << 30));
        let r = plan_lifecycle_tick(PhaseTag::Steady, &c, &env, LifecycleGate { dry_run: false, in_cooldown: false }, &LifecycleCarry::default());
        assert!(matches!(r, Err(LifecycleError::BadConfig(_))));
    }

    // ---- Confirmed parse boundary (rule (a), parse-time-rejected) ----

    #[test]
    fn confirmed_rejects_missing_metric() {
        let m = Metrics { used: None, capacity: 1 << 30, observed_for_secs: 10_000, peak_used: 0 };
        assert_eq!(Confirmed::parse(m, 600), Err(Uncertainty::MetricMissing));
    }

    #[test]
    fn confirmed_rejects_zero_used() {
        let m = Metrics { used: Some(0), capacity: 1 << 30, observed_for_secs: 10_000, peak_used: 0 };
        assert_eq!(Confirmed::parse(m, 600), Err(Uncertainty::ZeroUsed));
    }

    #[test]
    fn confirmed_rejects_no_capacity() {
        let m = Metrics { used: Some(100), capacity: 0, observed_for_secs: 10_000, peak_used: 100 };
        assert_eq!(Confirmed::parse(m, 600), Err(Uncertainty::NoCapacity));
    }

    #[test]
    fn confirmed_rejects_within_warmup() {
        let m = Metrics { used: Some(100), capacity: 1 << 30, observed_for_secs: 30, peak_used: 100 };
        assert_eq!(Confirmed::parse(m, 600), Err(Uncertainty::WithinWarmup { observed_for: 30, warmup: 600 }));
    }

    #[test]
    fn confirmed_accepts_a_good_reading() {
        let m = metrics(400 << 20, 1 << 30);
        let c = Confirmed::parse(m, 600).expect("good reading confirms");
        assert_eq!(c.used(), 400 << 20);
        assert!((c.util() - (400.0 / 1024.0)).abs() < 1e-6);
    }

    // ---- ShadowProvenStep (rules (a), (c)) ----

    #[test]
    fn step_is_monotone_and_floored() {
        let proof = Confirmed::parse(metrics(400 << 20, 4 << 30), 600).unwrap();
        let floor = 500 << 20;
        // Descend from 4Gi toward a 600Mi target, floored at 500Mi.
        let mut from = 4u64 << 30;
        let steady_target = 600 << 20;
        let mut steps = 0;
        while let Some(step) = ShadowProvenStep::plan(from, steady_target, floor, 0.90, &proof) {
            assert!(step.to() < step.from(), "monotone descent: {} < {}", step.to(), step.from());
            assert!(step.to() >= floor, "never below the proven-need floor: {} >= {}", step.to(), floor);
            from = step.to();
            steps += 1;
            assert!(steps < 1000, "must terminate");
        }
        // Terminates at or above the floor (the higher of steady_target / floor = floor here).
        assert!(from >= floor);
        assert!(steps > 0, "there was slack to tighten");
    }

    #[test]
    fn step_none_when_already_tight() {
        let proof = Confirmed::parse(metrics(400 << 20, 512 << 20), 600).unwrap();
        // from already below max(steady_target, floor) → no step.
        assert!(ShadowProvenStep::plan(400 << 20, 600 << 20, 700 << 20, 0.90, &proof).is_none());
    }

    #[test]
    fn step_floor_binds_over_steady_target() {
        // A proven-need floor ABOVE the steady target: the descent bottoms at the floor.
        let proof = Confirmed::parse(metrics(300 << 20, 4 << 30), 600).unwrap();
        let floor = 1u64 << 30; // 1Gi proven-need floor
        let steady_target = 300 << 20; // 300Mi setpoint target (below the floor)
        let mut from = 4u64 << 30;
        while let Some(step) = ShadowProvenStep::plan(from, steady_target, floor, 0.90, &proof) {
            assert!(step.to() >= floor, "floor dominates: {} >= {}", step.to(), floor);
            from = step.to();
        }
        assert!(from >= floor);
    }

    // ---- the phantom FSM (rule (b): transition legality, truly-unrep) ----

    #[test]
    fn typed_happy_path_cycle() {
        let z = Lifecycle::<Zero>::start(0);
        let w = z.wake(2 << 30, 10);
        assert_eq!(w.tag(), PhaseTag::Wake);
        assert_eq!(w.carry().current_limit, 2 << 30);
        let s = w.settle(20);
        assert_eq!(s.tag(), PhaseTag::Settle);
        let steady = s.reach_setpoint(30);
        assert_eq!(steady.tag(), PhaseTag::Steady);
        let idle = steady.go_idle(40);
        assert_eq!(idle.tag(), PhaseTag::Idle);
        let back = idle.rest(50);
        assert_eq!(back.tag(), PhaseTag::Zero);
    }

    #[test]
    fn typed_struggle_and_reload_exits() {
        // Settle → Wake (struggling re-expand).
        let s = Lifecycle::<Zero>::start(0).wake(2 << 30, 1).settle(2);
        let re = s.re_expand(3 << 30, 3);
        assert_eq!(re.tag(), PhaseTag::Wake);
        assert!(re.carry().current_limit >= 3 << 30);
        // Steady → Wake (struggling re-expand) + Idle → Steady (reload).
        let steady = Lifecycle::<Zero>::start(0).wake(1 << 30, 1).settle(2).reach_setpoint(3);
        assert_eq!(steady.re_expand(2 << 30, 4).tag(), PhaseTag::Wake);
        let idle = Lifecycle::<Zero>::start(0).wake(1 << 30, 1).settle(2).reach_setpoint(3).go_idle(4);
        assert_eq!(idle.reload(5).tag(), PhaseTag::Steady);
    }

    // ---- never-stuck: every phase has an exit + reaches BOTH good terminals (C1 CI) ----

    #[test]
    fn every_phase_has_a_legal_exit() {
        // Compile-time asserts already guarantee non-empty; this mirrors at runtime.
        for p in PhaseTag::all() {
            assert!(!p.exits().is_empty(), "{} is a dead-end", p.as_str());
        }
    }

    #[test]
    fn every_phase_reaches_both_good_resting_states() {
        // BFS the legal-edge graph from every phase; each must reach BOTH Zero AND
        // Steady — the never-stuck convergence property (no phase is a trap; a
        // workload can always get to right-sized-steady if load persists and to
        // cheapest-zero if idle persists). Mechanical CI forcing-function (C1) — Rust
        // cannot prove the reachability quantifier as a type.
        let edges = legal_edges();
        for start in PhaseTag::all() {
            let mut seen = vec![start];
            let mut frontier = vec![start];
            while let Some(cur) = frontier.pop() {
                for &(a, b) in &edges {
                    if a == cur && !seen.contains(&b) {
                        seen.push(b);
                        frontier.push(b);
                    }
                }
            }
            assert!(seen.contains(&PhaseTag::Steady), "{} cannot reach Steady", start.as_str());
            assert!(seen.contains(&PhaseTag::Zero), "{} cannot reach Zero", start.as_str());
        }
    }

    #[test]
    fn no_dead_ends_and_edges_are_within_the_phase_set() {
        let edges = legal_edges();
        for p in PhaseTag::all() {
            assert!(edges.iter().any(|&(a, _)| a == p), "{} has no outgoing edge", p.as_str());
        }
    }

    // ---- fusion decisions map to LEGAL edges (typed FSM ↔ runtime mirror) ----

    #[test]
    fn phase_edges_are_legal() {
        // Every decision fuse can emit from every phase must yield a next_phase that
        // is either the same phase (a hold/expand/shadow) or a LEGAL edge — proving the
        // runtime mirror never diverges from the typed phantom graph.
        let legal = legal_edges();
        let is_ok = |from: PhaseTag, to: PhaseTag| from == to || legal.contains(&(from, to));
        let c = cfg();
        // Exhaustive over phases × a spread of signal shapes.
        for phase in PhaseTag::all() {
            for sig in signal_matrix() {
                for dry in [false, true] {
                    let carry = LifecycleCarry { current_limit: 2 << 30, phase_entered_epoch: 0, ..LifecycleCarry::default() };
                    let d = fuse(phase, &c, &sig, &carry, LifecycleGate { dry_run: dry, in_cooldown: false });
                    let np = d.next_phase(phase);
                    assert!(is_ok(phase, np), "illegal edge {} → {} from {:?}", phase.as_str(), np.as_str(), d);
                }
            }
        }
    }

    // ---- never-stuck rule (a): uncertainty NEVER tightens, ALWAYS expands ----

    #[test]
    fn uncertainty_never_tightens_and_expands_in_running_phases() {
        let c = cfg();
        // Every "uncertain" metric shape in a non-Zero phase → NOT a downward tighten.
        let uncertain = [
            Metrics { used: None, capacity: 1 << 30, observed_for_secs: 10_000, peak_used: 0 }, // missing
            Metrics { used: Some(0), capacity: 1 << 30, observed_for_secs: 10_000, peak_used: 0 }, // zero
            Metrics { used: Some(100), capacity: 0, observed_for_secs: 10_000, peak_used: 100 }, // no cap
            Metrics { used: Some(100), capacity: 1 << 30, observed_for_secs: 5, peak_used: 100 }, // warmup
        ];
        for phase in [PhaseTag::Wake, PhaseTag::Settle, PhaseTag::Steady, PhaseTag::Idle] {
            for m in uncertain {
                let sig = Signals {
                    metrics: m, readiness: None, load: None, health: Health::default(),
                    dependency_ready: None, spot_reclaim_pending: false, schedule_active: None,
                    cost_over_budget: false, now_epoch: 100,
                };
                let carry = LifecycleCarry { current_limit: 1 << 30, ..LifecycleCarry::default() };
                let d = fuse(phase, &c, &sig, &carry, LifecycleGate { dry_run: false, in_cooldown: false });
                assert!(!d.is_downward_carve(), "uncertainty must not tighten: {phase:?} {m:?} → {d:?}");
                // In the running phases the uncertainty response is an expand/re-expand
                // that never drops below the current limit.
                if matches!(phase, PhaseTag::Wake | PhaseTag::Settle | PhaseTag::Steady) {
                    match d {
                        LifecycleDecision::Expand { to_limit, .. } | LifecycleDecision::ReExpand { expansion_limit: to_limit, .. } => {
                            assert!(to_limit >= carry.current_limit, "expand never shrinks: {to_limit} >= {}", carry.current_limit);
                        }
                        other => panic!("uncertainty in {phase:?} must expand, got {other:?}"),
                    }
                }
            }
        }
    }

    #[test]
    fn struggle_always_expands_never_tightens() {
        let c = cfg();
        // A confirmable metric BUT the workload is struggling (OOM / restart / probe).
        let good = metrics(300 << 20, 4 << 30);
        let struggles = [
            Health { oom: true, ..Health::default() },
            Health { restarts: 3, ..Health::default() },
            Health { probe_failures: 2, ..Health::default() },
        ];
        for phase in [PhaseTag::Settle, PhaseTag::Steady] {
            for h in struggles {
                let sig = Signals {
                    metrics: good, readiness: None, load: None, health: h,
                    dependency_ready: None, spot_reclaim_pending: false, schedule_active: None,
                    cost_over_budget: false, now_epoch: 100,
                };
                let carry = LifecycleCarry { current_limit: 1 << 30, ..LifecycleCarry::default() };
                let d = fuse(phase, &c, &sig, &carry, LifecycleGate { dry_run: false, in_cooldown: false });
                assert!(!d.is_downward_carve(), "struggle must not tighten");
                assert!(matches!(d, LifecycleDecision::ReExpand { .. }), "struggle in {phase:?} → re-expand, got {d:?}");
            }
        }
    }

    // ---- never-stuck rule (b): no phase Holds forever — the timeout escapes ----

    #[test]
    fn wake_times_out_into_settle_progress() {
        let c = cfg();
        // A Wake that never "stabilizes" (util pinned ABOVE setpoint) must still leave
        // Wake once expansion_max_secs elapses.
        let hot = Metrics { used: Some(950 << 20), capacity: 1 << 30, observed_for_secs: 10_000, peak_used: 950 << 20 };
        let carry = LifecycleCarry { current_limit: 4 << 30, phase_entered_epoch: 0, ..LifecycleCarry::default() };
        // Before timeout: holds (still cold-starting, util above setpoint).
        let sig_early = Signals { metrics: hot, readiness: None, load: None, health: Health::default(), dependency_ready: None, spot_reclaim_pending: false, schedule_active: None, cost_over_budget: false, now_epoch: 10 };
        assert_eq!(fuse(PhaseTag::Wake, &c, &sig_early, &carry, LifecycleGate { dry_run: false, in_cooldown: false }), LifecycleDecision::Hold);
        // After timeout: forced forward (a tighten step or reached-setpoint — leaves Wake logic's hold).
        let sig_late = Signals { now_epoch: (c.expansion_max_secs + 1) as i64, ..sig_early };
        let d = fuse(PhaseTag::Wake, &c, &sig_late, &carry, LifecycleGate { dry_run: false, in_cooldown: false });
        assert!(matches!(d, LifecycleDecision::Tighten { .. } | LifecycleDecision::ReachedSetpoint { .. }), "wake must escape, got {d:?}");
    }

    #[test]
    fn settle_times_out_into_steady() {
        let c = cfg();
        // A Settle with lots of slack (so a step always exists) must still reach the
        // setpoint once settle_max_secs elapses rather than stepping forever.
        let good = metrics(300 << 20, 8 << 30);
        let carry = LifecycleCarry { current_limit: 8 << 30, phase_entered_epoch: 0, shadow_confirmed_for: None, startup_observed_min: 0 };
        let sig_late = Signals { metrics: good, readiness: None, load: None, health: Health::default(), dependency_ready: None, spot_reclaim_pending: false, schedule_active: None, cost_over_budget: false, now_epoch: (c.settle_max_secs + 1) as i64 };
        let d = fuse(PhaseTag::Settle, &c, &sig_late, &carry, LifecycleGate { dry_run: false, in_cooldown: false });
        assert!(matches!(d, LifecycleDecision::ReachedSetpoint { .. }), "settle must escape to steady, got {d:?}");
    }

    #[test]
    fn idle_times_out_into_zero() {
        let c = cfg();
        let good = metrics(10 << 20, 1 << 30); // near-idle
        let carry = LifecycleCarry { current_limit: 1 << 30, phase_entered_epoch: 0, ..LifecycleCarry::default() };
        let sig = Signals { metrics: good, readiness: None, load: Some(Load::default()), health: Health::default(), dependency_ready: None, spot_reclaim_pending: false, schedule_active: None, cost_over_budget: false, now_epoch: (c.idle_to_zero_secs + 1) as i64 };
        assert_eq!(fuse(PhaseTag::Idle, &c, &sig, &carry, LifecycleGate { dry_run: false, in_cooldown: false }), LifecycleDecision::ScaleToZero);
    }

    // ---- the wake trigger (OR-fusion) ----

    #[test]
    fn zero_wakes_on_any_trigger_and_rests_otherwise() {
        let c = cfg();
        let carry = LifecycleCarry::default();
        let base = Signals { metrics: Metrics::default(), readiness: None, load: None, health: Health::default(), dependency_ready: None, spot_reclaim_pending: false, schedule_active: None, cost_over_budget: false, now_epoch: 0 };
        // No trigger → rest.
        assert_eq!(fuse(PhaseTag::Zero, &c, &base, &carry, LifecycleGate { dry_run: false, in_cooldown: false }), LifecycleDecision::Hold);
        // Each trigger wakes.
        let load = Signals { load: Some(Load { rps: 5.0, queue_depth: 0 }), ..base };
        assert!(matches!(fuse(PhaseTag::Zero, &c, &load, &carry, LifecycleGate { dry_run: false, in_cooldown: false }), LifecycleDecision::Wake { reason: WakeReason::Load, .. }));
        let sched = Signals { schedule_active: Some(true), ..base };
        assert!(matches!(fuse(PhaseTag::Zero, &c, &sched, &carry, LifecycleGate { dry_run: false, in_cooldown: false }), LifecycleDecision::Wake { reason: WakeReason::Schedule, .. }));
        let ready = Signals { readiness: Some(true), ..base };
        assert!(matches!(fuse(PhaseTag::Zero, &c, &ready, &carry, LifecycleGate { dry_run: false, in_cooldown: false }), LifecycleDecision::Wake { reason: WakeReason::Readiness, .. }));
        let dep = Signals { dependency_ready: Some(true), ..base };
        assert!(matches!(fuse(PhaseTag::Zero, &c, &dep, &carry, LifecycleGate { dry_run: false, in_cooldown: false }), LifecycleDecision::Wake { reason: WakeReason::Dependency, .. }));
        // Cost / reclaim never WAKE a resting workload.
        let cost = Signals { cost_over_budget: true, ..base };
        assert_eq!(fuse(PhaseTag::Zero, &c, &cost, &carry, LifecycleGate { dry_run: false, in_cooldown: false }), LifecycleDecision::Hold);
    }

    // ---- rule (d): shadow → confirm → effect on the tighten step ----

    #[test]
    fn tighten_shadows_first_then_effects_after_confirm() {
        let c = cfg();
        let good = metrics(300 << 20, 8 << 30);
        let from = 8u64 << 30;
        // Unconfirmed → shadow_only (no downward write).
        let carry_shadow = LifecycleCarry { current_limit: from, phase_entered_epoch: 0, shadow_confirmed_for: None, startup_observed_min: 0 };
        let sig = Signals { metrics: good, readiness: None, load: None, health: Health::default(), dependency_ready: None, spot_reclaim_pending: false, schedule_active: None, cost_over_budget: false, now_epoch: 10 };
        let d1 = fuse(PhaseTag::Settle, &c, &sig, &carry_shadow, LifecycleGate { dry_run: false, in_cooldown: false });
        let LifecycleDecision::Tighten { step, shadow_only } = d1 else { panic!("expected tighten, got {d1:?}") };
        assert!(shadow_only, "first sight of a step shadows");
        // Confirmed for THIS from → effects.
        let carry_confirmed = LifecycleCarry { shadow_confirmed_for: Some(step.from()), ..carry_shadow };
        let d2 = fuse(PhaseTag::Settle, &c, &sig, &carry_confirmed, LifecycleGate { dry_run: false, in_cooldown: false });
        assert!(matches!(d2, LifecycleDecision::Tighten { shadow_only: false, .. }), "confirmed step effects, got {d2:?}");
        // A dry-run band always shadows, even when confirmed.
        let d3 = fuse(PhaseTag::Settle, &c, &sig, &carry_confirmed, LifecycleGate { dry_run: true, in_cooldown: false });
        assert!(matches!(d3, LifecycleDecision::Tighten { shadow_only: true, .. }), "dry-run always shadows");
    }

    #[test]
    fn plan_never_writes_downward_under_shadow_or_cooldown() {
        let c = cfg();
        let good = metrics(300 << 20, 8 << 30);
        let env = MockNervousSystem { metrics: good, now_epoch: 10, ..MockNervousSystem::default() };
        let carry = LifecycleCarry { current_limit: 8 << 30, shadow_confirmed_for: Some(8 << 30), ..LifecycleCarry::default() };
        // dry-run → no actuation.
        let p_shadow = plan_lifecycle_tick(PhaseTag::Settle, &c, &env, LifecycleGate { dry_run: true, in_cooldown: false }, &carry).unwrap();
        assert_eq!(p_shadow.actuate, None, "shadow tighten never writes");
        // cooldown → no actuation even when confirmed + live.
        let p_cool = plan_lifecycle_tick(PhaseTag::Settle, &c, &env, LifecycleGate { dry_run: false, in_cooldown: true }, &carry).unwrap();
        assert_eq!(p_cool.actuate, None, "cooldown holds the tighten");
        // confirmed + live + no cooldown → writes the tighter limit.
        let p_live = plan_lifecycle_tick(PhaseTag::Settle, &c, &env, LifecycleGate { dry_run: false, in_cooldown: false }, &carry).unwrap();
        assert!(p_live.actuate.is_some_and(|to| to < 8 << 30), "confirmed live tighten writes downward: {:?}", p_live.actuate);
    }

    #[test]
    fn expand_always_actuates_upward_never_gated() {
        let c = cfg();
        // Missing metric in Steady → ReExpand, must actuate upward regardless of gate.
        let env = MockNervousSystem { metrics: Metrics { used: None, capacity: 1 << 30, observed_for_secs: 10_000, peak_used: 0 }, now_epoch: 10, ..MockNervousSystem::default() };
        let carry = LifecycleCarry { current_limit: 1 << 30, ..LifecycleCarry::default() };
        for gate in [LifecycleGate { dry_run: true, in_cooldown: true }, LifecycleGate { dry_run: false, in_cooldown: false }] {
            let p = plan_lifecycle_tick(PhaseTag::Steady, &c, &env, gate, &carry).unwrap();
            assert!(p.actuate.is_some_and(|to| to >= 1 << 30), "expand actuates upward even under shadow/cooldown: {:?}", p.actuate);
            assert_eq!(p.next_phase, PhaseTag::Wake);
        }
    }

    // ---- lapidar reuse: tighten revert (rule (d) reversibility) ----

    #[test]
    fn tighten_settled_reuses_lapidar_accept_rule() {
        let pre = ControlQuality { mean_waste: 0.6, setpoint_rmse: 0.2, oscillation: 0.0, carve_failure_rate: 0.0, breach_frac: 0.0, samples: 20 };
        // A step that INTRODUCED a breach regresses (safety-first) → revert.
        let worse = ControlQuality { mean_waste: 0.3, setpoint_rmse: 0.2, oscillation: 0.0, carve_failure_rate: 0.0, breach_frac: 0.2, samples: 20 };
        assert_eq!(tighten_settled(&pre, &worse), TightenOutcome::Reverted);
        // A step that cut waste with no breach improves → keep.
        let better = ControlQuality { mean_waste: 0.2, setpoint_rmse: 0.15, oscillation: 0.0, carve_failure_rate: 0.0, breach_frac: 0.0, samples: 20 };
        assert_eq!(tighten_settled(&pre, &better), TightenOutcome::Kept);
        // A tie reverts (never a change without a proven win — lapidar's rule).
        assert_eq!(tighten_settled(&pre, &pre), TightenOutcome::Reverted);
    }

    // ---- expansion-limit derivation (startup band ≫ setpoint) ----

    #[test]
    fn expansion_limit_exceeds_steady_target() {
        let c = cfg();
        let ws = 400u64 << 20;
        let expansion = c.expansion_limit(ws);
        // steady target for the same ws.
        let proof = Confirmed::parse(metrics(ws, expansion), 600).unwrap();
        let steady = c.steady_target(&proof);
        assert!(expansion > steady, "startup band ({expansion}) must exceed the steady target ({steady})");
        // Roughly expansion_factor × the steady headroom.
        assert!(expansion >= (ws as f64 * c.expansion_factor / c.steady.setpoint) as u64 - 1);
    }

    #[test]
    fn expansion_limit_never_zero_under_no_reading() {
        let c = cfg();
        // No startup reading (0) → still a generous, non-zero band (uncertainty → expand).
        let e = c.expansion_limit(0);
        assert!(e >= c.steady.floor_bytes, "no-reading expansion floors at the config floor");
        assert!(e > 0);
    }

    // ---- an end-to-end lifecycle run through the planner ----

    #[test]
    fn end_to_end_zero_to_steady_to_zero() {
        let c = cfg();
        let gate = LifecycleGate { dry_run: false, in_cooldown: false };

        // Zero + load → Wake.
        let z = Lifecycle::<Zero>::start(0);
        let env = MockNervousSystem { metrics: Metrics::default(), load: Some(Load { rps: 20.0, queue_depth: 0 }), now_epoch: 1, ..MockNervousSystem::default() };
        let p = plan_lifecycle_tick(z.tag(), &c, &env, gate, &z.carry()).unwrap();
        let LifecycleDecision::Wake { expansion_limit, .. } = p.decision else { panic!("wake") };
        let w = z.wake(expansion_limit, 1);
        assert_eq!(p.next_phase, PhaseTag::Wake);

        // Wake stabilized (util below setpoint under the generous band) → begin settle.
        let env = MockNervousSystem { metrics: metrics(300 << 20, expansion_limit), now_epoch: 100, ..MockNervousSystem::default() };
        let p = plan_lifecycle_tick(w.tag(), &c, &env, gate, &w.carry()).unwrap();
        assert!(matches!(p.decision, LifecycleDecision::Tighten { .. } | LifecycleDecision::ReachedSetpoint { .. }));
        let s = w.settle(100);

        // Settle: confirm + effect steps until ReachedSetpoint.
        let mut carry = s.carry();
        let mut guard = 0;
        loop {
            guard += 1;
            assert!(guard < 500, "settle must terminate");
            let env = MockNervousSystem { metrics: metrics(300 << 20, carry.current_limit.max(1)), now_epoch: 200, ..MockNervousSystem::default() };
            let p = plan_lifecycle_tick(PhaseTag::Settle, &c, &env, gate, &carry).unwrap();
            match p.decision {
                LifecycleDecision::Tighten { step, shadow_only } => {
                    if shadow_only {
                        // controller confirms the window: mark this from clean.
                        carry.shadow_confirmed_for = Some(step.from());
                    } else {
                        carry.current_limit = step.to();
                        carry.shadow_confirmed_for = None;
                    }
                }
                LifecycleDecision::ReachedSetpoint { limit } => {
                    carry.current_limit = limit;
                    break;
                }
                other => panic!("unexpected in settle: {other:?}"),
            }
        }
        let steady = s.reach_setpoint(300);
        assert_eq!(steady.tag(), PhaseTag::Steady);
        // The settled limit is at/above the steady target and floor — never starved.
        assert!(carry.current_limit >= crate::soft_min(300 << 20, &c.steady));

        // Steady idle → Idle → (timer) → Zero.
        let env = MockNervousSystem { metrics: metrics(5 << 20, carry.current_limit), load: Some(Load::default()), now_epoch: 400, ..MockNervousSystem::default() };
        let p = plan_lifecycle_tick(PhaseTag::Steady, &c, &env, gate, &carry).unwrap();
        assert!(matches!(p.decision, LifecycleDecision::Idle { .. }), "sustained idle arms Idle, got {:?}", p.decision);
        let idle = steady.go_idle(400);
        let env = MockNervousSystem { metrics: metrics(5 << 20, carry.current_limit), load: Some(Load::default()), now_epoch: 400 + (c.idle_to_zero_secs as i64) + 1, ..MockNervousSystem::default() };
        let p = plan_lifecycle_tick(PhaseTag::Idle, &c, &env, gate, &idle.carry()).unwrap();
        assert_eq!(p.decision, LifecycleDecision::ScaleToZero);
        assert!(p.scale_to_zero);
        let _z = idle.rest(500);
    }

    // ---- exhaustive never-stuck: fuse NEVER panics + NEVER downward-carves under
    //      any uncertainty, across a broad signal matrix × phases × gates ----

    fn signal_matrix() -> Vec<Signals> {
        let mut out = Vec::new();
        let metric_shapes = [
            Metrics { used: None, capacity: 1 << 30, observed_for_secs: 10_000, peak_used: 0 },
            Metrics { used: Some(0), capacity: 1 << 30, observed_for_secs: 10_000, peak_used: 0 },
            Metrics { used: Some(300 << 20), capacity: 8 << 30, observed_for_secs: 10_000, peak_used: 300 << 20 },
            Metrics { used: Some(950 << 20), capacity: 1 << 30, observed_for_secs: 10_000, peak_used: 950 << 20 },
            Metrics { used: Some(5 << 20), capacity: 1 << 30, observed_for_secs: 5, peak_used: 5 << 20 },
        ];
        let loads = [None, Some(Load::default()), Some(Load { rps: 50.0, queue_depth: 10 })];
        let healths = [Health::default(), Health { oom: true, ..Health::default() }, Health { restarts: 2, ..Health::default() }];
        let now_pts = [0i64, 100, 10_000];
        for m in metric_shapes {
            for l in loads {
                for h in healths {
                    for now in now_pts {
                        out.push(Signals {
                            metrics: m, readiness: None, load: l, health: h,
                            dependency_ready: None, spot_reclaim_pending: false,
                            schedule_active: None, cost_over_budget: false, now_epoch: now,
                        });
                    }
                }
            }
        }
        out
    }

    #[test]
    fn fuse_is_total_and_safe_over_the_matrix() {
        let c = cfg();
        for phase in PhaseTag::all() {
            for sig in signal_matrix() {
                for dry in [false, true] {
                    for cool in [false, true] {
                        for limit in [0u64, 256 << 20, 1 << 30, 8 << 30] {
                            let carry = LifecycleCarry { current_limit: limit, phase_entered_epoch: 0, shadow_confirmed_for: Some(limit), startup_observed_min: 128 << 20 };
                            let gate = LifecycleGate { dry_run: dry, in_cooldown: cool };
                            // Total: never panics.
                            let d = fuse(phase, &c, &sig, &carry, gate);
                            // If uncertain (metric cannot confirm) OR struggling → never a downward carve.
                            let uncertain = Confirmed::parse(sig.metrics, c.steady.warmup_seconds).is_err();
                            if uncertain || sig.is_struggling() {
                                assert!(!d.is_downward_carve(), "downward carve under uncertainty/struggle: {phase:?} {sig:?} {d:?}");
                            }
                            // A downward carve ALWAYS carries a step floored to the proven need.
                            if let LifecycleDecision::Tighten { step, shadow_only: false } = d {
                                let proof = Confirmed::parse(sig.metrics, c.steady.warmup_seconds).unwrap();
                                let floor = c.tighten_floor(&proof, carry.startup_observed_min);
                                assert!(step.to() >= floor, "tighten below proven-need floor: {} < {}", step.to(), floor);
                            }
                        }
                    }
                }
            }
        }
    }
}
