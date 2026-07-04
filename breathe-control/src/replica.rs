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

/// The k8s `kind` a STATEFUL topology REQUIRES as its target. Ordinal removal
/// (the workload controller drains the HIGHEST ordinal first) + PVC-per-replica
/// semantics — the two properties every stateful topology invariant rests on
/// ("primary = ordinal-0", "never scale the primary away", the Persistent
/// ordinal-drain) — hold ONLY on a StatefulSet. A Deployment scales down an
/// ARBITRARY pod (possibly the primary / an un-drained ordinal), so a stateful
/// band pointed at one is refused at the config/admission gate
/// ([`ReplicaBandConfig::validate_for_target`]).
pub const STATEFULSET_KIND: &str = "StatefulSet";

/// A quorum size that is **provably odd and ≥ 3** — the resting membership of a
/// consensus/Raft set. The field is private and the only constructors round to a
/// legal rung, so an even count and a sub-3 count are *unconstructible*: no
/// `OddQuorum` value can be even or below [`OddQuorum::MIN`]. This is the
/// truly-unrepresentable core of the [`Topology::FullyDistributed`] invariant —
/// there is no code path that yields an even/too-small quorum *target*, not a
/// runtime clamp sitting in front of one (★★ UNREPRESENTABILITY:
/// truly-unrepresentable, on the target-value axis).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct OddQuorum(u32); // private: construction ONLY via the smart constructors

impl OddQuorum {
    /// The smallest legal quorum. A consensus set below 3 cannot tolerate any
    /// fault (2 → majority 2 → no fault tolerance; 1 → no consensus), so 3 is the
    /// floor of the type itself.
    pub const MIN: u32 = 3;

    /// The smallest odd quorum `≥ max(MIN, n)` — rounds an even `n` UP to the next
    /// odd. Used to snap a *desired* count up onto a legal rung (grow / floor).
    #[must_use]
    pub fn at_least(n: u32) -> Self {
        let base = n.max(Self::MIN);
        Self(if base % 2 == 0 { base.saturating_add(1) } else { base })
    }

    /// The largest odd quorum `≤ ceiling` but never below [`Self::MIN`] — rounds an
    /// even `ceiling` DOWN to the next odd. Used to snap a *desired* count down onto
    /// a legal rung (shrink / ceiling). A `ceiling < MIN` yields `MIN` (a quorum
    /// below 3 is not representable; the config gate rejects such a ceiling at parse
    /// time, so this is a defensive floor, never a silently-wrong small quorum).
    #[must_use]
    pub fn at_most(ceiling: u32) -> Self {
        if ceiling <= Self::MIN {
            return Self(Self::MIN);
        }
        Self(if ceiling % 2 == 0 { ceiling - 1 } else { ceiling })
    }

    /// The next odd rung strictly ABOVE `current` (one membership step up), never
    /// below [`Self::MIN`]. `5 → 7`, `3 → 5`, an even `4 → 5`, `0 → 3`.
    #[must_use]
    pub fn step_up(current: u32) -> Self {
        Self::at_least(current.saturating_add(1))
    }

    /// The next odd rung strictly BELOW an odd-normalized `current` (one membership
    /// step down), never below [`Self::MIN`]. `7 → 5`, `5 → 3`, `3 → 3` (the floor
    /// binds), an even `4 → 3`.
    #[must_use]
    pub fn step_down(current: u32) -> Self {
        let odd = Self::at_least(current).0; // normalize a drifted even current up
        Self(odd.saturating_sub(2).max(Self::MIN))
    }

    /// The raw odd count. Every value returned here is `odd && ≥ MIN` by
    /// construction — the whole point of the type.
    #[must_use]
    pub fn get(self) -> u32 {
        self.0
    }
}

/// The workload TOPOLOGY — a **first-class axis** of the horizontal band
/// (theory/BREATHABILITY.md §II.5). It selects BOTH the scaling algorithm and the
/// hard invariant the band may never violate. One `ReplicaBand`, a topology tag,
/// four algorithms — so the horizontal dimension is correct across every layer
/// (persistent, non-persistent, master/slave, fully distributed), not just the
/// easy stateless case.
///
/// Tier-honesty (★★ UNREPRESENTABILITY — never round a runtime clamp up to
/// "unrepresentable"):
///   * **NonPersistent** — no extra invariant; the HA floor is the only bound.
///   * **Persistent** — a scale-in is emitted as the non-actuating
///     [`ReplicaDecision::HeldForRebalance`] (the reactive shrink has NO write
///     path — `target() == current`); the replication-factor floor is
///     *only-mitigated* (a clamp) at the decision and *parse-time-rejected* at the
///     config gate.
///   * **MasterSlave** — the primary count folds into the floor so a rest below it
///     is *only-mitigated* (clamp) + *parse-time-rejected* (config); the read
///     replicas breathe above it.
///   * **FullyDistributed** — the *quantized quorum target* is
///     *truly-unrepresentable* even/sub-3 (via [`OddQuorum`]); the majority-safe
///     *step* (one odd rung per tick) is *only-mitigated* (a velocity clamp — Rust
///     cannot prove the graph-reachability quantifier); the ceiling-≥-3 config is
///     *parse-time-rejected*.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Topology {
    /// Stateless: every pod is interchangeable. Free HPA-style scaling on the
    /// work-rate signal; a reclaim scales out on the survivors; the HA floor is the
    /// only invariant. The back-compat default (existing bands behave unchanged).
    #[default]
    NonPersistent,
    /// Stateful, PVC-per-replica (StatefulSet ordinals). Scale-UP adds an
    /// ordinal+PVC freely; a scale-DOWN must drain/rebalance the ordinal's data
    /// FIRST, so the band DEFERS the shrink ([`ReplicaDecision::HeldForRebalance`])
    /// rather than orphan un-rebalanced data, and never rests below the
    /// `replication_factor`.
    Persistent {
        /// The data-replication factor — the band never rests below this many
        /// replicas (a shrink under it would drop a data copy).
        replication_factor: u32,
    },
    /// Primary + read-replicas. Only the read-replica count breathes; the band
    /// never scales the primary away (a primary loss is a *failover* / retirada, not
    /// a replica scale). The total floor covers `primaries` (1 for a single primary,
    /// 2 for an HA pair).
    MasterSlave {
        /// The writable-primary count folded into the floor (never scaled away).
        primaries: u32,
    },
    /// Quorum/consensus (Raft/etcd). The resting count is always ODD and ≥ 3, a
    /// live majority is preserved, and membership changes one rung at a time — a
    /// scale-down never crosses the majority line.
    FullyDistributed,
}

impl Topology {
    /// Stable label (status / catalog rendering / logging).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NonPersistent => "non-persistent",
            Self::Persistent { .. } => "persistent",
            Self::MasterSlave { .. } => "master-slave",
            Self::FullyDistributed => "fully-distributed",
        }
    }

    /// The topology's own hard minimum-replica invariant, folded on top of the HA
    /// floor by [`ReplicaBandConfig::topology_floor`]. Not the whole floor — see
    /// [`ReplicaBandConfig::topology_floor`] for the odd-snap on `FullyDistributed`.
    #[must_use]
    pub fn hard_floor(self) -> u32 {
        match self {
            Self::NonPersistent => 0,
            Self::Persistent { replication_factor } => replication_factor,
            Self::MasterSlave { primaries } => primaries,
            Self::FullyDistributed => OddQuorum::MIN,
        }
    }

    /// Snap a target `to` onto the topology's legal envelope — identity for every
    /// topology except `FullyDistributed`, which snaps to an [`OddQuorum`] rung
    /// within `[floor, ceiling]`. Used by the break-glass force path so even a
    /// forced count stays inside the topology invariant (a forced even quorum is
    /// snapped to odd; the primary/replication floors already bound the clamp).
    #[must_use]
    pub fn quantize_target(self, to: u32, floor: u32, ceiling: u32) -> u32 {
        match self {
            Self::FullyDistributed => {
                let up = OddQuorum::at_least(to).get();
                let cap = OddQuorum::at_most(ceiling).get();
                up.min(cap).max(floor)
            }
            _ => to,
        }
    }

    /// `true` when this topology REQUIRES a StatefulSet target (see
    /// [`STATEFULSET_KIND`]). The three STATEFUL arms — `Persistent` (PVC-per-
    /// ordinal drain), `MasterSlave` (primary = ordinal-0, never scaled away),
    /// `FullyDistributed` (ordinal-stable quorum membership) — all lean on
    /// StatefulSet ordinal semantics; `NonPersistent` (interchangeable pods) does
    /// not, so it scales on a Deployment OR a StatefulSet. The [topology ↔
    /// target-kind] coupling gate ([`ReplicaBandConfig::validate_for_target`]) reads
    /// this: a `true` here plus a non-StatefulSet target is a parse-time refusal.
    #[must_use]
    pub fn requires_statefulset(self) -> bool {
        !matches!(self, Self::NonPersistent)
    }

    /// Every topology kind's stable [`Self::as_str`] label — the axis the catalog +
    /// CRD reflection cross-checks against (CATALOG REFLECTION: a new arm cannot be
    /// added to this enum without the catalog row + CRD kind agreeing). Kept in sync
    /// with [`Self::as_str`] by `all_labels_match_as_str` in this crate's tests.
    pub const ALL_LABELS: [&'static str; 4] =
        ["non-persistent", "persistent", "master-slave", "fully-distributed"];
}

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
    /// The workload TOPOLOGY — selects the per-topology scaling algorithm AND the
    /// hard invariant the band may never violate. Default [`Topology::NonPersistent`]
    /// (stateless), so an existing band's behaviour is byte-unchanged.
    pub topology: Topology,
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
            topology: Topology::NonPersistent,
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
    /// The [`Topology`] invariant cannot be satisfied by this config — the
    /// topology's hard floor (replication factor / primary count / quorum-3)
    /// exceeds the ceiling, or a required parameter is zero. Rejected at the config
    /// gate before any decision runs (★★ UNREPRESENTABILITY: parse-time-rejected).
    TopologyUnsatisfiable(&'static str),
    /// A STATEFUL [`Topology`] (`Persistent` / `MasterSlave` / `FullyDistributed`)
    /// is bound to a target whose `kind` is not [`STATEFULSET_KIND`]. The stateful
    /// invariants ("primary = ordinal-0, never scaled away", the ordinal-drain, the
    /// ordinal-stable quorum) hold ONLY on a StatefulSet; a Deployment scale-down
    /// removes an ARBITRARY pod (possibly the primary / an un-drained ordinal), so
    /// the mismatch is refused at the config/admission gate — the topology ↔
    /// target-kind coupling (★★ UNREPRESENTABILITY: parse-time-rejected). The field
    /// is the offending topology's [`Topology::as_str`] label.
    TopologyTargetMismatch(&'static str),
}

impl std::fmt::Display for ReplicaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyRange => f.write_str("floor must be ≤ ceiling"),
            Self::ZeroCeiling => f.write_str("ceiling must be ≥ 1"),
            Self::NoDenominator => f.write_str("target must be > 0"),
            Self::BadSignal => f.write_str("signal value must be finite and ≥ 0"),
            Self::Unreadable(what) => write!(f, "environment could not read {what}"),
            Self::TopologyUnsatisfiable(what) => write!(f, "topology invariant unsatisfiable: {what}"),
            Self::TopologyTargetMismatch(topo) => write!(
                f,
                "topology '{topo}' requires targetRef.kind = {STATEFULSET_KIND} \
                 (ordinal-drain + PVC-per-replica semantics); a non-StatefulSet target is refused"
            ),
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

    /// The TOPOLOGY-adjusted floor — [`Self::effective_floor`] raised to also cover
    /// the topology's hard invariant: the data-replication factor (Persistent), the
    /// primary count (MasterSlave), or an odd-snapped quorum floor ≥ 3
    /// (FullyDistributed). This is the floor every carve clamps against, so a rest
    /// below the topology invariant is bound here (only-mitigated: a clamp) on top of
    /// the config gate that parse-rejects a ceiling too small to hold it.
    #[must_use]
    pub fn topology_floor(&self) -> u32 {
        let base = self.effective_floor().max(self.topology.hard_floor());
        match self.topology {
            // the quorum floor must itself be a legal odd rung.
            Topology::FullyDistributed => OddQuorum::at_least(base).get(),
            _ => base,
        }
    }

    /// Parse-time validation — a malformed band is a typed error, never a silent
    /// wrong scale (★★ UNREPRESENTABILITY: parse-time-rejected).
    ///
    /// # Errors
    /// [`ReplicaError::ZeroCeiling`] / [`ReplicaError::EmptyRange`] /
    /// [`ReplicaError::NoDenominator`] when the respective invariant is violated;
    /// [`ReplicaError::TopologyUnsatisfiable`] when the topology's hard floor cannot
    /// fit under the ceiling (or a topology parameter is zero).
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
        // topology config invariants — a config that cannot hold the invariant is
        // refused BEFORE any decision runs (parse-time-rejected), never silently
        // clamped into a wrong shape at carve time.
        match self.topology {
            Topology::NonPersistent => {}
            Topology::Persistent { replication_factor } => {
                if replication_factor == 0 {
                    return Err(ReplicaError::TopologyUnsatisfiable("persistent replication_factor must be ≥ 1"));
                }
                if replication_factor > self.ceiling {
                    return Err(ReplicaError::TopologyUnsatisfiable("ceiling is below the data-replication factor"));
                }
            }
            Topology::MasterSlave { primaries } => {
                if primaries == 0 {
                    return Err(ReplicaError::TopologyUnsatisfiable("master-slave needs ≥ 1 primary"));
                }
                if primaries > self.ceiling {
                    return Err(ReplicaError::TopologyUnsatisfiable("ceiling is below the primary count"));
                }
            }
            Topology::FullyDistributed => {
                if self.ceiling < OddQuorum::MIN {
                    return Err(ReplicaError::TopologyUnsatisfiable("quorum ceiling must be ≥ 3"));
                }
            }
        }
        Ok(())
    }

    /// Parse-time validation INCLUDING the **topology ↔ target-kind coupling**: the
    /// numeric [`Self::validate`] gate (reused, never forked), then a refusal of a
    /// STATEFUL topology (`Persistent` / `MasterSlave` / `FullyDistributed`) bound to
    /// a target whose `kind` is not [`STATEFULSET_KIND`]. `NonPersistent`
    /// (interchangeable pods) is allowed on ANY workload kind. Matching is
    /// ASCII-case-insensitive so `statefulset`/`StatefulSet` both pass.
    ///
    /// This is where the "never scale the primary away" + Persistent ordinal-drain
    /// conventions become ENFORCED rather than assumed: a Deployment removes an
    /// arbitrary pod on scale-down, so only a StatefulSet (highest-ordinal-first
    /// removal + PVC-per-replica) makes the stateful invariants hold. The band's
    /// `targetRef.kind` lives on the CRD, so this is the gate the CRD/admission path
    /// calls (the control-layer numeric [`Self::validate`] alone cannot see the kind).
    ///
    /// # Errors
    /// Every [`Self::validate`] error, plus [`ReplicaError::TopologyTargetMismatch`]
    /// when a stateful topology targets a non-StatefulSet kind (★★ UNREPRESENTABILITY:
    /// parse-time-rejected — a mismatch never reaches a scaling decision).
    pub fn validate_for_target(&self, target_kind: &str) -> Result<(), ReplicaError> {
        self.validate()?;
        if self.topology.requires_statefulset() && !target_kind.eq_ignore_ascii_case(STATEFULSET_KIND) {
            return Err(ReplicaError::TopologyTargetMismatch(self.topology.as_str()));
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
    /// **Persistent (stateful) only.** A scale-IN is DUE but is HELD: the ordinal's
    /// data must be drained/rebalanced off the PVC-bearing replica BEFORE it is
    /// removed, so the band does NOT shrink `.spec.replicas` directly (that would
    /// orphan un-rebalanced data). Non-actuating BY CONSTRUCTION — [`Self::target`]
    /// returns `current`, so [`plan_replica_tick`] produces no write for it: the
    /// reactive persistent shrink simply has no actuation path. `would_shrink_to`
    /// is the count the band would rest at once a rebalance controller has drained
    /// the ordinal (surfaced for observability; an operator drains + break-glass
    /// forces the count to actually remove it).
    HeldForRebalance { current: u32, would_shrink_to: u32 },
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
            // HeldForRebalance is non-actuating: its target IS the current count, so a
            // caller that uniformly `assign(target())` writes nothing (the reactive
            // persistent shrink has no write path).
            Self::HeldForRebalance { current, .. } => current,
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
            Self::HeldForRebalance { current, .. } => current,
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
            Self::HeldForRebalance { .. } => "HeldForRebalance",
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
            Self::HeldForRebalance { current, would_shrink_to } => {
                write!(f, "HeldForRebalance@{current} (would shrink to {would_shrink_to} after drain)")
            }
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
    // Dispatch on the topology axis — each arm picks its algorithm AND enforces its
    // hard invariant. The shared HPA-ratio + anti-flap arithmetic lives once in
    // [`decide_core`] (parameterized on the effective floor); only `FullyDistributed`
    // needs its own odd-rung law. NonPersistent is the current behaviour verbatim.
    match cfg.topology {
        Topology::NonPersistent => decide_core(cfg, obs, cfg.effective_floor()),
        // MasterSlave: the read-replicas breathe on the SAME core law; the primary
        // count is folded into the floor (topology_floor) so a scale-in can never
        // rest below it — "never scale the primary away" is the floor invariant.
        Topology::MasterSlave { .. } => decide_core(cfg, obs, cfg.topology_floor()),
        // Persistent: the core law runs against the replication-factor floor, then a
        // would-be scale-in is re-typed to the non-actuating HeldForRebalance — the
        // reactive shrink is DEFERRED to a drain/rebalance, never written directly.
        Topology::Persistent { .. } => match decide_core(cfg, obs, cfg.topology_floor()) {
            ReplicaDecision::ScaleDown { from, to } => {
                ReplicaDecision::HeldForRebalance { current: from, would_shrink_to: to }
            }
            other => other,
        },
        // FullyDistributed: its own quorum-safe odd-rung law.
        Topology::FullyDistributed => decide_quorum(cfg, obs),
    }
}

/// The shared HPA-ratio + asymmetric-anti-flap core, parameterized on the effective
/// `floor` (the topology dispatcher supplies the right one). This is the exact
/// stateless algorithm; `NonPersistent` calls it with [`ReplicaBandConfig::effective_floor`]
/// (byte-identical to the pre-topology behaviour), Persistent/MasterSlave call it with
/// the topology-raised floor.
///
/// Order (each step is load-bearing): spot-reclaim → HPA ratio → asymmetric tolerance
/// dead-band → scale-down stabilization → floor/ceiling clamp → velocity cap.
#[must_use]
fn decide_core(cfg: &ReplicaBandConfig, obs: &ReplicaObservation, floor: u32) -> ReplicaDecision {
    let current = obs.current_replicas;
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

/// The FULLY-DISTRIBUTED (quorum/consensus) law: the resting count is ALWAYS an
/// [`OddQuorum`] (odd, ≥ 3), a live majority is preserved, and membership changes one
/// odd rung at a time — a scale-down never crosses the majority line.
///
/// Every carve `to` is produced by an [`OddQuorum`] constructor, so an even or sub-3
/// quorum *target* is truly-unrepresentable (there is no code path that builds one —
/// not a clamp in front of one). The one-rung-per-tick step is the majority-safe
/// mechanism: from an odd `k ≥ 5`, dropping to `k-2` leaves `k-2 ≥ majority(k) =
/// (k+1)/2`, so a single resting transition always retains quorum; at `k = 3` the
/// floor binds and it never shrinks below 3. (That the step is a velocity clamp, not
/// a compile error, is the only-mitigated half — Rust cannot prove the reachability
/// quantifier; the config gate parse-rejects a ceiling < 3.)
#[must_use]
fn decide_quorum(cfg: &ReplicaBandConfig, obs: &ReplicaObservation) -> ReplicaDecision {
    let current = obs.current_replicas;
    let floor = cfg.topology_floor(); // odd, ≥ 3 (topology_floor snaps it)
    // the ceiling snapped DOWN to a legal odd rung, never below the (odd) floor.
    let ceiling = OddQuorum::at_most(cfg.ceiling.max(floor)).get().max(floor);
    let raw = cfg.signal.desired_raw(current, obs.signal_value, cfg.target);

    // ── Spot reclaim → quorum scale-OUT (adding voters never loses majority). ───
    // Cover the doomed voters, snapped to the next legal odd rung within the wall.
    if obs.reclaim_pending > 0 {
        let want = current.saturating_add(obs.reclaim_pending).max(raw);
        let to = OddQuorum::at_least(want).get().min(ceiling).max(floor);
        return ReplicaDecision::SpotScaleOut { from: current, to, reclaim: obs.reclaim_pending };
    }

    // ── Asymmetric tolerance dead-band (same metric-ratio gate as the core). ────
    let ratio = cfg.signal.metric_ratio(current, obs.signal_value, cfg.target);
    let want_up = ratio > 1.0 + cfg.tolerance_up;
    let want_down = ratio < 1.0 - cfg.tolerance_down;
    if !want_up && !want_down {
        return ReplicaDecision::Hold { current };
    }

    if want_up {
        // GROW one odd rung toward the odd-snapped desired, within the odd ceiling.
        let desired = OddQuorum::at_least(clamp(raw, floor, ceiling)).get().min(ceiling);
        let to = desired.min(OddQuorum::step_up(current).get()).max(floor).min(ceiling);
        return if to > current {
            ReplicaDecision::ScaleUp { from: current, to }
        } else {
            ReplicaDecision::AtCeiling { current }
        };
    }

    // SHRINK one odd rung, majority-safe. Snap the (window-stabilized) desired DOWN
    // to an odd rung, and never remove more than one rung this tick.
    let stabilized = obs.window_max_desired.map_or(raw, |w| w.max(raw));
    let desired = OddQuorum::at_most(clamp(stabilized, floor, ceiling)).get().max(floor);
    let to = desired.max(OddQuorum::step_down(current).get()).clamp(floor, ceiling);
    if to < current {
        ReplicaDecision::ScaleDown { from: current, to }
    } else {
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
            // break-glass: pin to the forced count, still inside the TOPOLOGY safety
            // envelope — the topology floor (primary/replication/quorum) + ceiling
            // clamp AND the topology quantize (a forced even quorum snaps to odd).
            // Break-glass bypasses the band LAW, never the safety envelope.
            let current = env.current_replicas()?;
            let floor = cfg.topology_floor();
            let ceiling = cfg.ceiling.max(floor);
            let clamped = v.clamp(floor, ceiling);
            let to = cfg.topology.quantize_target(clamped, floor, ceiling);
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

    // ══════════════════════ TOPOLOGY axis (§II.5) ══════════════════════════════
    //
    // Each topology test proves TWO things: the algorithm scales correctly AND the
    // hard invariant holds. Tier per invariant is stated in the doc-comments on
    // `Topology` / `OddQuorum` — these tests are the mechanical evidence.

    // ── OddQuorum: the truly-unrepresentable core (even/sub-3 is unconstructible) ──

    #[test]
    fn odd_quorum_is_always_odd_and_at_least_three() {
        for n in 0u32..64 {
            // every constructor path yields an odd value ≥ 3 — there is no input that
            // builds an even or sub-3 quorum (the type has no other constructor).
            assert!(OddQuorum::at_least(n).get() % 2 == 1 && OddQuorum::at_least(n).get() >= 3);
            assert!(OddQuorum::at_most(n).get() % 2 == 1 && OddQuorum::at_most(n).get() >= 3);
            assert!(OddQuorum::step_up(n).get() % 2 == 1 && OddQuorum::step_up(n).get() >= 3);
            assert!(OddQuorum::step_down(n).get() % 2 == 1 && OddQuorum::step_down(n).get() >= 3);
        }
        assert_eq!(OddQuorum::at_least(4).get(), 5); // even rounds UP
        assert_eq!(OddQuorum::at_most(4).get(), 3); // even rounds DOWN
        assert_eq!(OddQuorum::at_least(0).get(), 3); // never below MIN
        assert_eq!(OddQuorum::step_up(5).get(), 7); // one rung up
        assert_eq!(OddQuorum::step_down(5).get(), 3); // one rung down
        assert_eq!(OddQuorum::step_down(3).get(), 3); // the floor binds at MIN
    }

    // ── NonPersistent (stateless): free scaling, identity to the pre-topology law ──

    #[test]
    fn stateless_topology_scales_free_and_matches_the_core() {
        let c = ReplicaBandConfig { topology: Topology::NonPersistent, ceiling: 50, ..Default::default() };
        // grows freely on load (the exact same decision the default already made).
        assert_eq!(
            decide_replicas(&c, &ReplicaObservation::reactive(4, 0.9)),
            ReplicaDecision::ScaleUp { from: 4, to: 5 }
        );
        // shrinks freely when idle (no rebalance hold, no odd-snap).
        assert_eq!(
            decide_replicas(&c, &ReplicaObservation::reactive(10, 0.4)),
            ReplicaDecision::ScaleDown { from: 10, to: 9 }
        );
    }

    // ── Persistent (stateful): grow free, scale-in HELD, replication-factor floor ──

    #[test]
    fn persistent_grows_freely_but_defers_scale_in_to_rebalance() {
        let c = ReplicaBandConfig {
            topology: Topology::Persistent { replication_factor: 3 },
            ceiling: 10,
            ..Default::default()
        };
        // GROW is free — a scale-out adds an ordinal+PVC with no hold.
        let grow = decide_replicas(&c, &ReplicaObservation::reactive(3, 1.6));
        assert!(matches!(grow, ReplicaDecision::ScaleUp { from: 3, .. }), "persistent grows free: {grow:?}");

        // a would-be SCALE-IN is re-typed to the non-actuating HeldForRebalance — the
        // reactive shrink has NO write path (drain/rebalance the ordinal first).
        let shrink = decide_replicas(&c, &ReplicaObservation::reactive(6, 0.1));
        assert!(matches!(shrink, ReplicaDecision::HeldForRebalance { current: 6, .. }), "got {shrink:?}");
        assert_eq!(shrink.target(), 6, "HeldForRebalance never actuates a shrink");
        assert!(!shrink.is_carve());
        // …and through the planner it writes nothing even when live + policy-permitted.
        let env = MockReplicaEnvironment { current_replicas: 6, signal_value: 0.1, ..Default::default() };
        let plan = plan_replica_tick(&c, &env, gate(false, false, true)).expect("plans");
        assert_eq!(plan.actuate, None, "a persistent reactive shrink is never written");
    }

    #[test]
    fn persistent_never_rests_below_the_replication_factor() {
        let c = ReplicaBandConfig {
            topology: Topology::Persistent { replication_factor: 3 },
            floor: 2, // base floor below rf — the rf floor must win
            ceiling: 10,
            ..Default::default()
        };
        assert_eq!(c.topology_floor(), 3, "the replication factor raises the floor");
        // at the rf floor, an idle band reports AtFloor — never a shrink below 3.
        assert_eq!(decide_replicas(&c, &ReplicaObservation::reactive(3, 0.05)), ReplicaDecision::AtFloor { current: 3 });
        // no observation ever produces a rest below the replication factor.
        for cur in 0u32..12 {
            for &v in &[0.0, 0.05, 0.5, 0.9, 3.0] {
                let d = decide_replicas(&c, &ReplicaObservation::reactive(cur, v));
                assert!(d.target() >= 3 || d.current() < 3, "rest below rf: cur={cur} v={v} -> {d:?}");
            }
        }
    }

    #[test]
    fn persistent_config_that_cannot_hold_the_factor_is_parse_rejected() {
        let bad_rf = ReplicaBandConfig {
            topology: Topology::Persistent { replication_factor: 20 },
            ceiling: 10,
            ..Default::default()
        };
        assert_eq!(
            bad_rf.validate(),
            Err(ReplicaError::TopologyUnsatisfiable("ceiling is below the data-replication factor"))
        );
        let zero_rf = ReplicaBandConfig {
            topology: Topology::Persistent { replication_factor: 0 },
            ceiling: 10,
            ..Default::default()
        };
        assert_eq!(
            zero_rf.validate(),
            Err(ReplicaError::TopologyUnsatisfiable("persistent replication_factor must be ≥ 1"))
        );
    }

    // ── MasterSlave: read-replicas breathe, the primary is never scaled away ──────

    #[test]
    fn master_slave_scales_read_replicas_but_never_the_primary() {
        // 2 primaries (an HA pair); base floor below that — the primary floor wins.
        let c = ReplicaBandConfig {
            topology: Topology::MasterSlave { primaries: 2 },
            floor: 1,
            ceiling: 12,
            ..Default::default()
        };
        assert_eq!(c.topology_floor(), 2, "the primary count is the hard floor");
        // read-replica scaling: a loaded 5-replica set (2 primary + 3 read) grows.
        assert!(matches!(
            decide_replicas(&c, &ReplicaObservation::reactive(5, 0.95)),
            ReplicaDecision::ScaleUp { from: 5, .. }
        ));
        // idle read-replicas shrink toward the primary floor, never below it.
        assert!(matches!(
            decide_replicas(&c, &ReplicaObservation::reactive(5, 0.1)),
            ReplicaDecision::ScaleDown { from: 5, .. }
        ));
        // AT the primary floor, an idle band reports AtFloor — the primary stays put.
        assert_eq!(decide_replicas(&c, &ReplicaObservation::reactive(2, 0.01)), ReplicaDecision::AtFloor { current: 2 });
        // INVARIANT sweep: no decision ever targets a count below the primaries.
        for cur in 0u32..14 {
            for &v in &[0.0, 0.05, 0.5, 0.9, 4.0] {
                let d = decide_replicas(&c, &ReplicaObservation::reactive(cur, v));
                assert!(
                    d.target() >= 2 || d.current() < 2,
                    "master-slave scaled the primary away: cur={cur} v={v} -> {d:?}"
                );
            }
        }
    }

    #[test]
    fn master_slave_config_without_room_for_the_primary_is_parse_rejected() {
        let too_many = ReplicaBandConfig {
            topology: Topology::MasterSlave { primaries: 20 },
            ceiling: 10,
            ..Default::default()
        };
        assert_eq!(too_many.validate(), Err(ReplicaError::TopologyUnsatisfiable("ceiling is below the primary count")));
        let zero = ReplicaBandConfig { topology: Topology::MasterSlave { primaries: 0 }, ceiling: 10, ..Default::default() };
        assert_eq!(zero.validate(), Err(ReplicaError::TopologyUnsatisfiable("master-slave needs ≥ 1 primary")));
    }

    // ── FullyDistributed: odd-only quorum, majority-safe one-rung steps ───────────

    fn quorum() -> ReplicaBandConfig {
        ReplicaBandConfig { topology: Topology::FullyDistributed, floor: 2, ceiling: 9, ..Default::default() }
    }

    #[test]
    fn quorum_grows_and_shrinks_one_odd_rung_at_a_time() {
        let c = quorum();
        // loaded 3-node quorum grows ONE rung: 3 → 5 (not straight to the desired 7+).
        assert_eq!(decide_replicas(&c, &ReplicaObservation::reactive(3, 1.6)), ReplicaDecision::ScaleUp { from: 3, to: 5 });
        // idle 5-node quorum shrinks ONE rung: 5 → 3.
        assert_eq!(decide_replicas(&c, &ReplicaObservation::reactive(5, 0.05)), ReplicaDecision::ScaleDown { from: 5, to: 3 });
    }

    #[test]
    fn quorum_shrink_never_crosses_the_majority_line() {
        let c = ReplicaBandConfig { topology: Topology::FullyDistributed, floor: 2, ceiling: 15, ..Default::default() };
        // a deeply-idle 9-node quorum drops only ONE rung to 7 (removing 2), never
        // 9 → 3 (which would remove 6 at once and risk crossing majority).
        assert_eq!(decide_replicas(&c, &ReplicaObservation::reactive(9, 0.01)), ReplicaDecision::ScaleDown { from: 9, to: 7 });
        // 7 → 5, 5 → 3, then the floor binds: a quorum NEVER shrinks below 3.
        assert_eq!(decide_replicas(&c, &ReplicaObservation::reactive(7, 0.01)), ReplicaDecision::ScaleDown { from: 7, to: 5 });
        assert_eq!(decide_replicas(&c, &ReplicaObservation::reactive(3, 0.01)), ReplicaDecision::AtFloor { current: 3 });
        // and the resting invariant holds each step: k → k-2 keeps a live majority.
        for k in [5u32, 7, 9, 11, 13] {
            if let ReplicaDecision::ScaleDown { from, to } = decide_replicas(&c, &ReplicaObservation::reactive(k, 0.01)) {
                assert!(to >= (from + 1) / 2, "shrink {from}->{to} crossed majority({from})={}", (from + 1) / 2);
            } else {
                panic!("expected a one-rung shrink at k={k}");
            }
        }
    }

    #[test]
    fn quorum_target_is_never_even_or_below_three_over_a_full_sweep() {
        // the truly-unrepresentable invariant, exercised: across every current count,
        // signal, ceiling parity, and a pending reclaim, a quorum CARVE target is
        // always odd and ≥ 3 (there is no code path that yields an even/sub-3 target).
        for &ceiling in &[3u32, 4, 5, 8, 9, 10] {
            let c = ReplicaBandConfig { topology: Topology::FullyDistributed, floor: 2, ceiling, ..Default::default() };
            for cur in 0u32..14 {
                for &v in &[0.0, 0.01, 0.4, 0.8, 0.95, 2.0, 100.0] {
                    for &reclaim in &[0u32, 1, 4] {
                        let obs = ReplicaObservation {
                            current_replicas: cur,
                            signal_value: v,
                            window_max_desired: None,
                            reclaim_pending: reclaim,
                        };
                        let d = decide_replicas(&c, &obs);
                        if d.is_carve() {
                            let to = d.target();
                            assert!(to % 2 == 1 && to >= 3, "even/sub-3 quorum target {to} from {d:?} (cur={cur} v={v} rc={reclaim})");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn quorum_reclaim_scales_out_to_an_odd_rung() {
        let c = quorum();
        // a 3-node quorum losing 1 voter pre-scales OUT to the next odd rung (5), odd.
        let obs = ReplicaObservation { current_replicas: 3, signal_value: 0.8, window_max_desired: None, reclaim_pending: 1 };
        let d = decide_replicas(&c, &obs);
        assert!(matches!(d, ReplicaDecision::SpotScaleOut { from: 3, to: 5, reclaim: 1 }), "got {d:?}");
    }

    #[test]
    fn quorum_config_with_a_sub_three_ceiling_is_parse_rejected() {
        let bad = ReplicaBandConfig { topology: Topology::FullyDistributed, floor: 1, ceiling: 2, ..Default::default() };
        assert_eq!(bad.validate(), Err(ReplicaError::TopologyUnsatisfiable("quorum ceiling must be ≥ 3")));
    }

    // ── break-glass stays inside the topology envelope ────────────────────────────

    #[test]
    fn break_glass_force_stays_inside_the_topology_envelope() {
        // FullyDistributed: a forced EVEN count is snapped to the nearest legal odd.
        let q = quorum();
        let env = MockReplicaEnvironment { current_replicas: 3, signal_value: 0.5, ..Default::default() };
        let forced = plan_replica_tick(&q, &env, ReplicaGate { force: Some(4), ..gate(false, false, true) }).expect("plans");
        assert_eq!(forced.actuate, Some(5), "a forced even quorum snaps to odd (4 -> 5)");
        // Persistent: a forced count below the replication factor is floored at it.
        let p = ReplicaBandConfig { topology: Topology::Persistent { replication_factor: 3 }, ceiling: 10, ..Default::default() };
        let penv = MockReplicaEnvironment { current_replicas: 5, signal_value: 0.5, ..Default::default() };
        let pf = plan_replica_tick(&p, &penv, ReplicaGate { force: Some(1), ..gate(false, false, true) }).expect("plans");
        assert_eq!(pf.actuate, Some(3), "a forced sub-factor count is floored at the replication factor");
    }

    // ── topology ↔ target-kind coupling (the stateful-needs-StatefulSet gate) ─────

    #[test]
    fn stateful_topologies_require_a_statefulset_target() {
        // Persistent / MasterSlave / FullyDistributed on a Deployment are REFUSED —
        // ordinal-drain + PVC-per-replica semantics don't hold on a Deployment (a
        // scale-down there removes an arbitrary pod, possibly the primary).
        let persistent = ReplicaBandConfig {
            topology: Topology::Persistent { replication_factor: 3 },
            ceiling: 10,
            ..Default::default()
        };
        assert_eq!(
            persistent.validate_for_target("Deployment"),
            Err(ReplicaError::TopologyTargetMismatch("persistent"))
        );
        let master = ReplicaBandConfig {
            topology: Topology::MasterSlave { primaries: 1 },
            ceiling: 8,
            ..Default::default()
        };
        assert_eq!(
            master.validate_for_target("Deployment"),
            Err(ReplicaError::TopologyTargetMismatch("master-slave"))
        );
        let quorum = ReplicaBandConfig { topology: Topology::FullyDistributed, ceiling: 9, ..Default::default() };
        assert_eq!(
            quorum.validate_for_target("Deployment"),
            Err(ReplicaError::TopologyTargetMismatch("fully-distributed"))
        );

        // …but each validates on a StatefulSet target (case-insensitive kind match).
        assert_eq!(persistent.validate_for_target("StatefulSet"), Ok(()));
        assert_eq!(master.validate_for_target("StatefulSet"), Ok(()));
        assert_eq!(quorum.validate_for_target("StatefulSet"), Ok(()));
        assert_eq!(quorum.validate_for_target("statefulset"), Ok(()), "kind match is ASCII-case-insensitive");
    }

    #[test]
    fn non_persistent_topology_validates_on_any_workload_kind() {
        // Stateless pods are interchangeable — a NonPersistent band is legal on a
        // Deployment OR a StatefulSet OR an owner-less pod group.
        let c = ReplicaBandConfig { topology: Topology::NonPersistent, ceiling: 50, ..Default::default() };
        assert_eq!(c.validate_for_target("Deployment"), Ok(()));
        assert_eq!(c.validate_for_target("StatefulSet"), Ok(()));
        assert_eq!(c.validate_for_target("Pod"), Ok(()));
    }

    #[test]
    fn validate_for_target_still_enforces_the_numeric_gate_first() {
        // The coupling gate REUSES validate(): a numerically-broken band fails on the
        // numeric error even on a StatefulSet, before the kind is ever considered.
        let empty = ReplicaBandConfig {
            topology: Topology::Persistent { replication_factor: 2 },
            floor: 9,
            ceiling: 3,
            ..Default::default()
        };
        assert_eq!(empty.validate_for_target("StatefulSet"), Err(ReplicaError::EmptyRange));
    }

    #[test]
    fn only_stateful_topologies_require_statefulset() {
        assert!(!Topology::NonPersistent.requires_statefulset());
        assert!(Topology::Persistent { replication_factor: 1 }.requires_statefulset());
        assert!(Topology::MasterSlave { primaries: 1 }.requires_statefulset());
        assert!(Topology::FullyDistributed.requires_statefulset());
    }

    #[test]
    fn all_labels_match_as_str() {
        // ALL_LABELS is the reflection axis the catalog + CRD cross-check; it must
        // stay a bijection with the four Topology arms' as_str().
        let arms = [
            Topology::NonPersistent,
            Topology::Persistent { replication_factor: 1 },
            Topology::MasterSlave { primaries: 1 },
            Topology::FullyDistributed,
        ];
        assert_eq!(arms.len(), Topology::ALL_LABELS.len());
        for a in arms {
            assert!(Topology::ALL_LABELS.contains(&a.as_str()), "as_str {} missing from ALL_LABELS", a.as_str());
        }
        // and no duplicates / stray labels.
        let mut seen = Topology::ALL_LABELS.to_vec();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), 4, "ALL_LABELS has a duplicate or stray label");
    }
}
