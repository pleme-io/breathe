//! The ISOLATION / INTERFERENCE dimension — the SEAL that bounds the carve.
//!
//! ## What this module is
//!
//! breathe carves requests/limits to a utilization SETPOINT (cost /
//! right-sizing). But requests+limits are ALSO the ISOLATION levers, and the
//! carve must respect that. **Isolation is a CONSTRAINT on the carve, exactly
//! like resiliency is** — it composes the dual-purpose property (cost AND
//! isolation together), it does not compete with it.
//!
//! The insight, lever by lever:
//! - **requests-floor** = the guaranteed-reservation FLOOR = isolation from
//!   noisy neighbors (a workload gets its reserved share regardless of
//!   contention). The carve LOWER BOUND.
//! - **limits-ceiling** = the interference CEILING = stops one workload
//!   starving others. The carve upper preference (right-size down).
//! - **QoS class** = who is evictable/throttleable under pressure. In
//!   Kubernetes it is *derived*: `requests==limits ⇒ Guaranteed` (never
//!   throttled below request, last evicted); `requests<limits ⇒ Burstable`
//!   (reserved floor + burst headroom); `no requests ⇒ BestEffort` (first
//!   evicted — NO isolation). So the posture and the resource carve are the
//!   same act, seen two ways.
//! - **placement isolation** = taints / anti-affinity / topology-spread /
//!   dedicated-nodes = how much a workload co-locates (bin-pack) vs separates
//!   (isolate-away).
//!
//! ## The worked receipt (the gap in action)
//!
//! The staging tendril session found `victoria-logs` STUCK because a
//! BestEffort-QoS pod (no requests) hit a `carve_failed` 422 → it had no
//! isolation floor → evictable / disturbed. A workload with no requests has no
//! seal. The fix was "add a request" = give it an isolation floor. This module
//! makes that class a typed invariant, not a live discovery.
//!
//! ## `/algorithmic-prowess-seal` — best-fit, NO ML
//!
//! - The **per-workload seal** (a critical workload cannot be unsealed) is a
//!   **refined type**: [`IsolationPosture::try_seal`] rejects
//!   Critical-with-BestEffort / zero-floor at construction; the fields are
//!   private, so an unsealed critical posture is unrepresentable past the
//!   boundary (parse-time-rejected).
//! - The **carve-preserves-the-seal** constraint is a **clamp with a sealed
//!   output type**: [`carve_respecting_seal`] returns a [`SealedCarve`] whose
//!   value is always `≥ seal_floor` — a below-seal carve has no code path in
//!   the output (parse-time-rejected). The raw `max` is only-mitigated; the
//!   `SealedCarve` OUTPUT is the seal.
//! - The **isolation-vs-efficiency-vs-cost optimization** is a typed
//!   **constrained** minimization ([`optimize_reserved`]): minimize cost
//!   (reserved capacity) s.t. the seal-floor is never crossed — the constraint
//!   is `max(objective, constraint)`, structural, not a soft penalty. Classical
//!   constrained bin-packing / QoS-assignment reduced to its contract core, no
//!   ML.

use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────────────────────
// LEVER 1 — QoS CLASS (the seal STRENGTH)
// ─────────────────────────────────────────────────────────────────────────────

/// The Kubernetes QoS class a workload's request/limit carve DERIVES. This is
/// the seal strength: who is throttled/evicted under pressure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QosClass {
    /// `requests == limits`. Never throttled below its request; LAST evicted.
    /// The strongest seal — a critical workload's posture.
    Guaranteed,
    /// `requests < limits`. A reserved floor (isolation from noisy neighbors up
    /// to the request) plus burst headroom to the limit. The standard seal.
    Burstable,
    /// No requests. FIRST evicted, throttled first — **NO isolation**. The
    /// no-seal posture (batch / interruptible work only). This is the
    /// victoria-logs-422 shape when a critical workload lands here.
    BestEffort,
}

impl QosClass {
    /// Does this QoS class carry an isolation FLOOR (a reserved share)? Only
    /// `BestEffort` does not — it is the no-seal posture.
    #[must_use]
    pub const fn is_sealed(self) -> bool {
        matches!(self, Self::Guaranteed | Self::Burstable)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Guaranteed => "guaranteed",
            Self::Burstable => "burstable",
            Self::BestEffort => "best-effort",
        }
    }

    /// Every QoS class, weakest-seal first.
    pub const ALL: [QosClass; 3] = [Self::BestEffort, Self::Burstable, Self::Guaranteed];
}

// ─────────────────────────────────────────────────────────────────────────────
// LEVER 4 — PLACEMENT ISOLATION (the seal BREADTH)
// ─────────────────────────────────────────────────────────────────────────────

/// The node-level placement isolation axis — how much a workload co-locates
/// (bin-pack / efficient) vs separates (isolate / sealed). The Rust contract
/// mirror of the config-spread placement/anti-affinity axis (breathe-spread
/// `axis::Placement` / `StorageBinding`) — REFERENCED, never re-implemented.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlacementIsolation {
    /// Co-locate freely — bin-pack. The efficient default (standard / batch).
    CoLocate,
    /// Pod anti-affinity — spread replicas off each other (survive a node loss;
    /// keep a noisy neighbor off a sensitive workload).
    AntiAffinity,
    /// Topology-spread — even distribution across zones/nodes (AZ resilience +
    /// contention diffusion).
    TopologySpread,
    /// Dedicated nodes (taint/toleration) — the strongest node-level seal; the
    /// workload runs alone. For a critical workload that must not share a kernel
    /// with a noisy neighbor, or a noisy workload isolated AWAY from everything.
    Dedicated,
}

impl PlacementIsolation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CoLocate => "co-locate",
            Self::AntiAffinity => "anti-affinity",
            Self::TopologySpread => "topology-spread",
            Self::Dedicated => "dedicated",
        }
    }

    /// Does this placement SEPARATE the workload from neighbors (isolate) rather
    /// than co-locate it (bin-pack)? Everything but `CoLocate` separates.
    #[must_use]
    pub const fn separates(self) -> bool {
        !matches!(self, Self::CoLocate)
    }

    pub const ALL: [PlacementIsolation; 4] =
        [Self::CoLocate, Self::AntiAffinity, Self::TopologySpread, Self::Dedicated];
}

// ─────────────────────────────────────────────────────────────────────────────
// THE WORKLOAD CLASS — the VARIANT axis (criticality × interference-sensitivity)
// ─────────────────────────────────────────────────────────────────────────────

/// The workload class — the criticality / interference-sensitivity variant that
/// selects the default isolation posture. Each is a typed lattice point on the
/// isolation dimension.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkloadClass {
    /// Interference-sensitive + must-not-be-disturbed. Guaranteed + anti-affinity
    /// + full isolation floor (dedicated if named). MUST be sealed — an unsealed
    /// Critical is the invariant violation.
    Critical,
    /// The ordinary workload. Burstable — a reserved floor absorbs contention,
    /// burst headroom above. Co-locate ok.
    Standard,
    /// Interruptible / re-runnable work. BestEffort + bin-packed — no seal needed
    /// (evicted first, re-dispatched cheaply). The one class for which a no-seal
    /// posture is CORRECT.
    Batch,
    /// A workload that DISTURBS others (a noisy neighbor). Burstable but hard-CAPPED
    /// (limit stops it starving others) + isolated AWAY (anti-affinity / dedicated)
    /// so its noise cannot reach a sensitive workload.
    Noisy,
}

impl WorkloadClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::Standard => "standard",
            Self::Batch => "batch",
            Self::Noisy => "noisy",
        }
    }

    /// **THE seal invariant, per class.** A Critical workload MUST be sealed
    /// (never BestEffort / zero-floor). Every other class may legally be
    /// unsealed (Batch is *meant* to be; Standard/Noisy carry a floor by default
    /// but a zero-floor is not a hard violation for them).
    #[must_use]
    pub const fn requires_seal(self) -> bool {
        matches!(self, Self::Critical)
    }

    /// The default QoS class for this workload class (the best-known posture).
    #[must_use]
    pub const fn default_qos(self) -> QosClass {
        match self {
            Self::Critical => QosClass::Guaranteed,
            Self::Standard | Self::Noisy => QosClass::Burstable,
            Self::Batch => QosClass::BestEffort,
        }
    }

    /// The default placement isolation for this class (the best-known posture).
    #[must_use]
    pub const fn default_placement(self) -> PlacementIsolation {
        match self {
            Self::Critical | Self::Noisy => PlacementIsolation::AntiAffinity,
            Self::Standard | Self::Batch => PlacementIsolation::CoLocate,
        }
    }

    pub const ALL: [WorkloadClass; 4] =
        [Self::Critical, Self::Standard, Self::Batch, Self::Noisy];
}

// ─────────────────────────────────────────────────────────────────────────────
// THE DISCOVERED SIGNAL — interference sensitivity (kanchi-discovered)
// ─────────────────────────────────────────────────────────────────────────────

/// A workload's DISCOVERED interference sensitivity — how much it degrades under
/// contention, read from live metrics (throttle ratio, eviction events,
/// latency-under-contention). Basis points `0..=10000` (0 = immune, 10000 =
/// maximally sensitive). The `default ← discovered ← contextual ← override`
/// precedence molds the posture from this signal (clause 5, discovery-molded).
///
/// A bounded integer (Copy, exact, no float in the stored value) — the
/// smallest-sufficient rung, no ML.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InterferenceSensitivity(u16);

impl InterferenceSensitivity {
    /// Clamp a raw basis-point reading into range. Total (no failure mode) — an
    /// out-of-range reading saturates rather than erroring, because a sensitivity
    /// reading is an observation, not a contract value.
    #[must_use]
    pub const fn from_bps(bps: u16) -> Self {
        Self(if bps > 10_000 { 10_000 } else { bps })
    }

    #[must_use]
    pub const fn bps(self) -> u16 {
        self.0
    }

    /// Is this workload interference-sensitive enough that it MUST be sealed,
    /// regardless of its declared class? The discovery escalation: a Standard
    /// workload observed highly sensitive is treated as needing the seal. The
    /// threshold (80%) is the homeostasis default.
    #[must_use]
    pub const fn is_seal_forcing(self) -> bool {
        self.0 >= 8_000
    }

    /// A low-sensitivity workload (< 20%) — safe to loosen toward efficiency
    /// (co-locate, tighter floor). The discovery can relax the default here.
    #[must_use]
    pub const fn is_loosenable(self) -> bool {
        self.0 < 2_000
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// THE SEALED POSTURE — the refined type (parse-time-rejected)
// ─────────────────────────────────────────────────────────────────────────────

/// Why a candidate isolation posture is not a legal SEAL for its class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SealError {
    /// A Critical (or discovered-seal-forcing) workload declared `BestEffort` —
    /// the no-isolation posture. THE victoria-logs-422 class.
    CriticalIsBestEffort,
    /// A Critical (or seal-forcing) workload with a zero requests-floor — no
    /// guaranteed reservation, so a noisy neighbor can starve it.
    CriticalHasNoFloor,
    /// The requests-floor exceeds the limits-ceiling — an impossible carve (the
    /// reservation cannot be larger than the cap).
    FloorAboveCeiling,
}

impl std::fmt::Display for SealError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SealError::CriticalIsBestEffort => write!(
                f,
                "a critical / interference-sensitive workload cannot be BestEffort (no isolation — the victoria-logs-422 class)"
            ),
            SealError::CriticalHasNoFloor => write!(
                f,
                "a critical / interference-sensitive workload needs a nonzero requests-floor (a guaranteed reservation)"
            ),
            SealError::FloorAboveCeiling => {
                write!(f, "the requests-floor cannot exceed the limits-ceiling")
            }
        }
    }
}

impl std::error::Error for SealError {}

/// A workload's ISOLATION POSTURE — the four levers as one sealed value. The
/// fields are PRIVATE; the only ingress is [`IsolationPosture::try_seal`], which
/// rejects an unsealed critical posture at construction. So a Critical posture
/// with `BestEffort` / zero-floor is unrepresentable past the boundary
/// (parse-time-rejected — a `Result::Err` at the seam + sealed in-Rust
/// construction).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IsolationPosture {
    class: WorkloadClass,
    qos: QosClass,
    /// The guaranteed reservation (requests), in dimension-agnostic units
    /// (bytes / millicores). The isolation FLOOR — the carve never goes below it.
    requests_floor: u64,
    /// The interference ceiling (limits). `0` = no cap (only legal for
    /// BestEffort). Otherwise `>= requests_floor`.
    limits_ceiling: u64,
    placement: PlacementIsolation,
}

impl IsolationPosture {
    /// **THE SEAL constructor (parse-time-rejected).** Construct a posture,
    /// rejecting an unsealed critical/seal-forcing workload:
    /// - a seal-required class that is `BestEffort` → [`SealError::CriticalIsBestEffort`];
    /// - a seal-required class with a zero floor → [`SealError::CriticalHasNoFloor`];
    /// - any floor above a nonzero ceiling → [`SealError::FloorAboveCeiling`].
    ///
    /// `seal_forced` folds the discovered signal in: a Standard workload observed
    /// `is_seal_forcing()` is treated as seal-required here, so discovery cannot
    /// silently leave a sensitive workload unsealed.
    ///
    /// # Errors
    /// [`SealError`] when the posture would leave a seal-required workload unsealed.
    pub fn try_seal(
        class: WorkloadClass,
        qos: QosClass,
        requests_floor: u64,
        limits_ceiling: u64,
        placement: PlacementIsolation,
        seal_forced: bool,
    ) -> Result<Self, SealError> {
        let must_seal = class.requires_seal() || seal_forced;
        if must_seal && matches!(qos, QosClass::BestEffort) {
            return Err(SealError::CriticalIsBestEffort);
        }
        if must_seal && requests_floor == 0 {
            return Err(SealError::CriticalHasNoFloor);
        }
        if limits_ceiling != 0 && requests_floor > limits_ceiling {
            return Err(SealError::FloorAboveCeiling);
        }
        Ok(Self { class, qos, requests_floor, limits_ceiling, placement })
    }

    /// Build the DEFAULT posture for a class (the best-known variant, §3). The
    /// floor/ceiling come from the observed working set; here they are inputs.
    /// Infallible for the class defaults because each class's default QoS already
    /// satisfies its own seal requirement (Critical→Guaranteed with a floor).
    ///
    /// # Errors
    /// [`SealError`] only if a caller passes a zero floor for `Critical`.
    pub fn for_class(
        class: WorkloadClass,
        requests_floor: u64,
        limits_ceiling: u64,
    ) -> Result<Self, SealError> {
        Self::try_seal(
            class,
            class.default_qos(),
            requests_floor,
            limits_ceiling,
            class.default_placement(),
            false,
        )
    }

    #[must_use]
    pub const fn class(&self) -> WorkloadClass {
        self.class
    }
    #[must_use]
    pub const fn qos(&self) -> QosClass {
        self.qos
    }
    #[must_use]
    pub const fn requests_floor(&self) -> u64 {
        self.requests_floor
    }
    #[must_use]
    pub const fn limits_ceiling(&self) -> u64 {
        self.limits_ceiling
    }
    #[must_use]
    pub const fn placement(&self) -> PlacementIsolation {
        self.placement
    }

    /// Is this posture sealed (carries an isolation floor)? True unless the QoS
    /// is `BestEffort`. A constructed Critical posture is ALWAYS sealed (the
    /// constructor guarantees it) — this is the runtime witness of that.
    #[must_use]
    pub const fn is_sealed(&self) -> bool {
        self.qos.is_sealed() && self.requests_floor > 0
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// THE CARVE-PRESERVES-THE-SEAL CONSTRAINT (the load-bearing bound)
// ─────────────────────────────────────────────────────────────────────────────

/// The output of a carve that RESPECTS a workload's isolation floor — its value
/// is ALWAYS `>= seal_floor`. Constructed only through [`carve_respecting_seal`],
/// which applies the `max` clamp, so a below-seal carved target has no code path
/// in this type (parse-time-rejected — the seal is the output type, not a runtime
/// check the caller might forget).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SealedCarve {
    target: u64,
    floor: u64,
}

impl SealedCarve {
    /// The sealed carved target (`>= floor`).
    #[must_use]
    pub const fn target(self) -> u64 {
        self.target
    }

    /// The isolation floor this carve preserved.
    #[must_use]
    pub const fn floor(self) -> u64 {
        self.floor
    }

    /// Did the seal BIND — i.e. did the raw carve want to go below the floor and
    /// get clamped up? `true` ⇒ cost wanted less but isolation held the line.
    #[must_use]
    pub const fn seal_bound(self) -> bool {
        self.target == self.floor
    }
}

/// **THE carve-preserves-the-seal constraint.** breathe's carve
/// (`carve_to_setpoint`) right-sizes toward the working set; this bounds it from
/// below by the isolation floor, so a critical workload's reservation is NEVER
/// carved away for cost. The one clamp achieves BOTH: the carve above the floor
/// is the cost win, the floor is the isolation/resiliency guarantee — the
/// dual-purpose lemma, extended to the isolation dimension.
///
/// `raw_carve_target` is what the cost carve wanted; `posture.requests_floor()`
/// is the seal. The result is `max(raw, floor)` as a [`SealedCarve`].
#[must_use]
pub fn carve_respecting_seal(raw_carve_target: u64, posture: &IsolationPosture) -> SealedCarve {
    let floor = posture.requests_floor();
    SealedCarve { target: raw_carve_target.max(floor), floor }
}

// ─────────────────────────────────────────────────────────────────────────────
// THE OVERLAY PRECEDENCE — default ← discovered ← contextual ← override
// ─────────────────────────────────────────────────────────────────────────────

/// A partial isolation override — the enjulho config shape (typed,
/// `deny_unknown_fields`, no `format!()`). Each field is `Option`: `None` = do
/// not touch this layer's value; `Some` = this layer's contribution to the
/// precedence fold. This is the ONE version-controlled config surface the
/// pleme-lib/breathe baseline renders (default-on, pangea-operator-reconciled —
/// declare + observe), mirroring the storage/costGuard/breathe enjulho surfaces.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IsolationOverlay {
    /// Override the workload class (rare — usually discovered/declared once).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class: Option<WorkloadClass>,
    /// Override the QoS class (e.g. a prod tenant forces Guaranteed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qos: Option<QosClass>,
    /// Override the requests-floor (the guaranteed reservation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests_floor: Option<u64>,
    /// Override the limits-ceiling (the interference cap).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limits_ceiling: Option<u64>,
    /// Override the placement isolation (e.g. a shared node-pool tightens to
    /// anti-affinity / dedicated).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placement: Option<PlacementIsolation>,
}

impl IsolationOverlay {
    /// Apply this overlay ON TOP of a base posture, re-sealing the result. A
    /// layer that would UNSEAL a critical workload is REJECTED at the fold (the
    /// seal survives every overlay) — so no contextual/override layer can strip
    /// the seal.
    ///
    /// # Errors
    /// [`SealError`] if the composed posture would leave a seal-required
    /// workload unsealed.
    pub fn apply(&self, base: &IsolationPosture, seal_forced: bool) -> Result<IsolationPosture, SealError> {
        IsolationPosture::try_seal(
            self.class.unwrap_or_else(|| base.class()),
            self.qos.unwrap_or_else(|| base.qos()),
            self.requests_floor.unwrap_or_else(|| base.requests_floor()),
            self.limits_ceiling.unwrap_or_else(|| base.limits_ceiling()),
            self.placement.unwrap_or_else(|| base.placement()),
            seal_forced,
        )
    }
}

/// **THE overlay precedence fold** (clause 5, discovery-molded): the best
/// default posture per class+context, resolved as
/// `default ← discovered ← contextual ← override`, applied left-to-right so the
/// rightmost layer wins each field. The discovered `sensitivity` folds in as the
/// `seal_forced` flag: a workload discovered `is_seal_forcing()` MUST be sealed
/// no matter what the layers say, so a highly-sensitive workload can never be
/// left unsealed by a loose override.
///
/// # Errors
/// [`SealError`] if the fully-resolved posture would leave a seal-required (or
/// discovered-seal-forcing) workload unsealed.
pub fn resolve_posture(
    default: &IsolationPosture,
    discovered: &IsolationOverlay,
    sensitivity: InterferenceSensitivity,
    contextual: &IsolationOverlay,
    override_: &IsolationOverlay,
) -> Result<IsolationPosture, SealError> {
    let seal_forced = sensitivity.is_seal_forcing();
    let a = discovered.apply(default, seal_forced)?;
    let b = contextual.apply(&a, seal_forced)?;
    override_.apply(&b, seal_forced)
}

// ─────────────────────────────────────────────────────────────────────────────
// THE CONSTRAINED OPTIMIZATION (isolation vs efficiency vs cost — NO ML)
// ─────────────────────────────────────────────────────────────────────────────

/// The reserved capacity a workload is optimized to — the minimum-cost value
/// that still HONORS its isolation floor. The objective is `carve_target` (the
/// efficient / cheap point); the constraint is `seal_floor`; the answer is
/// `max(objective, constraint)` — the constraint bounds the objective by
/// construction, so the minimum-cost feasible point is ALWAYS sealed.
///
/// This is the per-workload core of the constrained bin-packing / QoS-assignment
/// optimization: minimize `Σ reserved` subject to `∀ critical: sealed`. Because
/// the seal is a HARD constraint (a `max`, not a soft penalty), the optimizer
/// can never trade the seal away for cost. Classical, deterministic, no ML.
#[must_use]
pub fn optimize_reserved(posture: &IsolationPosture, carve_target: u64) -> u64 {
    carve_respecting_seal(carve_target, posture).target()
}

/// The total reserved cost across a fleet of (posture, carve_target) — the
/// objective the optimization minimizes, evaluated. Each term already honors its
/// seal, so the total is the minimum-cost SEALED assignment.
#[must_use]
pub fn total_reserved_cost(workloads: &[(IsolationPosture, u64)]) -> u64 {
    workloads.iter().map(|(p, t)| optimize_reserved(p, *t)).sum()
}

/// **THE feasibility predicate.** An assignment is feasible iff every
/// seal-required workload is sealed. This is the hard constraint the optimization
/// is subject to — the fleet-coverage witness (`all_critical_sealed`), the
/// runtime peer of the `critical_workload_must_be_sealed` matrix forcing-function.
#[must_use]
pub fn all_critical_sealed(postures: &[IsolationPosture]) -> bool {
    postures.iter().all(|p| !p.class().requires_seal() || p.is_sealed())
}

/// Find any seal-required workload that is NOT sealed — the violation set for a
/// `confirm` report. Empty ⇒ the fleet's critical workloads are all sealed.
/// (A constructed `IsolationPosture` cannot itself be an unsealed critical — this
/// scans postures that may have been mutated through a non-sealing path, e.g. a
/// deserialize that bypassed `try_seal`; it is the belt to the constructor's
/// braces.)
#[must_use]
pub fn unsealed_critical_workloads(postures: &[IsolationPosture]) -> Vec<WorkloadClass> {
    postures
        .iter()
        .filter(|p| p.class().requires_seal() && !p.is_sealed())
        .map(IsolationPosture::class)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── the per-class variant table (§3) ──────────────────────────────────────
    #[test]
    fn each_workload_class_has_its_best_known_posture() {
        assert_eq!(WorkloadClass::Critical.default_qos(), QosClass::Guaranteed);
        assert_eq!(WorkloadClass::Critical.default_placement(), PlacementIsolation::AntiAffinity);
        assert!(WorkloadClass::Critical.requires_seal());

        assert_eq!(WorkloadClass::Standard.default_qos(), QosClass::Burstable);
        assert!(!WorkloadClass::Standard.requires_seal());

        assert_eq!(WorkloadClass::Batch.default_qos(), QosClass::BestEffort);
        assert!(!WorkloadClass::Batch.requires_seal(), "batch is meant to be unsealed");

        assert_eq!(WorkloadClass::Noisy.default_qos(), QosClass::Burstable);
        assert_eq!(WorkloadClass::Noisy.default_placement(), PlacementIsolation::AntiAffinity, "noisy is isolated away");
    }

    // ── THE per-workload seal (parse-time-rejected) ───────────────────────────
    #[test]
    fn a_critical_workload_cannot_be_best_effort() {
        // THE victoria-logs-422 class — unrepresentable past the constructor.
        let err = IsolationPosture::try_seal(
            WorkloadClass::Critical,
            QosClass::BestEffort,
            0,
            0,
            PlacementIsolation::AntiAffinity,
            false,
        );
        assert_eq!(err, Err(SealError::CriticalIsBestEffort));
    }

    #[test]
    fn a_critical_workload_cannot_have_a_zero_floor() {
        let err = IsolationPosture::try_seal(
            WorkloadClass::Critical,
            QosClass::Guaranteed,
            0,
            100,
            PlacementIsolation::AntiAffinity,
            false,
        );
        assert_eq!(err, Err(SealError::CriticalHasNoFloor));
    }

    #[test]
    fn a_sealed_critical_posture_constructs_and_is_sealed() {
        let p = IsolationPosture::for_class(WorkloadClass::Critical, 512, 512).unwrap();
        assert!(p.is_sealed());
        assert_eq!(p.qos(), QosClass::Guaranteed);
    }

    #[test]
    fn a_batch_workload_may_be_best_effort() {
        // The one class for which the no-seal posture is CORRECT.
        let p = IsolationPosture::for_class(WorkloadClass::Batch, 0, 0).unwrap();
        assert!(!p.is_sealed());
        assert_eq!(p.qos(), QosClass::BestEffort);
    }

    #[test]
    fn discovery_can_force_a_seal_on_a_standard_workload() {
        // A Standard workload observed highly interference-sensitive MUST seal.
        let err = IsolationPosture::try_seal(
            WorkloadClass::Standard,
            QosClass::BestEffort,
            0,
            0,
            PlacementIsolation::CoLocate,
            true, // seal_forced by discovery
        );
        assert_eq!(err, Err(SealError::CriticalIsBestEffort));
    }

    #[test]
    fn a_floor_above_the_ceiling_is_rejected() {
        let err = IsolationPosture::try_seal(
            WorkloadClass::Standard,
            QosClass::Burstable,
            200,
            100,
            PlacementIsolation::CoLocate,
            false,
        );
        assert_eq!(err, Err(SealError::FloorAboveCeiling));
    }

    // ── THE carve-preserves-the-seal constraint ───────────────────────────────
    #[test]
    fn the_carve_never_strips_the_seal() {
        // A critical workload reserving 512; the cost carve wants to drop to 64
        // (idle). The seal holds the reservation at 512 — cost cannot strip it.
        let p = IsolationPosture::for_class(WorkloadClass::Critical, 512, 512).unwrap();
        let carved = carve_respecting_seal(64, &p);
        assert_eq!(carved.target(), 512, "the carve must not go below the isolation floor");
        assert!(carved.seal_bound(), "the seal bound the carve");
    }

    #[test]
    fn the_carve_wins_above_the_floor() {
        // When the working set is above the floor, the cost carve is free to
        // right-size down to it — cost AND isolation together (dual-purpose).
        let p = IsolationPosture::for_class(WorkloadClass::Standard, 64, 1024).unwrap();
        let carved = carve_respecting_seal(256, &p);
        assert_eq!(carved.target(), 256, "above the floor the carve wins (cost)");
        assert!(!carved.seal_bound());
    }

    #[test]
    fn a_below_seal_carve_is_unrepresentable_in_the_output() {
        // There is NO SealedCarve whose target is below its floor — the type is
        // the seal.
        let p = IsolationPosture::for_class(WorkloadClass::Critical, 1000, 1000).unwrap();
        for raw in [0u64, 1, 500, 999] {
            let c = carve_respecting_seal(raw, &p);
            assert!(c.target() >= c.floor(), "SealedCarve target below floor is unrepresentable");
        }
    }

    // ── the overlay precedence (default ← discovered ← contextual ← override) ──
    #[test]
    fn overlay_precedence_folds_left_to_right() {
        let default = IsolationPosture::for_class(WorkloadClass::Standard, 64, 256).unwrap();
        let discovered = IsolationOverlay { requests_floor: Some(128), ..Default::default() };
        let contextual = IsolationOverlay { placement: Some(PlacementIsolation::AntiAffinity), ..Default::default() };
        let over = IsolationOverlay { limits_ceiling: Some(512), ..Default::default() };
        let resolved = resolve_posture(
            &default,
            &discovered,
            InterferenceSensitivity::from_bps(3_000),
            &contextual,
            &over,
        )
        .unwrap();
        assert_eq!(resolved.requests_floor(), 128, "discovered layer applied");
        assert_eq!(resolved.placement(), PlacementIsolation::AntiAffinity, "contextual layer applied");
        assert_eq!(resolved.limits_ceiling(), 512, "override layer applied");
    }

    #[test]
    fn no_overlay_can_strip_the_seal_of_a_sensitive_workload() {
        // A highly-sensitive workload + a loose override that tries to make it
        // BestEffort → the seal survives (rejected at the fold).
        let default = IsolationPosture::for_class(WorkloadClass::Standard, 128, 256).unwrap();
        let loose = IsolationOverlay { qos: Some(QosClass::BestEffort), requests_floor: Some(0), ..Default::default() };
        let resolved = resolve_posture(
            &default,
            &IsolationOverlay::default(),
            InterferenceSensitivity::from_bps(9_500), // seal-forcing
            &IsolationOverlay::default(),
            &loose,
        );
        assert_eq!(resolved, Err(SealError::CriticalIsBestEffort), "a seal-forcing workload cannot be unsealed by an override");
    }

    #[test]
    fn a_loose_override_is_allowed_for_an_insensitive_workload() {
        // The same loose override on a low-sensitivity Batch-ish workload is fine.
        let default = IsolationPosture::for_class(WorkloadClass::Batch, 0, 0).unwrap();
        let resolved = resolve_posture(
            &default,
            &IsolationOverlay::default(),
            InterferenceSensitivity::from_bps(500), // loosenable
            &IsolationOverlay::default(),
            &IsolationOverlay::default(),
        );
        assert!(resolved.is_ok());
    }

    // ── the constrained optimization ───────────────────────────────────────────
    #[test]
    fn optimize_minimizes_cost_subject_to_the_seal() {
        let critical = IsolationPosture::for_class(WorkloadClass::Critical, 512, 512).unwrap();
        let standard = IsolationPosture::for_class(WorkloadClass::Standard, 64, 512).unwrap();
        // critical: carve wants 100, seal holds 512. standard: carve wants 200 > 64.
        assert_eq!(optimize_reserved(&critical, 100), 512, "seal bounds the critical carve");
        assert_eq!(optimize_reserved(&standard, 200), 200, "standard carve is free above its floor");
        let cost = total_reserved_cost(&[(critical, 100), (standard, 200)]);
        assert_eq!(cost, 512 + 200, "total reserved is the minimum-cost SEALED assignment");
    }

    #[test]
    fn all_critical_sealed_is_the_feasibility_predicate() {
        let critical = IsolationPosture::for_class(WorkloadClass::Critical, 512, 512).unwrap();
        let batch = IsolationPosture::for_class(WorkloadClass::Batch, 0, 0).unwrap();
        assert!(all_critical_sealed(&[critical, batch]), "a sealed critical + an unsealed batch is feasible");
        assert!(unsealed_critical_workloads(&[critical, batch]).is_empty());
    }

    #[test]
    fn qos_and_placement_labels_are_stable_and_unique() {
        fn uniq(v: &[&str]) -> bool {
            let mut s: Vec<&str> = v.to_vec();
            s.sort_unstable();
            s.dedup();
            s.len() == v.len()
        }
        assert!(uniq(&QosClass::ALL.map(QosClass::as_str)));
        assert!(uniq(&PlacementIsolation::ALL.map(PlacementIsolation::as_str)));
        assert!(uniq(&WorkloadClass::ALL.map(WorkloadClass::as_str)));
    }

    #[test]
    fn overlay_round_trips_and_rejects_unknown_fields() {
        let o = IsolationOverlay { qos: Some(QosClass::Guaranteed), requests_floor: Some(256), ..Default::default() };
        let js = serde_json::to_string(&o).unwrap();
        let back: IsolationOverlay = serde_json::from_str(&js).unwrap();
        assert_eq!(o, back);
        // deny_unknown_fields: a typo'd key is a parse error, not a silent drop.
        assert!(serde_json::from_str::<IsolationOverlay>(r#"{"qoss":"guaranteed"}"#).is_err());
    }
}
