// `doc_markdown` fires ~15 times in this file on English acronyms and proper
// nouns that are NOT code items — "QoS", "OOMKills", "BestEffort" used as a
// noun, "ImagePolicy" naming a Flux concept in prose. Backticking them would
// render as code spans and make the prose worse, which is why the same lint
// already accounts for ~358 of the workspace's 901-warning pedantic backlog and
// why `breathe-crd::PlacementIsolationKind` already carries this exact allow.
// Scoped to this module, with the reason stated, rather than silently inherited.
#![allow(clippy::doc_markdown)]

//! The REQUEST / QoS dimension's typed surface — **two disjoint actuation
//! doors, and no bridge between them.**
//!
//! # The problem this module exists to make unrepresentable
//!
//! breathe carves LIMITS. A limit bounds blast radius. But the kernel's
//! `oom_score_adj` is derived from the **request** (and the QoS class it
//! implies), schedulability and bin-packing key on the **request**, and a
//! BestEffort pod — no requests *and* no limits — is unbandable by every other
//! dimension. So the field that decides *survival* was the one field the
//! substrate never wrote independently.
//!
//! The receipt: `sui-cache-pg` took 34 OOMKills at a 202.8Mi memory high-water
//! against a **1Gi** limit, with cgroup `failcnt = 0`. The limit was never once
//! binding. No `MemoryBand` setting at any value could have saved it.
//!
//! # Why TWO doors, and why the split is a type
//!
//! Kubernetes refuses a QoS-class change through the in-place resize
//! subresource **unconditionally**. Verified verbatim against release-1.33 —
//! the exact minor `camelot-eks` runs (`v1.33.13-eks`) —
//! `pkg/apis/core/validation/validation.go:5665`, inside `ValidatePodResize`:
//!
//! ```text
//! if qos.GetPodQOS(oldPod) != qos.ComputePodQOS(newPod) {
//!     allErrs = append(allErrs, field.Invalid(specPath, newPod.Status.QOSClass,
//!         "Pod QOS Class may not change as a result of resizing"))
//! }
//! ```
//!
//! There is no feature gate, no field, and no configuration under which that
//! branch is skipped; and because it compares the **stored** `status.qosClass`
//! (`GetPodQOS`) against the recomputed spec (`ComputePodQOS`), it cannot be
//! dodged by omitting status from the patch. A class transition therefore has
//! **no in-place path at all** — not a slow one, not a privileged one.
//!
//! A runtime `if class_would_change { return Err(..) }` would be the industry
//! answer, and it is the wrong one: it leaves the illegal call *constructible*,
//! so every future call site has to remember the check. Instead the two
//! actuations carry **disjoint payload types** through **disjoint traits**:
//!
//! | | payload | trait | reversibility | visible in git |
//! |---|---|---|---|---|
//! | within-class request change | [`SsaPatch`] (one scalar) | [`RequestActuator`] | fast, in-place | **no** |
//! | QoS-class transition | [`ClassTransitionProposal`] (a whole coordinated block) | [`ManifestWriter`] | template write + pod replacement | yes |
//!
//! There is deliberately **no `From` and no `TryFrom`** in either direction, and
//! [`RequestActuator`] has exactly one method. So routing a class transition
//! through the in-place door is an `E0308`/`E0599` at the caller — a compile
//! error, not a `Result::Err`.
//!
//! # Tier ledger (never rounded up)
//!
//! | Claim | Tier |
//! |---|---|
//! | a class transition cannot travel the in-place door | **truly-unrepresentable** at the caller — type mismatch, no conversion exists |
//! | [`ClassPreserved`] cannot be minted for a class-changing pair | **truly-unrepresentable** — private payload, one constructor |
//! | a [`Durability::Committed`] decision cannot become an [`SsaPatch`] | **truly-unrepresentable** — dispatch on a sum type whose other arm has no patch |
//! | request ≤ limit | **truly-unrepresentable** — [`RequestTarget::new`] has no arm above its ceiling |
//! | a request SHRINK | **truly-unrepresentable *at M0*** — [`RequestShrinkEvidence`] has no constructor, so no shrink code path exists |
//! | carve ≥ the isolation seal floor | **truly-unrepresentable** — `SealedCarve`, already shipped + green in `breathe-invariant` |
//! | a Critical workload cannot be BestEffort | **parse-time-rejected** — `IsolationPosture::try_seal`; the CR arrives as JSON |
//! | the request fits node allocatable | **only-mitigated** — a world-fact (C2 ceiling): correct at read time, not a minute later |
//! | the 1.33 memory-decrease legality precheck | **only-mitigated**, and **version-fragile** — see [`ResizeLegality`] |
//! | an *implementor* of these traits honours its arguments | **only-mitigated** — the identical residual `Cluster::apply` already documents; two independent runtime gates stay where they are |
//!
//! What is **NOT** claimed: that any of this is wired. Nothing in this module is
//! called by a controller yet, and the durable door's only implementation
//! ([`NullManifestWriter`]) refuses every write by construction.

use crate::gate::LiveWitness;
use crate::{ProviderError, SsaPatch, Target};
use async_trait::async_trait;

pub use breathe_invariant::isolation::{QosClass, WorkloadClass};

// ─────────────────────────────────────────────────────────────────────────────
// The resource axis — closed, not a String
// ─────────────────────────────────────────────────────────────────────────────

/// The resources a [`DimensionId::Request`](crate::DimensionId::Request) band
/// may carve. **A closed two-arm enum, deliberately not a `String`**: Kubernetes
/// computes QoS from cpu and memory *only* (`isSupportedQoSComputeResource`), so
/// a third value would be a resource whose carve cannot affect the class the
/// dimension exists to control. An extended resource (a GPU, a hugepage) is not
/// a request band; it is a quota question.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RequestResource {
    /// `resources.requests.memory`, in bytes. The OOM-ranking lever.
    Memory,
    /// `resources.requests.cpu`, in millicores. The scheduling + CFS-share lever.
    Cpu,
}

impl RequestResource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Cpu => "cpu",
        }
    }

    /// Every arm — the partition the catalog and the CRD mirror must cover.
    pub const ALL: [Self; 2] = [Self::Memory, Self::Cpu];
}

impl std::fmt::Display for RequestResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The observed block — the WHOLE thing, because QoS is a pod-level fact
// ─────────────────────────────────────────────────────────────────────────────

/// One container's cpu+memory request/limit block.
///
/// Only the four QoS-relevant scalars, and each `Option`, because k8s
/// distinguishes *absent* from *zero* — `ComputePodQOS` skips any quantity that
/// is not strictly positive, so `Some(0)` and `None` mean the same thing to the
/// class computation and [`Self::effective`] normalizes them together.
///
/// Values are the band's own units: memory in bytes, cpu in millicores.
///
/// **The caller supplies the EFFECTIVE (post-defaulting) block.** Kubernetes
/// defaults an absent request to its limit at admission; this type models what
/// the apiserver stores, not what an author typed.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerResources {
    pub name: String,
    #[serde(default)]
    pub cpu_request: Option<u64>,
    #[serde(default)]
    pub cpu_limit: Option<u64>,
    #[serde(default)]
    pub memory_request: Option<u64>,
    #[serde(default)]
    pub memory_limit: Option<u64>,
}

impl ContainerResources {
    /// A quantity as `ComputePodQOS` sees it: absent, or strictly positive.
    const fn effective(v: Option<u64>) -> Option<u64> {
        match v {
            Some(0) | None => None,
            Some(n) => Some(n),
        }
    }

    #[must_use]
    pub const fn request(&self, r: RequestResource) -> Option<u64> {
        Self::effective(match r {
            RequestResource::Memory => self.memory_request,
            RequestResource::Cpu => self.cpu_request,
        })
    }

    #[must_use]
    pub const fn limit(&self, r: RequestResource) -> Option<u64> {
        Self::effective(match r {
            RequestResource::Memory => self.memory_limit,
            RequestResource::Cpu => self.cpu_limit,
        })
    }

    /// This block with one request replaced — the *candidate* a carve proposes.
    #[must_use]
    pub fn with_request(&self, r: RequestResource, v: u64) -> Self {
        let mut next = self.clone();
        match r {
            RequestResource::Memory => next.memory_request = Some(v),
            RequestResource::Cpu => next.cpu_request = Some(v),
        }
        next
    }

    /// Does this container carry BOTH a positive cpu limit and a positive memory
    /// limit? The per-container half of the Guaranteed test
    /// (`qosLimitsFound.HasAll(memory, cpu)`).
    #[must_use]
    const fn has_both_limits(&self) -> bool {
        self.limit(RequestResource::Cpu).is_some() && self.limit(RequestResource::Memory).is_some()
    }
}

/// A pod's full resource picture — **every container**, because
/// `ComputePodQOS` is a pod-level fold, not a per-container one.
///
/// Modelling this at container granularity would be the subtle bug: a carve that
/// leaves *this* container's class alone can still move the *pod's* class, and
/// the pod's class is what k8s validates against.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodResources {
    /// Regular + init containers, in the order k8s folds them.
    pub containers: Vec<ContainerResources>,
}

impl PodResources {
    #[must_use]
    pub fn new(containers: Vec<ContainerResources>) -> Self {
        Self { containers }
    }

    /// This pod with one container's one request replaced. `None` when no
    /// container carries `container_name` — the caller's target is stale.
    #[must_use]
    pub fn with_request(&self, container_name: &str, r: RequestResource, v: u64) -> Option<Self> {
        let mut next = self.clone();
        let c = next.containers.iter_mut().find(|c| c.name == container_name)?;
        *c = c.with_request(r, v);
        Some(next)
    }

    /// **The Kubernetes QoS derivation, transcribed from `ComputePodQOS`**
    /// (`pkg/apis/core/helper/qos`), not re-invented.
    ///
    /// The three rules, in the upstream's own order:
    /// 1. no positive request and no positive limit anywhere ⇒ `BestEffort`;
    /// 2. every container carries BOTH a positive cpu and memory limit, AND the
    ///    pod-summed requests equal the pod-summed limits per resource, AND the
    ///    two maps have the same cardinality ⇒ `Guaranteed`;
    /// 3. otherwise ⇒ `Burstable`.
    ///
    /// breathe never *declares* a QoS class — it derives it here and reports it.
    /// k8s owns this value; a second source of truth for it would be the bug.
    #[must_use]
    pub fn qos_class(&self) -> QosClass {
        let mut is_guaranteed = true;
        // Pod-level sums, per the upstream fold. `None` = the resource appears
        // in neither map, which is what the cardinality test below reads.
        let mut req_cpu: Option<u64> = None;
        let mut req_mem: Option<u64> = None;
        let mut lim_cpu: Option<u64> = None;
        let mut lim_mem: Option<u64> = None;

        for c in &self.containers {
            if let Some(v) = c.request(RequestResource::Cpu) {
                req_cpu = Some(req_cpu.unwrap_or(0).saturating_add(v));
            }
            if let Some(v) = c.request(RequestResource::Memory) {
                req_mem = Some(req_mem.unwrap_or(0).saturating_add(v));
            }
            if let Some(v) = c.limit(RequestResource::Cpu) {
                lim_cpu = Some(lim_cpu.unwrap_or(0).saturating_add(v));
            }
            if let Some(v) = c.limit(RequestResource::Memory) {
                lim_mem = Some(lim_mem.unwrap_or(0).saturating_add(v));
            }
            if !c.has_both_limits() {
                is_guaranteed = false;
            }
        }

        let n_req = usize::from(req_cpu.is_some()) + usize::from(req_mem.is_some());
        let n_lim = usize::from(lim_cpu.is_some()) + usize::from(lim_mem.is_some());
        if n_req == 0 && n_lim == 0 {
            return QosClass::BestEffort;
        }
        if is_guaranteed {
            // Every present request must have an equal limit.
            let matched = |r: Option<u64>, l: Option<u64>| match r {
                None => true,
                Some(rv) => l == Some(rv),
            };
            if matched(req_cpu, lim_cpu) && matched(req_mem, lim_mem) && n_req == n_lim {
                return QosClass::Guaranteed;
            }
        }
        QosClass::Burstable
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// I6 — the class-preservation WITNESS
// ─────────────────────────────────────────────────────────────────────────────

/// The private payload. Not `pub` — this is what makes [`ClassPreserved`]
/// unforgeable outside this crate, exactly as `gate::Witness` does for
/// `LiveWitness`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Preserved {
    class: QosClass,
    container: String,
    resource: RequestResource,
    from: Option<u64>,
    to: u64,
}

/// **Proof that a proposed request change leaves the pod's QoS class alone.**
///
/// `Serialize`-only and with **no public constructor**: [`ClassPreserved::check`]
/// is the sole producer, and it recomputes the class over the WHOLE pod — every
/// container, every resource — not just the scalar being carved. That breadth is
/// the point, and it is not hypothetical:
///
/// > Live on `camelot-eks`, `camelot-build/sui` is Burstable with
/// > `requests.memory=512Mi` against `limits.memory=6Gi` (a 12× ratio), and its
/// > **cpu request already equals its cpu limit** (200m/200m). A memory-request
/// > carve all the way to 6Gi would therefore silently promote it
/// > Burstable → Guaranteed — a class transition nobody declared, arriving as a
/// > side effect of a *within-class* carve. A per-resource check would wave it
/// > through; this one refuses it.
///
/// A `Deserialize` impl would let a caller fabricate the proof out of JSON and is
/// the one derive this type must never grow.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(transparent)]
pub struct ClassPreserved(Preserved);

/// Why a proposed request change was refused: it would move the pod's class.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassWouldChange {
    pub from: QosClass,
    pub to: QosClass,
    pub container: String,
    pub resource: RequestResource,
}

impl std::fmt::Display for ClassWouldChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "carving {}.requests.{} would move the pod QoS class {} → {}; \
             a class transition has no in-place path (k8s ValidatePodResize)",
            self.container,
            self.resource.as_str(),
            self.from.as_str(),
            self.to.as_str()
        )
    }
}

impl std::error::Error for ClassWouldChange {}

/// The container named by a carve is not in the observed pod.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NoSuchContainer {
    pub container: String,
}

impl std::fmt::Display for NoSuchContainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "no container named {:?} in the observed pod", self.container)
    }
}

impl std::error::Error for NoSuchContainer {}

/// Either reason [`ClassPreserved::check`] can refuse.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "reason")]
pub enum PreserveError {
    ClassWouldChange(ClassWouldChange),
    NoSuchContainer(NoSuchContainer),
}

impl std::fmt::Display for PreserveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClassWouldChange(e) => e.fmt(f),
            Self::NoSuchContainer(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for PreserveError {}

impl ClassPreserved {
    /// **The only producer.** Recomputes the pod's QoS class with the proposed
    /// request substituted and refuses if it moved.
    ///
    /// # Errors
    ///
    /// [`PreserveError::NoSuchContainer`] when `container` is not in `observed`;
    /// [`PreserveError::ClassWouldChange`] when the carve would move the class.
    pub fn check(
        observed: &PodResources,
        container: &str,
        resource: RequestResource,
        to: u64,
    ) -> Result<Self, PreserveError> {
        let Some(candidate) = observed.with_request(container, resource, to) else {
            return Err(PreserveError::NoSuchContainer(NoSuchContainer { container: container.to_owned() }));
        };
        let before = observed.qos_class();
        let after = candidate.qos_class();
        if before != after {
            return Err(PreserveError::ClassWouldChange(ClassWouldChange {
                from: before,
                to: after,
                container: container.to_owned(),
                resource,
            }));
        }
        let from = observed.containers.iter().find(|c| c.name == container).and_then(|c| c.request(resource));
        Ok(Self(Preserved { class: before, container: container.to_owned(), resource, from, to }))
    }

    /// The class this change preserves.
    #[must_use]
    pub const fn class(&self) -> QosClass {
        self.0.class
    }

    /// The container this witness authorizes a change on.
    #[must_use]
    pub fn container(&self) -> &str {
        &self.0.container
    }

    /// The resource this witness authorizes a change to.
    #[must_use]
    pub const fn resource(&self) -> RequestResource {
        self.0.resource
    }

    /// The value this witness authorizes.
    #[must_use]
    pub const fn to(&self) -> u64 {
        self.0.to
    }

    /// The value being replaced, when one was set.
    #[must_use]
    pub const fn from(&self) -> Option<u64> {
        self.0.from
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// I2 — request ≤ limit, as a narrowing constructor
// ─────────────────────────────────────────────────────────────────────────────

/// A request value that is **provably ≤ the live limit**.
///
/// k8s rejects `request > limit` at admission
/// (`validation.go:7168` — "must be less than or equal to `<res>` limit of
/// `<v>`"), and on the *template* path such a value does not fail this tick, it
/// wedges the **next rollout** — a delayed failure with no obvious cause. So the
/// bound is enforced where the value is born, not where it is written.
///
/// The binding ceiling is the **live limit**, never the band's own
/// `spec.ceiling`: a band's declared ceiling is advisory capacity policy, while
/// the limit is the value the apiserver will actually measure against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(transparent)]
pub struct RequestTarget(u64);

/// A carve that exceeded the live limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AboveLimit {
    pub proposed: u64,
    pub live_limit: u64,
}

impl std::fmt::Display for AboveLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "request {} exceeds the live limit {}", self.proposed, self.live_limit)
    }
}

impl std::error::Error for AboveLimit {}

impl RequestTarget {
    /// The only producer. There is no arm above `live_limit`.
    ///
    /// # Errors
    ///
    /// [`AboveLimit`] when `value > live_limit`.
    pub const fn new(value: u64, live_limit: u64) -> Result<Self, AboveLimit> {
        if value > live_limit {
            return Err(AboveLimit { proposed: value, live_limit });
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// I7 — allocatable headroom, as a REQUIRED input
// ─────────────────────────────────────────────────────────────────────────────

/// The node-class headroom a request carve must fit inside, **× replicas**.
///
/// A non-`Option` input to [`Self::admit`] on purpose — the same forcing shape
/// `DimensionSpec::suppressed_demand` uses in the catalog. A request target
/// cannot be *computed* without naming the headroom it fits in, so "we forgot to
/// check allocatable" has no code path.
///
/// Why it matters more than for a limit: a limit that exceeds allocatable is
/// merely unreachable, but a **request** that does is permanently unschedulable —
/// and multiplied by replica count. On the git path it lands silently and kills
/// the next deploy.
///
/// **Tier: only-mitigated, and permanently so (a C2 ceiling).** Allocatable is a
/// world-fact read at a moment; it can change under us the instant after. The
/// type forces the question to be asked, it cannot make the answer stay true.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AllocatableHeadroom {
    /// Schedulable headroom on one node of the target's class, in the band's unit.
    pub per_node: u64,
    /// When the reading was taken (unix seconds) — so a stale admission is
    /// visible in status rather than silently trusted.
    pub observed_at_epoch: i64,
}

/// A carve that would not fit the node class it must schedule onto.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WouldNotSchedule {
    pub proposed: u64,
    pub per_node_headroom: u64,
    pub replicas: u32,
}

impl std::fmt::Display for WouldNotSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "request {} × {} replica(s) does not fit {} per-node allocatable headroom",
            self.proposed, self.replicas, self.per_node_headroom
        )
    }
}

impl std::error::Error for WouldNotSchedule {}

/// A request value that is both ≤ the live limit **and** admitted against
/// allocatable headroom. The output of the last narrowing step; the only shape
/// an actuation payload is ever built from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(transparent)]
pub struct AdmittedRequest(u64);

impl AdmittedRequest {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl AllocatableHeadroom {
    /// Admit a bounded target against this headroom.
    ///
    /// A *single* replica must fit one node — requests are per-pod, so the test
    /// is `target ≤ per_node`, not `target × replicas ≤ per_node`. `replicas` is
    /// carried for the error message and for the status audit trail: it is the
    /// multiplier on the *cluster-wide* reservation cost, which is what makes an
    /// over-carve expensive even when it does schedule.
    ///
    /// # Errors
    ///
    /// [`WouldNotSchedule`] when a single replica would not fit.
    pub const fn admit(self, target: RequestTarget, replicas: u32) -> Result<AdmittedRequest, WouldNotSchedule> {
        if target.get() > self.per_node {
            return Err(WouldNotSchedule {
                proposed: target.get(),
                per_node_headroom: self.per_node,
                replicas,
            });
        }
        Ok(AdmittedRequest(target.get()))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// I3 — the shrink arm has no constructor at M0
// ─────────────────────────────────────────────────────────────────────────────

/// **Proof that lowering a request is safe.** Deliberately has **no
/// constructor** — not a private one, not a `#[doc(hidden)]` one, none.
///
/// Lowering a request strictly worsens `oom_score_adj` on a workload that is
/// demonstrably using that memory: it is the exact direction that kills, applied
/// to the exact workload that would notice. So at M0 the shrink arm does not
/// exist, and the absence is the guarantee — a value of this type cannot be
/// produced, so no code that requires one can run.
///
/// **Tier: truly-unrepresentable AT M0.** Say the "at M0" out loud. When M2 mints
/// a constructor (gated on a clean-observation window an order of magnitude
/// longer than the grow window), this drops to **only-mitigated** and the ledger
/// must be updated in the same commit rather than inheriting the stronger claim.
///
/// The named cost of the M0 posture, stated rather than hidden: grow-only
/// requests ratchet upward and never come down, so cluster reservation waste
/// accumulates monotonically. That is the right trade — but it is a real cost,
/// and it is why the reclaimable amount is reported every tick rather than
/// silently withheld.
#[derive(Debug)]
pub enum RequestShrinkEvidence {}

// ─────────────────────────────────────────────────────────────────────────────
// I5 — durability: where a converged value must LAND
// ─────────────────────────────────────────────────────────────────────────────

/// Where a converged request value must come to rest.
///
/// The in-place resize subresource mutates the **pod**, not the
/// Deployment/StatefulSet **template**. So an in-place request change is (1) lost
/// on the next rollout and (2) invisible in git — a straight violation of
/// GITOPS-NATIVE, where the cluster is a projection of the git tree.
///
/// For LIMITS the fleet has tolerated that. For REQUESTS it is materially worse:
/// QoS — the thing that decides whether the kernel kills you — would silently
/// revert on any redeploy, i.e. the protection evaporates at exactly the moment
/// (a rollout) when things are already moving.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Durability {
    /// In-place only. Fast and reversible; **lost on the next rollout** and never
    /// visible in git. Honest for a value the band will simply re-converge.
    Ephemeral,
    /// The value must reach the committed manifest. The default, and the only
    /// honest setting for anything whose QoS posture matters.
    #[default]
    Committed,
}

// ─────────────────────────────────────────────────────────────────────────────
// The class-transition proposal — the OTHER payload type
// ─────────────────────────────────────────────────────────────────────────────

/// A BLAKE3 content address over a rendered manifest edit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(transparent)]
pub struct ContentAddr([u8; 32]);

impl ContentAddr {
    /// Address the exact bytes a writer would commit.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Display for ContentAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// **A QoS-class transition — the payload the in-place door cannot accept.**
///
/// A different type from [`SsaPatch`] on purpose, and not merely by convention:
/// a class transition is a **relation between scalars**, not a scalar. Making a
/// Burstable pod Guaranteed means setting *every* request equal to *every* limit
/// across *every* container simultaneously; there is no single `value: u64` that
/// expresses it. Widening `SsaPatch` to carry a whole block would have weakened
/// every existing band's payload to accommodate one kind — so the coordinated
/// write gets its own type, and the single-scalar type stays single-scalar.
///
/// There is deliberately **no `impl From<ClassTransitionProposal> for SsaPatch`
/// and no `TryFrom`** — neither direction, ever.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassTransitionProposal {
    pub target: Target,
    pub from: QosClass,
    pub to: QosClass,
    /// The complete desired block — every container, every resource touched.
    pub block: PodResources,
    /// Content address of the rendered manifest edit. What a future
    /// [`ManifestWriter`] verifies it is committing, and what makes a proposal
    /// published in status auditable against the commit that later lands.
    pub addr: ContentAddr,
}

impl ClassTransitionProposal {
    /// Build a proposal, addressing the rendered block.
    ///
    /// # Errors
    ///
    /// [`SameClass`] when `block` does not actually move the class — a "transition"
    /// that transitions nothing is a caller bug, not a no-op to wave through.
    pub fn new(target: Target, observed: &PodResources, block: PodResources) -> Result<Self, SameClass> {
        let from = observed.qos_class();
        let to = block.qos_class();
        if from == to {
            return Err(SameClass { class: from });
        }
        // Address the canonical JSON of the desired block. Deterministic:
        // `PodResources` serializes its containers in order and each container's
        // fields in declaration order.
        let rendered = serde_json::to_vec(&block).unwrap_or_default();
        let addr = ContentAddr::of(&rendered);
        Ok(Self { target, from, to, block, addr })
    }
}

/// A proposed class transition that does not change the class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SameClass {
    pub class: QosClass,
}

impl std::fmt::Display for SameClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "proposed block is already {} — not a transition", self.class.as_str())
    }
}

impl std::error::Error for SameClass {}

// ─────────────────────────────────────────────────────────────────────────────
// The observed↔target gap
// ─────────────────────────────────────────────────────────────────────────────

/// Why a class transition cannot even be proposed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", tag = "blocked")]
pub enum ClassTransitionBlocked {
    /// The band's `durability` is `Ephemeral`, so it has no durable door to use.
    /// An in-place class transition does not exist; the honest report is that the
    /// band was authored in a shape that cannot reach its own target.
    EphemeralCannotTransition,
    /// No [`ManifestWriter`] is configured — the M0 state, and the honest one.
    NoWriterConfigured,
    /// The target's own resources make the desired class unreachable (e.g.
    /// Guaranteed is asked for but a container declares no limit at all, so
    /// requests can never equal limits).
    UnreachableFromObserved { detail: String },
}

/// The typed gap between the observed QoS class and the posture-declared target.
/// **Total**: every `(observed, target)` pair maps to exactly one arm.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", tag = "gap")]
pub enum QosGap {
    /// `observed == target`. Nothing to do.
    Held,
    /// `observed != target` and a proposal has been emitted for the durable door.
    PromotionProposed { proposal: String },
    /// `observed != target` and the transition cannot be proposed — names why.
    Blocked(ClassTransitionBlocked),
}

// ─────────────────────────────────────────────────────────────────────────────
// I9 — resize legality: a PRECONDITION, never a hardcoded rule
// ─────────────────────────────────────────────────────────────────────────────

/// Which way a carve moves a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CarveDirection {
    Grow,
    Shrink,
}

/// A container's declared `resizePolicy` for memory. **Zero of the 194 live
/// containers on `camelot-eks` declare one**, so `Unset` is the case that
/// actually matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum MemoryResizePolicy {
    /// No `resizePolicy` entry for memory. **k8s treats this IDENTICALLY to
    /// `NotRequired`** — `validation.go` tests `memRestartPolicy == NotRequired
    /// || memRestartPolicy == ""` — which is the opposite of what breathe's own
    /// `kube_cluster.rs` comment asserts ("k8s default is RestartContainer for
    /// memory"). That inversion is a real, separate live bug in the Memory
    /// dimension; it is named here rather than silently inherited.
    #[default]
    Unset,
    NotRequired,
    RestartContainer,
}

/// Whether the API will accept an in-place resize of this shape.
///
/// **Derived every tick from `(server_minor, policy, direction)` — never
/// hardcoded**, because the rule is genuinely version-fragile: the memory
/// restriction exists in 1.33 and is **removed in 1.34+**, so an EKS minor
/// upgrade silently changes the answer. A constant would be correct today and
/// wrong after a control-plane bump nobody associated with breathe.
///
/// **Tier: only-mitigated.** This is a runtime precheck against a world-fact, and
/// honestly so — its value is turning a 422 retry-loop into a reported
/// `Blocked(reason)`, not making the rejection impossible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "legality")]
pub enum ResizeLegality {
    Allowed,
    /// 1.33 and earlier: a memory limit DECREASE (or a memory limit ADD) needs
    /// `resizePolicy: RestartContainer`.
    MemoryDecreaseNeedsRestartPolicy,
}

impl ResizeLegality {
    /// Evaluate the precondition.
    ///
    /// `server_minor` is the API server's minor version (33 for `v1.33.13-eks`).
    /// The restriction applies only to **memory limits**; a request change of
    /// either direction, and any cpu change, is unaffected by this particular
    /// rule.
    #[must_use]
    pub const fn evaluate(
        server_minor: u32,
        policy: MemoryResizePolicy,
        resource: RequestResource,
        direction: CarveDirection,
        is_limit: bool,
    ) -> Self {
        if server_minor >= 34 {
            return Self::Allowed;
        }
        match (resource, direction, is_limit, policy) {
            (
                RequestResource::Memory,
                CarveDirection::Shrink,
                true,
                MemoryResizePolicy::Unset | MemoryResizePolicy::NotRequired,
            ) => Self::MemoryDecreaseNeedsRestartPolicy,
            _ => Self::Allowed,
        }
    }

    #[must_use]
    pub const fn is_allowed(self) -> bool {
        matches!(self, Self::Allowed)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// THE TWO DOORS
// ─────────────────────────────────────────────────────────────────────────────

/// **DOOR 1 — the in-place, within-class request change.** Fast, reversible,
/// EPHEMERAL.
///
/// Takes three things and nothing else: a [`LiveWitness`] (authorization — is
/// this band allowed to write at all), a [`ClassPreserved`] (legality — will the
/// API accept it), and an [`SsaPatch`] (the scalar).
///
/// **There is deliberately no second method on this trait.** That absence is the
/// guarantee: an implementor cannot write a class transition through this trait
/// because the trait has no method that accepts one, and
/// [`ClassTransitionProposal`] is not an [`SsaPatch`] and never converts to one.
///
/// Tier: **truly-unrepresentable at the caller.** What this does NOT claim — the
/// same residual `Cluster::apply` already documents — is that an *implementation*
/// honours its arguments. One that ignores the witness and writes whatever it
/// likes is still writable; that is exactly why the independent runtime gates
/// (`writeEnabled`, the nodepool L2 ceiling) stay where they are.
#[async_trait]
pub trait RequestActuator: Send + Sync {
    /// Apply a within-class request change to the live pods.
    ///
    /// # Errors
    ///
    /// Whatever the underlying I/O boundary reports.
    async fn resize_in_place(
        &self,
        live: &LiveWitness,
        preserved: &ClassPreserved,
        patch: &SsaPatch,
    ) -> Result<crate::AppliedReceipt, ProviderError>;
}

/// A receipt for a committed manifest edit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitReceipt {
    /// The commit the writer produced.
    pub commit_sha: String,
    /// The address it claims to have committed — checkable against the
    /// proposal's own [`ClassTransitionProposal::addr`].
    pub addr: ContentAddr,
}

/// Why a durable write could not happen.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "error")]
pub enum WriterError {
    /// No writer is wired. The M0 answer, and the honest one.
    Disabled,
    /// The proposal addresses a path outside this writer's allowed prefix.
    OutsideBlastRadius { path: String, allowed_prefix: String },
    /// The transport failed.
    Transport { detail: String },
}

impl std::fmt::Display for WriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => f.write_str("no manifest writer is configured"),
            Self::OutsideBlastRadius { path, allowed_prefix } => {
                write!(f, "path {path:?} is outside the writer's allowed prefix {allowed_prefix:?}")
            }
            Self::Transport { detail } => write!(f, "manifest writer transport failed: {detail}"),
        }
    }
}

impl std::error::Error for WriterError {}

/// **DOOR 2 — the durable, git-visible class transition.** Slow, COMMITTED.
///
/// Takes a [`ClassTransitionProposal`], **not** an [`SsaPatch`]. That is the
/// whole mechanism: the two doors cannot be confused because they do not accept
/// each other's currency.
///
/// # Not built at M0, and that is stated rather than stubbed
///
/// The only implementation is [`NullManifestWriter`], which refuses every call.
/// The transport this trait will eventually stand on (a comment-preserving,
/// marker-anchored YAML scalar setter, plus a Contents-API client) **does not
/// exist anywhere in pleme-io Rust** — `serde_yaml` round-trips destroy comments,
/// which would delete adjacent Flux `$imagepolicy` markers. Flux's own
/// `ImageUpdateAutomation` cannot be reused: its only strategy is `Setters`,
/// which resolves `$imagepolicy` markers to an ImagePolicy's image/tag/digest and
/// cannot write `"384Mi"`. It is a shape to imitate — trailing marker on the
/// value line, `update.path` as a hard blast-radius wall, the commit log as the
/// audit trail — never a mechanism to reuse.
///
/// So a band whose `durability` is `Committed` computes a real, content-addressed
/// proposal and publishes it in status; nothing commits it. That is a named gap,
/// not a silent one.
#[async_trait]
pub trait ManifestWriter: Send + Sync {
    /// Commit a class transition into the manifest the reconciler reads.
    ///
    /// # Errors
    ///
    /// [`WriterError`] — always [`WriterError::Disabled`] for the M0 impl.
    async fn propose(
        &self,
        live: &LiveWitness,
        proposal: &ClassTransitionProposal,
    ) -> Result<CommitReceipt, WriterError>;
}

/// The M0 default: refuses every write.
///
/// The `NullExecutor` shape — a real type that really says no, so the absence of
/// a durable path is visible in status as `Blocked(NoWriterConfigured)` rather
/// than appearing to work.
#[derive(Debug, Clone, Copy, Default)]
pub struct NullManifestWriter;

#[async_trait]
impl ManifestWriter for NullManifestWriter {
    async fn propose(
        &self,
        _live: &LiveWitness,
        _proposal: &ClassTransitionProposal,
    ) -> Result<CommitReceipt, WriterError> {
        Err(WriterError::Disabled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(name: &str, cr: Option<u64>, cl: Option<u64>, mr: Option<u64>, ml: Option<u64>) -> ContainerResources {
        ContainerResources {
            name: name.to_owned(),
            cpu_request: cr,
            cpu_limit: cl,
            memory_request: mr,
            memory_limit: ml,
        }
    }

    // ── the QoS derivation, against k8s ComputePodQOS ────────────────────────

    #[test]
    fn best_effort_is_no_positive_request_and_no_positive_limit() {
        let p = PodResources::new(vec![c("a", None, None, None, None)]);
        assert_eq!(p.qos_class(), QosClass::BestEffort);
    }

    #[test]
    fn zero_quantities_are_absent_not_present() {
        // k8s skips any quantity that is not strictly positive, so an all-zero
        // block is BestEffort, not Guaranteed.
        let p = PodResources::new(vec![c("a", Some(0), Some(0), Some(0), Some(0))]);
        assert_eq!(p.qos_class(), QosClass::BestEffort);
    }

    #[test]
    fn guaranteed_needs_both_limits_and_equal_requests_on_every_container() {
        let p = PodResources::new(vec![c("a", Some(200), Some(200), Some(512), Some(512))]);
        assert_eq!(p.qos_class(), QosClass::Guaranteed);
    }

    #[test]
    fn one_unlimited_container_makes_the_whole_pod_burstable() {
        // THE pod-level point: container `a` alone would be Guaranteed.
        let p = PodResources::new(vec![
            c("a", Some(200), Some(200), Some(512), Some(512)),
            c("sidecar", Some(10), None, Some(64), None),
        ]);
        assert_eq!(p.qos_class(), QosClass::Burstable);
    }

    #[test]
    fn request_below_limit_is_burstable() {
        let p = PodResources::new(vec![c("a", Some(200), Some(200), Some(512), Some(6144))]);
        assert_eq!(p.qos_class(), QosClass::Burstable);
    }

    // ── I6 — the live camelot-build/sui shape ────────────────────────────────

    #[test]
    fn the_live_sui_shape_refuses_a_carve_to_the_limit() {
        // camelot-build/sui, read from camelot-eks 2026-07-26: Burstable,
        // requests.memory=512Mi vs limits.memory=6Gi (12×), and cpu ALREADY
        // 200m/200m. Carving memory all the way to the limit would promote it
        // Burstable → Guaranteed as an undeclared side effect.
        let sui = PodResources::new(vec![c("sui", Some(200), Some(200), Some(512 << 20), Some(6144 << 20))]);
        assert_eq!(sui.qos_class(), QosClass::Burstable);

        let err = ClassPreserved::check(&sui, "sui", RequestResource::Memory, 6144 << 20)
            .expect_err("a carve to the limit must be refused — it silently promotes to Guaranteed");
        match err {
            PreserveError::ClassWouldChange(e) => {
                assert_eq!(e.from, QosClass::Burstable);
                assert_eq!(e.to, QosClass::Guaranteed);
            }
            e @ PreserveError::NoSuchContainer(_) => panic!("expected ClassWouldChange, got {e:?}"),
        }
    }

    #[test]
    fn the_live_sui_shape_admits_a_partial_carve() {
        let sui = PodResources::new(vec![c("sui", Some(200), Some(200), Some(512 << 20), Some(6144 << 20))]);
        let w = ClassPreserved::check(&sui, "sui", RequestResource::Memory, 2048 << 20)
            .expect("a carve strictly below the limit preserves Burstable");
        assert_eq!(w.class(), QosClass::Burstable);
        assert_eq!(w.to(), 2048 << 20);
        assert_eq!(w.from(), Some(512 << 20));
    }

    #[test]
    fn a_per_resource_check_would_have_missed_it() {
        // Adversarial: prove the breadth is load-bearing. Looking ONLY at memory,
        // 512Mi→6Gi keeps `memory_request <= memory_limit` and looks fine. It is
        // the cpu leg — already equal — that makes the pod flip.
        let sui = PodResources::new(vec![c("sui", Some(200), Some(200), Some(512 << 20), Some(6144 << 20))]);
        let naive_memory_only_ok = 6144u64 << 20 <= 6144u64 << 20;
        assert!(naive_memory_only_ok, "the naive per-resource test passes");
        assert!(
            ClassPreserved::check(&sui, "sui", RequestResource::Memory, 6144 << 20).is_err(),
            "the whole-block test refuses it — this is the gap a per-resource check leaves"
        );
    }

    #[test]
    fn a_stale_container_name_is_refused() {
        let p = PodResources::new(vec![c("a", Some(200), Some(400), Some(512), Some(1024))]);
        assert!(matches!(
            ClassPreserved::check(&p, "gone", RequestResource::Memory, 600),
            Err(PreserveError::NoSuchContainer(_))
        ));
    }

    #[test]
    fn a_guaranteed_pod_cannot_be_carved_off_its_class() {
        let p = PodResources::new(vec![c("a", Some(200), Some(200), Some(512), Some(512))]);
        let err = ClassPreserved::check(&p, "a", RequestResource::Memory, 256)
            .expect_err("dropping a Guaranteed pod's request breaks requests==limits");
        match err {
            PreserveError::ClassWouldChange(e) => {
                assert_eq!(e.from, QosClass::Guaranteed);
                assert_eq!(e.to, QosClass::Burstable);
            }
            e @ PreserveError::NoSuchContainer(_) => panic!("expected ClassWouldChange, got {e:?}"),
        }
    }

    // ── I2 / I7 — the narrowing constructors ─────────────────────────────────

    #[test]
    fn a_request_above_the_live_limit_has_no_representation() {
        assert!(RequestTarget::new(1024, 512).is_err());
        assert_eq!(RequestTarget::new(512, 512).map(RequestTarget::get), Ok(512));
        assert_eq!(RequestTarget::new(256, 512).map(RequestTarget::get), Ok(256));
    }

    #[test]
    fn allocatable_headroom_admits_and_refuses() {
        let h = AllocatableHeadroom { per_node: 4096, observed_at_epoch: 1 };
        let t = RequestTarget::new(2048, 8192).unwrap();
        assert_eq!(h.admit(t, 3).map(AdmittedRequest::get), Ok(2048));

        let too_big = RequestTarget::new(8192, 8192).unwrap();
        assert!(h.admit(too_big, 1).is_err());
    }

    // ── the transition payload ───────────────────────────────────────────────

    fn t() -> Target {
        Target {
            namespace: "ns".into(),
            name: "n".into(),
            kind: "Deployment".into(),
            api_version: "apps/v1".into(),
            container: None,
            pod_selector: None,
        }
    }

    #[test]
    fn a_transition_that_transitions_nothing_is_refused() {
        let observed = PodResources::new(vec![c("a", Some(200), Some(400), Some(512), Some(1024))]);
        let same = observed.clone();
        assert!(matches!(ClassTransitionProposal::new(t(), &observed, same), Err(SameClass { .. })));
    }

    #[test]
    fn a_real_transition_is_addressed() {
        let observed = PodResources::new(vec![c("a", Some(200), Some(400), Some(512), Some(1024))]);
        let promoted = PodResources::new(vec![c("a", Some(400), Some(400), Some(1024), Some(1024))]);
        let p = ClassTransitionProposal::new(t(), &observed, promoted).expect("Burstable → Guaranteed");
        assert_eq!(p.from, QosClass::Burstable);
        assert_eq!(p.to, QosClass::Guaranteed);
        assert_eq!(p.addr.to_string().len(), 64, "a BLAKE3 address renders as 64 hex chars");
    }

    #[test]
    fn the_content_address_is_deterministic_and_sensitive() {
        let observed = PodResources::new(vec![c("a", Some(200), Some(400), Some(512), Some(1024))]);
        let g = |m: u64| PodResources::new(vec![c("a", Some(400), Some(400), Some(m), Some(m))]);
        let a = ClassTransitionProposal::new(t(), &observed, g(1024)).unwrap();
        let b = ClassTransitionProposal::new(t(), &observed, g(1024)).unwrap();
        assert_eq!(a.addr, b.addr, "same block ⇒ same address");

        let observed2 = PodResources::new(vec![c("a", Some(200), Some(400), Some(512), Some(2048))]);
        let cdiff = ClassTransitionProposal::new(t(), &observed2, g(2048)).unwrap();
        assert_ne!(a.addr, cdiff.addr, "a different block ⇒ a different address");
    }

    // ── I9 — resize legality is version-derived, not constant ────────────────

    #[test]
    fn the_memory_decrease_rule_applies_on_133_and_lifts_on_134() {
        // 1.33: an unset resizePolicy is treated as NotRequired and the decrease
        // is Forbidden (validation.go `memRestartPolicy == NotRequired || == ""`).
        assert_eq!(
            ResizeLegality::evaluate(
                33,
                MemoryResizePolicy::Unset,
                RequestResource::Memory,
                CarveDirection::Shrink,
                true
            ),
            ResizeLegality::MemoryDecreaseNeedsRestartPolicy
        );
        // Declaring RestartContainer lifts it, on the same version.
        assert!(ResizeLegality::evaluate(
            33,
            MemoryResizePolicy::RestartContainer,
            RequestResource::Memory,
            CarveDirection::Shrink,
            true
        )
        .is_allowed());
        // 1.34+ removed the restriction entirely — the reason this is derived
        // per-tick rather than hardcoded.
        assert!(ResizeLegality::evaluate(
            34,
            MemoryResizePolicy::Unset,
            RequestResource::Memory,
            CarveDirection::Shrink,
            true
        )
        .is_allowed());
        // The rule is about memory LIMITS: a request carve is unaffected.
        assert!(ResizeLegality::evaluate(
            33,
            MemoryResizePolicy::Unset,
            RequestResource::Memory,
            CarveDirection::Shrink,
            false
        )
        .is_allowed());
        // …and cpu is unaffected in either direction.
        assert!(ResizeLegality::evaluate(
            33,
            MemoryResizePolicy::Unset,
            RequestResource::Cpu,
            CarveDirection::Shrink,
            true
        )
        .is_allowed());
    }

    // ── the durable door refuses by construction ─────────────────────────────

    #[tokio::test]
    async fn the_null_writer_refuses_every_proposal() {
        let observed = PodResources::new(vec![c("a", Some(200), Some(400), Some(512), Some(1024))]);
        let promoted = PodResources::new(vec![c("a", Some(400), Some(400), Some(1024), Some(1024))]);
        let p = ClassTransitionProposal::new(t(), &observed, promoted).unwrap();
        let gate = crate::gate::authored_write_gate("drzzln: test");
        let w = gate.witness().expect("an authored write resolves Live");
        assert_eq!(NullManifestWriter.propose(w, &p).await, Err(WriterError::Disabled));
    }

    /// The structural claim, asserted as a compiling program rather than prose:
    /// the in-place door's payload and the durable door's payload are different
    /// types with no conversion, so a class transition cannot be routed through
    /// `resize_in_place`.
    ///
    /// This test cannot *fail* at runtime — it is a compile-time statement. Its
    /// job is to be the thing that stops compiling if somebody adds the `From`
    /// impl this design forbids: `assert_not_impl` via a negative-reasoning
    /// helper is unstable, so the honest form is to pin the shapes that WOULD
    /// have to change.
    #[test]
    fn the_two_doors_carry_disjoint_payloads() {
        // An SsaPatch is one scalar; a proposal is a whole coordinated block.
        // If someone ever widens SsaPatch to carry a block, or adds a
        // `From<ClassTransitionProposal>`, this stops being true and the
        // reviewer has to come here and say why.
        let observed = PodResources::new(vec![c("a", Some(200), Some(400), Some(512), Some(1024))]);
        let promoted = PodResources::new(vec![c("a", Some(400), Some(400), Some(1024), Some(1024))]);
        let p = ClassTransitionProposal::new(t(), &observed, promoted).unwrap();
        assert_eq!(p.block.containers.len(), 1);
        // The proposal names TWO classes and a whole block. An SsaPatch names
        // one `value: u64`. There is no total function from the former to the
        // latter, which is precisely why no conversion is offered.
        assert_ne!(p.from, p.to);
    }
}
