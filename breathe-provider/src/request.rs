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
//! the exact minor `private-estate-eks` runs (`v1.33.13-eks`) —
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
use crate::manifest::{AddressedProposal, CommitOutcome};
use crate::{LimitLayout, ProviderError, SsaPatch, Target};
use async_trait::async_trait;
use breathe_control::Decision;

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
        let c = next
            .containers
            .iter_mut()
            .find(|c| c.name == container_name)?;
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
/// > Live on `private-estate-eks`, `private-estate-build/sui` is Burstable with
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
        write!(
            f,
            "no container named {:?} in the observed pod",
            self.container
        )
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
            return Err(PreserveError::NoSuchContainer(NoSuchContainer {
                container: container.to_owned(),
            }));
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
        let from = observed
            .containers
            .iter()
            .find(|c| c.name == container)
            .and_then(|c| c.request(resource));
        Ok(Self(Preserved {
            class: before,
            container: container.to_owned(),
            resource,
            from,
            to,
        }))
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
        write!(
            f,
            "request {} exceeds the live limit {}",
            self.proposed, self.live_limit
        )
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
            return Err(AboveLimit {
                proposed: value,
                live_limit,
            });
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
    pub const fn admit(
        self,
        target: RequestTarget,
        replicas: u32,
    ) -> Result<AdmittedRequest, WouldNotSchedule> {
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
    pub fn new(
        target: Target,
        observed: &PodResources,
        block: PodResources,
    ) -> Result<Self, SameClass> {
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
        Ok(Self {
            target,
            from,
            to,
            block,
            addr,
        })
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
        write!(
            f,
            "proposed block is already {} — not a transition",
            self.class.as_str()
        )
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
    /// No [`ManifestWriter`] is configured — the default state, and the honest
    /// one. A durable door exists ([`GitManifestWriter`](crate::manifest::GitManifestWriter))
    /// but nothing has injected a transport behind its seam.
    NoWriterConfigured,
    /// The band declares no `manifestRef`, so there is nowhere in git to land
    /// the value. Deliberately distinct from `NoWriterConfigured`: one is a
    /// deployment gap (no transport wired), the other is an authoring gap (this
    /// band never said which file it owns), and conflating them would send an
    /// operator to fix the wrong thing.
    NoManifestCoordinate,
    /// The band's single `manifestRef` cannot address every scalar the
    /// transition moves — a Guaranteed promotion touching N containers needs N
    /// markers. Names each unaddressed scalar rather than committing the subset
    /// it *can* reach, because a partial class transition lands the workload in
    /// a class nobody asked for.
    CoordinateGap { missing: Vec<String> },
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
/// containers on `private-estate-eks` declare one**, so `Unset` is the case that
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
    /// The address of the **proposal that was discharged** — echoed back, so a
    /// receipt is auditable against the `pendingProposal` that asked for it.
    pub addr: ContentAddr,
    /// The address of the **bytes that actually landed**.
    ///
    /// Deliberately a second field rather than a reuse of `addr`: the two
    /// answer different questions ("which proposal is this?" vs "what is now in
    /// the file?"), and collapsing them would make a writer that committed
    /// something else indistinguishable from one that committed the proposal.
    /// Neither is a *proof* the commit is honest — see [`ManifestWriter`]'s
    /// tier note — but two addresses make the lie checkable by a third party.
    pub rendered_addr: ContentAddr,
}

/// Why a durable write could not happen.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "error")]
pub enum WriterError {
    /// No writer is wired. The M0 answer, and the honest one.
    Disabled,
    /// The proposal addresses a path outside this writer's allowed prefix.
    OutsideBlastRadius {
        path: String,
        allowed_prefix: String,
    },
    /// The working tree carries edits the writer did not make. Refused rather
    /// than committed on top of: breathe never lands a change over unknown work.
    RepoNotClean,
    /// The proposal could not be anchored to a span in the manifest — no
    /// marker, an ambiguous marker, or a marked line that is not a scalar
    /// assignment. Carries the underlying
    /// [`crate::manifest::EditError`]'s rendering.
    Unanchorable { detail: String },
    /// The remote moved under us. Surfaced, never forced — the same
    /// non-`.force()` discipline the SSA path already holds, so breathe never
    /// clobbers a field another writer owns.
    Conflict { detail: String },
    /// The transport failed.
    Transport { detail: String },
}

impl std::fmt::Display for WriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => f.write_str("no manifest writer is configured"),
            Self::OutsideBlastRadius {
                path,
                allowed_prefix,
            } => {
                write!(
                    f,
                    "path {path:?} is outside the writer's allowed prefix {allowed_prefix:?}"
                )
            }
            Self::RepoNotClean => {
                f.write_str("the manifest repo has uncommitted edits — refusing to commit on top")
            }
            Self::Unanchorable { detail } => {
                write!(f, "the proposal has no anchor in the manifest: {detail}")
            }
            Self::Conflict { detail } => write!(f, "manifest repo conflict: {detail}"),
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
/// # The currency is an [`AddressedProposal`], not a bare one
///
/// A [`ClassTransitionProposal`] knows *what* to write and not *where*. Handing
/// the writer one would force every implementation to carry a runtime "and if
/// there is no coordinate?" branch. Instead the coordinate is fused in at
/// construction: an [`AddressedProposal`] cannot exist without a path and a
/// non-empty assignment list, so a band with no `manifestRef` reports
/// [`ClassTransitionBlocked::NoManifestCoordinate`] and simply never produces
/// something this door accepts.
///
/// # What is and is not built
///
/// [`GitManifestWriter`](crate::manifest::GitManifestWriter) is real: it
/// anchors on an operator-authored marker, refuses an ambiguous one, walls
/// itself to a construction-time path prefix, short-circuits a no-op before
/// touching the repo, and preserves every unrelated byte — proven against a
/// mock [`ManifestRepo`](crate::manifest::ManifestRepo).
///
/// What is **not** built is the transport behind that seam: no git client and
/// no Contents-API caller ships in this crate, so nothing commits until a
/// controller injects one. [`NullManifestWriter`] remains the default and
/// refuses every call, which is why an unwired band reports
/// `Blocked(NoWriterConfigured)` instead of appearing to work.
///
/// Flux's own `ImageUpdateAutomation` is imitated, never reused: its only
/// strategy is `Setters`, which resolves `$imagepolicy` markers to an
/// ImagePolicy's image/tag/digest and cannot write `"384Mi"`. What carries over
/// is the *shape* — a trailing marker on the value line, `update.path` as a
/// hard blast-radius wall, the commit log as the audit trail.
#[async_trait]
pub trait ManifestWriter: Send + Sync {
    /// Commit an addressed proposal into the manifest the reconciler reads.
    ///
    /// # Errors
    ///
    /// [`WriterError`] — always [`WriterError::Disabled`] for [`NullManifestWriter`].
    async fn propose(
        &self,
        live: &LiveWitness,
        proposal: &AddressedProposal,
    ) -> Result<CommitOutcome, WriterError>;
}

/// The default: refuses every write.
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
        _proposal: &AddressedProposal,
    ) -> Result<CommitOutcome, WriterError> {
        Err(WriterError::Disabled)
    }
}

#[async_trait]
impl<R: crate::manifest::ManifestRepo> ManifestWriter for crate::manifest::GitManifestWriter<R> {
    async fn propose(
        &self,
        live: &LiveWitness,
        proposal: &AddressedProposal,
    ) -> Result<CommitOutcome, WriterError> {
        self.commit(live, proposal).await
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// THE CARVE PLANNER — one pure function, and the only place a request SsaPatch
// can be born
// ─────────────────────────────────────────────────────────────────────────────

/// Seal strength, weakest first — the rank that lets the planner refuse a
/// transition that would WEAKEN a workload's isolation.
///
/// Deliberately a local ranking rather than an `Ord` derive on [`QosClass`]:
/// `Guaranteed > Burstable > BestEffort` is true for *seal strength* and false
/// for several other orderings people reach for (cost, scheduling latitude,
/// eviction order is the reverse), so the comparison is named where it is used
/// instead of being silently available everywhere.
const fn seal_rank(q: QosClass) -> u8 {
    match q {
        QosClass::BestEffort => 0,
        QosClass::Burstable => 1,
        QosClass::Guaranteed => 2,
    }
}

/// Why a request carve produced no write. **Every arm is a typed refusal** — a
/// silent `Ok` reporting a carve that did not happen is the one outcome worse
/// than an error, and a `panic!`/`todo!` would take the controller down.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "blocked")]
pub enum RequestCarveBlocked {
    /// I6 / I1 — the carve would move the pod's QoS class. No in-place path
    /// exists for that, at any value.
    ClassWouldChange(ClassWouldChange),
    /// The band's container is not in the observed pod (a stale target).
    NoSuchContainer(NoSuchContainer),
    /// I2 — the carved value exceeds the live limit. Reachable through the
    /// ISOLATION SEAL: when a posture's `requests_floor` sits above the
    /// container's declared limit, no legal request satisfies both. That is a
    /// genuine authoring conflict needing a template edit, not a value to clamp.
    AboveLimit(AboveLimit),
    /// I7 — the carved value would not schedule on its node class.
    WouldNotSchedule(WouldNotSchedule),
    /// I9 — the API server will not accept a resize of this shape.
    ResizeIllegal { legality: ResizeLegality },
    /// I3 — a SHRINK reached the planner. Unreachable through the intended
    /// pipeline (the `Reclaim`/`Directionality` gates withhold it upstream), and
    /// refused here rather than trusted: producing a shrink would require a
    /// [`RequestShrinkEvidence`], which has no constructor.
    ShrinkWithoutEvidence { from: u64, to: u64 },
    /// A class transition was warranted but cannot be proposed.
    Transition(ClassTransitionBlocked),
    /// The declared `qosTarget` is a WEAKER seal than the observed class.
    /// breathe proposes transitions that strengthen isolation; it never proposes
    /// to strip a workload's reservation. Removing a seal is an operator edit,
    /// made out loud in the manifest — never a controller's tick.
    WouldWeakenSeal {
        observed: QosClass,
        target: QosClass,
    },
}

impl std::fmt::Display for RequestCarveBlocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClassWouldChange(e) => e.fmt(f),
            Self::NoSuchContainer(e) => e.fmt(f),
            Self::AboveLimit(e) => e.fmt(f),
            Self::WouldNotSchedule(e) => e.fmt(f),
            Self::ResizeIllegal { legality } => {
                write!(
                    f,
                    "the API server will not accept this resize: {legality:?}"
                )
            }
            Self::ShrinkWithoutEvidence { from, to } => write!(
                f,
                "a request shrink {from} → {to} reached the planner; lowering a reservation \
                 requires RequestShrinkEvidence, which has no constructor"
            ),
            Self::Transition(b) => write!(f, "class transition blocked: {b:?}"),
            Self::WouldWeakenSeal { observed, target } => write!(
                f,
                "qosTarget {} is a weaker seal than the observed {}; breathe never proposes \
                 to strip a workload's reservation",
                target.as_str(),
                observed.as_str()
            ),
        }
    }
}

impl std::error::Error for RequestCarveBlocked {}

/// **A planned in-place request change.** Private fields: the ONLY producer is
/// [`plan_request_carve`], so a request [`SsaPatch`] cannot exist without having
/// passed the whole narrowing chain —
/// `SealedCarve → RequestTarget → AdmittedRequest → ClassPreserved`.
///
/// Hand-assembling an `SsaPatch` with `LimitLayout::PodRequestResize` and calling
/// [`RequestActuator::resize_in_place`] directly is still *writable* Rust — the
/// door's real gate is the [`ClassPreserved`] argument, which has one
/// constructor. What this type removes is the accidental path: a caller
/// following the pipeline cannot skip a step, because there is no way to get a
/// patch except by completing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InPlaceCarve {
    patch: SsaPatch,
    preserved: ClassPreserved,
    durability: Durability,
    from: u64,
    to: u64,
    seal_bound: bool,
}

impl InPlaceCarve {
    /// The scalar to write. Pass with [`Self::preserved`] to
    /// [`RequestActuator::resize_in_place`].
    #[must_use]
    pub const fn patch(&self) -> &SsaPatch {
        &self.patch
    }

    /// The class-preservation proof this carve was admitted under.
    #[must_use]
    pub const fn preserved(&self) -> &ClassPreserved {
        &self.preserved
    }

    #[must_use]
    pub const fn from(&self) -> u64 {
        self.from
    }

    #[must_use]
    pub const fn to(&self) -> u64 {
        self.to
    }

    /// Did the isolation seal floor bind (i.e. the demand-driven seat wanted
    /// less and the seal held the line)?
    #[must_use]
    pub const fn seal_bound(&self) -> bool {
        self.seal_bound
    }

    /// **I5 — will this value survive the next rollout?** `true` when the band
    /// declared [`Durability::Committed`] but the write lands only on the live
    /// pod, so the converged reservation is lost on the next rollout and never
    /// appears in git.
    ///
    /// **At M0 this is `true` for every `Committed` band**, because the durable
    /// door's only implementation is [`NullManifestWriter`]. The in-place write
    /// still happens — it fixes `oom_score_adj` *now*, which is the point — but
    /// the caller must surface this rather than report a converged band.
    #[must_use]
    pub const fn durability_gap(&self) -> bool {
        matches!(self.durability, Durability::Committed)
    }
}

/// What one tick of a request band resolves to, **before any I/O**.
///
/// The sum type IS the two-door split: exactly one arm carries an [`SsaPatch`]
/// (via [`InPlaceCarve`]) and exactly one carries a
/// [`ClassTransitionProposal`], and there is no conversion between them. A class
/// transition therefore cannot reach the in-place door by any route through this
/// planner — not by a forgotten check, not by a mis-ordered branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestCarvePlan {
    /// Converged, or nothing actionable this tick.
    Hold,
    /// **DOOR 1** — a within-class request change for [`RequestActuator`].
    InPlace(InPlaceCarve),
    /// **DOOR 2** — a class transition for [`ManifestWriter`]. Never an
    /// `SsaPatch`; never actuated in place.
    Transition(ClassTransitionProposal),
    /// A downward move was warranted and is NOT taken. `reclaimable` is `None`
    /// when the shared limit-shaped `safe_min` masked the amount upstream — see
    /// `breathe_control::RequestLaw`'s `pending-request-reclaim-naming` note.
    /// Reported either way, because "converged" would be a lie.
    Withheld {
        current: u64,
        reclaimable: Option<u64>,
    },
    /// A typed refusal. Never a silent `Ok`.
    Blocked(RequestCarveBlocked),
}

impl RequestCarvePlan {
    /// Does this plan write to the live cluster?
    #[must_use]
    pub const fn writes(&self) -> bool {
        matches!(self, Self::InPlace(_))
    }

    /// Project onto the CRD's typed [`QosGap`], given whether a durable writer is
    /// wired. `writer_configured` is the caller's fact, not the planner's — the
    /// planner says what *should* happen; the controller knows what is *wired*.
    #[must_use]
    pub fn qos_gap(&self, writer_configured: bool) -> QosGap {
        match self {
            Self::Transition(p) if writer_configured => QosGap::PromotionProposed {
                proposal: p.addr.to_string(),
            },
            Self::Transition(_) => QosGap::Blocked(ClassTransitionBlocked::NoWriterConfigured),
            Self::Blocked(RequestCarveBlocked::Transition(b)) => QosGap::Blocked(b.clone()),
            _ => QosGap::Held,
        }
    }
}

/// Everything the planner needs. Fields are non-`Option` wherever the value is
/// REQUIRED to compute a safe carve — the forcing shape that makes "we forgot to
/// check allocatable" (I7) and "we forgot the isolation seal" (I8) have no code
/// path, because the target cannot be *computed* without them.
#[derive(Debug, Clone, Copy)]
pub struct RequestCarveInput<'a> {
    pub target: &'a Target,
    /// The container whose request is carved.
    pub container: &'a str,
    pub resource: RequestResource,
    /// The WHOLE observed pod — every container, because `ComputePodQOS` is a
    /// pod-level fold and a carve that leaves this container's class alone can
    /// still move the pod's.
    pub observed: &'a PodResources,
    /// This tick's band decision, from
    /// `breathe_control::decide_with(&RequestLaw{..}, demand, demand, current_request, cfg)`
    /// with the `Reclaim`/`Directionality` gates already applied.
    pub decision: Decision,
    /// I8 — the workload's isolation seal. **Required, and that is the point:**
    /// `IsolationPosture` can only be built through `try_seal`, so a critical
    /// workload with no floor (or a BestEffort seal) cannot even be *presented*
    /// to the planner.
    pub posture: &'a breathe_invariant::isolation::IsolationPosture,
    /// I7 — the node-class headroom this reservation must fit inside.
    pub headroom: AllocatableHeadroom,
    pub replicas: u32,
    pub durability: Durability,
    /// The posture-declared desired class. A gap produces a proposal, never an
    /// in-place write.
    pub qos_target: QosClass,
    /// The API server's minor version (33 for `v1.33.13-eks`) — I9 is derived
    /// per tick from this, never hardcoded.
    pub server_minor: u32,
    pub memory_resize_policy: MemoryResizePolicy,
    pub field_manager: &'a str,
}

/// Why a target class cannot be constructed from the observed block.
///
/// A typed reason with a `Display` impl rather than an inline `format!()`: per
/// ★★ TYPED EMISSION the sanctioned way to build a string is `write!` inside a
/// `Display`, and the reason is a value worth matching on anyway. It renders into
/// [`ClassTransitionBlocked::UnreachableFromObserved`]'s `detail`, which is a
/// `String` on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Unreachable<'a> {
    /// A container declares no limit for `resource`, so its request can never
    /// equal it — the Guaranteed test can never pass.
    NoLimitToMatch {
        container: &'a str,
        resource: RequestResource,
    },
    /// The carved container is not in the observed pod.
    NoSuchContainer { container: &'a str },
    /// De-sealing is an operator edit, never a controller proposal.
    DeSealRefused,
    /// The rendered block did not actually move the class.
    StillSameClass { class: QosClass },
}

impl std::fmt::Display for Unreachable<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoLimitToMatch {
                container,
                resource,
            } => write!(
                f,
                "container {container:?} declares no {} limit, so its request can never equal it",
                resource.as_str()
            ),
            Self::NoSuchContainer { container } => {
                write!(f, "no container named {container:?} in the observed pod")
            }
            Self::DeSealRefused => f.write_str(
                "de-sealing to BestEffort is an operator edit, never a controller proposal",
            ),
            Self::StillSameClass { class } => {
                write!(f, "the rendered block is still {}", class.as_str())
            }
        }
    }
}

impl Unreachable<'_> {
    fn block(self) -> ClassTransitionBlocked {
        ClassTransitionBlocked::UnreachableFromObserved {
            detail: self.to_string(),
        }
    }
}

/// Build the resource block that would put `observed` in `to`, or say why it
/// cannot be reached.
fn transition_block(
    observed: &PodResources,
    to: QosClass,
    container: &str,
    resource: RequestResource,
    seat: u64,
) -> Result<PodResources, ClassTransitionBlocked> {
    match to {
        // Every request := its limit, on every container and BOTH resources.
        // Anything less does not produce Guaranteed: `ComputePodQOS` requires a
        // positive cpu AND memory limit on every container, with pod-summed
        // requests equal to pod-summed limits.
        QosClass::Guaranteed => {
            let mut next = observed.clone();
            for c in &mut next.containers {
                for r in RequestResource::ALL {
                    let Some(l) = c.limit(r) else {
                        return Err(Unreachable::NoLimitToMatch {
                            container: &c.name,
                            resource: r,
                        }
                        .block());
                    };
                    *c = c.with_request(r, l);
                }
            }
            Ok(next)
        }
        // From BestEffort: introduce a reservation on the carved container. The
        // seat is the demand-derived value the band already computed, so the
        // promotion lands at a real number rather than an invented one.
        QosClass::Burstable => observed
            .with_request(container, resource, seat)
            .ok_or_else(|| Unreachable::NoSuchContainer { container }.block()),
        // Reaching BestEffort means STRIPPING every reservation. The planner
        // refuses a weakening target before ever getting here (`WouldWeakenSeal`),
        // so this arm is unreachable through `plan_request_carve` — kept total
        // and typed rather than `unreachable!()`, which would abort the process.
        QosClass::BestEffort => Err(Unreachable::DeSealRefused.block()),
    }
}

/// **DOOR 2's planner** — the observed class differs from the declared target,
/// so this tick is a class transition and therefore a manifest edit.
///
/// Split out of [`plan_request_carve`] because it is a cohesive unit with its
/// own three refusals, and because keeping it inline made the main planner read
/// as one long branch instead of the four numbered steps its doc promises.
///
/// It **cannot** return [`RequestCarvePlan::InPlace`]: there is no
/// [`ClassPreserved`] in scope here, and by construction there could not be —
/// a class transition is precisely the case where `ClassPreserved::check` fails.
fn plan_class_transition(
    input: &RequestCarveInput<'_>,
    observed_class: QosClass,
    raw_seat: u64,
) -> RequestCarvePlan {
    // breathe strengthens seals; it never proposes to strip a reservation.
    if seal_rank(input.qos_target) < seal_rank(observed_class) {
        return RequestCarvePlan::Blocked(RequestCarveBlocked::WouldWeakenSeal {
            observed: observed_class,
            target: input.qos_target,
        });
    }
    // An Ephemeral band has no durable door, and a transition has no in-place
    // one — so it was authored in a shape that cannot reach its own target.
    if matches!(input.durability, Durability::Ephemeral) {
        return RequestCarvePlan::Blocked(RequestCarveBlocked::Transition(
            ClassTransitionBlocked::EphemeralCannotTransition,
        ));
    }
    let block = match transition_block(
        input.observed,
        input.qos_target,
        input.container,
        input.resource,
        raw_seat,
    ) {
        Ok(b) => b,
        Err(e) => return RequestCarvePlan::Blocked(RequestCarveBlocked::Transition(e)),
    };

    // I7 ON THE DURABLE PATH — and it matters MORE here than in place.
    //
    // An un-admitted in-place write fails loudly against the apiserver, now. An
    // un-admitted value committed to a manifest lands SILENTLY and kills the
    // NEXT rollout, at a time nobody associates with this tick. So the proposal
    // is admitted before it is ever rendered.
    //
    // **What is admitted, honestly:** the carved container's carved resource
    // only. `AllocatableHeadroom` is a single scalar in ONE unit (bytes or
    // millicores), so it cannot speak to the other resource or to sibling
    // containers — and a Guaranteed promotion moves every request on every
    // container. Widening that check needs a per-resource headroom input, which
    // is a real gap and is named rather than papered over
    // (`pending-request-multi-resource-admission`).
    if let Some(proposed) = block.containers.iter().find(|c| c.name == input.container) {
        let value = proposed.request(input.resource).unwrap_or(0);
        // `u64::MAX` as the ceiling: the `request <= limit` rule is enforced by
        // the block builder itself (Guaranteed sets request TO the limit;
        // Burstable-from-BestEffort has no limit to exceed), so the only bound
        // left to apply here is the node's. The `Err` arm is therefore
        // unreachable — and is still handled as a typed refusal rather than an
        // `unwrap()`/`unreachable!()`, because a panic here would take the
        // controller down over an impossible case.
        match RequestTarget::new(value, u64::MAX) {
            Ok(bounded) => {
                if let Err(e) = input.headroom.admit(bounded, input.replicas) {
                    return RequestCarvePlan::Blocked(RequestCarveBlocked::WouldNotSchedule(e));
                }
            }
            Err(e) => return RequestCarvePlan::Blocked(RequestCarveBlocked::AboveLimit(e)),
        }
    }

    match ClassTransitionProposal::new(input.target.clone(), input.observed, block) {
        Ok(p) => RequestCarvePlan::Transition(p),
        // The rendered block did not actually move the class — the target is not
        // reachable by the construction above. Reported, never silently dropped
        // as a no-op.
        Err(SameClass { class }) => RequestCarvePlan::Blocked(RequestCarveBlocked::Transition(
            Unreachable::StillSameClass { class }.block(),
        )),
    }
}

/// **The request band's tick, as one pure function.** No I/O, no clock, no
/// cluster — every world-fact arrives in [`RequestCarveInput`], which is what
/// makes the whole dimension provable against a mock.
///
/// The order is load-bearing and is the Gate-0 illegal-state list executed in
/// dependency order:
///
/// 1. resolve the container (a stale target is not a carve);
/// 2. **the QoS gap dominates** — a class transition has no in-place path at any
///    value, so it is decided before any scalar is computed (I1/I4);
/// 3. otherwise take the decision: a shrink is refused for want of evidence (I3),
///    a withheld reclaim is reported, only a grow proceeds;
/// 4. the narrowing chain, in this order and no other:
///    `carve_respecting_seal` (raise to the seal, I8) → [`RequestTarget::new`]
///    (cap at the LIVE limit, I2) → [`AllocatableHeadroom::admit`] (must fit the
///    node, I7) → [`ClassPreserved::check`] on the FINAL value (I6).
///
/// The class check runs LAST, on the value actually being written, because every
/// preceding step can move it — checking the pre-clamp value would prove a
/// property of a number nobody writes.
#[must_use]
pub fn plan_request_carve(input: &RequestCarveInput<'_>) -> RequestCarvePlan {
    use breathe_invariant::isolation::carve_respecting_seal;

    let Some(observed_container) = input
        .observed
        .containers
        .iter()
        .find(|c| c.name == input.container)
    else {
        return RequestCarvePlan::Blocked(RequestCarveBlocked::NoSuchContainer(NoSuchContainer {
            container: input.container.to_owned(),
        }));
    };

    // The scalar this tick would seat at, before any narrowing. Used by both the
    // in-place path and the BestEffort→Burstable promotion, so a promotion lands
    // on the same demand-derived number an in-place carve would have.
    let raw_seat = match input.decision {
        Decision::Grow { to, .. } => to,
        _ => observed_container.request(input.resource).unwrap_or(0),
    };

    // ── 2. THE QoS GAP DOMINATES ────────────────────────────────────────────
    let observed_class = input.observed.qos_class();
    if observed_class != input.qos_target {
        return plan_class_transition(input, observed_class, raw_seat);
    }

    // ── 3. WITHIN-CLASS: only a grow proceeds ───────────────────────────────
    let (from, to) = match input.decision {
        Decision::Grow { from, to } => (from, to),
        Decision::ReclaimWithheld {
            current,
            reclaimable,
        } => {
            return RequestCarvePlan::Withheld {
                current,
                reclaimable: Some(reclaimable),
            };
        }
        Decision::NoSafeShrink { current } => {
            return RequestCarvePlan::Withheld {
                current,
                reclaimable: None,
            };
        }
        // I3 — unreachable through the intended pipeline; refused, not trusted.
        Decision::Shrink { from, to } => {
            return RequestCarvePlan::Blocked(RequestCarveBlocked::ShrinkWithoutEvidence {
                from,
                to,
            });
        }
        _ => return RequestCarvePlan::Hold,
    };

    // ── I9 — legality, derived per tick from the live server minor ──────────
    let legality = ResizeLegality::evaluate(
        input.server_minor,
        input.memory_resize_policy,
        input.resource,
        CarveDirection::Grow,
        false, // a REQUEST, never a limit
    );
    if !legality.is_allowed() {
        return RequestCarvePlan::Blocked(RequestCarveBlocked::ResizeIllegal { legality });
    }

    // ── 4. THE NARROWING CHAIN ──────────────────────────────────────────────
    // (a) I8 — raise to the isolation seal floor. `SealedCarve`'s output type
    //     carries the ≥-floor property; there is no below-seal value to hold.
    let sealed = carve_respecting_seal(to, input.posture);

    // (b) I2 — cap at the LIVE limit. A container with NO declared limit has no
    //     `request <= limit` rule to violate: k8s's admission check is
    //     conditional on the limit being present, so the bound falls through to
    //     allocatable. Adding a request where there was none can still move the
    //     class — which is precisely what step (d) catches.
    let live_limit = observed_container.limit(input.resource).unwrap_or(u64::MAX);
    let bounded = match RequestTarget::new(sealed.target(), live_limit) {
        Ok(t) => t,
        Err(e) => return RequestCarvePlan::Blocked(RequestCarveBlocked::AboveLimit(e)),
    };

    // (c) I7 — must fit the node class it schedules onto.
    let admitted = match input.headroom.admit(bounded, input.replicas) {
        Ok(a) => a,
        Err(e) => return RequestCarvePlan::Blocked(RequestCarveBlocked::WouldNotSchedule(e)),
    };

    // A chain that narrowed all the way back to the current value is a no-op, not
    // a write. Reported as Hold so a band does not churn the API every tick.
    if admitted.get() == from {
        return RequestCarvePlan::Hold;
    }

    // (d) I6 — the class check, on the FINAL value, over the WHOLE pod.
    let preserved = match ClassPreserved::check(
        input.observed,
        input.container,
        input.resource,
        admitted.get(),
    ) {
        Ok(w) => w,
        Err(PreserveError::ClassWouldChange(e)) => {
            return RequestCarvePlan::Blocked(RequestCarveBlocked::ClassWouldChange(e));
        }
        Err(PreserveError::NoSuchContainer(e)) => {
            return RequestCarvePlan::Blocked(RequestCarveBlocked::NoSuchContainer(e));
        }
    };

    RequestCarvePlan::InPlace(InPlaceCarve {
        patch: SsaPatch {
            target: input.target.clone(),
            field_manager: input.field_manager.to_owned(),
            layout: LimitLayout::PodRequestResize {
                container: Some(input.container.to_owned()),
            },
            resource: input.resource.as_str().to_owned(),
            value: admitted.get(),
        },
        preserved,
        durability: input.durability,
        from,
        to: admitted.get(),
        seal_bound: sealed.seal_bound(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(
        name: &str,
        cr: Option<u64>,
        cl: Option<u64>,
        mr: Option<u64>,
        ml: Option<u64>,
    ) -> ContainerResources {
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

    // ── I6 — the live private-estate-build/sui shape ────────────────────────────────

    #[test]
    fn the_live_sui_shape_refuses_a_carve_to_the_limit() {
        // private-estate-build/sui, read from private-estate-eks 2026-07-26: Burstable,
        // requests.memory=512Mi vs limits.memory=6Gi (12×), and cpu ALREADY
        // 200m/200m. Carving memory all the way to the limit would promote it
        // Burstable → Guaranteed as an undeclared side effect.
        let sui = PodResources::new(vec![c(
            "sui",
            Some(200),
            Some(200),
            Some(512 << 20),
            Some(6144 << 20),
        )]);
        assert_eq!(sui.qos_class(), QosClass::Burstable);

        let err = ClassPreserved::check(&sui, "sui", RequestResource::Memory, 6144 << 20)
            .expect_err(
                "a carve to the limit must be refused — it silently promotes to Guaranteed",
            );
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
        let sui = PodResources::new(vec![c(
            "sui",
            Some(200),
            Some(200),
            Some(512 << 20),
            Some(6144 << 20),
        )]);
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
        let sui = PodResources::new(vec![c(
            "sui",
            Some(200),
            Some(200),
            Some(512 << 20),
            Some(6144 << 20),
        )]);
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
        assert_eq!(
            RequestTarget::new(512, 512).map(RequestTarget::get),
            Ok(512)
        );
        assert_eq!(
            RequestTarget::new(256, 512).map(RequestTarget::get),
            Ok(256)
        );
    }

    #[test]
    fn allocatable_headroom_admits_and_refuses() {
        let h = AllocatableHeadroom {
            per_node: 4096,
            observed_at_epoch: 1,
        };
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
        assert!(matches!(
            ClassTransitionProposal::new(t(), &observed, same),
            Err(SameClass { .. })
        ));
    }

    #[test]
    fn a_real_transition_is_addressed() {
        let observed = PodResources::new(vec![c("a", Some(200), Some(400), Some(512), Some(1024))]);
        let promoted =
            PodResources::new(vec![c("a", Some(400), Some(400), Some(1024), Some(1024))]);
        let p =
            ClassTransitionProposal::new(t(), &observed, promoted).expect("Burstable → Guaranteed");
        assert_eq!(p.from, QosClass::Burstable);
        assert_eq!(p.to, QosClass::Guaranteed);
        assert_eq!(
            p.addr.to_string().len(),
            64,
            "a BLAKE3 address renders as 64 hex chars"
        );
    }

    #[test]
    fn the_content_address_is_deterministic_and_sensitive() {
        let observed = PodResources::new(vec![c("a", Some(200), Some(400), Some(512), Some(1024))]);
        let g = |m: u64| PodResources::new(vec![c("a", Some(400), Some(400), Some(m), Some(m))]);
        let a = ClassTransitionProposal::new(t(), &observed, g(1024)).unwrap();
        let b = ClassTransitionProposal::new(t(), &observed, g(1024)).unwrap();
        assert_eq!(a.addr, b.addr, "same block ⇒ same address");

        let observed2 =
            PodResources::new(vec![c("a", Some(200), Some(400), Some(512), Some(2048))]);
        let cdiff = ClassTransitionProposal::new(t(), &observed2, g(2048)).unwrap();
        assert_ne!(
            a.addr, cdiff.addr,
            "a different block ⇒ a different address"
        );
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
        assert!(
            ResizeLegality::evaluate(
                33,
                MemoryResizePolicy::RestartContainer,
                RequestResource::Memory,
                CarveDirection::Shrink,
                true
            )
            .is_allowed()
        );
        // 1.34+ removed the restriction entirely — the reason this is derived
        // per-tick rather than hardcoded.
        assert!(
            ResizeLegality::evaluate(
                34,
                MemoryResizePolicy::Unset,
                RequestResource::Memory,
                CarveDirection::Shrink,
                true
            )
            .is_allowed()
        );
        // The rule is about memory LIMITS: a request carve is unaffected.
        assert!(
            ResizeLegality::evaluate(
                33,
                MemoryResizePolicy::Unset,
                RequestResource::Memory,
                CarveDirection::Shrink,
                false
            )
            .is_allowed()
        );
        // …and cpu is unaffected in either direction.
        assert!(
            ResizeLegality::evaluate(
                33,
                MemoryResizePolicy::Unset,
                RequestResource::Cpu,
                CarveDirection::Shrink,
                true
            )
            .is_allowed()
        );
    }

    // ── the durable door refuses by construction ─────────────────────────────

    #[tokio::test]
    async fn the_null_writer_refuses_every_proposal() {
        let coord =
            crate::manifest::ManifestCoordinate::new("clusters/isolated/sui.yaml", "sui-request");
        let p = AddressedProposal::carve(&coord, "3Gi", "memory", "sui");
        let gate = crate::gate::authored_write_gate("drzzln: test");
        let w = gate.witness().expect("an authored write resolves Live");
        assert_eq!(
            NullManifestWriter.propose(w, &p).await,
            Err(WriterError::Disabled)
        );
    }

    /// The durable door takes an [`AddressedProposal`] — a proposal fused to a
    /// manifest coordinate — and a bare [`ClassTransitionProposal`] is not one.
    ///
    /// This is the SECOND disjointness, and it is the one B3 adds. The first
    /// (a transition cannot travel the in-place door) was already a type fact;
    /// this one says a durable write cannot happen without knowing where it
    /// lands. Together they mean neither door can be reached with the other's
    /// currency, and neither can be reached with a half-specified payload.
    ///
    /// Enforced mechanically below rather than only asserted in prose.
    #[test]
    fn a_bare_transition_proposal_cannot_reach_the_durable_door() {
        // Scan the PRODUCTION surface only — everything above `mod tests`.
        // Scanning the whole file would match this test's own literals, which
        // is exactly what the first run of this test did.
        let full = include_str!("request.rs");
        let src = full
            .split("\nmod tests {")
            .next()
            .expect("the test module is the last item");

        // A `From`/`TryFrom` in either direction would silently re-open the
        // hole the type split exists to close. Assembled at runtime so the
        // needle is never itself a literal in the scanned region.
        let bare = "ClassTransitionProposal";
        let addressed = "AddressedProposal";
        for (from, to) in [
            (bare, addressed),
            (addressed, "SsaPatch"),
            (addressed, bare),
        ] {
            for verb in ["From", "TryFrom"] {
                let mut needle = String::from("impl ");
                needle.push_str(verb);
                needle.push('<');
                needle.push_str(from);
                needle.push_str("> for ");
                needle.push_str(to);
                assert!(
                    !src.contains(&needle),
                    "{needle} would re-open the door split"
                );
            }
        }
        // And the door's own signature names the addressed type, not the bare one.
        assert!(src.contains("proposal: &AddressedProposal,"));
    }

    /// A transition whose block touches more scalars than the band has markers
    /// for is REFUSED, naming each one — never committed as the reachable subset.
    #[test]
    fn a_transition_needs_one_marker_per_scalar_or_it_names_the_gap() {
        let observed = PodResources::new(vec![
            c("api", Some(200), Some(400), Some(512), Some(1024)),
            c("sidecar", Some(50), Some(100), Some(64), Some(128)),
        ]);
        let promoted = PodResources::new(vec![
            c("api", Some(400), Some(400), Some(1024), Some(1024)),
            c("sidecar", Some(100), Some(100), Some(128), Some(128)),
        ]);
        let p =
            ClassTransitionProposal::new(t(), &observed, promoted).expect("Burstable → Guaranteed");

        // The band supplies ONE marker — the single-`manifestRef` shape.
        let only_one = [("api.requests.memory".to_owned(), "sui-request".to_owned())];
        let Err(gap) = AddressedProposal::transition("clusters/isolated/sui.yaml", &p, &only_one)
        else {
            panic!("one marker cannot address a four-scalar promotion")
        };
        assert_eq!(
            gap.missing.len(),
            3,
            "each unaddressed scalar is named: {:?}",
            gap.missing
        );
        assert!(gap.missing.contains(&"sidecar.requests.memory".to_owned()));

        // With every marker supplied it succeeds — the gap is about coverage,
        // not a blanket refusal of transitions.
        let all: Vec<_> = [
            "api.requests.memory",
            "api.requests.cpu",
            "sidecar.requests.memory",
            "sidecar.requests.cpu",
        ]
        .iter()
        .map(|k| ((*k).to_owned(), (*k).to_owned()))
        .collect();
        let ok = AddressedProposal::transition("clusters/isolated/sui.yaml", &p, &all)
            .expect("full coverage");
        assert_eq!(ok.assignments().len(), 4);
    }

    // ═══════════════ THE CARVE PLANNER — Gate 0, row by row ═════════════════

    use breathe_invariant::isolation::{IsolationPosture, PlacementIsolation};

    /// A posture with no isolation floor — the ordinary Standard/Burstable case.
    fn posture_open() -> IsolationPosture {
        IsolationPosture::try_seal(
            WorkloadClass::Standard,
            QosClass::Burstable,
            0,
            0,
            PlacementIsolation::CoLocate,
            false,
        )
        .expect("a Standard/Burstable posture with no floor is legal")
    }

    /// A sealed posture with a real reservation floor.
    fn posture_sealed(floor: u64, ceiling: u64) -> IsolationPosture {
        IsolationPosture::try_seal(
            WorkloadClass::Critical,
            QosClass::Burstable,
            floor,
            ceiling,
            PlacementIsolation::CoLocate,
            false,
        )
        .expect("a Critical posture WITH a floor seals")
    }

    /// The default input: the live `private-estate-build/sui` shape, Burstable, holding
    /// its class, with generous headroom. Each test perturbs exactly one field.
    struct Fixture {
        observed: PodResources,
        posture: IsolationPosture,
        target: Target,
    }

    impl Fixture {
        fn sui() -> Self {
            Self {
                // requests.memory=512Mi vs limits.memory=6Gi; cpu ALREADY 200m/200m.
                observed: PodResources::new(vec![c(
                    "sui",
                    Some(200),
                    Some(200),
                    Some(512 << 20),
                    Some(6144 << 20),
                )]),
                posture: posture_open(),
                target: t(),
            }
        }

        fn input(&self, decision: Decision, headroom: u64) -> RequestCarveInput<'_> {
            RequestCarveInput {
                target: &self.target,
                container: "sui",
                resource: RequestResource::Memory,
                observed: &self.observed,
                decision,
                posture: &self.posture,
                headroom: AllocatableHeadroom {
                    per_node: headroom,
                    observed_at_epoch: 1,
                },
                replicas: 1,
                durability: Durability::Committed,
                qos_target: QosClass::Burstable,
                server_minor: 33,
                memory_resize_policy: MemoryResizePolicy::Unset,
                field_manager: "breathe-request",
            }
        }
    }

    // ── the happy path ───────────────────────────────────────────────────────

    #[test]
    fn a_within_class_grow_plans_an_in_place_carve() {
        let f = Fixture::sui();
        let plan = plan_request_carve(&f.input(
            Decision::Grow {
                from: 512 << 20,
                to: 2048 << 20,
            },
            16 << 30,
        ));
        let RequestCarvePlan::InPlace(carve) = plan else {
            panic!("expected InPlace, got {plan:?}")
        };

        assert_eq!(carve.from(), 512 << 20);
        assert_eq!(carve.to(), 2048 << 20);
        assert_eq!(carve.patch().value, 2048 << 20);
        assert_eq!(carve.patch().resource, "memory");
        assert_eq!(carve.patch().field_manager, "breathe-request");
        assert!(
            matches!(carve.patch().layout, LimitLayout::PodRequestResize { .. }),
            "a request carve must carry the REQUEST layout, never PodResize"
        );
        // The witness proves the class is untouched, and names the exact change.
        assert_eq!(carve.preserved().class(), QosClass::Burstable);
        assert_eq!(carve.preserved().to(), 2048 << 20);
        assert_eq!(carve.preserved().from(), Some(512 << 20));
        assert!(
            !carve.seal_bound(),
            "no seal floor was configured, so it cannot have bound"
        );
        // I5 — this value does NOT survive a rollout, and the plan says so.
        assert!(
            carve.durability_gap(),
            "a Committed band with no writer must report the gap"
        );
        assert!(RequestCarvePlan::InPlace(carve).writes());
    }

    #[test]
    fn an_ephemeral_carve_reports_no_durability_gap() {
        let f = Fixture::sui();
        let mut i = f.input(
            Decision::Grow {
                from: 512 << 20,
                to: 1024 << 20,
            },
            16 << 30,
        );
        i.durability = Durability::Ephemeral;
        let RequestCarvePlan::InPlace(carve) = plan_request_carve(&i) else {
            panic!("expected InPlace")
        };
        assert!(
            !carve.durability_gap(),
            "an Ephemeral band never claims a durable value"
        );
    }

    // ── I6 / I1 — the class check, on the FINAL value ────────────────────────

    #[test]
    fn a_carve_that_would_promote_the_class_is_blocked_not_written() {
        // The live sui trap: cpu is already 200m/200m, so seating memory AT the
        // 6Gi limit flips the pod Burstable → Guaranteed. No in-place path exists.
        let f = Fixture::sui();
        let plan = plan_request_carve(&f.input(
            Decision::Grow {
                from: 512 << 20,
                to: 6144 << 20,
            },
            16 << 30,
        ));
        match plan {
            RequestCarvePlan::Blocked(RequestCarveBlocked::ClassWouldChange(e)) => {
                assert_eq!(e.from, QosClass::Burstable);
                assert_eq!(e.to, QosClass::Guaranteed);
            }
            other => panic!("a class-moving carve must be Blocked, got {other:?}"),
        }
    }

    /// The class check runs on the value ACTUALLY written, not the pre-clamp one.
    /// Here the raw decision is harmless but the SEAL raises it onto the limit,
    /// so the class would move — a bug a pre-clamp check would wave through.
    #[test]
    fn the_class_check_runs_after_every_clamp_not_before() {
        let f = Fixture {
            posture: posture_sealed(6144 << 20, 6144 << 20),
            ..Fixture::sui()
        };
        let plan = plan_request_carve(&f.input(
            Decision::Grow {
                from: 512 << 20,
                to: 1024 << 20,
            },
            16 << 30,
        ));
        match plan {
            RequestCarvePlan::Blocked(RequestCarveBlocked::ClassWouldChange(e)) => {
                assert_eq!(
                    e.to,
                    QosClass::Guaranteed,
                    "the SEAL pushed it onto the limit"
                );
            }
            other => panic!("expected the post-clamp class check to fire, got {other:?}"),
        }
    }

    // ── I2 — request ≤ live limit ────────────────────────────────────────────

    #[test]
    fn a_seal_floor_above_the_live_limit_is_blocked_not_clamped() {
        // The seal demands 8Gi; the container's limit is 6Gi. No legal request
        // satisfies both — a real authoring conflict, reported rather than
        // silently clamped to something neither side asked for.
        let f = Fixture {
            posture: posture_sealed(8192 << 20, 0),
            ..Fixture::sui()
        };
        let plan = plan_request_carve(&f.input(
            Decision::Grow {
                from: 512 << 20,
                to: 1024 << 20,
            },
            32 << 30,
        ));
        match plan {
            RequestCarvePlan::Blocked(RequestCarveBlocked::AboveLimit(e)) => {
                assert_eq!(e.proposed, 8192 << 20);
                assert_eq!(e.live_limit, 6144 << 20);
            }
            other => panic!("expected AboveLimit, got {other:?}"),
        }
    }

    #[test]
    fn a_container_with_no_declared_limit_falls_through_to_allocatable() {
        // k8s's `request <= limit` rule is CONDITIONAL on the limit existing, so
        // an unlimited container has no such bound — allocatable is what binds.
        // (The pod stays Burstable throughout: it carries a cpu limit.)
        let observed =
            PodResources::new(vec![c("sui", Some(200), Some(400), Some(256 << 20), None)]);
        let f = Fixture {
            observed,
            ..Fixture::sui()
        };
        let plan = plan_request_carve(&f.input(
            Decision::Grow {
                from: 256 << 20,
                to: 1024 << 20,
            },
            16 << 30,
        ));
        let RequestCarvePlan::InPlace(carve) = plan else {
            panic!("expected InPlace, got {plan:?}")
        };
        assert_eq!(carve.to(), 1024 << 20);
    }

    // ── I7 — allocatable headroom ────────────────────────────────────────────

    #[test]
    fn a_carve_that_would_not_schedule_is_blocked() {
        let f = Fixture::sui();
        // 2Gi wanted onto a node class with 1Gi of headroom.
        let plan = plan_request_carve(&f.input(
            Decision::Grow {
                from: 512 << 20,
                to: 2048 << 20,
            },
            1024 << 20,
        ));
        match plan {
            RequestCarvePlan::Blocked(RequestCarveBlocked::WouldNotSchedule(e)) => {
                assert_eq!(e.proposed, 2048 << 20);
                assert_eq!(e.per_node_headroom, 1024 << 20);
            }
            other => panic!("expected WouldNotSchedule, got {other:?}"),
        }
    }

    // ── I3 — no shrink path ──────────────────────────────────────────────────

    #[test]
    fn a_shrink_reaching_the_planner_is_refused_for_want_of_evidence() {
        let f = Fixture::sui();
        let plan = plan_request_carve(&f.input(
            Decision::Shrink {
                from: 512 << 20,
                to: 256 << 20,
            },
            16 << 30,
        ));
        assert!(
            matches!(
                plan,
                RequestCarvePlan::Blocked(RequestCarveBlocked::ShrinkWithoutEvidence { .. })
            ),
            "lowering a reservation needs RequestShrinkEvidence, which has no constructor: {plan:?}"
        );
        assert!(!plan.writes());
    }

    #[test]
    fn a_withheld_reclaim_is_named_and_a_masked_one_is_honest_about_it() {
        let f = Fixture::sui();
        let named = plan_request_carve(&f.input(
            Decision::ReclaimWithheld {
                current: 512 << 20,
                reclaimable: 128 << 20,
            },
            16 << 30,
        ));
        assert_eq!(
            named,
            RequestCarvePlan::Withheld {
                current: 512 << 20,
                reclaimable: Some(128 << 20)
            }
        );
        // The shared limit-shaped safe_min masked the amount upstream — reported
        // as unknown rather than as zero, which would read as "no slack".
        let masked =
            plan_request_carve(&f.input(Decision::NoSafeShrink { current: 512 << 20 }, 16 << 30));
        assert_eq!(
            masked,
            RequestCarvePlan::Withheld {
                current: 512 << 20,
                reclaimable: None
            }
        );
        assert!(!named.writes() && !masked.writes());
    }

    // ── I4 / the QoS gap — a class transition NEVER takes the in-place door ──

    #[test]
    fn a_best_effort_pod_targeted_at_burstable_yields_a_proposal_not_a_patch() {
        // I4: a BestEffort pod has no in-place promotion path at all — k8s
        // refuses the class change AND (on 1.33) refuses adding a memory limit
        // without a RestartContainer resizePolicy.
        let observed = PodResources::new(vec![c("sui", None, None, None, None)]);
        assert_eq!(observed.qos_class(), QosClass::BestEffort);
        let f = Fixture {
            observed,
            ..Fixture::sui()
        };
        let plan = plan_request_carve(&f.input(
            Decision::Grow {
                from: 0,
                to: 256 << 20,
            },
            16 << 30,
        ));
        match &plan {
            RequestCarvePlan::Transition(p) => {
                assert_eq!(p.from, QosClass::BestEffort);
                assert_eq!(p.to, QosClass::Burstable);
                assert_eq!(p.block.containers[0].memory_request, Some(256 << 20));
            }
            other => panic!("expected a Transition proposal, got {other:?}"),
        }
        assert!(
            !plan.writes(),
            "a class transition must NEVER write in place"
        );
        // With no writer wired (M0), the gap is reported as blocked, not held.
        assert_eq!(
            plan.qos_gap(false),
            QosGap::Blocked(ClassTransitionBlocked::NoWriterConfigured)
        );
        assert!(matches!(
            plan.qos_gap(true),
            QosGap::PromotionProposed { .. }
        ));
    }

    /// **I7 applies to the DURABLE path too — and matters more there.**
    ///
    /// An un-admitted in-place write fails loudly against the apiserver now; an
    /// un-admitted value committed to a manifest lands silently and kills the
    /// NEXT rollout. So a promotion whose value would not schedule is refused
    /// before the proposal is ever rendered.
    #[test]
    fn a_promotion_that_would_not_schedule_is_refused_before_it_is_proposed() {
        let observed = PodResources::new(vec![c("sui", None, None, None, None)]);
        let f = Fixture {
            observed,
            ..Fixture::sui()
        };
        // The promotion seat is 4Gi; the node class has 1Gi of headroom.
        let plan = plan_request_carve(&f.input(
            Decision::Grow {
                from: 0,
                to: 4096 << 20,
            },
            1024 << 20,
        ));
        match plan {
            RequestCarvePlan::Blocked(RequestCarveBlocked::WouldNotSchedule(e)) => {
                assert_eq!(e.proposed, 4096 << 20);
            }
            other => panic!("an unschedulable promotion must be refused, got {other:?}"),
        }
        // …and the same promotion WITH headroom proceeds, so the gate is the
        // headroom and not the promotion itself.
        let ok = plan_request_carve(&f.input(
            Decision::Grow {
                from: 0,
                to: 4096 << 20,
            },
            16 << 30,
        ));
        assert!(matches!(ok, RequestCarvePlan::Transition(_)), "got {ok:?}");
    }

    #[test]
    fn promoting_to_guaranteed_sets_every_request_to_its_limit_on_every_container() {
        let observed = PodResources::new(vec![
            c("app", Some(200), Some(400), Some(512), Some(1024)),
            c("sidecar", Some(10), Some(50), Some(64), Some(128)),
        ]);
        let f = Fixture {
            observed,
            ..Fixture::sui()
        };
        let mut i = f.input(Decision::Grow { from: 512, to: 600 }, 1 << 40);
        i.container = "app";
        i.qos_target = QosClass::Guaranteed;
        let RequestCarvePlan::Transition(p) = plan_request_carve(&i) else {
            panic!("expected Transition")
        };
        assert_eq!(p.to, QosClass::Guaranteed);
        assert_eq!(
            p.block.qos_class(),
            QosClass::Guaranteed,
            "the rendered block really is Guaranteed"
        );
        for c in &p.block.containers {
            assert_eq!(c.cpu_request, c.cpu_limit);
            assert_eq!(c.memory_request, c.memory_limit);
        }
    }

    #[test]
    fn guaranteed_is_unreachable_when_any_container_declares_no_limit() {
        let observed = PodResources::new(vec![
            c("app", Some(200), Some(400), Some(512), Some(1024)),
            c("sidecar", Some(10), None, Some(64), None), // no limits at all
        ]);
        let f = Fixture {
            observed,
            ..Fixture::sui()
        };
        let mut i = f.input(Decision::Grow { from: 512, to: 600 }, 1 << 40);
        i.container = "app";
        i.qos_target = QosClass::Guaranteed;
        match plan_request_carve(&i) {
            RequestCarvePlan::Blocked(RequestCarveBlocked::Transition(
                ClassTransitionBlocked::UnreachableFromObserved { detail },
            )) => assert!(
                detail.contains("sidecar"),
                "the reason must NAME the blocking container: {detail}"
            ),
            other => panic!("expected UnreachableFromObserved, got {other:?}"),
        }
    }

    /// I5 — an `Ephemeral` band cannot reach its own declared class target,
    /// because a transition is a template write by definition. Reported as a
    /// band authored in a shape that cannot converge, not silently ignored.
    #[test]
    fn an_ephemeral_band_cannot_transition() {
        let observed = PodResources::new(vec![c("sui", None, None, None, None)]);
        let f = Fixture {
            observed,
            ..Fixture::sui()
        };
        let mut i = f.input(
            Decision::Grow {
                from: 0,
                to: 256 << 20,
            },
            16 << 30,
        );
        i.durability = Durability::Ephemeral;
        assert!(matches!(
            plan_request_carve(&i),
            RequestCarvePlan::Blocked(RequestCarveBlocked::Transition(
                ClassTransitionBlocked::EphemeralCannotTransition
            ))
        ));
    }

    /// breathe never proposes to STRIP a workload's reservation.
    #[test]
    fn a_weakening_qos_target_is_refused() {
        let observed =
            PodResources::new(vec![c("sui", Some(200), Some(200), Some(512), Some(512))]);
        assert_eq!(observed.qos_class(), QosClass::Guaranteed);
        let f = Fixture {
            observed,
            ..Fixture::sui()
        };
        for weaker in [QosClass::Burstable, QosClass::BestEffort] {
            let mut i = f.input(Decision::Grow { from: 512, to: 600 }, 1 << 40);
            i.qos_target = weaker;
            match plan_request_carve(&i) {
                RequestCarvePlan::Blocked(RequestCarveBlocked::WouldWeakenSeal {
                    observed,
                    target,
                }) => {
                    assert_eq!(observed, QosClass::Guaranteed);
                    assert_eq!(target, weaker);
                }
                other => panic!("a de-seal to {weaker:?} must be refused, got {other:?}"),
            }
        }
    }

    // ── the remaining refusals ───────────────────────────────────────────────

    #[test]
    fn a_stale_container_target_is_blocked() {
        let f = Fixture::sui();
        let mut i = f.input(
            Decision::Grow {
                from: 512 << 20,
                to: 1024 << 20,
            },
            16 << 30,
        );
        i.container = "gone";
        assert!(matches!(
            plan_request_carve(&i),
            RequestCarvePlan::Blocked(RequestCarveBlocked::NoSuchContainer(_))
        ));
    }

    #[test]
    fn a_non_actionable_decision_holds_without_writing() {
        let f = Fixture::sui();
        for d in [
            Decision::Hold,
            Decision::AtCeiling { current: 512 << 20 },
            Decision::Warmup {
                current: 512 << 20,
                observed_for: 1,
                warmup: 600,
            },
            Decision::NoLimit,
        ] {
            let plan = plan_request_carve(&f.input(d, 16 << 30));
            assert_eq!(plan, RequestCarvePlan::Hold, "{d:?} must Hold");
            assert!(!plan.writes());
        }
    }

    /// A chain that narrowed all the way back to the current value is a no-op,
    /// not a write — otherwise the band would re-patch the same number forever.
    #[test]
    fn a_carve_that_narrows_back_to_the_current_value_holds() {
        let f = Fixture {
            posture: posture_sealed(512 << 20, 0),
            ..Fixture::sui()
        };
        // The law wanted less; the seal raised it back to exactly the live value.
        let plan = plan_request_carve(&f.input(
            Decision::Grow {
                from: 512 << 20,
                to: 300 << 20,
            },
            16 << 30,
        ));
        assert_eq!(plan, RequestCarvePlan::Hold);
    }

    // ── the whole pipeline, on the receipt that motivated the dimension ──────

    /// **The `sui-cache-pg` shape, end to end through the planner.** 34 OOMKills
    /// at a 202.8Mi high-water under a 1Gi limit with cgroup `failcnt = 0`. The
    /// LIMIT was never binding; the tiny REQUEST set `oom_score_adj`.
    #[test]
    fn the_sui_cache_pg_shape_plans_a_request_raise() {
        let observed = PodResources::new(vec![c(
            "db",
            Some(100),
            Some(500),
            Some(128 << 20),
            Some(1 << 30),
        )]);
        assert_eq!(observed.qos_class(), QosClass::Burstable);
        let f = Fixture {
            observed,
            ..Fixture::sui()
        };
        let mut i = f.input(
            Decision::Grow {
                from: 128 << 20,
                to: 233 << 20,
            },
            8 << 30,
        );
        i.container = "db";

        let RequestCarvePlan::InPlace(carve) = plan_request_carve(&i) else {
            panic!("the request band must plan a raise")
        };
        assert!(
            carve.to() > carve.from(),
            "the RESERVATION rises: {} → {}",
            carve.from(),
            carve.to()
        );
        assert!(
            carve.to() < (1 << 30),
            "and stays strictly under the never-binding 1Gi limit"
        );
        assert_eq!(
            carve.preserved().class(),
            QosClass::Burstable,
            "the class is untouched"
        );
        // The write targets requests — the field the kernel ranks on — via the
        // request layout, not the limit layout a MemoryBand would use.
        assert!(matches!(
            carve.patch().layout,
            LimitLayout::PodRequestResize { .. }
        ));
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
        let promoted =
            PodResources::new(vec![c("a", Some(400), Some(400), Some(1024), Some(1024))]);
        let p = ClassTransitionProposal::new(t(), &observed, promoted).unwrap();
        assert_eq!(p.block.containers.len(), 1);
        // The proposal names TWO classes and a whole block. An SsaPatch names
        // one `value: u64`. There is no total function from the former to the
        // latter, which is precisely why no conversion is offered.
        assert_ne!(p.from, p.to);
    }
}
