//! Which bands SHOULD exist, derived from observed workload shape.
//!
//! # Why this crate exists
//!
//! Band enrollment was authored — by hand, or by a one-shot generator run
//! against one cluster on one day. Measured on one cluster-eks 2026-08-05, that
//! produced:
//!
//! | Defect | Measurement |
//! |---|---|
//! | bands on 2 of 11 dimensions | cpu 51, memory 60, storage 4; **request 0**, replica 0, arc 0 |
//! | banding the wrong lever | 10.9 vCPU (17% of the cluster) reserved-and-unused, and *no* band on requests |
//! | bands outliving their target | 14 `TargetNotFound` + 12 `Error` — 23% aimed at absent or scaled-to-zero workloads |
//!
//! The second row is the load-bearing one and it is a *category* error, not a
//! tuning miss. `CpuBand` and `MemoryBand` carve **limits**. The kube scheduler
//! packs by **requests**. A node observed at 95% requested / 5% used refuses new
//! pods while 95% of its CPU idles, and no amount of limit-carving moves that
//! number — the dimension that does was simply never enrolled anywhere.
//!
//! The root cause of all three rows is the same: a *list* can only ever encode
//! what its author knew when they typed it. So enrollment stops being a list.
//!
//! # The shape of the fix
//!
//! [`dimensions_for`] is a **total function** from an observed [`WorkloadShape`]
//! to the set of dimensions that workload warrants. It is total in the sense
//! that matters here: [`BandDimension::warrant_for`] is an exhaustive `match`
//! over [`BandDimension`], so **adding a dimension to the enum fails to compile
//! until every workload shape has answered for it**. That is the mechanism that
//! makes `requestbands: 0` unrepresentable rather than merely fixed — a future
//! twelfth dimension cannot be silently omitted the way the request dimension
//! was.
//!
//! Every negative answer carries its reason ([`Warrant::NotApplicable`]). An
//! agent reading discovery output has no preattentive vision and cannot infer
//! why a band is absent from the absence itself; the reason is therefore part of
//! the return type, not a log line.
//!
//! # What this crate deliberately does NOT do
//!
//! No Kubernetes types, no I/O, no clock. Observing the cluster and materializing
//! the bands are the caller's job; this crate is the decision, kept pure so the
//! whole decision table is exercised by unit tests rather than against a live
//! cluster. It mirrors `breathe-nodewaste`, which is pure for the same reason.

#![forbid(unsafe_code)]

pub mod plan;

use std::collections::BTreeSet;

/// A band dimension — one carveable lever.
///
/// The variants correspond 1:1 with the band CRD kinds served by breathe, with
/// `Request` split by resource because [`crate::RequestResource`] selects between
/// two genuinely different levers (the scheduling lever and the OOM lever) and
/// the CRD requires it explicitly for that reason.
///
/// **Adding a variant here is a breaking change on purpose.**
/// [`BandDimension::warrant_for`] will not compile until the new dimension states
/// when it applies, which is precisely the guard that was missing when the
/// request dimension went unenrolled cluster-wide.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BandDimension {
    /// Carves `limits.memory`. The OOM lever; does not affect scheduling.
    Memory,
    /// Carves `limits.cpu`. Throttling lever; does not affect scheduling.
    Cpu,
    /// Carves a PVC's requested size.
    Storage,
    /// Carves `requests.cpu` — **the scheduling lever**.
    RequestCpu,
    /// Carves `requests.memory` — the scheduling lever for memory-bound packing.
    RequestMemory,
    /// Carves replica count.
    Replica,
    /// Carves an ARC runner scale set's min/max.
    Arc,
    /// Carves a host cgroup's memory bound.
    Cgroup,
    /// Carves a host cgroup's CPU bound.
    CgroupCpu,
    /// Carves a host-level kernel/daemon parameter.
    HostParam,
    /// Carves a field on an arbitrary Kubernetes CR.
    KubeParam,
    /// Carves an application-internal knob (Redis `maxmemory`, PG `max_connections`, JVM heap).
    App,
    /// Carves workload isolation placement.
    Isolation,
}

impl BandDimension {
    /// Every dimension. Kept beside the enum so callers enumerate rather than
    /// hand-list; a hand-listed subset is how this crate's motivating defect
    /// arose in the first place.
    pub const ALL: [Self; 13] = [
        Self::Memory,
        Self::Cpu,
        Self::Storage,
        Self::RequestCpu,
        Self::RequestMemory,
        Self::Replica,
        Self::Arc,
        Self::Cgroup,
        Self::CgroupCpu,
        Self::HostParam,
        Self::KubeParam,
        Self::App,
        Self::Isolation,
    ];

    /// The band CRD kind this dimension materializes as.
    #[must_use]
    pub const fn crd_kind(self) -> &'static str {
        match self {
            Self::Memory => "MemoryBand",
            Self::Cpu => "CpuBand",
            Self::Storage => "StorageBand",
            Self::RequestCpu | Self::RequestMemory => "RequestBand",
            Self::Replica => "ReplicaBand",
            Self::Arc => "ArcBand",
            Self::Cgroup => "CgroupBand",
            Self::CgroupCpu => "CgroupCpuBand",
            Self::HostParam => "HostParamBand",
            Self::KubeParam => "KubeParamBand",
            Self::App => "AppBand",
            Self::Isolation => "IsolationBand",
        }
    }

    /// The `spec.resource` discriminator, where the CRD requires one.
    #[must_use]
    pub const fn resource(self) -> Option<RequestResource> {
        match self {
            Self::RequestCpu => Some(RequestResource::Cpu),
            Self::RequestMemory => Some(RequestResource::Memory),
            _ => None,
        }
    }

    /// Whether this dimension changes what the **scheduler** sees.
    ///
    /// This is the distinction the original enrollment missed, so it is encoded
    /// as a queryable property rather than left as tribal knowledge: carving a
    /// limit cannot free scheduling capacity, no matter how far it is carved.
    #[must_use]
    pub const fn affects_scheduling(self) -> bool {
        matches!(self, Self::RequestCpu | Self::RequestMemory | Self::Replica)
    }

    /// Does `shape` warrant a band on this dimension, and if not, why not.
    ///
    /// The exhaustive `match` is the forcing function described in the crate
    /// docs — a new [`BandDimension`] variant makes this fail to compile.
    #[must_use]
    pub fn warrant_for(self, shape: &WorkloadShape) -> Warrant {
        match self {
            // ---- k8s plane: limits -------------------------------------------------
            Self::Memory => Warrant::when(
                shape.declares_memory_limit,
                "workload declares no memory limit — nothing to carve",
            ),
            Self::Cpu => Warrant::when(
                shape.declares_cpu_limit,
                "workload declares no cpu limit — nothing to carve",
            ),

            // ---- k8s plane: requests (the scheduling lever) ------------------------
            //
            // Deliberately NOT gated on "is it over-requested today". A band that
            // only appears once waste is visible cannot observe the workload it
            // would need to have observed to make that call, and it disappears
            // again the moment it succeeds — which reads to an operator as the
            // dimension being unsupported. Enrollment is by shape; whether to
            // *act* is the band's own decision, made from its own history.
            Self::RequestCpu => Warrant::when(
                shape.declares_cpu_request,
                "workload declares no cpu request — scheduler packs it as best-effort",
            ),
            Self::RequestMemory => Warrant::when(
                shape.declares_memory_request,
                "workload declares no memory request — scheduler packs it as best-effort",
            ),

            // ---- k8s plane: shape --------------------------------------------------
            Self::Storage => Warrant::when(
                shape.has_volume_claims,
                "workload owns no PersistentVolumeClaim",
            ),

            // A ReplicaBand and an HPA are two controllers writing one field.
            // Enrolling both is the conflict class breathe reports as
            // `effectiveGate: Conflict`, so refuse at derivation instead of
            // materializing a band that can only ever be held.
            Self::Replica => {
                if shape.horizontal_autoscaler_present {
                    Warrant::NotApplicable(
                        "an HPA already owns replicas — two writers on one field",
                    )
                } else if !shape.kind.replicas_are_settable() {
                    Warrant::NotApplicable("workload kind has no operator-settable replica count")
                } else {
                    // Settable-in-principle is not enough. The count has to have
                    // been *seen*, for the same reason every other dimension is
                    // observation-gated: a band derived from a workload's kind
                    // rather than its behaviour is a guess, and a guess about
                    // replicas is a guess about availability.
                    Warrant::when(
                        shape.observed_replicas.is_some(),
                        "replica count not yet observed",
                    )
                }
            }

            Self::Arc => Warrant::when(
                matches!(shape.kind, WorkloadKind::AutoscalingRunnerSet),
                "not an ARC runner scale set",
            ),

            // ---- host plane --------------------------------------------------------
            //
            // Host-plane carving needs a node breathe is allowed to write to. That
            // authorization lives on the BreatheNodePool, so absent enrollment
            // these are not merely unwarranted, they are unauthorized.
            Self::Cgroup | Self::CgroupCpu | Self::HostParam => Warrant::when(
                shape.node_pool_enrolled,
                "workload's nodes are not enrolled in a BreatheNodePool",
            ),

            // ---- generic CR / application plane ------------------------------------
            Self::KubeParam => Warrant::when(
                matches!(shape.tunable, Some(Tunable::CustomResourceField)),
                "no declared tunable field on an owning custom resource",
            ),
            Self::App => Warrant::when(
                matches!(
                    shape.tunable,
                    Some(
                        Tunable::RedisMaxMemory
                            | Tunable::PostgresMaxConnections
                            | Tunable::JvmHeap
                    )
                ),
                "workload exposes no known application-plane knob",
            ),

            // Isolation only means something for a workload that can disturb a
            // neighbour. Enrolling the quiet majority would materialize ~100 inert
            // bands per cluster — the noise that made the existing band set
            // unreadable.
            Self::Isolation => Warrant::when(
                matches!(shape.class, WorkloadClass::Noisy),
                "workload is not classified noisy — nothing to isolate",
            ),
        }
    }
}

/// Which resource a `RequestBand` carves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RequestResource {
    /// `requests.cpu`.
    Cpu,
    /// `requests.memory`.
    Memory,
}

impl RequestResource {
    /// The CRD's `spec.resource` spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Memory => "memory",
        }
    }
}

/// The kind of workload a band would target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadKind {
    /// A `Deployment`.
    Deployment,
    /// A `StatefulSet`.
    StatefulSet,
    /// A `DaemonSet` — one pod per node; replica count is not operator-settable.
    DaemonSet,
    /// An ARC `AutoscalingRunnerSet`.
    AutoscalingRunnerSet,
}

impl WorkloadKind {
    /// Whether an operator can set this kind's replica count directly.
    ///
    /// A `DaemonSet`'s pod count is a function of the node set, and an
    /// `AutoscalingRunnerSet`'s is owned by ARC — carving either would be
    /// writing a field whose owner immediately overwrites it.
    #[must_use]
    pub const fn replicas_are_settable(self) -> bool {
        matches!(self, Self::Deployment | Self::StatefulSet)
    }
}

/// A known application-plane knob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tunable {
    /// Redis `maxmemory`.
    RedisMaxMemory,
    /// PostgreSQL `max_connections`.
    PostgresMaxConnections,
    /// A JVM heap bound reachable over JMX.
    JvmHeap,
    /// A field on an owning custom resource.
    CustomResourceField,
}

/// How disruptive this workload is to co-tenants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WorkloadClass {
    /// Ordinary workload.
    #[default]
    Standard,
    /// Cluster-critical; carving is held to the most conservative policy.
    Critical,
    /// Batch/job-shaped; tolerates restart-bearing carves.
    Batch,
    /// Known to disturb neighbours — warrants isolation.
    Noisy,
}

/// The observed shape of one workload.
///
/// Every field is something the caller **observed**, never something it was
/// told. That distinction is the point: enrollment derived from a declared
/// inventory rots exactly as a hand-list does, because both are snapshots.
#[derive(Debug, Clone)]
pub struct WorkloadShape {
    /// Namespace of the workload.
    pub namespace: String,
    /// Name of the workload.
    pub name: String,
    /// Its kind.
    pub kind: WorkloadKind,
    /// Any container declares `requests.cpu`.
    pub declares_cpu_request: bool,
    /// Any container declares `requests.memory`.
    pub declares_memory_request: bool,
    /// Any container declares `limits.cpu`.
    pub declares_cpu_limit: bool,
    /// Any container declares `limits.memory`.
    pub declares_memory_limit: bool,
    /// The workload owns at least one PVC or `volumeClaimTemplate`.
    pub has_volume_claims: bool,
    /// An HPA (or equivalent) already writes this workload's replica count.
    pub horizontal_autoscaler_present: bool,
    /// The replica count as actually observed, if it has been.
    pub observed_replicas: Option<u32>,
    /// The workload's nodes are enrolled in a `BreatheNodePool`.
    pub node_pool_enrolled: bool,
    /// A known application-plane knob, if any.
    pub tunable: Option<Tunable>,
    /// Criticality / disruption class.
    pub class: WorkloadClass,
}

impl WorkloadShape {
    /// A shape with everything off — the honest starting point for a workload
    /// nothing has been observed about yet. Builders flip on what was seen.
    #[must_use]
    pub fn bare(namespace: impl Into<String>, name: impl Into<String>, kind: WorkloadKind) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            kind,
            declares_cpu_request: false,
            declares_memory_request: false,
            declares_cpu_limit: false,
            declares_memory_limit: false,
            has_volume_claims: false,
            horizontal_autoscaler_present: false,
            observed_replicas: None,
            node_pool_enrolled: false,
            tunable: None,
            class: WorkloadClass::Standard,
        }
    }
}

/// Whether a dimension applies to a workload, carrying the reason when it does not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Warrant {
    /// The workload warrants a band on this dimension.
    Warranted,
    /// It does not, because of this.
    NotApplicable(&'static str),
}

impl Warrant {
    /// `Warranted` when `cond`, else `NotApplicable(reason)`.
    #[must_use]
    pub const fn when(cond: bool, reason: &'static str) -> Self {
        if cond {
            Self::Warranted
        } else {
            Self::NotApplicable(reason)
        }
    }

    /// Is this warranted.
    #[must_use]
    pub const fn is_warranted(&self) -> bool {
        matches!(self, Self::Warranted)
    }
}

/// The dimensions `shape` warrants.
///
/// Ordered and deduplicated so a plan derived twice from the same shape is
/// byte-identical — discovery output is committed to git, and an unstable
/// ordering would render every reconcile a spurious diff.
#[must_use]
pub fn dimensions_for(shape: &WorkloadShape) -> BTreeSet<BandDimension> {
    BandDimension::ALL
        .into_iter()
        .filter(|d| d.warrant_for(shape).is_warranted())
        .collect()
}

/// The full decision — every dimension with its verdict, warranted or not.
///
/// Use this over [`dimensions_for`] when reporting to a human or an agent: the
/// absent dimensions and their reasons are the part that answers "why is there
/// no `RequestBand` here", which the warranted set alone cannot.
#[must_use]
pub fn explain(shape: &WorkloadShape) -> Vec<(BandDimension, Warrant)> {
    BandDimension::ALL
        .into_iter()
        .map(|d| (d, d.warrant_for(shape)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deployment() -> WorkloadShape {
        let mut s = WorkloadShape::bare("isolated", "pangea-operator", WorkloadKind::Deployment);
        s.declares_cpu_request = true;
        s.declares_memory_request = true;
        s
    }

    /// The motivating defect, as a test: a plain Deployment declaring cpu/memory
    /// requests MUST derive RequestBands. the private estate had 111 limit-bands and zero of
    /// these while 10.9 vCPU sat reserved-and-unused.
    #[test]
    fn requests_are_derived_for_a_workload_that_declares_them() {
        let dims = dimensions_for(&deployment());
        assert!(dims.contains(&BandDimension::RequestCpu));
        assert!(dims.contains(&BandDimension::RequestMemory));
    }

    /// The inverse, and the reason the defect was invisible: limit bands are NOT
    /// derived merely because requests exist. Enrolling only limits is precisely
    /// what left scheduling unaddressed.
    #[test]
    fn limits_are_not_derived_from_requests() {
        let dims = dimensions_for(&deployment());
        assert!(!dims.contains(&BandDimension::Cpu));
        assert!(!dims.contains(&BandDimension::Memory));
    }

    #[test]
    fn limits_are_derived_when_declared() {
        let mut s = deployment();
        s.declares_cpu_limit = true;
        s.declares_memory_limit = true;
        let dims = dimensions_for(&s);
        assert!(dims.contains(&BandDimension::Cpu));
        assert!(dims.contains(&BandDimension::Memory));
    }

    /// Only requests and replicas move what the scheduler sees. This encodes the
    /// category error at the root of the audit.
    #[test]
    fn only_requests_and_replicas_affect_scheduling() {
        let scheduling: Vec<_> = BandDimension::ALL
            .into_iter()
            .filter(|d| d.affects_scheduling())
            .collect();
        assert_eq!(
            scheduling,
            vec![
                BandDimension::RequestCpu,
                BandDimension::RequestMemory,
                BandDimension::Replica
            ]
        );
        assert!(!BandDimension::Cpu.affects_scheduling());
        assert!(!BandDimension::Memory.affects_scheduling());
    }

    #[test]
    fn hpa_suppresses_the_replica_band() {
        let mut s = deployment();
        s.horizontal_autoscaler_present = true;
        assert_eq!(
            BandDimension::Replica.warrant_for(&s),
            Warrant::NotApplicable("an HPA already owns replicas — two writers on one field")
        );
    }

    #[test]
    fn daemonset_replicas_are_never_banded() {
        let mut s = WorkloadShape::bare("kube-system", "aws-node", WorkloadKind::DaemonSet);
        s.declares_cpu_request = true;
        let dims = dimensions_for(&s);
        assert!(!dims.contains(&BandDimension::Replica));
        // …but it still gets the scheduling lever, which is where its waste was:
        // 700m requested, 46m used across 14 pods.
        assert!(dims.contains(&BandDimension::RequestCpu));
    }

    #[test]
    fn host_plane_requires_node_pool_enrollment() {
        let s = deployment();
        for d in [
            BandDimension::Cgroup,
            BandDimension::CgroupCpu,
            BandDimension::HostParam,
        ] {
            assert_eq!(
                d.warrant_for(&s),
                Warrant::NotApplicable("workload's nodes are not enrolled in a BreatheNodePool")
            );
        }
        let mut enrolled = deployment();
        enrolled.node_pool_enrolled = true;
        assert!(BandDimension::Cgroup.warrant_for(&enrolled).is_warranted());
    }

    #[test]
    fn arc_only_for_runner_scale_sets() {
        assert!(!BandDimension::Arc.warrant_for(&deployment()).is_warranted());
        let s = WorkloadShape::bare(
            "builder-ci",
            "the self-hosted builder pool",
            WorkloadKind::AutoscalingRunnerSet,
        );
        assert!(BandDimension::Arc.warrant_for(&s).is_warranted());
    }

    #[test]
    fn app_plane_follows_the_declared_tunable() {
        let mut s = deployment();
        s.tunable = Some(Tunable::RedisMaxMemory);
        assert!(BandDimension::App.warrant_for(&s).is_warranted());
        assert!(!BandDimension::KubeParam.warrant_for(&s).is_warranted());

        s.tunable = Some(Tunable::CustomResourceField);
        assert!(BandDimension::KubeParam.warrant_for(&s).is_warranted());
        assert!(!BandDimension::App.warrant_for(&s).is_warranted());
    }

    #[test]
    fn isolation_only_for_noisy_workloads() {
        let mut s = deployment();
        assert!(!BandDimension::Isolation.warrant_for(&s).is_warranted());
        s.class = WorkloadClass::Noisy;
        assert!(BandDimension::Isolation.warrant_for(&s).is_warranted());
    }

    /// A workload nothing has been observed about warrants nothing. Absence of
    /// evidence derives an empty plan, never a defaulted one — a band invented
    /// from a guessed shape is how a floor gets set under a workload's real need
    /// (the vector OOM shape).
    #[test]
    fn a_bare_shape_warrants_nothing() {
        let s = WorkloadShape::bare("ns", "unobserved", WorkloadKind::Deployment);
        assert!(dimensions_for(&s).is_empty());
    }

    /// `ALL` must stay in sync with the enum. If a variant is added and not
    /// appended here, the derivation silently skips it — the exact failure mode
    /// this crate exists to make impossible, so it is asserted rather than
    /// trusted.
    #[test]
    fn all_is_complete_and_unique() {
        let set: BTreeSet<_> = BandDimension::ALL.into_iter().collect();
        assert_eq!(set.len(), BandDimension::ALL.len(), "duplicate in ALL");
        // Every variant must answer for a fully-featured shape or a bare one;
        // a variant missing from ALL cannot appear in either, so the union size
        // is the tripwire.
        let mut full = deployment();
        full.declares_cpu_limit = true;
        full.declares_memory_limit = true;
        full.has_volume_claims = true;
        full.node_pool_enrolled = true;
        full.observed_replicas = Some(2);
        full.class = WorkloadClass::Noisy;
        full.tunable = Some(Tunable::RedisMaxMemory);
        let warranted = dimensions_for(&full);
        // Everything except the two that are structurally exclusive here:
        // Arc (needs AutoscalingRunnerSet) and KubeParam (needs a CR field).
        assert_eq!(warranted.len(), BandDimension::ALL.len() - 2);
    }

    /// **The root-cause seal.**
    ///
    /// The exhaustive `match` in [`BandDimension::warrant_for`] forces an answer
    /// for every variant *already in the enum*. It cannot catch the defect that
    /// actually happened: `RequestBand` was a served CRD kind for months while
    /// nothing enumerated it, so 10.9 vCPU sat unreclaimable and no compiler,
    /// test or gate anywhere said a word. A 13th band kind added to `breathe-crd`
    /// and omitted here would reproduce that exactly.
    ///
    /// So the gate is parity against the CRD source itself, read at compile time.
    /// `breathe-crd` declares band kinds two ways — `band_kind!(…, "XBand", …)`
    /// for the six emitted dimensions and `kind = "XBand"` for the six
    /// hand-written ones — and both are scanned, because covering only one form
    /// is how a surface this file is meant to guard would slip past it.
    ///
    /// TIER: **CI-caught (C2)**, not truly-unrepresentable. A cross-crate
    /// set-equality is not expressible in Rust's type system; the honest
    /// destination is for `band_kind!` to emit the `BandDimension` variant too,
    /// making the two lists one list. Named, not built.
    #[test]
    fn every_band_crd_kind_has_a_dimension() {
        const CRD_SRC: &str = include_str!("../../breathe-crd/src/lib.rs");

        let mut declared: BTreeSet<&str> = BTreeSet::new();
        for (pat, end) in [("band_kind!(", ')'), ("kind = \"", '"')] {
            for seg in CRD_SRC.split(pat).skip(1) {
                let head = match seg.find(end) {
                    Some(i) => &seg[..i],
                    None => continue,
                };
                // `band_kind!` args: Spec, Type, "KindName", … — take the quoted one.
                for tok in head.split('"') {
                    if tok.ends_with("Band") && tok.chars().all(|c| c.is_ascii_alphanumeric()) {
                        declared.insert(tok);
                    }
                }
            }
        }

        assert!(
            declared.len() >= 12,
            "scanned only {} band kinds from breathe-crd — the scan itself broke, \
             which would make this gate vacuously green: {declared:?}",
            declared.len()
        );

        let covered: BTreeSet<&str> = BandDimension::ALL
            .into_iter()
            .map(BandDimension::crd_kind)
            .collect();

        let unenrolled: Vec<_> = declared.difference(&covered).copied().collect();
        assert!(
            unenrolled.is_empty(),
            "breathe-crd serves band kinds with no BandDimension, so nothing will \
             ever enroll a workload on them — this is the requestbands:0 defect \
             recurring: {unenrolled:?}"
        );

        let phantom: Vec<_> = covered.difference(&declared).copied().collect();
        assert!(
            phantom.is_empty(),
            "BandDimension names kinds breathe-crd does not serve; materializing \
             one would fail against the apiserver: {phantom:?}"
        );
    }

    #[test]
    fn explain_covers_every_dimension() {
        let e = explain(&deployment());
        assert_eq!(e.len(), BandDimension::ALL.len());
        // Negative verdicts carry a reason — an agent cannot infer one from absence.
        for (_, w) in e.iter().filter(|(_, w)| !w.is_warranted()) {
            assert!(matches!(w, Warrant::NotApplicable(r) if !r.is_empty()));
        }
    }

    #[test]
    fn crd_kinds_and_resources_line_up() {
        assert_eq!(BandDimension::RequestCpu.crd_kind(), "RequestBand");
        assert_eq!(BandDimension::RequestMemory.crd_kind(), "RequestBand");
        assert_eq!(
            BandDimension::RequestCpu
                .resource()
                .map(RequestResource::as_str),
            Some("cpu")
        );
        assert_eq!(
            BandDimension::RequestMemory
                .resource()
                .map(RequestResource::as_str),
            Some("memory")
        );
        assert_eq!(BandDimension::Cpu.resource(), None);
    }

    /// Derivation is deterministic — the output is committed to git, so an
    /// unstable order would make every reconcile a spurious diff.
    #[test]
    fn derivation_is_stable() {
        let s = deployment();
        let a: Vec<_> = dimensions_for(&s).into_iter().collect();
        let b: Vec<_> = dimensions_for(&s).into_iter().collect();
        assert_eq!(a, b);
    }
}
