//! From a policy plus observed shapes to the exact set of bands that should exist.
//!
//! [`super`] answers *which dimensions* a workload warrants. This module answers
//! the three questions that follow, each of which is a measured defect on
//! camelot-eks:
//!
//! 1. **Who owns the band** — 23% of camelot's bands (14 `TargetNotFound` + 12
//!    `Error`) point at workloads that are absent or scaled to zero. They report
//!    into the void forever because nothing retires them. Every planned band
//!    carries an [`OwnerRef`] to its target, so the apiserver garbage-collects it
//!    when the target dies. A band outliving its target stops being a thing that
//!    has to be noticed.
//! 2. **How it is armed** — camelot has `mode: shadow` ×115 and
//!    `writeIntent: None` ×115. The progressive-arming machinery
//!    (`calibrateThenWrite`, `confirmAfterSeconds`, `shadowConfirmEffect`) all
//!    ships in the CRD and *nothing climbs it*. A band that can never leave
//!    shadow is an observation tool wearing a control tool's name.
//! 3. **What to do about drift** — [`reconcile`] diffs desired against actual and
//!    returns typed [`Action`]s, so removing a limit from a workload retires its
//!    limit band rather than leaving it to fail.
//!
//! Purity is deliberate, as in [`super`]: no Kubernetes types and no clock. The
//! caller supplies observed shapes and the current band inventory; this module
//! decides. That keeps the ownership and arming rules — the parts that are
//! genuinely hard to get right — under unit test rather than under a cluster.

use crate::{BandDimension, WorkloadShape, dimensions_for};
use std::collections::{BTreeMap, BTreeSet};

/// How a band is authorized to act, mirroring the CRD's `writeIntent`.
///
/// Named here rather than imported so this crate stays dependency-free; the
/// controller maps these onto the CRD spellings at the boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteIntent {
    /// Never write; keep deciding and reporting. The honest default for a band
    /// that has not yet earned anything.
    #[default]
    Observe,
    /// Shadow until a clean observation window passes, then write.
    CalibrateThenWrite,
    /// Write now. Requires a named authority — see [`ArmingPolicy::authorized_by`].
    Write,
    /// Never write, keep observing. Distinct from [`Self::Observe`]: an operator
    /// pinned this, so calibration must not promote it.
    Frozen,
}

impl WriteIntent {
    /// The CRD spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::CalibrateThenWrite => "calibrateThenWrite",
            Self::Write => "write",
            Self::Frozen => "frozen",
        }
    }

    /// Whether this intent can ever result in a write.
    #[must_use]
    pub const fn can_write(self) -> bool {
        matches!(self, Self::CalibrateThenWrite | Self::Write)
    }
}

/// How aggressively a policy arms the bands it materializes.
///
/// The default is [`WriteIntent::CalibrateThenWrite`] rather than
/// [`WriteIntent::Observe`], and that is the whole point of the type. Observe
/// never promotes itself, which is how 115 bands sat in shadow indefinitely
/// while the cluster ran at 30% requested and 13% used. Calibrate-then-write
/// still refuses to act until the band's own observation window is clean — the
/// safety comes from the evidence gate, not from never arming.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArmingPolicy {
    /// The intent stamped on newly materialized bands.
    pub initial_intent: WriteIntent,
    /// Seconds of clean observation before a calibrating band promotes itself.
    pub confirm_after_seconds: u32,
    /// Who authorized writing. **Required** when `initial_intent` is
    /// [`WriteIntent::Write`]; the CRD refuses an unattributed write and so does
    /// [`ArmingPolicy::validate`].
    pub authorized_by: Option<String>,
    /// Dimensions this policy refuses to arm regardless of the above, by name.
    ///
    /// The escape hatch for a workload whose carve is genuinely unsafe, kept as
    /// data so the refusal is visible in the policy rather than special-cased in
    /// code.
    pub never_arm: BTreeSet<BandDimension>,
}

impl Default for ArmingPolicy {
    fn default() -> Self {
        Self {
            initial_intent: WriteIntent::CalibrateThenWrite,
            confirm_after_seconds: 3600,
            authorized_by: None,
            never_arm: BTreeSet::new(),
        }
    }
}

/// Why an [`ArmingPolicy`] is not usable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyError {
    /// `initial_intent: write` without `authorized_by`.
    WriteWithoutAuthority,
}

impl ArmingPolicy {
    /// Reject a policy that cannot be honoured.
    ///
    /// # Errors
    /// [`PolicyError::WriteWithoutAuthority`] when arming to `write` with no
    /// named authority — an unattributed standing write authorization is exactly
    /// the thing that should not be expressible.
    pub fn validate(&self) -> Result<(), PolicyError> {
        if self.initial_intent == WriteIntent::Write && self.authorized_by.is_none() {
            return Err(PolicyError::WriteWithoutAuthority);
        }
        Ok(())
    }

    /// The intent to stamp on a band for `dimension`.
    #[must_use]
    pub fn intent_for(&self, dimension: BandDimension) -> WriteIntent {
        if self.never_arm.contains(&dimension) {
            WriteIntent::Frozen
        } else {
            self.initial_intent
        }
    }
}

/// The owner a materialized band points at, so the apiserver can collect it.
///
/// # The seal
///
/// `uid` is a `String`, not an `Option<String>`, and the field is private with
/// [`OwnerRef::observed`] as the only constructor. That is deliberate and it is
/// the whole invariant: a Kubernetes `ownerReference` without a UID does not
/// establish ownership, so a band built from one is exactly the orphan that
/// produced camelot's 14 `TargetNotFound` + 12 `Error` bands.
///
/// Because [`BandPlan`] requires an `OwnerRef` by value, and an `OwnerRef` cannot
/// exist without a UID, **"planned a band that will not be garbage-collected with
/// its target" has no expressible path**. The earlier shape — an `Option` plus an
/// `is_collectable()` predicate — left that state constructible and merely
/// *reported*, which is a check a caller can ignore.
///
/// The failure to observe a UID is pushed to the boundary where it belongs and
/// typed there ([`ObserveError`]), rather than carried inward as a `None` that
/// every downstream consumer must remember to test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerRef {
    kind: &'static str,
    name: String,
    uid: String,
}

/// Why a workload could not be turned into an [`OwnerRef`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObserveError {
    /// The workload was seen but carried no UID. Not a band that "might leak" —
    /// a band that cannot be planned at all until the UID is observed.
    MissingUid {
        /// The workload's name, for the operator-facing report.
        name: String,
    },
}

impl OwnerRef {
    /// Build an owner reference from an observed workload.
    ///
    /// # Errors
    /// [`ObserveError::MissingUid`] when the observation carried no UID. Refusing
    /// here is what makes the orphan class unconstructible downstream.
    pub fn observed(
        kind: &'static str,
        name: impl Into<String>,
        uid: Option<impl Into<String>>,
    ) -> Result<Self, ObserveError> {
        let name = name.into();
        match uid {
            Some(uid) => Ok(Self {
                kind,
                name,
                uid: uid.into(),
            }),
            None => Err(ObserveError::MissingUid { name }),
        }
    }

    /// Owning workload's API kind.
    #[must_use]
    pub const fn kind(&self) -> &str {
        self.kind
    }

    /// Owning workload's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Owning workload's UID.
    #[must_use]
    pub fn uid(&self) -> &str {
        &self.uid
    }
}

/// One band that should exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BandPlan {
    /// Namespace, always the target's.
    pub namespace: String,
    /// Deterministic object name.
    pub name: String,
    /// Which dimension.
    pub dimension: BandDimension,
    /// Name of the targeted workload.
    pub target_name: String,
    /// Kind of the targeted workload.
    pub target_kind: &'static str,
    /// The owner reference that makes this band collectable.
    pub owner: OwnerRef,
    /// Authorization stamped at materialization.
    pub intent: WriteIntent,
    /// Clean-observation seconds before self-promotion.
    pub confirm_after_seconds: u32,
    /// The `BreathePosture` supplying unset behavioural fields.
    pub posture_ref: Option<String>,
}

/// The deterministic object name for a band.
///
/// `<workload>-<suffix>`, where the suffix distinguishes the two `RequestBand`
/// resources that would otherwise collide on one name. Determinism matters
/// because these are committed to git: a name derived from anything unstable
/// makes every reconcile a spurious diff.
#[must_use]
pub fn band_name(workload: &str, dimension: BandDimension) -> String {
    let suffix = match dimension {
        BandDimension::Memory => "memory",
        BandDimension::Cpu => "cpu",
        BandDimension::Storage => "storage",
        BandDimension::RequestCpu => "request-cpu",
        BandDimension::RequestMemory => "request-memory",
        BandDimension::Replica => "replica",
        BandDimension::Arc => "arc",
        BandDimension::Cgroup => "cgroup",
        BandDimension::CgroupCpu => "cgroup-cpu",
        BandDimension::HostParam => "host-param",
        BandDimension::KubeParam => "kube-param",
        BandDimension::App => "app",
        BandDimension::Isolation => "isolation",
    };
    let mut s = String::with_capacity(workload.len() + 1 + suffix.len());
    s.push_str(workload);
    s.push('-');
    s.push_str(suffix);
    s
}

/// The kind string for a workload, for the band's `targetRef`.
#[must_use]
pub const fn target_kind(kind: crate::WorkloadKind) -> &'static str {
    match kind {
        crate::WorkloadKind::Deployment => "Deployment",
        crate::WorkloadKind::StatefulSet => "StatefulSet",
        crate::WorkloadKind::DaemonSet => "DaemonSet",
        crate::WorkloadKind::AutoscalingRunnerSet => "AutoscalingRunnerSet",
    }
}

/// Every band that should exist for `shapes` under `arming`.
///
/// Output is ordered by (namespace, name, dimension) so two runs over the same
/// input are byte-identical.
#[must_use]
pub fn plan_for(
    shapes: &[(WorkloadShape, OwnerRef)],
    arming: &ArmingPolicy,
    posture_ref: Option<&str>,
) -> Vec<BandPlan> {
    let mut out: Vec<BandPlan> = shapes
        .iter()
        .flat_map(|(shape, owner)| {
            dimensions_for(shape).into_iter().map(move |dimension| BandPlan {
                namespace: shape.namespace.clone(),
                name: band_name(&shape.name, dimension),
                dimension,
                target_name: shape.name.clone(),
                target_kind: target_kind(shape.kind),
                owner: owner.clone(),
                intent: arming.intent_for(dimension),
                confirm_after_seconds: arming.confirm_after_seconds,
                posture_ref: posture_ref.map(String::from),
            })
        })
        .collect();
    out.sort_by(|a, b| {
        (&a.namespace, &a.name, a.dimension).cmp(&(&b.namespace, &b.name, b.dimension))
    });
    out
}

/// A band that currently exists, as observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingBand {
    /// Its namespace.
    pub namespace: String,
    /// Its name.
    pub name: String,
    /// Its dimension.
    pub dimension: BandDimension,
    /// Whether it carries a collectable owner reference.
    pub owned: bool,
}

/// What to do to converge actual onto desired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Materialize a band that should exist and does not.
    Create(BandPlan),
    /// Adopt an existing unowned band — attach the owner reference it is missing.
    ///
    /// The migration path for camelot's 115 pre-existing hand-authored bands:
    /// they are *correct*, merely orphan-prone, so they are adopted rather than
    /// deleted and recreated. Deleting them would drop their accumulated
    /// observation history, which is the one thing a band cannot rebuild quickly.
    Adopt(BandPlan),
    /// Retire a band whose dimension is no longer warranted.
    ///
    /// Only reachable for bands the target still owns. A band whose *target* is
    /// gone is collected by the apiserver and never appears here.
    Retire {
        /// Namespace of the band to retire.
        namespace: String,
        /// Name of the band to retire.
        name: String,
        /// Why it is no longer warranted.
        reason: &'static str,
    },
}

/// Diff desired against actual.
///
/// Ordering is Create, then Adopt, then Retire — additive work lands before any
/// removal, so a mid-reconcile interruption leaves the cluster over-covered
/// rather than under-covered.
#[must_use]
pub fn reconcile(desired: &[BandPlan], actual: &[ExistingBand]) -> Vec<Action> {
    let actual_by_key: BTreeMap<(&str, &str), &ExistingBand> = actual
        .iter()
        .map(|b| ((b.namespace.as_str(), b.name.as_str()), b))
        .collect();
    let desired_keys: BTreeSet<(&str, &str)> = desired
        .iter()
        .map(|p| (p.namespace.as_str(), p.name.as_str()))
        .collect();

    let mut creates = Vec::new();
    let mut adopts = Vec::new();
    for plan in desired {
        match actual_by_key.get(&(plan.namespace.as_str(), plan.name.as_str())) {
            None => creates.push(Action::Create(plan.clone())),
            Some(existing) if !existing.owned => adopts.push(Action::Adopt(plan.clone())),
            Some(_) => {}
        }
    }

    let retires = actual
        .iter()
        .filter(|b| !desired_keys.contains(&(b.namespace.as_str(), b.name.as_str())))
        .map(|b| Action::Retire {
            namespace: b.namespace.clone(),
            name: b.name.clone(),
            reason: "dimension no longer warranted by the workload's observed shape",
        });

    creates.into_iter().chain(adopts).chain(retires).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WorkloadKind, WorkloadShape};

    fn owner(uid: Option<&str>) -> OwnerRef {
        OwnerRef::observed("Deployment", "pangea-operator", uid).expect("a uid")
    }

    fn shape() -> WorkloadShape {
        let mut s = WorkloadShape::bare("camelot", "pangea-operator", WorkloadKind::Deployment);
        s.declares_cpu_request = true;
        s.declares_memory_request = true;
        s
    }

    #[test]
    fn plans_request_bands_for_the_worst_offender() {
        // pangea-operator: requests 2000m, uses 2m. Measured on camelot 2026-08-05.
        let plans = plan_for(&[(shape(), owner(Some("uid-1")))], &ArmingPolicy::default(), None);
        let names: Vec<_> = plans.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["pangea-operator-request-cpu", "pangea-operator-request-memory"]
        );
    }

    /// The `TargetNotFound` seal, stated as a boundary refusal.
    ///
    /// There is deliberately NO test that a planned band "is collectable" —
    /// such a test could not fail. `OwnerRef` has no public field and no
    /// constructor that yields one without a UID, so an uncollectable plan is
    /// not merely absent from the test corpus, it is unconstructible. What CAN
    /// be tested is that the boundary refuses, which is where the state is
    /// still expressible.
    #[test]
    fn a_workload_without_a_uid_cannot_produce_an_owner() {
        let e = OwnerRef::observed("Deployment", "pangea-operator", None::<String>);
        assert_eq!(
            e,
            Err(ObserveError::MissingUid {
                name: "pangea-operator".into()
            })
        );
    }

    /// The arming fix: the default actually climbs the ladder.
    #[test]
    fn the_default_arming_is_not_observe_forever() {
        let a = ArmingPolicy::default();
        assert_eq!(a.initial_intent, WriteIntent::CalibrateThenWrite);
        assert!(a.initial_intent.can_write());
        assert!(a.validate().is_ok());
    }

    #[test]
    fn write_without_authority_is_refused() {
        let a = ArmingPolicy {
            initial_intent: WriteIntent::Write,
            ..Default::default()
        };
        assert_eq!(a.validate(), Err(PolicyError::WriteWithoutAuthority));

        let ok = ArmingPolicy {
            initial_intent: WriteIntent::Write,
            authorized_by: Some("drzzln".into()),
            ..Default::default()
        };
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn never_arm_freezes_just_that_dimension() {
        let mut a = ArmingPolicy::default();
        a.never_arm.insert(BandDimension::RequestMemory);
        assert_eq!(a.intent_for(BandDimension::RequestMemory), WriteIntent::Frozen);
        assert_eq!(
            a.intent_for(BandDimension::RequestCpu),
            WriteIntent::CalibrateThenWrite
        );
        assert!(!WriteIntent::Frozen.can_write());
    }

    #[test]
    fn reconcile_creates_what_is_missing() {
        let desired = plan_for(&[(shape(), owner(Some("u")))], &ArmingPolicy::default(), None);
        let actions = reconcile(&desired, &[]);
        assert_eq!(actions.len(), 2);
        assert!(actions.iter().all(|a| matches!(a, Action::Create(_))));
    }

    /// camelot's 115 existing bands are correct but unowned — adopt, never
    /// delete-and-recreate, because their observation history is not rebuildable.
    #[test]
    fn unowned_existing_bands_are_adopted_not_recreated() {
        let desired = plan_for(&[(shape(), owner(Some("u")))], &ArmingPolicy::default(), None);
        let actual = vec![ExistingBand {
            namespace: "camelot".into(),
            name: "pangea-operator-request-cpu".into(),
            dimension: BandDimension::RequestCpu,
            owned: false,
        }];
        let actions = reconcile(&desired, &actual);
        assert!(actions.iter().any(|a| matches!(a, Action::Adopt(p) if p.name == "pangea-operator-request-cpu")));
        assert!(!actions.iter().any(|a| matches!(a, Action::Retire { name, .. } if name == "pangea-operator-request-cpu")));
    }

    #[test]
    fn an_already_owned_band_is_left_alone() {
        let desired = plan_for(&[(shape(), owner(Some("u")))], &ArmingPolicy::default(), None);
        let actual: Vec<_> = desired
            .iter()
            .map(|p| ExistingBand {
                namespace: p.namespace.clone(),
                name: p.name.clone(),
                dimension: p.dimension,
                owned: true,
            })
            .collect();
        assert!(reconcile(&desired, &actual).is_empty());
    }

    /// Removing a limit from a workload retires its limit band — the drift arm.
    #[test]
    fn an_unwarranted_dimension_is_retired() {
        let desired = plan_for(&[(shape(), owner(Some("u")))], &ArmingPolicy::default(), None);
        let actual = vec![ExistingBand {
            namespace: "camelot".into(),
            name: "pangea-operator-cpu".into(), // a limit band; no limit is declared now
            dimension: BandDimension::Cpu,
            owned: true,
        }];
        let actions = reconcile(&desired, &actual);
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::Retire { name, .. } if name == "pangea-operator-cpu"))
        );
    }

    /// Additive work must precede removals, so an interrupted reconcile leaves
    /// the cluster over-covered rather than under-covered.
    #[test]
    fn creates_and_adopts_precede_retires() {
        let desired = plan_for(&[(shape(), owner(Some("u")))], &ArmingPolicy::default(), None);
        let actual = vec![ExistingBand {
            namespace: "camelot".into(),
            name: "pangea-operator-storage".into(),
            dimension: BandDimension::Storage,
            owned: true,
        }];
        let actions = reconcile(&desired, &actual);
        let first_retire = actions
            .iter()
            .position(|a| matches!(a, Action::Retire { .. }))
            .expect("a retire");
        assert!(
            actions[..first_retire]
                .iter()
                .all(|a| matches!(a, Action::Create(_) | Action::Adopt(_)))
        );
    }

    /// Names are stable across runs and unique per dimension — they are committed
    /// to git, so instability would make every reconcile a spurious diff.
    #[test]
    fn names_are_deterministic_and_collision_free() {
        let mut seen = BTreeSet::new();
        for d in BandDimension::ALL {
            assert!(seen.insert(band_name("w", d)), "collision on {d:?}");
        }
        assert_eq!(band_name("w", BandDimension::RequestCpu), "w-request-cpu");
        assert_eq!(
            band_name("w", BandDimension::RequestMemory),
            "w-request-memory"
        );
    }

    #[test]
    fn plan_output_is_ordered() {
        let mut b = shape();
        b.name = "aaa".into();
        let plans = plan_for(
            &[(shape(), owner(Some("u"))), (b, owner(Some("u2")))],
            &ArmingPolicy::default(),
            Some("batch"),
        );
        let names: Vec<_> = plans.iter().map(|p| p.name.as_str()).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        assert_eq!(names, sorted);
        assert!(plans.iter().all(|p| p.posture_ref.as_deref() == Some("batch")));
    }
}
