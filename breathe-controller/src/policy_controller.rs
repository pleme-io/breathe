//! The `BreathePolicy` reconciler — selector-based band auto-enrollment.
//!
//! Observes workloads, derives the bands they warrant via `breathe-discovery`,
//! and converges the cluster onto that set. The decision is not made here: this
//! module is I/O, and every rule it applies is a pure function under unit test in
//! `breathe-discovery`. That split is deliberate — the enrollment logic is the
//! part that was wrong for months, and it is now testable without a cluster.
//!
//! Three properties worth stating because they are the measured defects, fixed:
//!
//! - **Bands are owned.** Every materialized band carries an `ownerReference` to
//!   its workload, so the apiserver collects it when the target dies. camelot's
//!   14 `TargetNotFound` + 12 `Error` bands exist because nothing did this.
//! - **Bands are armed.** `calibrateThenWrite` by default, so a band climbs to
//!   effect on its own clean observation rather than sitting in shadow forever.
//! - **All 12 kinds, one code path.** Bands are written as `DynamicObject`s keyed
//!   by GVK. Twelve typed paths would be twelve places for a thirteenth kind to
//!   be forgotten, which is the exact shape of the original bug.

use std::collections::BTreeSet;
use std::sync::Arc;

use breathe_crd::{BreathePolicy, BreathePolicyStatus};
use breathe_discovery::plan::{
    Action, ArmingPolicy, BandPlan, ExistingBand, OwnerRef, WriteIntent, plan_for, reconcile,
};
use breathe_discovery::{BandDimension, Tunable, WorkloadClass, WorkloadKind, WorkloadShape};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::PodTemplateSpec;
use kube::api::{Api, ApiResource, DeleteParams, DynamicObject, GroupVersionKind, ListParams, Patch, PatchParams};
use kube::runtime::controller::Action as CtrlAction;
use kube::{Client, Resource, ResourceExt};
use serde_json::json;
use tracing::{info, warn};

/// Field manager for server-side apply. Distinct from the band controllers' own,
/// so ownership of the *band object* (this controller) never collides with
/// ownership of the *carved field* (the band's own reconciler).
const FIELD_MANAGER: &str = "breathe-policy";

/// Errors this reconciler surfaces.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A Kubernetes API call failed.
    #[error("kube api: {0}")]
    Kube(#[from] kube::Error),
    /// The policy is not usable as written.
    #[error("invalid policy: {0}")]
    Policy(String),
}

/// Context handed to the reconciler.
pub struct PolicyCtx {
    /// Cluster client.
    pub client: Client,
    /// Requeue interval.
    pub requeue: std::time::Duration,
}

/// Does `ns` pass the policy's namespace selector.
///
/// Exclusion wins over inclusion. A namespace named in both is excluded, because
/// the failure modes are not symmetric: wrongly skipping a namespace loses
/// observation, wrongly carving in one can take a workload down.
#[must_use]
pub fn namespace_matches(ns: &str, include: &[String], exclude: &[String]) -> bool {
    if exclude.iter().any(|e| e == ns) {
        return false;
    }
    include.is_empty() || include.iter().any(|i| i == ns)
}

/// Read a pod template into the resource-shape flags the derivation needs.
fn observe_resources(tpl: &PodTemplateSpec, shape: &mut WorkloadShape) {
    let Some(spec) = tpl.spec.as_ref() else { return };
    for c in &spec.containers {
        let Some(res) = c.resources.as_ref() else {
            continue;
        };
        if let Some(r) = res.requests.as_ref() {
            shape.declares_cpu_request |= r.contains_key("cpu");
            shape.declares_memory_request |= r.contains_key("memory");
        }
        if let Some(l) = res.limits.as_ref() {
            shape.declares_cpu_limit |= l.contains_key("cpu");
            shape.declares_memory_limit |= l.contains_key("memory");
        }
    }
}

/// Map a `BandDimension` onto the GVK its object is written as.
fn band_gvk(dimension: BandDimension) -> GroupVersionKind {
    GroupVersionKind::gvk("breathe.pleme.io", "v1", dimension.crd_kind())
}

/// Build the band object for a plan.
///
/// `targetRef` and `ownerReferences` both point at the workload but mean
/// different things: the first is what the band carves, the second is what makes
/// the band collectable. They are set from the same `OwnerRef` so they cannot
/// drift apart.
fn band_object(plan: &BandPlan, policy: &BreathePolicy) -> DynamicObject {
    let mut spec = json!({
        "targetRef": { "kind": plan.target_kind, "name": plan.target_name },
        "writeIntent": { "intent": plan.intent.as_str() },
        "confirmAfterSeconds": plan.confirm_after_seconds,
    });
    if let Some(r) = plan.dimension.resource() {
        spec["resource"] = json!(r.as_str());
    }
    if let Some(p) = plan.posture_ref.as_deref() {
        spec["postureRef"] = json!(p);
    }
    if plan.intent == WriteIntent::Write {
        if let Some(a) = policy.spec.arming.authorized_by.as_deref() {
            spec["writeIntent"]["authorizedBy"] = json!(a);
        }
    }

    let ar = ApiResource::from_gvk(&band_gvk(plan.dimension));
    DynamicObject::new(&plan.name, &ar)
        .within(&plan.namespace)
        .data(json!({ "spec": spec }))
}

/// The owner reference block that makes a band collectable with its target.
fn owner_reference(owner: &OwnerRef) -> serde_json::Value {
    json!([{
        "apiVersion": if owner.kind() == "AutoscalingRunnerSet" { "actions.github.com/v1alpha1" } else { "apps/v1" },
        "kind": owner.kind(),
        "name": owner.name(),
        "uid": owner.uid(),
        // Not a controller reference: breathe does not own the workload's
        // lifecycle, it only wants collection. blockOwnerDeletion would let a
        // stuck band delay deleting the workload itself, which inverts the
        // relationship — the workload is primary.
        "controller": false,
        "blockOwnerDeletion": false,
    }])
}

/// Reconcile one `BreathePolicy`.
///
/// # Errors
/// Propagates API failures; an invalid policy is reported as [`Error::Policy`]
/// rather than retried, since retrying a malformed spec cannot succeed.
#[allow(clippy::too_many_lines)]
pub async fn reconcile_policy(
    policy: Arc<BreathePolicy>,
    ctx: Arc<PolicyCtx>,
) -> Result<CtrlAction, Error> {
    let name = policy.name_any();

    if policy.spec.suspend {
        patch_status(&ctx.client, &policy, BreathePolicyStatus {
            phase: Some("Suspended".into()),
            ..Default::default()
        })
        .await?;
        return Ok(CtrlAction::requeue(ctx.requeue));
    }

    // Arming is validated before anything is observed: an unusable policy must
    // not half-apply. `write` without a named authority is refused here exactly
    // as the pure type refuses it.
    let arming = ArmingPolicy {
        initial_intent: match policy.spec.arming.initial_intent.as_str() {
            "observe" => WriteIntent::Observe,
            "calibrateThenWrite" => WriteIntent::CalibrateThenWrite,
            "write" => WriteIntent::Write,
            "frozen" => WriteIntent::Frozen,
            other => {
                return Err(Error::Policy(format!("unknown initialIntent {other:?}")));
            }
        },
        confirm_after_seconds: policy.spec.arming.confirm_after_seconds,
        authorized_by: policy.spec.arming.authorized_by.clone(),
        never_arm: policy
            .spec
            .arming
            .never_arm
            .iter()
            .filter_map(|n| dimension_by_name(n))
            .collect(),
    };
    arming
        .validate()
        .map_err(|e| Error::Policy(format!("{e:?}")))?;

    let inc = &policy.spec.selector.namespaces;
    let exc = &policy.spec.selector.exclude_namespaces;
    let lp = policy.spec.selector.match_labels.as_ref().map_or_else(
        ListParams::default,
        |l| ListParams::default().labels(l),
    );

    // ---- observe -----------------------------------------------------------
    let mut shapes: Vec<(WorkloadShape, OwnerRef)> = Vec::new();
    let mut unobservable = 0_u32;

    let hpas: Api<HorizontalPodAutoscaler> = Api::all(ctx.client.clone());
    let hpa_targets: BTreeSet<(String, String)> = hpas
        .list(&ListParams::default())
        .await?
        .into_iter()
        .filter_map(|h| {
            let ns = h.namespace()?;
            Some((ns, h.spec?.scale_target_ref.name))
        })
        .collect();

    for (kind, wk) in [
        ("Deployment", WorkloadKind::Deployment),
        ("StatefulSet", WorkloadKind::StatefulSet),
        ("DaemonSet", WorkloadKind::DaemonSet),
    ] {
        let observed: Vec<ObservedWorkload> = match wk {
            WorkloadKind::Deployment => Api::<Deployment>::all(ctx.client.clone())
                .list(&lp)
                .await?
                .into_iter()
                .map(ObservedWorkload::from)
                .collect(),
            WorkloadKind::StatefulSet => Api::<StatefulSet>::all(ctx.client.clone())
                .list(&lp)
                .await?
                .into_iter()
                .map(ObservedWorkload::from)
                .collect(),
            _ => Api::<DaemonSet>::all(ctx.client.clone())
                .list(&lp)
                .await?
                .into_iter()
                .map(ObservedWorkload::from)
                .collect(),
        };
        for o in observed {
            if !namespace_matches(&o.namespace, inc, exc) {
                continue;
            }
            let Ok(owner) = OwnerRef::observed(kind, o.name.clone(), o.uid.clone()) else {
                // No UID observed ⇒ no collectable owner ⇒ no band. Counted, never
                // silently dropped: a shortfall that is not reported reads as
                // success from the outside.
                unobservable += 1;
                continue;
            };
            let mut shape = WorkloadShape::bare(o.namespace.clone(), o.name.clone(), wk);
            shape.declares_cpu_request = o.cpu_request;
            shape.declares_memory_request = o.memory_request;
            shape.declares_cpu_limit = o.cpu_limit;
            shape.declares_memory_limit = o.memory_limit;
            shape.has_volume_claims = o.volume_claims;
            shape.observed_replicas = o.replicas;
            shape.horizontal_autoscaler_present =
                hpa_targets.contains(&(o.namespace.clone(), o.name.clone()));
            shape.class = WorkloadClass::Standard;
            shape.tunable = None::<Tunable>;
            shapes.push((shape, owner));
        }
    }

    // ---- decide ------------------------------------------------------------
    let desired = plan_for(&shapes, &arming, policy.spec.posture_ref.as_deref());
    let dimensions: BTreeSet<&str> = desired
        .iter()
        .map(|p| dimension_name(p.dimension))
        .collect();

    // Existing bands, per kind actually in play — never a blind list of all 12.
    let mut actual: Vec<ExistingBand> = Vec::new();
    for dim in desired.iter().map(|p| p.dimension).collect::<BTreeSet<_>>() {
        let ar = ApiResource::from_gvk(&band_gvk(dim));
        let api: Api<DynamicObject> = Api::all_with(ctx.client.clone(), &ar);
        let Ok(list) = api.list(&ListParams::default()).await else {
            continue;
        };
        for b in list {
            let Some(ns) = b.namespace() else { continue };
            if !namespace_matches(&ns, inc, exc) {
                continue;
            }
            actual.push(ExistingBand {
                namespace: ns,
                name: b.name_any(),
                dimension: dim,
                owned: b.metadata.owner_references.as_ref().is_some_and(|o| !o.is_empty()),
            });
        }
    }

    let actions = reconcile(&desired, &actual);
    let plan_only = policy.spec.plan_only;
    let (mut created, mut adopted, mut retired) = (0_u32, 0_u32, 0_u32);

    // ---- converge ----------------------------------------------------------
    for action in actions {
        match action {
            Action::Create(plan) | Action::Adopt(plan) => {
                let is_adopt = actual
                    .iter()
                    .any(|b| b.namespace == plan.namespace && b.name == plan.name);
                if plan_only {
                    if is_adopt { adopted += 1 } else { created += 1 }
                    continue;
                }
                let ar = ApiResource::from_gvk(&band_gvk(plan.dimension));
                let api: Api<DynamicObject> = Api::namespaced_with(
                    ctx.client.clone(),
                    &plan.namespace,
                    &ar,
                );
                let mut obj = band_object(&plan, &policy);
                obj.metadata.owner_references =
                    serde_json::from_value(owner_reference(&plan.owner)).ok();
                match api
                    .patch(
                        &plan.name,
                        &PatchParams::apply(FIELD_MANAGER).force(),
                        &Patch::Apply(&obj),
                    )
                    .await
                {
                    Ok(_) => {
                        if is_adopt {
                            adopted += 1;
                        } else {
                            created += 1;
                        }
                    }
                    Err(e) => warn!(band = %plan.name, ns = %plan.namespace, error = %e,
                                    "materializing band failed"),
                }
            }
            Action::Retire {
                namespace,
                name,
                reason,
            } => {
                // Retire only bands this controller owns. A hand-authored band
                // that predates the policy is left alone — deleting an operator's
                // object because a derivation disagrees is not this loop's call.
                //
                // This guard is evaluated BEFORE the planOnly branch on purpose.
                // With it after, a plan counts every retire candidate while a real
                // run skips the unowned ones, so the plan overstates deletions —
                // measured on camelot's first plan as retired=36 where a real
                // first pass would retire 0, since nothing is owned until this
                // loop has adopted it. A plan that does not predict the run is
                // worse than no plan: it is read as a forecast.
                let Some(existing) = actual
                    .iter()
                    .find(|b| b.namespace == namespace && b.name == name)
                else {
                    continue;
                };
                if !existing.owned {
                    continue;
                }
                if plan_only {
                    retired += 1;
                    continue;
                }
                let ar = ApiResource::from_gvk(&band_gvk(existing.dimension));
                let api: Api<DynamicObject> =
                    Api::namespaced_with(ctx.client.clone(), &namespace, &ar);
                if let Err(e) = api.delete(&name, &DeleteParams::default()).await {
                    warn!(band = %name, ns = %namespace, error = %e, "retiring band failed");
                } else {
                    info!(band = %name, ns = %namespace, reason, "retired band");
                    retired += 1;
                }
            }
        }
    }

    let status = BreathePolicyStatus {
        phase: Some(if plan_only { "PlanOnly" } else { "Reconciled" }.into()),
        workloads_matched: u32::try_from(shapes.len()).unwrap_or(u32::MAX),
        bands_desired: u32::try_from(desired.len()).unwrap_or(u32::MAX),
        bands_created: created,
        bands_adopted: adopted,
        bands_retired: retired,
        workloads_unobservable: unobservable,
        dimensions: dimensions.into_iter().map(String::from).collect(),
        last_reconciled: None,
        conditions: Vec::new(),
    };
    info!(
        policy = %name,
        matched = status.workloads_matched,
        desired = status.bands_desired,
        created, adopted, retired,
        "BreathePolicy reconciled"
    );
    patch_status(&ctx.client, &policy, status).await?;
    Ok(CtrlAction::requeue(ctx.requeue))
}

/// Error policy — retry on the requeue interval.
#[must_use]
pub fn error_policy_policy(
    _obj: Arc<BreathePolicy>,
    err: &Error,
    ctx: Arc<PolicyCtx>,
) -> CtrlAction {
    warn!(error = %err, "BreathePolicy reconcile failed");
    CtrlAction::requeue(ctx.requeue)
}

async fn patch_status(
    client: &Client,
    policy: &BreathePolicy,
    status: BreathePolicyStatus,
) -> Result<(), Error> {
    let api: Api<BreathePolicy> = Api::all(client.clone());
    api.patch_status(
        &policy.name_any(),
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(json!({
            "apiVersion": "breathe.pleme.io/v1",
            "kind": "BreathePolicy",
            "status": status,
        })),
    )
    .await?;
    Ok(())
}

/// `BandDimension` → its stable name, for status and `neverArm`.
#[must_use]
pub const fn dimension_name(d: BandDimension) -> &'static str {
    match d {
        BandDimension::Memory => "Memory",
        BandDimension::Cpu => "Cpu",
        BandDimension::Storage => "Storage",
        BandDimension::RequestCpu => "RequestCpu",
        BandDimension::RequestMemory => "RequestMemory",
        BandDimension::Replica => "Replica",
        BandDimension::Arc => "Arc",
        BandDimension::Cgroup => "Cgroup",
        BandDimension::CgroupCpu => "CgroupCpu",
        BandDimension::HostParam => "HostParam",
        BandDimension::KubeParam => "KubeParam",
        BandDimension::App => "App",
        BandDimension::Isolation => "Isolation",
    }
}

/// The inverse of [`dimension_name`], for parsing `neverArm`.
#[must_use]
pub fn dimension_by_name(s: &str) -> Option<BandDimension> {
    BandDimension::ALL
        .into_iter()
        .find(|d| dimension_name(*d) == s)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exclusion beats inclusion: the two failure modes are not symmetric.
    #[test]
    fn exclusion_wins_over_inclusion() {
        let inc = vec!["camelot".to_owned()];
        let exc = vec!["camelot".to_owned()];
        assert!(!namespace_matches("camelot", &inc, &exc));
        assert!(namespace_matches("camelot", &inc, &[]));
    }

    #[test]
    fn empty_include_means_all_but_excluded() {
        let exc = vec!["kube-system".to_owned()];
        assert!(namespace_matches("camelot", &[], &exc));
        assert!(!namespace_matches("kube-system", &[], &exc));
    }

    /// Every dimension round-trips through its name, so a `neverArm` entry can
    /// address any dimension and a status can report any.
    #[test]
    fn dimension_names_round_trip() {
        for d in BandDimension::ALL {
            assert_eq!(dimension_by_name(dimension_name(d)), Some(d));
        }
    }

    #[test]
    fn an_unknown_dimension_name_is_none() {
        assert_eq!(dimension_by_name("Nonexistent"), None);
    }

    /// The GVK a band is written under must match the CRD kind the derivation
    /// names, or every apply 404s.
    #[test]
    fn band_gvk_tracks_the_crd_kind() {
        for d in BandDimension::ALL {
            let gvk = band_gvk(d);
            assert_eq!(gvk.group, "breathe.pleme.io");
            assert_eq!(gvk.version, "v1");
            assert_eq!(gvk.kind, d.crd_kind());
        }
    }

    /// Ownership is non-blocking and non-controlling: breathe wants collection,
    /// not authority over the workload's lifecycle.
    #[test]
    fn owner_reference_never_blocks_workload_deletion() {
        let o = OwnerRef::observed("Deployment", "pangea-operator", Some("uid-1")).unwrap();
        let v = owner_reference(&o);
        assert_eq!(v[0]["controller"], json!(false));
        assert_eq!(v[0]["blockOwnerDeletion"], json!(false));
        assert_eq!(v[0]["uid"], json!("uid-1"));
    }
}

/// One workload flattened to exactly the facts the derivation consumes.
///
/// The three apps/v1 kinds share no trait that exposes a pod template, so rather
/// than a macro over their differing shapes (which fights the borrow checker for
/// no gain) each is projected once, here, into a kind-agnostic record. Observation
/// stops at this boundary; everything past it is pure.
struct ObservedWorkload {
    namespace: String,
    name: String,
    uid: Option<String>,
    cpu_request: bool,
    memory_request: bool,
    cpu_limit: bool,
    memory_limit: bool,
    volume_claims: bool,
    replicas: Option<u32>,
}

impl ObservedWorkload {
    fn build(
        meta: &kube::core::ObjectMeta,
        tpl: Option<&PodTemplateSpec>,
        replicas: Option<i32>,
        has_claims: bool,
    ) -> Self {
        let mut shape = WorkloadShape::bare("", "", WorkloadKind::Deployment);
        if let Some(t) = tpl {
            observe_resources(t, &mut shape);
        }
        let volume_claims = has_claims
            || tpl.and_then(|t| t.spec.as_ref()).is_some_and(|s| {
                s.volumes
                    .as_ref()
                    .is_some_and(|vs| vs.iter().any(|v| v.persistent_volume_claim.is_some()))
            });
        Self {
            namespace: meta.namespace.clone().unwrap_or_default(),
            name: meta.name.clone().unwrap_or_default(),
            uid: meta.uid.clone(),
            cpu_request: shape.declares_cpu_request,
            memory_request: shape.declares_memory_request,
            cpu_limit: shape.declares_cpu_limit,
            memory_limit: shape.declares_memory_limit,
            volume_claims,
            replicas: replicas.map(|r| u32::try_from(r.max(0)).unwrap_or(0)),
        }
    }
}

impl From<Deployment> for ObservedWorkload {
    fn from(w: Deployment) -> Self {
        let tpl = w.spec.as_ref().map(|s| &s.template);
        let replicas = w.spec.as_ref().and_then(|s| s.replicas);
        Self::build(&w.metadata, tpl, replicas, false)
    }
}

impl From<StatefulSet> for ObservedWorkload {
    fn from(w: StatefulSet) -> Self {
        let tpl = w.spec.as_ref().map(|s| &s.template);
        let replicas = w.spec.as_ref().and_then(|s| s.replicas);
        // A StatefulSet's storage is its volumeClaimTemplates, not a pod volume —
        // reading only pod volumes would leave every StatefulSet unbanded on the
        // storage dimension.
        let claims = w
            .spec
            .as_ref()
            .and_then(|s| s.volume_claim_templates.as_ref())
            .is_some_and(|v| !v.is_empty());
        Self::build(&w.metadata, tpl, replicas, claims)
    }
}

impl From<DaemonSet> for ObservedWorkload {
    fn from(w: DaemonSet) -> Self {
        let tpl = w.spec.as_ref().map(|s| &s.template);
        // A DaemonSet's pod count is a function of the node set, never an
        // operator-settable field: leaving this None is what keeps the derivation
        // from ever planning a ReplicaBand for one.
        Self::build(&w.metadata, tpl, None, false)
    }
}
