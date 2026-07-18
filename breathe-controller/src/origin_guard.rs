//! The membership-CLOSING peer of `node_forma`'s correnteza M0 claim family.
//!
//! `node_forma::claim_unassigned_node_for_pool` OPENS membership: on a `Grew`
//! tick it claims one Ready, unclaimed node INTO a `BreatheCloudPool`.
//! `origin_guard` is the CLOSING half: it PROTECTS a named node — Camelot's
//! origin/control-plane node is the first, driving use — by keeping it
//! tainted against every workload except an explicit allowlist.
//! theory/CORRENTEZA.md §4/§11.3 already names this shape as a degenerate
//! N=1 instance of the not-yet-built generic `IsolationBand`; this module is
//! exactly that CR's reconciler.
//!
//! Reconciles UNCONDITIONALLY every tick (a standing PROTECT posture, unlike
//! the claim path which fires only on `Grew`) — gated by the SAME
//! `dryRun`/`writeEnabled` shadow convention every other band uses:
//! `effective dry_run = cr.spec.dry_run || !cr.spec.write_enabled`.
//!
//! Two things this module deliberately does NOT do:
//! - it does not stamp `node-role.kubernetes.io/control-plane` (or any other
//!   label) — that is an ecosystem-tool-recognition convenience an operator
//!   applies out of band to the SPECIFIC node they've decided is the origin;
//!   baking it into a GENERIC `IsolationBand` reconciler would be wrong for
//!   every non-origin use of this CRD kind.
//! - it does not enforce the allowlist — `unauthorized_pods` is OBSERVATION
//!   ONLY (only-mitigated / C2 tier, the same ceiling
//!   `breathe-lifecycle::OrphanTracker` names for itself: a live fact about
//!   cluster state). A pod carrying a wildcard `Toleration{operator: Exists,
//!   key: None}` still bypasses the taint entirely; this reconciler REPORTS
//!   that, it does not — and structurally cannot, from user-space RBAC alone
//!   — prevent it. The `ValidatingAdmissionPolicy` that would reject such
//!   scheduling at admission time is a named DESTINATION, not this module.
#![allow(clippy::doc_markdown)]

use std::sync::Arc;

use breathe_crd::{IsolationBand, IsolationBandStatus, TaintSpec, WorkloadRef};
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    api::{Api, ListParams, Patch, PatchParams},
    runtime::controller::Action,
    Client, ResourceExt,
};
use metrics::{counter, gauge};
use tracing::{debug, info, warn};

use crate::node_forma::upsert_taint;
use crate::{Ctx, Error};

/// PURE: does `node` already carry a taint matching `key`/`value`/`effect`
/// exactly? The idempotency check for a re-apply — the origin-guard peer of
/// `node_forma::is_kwok_managed` / `is_claim_candidate`.
fn has_taint(node: &Node, key: &str, value: Option<&str>, effect: &str) -> bool {
    node.spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .is_some_and(|taints| taints.iter().any(|t| t.key == key && t.value.as_deref() == value && t.effect == effect))
}

/// The outcome of ensuring ONE target node carries the band's taint this tick.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TaintOutcome {
    /// The node already carried the taint — nothing to do.
    AlreadyTainted,
    /// Shadow: the node WOULD be tainted this tick. Mutates nothing.
    WouldTaint,
    /// Live: the node WAS tainted this tick.
    Tainted,
    /// The named node does not exist (or a transient list/get error) —
    /// non-fatal, logged, retried next tick.
    NodeNotFound,
    /// Live: a patch was attempted and failed — non-fatal, retried next tick.
    TaintFailed,
}

/// The metrics-label outcome for `breathe_isolation_taint_total`.
fn taint_outcome_label(o: &TaintOutcome) -> &'static str {
    match o {
        TaintOutcome::AlreadyTainted => "already_tainted",
        TaintOutcome::WouldTaint => "would_taint",
        TaintOutcome::Tainted => "tainted",
        TaintOutcome::NodeNotFound => "node_not_found",
        TaintOutcome::TaintFailed => "taint_failed",
    }
}

/// Ensure `node_name` carries `taint`. Reads the live node; if it is already
/// tainted, does nothing (idempotent — no duplicate entry, no needless
/// patch); otherwise, ONLY when `!dry_run`, preserves every OTHER existing
/// taint via [`upsert_taint`] (a k8s JSON merge patch REPLACES the whole
/// `spec.taints` list) and patches.
async fn ensure_taint(client: &Client, node_name: &str, taint: &TaintSpec, dry_run: bool) -> TaintOutcome {
    let api = Api::<Node>::all(client.clone());
    let node = match api.get_opt(node_name).await {
        Ok(Some(n)) => n,
        Ok(None) => {
            warn!(node = node_name, "IsolationBand: target node not found (non-fatal; retried next tick)");
            return TaintOutcome::NodeNotFound;
        }
        Err(e) => {
            warn!(node = node_name, error = %e, "IsolationBand: node get failed (non-fatal; retried next tick)");
            return TaintOutcome::NodeNotFound;
        }
    };
    if has_taint(&node, &taint.key, taint.value.as_deref(), &taint.effect) {
        return TaintOutcome::AlreadyTainted;
    }
    if dry_run {
        return TaintOutcome::WouldTaint;
    }
    let existing = node.spec.as_ref().and_then(|s| s.taints.clone()).unwrap_or_default();
    let taints = upsert_taint(&existing, &taint.key, taint.value.as_deref(), &taint.effect);
    let patch = serde_json::json!({ "spec": { "taints": taints } });
    match api.patch(node_name, &PatchParams::default(), &Patch::Merge(&patch)).await {
        Ok(_) => {
            info!(node = node_name, key = %taint.key, effect = %taint.effect, "IsolationBand: tainted node");
            TaintOutcome::Tainted
        }
        Err(e) => {
            warn!(node = node_name, error = %e, "IsolationBand: taint patch failed (non-fatal; retried next tick)");
            TaintOutcome::TaintFailed
        }
    }
}

/// Does `owner_name` follow Kubernetes' ReplicaSet-for-a-Deployment naming
/// shape `<deployment>-<pod-template-hash>` for `deployment_name`? The
/// pod-template-hash is always a SINGLE alphanumeric segment (no further
/// dashes) — requiring that closes the false-match a bare prefix check would
/// open: `"pangea-operator-canary-7f8b9c".starts_with("pangea-operator-")`
/// is true, which would wrongly authorize a DIFFERENT deployment
/// (`pangea-operator-canary`) under a `pangea-operator` allowlist entry.
/// Requiring the remainder to be dash-free rejects that case while still
/// matching the real shape (`"pangea-operator-7f8b9c"` → remainder `"7f8b9c"`).
fn is_replicaset_of(owner_name: &str, deployment_name: &str) -> bool {
    owner_name
        .strip_prefix(deployment_name)
        .and_then(|rest| rest.strip_prefix('-'))
        .is_some_and(|hash| !hash.is_empty() && !hash.contains('-'))
}

/// PURE (tested): is a pod on a protected node AUTHORIZED, given its
/// namespace, its own name, and its owner references' names, against the
/// band's `allowed_workloads`? Matches either (a) the pod's own name
/// (bare/unmanaged pods, and DaemonSet/StatefulSet-owned pods whose DIRECT
/// owner name equals the allowed name) or (b) an owner reference name
/// exactly equal to, or [`is_replicaset_of`], the allowed name — so a
/// Deployment's `WorkloadRef` authorizes its pods without an extra apiserver
/// hop to resolve the ReplicaSet's own owner.
pub(crate) fn is_authorized_pod(
    pod_namespace: &str,
    pod_name: &str,
    owner_names: &[String],
    allowed: &[WorkloadRef],
) -> bool {
    allowed.iter().any(|w| {
        w.namespace == pod_namespace
            && (pod_name == w.name || owner_names.iter().any(|o| *o == w.name || is_replicaset_of(o, &w.name)))
    })
}

/// List every pod placed on `node_name` (via the `spec.nodeName` field
/// selector — no client-side scan of the whole cluster) and return
/// `"<namespace>/<pod-name>"` for each that [`is_authorized_pod`] rejects.
/// OBSERVATION ONLY — mutates nothing. Best-effort: a list error logs +
/// yields an empty (i.e. "found nothing wrong") result rather than a status
/// that misreports a violation the reconcile could not actually observe.
async fn unauthorized_pods_on(client: &Client, node_name: &str, allowed: &[WorkloadRef]) -> Vec<String> {
    let lp = ListParams::default().fields(&format!("spec.nodeName={node_name}"));
    let pods = match Api::<Pod>::all(client.clone()).list(&lp).await {
        Ok(l) => l.items,
        Err(e) => {
            warn!(node = node_name, error = %e, "IsolationBand: pod list failed (non-fatal; retried next tick)");
            return Vec::new();
        }
    };
    pods.iter()
        .filter_map(|p| {
            let ns = p.namespace().unwrap_or_default();
            let name = p.name_any();
            let owners: Vec<String> = p
                .metadata
                .owner_references
                .as_ref()
                .map(|refs| refs.iter().map(|r| r.name.clone()).collect())
                .unwrap_or_default();
            (!is_authorized_pod(&ns, &name, &owners, allowed)).then(|| format!("{ns}/{name}"))
        })
        .collect()
}

/// PURE (tested): map this tick's per-node outcomes onto the typed
/// `IsolationBandStatus`. The origin-guard peer of `node_forma::cloud_pool_status`.
#[must_use]
pub(crate) fn isolation_band_status(nodes_tainted: i64, unauthorized: &[String], dry_run: bool) -> IsolationBandStatus {
    IsolationBandStatus {
        phase: Some(if unauthorized.is_empty() { "Protecting".into() } else { "Violated".into() }),
        nodes_tainted: Some(nodes_tainted),
        unauthorized_pods: unauthorized.to_vec(),
        unauthorized_count: Some(unauthorized.len() as i64),
        effective_dry_run: Some(dry_run),
        last_seen_epoch: Some(breathe_runtime::now_secs()),
    }
}

/// Reconcile ONE `IsolationBand`: for every `spec.targetNodes` entry, ensure
/// the taint (shadow-gated) and observe unauthorized occupants (unconditional
/// — observation mutates nothing, so it runs even in shadow). Unlike
/// `node_forma::reconcile_cloud_pool` (which acts only on a `Grew` tick),
/// this reconcile runs the SAME protect-and-observe pass every tick — a
/// standing posture, not an event response.
pub async fn reconcile_isolation_band(cr: Arc<IsolationBand>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let name = cr.name_any();
    // Threaded through the SAME two-key `outorga::PromotionPolicy::decide` every
    // `Band` uses (`breathe_crd::legacy_effective_dry_run` — see its doc for the
    // full migration note; `IsolationBand` has no `mode` field or Ready/Stale/
    // Conflict status yet, so it rides the pure two-state Shadow/Effect arm).
    let promotion = breathe_crd::legacy_effective_dry_run(cr.spec.dry_run, !cr.spec.write_enabled);
    let dry_run = promotion.is_shadow();
    if let Some(reason) = promotion.shadow_reason() {
        debug!(band = %name, reason = ?reason, "IsolationBand: held in shadow");
    }

    let mut nodes_tainted: i64 = 0;
    let mut unauthorized: Vec<String> = Vec::new();
    for node_name in &cr.spec.target_nodes {
        let outcome = ensure_taint(&ctx.client, node_name, &cr.spec.taint, dry_run).await;
        counter!(
            "breathe_isolation_taint_total",
            "band" => name.clone(), "node" => node_name.clone(), "outcome" => taint_outcome_label(&outcome)
        )
        .increment(1);
        if matches!(outcome, TaintOutcome::AlreadyTainted | TaintOutcome::Tainted) {
            nodes_tainted += 1;
        }
        let mut found = unauthorized_pods_on(&ctx.client, node_name, &cr.spec.allowed_workloads).await;
        unauthorized.append(&mut found);
    }

    gauge!("breathe_isolation_nodes_tainted", "band" => name.clone()).set(nodes_tainted as f64);
    gauge!("breathe_isolation_unauthorized_pods", "band" => name.clone()).set(unauthorized.len() as f64);

    let status = isolation_band_status(nodes_tainted, &unauthorized, dry_run);
    info!(
        band = %name, nodes_tainted, unauthorized = unauthorized.len(), dry_run,
        "IsolationBand reconciled"
    );
    patch_status(&ctx.client, &name, &status).await;

    Ok(Action::requeue(ctx.requeue))
}

/// Patch an `IsolationBand`'s `.status` (cluster-scoped, status subresource).
/// Non-fatal — a failed patch logs + continues (status is observability).
async fn patch_status(client: &Client, name: &str, status: &IsolationBandStatus) {
    let api: Api<IsolationBand> = Api::all(client.clone());
    let patch = serde_json::json!({ "status": status });
    if let Err(e) = api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch)).await {
        warn!(band = %name, error = %e, "IsolationBand status patch failed (non-fatal)");
    }
}

/// Error policy for the isolation-band controller — back off + requeue.
pub fn error_policy_isolation_band(_cr: Arc<IsolationBand>, err: &Error, ctx: Arc<Ctx>) -> Action {
    warn!(error = %err, "IsolationBand reconcile error — backing off");
    Action::requeue(ctx.requeue)
}

#[cfg(test)]
mod tests {
    use super::{has_taint, is_authorized_pod, is_replicaset_of, isolation_band_status, taint_outcome_label, TaintOutcome};
    use k8s_openapi::api::core::v1::{Node, NodeSpec, Taint};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn node_with_taints(taints: Vec<Taint>) -> Node {
        Node {
            metadata: ObjectMeta::default(),
            spec: Some(NodeSpec { taints: (!taints.is_empty()).then_some(taints), ..Default::default() }),
            status: None,
        }
    }

    fn t(key: &str, value: Option<&str>, effect: &str) -> Taint {
        Taint { key: key.to_string(), value: value.map(str::to_string), effect: effect.to_string(), ..Default::default() }
    }

    fn wref(ns: &str, name: &str) -> breathe_crd::WorkloadRef {
        breathe_crd::WorkloadRef { namespace: ns.to_string(), name: name.to_string() }
    }

    // ── has_taint ──────────────────────────────────────────────────────────

    #[test]
    fn has_taint_matches_key_value_and_effect_exactly() {
        let node = node_with_taints(vec![t("breathe.pleme.io/origin-reserved", None, "NoSchedule")]);
        assert!(has_taint(&node, "breathe.pleme.io/origin-reserved", None, "NoSchedule"));
        assert!(!has_taint(&node, "breathe.pleme.io/origin-reserved", None, "NoExecute"), "effect must match exactly");
        assert!(!has_taint(&node, "some-other-key", None, "NoSchedule"), "key must match exactly");
    }

    #[test]
    fn has_taint_is_false_on_a_bare_node() {
        let node = node_with_taints(vec![]);
        assert!(!has_taint(&node, "breathe.pleme.io/origin-reserved", None, "NoSchedule"));
    }

    #[test]
    fn has_taint_distinguishes_by_value() {
        let node = node_with_taints(vec![t("dedicated", Some("gpu"), "NoSchedule")]);
        assert!(has_taint(&node, "dedicated", Some("gpu"), "NoSchedule"));
        assert!(!has_taint(&node, "dedicated", Some("cpu"), "NoSchedule"), "a differing value is a different taint");
        assert!(!has_taint(&node, "dedicated", None, "NoSchedule"), "a valueless probe does not match a valued taint");
    }

    // ── is_replicaset_of ───────────────────────────────────────────────────

    #[test]
    fn replicaset_of_matches_the_single_hash_segment_shape() {
        assert!(is_replicaset_of("pangea-operator-7f8b9c", "pangea-operator"));
        assert!(!is_replicaset_of("pangea-operator", "pangea-operator"), "no trailing hash at all is not a ReplicaSet name");
        assert!(!is_replicaset_of("pangea-operator-", "pangea-operator"), "an empty hash segment is not a ReplicaSet name");
    }

    #[test]
    fn replicaset_of_rejects_a_different_deployment_sharing_a_name_prefix() {
        // THE false-match this function exists to close: a bare `starts_with`
        // check would wrongly treat "pangea-operator-canary"'s ReplicaSet as
        // belonging to the "pangea-operator" Deployment.
        assert!(!is_replicaset_of("pangea-operator-canary-7f8b9c", "pangea-operator"));
    }

    // ── is_authorized_pod ─────────────────────────────────────────────────

    #[test]
    fn a_bare_pod_matches_by_its_own_name() {
        let allowed = vec![wref("breathe-system", "breathe-controller")];
        assert!(is_authorized_pod("breathe-system", "breathe-controller", &[], &allowed));
        assert!(!is_authorized_pod("breathe-system", "breathe-controller-2", &[], &allowed), "no owner + a differing bare name is unauthorized");
    }

    #[test]
    fn a_daemonset_pod_matches_by_its_direct_owner_name() {
        // A DaemonSet-owned pod's own name carries a random suffix
        // (`cilium-abcde`); the owner reference name is the DaemonSet's exact
        // name (`cilium`) — no ReplicaSet hop in between.
        let allowed = vec![wref("kube-system", "cilium")];
        assert!(is_authorized_pod("kube-system", "cilium-abcde", &["cilium".to_string()], &allowed));
    }

    #[test]
    fn a_deployment_pod_matches_via_the_replicaset_prefix_shape() {
        // pangea-operator's pod is owned by a ReplicaSet named
        // "pangea-operator-<hash>" — the Deployment's own name is never a
        // direct owner reference. The prefix match closes that hop without
        // an extra apiserver call to resolve the ReplicaSet's own owner.
        let allowed = vec![wref("camelot", "pangea-operator")];
        assert!(is_authorized_pod("camelot", "pangea-operator-7f8b9c-x2z9k", &["pangea-operator-7f8b9c".to_string()], &allowed));
        // A DIFFERENT deployment sharing a name prefix must NOT false-match
        // ("pangea-operator-canary" is not "pangea-operator").
        let not_allowed = vec![wref("camelot", "pangea-operator")];
        assert!(!is_authorized_pod("camelot", "pangea-operator-canary-abcde", &["pangea-operator-canary-7f8b9c".to_string()], &not_allowed));
    }

    #[test]
    fn namespace_must_match_even_if_the_name_matches() {
        let allowed = vec![wref("kube-system", "cilium")];
        assert!(!is_authorized_pod("camelot", "cilium-abcde", &["cilium".to_string()], &allowed), "a same-named workload in a different namespace is not the allowed one");
    }

    #[test]
    fn an_empty_allowlist_authorizes_nothing() {
        assert!(!is_authorized_pod("kube-system", "anything", &["anything".to_string()], &[]));
    }

    // ── isolation_band_status ─────────────────────────────────────────────

    #[test]
    fn status_is_protecting_when_no_unauthorized_pods() {
        let s = isolation_band_status(1, &[], true);
        assert_eq!(s.phase.as_deref(), Some("Protecting"));
        assert_eq!(s.nodes_tainted, Some(1));
        assert_eq!(s.unauthorized_count, Some(0));
        assert!(s.unauthorized_pods.is_empty());
        assert_eq!(s.effective_dry_run, Some(true));
    }

    #[test]
    fn status_is_violated_when_unauthorized_pods_are_found() {
        let found = vec!["default/stray-pod".to_string()];
        let s = isolation_band_status(1, &found, false);
        assert_eq!(s.phase.as_deref(), Some("Violated"));
        assert_eq!(s.unauthorized_count, Some(1));
        assert_eq!(s.unauthorized_pods, found);
        assert_eq!(s.effective_dry_run, Some(false));
    }

    // ── taint_outcome_label ────────────────────────────────────────────────

    #[test]
    fn taint_outcome_labels_are_distinct() {
        let labels: std::collections::HashSet<&str> = [
            taint_outcome_label(&TaintOutcome::AlreadyTainted),
            taint_outcome_label(&TaintOutcome::WouldTaint),
            taint_outcome_label(&TaintOutcome::Tainted),
            taint_outcome_label(&TaintOutcome::NodeNotFound),
            taint_outcome_label(&TaintOutcome::TaintFailed),
        ]
        .into_iter()
        .collect();
        assert_eq!(labels.len(), 5, "every taint outcome gets a distinct metric label");
    }
}
