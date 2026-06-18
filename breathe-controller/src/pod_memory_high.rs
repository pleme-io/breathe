//! Controller side of the SOFT-k8s-carve hand-off (`docs/OOM-VERIFICATION.md` §
//! Part 1). A `MemoryBand` efficiency carve must NOT lower the pod's k8s
//! `limits.memory` (`memory.max`, HARD/kill) — it writes the pod's cgroup-v2
//! `memory.high` (SOFT/reclaim) instead. The DECISION is the controller's (it reads
//! the pod working set via metrics-server, runs the band law, and pins the HARD
//! `memory.max` at the never-OOM peak ceiling while routing the efficiency pressure
//! to the SOFT plane via `breathe_control::plan_k8s_memory_carve`). The WRITE is the
//! host-agent's (it owns the node's cgroup files). This module is the controller
//! half: resolve the live pods' cgroup coordinates + emit one `PodMemoryHigh`
//! dispatch CR per pod carrying the soft target, which the host-agent reconciles.
//!
//! `tier-honest` (`theory/UNREPRESENTABILITY.md` §II): the dispatch-BUILDER
//! ([`build_pod_memory_high_dispatch`]) is PURE + library-tested — it can never
//! emit a HARD value (it carries only the SOFT `desiredBytes`, the typed
//! `K8sMemoryCarve.soft_target`), so a dispatch is OOM-impossible by construction.
//! The end-to-end live convergence — the controller SSA-applying the CR, the
//! apiserver storing it, the host-agent reconciling the cgroup write on the node —
//! needs the LIVE cluster (`pending-deploy`); only the build is unrep-at-library.

use breathe_control::{plan_k8s_memory_carve, BandConfig, BandLaw};
use breathe_crd::{CgroupDriverSpec, MemoryBand, PodMemoryHigh, PodMemoryHighSpec};
use breathe_kube::{KubeCluster, PodCgroupCoords};
use breathe_provider::Target;
use kube::{
    api::{Api, Patch, PatchParams},
    ResourceExt,
};

/// Build the typed `PodMemoryHigh` DISPATCH CR for ONE managed pod — the pure
/// controller→host-agent hand-off (no cluster). The CR carries ONLY the SOFT
/// `memory.high` target (`soft_bytes`, the typed `K8sMemoryCarve.soft_target`), the
/// pod's resolved cgroup coordinates, and the node that hosts it; it has NO way to
/// express a HARD `memory.max` write, so a dispatch can never lower the kill ceiling.
///
/// `name` is the stable dispatch CR name (controller-owned; derived from the band +
/// pod so re-applies are idempotent). `node_name` is where the pod runs (the
/// host-agent node-match). `owner_band` is `namespace/name` of the source MemoryBand.
#[must_use]
pub fn build_pod_memory_high_dispatch(
    name: &str,
    node_name: &str,
    coords: &PodCgroupCoords,
    driver: CgroupDriverSpec,
    soft_bytes: u64,
    owner_band: &str,
    dry_run: bool,
) -> PodMemoryHigh {
    PodMemoryHigh::new(
        name,
        PodMemoryHighSpec {
            node_name: node_name.to_string(),
            qos_class: coords.qos.clone(),
            pod_uid: coords.pod_uid.clone(),
            container_runtime_id: coords.container_runtime_id.clone(),
            cgroup_driver: driver,
            desired_bytes: soft_bytes,
            owner_band: Some(owner_band.to_string()),
            dry_run,
        },
    )
}

/// The stable, idempotent dispatch CR name for a `(band, pod_uid)` pair — so the
/// controller re-applies (SSA) the SAME object every tick rather than spawning a
/// new one. Cluster-scoped names must be DNS-1123; the pod UID is already DNS-safe,
/// the band name is, and we join with `-` and truncate defensively. Pure.
#[must_use]
pub fn dispatch_name(band_name: &str, pod_uid: &str) -> String {
    let mut n = String::from("pmh-");
    n.push_str(band_name);
    n.push('-');
    // a UID is 36 chars; keep it whole (DNS-1123 max is 253, well within bound).
    n.push_str(pod_uid);
    n.truncate(253);
    n
}

/// Compute the SOFT `memory.high` target for a MemoryBand tick from its observed
/// status + config — the pure routing the dispatch carries. `live_soft` is the
/// pod's current `memory.high` (`u64::MAX` when unset). Returns `Some(bytes)` when
/// the efficiency carve warrants a soft write, `None` for an in-band hold / refused
/// shrink. Reuses `plan_k8s_memory_carve` so the HARD `memory.max` is never touched.
#[must_use]
pub fn soft_target_for(used: u64, peak_used: u64, hard_current: u64, live_soft: u64, cfg: &BandConfig) -> Option<u64> {
    plan_k8s_memory_carve(&BandLaw, used, peak_used, hard_current, live_soft, cfg).soft_target
}

/// **The controller→host-agent SOFT-carve hand-off (live).** For a `MemoryBand`
/// whose efficiency carve warrants reclaiming `memory.high`, resolve every managed
/// pod's `(coords, node)` and SSA-apply one idempotent `PodMemoryHigh` dispatch per
/// pod carrying the soft target. The host-agent on each node reconciles the cgroup
/// write; the k8s `limits.memory` (`memory.max`, HARD) is NEVER touched here.
///
/// `soft_bytes` is the routed soft target (from [`soft_target_for`] — already
/// `K8sMemoryCarve.soft_target`, so it can never be a HARD value). `dry_run` mirrors
/// the band's effective shadow state so a shadow band only declares (the agent
/// observes). Returns the number of dispatches applied.
///
/// `tier-honest`: this runs against the LIVE apiserver (`pending-deploy`) — the pod
/// list, the SSA apply, and the downstream host-agent cgroup write all need the
/// cluster. The dispatch payload it builds is the pure, library-tested
/// [`build_pod_memory_high_dispatch`].
///
/// # Errors
/// A `kube::Error` if listing the pods or applying a dispatch fails.
pub async fn ensure_soft_carve_dispatch(
    client: &kube::Client,
    cluster: &KubeCluster,
    band: &MemoryBand,
    target: &Target,
    driver: CgroupDriverSpec,
    soft_bytes: u64,
    dry_run: bool,
) -> Result<usize, kube::Error> {
    let band_name = band.name_any();
    let band_ns = band.namespace().unwrap_or_default();
    let owner = {
        let mut s = band_ns.clone();
        s.push('/');
        s.push_str(&band_name);
        s
    };
    let targets = match cluster.resolve_pod_soft_carve_targets(target).await {
        Ok(t) => t,
        // a dormant / unscheduled / not-yet-running pod set is benign — nothing to
        // dispatch this tick (the host/cgroup never-OOM path still protects via the
        // unchanged HARD memory.max). Surface as zero dispatches, not an error.
        Err(_) => return Ok(0),
    };
    let api: Api<PodMemoryHigh> = Api::all(client.clone());
    let mut applied = 0usize;
    for (coords, node) in targets {
        let name = dispatch_name(&band_name, &coords.pod_uid);
        let dispatch = build_pod_memory_high_dispatch(&name, &node, &coords, driver, soft_bytes, &owner, dry_run);
        // idempotent SSA — the controller owns the dispatch field manager; re-applying
        // the same (band, pod) updates the desired bytes in place, never spawns dupes.
        api.patch(&name, &PatchParams::apply("breathe/soft-carve"), &Patch::Apply(&dispatch)).await?;
        applied += 1;
    }
    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coords() -> PodCgroupCoords {
        PodCgroupCoords {
            qos: "Burstable".into(),
            pod_uid: "abc12345-6789-def0-1234-56789abcdef0".into(),
            container_runtime_id: "containerd://deadbeefcafe".into(),
        }
    }

    #[test]
    fn dispatch_carries_only_the_soft_target_never_a_hard_value() {
        let pmh = build_pod_memory_high_dispatch(
            "pmh-authentik-worker-mem-pod1",
            "rio",
            &coords(),
            CgroupDriverSpec::Systemd,
            448 * 1024 * 1024, // 448Mi soft reclaim seat
            "authentik/authentik-worker-memory",
            false,
        );
        assert_eq!(pmh.spec.node_name, "rio");
        assert_eq!(pmh.spec.desired_bytes, 448 * 1024 * 1024);
        assert_eq!(pmh.spec.qos_class, "Burstable");
        assert_eq!(pmh.spec.pod_uid, "abc12345-6789-def0-1234-56789abcdef0");
        assert_eq!(pmh.spec.container_runtime_id, "containerd://deadbeefcafe");
        assert_eq!(pmh.spec.owner_band.as_deref(), Some("authentik/authentik-worker-memory"));
        // the dispatch maps to the SOFT pod-memory.high knob — NEVER memory.max.
        match pmh.spec.provider_knob() {
            breathe_provider::HostKnob::PodCgroupMemoryHigh { driver, .. } => {
                assert_eq!(driver, breathe_provider::CgroupDriver::Systemd);
            }
            other => panic!("a dispatch must map to the SOFT memory.high knob, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_carries_the_chosen_cgroup_driver() {
        let pmh = build_pod_memory_high_dispatch("n", "rio", &coords(), CgroupDriverSpec::Cgroupfs, 1024, "b/m", false);
        assert_eq!(pmh.spec.cgroup_driver, CgroupDriverSpec::Cgroupfs);
        assert!(matches!(
            pmh.spec.provider_knob(),
            breathe_provider::HostKnob::PodCgroupMemoryHigh { driver: breathe_provider::CgroupDriver::Cgroupfs, .. }
        ));
    }

    #[test]
    fn dispatch_name_is_stable_and_idempotent_per_band_and_pod() {
        let a = dispatch_name("authentik-worker-memory", "uid-1");
        let b = dispatch_name("authentik-worker-memory", "uid-1");
        assert_eq!(a, b, "the same (band, pod) yields the same dispatch name (idempotent SSA)");
        assert_ne!(a, dispatch_name("authentik-worker-memory", "uid-2"), "a different pod is a different dispatch");
        assert!(a.len() <= 253, "the dispatch name must be a valid DNS-1123 object name");
    }

    #[test]
    fn dry_run_dispatch_is_marked_shadow() {
        let pmh = build_pod_memory_high_dispatch("n", "rio", &coords(), CgroupDriverSpec::Systemd, 1024, "b/m", true);
        assert!(pmh.spec.dry_run, "a shadow dispatch carries dryRun so the agent observes, never writes");
    }

    #[test]
    fn soft_target_routes_an_efficiency_carve_and_never_a_hard_value() {
        let cfg = BandConfig::default();
        const MI: u64 = 1 << 20;
        const GI: u64 = 1 << 30;
        // idle 400Mi @ 2Gi hard, 2Gi soft ⇒ an efficiency carve reclaims memory.high.
        let t = soft_target_for(400 * MI, 400 * MI, 2 * GI, 2 * GI, &cfg);
        assert!(t.is_some_and(|v| v < 2 * GI), "an efficiency carve dispatches a tighter soft target");
        // in-band (util ~0.78 @ 1Gi) ⇒ no dispatch (hold).
        assert_eq!(soft_target_for(800 * MI, 800 * MI, GI, GI, &cfg), None);
        // an unset soft cgroup (u64::MAX) on an idle pod snaps down to a real target.
        assert!(soft_target_for(400 * MI, 400 * MI, 2 * GI, u64::MAX, &cfg).is_some());
    }
}
