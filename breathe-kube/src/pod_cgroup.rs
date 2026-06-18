//! Pod → cgroup-v2 coordinate resolution — the apiserver side of the Part-1 SOFT
//! k8s carve (`docs/OOM-VERIFICATION.md` § Part 1). A `MemoryBand` targets a
//! workload; its efficiency carve must write the live pod's `memory.high` (SOFT,
//! reclaim) cgroup file, NOT the k8s `limits.memory` (`memory.max`, HARD/kill).
//! To do that the host-agent needs the pod's cgroup-path COORDINATES — the QoS
//! class (which kubepods subtree), the pod UID, and the CRI container-runtime id.
//!
//! This module is the **pure** extraction of those coordinates from a live pod's
//! JSON `status` (`status.qosClass`, `status.containerStatuses[].containerID`) +
//! `metadata.uid`. The extraction is a total typed function over a pod value: a
//! missing/garbled field is a typed [`PodCoordError`], never a silent wrong path.
//! The only impure edge — listing the live pods off the apiserver — lives in
//! [`crate::kube_cluster::KubeCluster::resolve_pod_cgroup_coords`]; everything in
//! THIS module is exercised against pod JSON with zero cluster.
//!
//! `tier-honest` (per `theory/UNREPRESENTABILITY.md` §II): the coordinate
//! EXTRACTION is `parse-time-rejected` — a pod that doesn't carry a usable
//! `(uid, qos, containerID)` triple yields an `Err` at the parse boundary, before
//! any coordinate flows to a writer. The LIVE list against a real kubelet (and the
//! actual cgroup write on the node) stays `pending-deploy` — it needs the cluster.

use serde_json::Value;

/// The cgroup-path coordinates of ONE live pod's managed container — everything
/// the host-agent needs to address the pod's `memory.high` file, with NO cluster
/// dependency once extracted. The serde-mirror the controller hands to the
/// host-agent is `HostKnob::PodCgroupMemoryHigh`; this is the in-`breathe-kube`
/// typed result of resolving it from the live pod.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodCgroupCoords {
    /// `status.qosClass` (`Guaranteed`/`Burstable`/`BestEffort`) — which kubepods
    /// QoS subtree the pod's cgroup lives under.
    pub qos: String,
    /// `metadata.uid` — the pod UID the per-pod cgroup slice/dir is named for.
    pub pod_uid: String,
    /// `status.containerStatuses[<managed>].containerID` — the CRI container id
    /// (still carrying its `containerd://`/`cri-o://` scheme; the host-agent's
    /// path mapper scheme-strips it).
    pub container_runtime_id: String,
}

/// Why a pod's cgroup coordinates could not be resolved — a typed parse-boundary
/// rejection (never a silent wrong cgroup path). Each arm names the missing field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodCoordError {
    /// `metadata.uid` is absent — the pod hasn't been admitted/assigned a UID.
    NoPodUid,
    /// `status.qosClass` is absent — the pod hasn't been scheduled (no QoS yet).
    NoQosClass,
    /// No `status.containerStatuses` entry for the managed container carries a
    /// non-empty `containerID` — the container isn't running yet (no cgroup exists),
    /// so there is nothing to carve. A benign "not ready", surfaced typed.
    NoContainerId { container: Option<String> },
}

impl std::fmt::Display for PodCoordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoPodUid => f.write_str("pod has no metadata.uid"),
            Self::NoQosClass => f.write_str("pod has no status.qosClass (not yet scheduled)"),
            Self::NoContainerId { container } => {
                write!(f, "no running container with a containerID for {container:?} (container not started)")
            }
        }
    }
}

impl std::error::Error for PodCoordError {}

/// Extract a running container's CRI `containerID` from a pod's
/// `status.containerStatuses`. When `want` names a container, that container's id
/// is returned (and ONLY it — never a sibling's); when `want` is `None`, the FIRST
/// container status carrying a non-empty `containerID` is used. A status entry with
/// an empty/absent `containerID` is skipped (the container has no cgroup yet).
#[must_use]
pub fn container_id_from_status(pod: &Value, want: &Option<String>) -> Option<String> {
    let statuses = pod.pointer("/status/containerStatuses")?.as_array()?;
    let pick = |s: &Value| -> Option<String> {
        s.get("containerID").and_then(Value::as_str).filter(|id| !id.is_empty()).map(String::from)
    };
    match want {
        Some(name) => statuses
            .iter()
            .find(|s| s.get("name").and_then(Value::as_str) == Some(name.as_str()))
            .and_then(pick),
        None => statuses.iter().find_map(pick),
    }
}

/// The node a pod is scheduled on (`spec.nodeName`) — orthogonal to the cgroup
/// coordinates (the host-agent on THIS node owns the pod's cgroup file). `None`
/// until the pod is scheduled. Pure.
#[must_use]
pub fn node_name_from_pod(pod: &Value) -> Option<String> {
    pod.pointer("/spec/nodeName").and_then(Value::as_str).filter(|s| !s.is_empty()).map(String::from)
}

/// Resolve ONE pod's cgroup coordinates from its live JSON — the pure core of the
/// apiserver→host-agent routing. Reads `metadata.uid`, `status.qosClass`, and the
/// managed container's `status.containerStatuses[].containerID`; each missing field
/// is a typed [`PodCoordError`]. `container` is the band's managed container name
/// (`None` ⇒ first running container).
///
/// # Errors
/// A typed [`PodCoordError`] naming the first missing coordinate field.
pub fn pod_coords_from_value(pod: &Value, container: &Option<String>) -> Result<PodCgroupCoords, PodCoordError> {
    let pod_uid = pod
        .pointer("/metadata/uid")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .ok_or(PodCoordError::NoPodUid)?;
    let qos = pod
        .pointer("/status/qosClass")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .ok_or(PodCoordError::NoQosClass)?;
    let container_runtime_id =
        container_id_from_status(pod, container).ok_or_else(|| PodCoordError::NoContainerId { container: container.clone() })?;
    Ok(PodCgroupCoords { qos, pod_uid, container_runtime_id })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn burstable_pod() -> Value {
        json!({
            "metadata": { "uid": "abc12345-6789-def0-1234-56789abcdef0", "name": "authentik-worker-xyz" },
            "status": {
                "qosClass": "Burstable",
                "containerStatuses": [
                    { "name": "worker", "containerID": "containerd://deadbeefcafe" },
                    { "name": "istio-proxy", "containerID": "containerd://feedface" }
                ]
            }
        })
    }

    #[test]
    fn resolves_the_named_container_coords() {
        let coords = pod_coords_from_value(&burstable_pod(), &Some("worker".into())).unwrap();
        assert_eq!(
            coords,
            PodCgroupCoords {
                qos: "Burstable".into(),
                pod_uid: "abc12345-6789-def0-1234-56789abcdef0".into(),
                container_runtime_id: "containerd://deadbeefcafe".into(),
            }
        );
    }

    #[test]
    fn picks_a_specific_container_never_a_sibling() {
        // naming istio-proxy must return ITS id, not the worker's (no cross-container leak).
        let coords = pod_coords_from_value(&burstable_pod(), &Some("istio-proxy".into())).unwrap();
        assert_eq!(coords.container_runtime_id, "containerd://feedface");
    }

    #[test]
    fn none_container_takes_the_first_running_one() {
        let coords = pod_coords_from_value(&burstable_pod(), &None).unwrap();
        assert_eq!(coords.container_runtime_id, "containerd://deadbeefcafe");
    }

    #[test]
    fn cri_o_scheme_is_carried_through_for_the_host_agent_to_strip() {
        // the extractor does NOT scheme-strip — that's the host-agent path mapper's
        // job (so the coordinate stays the literal CRI id). cri-o:// rides through.
        let pod = json!({
            "metadata": { "uid": "u" },
            "status": { "qosClass": "Guaranteed", "containerStatuses": [ { "name": "c", "containerID": "cri-o://abc123" } ] }
        });
        assert_eq!(pod_coords_from_value(&pod, &Some("c".into())).unwrap().container_runtime_id, "cri-o://abc123");
    }

    #[test]
    fn a_missing_uid_is_a_typed_parse_rejection() {
        let pod = json!({ "status": { "qosClass": "Burstable", "containerStatuses": [ { "name": "c", "containerID": "containerd://x" } ] } });
        assert_eq!(pod_coords_from_value(&pod, &Some("c".into())), Err(PodCoordError::NoPodUid));
    }

    #[test]
    fn a_missing_qos_is_a_typed_parse_rejection() {
        let pod = json!({ "metadata": { "uid": "u" }, "status": { "containerStatuses": [ { "name": "c", "containerID": "containerd://x" } ] } });
        assert_eq!(pod_coords_from_value(&pod, &Some("c".into())), Err(PodCoordError::NoQosClass));
    }

    #[test]
    fn node_name_is_read_from_spec_node_name() {
        let pod = json!({ "spec": { "nodeName": "rio" }, "metadata": { "uid": "u" } });
        assert_eq!(node_name_from_pod(&pod), Some("rio".to_string()));
        // unscheduled pod ⇒ no node.
        assert_eq!(node_name_from_pod(&json!({ "metadata": { "uid": "u" } })), None);
        // empty nodeName is not a node.
        assert_eq!(node_name_from_pod(&json!({ "spec": { "nodeName": "" } })), None);
    }

    #[test]
    fn an_unstarted_container_is_a_typed_parse_rejection_never_a_wrong_path() {
        // a container with no (or empty) containerID has no cgroup yet — refused, not
        // resolved to a bogus path.
        let pod = json!({
            "metadata": { "uid": "u" },
            "status": { "qosClass": "Burstable", "containerStatuses": [ { "name": "c", "containerID": "" } ] }
        });
        assert_eq!(
            pod_coords_from_value(&pod, &Some("c".into())),
            Err(PodCoordError::NoContainerId { container: Some("c".into()) })
        );
        // a named container that doesn't exist is likewise refused.
        assert_eq!(
            pod_coords_from_value(&burstable_pod(), &Some("missing".into())),
            Err(PodCoordError::NoContainerId { container: Some("missing".into()) })
        );
    }
}
