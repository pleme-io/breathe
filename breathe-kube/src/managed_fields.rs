//! The pure `metadata.managedFields` → [`FieldOwner`] ownership parser.
//!
//! This is the real data source for breathe-control's *field-granular*
//! single-writer guard (BREATHE.md §15.3): given an object's `managedFields`
//! and the fieldsV1 path of the field a provider intends to write, it returns
//! every manager that already owns that exact field. No kube client, no
//! cluster — pure `serde_json`, fully unit-testable.
//!
//! Kubernetes encodes ownership in fieldsV1 with `f:<key>` segments and, for
//! keyed list entries (containers, keyed by name), a `k:{"name":"<c>"}` segment.
//! A manager "owns" a leaf iff that full nested path is present in its set.

use breathe_control::FieldOwner;
use serde_json::Value;

/// Walk one fieldsV1 object following `segments`; true iff the full path is present.
fn owns_path(fields_v1: &Value, segments: &[String]) -> bool {
    let mut cur = fields_v1;
    for seg in segments {
        match cur.get(seg) {
            Some(next) => cur = next,
            None => return false,
        }
    }
    true
}

/// From an object's `metadata.managedFields` array, the managers that own the
/// field reached by `segments`. `logical_field` is the dotted label stamped onto
/// each returned [`FieldOwner`] so it matches the provider's `owned_field().path`
/// for the guard's equality check (the fieldsV1 segments are the *encoding*; the
/// logical path is the *contract*).
#[must_use]
pub fn field_owners(managed_fields: &Value, segments: &[String], logical_field: &str) -> Vec<FieldOwner> {
    let Some(entries) = managed_fields.as_array() else {
        return Vec::new();
    };
    let mut owners = Vec::new();
    for e in entries {
        let Some(fv1) = e.get("fieldsV1") else { continue };
        if owns_path(fv1, segments) {
            let manager = e
                .get("manager")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            owners.push(FieldOwner { manager, field: logical_field.to_string() });
        }
    }
    owners
}

/// fieldsV1 segments for a container resource limit on a Deployment/StatefulSet
/// pod template (`spec.template.spec.containers[name].resources.limits.<res>`).
#[must_use]
pub fn pod_template_limit_segments(container: &str, resource: &str) -> Vec<String> {
    vec![
        "f:spec".into(),
        "f:template".into(),
        "f:spec".into(),
        "f:containers".into(),
        format!("k:{{\"name\":\"{container}\"}}"),
        "f:resources".into(),
        "f:limits".into(),
        format!("f:{resource}"),
    ]
}

/// fieldsV1 segments for a CNPG `Cluster`'s top-level resource limit
/// (`spec.resources.limits.<res>`) — the M0 anchor (pangea-database) patches here.
#[must_use]
pub fn cnpg_cluster_limit_segments(resource: &str) -> Vec<String> {
    vec![
        "f:spec".into(),
        "f:resources".into(),
        "f:limits".into(),
        format!("f:{resource}"),
    ]
}

/// fieldsV1 segments for `spec.replicas` (KEDA/HPA's field — what breathe yields).
#[must_use]
pub fn replicas_segments() -> Vec<String> {
    vec!["f:spec".into(), "f:replicas".into()]
}

/// fieldsV1 segments for a PVC's `spec.resources.requests.storage` (grow-only).
#[must_use]
pub fn pvc_request_segments() -> Vec<String> {
    vec![
        "f:spec".into(),
        "f:resources".into(),
        "f:requests".into(),
        "f:storage".into(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // A Deployment managedFields where `vpa-updater` owns the memory limit and
    // `flux`/`helm` own unrelated fields.
    fn deployment_managed_fields() -> Value {
        json!([
            {
                "manager": "flux",
                "fieldsV1": { "f:metadata": { "f:labels": { "f:app": {} } } }
            },
            {
                "manager": "vpa-updater",
                "fieldsV1": {
                    "f:spec": { "f:template": { "f:spec": { "f:containers": {
                        "k:{\"name\":\"app\"}": {
                            "f:resources": { "f:limits": { "f:memory": {}, "f:cpu": {} } }
                        }
                    }}}}
                }
            }
        ])
    }

    #[test]
    fn finds_the_manager_owning_the_memory_limit() {
        let mf = deployment_managed_fields();
        let segs = pod_template_limit_segments("app", "memory");
        let owners = field_owners(&mf, &segs, "resources.limits.memory");
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].manager, "vpa-updater");
        assert_eq!(owners[0].field, "resources.limits.memory");
    }

    #[test]
    fn distinguishes_memory_from_cpu_ownership() {
        // vpa owns BOTH cpu+memory here; a query for cpu still returns it,
        // and (critically) a query for a field nobody owns returns empty.
        let mf = deployment_managed_fields();
        assert_eq!(field_owners(&mf, &pod_template_limit_segments("app", "cpu"), "resources.limits.cpu").len(), 1);
        // ephemeral-storage is owned by no one
        assert!(field_owners(&mf, &pod_template_limit_segments("app", "ephemeral-storage"), "x").is_empty());
    }

    #[test]
    fn wrong_container_is_not_a_match() {
        // The keyed entry is k:{"name":"app"}; querying container "sidecar" misses.
        let mf = deployment_managed_fields();
        let owners = field_owners(&mf, &pod_template_limit_segments("sidecar", "memory"), "resources.limits.memory");
        assert!(owners.is_empty(), "container key must match exactly");
    }

    #[test]
    fn breathe_alone_owning_is_no_conflict_upstream() {
        // When breathe is the only owner of the memory limit, the guard (which
        // filters manager != ours) sees no competitor. Proven here at the data
        // layer: field_owners returns exactly breathe/memory.
        let mf = json!([{
            "manager": "breathe/memory",
            "fieldsV1": { "f:spec": { "f:template": { "f:spec": { "f:containers": {
                "k:{\"name\":\"app\"}": { "f:resources": { "f:limits": { "f:memory": {} } } }
            }}}}}
        }]);
        let owners = field_owners(&mf, &pod_template_limit_segments("app", "memory"), "resources.limits.memory");
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].manager, "breathe/memory");
    }

    #[test]
    fn cnpg_cluster_top_level_resources_path() {
        // The pangea-database anchor: CNPG Cluster.spec.resources.limits.memory.
        let mf = json!([{
            "manager": "cnpg-cloudnative-pg",
            "fieldsV1": { "f:spec": { "f:resources": { "f:limits": { "f:memory": {} } } } }
        }]);
        let owners = field_owners(&mf, &cnpg_cluster_limit_segments("memory"), "spec.resources.limits.memory");
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].manager, "cnpg-cloudnative-pg");
    }

    #[test]
    fn keda_owns_replicas_not_a_memory_competitor() {
        // The composition contract at the data layer: KEDA owns spec.replicas,
        // so a memory-limit query returns no KEDA owner.
        let mf = json!([{
            "manager": "keda-operator",
            "fieldsV1": { "f:spec": { "f:replicas": {} } }
        }]);
        assert!(field_owners(&mf, &pod_template_limit_segments("app", "memory"), "resources.limits.memory").is_empty());
        assert_eq!(field_owners(&mf, &replicas_segments(), "spec.replicas").len(), 1);
    }

    #[test]
    fn empty_or_missing_managed_fields_is_empty() {
        assert!(field_owners(&json!(null), &replicas_segments(), "x").is_empty());
        assert!(field_owners(&json!([]), &replicas_segments(), "x").is_empty());
        // an entry with no fieldsV1 is skipped, not panicked on
        assert!(field_owners(&json!([{"manager": "x"}]), &replicas_segments(), "x").is_empty());
    }
}
