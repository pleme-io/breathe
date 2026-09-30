//! The in-place pod resize (`pods/{name}/resize`), as pure functions.
//!
//! One home for the decisions an in-place resize makes, whoever performs the
//! I/O: breathe-kube's actuators over kube-rs, ensaio visita's reversible
//! fixture over kubectl.
//!
//! | question | function |
//! |---|---|
//! | will resizing `resource` restart the container? | [`resize_restart_policy`] (typed, three arms), [`restart_free`] (declared `NotRequired` only) |
//! | what does a LIMIT resize write, keeping the QoS class? | [`limit_block`] |
//! | what does a REQUEST resize write? | [`request_block`] |
//! | what body does the subresource take? | [`resize_body`] |
//! | what does the container declare now (the inverse)? | [`container_resources`] |
//! | has the kubelet applied it? | [`converged`] |
//! | a quantity as a number, and back | [`parse`], [`render`] |

use breathe_control::{Quantity, Unit};
use serde_json::{Value, json};

/// A k8s quantity for `resource` as its scalar (millicores for cpu, bytes
/// otherwise), by breathe's one parser. `None` when malformed.
#[must_use]
pub fn parse(resource: &str, q: &str) -> Option<u64> {
    Unit::for_resource(resource).parse(q)
}

/// The scalar rendered back to a k8s quantity for `resource`.
#[must_use]
pub fn render(resource: &str, value: u64) -> String {
    Quantity {
        value,
        unit: Unit::for_resource(resource),
    }
    .to_string()
}

fn container<'a>(pod: &'a Value, name: Option<&str>, at: &str) -> Option<&'a Value> {
    let list = pod.pointer(at)?.as_array()?;
    match name {
        Some(n) => list
            .iter()
            .find(|c| c.get("name").and_then(Value::as_str) == Some(n)),
        None => list.first(),
    }
}

/// What a container DECLARES about restarting when `resource` is resized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartPolicy {
    /// Declared `NotRequired`: resized in place, no restart (the runtime
    /// permitting).
    NotRequired,
    /// Declared `RestartContainer`: the resize restarts the container.
    RestartContainer,
    /// Nothing declared for this resource. The API default is `NotRequired`
    /// (k8s.io/api core/v1 `ContainerResizePolicy.RestartPolicy`: "If not
    /// specified, it defaults to `NotRequired`") — but it is not what the pod
    /// SAYS, and a caller that wants the declaration in writing must not read
    /// this arm as `NotRequired`.
    Unspecified,
}

/// The container's declared resize restart policy for `resource`. A missing
/// container or spec reads as [`RestartPolicy::Unspecified`].
#[must_use]
pub fn resize_restart_policy(pod: &Value, container_name: Option<&str>, resource: &str) -> RestartPolicy {
    let declared = container(pod, container_name, "/spec/containers")
        .and_then(|c| c.pointer("/resizePolicy"))
        .and_then(Value::as_array)
        .and_then(|policies| {
            policies
                .iter()
                .find(|p| p.get("resourceName").and_then(Value::as_str) == Some(resource))
                .map(|p| p.get("restartPolicy").and_then(Value::as_str).unwrap_or("NotRequired"))
        });
    match declared {
        Some("RestartContainer") => RestartPolicy::RestartContainer,
        Some(_) => RestartPolicy::NotRequired,
        None => RestartPolicy::Unspecified,
    }
}

/// True iff the container DECLARES `resizePolicy[<resource>] = NotRequired`.
///
/// Deliberately stricter than the API: an unspecified policy defaults to
/// `NotRequired` (see [`RestartPolicy::Unspecified`]), but this returns
/// `false` for it — breathe's actuators require the declaration in writing
/// before treating a shrink as restart-free. A caller that accepts the API
/// default reads [`resize_restart_policy`] instead. A missing container or
/// spec ⇒ false. `container: None` means the first one.
#[must_use]
pub fn restart_free(pod: &Value, container_name: Option<&str>, resource: &str) -> bool {
    resize_restart_policy(pod, container_name, resource) == RestartPolicy::NotRequired
}

/// The QoS-preserving `resources` block for an in-place LIMIT resize. A
/// Guaranteed pod (requests == limits) keeps requests == limits so it STAYS
/// Guaranteed (grow and shrink); a Burstable/BestEffort pod sets the limit and
/// clamps its request DOWN to the new limit only if the old request would now
/// exceed it (k8s rejects request > limit) — otherwise the request is left
/// untouched.
#[must_use]
pub fn limit_block(qos: &str, resource: &str, value: u64, current_request: Option<&str>) -> Value {
    let unit = Unit::for_resource(resource);
    let qty = Quantity { value, unit }.to_string();
    if qos == "Guaranteed" {
        return json!({ "limits": { resource: qty.clone() }, "requests": { resource: qty } });
    }
    match current_request.and_then(|r| unit.parse(r)) {
        Some(req) if req > value => {
            json!({ "limits": { resource: qty.clone() }, "requests": { resource: qty } })
        }
        _ => json!({ "limits": { resource: qty } }),
    }
}

/// The `resources` block for an in-place REQUEST resize: `requests` and
/// nothing else. Never touching `limits` is the safety property — moving the
/// other side of the pair is how a within-class change becomes an undeclared
/// QoS-class transition, which `ValidatePodResize` rejects.
#[must_use]
pub fn request_block(resource: &str, value: u64) -> Value {
    let qty = Quantity {
        value,
        unit: Unit::for_resource(resource),
    }
    .to_string();
    json!({ "requests": { resource: qty } })
}

/// The strategic-merge body `pods/{name}/resize` takes: one container's
/// `resources`.
#[must_use]
pub fn resize_body(container_name: &str, block: &Value) -> Value {
    json!({ "spec": { "containers": [ { "name": container_name, "resources": block } ] } })
}

/// The container's declared `spec.resources` — what an inverse puts back.
#[must_use]
pub fn container_resources(pod: &Value, container_name: &str) -> Option<Value> {
    container(pod, Some(container_name), "/spec/containers")
        .map(|c| c.get("resources").cloned().unwrap_or_else(|| json!({})))
}

/// Whether the kubelet has applied `block`: every quantity in it (under
/// `limits` and `requests`) equals, numerically, the one reported in
/// `status.containerStatuses[<container>].resources`. `spec` changing is the
/// request being accepted; `status` matching is the resize having happened.
/// An unparseable or absent quantity is not converged.
#[must_use]
pub fn converged(pod: &Value, container_name: &str, block: &Value) -> bool {
    let Some(status) = container(pod, Some(container_name), "/status/containerStatuses")
        .and_then(|c| c.get("resources"))
    else {
        return false;
    };
    ["limits", "requests"].iter().all(|side| {
        block
            .get(*side)
            .and_then(Value::as_object)
            .is_none_or(|want| {
                want.iter().all(|(resource, q)| {
                    let unit = Unit::for_resource(resource);
                    let want = q.as_str().and_then(|s| unit.parse(s));
                    let have = status
                        .pointer(&format!("/{side}/{resource}"))
                        .and_then(Value::as_str)
                        .and_then(|s| unit.parse(s));
                    want.is_some() && want == have
                })
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(policy: Option<&str>, status_limit: &str) -> Value {
        let mut c = json!({"name": "app", "resources": {"limits": {"cpu": "2"}, "requests": {"cpu": "300m"}}});
        if let Some(p) = policy {
            c["resizePolicy"] = json!([{"resourceName": "cpu", "restartPolicy": p}]);
        }
        json!({"spec": {"containers": [c]},
               "status": {"containerStatuses": [{"name": "app", "resources": {"limits": {"cpu": status_limit}, "requests": {"cpu": "250m"}}}]}})
    }

    #[test]
    fn restart_free_only_when_declared_not_required() {
        assert!(restart_free(&pod(Some("NotRequired"), "2"), Some("app"), "cpu"));
        assert!(!restart_free(&pod(Some("RestartContainer"), "2"), Some("app"), "cpu"));
        assert!(!restart_free(&pod(None, "2"), None, "cpu"), "absent policy is RestartContainer");
        assert!(!restart_free(&pod(Some("NotRequired"), "2"), Some("other"), "cpu"));
    }

    #[test]
    fn an_unspecified_policy_is_its_own_arm_not_a_restart() {
        assert_eq!(resize_restart_policy(&pod(None, "2"), Some("app"), "cpu"), RestartPolicy::Unspecified);
        assert_eq!(resize_restart_policy(&pod(Some("RestartContainer"), "2"), Some("app"), "cpu"), RestartPolicy::RestartContainer);
        assert_eq!(resize_restart_policy(&pod(Some("NotRequired"), "2"), Some("app"), "cpu"), RestartPolicy::NotRequired);
        assert_eq!(resize_restart_policy(&pod(Some("NotRequired"), "2"), Some("app"), "memory"), RestartPolicy::Unspecified);
    }

    #[test]
    fn a_burstable_limit_shrink_below_the_request_clamps_the_request() {
        assert_eq!(
            limit_block("Burstable", "cpu", 250, Some("300m")),
            json!({"limits": {"cpu": "250m"}, "requests": {"cpu": "250m"}})
        );
        assert_eq!(limit_block("Burstable", "cpu", 500, Some("300m")), json!({"limits": {"cpu": "500m"}}));
    }

    #[test]
    fn the_body_names_one_container() {
        let b = resize_body("app", &json!({"limits": {"cpu": "250m"}}));
        assert_eq!(b["spec"]["containers"][0]["name"], "app");
        assert_eq!(b["spec"]["containers"][0]["resources"]["limits"]["cpu"], "250m");
    }

    #[test]
    fn converged_compares_quantities_numerically_against_status() {
        let block = json!({"limits": {"cpu": "250m"}, "requests": {"cpu": "0.25"}});
        assert!(converged(&pod(None, "250m"), "app", &block), "250m == 0.25 cores");
        assert!(!converged(&pod(None, "2"), "app", &block), "status still at the old limit");
        assert!(!converged(&json!({"spec": {}}), "app", &block), "no status is not converged");
    }

    #[test]
    fn quantities_round_trip_through_the_one_parser() {
        assert_eq!(parse("cpu", "250m"), Some(250));
        assert_eq!(parse("cpu", "2"), Some(2000));
        assert_eq!(parse("cpu", "nonsense"), None);
        assert_eq!(render("cpu", 250), "250m");
    }

    #[test]
    fn container_resources_is_the_inverse_source() {
        assert_eq!(
            container_resources(&pod(None, "2"), "app"),
            Some(json!({"limits": {"cpu": "2"}, "requests": {"cpu": "300m"}}))
        );
        assert_eq!(container_resources(&pod(None, "2"), "nope"), None);
    }
}
