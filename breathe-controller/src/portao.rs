//! `portao` — the controller-side half of the admission gate. Lifts a node's
//! startup taint against a gate verdict, and never for any other reason.
//!
//! Named for the `Portao` it actuates rather than for a new word. This was
//! briefly `portaria` (the porter's lodge), which `theory/NATURALIZE-SSH.md:399`
//! already proposes for ssh naturalization — and proposes with the SAME sense,
//! "where you are identified before you are admitted". An adjacent sense is a
//! worse collision than an unrelated one, and no lint catches it; the naming
//! skill's step 4 does, which is what happened here.
//!
//! # The mechanism
//!
//! Karpenter applies [`TAINT_KEY`]`:NoSchedule` as a `startupTaint` at node
//! creation, so a node is **born unschedulable**. The gate chain runs; only
//! [`AcaoPortao::Liberar`] removes the taint. There is no window between "node
//! is Ready" and "node is governed", because the node is not schedulable during
//! that window at all — which is the whole point, and the thing a post-hoc
//! reconciler structurally cannot give you.
//!
//! # It composes the shared taint primitive rather than a private one
//!
//! The patch body comes from [`crate::node_forma::remove_taint`], the inverse of
//! the `upsert_taint` that `claim_patch` and `origin_guard` already share. This
//! module briefly carried its own `taints_after` returning `Vec<Taint>` — a
//! second taint vocabulary in a codebase that had deliberately built one, and
//! exactly the near-miss-duplication the compounding directive forbids. The
//! merge-patch hazard is identical in both directions, so both directions live
//! in one place.
//!
//! # Tier honesty
//!
//! [`release_patch`] is PURE and library-tested: it cannot add the taint, and it
//! cannot touch a taint it does not own. That much holds by construction here.
//! The live path — watching Nodes, gathering the pod readings, applying the
//! patch — needs a real cluster and is **`pending-deploy`**, exactly as
//! `pod_memory_high`'s dispatch builder is. A green test here says the
//! *decision* is right, never that a node was released.
//!
//! # Ordering, which is load-bearing
//!
//! The taint must NOT be added to any `NodePool` until this controller is
//! deployed and observed lifting it, and until `breathe-controller` itself
//! TOLERATES the taint — otherwise a reclaimed controller node can never be
//! replaced and nothing removes a taint again, cluster-wide and permanent.
//! Separately: because seven of the nine `PortaoKind` are still fail-safe stubs
//! that always `Defer`, the chain this module's caller runs must contain the
//! REAL gates only. A stub in a live chain can never reach `Liberar` (proven by
//! `breathe-admission`'s `a_chain_containing_a_stub_can_never_release_a_node`),
//! so it would burn the defer budget and hand every node back as `Expirado`.

// AUTHORED AHEAD OF ITS CALLER, deliberately — the same "safe to author ahead
// of the wiring" precedent the builder NodePool uses while INERT. The reconcile
// loop that gathers pod readings and applies the patch is the next step; until
// it lands nothing in this crate calls these, and suppressing the resulting
// dead-code warning is preferable to wiring a loop that would run before it has
// been observed. Remove this attribute when the loop lands.
#![allow(dead_code)]

use breathe_admission::AcaoPortao;
use k8s_openapi::api::core::v1::Taint;

use crate::node_forma::remove_taint;

/// The startup taint Karpenter stamps on a new node and this module lifts.
///
/// A node carrying it is unschedulable for anything that does not explicitly
/// tolerate it. **`DaemonSet`s that tolerate all taints still land** — which is
/// required, since the very components the gate looks for arrive that way. The
/// taint governs *workload*, not the instrumentation it is checking for.
pub const TAINT_KEY: &str = "pleme.io/unbreathed";

/// The `spec.taints` merge-patch that releases the node, or `None` when no write
/// is warranted.
///
/// Three properties, tested rather than asserted:
/// 1. **It never adds the taint** — [`remove_taint`] returns a strict subset.
/// 2. **It only ever removes [`TAINT_KEY`]** — every other taint passes through.
/// 3. **Only `Liberar` writes at all.** Deferral holds the node, and handing it
///    back leaves the taint ON, so nothing schedules onto a node being reclaimed.
#[must_use]
pub fn release_patch(action: AcaoPortao, current: &[Taint]) -> Option<Vec<serde_json::Value>> {
    if !action.releases_node() {
        return None;
    }
    remove_taint(current, TAINT_KEY)
}

#[cfg(test)]
mod tests {
    use super::{release_patch, TAINT_KEY};
    use breathe_admission::{AcaoPortao, MotivoDevolucao};
    use k8s_openapi::api::core::v1::Taint;

    fn taint(key: &str) -> Taint {
        Taint { key: key.to_owned(), effect: "NoSchedule".to_owned(), ..Default::default() }
    }

    fn every_non_release() -> Vec<AcaoPortao> {
        vec![
            AcaoPortao::Reter { orcamento_restante: 3 },
            AcaoPortao::Reter { orcamento_restante: 0 },
            AcaoPortao::Devolver { motivo: MotivoDevolucao::Rejeitado },
            AcaoPortao::Devolver { motivo: MotivoDevolucao::Expirado },
        ]
    }

    #[test]
    fn releasing_removes_the_startup_taint() {
        let after = release_patch(AcaoPortao::Liberar, &[taint(TAINT_KEY)]).expect("a write");
        assert!(after.is_empty());
    }

    /// Property 3 — a node still being judged, or being handed back, keeps the
    /// taint. Nothing may schedule onto either.
    #[test]
    fn no_action_but_release_ever_lifts_the_taint() {
        for action in every_non_release() {
            assert_eq!(
                release_patch(action, &[taint(TAINT_KEY)]),
                None,
                "{action:?} must leave the node unschedulable"
            );
        }
    }

    /// Property 2 — other taints are none of this controller's business. This is
    /// also the merge-patch hazard: a removal that dropped the survivors would
    /// silently un-taint the node for everything else it carries.
    #[test]
    fn other_taints_pass_through_untouched() {
        let current =
            vec![taint("node.kubernetes.io/unreachable"), taint(TAINT_KEY), taint("team/gpu")];
        let after = release_patch(AcaoPortao::Liberar, &current).expect("a write");
        assert_eq!(after.len(), 2);
        assert!(after.iter().all(|t| t["key"] != TAINT_KEY));
        assert!(after.iter().any(|t| t["key"] == "node.kubernetes.io/unreachable"));
        assert!(after.iter().any(|t| t["key"] == "team/gpu"));
    }

    /// Property 1 — the strong one. Whatever the verdict and whatever the node
    /// already carries, this cannot introduce the taint.
    #[test]
    fn the_controller_can_never_add_the_taint() {
        let mut actions = every_non_release();
        actions.push(AcaoPortao::Liberar);
        for action in actions {
            for current in [vec![], vec![taint("team/gpu")], vec![taint(TAINT_KEY)]] {
                let had = current.iter().any(|t| t.key == TAINT_KEY);
                if let Some(after) = release_patch(action, &current) {
                    assert!(
                        !after.iter().any(|t| t["key"] == TAINT_KEY),
                        "{action:?} produced a list carrying the taint"
                    );
                    assert!(had, "{action:?} wrote against a node that was not tainted");
                }
            }
        }
    }

    /// An untainted node needs no write — a released node is not re-released on
    /// every reconcile, so the audit log means something.
    #[test]
    fn an_already_released_node_is_not_written_again() {
        assert_eq!(release_patch(AcaoPortao::Liberar, &[]), None);
        assert_eq!(release_patch(AcaoPortao::Liberar, &[taint("team/gpu")]), None);
    }
}
