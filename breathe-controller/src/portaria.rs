//! `portaria` — the gatehouse. Lifts a node's startup taint against a gate
//! verdict, and never for any other reason.
//!
//! This is the controller half of the admission seal. `breathe-admission`
//! decides ([`AcaoPortao`]); this module is the only thing that turns a
//! decision into a mutation of a real node, and it is deliberately the
//! smallest surface that can do so.
//!
//! # The mechanism
//!
//! Karpenter applies `pleme.io/unbreathed:NoSchedule` as a `startupTaint` at
//! node creation, so a node is **born unschedulable**. The gate chain runs;
//! only [`AcaoPortao::Liberar`] removes the taint. There is no window between
//! "node is Ready" and "node is governed", because the node is not schedulable
//! during that window at all — which is the whole point, and the thing a
//! post-hoc reconciler can never give you.
//!
//! # Tier honesty
//!
//! [`taints_after`] is PURE and library-tested: it cannot add the taint, and it
//! cannot touch a taint it does not own. That much is unrepresentable-by-
//! construction at this layer. The live path — the controller watching Nodes,
//! gathering the pod readings, SSA-patching the result — needs a real cluster
//! and is **`pending-deploy`**, exactly as `pod_memory_high`'s dispatch builder
//! is. A green test here says the *decision* is right, never that a node was
//! released.
//!
//! # Ordering, which is load-bearing
//!
//! The taint must NOT be added to any `NodePool` until this controller is
//! deployed and observed lifting it. A taint whose remover does not exist
//! leaves every new node permanently unschedulable — and because seven of the
//! nine `PortaoKind` are still fail-safe stubs that always `Defer`, the chain
//! this controller runs must contain the REAL gates only. A stub in a live
//! chain can never reach `Liberar` (proven in `breathe-admission`'s
//! `a_chain_containing_a_stub_can_never_release_a_node`), so it would burn the
//! defer budget and hand every node back as `Expirado`.

// AUTHORED AHEAD OF ITS CALLER, deliberately — the same "safe to author ahead
// of the wiring" precedent the builder NodePool uses while INERT. The reconcile
// loop that gathers pod readings and SSA-patches the result is the next step
// (P2); until it lands nothing in this crate calls these, and suppressing the
// resulting dead-code warning is preferable to wiring a loop that would run
// before it has been observed. Remove this attribute when the loop lands.
#![allow(dead_code)]

use breathe_admission::AcaoPortao;
use k8s_openapi::api::core::v1::Taint;

/// The startup taint Karpenter stamps on a new node and this module lifts.
///
/// A node carrying it is unschedulable for anything that does not explicitly
/// tolerate it. **`DaemonSet`s that tolerate all taints still land** — which is
/// required, since the very components the gate looks for arrive that way.
/// The taint governs *workload*, not the instrumentation it is checking for.
pub const TAINT_KEY: &str = "pleme.io/unbreathed";

/// The taint list a node should carry after `action`, or `None` when no write
/// is needed.
///
/// `None` rather than "the same list" on purpose: the caller must not issue a
/// patch that changes nothing, both because it is churn against the apiserver
/// and because an unconditional write makes "did we act?" unanswerable from
/// the audit log.
///
/// Three properties hold by construction and are tested:
/// 1. **It never adds the taint.** Applying it is Karpenter's job at creation;
///    a controller that could stamp it could make a running node unschedulable.
/// 2. **It only ever removes `TAINT_KEY`.** Every other taint — a user's, a
///    cloud provider's, another controller's — passes through untouched.
/// 3. **Only `Liberar` removes anything.** Deferral holds the node, and handing
///    it back leaves the taint on so nothing schedules onto a node being
///    reclaimed.
#[must_use]
pub fn taints_after(action: AcaoPortao, current: &[Taint]) -> Option<Vec<Taint>> {
    if !action.releases_node() {
        return None;
    }
    let kept: Vec<Taint> = current.iter().filter(|t| t.key != TAINT_KEY).cloned().collect();
    // Already released (or never tainted): nothing to write.
    if kept.len() == current.len() {
        return None;
    }
    Some(kept)
}

#[cfg(test)]
mod tests {
    use super::{taints_after, TAINT_KEY};
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
        let after = taints_after(AcaoPortao::Liberar, &[taint(TAINT_KEY)]).expect("a write");
        assert!(after.is_empty());
    }

    /// Property 3 — a node still being judged, or being handed back, keeps the
    /// taint. Nothing may schedule onto either.
    #[test]
    fn no_action_but_release_ever_lifts_the_taint() {
        for action in every_non_release() {
            assert_eq!(
                taints_after(action, &[taint(TAINT_KEY)]),
                None,
                "{action:?} must leave the node unschedulable"
            );
        }
    }

    /// Property 2 — other taints are none of this controller's business.
    #[test]
    fn other_taints_pass_through_untouched() {
        let current =
            vec![taint("node.kubernetes.io/unreachable"), taint(TAINT_KEY), taint("team/gpu")];
        let after = taints_after(AcaoPortao::Liberar, &current).expect("a write");
        assert_eq!(after.len(), 2);
        assert!(after.iter().all(|t| t.key != TAINT_KEY));
        assert!(after.iter().any(|t| t.key == "node.kubernetes.io/unreachable"));
        assert!(after.iter().any(|t| t.key == "team/gpu"));
    }

    /// Property 1 — the strong one. Whatever the verdict and whatever the node
    /// already carries, this function cannot introduce the taint.
    #[test]
    fn the_controller_can_never_add_the_taint() {
        let mut actions = every_non_release();
        actions.push(AcaoPortao::Liberar);
        for action in actions {
            for current in [vec![], vec![taint("team/gpu")], vec![taint(TAINT_KEY)]] {
                let had = current.iter().any(|t| t.key == TAINT_KEY);
                if let Some(after) = taints_after(action, &current) {
                    assert!(
                        !after.iter().any(|t| t.key == TAINT_KEY),
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
        assert_eq!(taints_after(AcaoPortao::Liberar, &[]), None);
        assert_eq!(taints_after(AcaoPortao::Liberar, &[taint("team/gpu")]), None);
    }
}
