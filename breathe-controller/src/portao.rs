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

// ── Gathering: which pods prove which component, on which node ─────────────

use std::collections::BTreeMap;

use breathe_admission::{ComponenteExigido, ConformanceBinding, ObservacaoPod, VistaNo};

/// Where a component's pod lives and how to recognise it.
///
/// Typed config rather than constants because the answer is platform-specific
/// and getting it wrong is indistinguishable from the component being absent.
/// Measured on camelot (EKS): the CNI is `k8s-app=aws-node` and the CSI node
/// plugin is `app=ebs-csi-node`, both in `kube-system`. On rio (k3s) neither
/// exists under those names — flannel and local-path answer instead. A
/// hardcoded EKS selector would make every k3s node look permanently unbreathed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeletorComponente {
    pub namespace: String,
    /// ALL of these must match — the same semantics as a `DaemonSet`'s
    /// `selector.matchLabels`, which is where these values come from.
    pub labels: BTreeMap<String, String>,
}

impl SeletorComponente {
    fn new(namespace: &str, labels: &[(&str, &str)]) -> Self {
        Self {
            namespace: namespace.to_owned(),
            labels: labels.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect(),
        }
    }
    fn matches(&self, p: &PodLeitura) -> bool {
        p.namespace == self.namespace
            && self.labels.iter().all(|(k, v)| p.labels.get(k) == Some(v))
    }
}

/// Every required component's selector. Must COVER the gate's `required` set —
/// see [`catalogo_cobre_o_portao`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogoComponentes(BTreeMap<ComponenteExigido, SeletorComponente>);

impl CatalogoComponentes {
    /// The EKS shape, read off camelot's live `DaemonSet`s on 2026-08-08.
    #[must_use]
    pub fn eks() -> Self {
        Self(
            [
                (
                    ComponenteExigido::BreatheHostAgent,
                    SeletorComponente::new(
                        "breathe-system",
                        &[
                            ("app.kubernetes.io/name", "pleme-breathe"),
                            ("app.kubernetes.io/component", "host-agent"),
                        ],
                    ),
                ),
                (
                    ComponenteExigido::ContainerNetwork,
                    SeletorComponente::new("kube-system", &[("k8s-app", "aws-node")]),
                ),
                (
                    ComponenteExigido::StorageDriver,
                    SeletorComponente::new("kube-system", &[("app", "ebs-csi-node")]),
                ),
            ]
            .into_iter()
            .collect(),
        )
    }

    #[must_use]
    pub fn get(&self, c: ComponenteExigido) -> Option<&SeletorComponente> {
        self.0.get(&c)
    }
}

/// One pod, reduced to what the gate can use. Keeps the pure gather free of
/// `k8s_openapi` shapes so every interesting case is a struct literal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodLeitura {
    pub namespace: String,
    pub labels: BTreeMap<String, String>,
    /// `None` for an unscheduled pod — it proves nothing about any node.
    pub node_name: Option<String>,
    pub ready: bool,
    /// `now - Ready.lastTransitionTime`. `None` when unparsable.
    pub ready_for: Option<core::time::Duration>,
}

/// **The precondition that prevents a silent stall.** Does the catalog have a
/// selector for every component the gate requires?
///
/// A component with no selector can never be observed, so it classifies as
/// `Indeterminate`, so the gate defers, so the node is never released — and
/// Karpenter will not complain, because an un-Initialized node is exempt from
/// consolidation AND from drift (v1.8.1: `statenode.go:209-211`). The pod that
/// triggered the scale-up just stays Pending forever with nothing reporting
/// why. That is the failure mode this whole rollout has to avoid, and it is a
/// *configuration* mistake, so it is caught before the loop runs rather than
/// discovered as silence.
pub fn catalogo_cobre_o_portao(
    cat: &CatalogoComponentes,
    gate: &ConformanceBinding,
) -> Result<(), Vec<ComponenteExigido>> {
    let faltando: Vec<_> =
        gate.required.iter().map(|(c, _)| *c).filter(|c| cat.get(*c).is_none()).collect();
    if faltando.is_empty() {
        Ok(())
    } else {
        Err(faltando)
    }
}

/// Build one node's view from a cluster-wide pod listing. Pure.
///
/// A component whose selector matches no pod ON THIS NODE is `Absent` — the
/// node is still coming up — which defers rather than rejects.
#[must_use]
pub fn vista_para_no(node: &str, pods: &[PodLeitura], cat: &CatalogoComponentes) -> VistaNo {
    let mut v = VistaNo::new();
    for c in ComponenteExigido::ALL {
        let Some(sel) = cat.get(c) else { continue };
        let found = pods
            .iter()
            .find(|p| p.node_name.as_deref() == Some(node) && sel.matches(p));
        let obs = found.map_or(ObservacaoPod::default(), |p| ObservacaoPod {
            found: true,
            ready: p.ready,
            ready_for: p.ready_for,
            lookup_failed: false,
        });
        v = v.observando(c, &obs);
    }
    v
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

#[cfg(test)]
mod gather_tests {
    use super::{
        catalogo_cobre_o_portao, vista_para_no, CatalogoComponentes, PodLeitura, SeletorComponente,
    };
    use breathe_admission::{
        ComponenteExigido, ConformanceBinding, Conformant, EstadoComponente, ProvaExigida,
    };
    use core::time::Duration;
    use std::collections::BTreeMap;

    fn labels(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
        kv.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
    }

    fn agent_on(node: &str, ready_for_secs: u64) -> PodLeitura {
        PodLeitura {
            namespace: "breathe-system".into(),
            labels: labels(&[
                ("app.kubernetes.io/name", "pleme-breathe"),
                ("app.kubernetes.io/component", "host-agent"),
            ]),
            node_name: Some(node.into()),
            ready: true,
            ready_for: Some(Duration::from_secs(ready_for_secs)),
        }
    }
    fn cni_on(node: &str) -> PodLeitura {
        PodLeitura {
            namespace: "kube-system".into(),
            labels: labels(&[("k8s-app", "aws-node")]),
            node_name: Some(node.into()),
            ready: true,
            ready_for: Some(Duration::from_secs(600)),
        }
    }
    fn csi_on(node: &str) -> PodLeitura {
        PodLeitura {
            namespace: "kube-system".into(),
            labels: labels(&[
                ("app", "ebs-csi-node"),
                ("app.kubernetes.io/name", "aws-ebs-csi-driver"),
            ]),
            node_name: Some(node.into()),
            ready: true,
            ready_for: Some(Duration::from_secs(600)),
        }
    }

    #[test]
    fn a_fully_instrumented_node_gathers_present_for_everything() {
        let pods = vec![agent_on("n1", 900), cni_on("n1"), csi_on("n1")];
        let v = vista_para_no("n1", &pods, &CatalogoComponentes::eks());
        for c in ComponenteExigido::ALL {
            assert!(
                matches!(v.component_state(c), EstadoComponente::Present { .. }),
                "{c:?} should be Present"
            );
        }
    }

    /// **Pods on OTHER nodes prove nothing about this one.** The gather is
    /// per-node; a cluster-wide `DaemonSet` listing would otherwise make every
    /// node look instrumented the moment any one node was.
    #[test]
    fn another_nodes_pods_do_not_breathe_this_node() {
        let pods = vec![agent_on("other", 900), cni_on("other"), csi_on("other")];
        let v = vista_para_no("n1", &pods, &CatalogoComponentes::eks());
        for c in ComponenteExigido::ALL {
            assert_eq!(v.component_state(c), EstadoComponente::Absent, "{c:?}");
        }
    }

    #[test]
    fn a_missing_component_is_absent_and_the_others_are_unaffected() {
        let pods = vec![agent_on("n1", 900), cni_on("n1")]; // no CSI
        let v = vista_para_no("n1", &pods, &CatalogoComponentes::eks());
        assert_eq!(v.component_state(ComponenteExigido::StorageDriver), EstadoComponente::Absent);
        assert!(matches!(
            v.component_state(ComponenteExigido::BreatheHostAgent),
            EstadoComponente::Present { .. }
        ));
    }

    /// A selector needs ALL its labels — a partial match is a different pod.
    #[test]
    fn a_partial_label_match_is_not_the_component() {
        let mut impostor = agent_on("n1", 900);
        impostor.labels = labels(&[("app.kubernetes.io/name", "pleme-breathe")]); // missing component=
        let v = vista_para_no("n1", &[impostor], &CatalogoComponentes::eks());
        assert_eq!(
            v.component_state(ComponenteExigido::BreatheHostAgent),
            EstadoComponente::Absent
        );
    }

    /// Right labels, wrong namespace — not it.
    #[test]
    fn the_namespace_is_part_of_the_selector() {
        let mut elsewhere = agent_on("n1", 900);
        elsewhere.namespace = "default".into();
        let v = vista_para_no("n1", &[elsewhere], &CatalogoComponentes::eks());
        assert_eq!(
            v.component_state(ComponenteExigido::BreatheHostAgent),
            EstadoComponente::Absent
        );
    }

    /// The measured camelot reality: agents Ready for hours pass, because
    /// `ready_for` is a stability floor and not a staleness ceiling.
    #[test]
    fn the_live_camelot_readiness_ages_gather_to_present() {
        for secs in [6_004u64, 20_959] {
            let pods = vec![agent_on("n1", secs), cni_on("n1"), csi_on("n1")];
            let v = vista_para_no("n1", &pods, &CatalogoComponentes::eks());
            assert_eq!(
                v.component_state(ComponenteExigido::BreatheHostAgent),
                EstadoComponente::Present { ready_for: Duration::from_secs(secs) }
            );
        }
    }

    // ── the coverage precondition ──────────────────────────────────────

    #[test]
    fn the_eks_catalog_covers_the_fleet_default_gate() {
        assert_eq!(
            catalogo_cobre_o_portao(&CatalogoComponentes::eks(), &ConformanceBinding::fleet_default()),
            Ok(())
        );
    }

    /// **The silent-stall guard.** A required component with no selector can
    /// never be observed, so the gate defers forever, so the node is never
    /// released — and Karpenter will not complain, because an un-Initialized
    /// node is exempt from both consolidation and drift. Caught as
    /// configuration, not discovered as silence.
    #[test]
    fn a_gate_requiring_an_uncatalogued_component_is_refused_up_front() {
        let empty = CatalogoComponentes::default();
        let err = catalogo_cobre_o_portao(&empty, &ConformanceBinding::fleet_default())
            .expect_err("an empty catalog cannot serve the fleet gate");
        assert_eq!(err.len(), ComponenteExigido::ALL.len());
        assert!(err.contains(&ComponenteExigido::BreatheHostAgent));
    }

    /// Coverage is judged against what the gate ACTUALLY requires, so a
    /// narrowed gate is servable by a narrowed catalog.
    #[test]
    fn coverage_is_relative_to_the_gates_own_required_set() {
        let only_agent = ConformanceBinding {
            required: vec![(ComponenteExigido::BreatheHostAgent, ProvaExigida::PresenteOuMelhor)],
            ..ConformanceBinding::fleet_default()
        };
        let mut cat = CatalogoComponentes::default();
        cat.0.insert(
            ComponenteExigido::BreatheHostAgent,
            SeletorComponente::new("breathe-system", &[("app.kubernetes.io/name", "pleme-breathe")]),
        );
        assert_eq!(catalogo_cobre_o_portao(&cat, &only_agent), Ok(()));
        assert!(catalogo_cobre_o_portao(&cat, &ConformanceBinding::fleet_default()).is_err());
    }
}
