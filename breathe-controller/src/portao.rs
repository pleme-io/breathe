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

// The loop is spawned from `main` in SHADOW. A few items here are reached only
// by a future live promotion or by tests, so the crate-level allow stays until
// `shadow: false` lands and exercises the write path.
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

use breathe_admission::{ComponenteExigido, ConformanceBinding, ObservacaoPod, Portao, VistaNo};

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

// ── The per-node outcome ───────────────────────────────────────────────────

/// What the reconcile did, or would have done, to one node.
///
/// Mirrors [`crate::node_forma::ClaimOutcome`]'s `WouldTaint`/`Tainted` split
/// so the shadow ladder reads the same at both taint surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultadoPortao {
    /// The node does not carry the taint. Nothing to decide, nothing to write —
    /// every node in the fleet today, since no `NodePool` stamps it yet.
    NaoGuardado,
    /// Shadow: the gate passed and the taint WOULD be lifted.
    Liberaria,
    /// Live: the gate passed and the taint was lifted.
    Liberado,
    /// Still gated.
    ///
    /// `preso` marks a node held past the stuck bound. It exists because
    /// **nothing else in the system will report this**: an un-Initialized node
    /// is exempt from Karpenter consolidation and from drift
    /// (v1.8.1 `statenode.go:209-211`), so a node wedged here is invisible —
    /// its pending pod simply never schedules and no controller complains.
    Retido { preso: bool },
    /// A gate rejected, or the defer budget ran out. The taint STAYS ON, so
    /// nothing schedules onto a node being handed back.
    Devolvido,
    /// The gate PASSED and the write failed. Distinct from every other arm on
    /// purpose: the node is gated for a reason that has nothing to do with its
    /// readiness, and conflating it with `Retido` would hide a broken actuator
    /// behind a normal-looking deferral.
    FalhouAoLiberar,
}

impl ResultadoPortao {
    /// The metric/status label, in the style [`crate::node_forma::outcome_of`]
    /// already uses.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NaoGuardado => "not_gated",
            Self::Liberaria => "would_release",
            Self::Liberado => "released",
            Self::Retido { preso: false } => "held",
            Self::Retido { preso: true } => "stuck",
            Self::Devolvido => "handed_back",
            Self::FalhouAoLiberar => "release_failed",
        }
    }
}

/// PURE: the whole per-node decision.
///
/// **The safety property, tested: `shadow == true` can never yield
/// [`ResultadoPortao::Liberado`].** A shadow reconcile computes the same verdict
/// and writes nothing, which is what makes it safe to run against a live
/// cluster before any pool carries the taint.
#[must_use]
pub fn resultado_para_no(
    carries_taint: bool,
    action: AcaoPortao,
    shadow: bool,
    node_age: core::time::Duration,
    stuck_after: core::time::Duration,
) -> ResultadoPortao {
    if !carries_taint {
        return ResultadoPortao::NaoGuardado;
    }
    if action.releases_node() {
        return if shadow { ResultadoPortao::Liberaria } else { ResultadoPortao::Liberado };
    }
    match action {
        AcaoPortao::Devolver { .. } => ResultadoPortao::Devolvido,
        // Age is measured from the NODE, not from when this loop first saw it:
        // a controller restart must not reset the clock on a node that has been
        // wedged for an hour.
        _ => ResultadoPortao::Retido { preso: node_age >= stuck_after },
    }
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

// ── The kube half ──────────────────────────────────────────────────────────
//
// Thin by design: everything decidable lives above this line and is tested
// without a cluster. What remains here is listing, mapping, and one patch.

use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    api::{Api, ListParams, Patch, PatchParams},
    ResourceExt,
};
use metrics::{counter, gauge, histogram};

/// How the reconcile is configured for one cluster.
pub struct PortaoConfig {
    pub catalogo: CatalogoComponentes,
    pub portao: ConformanceBinding,
    /// Shadow computes and reports without writing. **Default true** — the
    /// two-gate ladder this codebase already uses everywhere else.
    pub shadow: bool,
    /// How long a node may stay gated before it is reported as stuck.
    pub stuck_after: core::time::Duration,
}

/// Age of a `Ready` condition, and whether it is currently `True`.
fn ready_state(p: &Pod, now: std::time::SystemTime) -> (bool, Option<core::time::Duration>) {
    let Some(c) = p
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.type_ == "Ready"))
    else {
        return (false, None);
    };
    let ready = c.status == "True";
    // `Time` already wraps a DateTime<Utc>; no string round-trip needed.
    let age = c
        .last_transition_time
        .as_ref()
        .and_then(|t| u64::try_from(t.0.timestamp()).ok())
        .and_then(|ts| {
            now.duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|n| core::time::Duration::from_secs(n.as_secs().saturating_sub(ts)))
        });
    (ready, age)
}

fn leitura(p: &Pod, now: std::time::SystemTime) -> PodLeitura {
    let (ready, ready_for) = ready_state(p, now);
    PodLeitura {
        namespace: p.namespace().unwrap_or_default(),
        labels: p.labels().clone().into_iter().collect(),
        node_name: p.spec.as_ref().and_then(|s| s.node_name.clone()),
        ready,
        ready_for,
    }
}

fn node_age(n: &Node, now: std::time::SystemTime) -> core::time::Duration {
    n.creation_timestamp()
        .and_then(|t| u64::try_from(t.0.timestamp()).ok())
        .and_then(|ts| {
            now.duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|n| core::time::Duration::from_secs(n.as_secs().saturating_sub(ts)))
        })
        .unwrap_or_default()
}

/// One pass over every node. Returns each node's outcome.
///
/// # Errors
/// Propagates apiserver failures. A failed LIST is never treated as "no pods" —
/// that would read as every component absent and hand back the whole fleet.
pub async fn reconcile_portao(
    client: &kube::Client,
    cfg: &PortaoConfig,
) -> Result<Vec<(String, ResultadoPortao)>, kube::Error> {
    // Refuse to run a gate the catalog cannot serve, rather than defer every
    // node forever while nothing reports it.
    if let Err(missing) = catalogo_cobre_o_portao(&cfg.catalogo, &cfg.portao) {
        tracing::error!(
            ?missing,
            "portao: catalog does not cover the gate's required components — refusing to \
             reconcile. Every node would defer forever and nothing would report it."
        );
        return Ok(vec![]);
    }

    let now = std::time::SystemTime::now();
    let nodes = Api::<Node>::all(client.clone()).list(&ListParams::default()).await?;
    let pods = Api::<Pod>::all(client.clone()).list(&ListParams::default()).await?;
    let leituras: Vec<PodLeitura> = pods.items.iter().map(|p| leitura(p, now)).collect();

    let mut out = Vec::with_capacity(nodes.items.len());
    let mut tally: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut blocked: BTreeMap<(&'static str, &'static str), u64> = BTreeMap::new();

    for n in &nodes.items {
        let name = n.name_any();
        let taints = n.spec.as_ref().and_then(|s| s.taints.clone()).unwrap_or_default();
        let carries = taints.iter().any(|t| t.key == TAINT_KEY);

        let vista = vista_para_no(&name, &leituras, &cfg.catalogo);
        let receipts = vec![cfg.portao.check(&candidato(&name), &vista).await];
        let action = breathe_admission::acao_para(&breathe_admission::classify(
            candidato(&name),
            &receipts,
            DEFER_BUDGET,
        ));

        let age = node_age(n, now);
        let r = resultado_para_no(carries, action, cfg.shadow, age, cfg.stuck_after);

        // ── The measurement breathability actually needs ───────────────────
        //
        // `reliefLatencySeconds` is the lookahead the predictive previsor must
        // meet or exceed (BREATHABILITY-NODE-LIFECYCLE, P8) — and on camelot it
        // is 180 on both pools: a round, identical, hand-picked number. This
        // loop is the only thing in the fleet positioned to MEASURE the real
        // value, because relief latency is not boot-to-Ready, it is
        // creation-to-USABLE, and the gap between them is exactly the
        // DaemonSet-landing window this gate was built to close.
        //
        // Recorded on the release edge only. A node that is already released
        // re-resolves to NaoGuardado on every later pass and must not be
        // re-counted, or the distribution fills with the age of long-lived
        // nodes rather than the latency of new ones.
        if matches!(r, ResultadoPortao::Liberado | ResultadoPortao::Liberaria) {
            histogram!("breathe_portao_time_to_ready_seconds").record(age.as_secs_f64());
            gauge!("breathe_portao_last_time_to_ready_seconds").set(age.as_secs_f64());
            tracing::info!(
                node = %name,
                seconds = age.as_secs(),
                shadow = cfg.shadow,
                "portao: node reached usable — this is measured relief latency, \
                 not boot-to-Ready"
            );
        }

        // Which component is the long pole, in typed form. Prose reasons are
        // right for a receipt and useless for a dashboard; parsing them back
        // out would be the format-then-reparse shape the fleet bans.
        if let Some((comp, estado)) = cfg.portao.bloqueador(&vista) {
            let est = match estado {
                breathe_admission::EstadoComponente::Absent => "absent",
                breathe_admission::EstadoComponente::Indeterminate => "indeterminate",
                breathe_admission::EstadoComponente::Present { .. } => "present_unstable",
                breathe_admission::EstadoComponente::Reporting { .. } => "reporting_stale",
            };
            *blocked.entry((comp.as_str(), est)).or_default() += 1;
        }
        // A write that FAILS must not look like a node that was held. It gets
        // its own outcome, its own metric label, and ERROR not WARN — because
        // the first live test of this loop failed exactly here (force+Merge is
        // rejected by kube-rs at request-build time) and the node simply stayed
        // gated. Nothing else reports a wedged node: an un-Initialized node is
        // exempt from Karpenter consolidation AND drift.
        let mut r = r;
        if r == ResultadoPortao::Liberado
            && let Some(patch) = release_patch(action, &taints)
        {
                // PatchParams::default() + Patch::Merge — the shape every other
                // taint write in this crate uses (node_forma.rs:764,
                // origin_guard.rs:112). Do not reintroduce `.force()`: it is
                // server-side-apply only.
                if let Err(e) = Api::<Node>::all(client.clone())
                    .patch(
                        &name,
                        &PatchParams::default(),
                        &Patch::Merge(serde_json::json!({ "spec": { "taints": patch } })),
                    )
                    .await
                {
                    tracing::error!(
                        node = %name, error = %e,
                        "portao: gate PASSED but the taint write failed — the node stays \
                         unschedulable and nothing else will report it"
                    );
                    r = ResultadoPortao::FalhouAoLiberar;
                }
        }
        if let ResultadoPortao::Retido { preso: true } = r {
            tracing::warn!(
                node = %name,
                "portao: node has been gated past the stuck bound. Nothing else reports this — \
                 an un-Initialized node is exempt from Karpenter consolidation AND drift."
            );
        }
        *tally.entry(r.as_str()).or_default() += 1;
        out.push((name, r));
    }

    for (state, n) in &tally {
        #[allow(clippy::cast_precision_loss)] // a node count never nears 2^53
        gauge!("breathe_portao_nodes", "state" => *state).set(*n as f64);
    }
    // Emitted even when empty is impossible for a gauge, so zero out nothing:
    // an absent series and a zero series read differently, and the catalog of
    // components is closed, so every component that is NOT blocking simply has
    // no sample this pass.
    for ((comp, est), n) in &blocked {
        #[allow(clippy::cast_precision_loss)]
        gauge!("breathe_portao_blocked_by", "component" => *comp, "state" => *est).set(*n as f64);
    }
    counter!("breathe_portao_passes_total").increment(1);
    Ok(out)
}

/// The defer budget for one pass. A node that keeps deferring is re-judged on
/// the NEXT pass with a fresh budget — the stuck bound, not this counter, is
/// what bounds the total wait, because a node legitimately takes minutes to
/// land its `DaemonSet`s and burning the budget in one pass would hand it back.
const DEFER_BUDGET: u32 = 3;

fn candidato(name: &str) -> breathe_admission::Recurso<breathe_admission::Validando> {
    breathe_admission::Recurso::<breathe_admission::Descoberto>::discover(
        breathe_admission::ResourceId::new(name),
        breathe_provider::Forma::NodeOnDemand,
    )
    .begin_provision()
    .provisioned()
    .begin_validation()
}

#[cfg(test)]
mod outcome_tests {
    use super::{resultado_para_no, ResultadoPortao};
    use breathe_admission::{AcaoPortao, MotivoDevolucao};
    use core::time::Duration;

    const YOUNG: Duration = Duration::from_secs(10);
    const OLD: Duration = Duration::from_secs(3600);
    const BOUND: Duration = Duration::from_secs(600);

    /// **The safety property.** Shadow can never produce a live release, for any
    /// action, at any age. This is what makes it safe to run the loop against a
    /// live cluster before a single pool carries the taint.
    #[test]
    fn shadow_never_releases() {
        for action in [
            AcaoPortao::Liberar,
            AcaoPortao::Reter { orcamento_restante: 2 },
            AcaoPortao::Devolver { motivo: MotivoDevolucao::Rejeitado },
            AcaoPortao::Devolver { motivo: MotivoDevolucao::Expirado },
        ] {
            for age in [YOUNG, OLD] {
                for carries in [true, false] {
                    assert_ne!(
                        resultado_para_no(carries, action, true, age, BOUND),
                        ResultadoPortao::Liberado,
                        "{action:?} in shadow must never report a live release"
                    );
                }
            }
        }
    }

    #[test]
    fn an_ungated_node_is_never_touched() {
        for action in [AcaoPortao::Liberar, AcaoPortao::Devolver { motivo: MotivoDevolucao::Rejeitado }] {
            assert_eq!(
                resultado_para_no(false, action, false, OLD, BOUND),
                ResultadoPortao::NaoGuardado,
                "a node without the taint is not this loop's business"
            );
        }
    }

    #[test]
    fn a_passing_gate_releases_when_live_and_reports_when_shadow() {
        assert_eq!(resultado_para_no(true, AcaoPortao::Liberar, false, YOUNG, BOUND), ResultadoPortao::Liberado);
        assert_eq!(resultado_para_no(true, AcaoPortao::Liberar, true, YOUNG, BOUND), ResultadoPortao::Liberaria);
    }

    /// The stuck bound is what makes a wedged node visible at all.
    #[test]
    fn a_node_held_past_the_bound_is_reported_stuck() {
        let held = AcaoPortao::Reter { orcamento_restante: 1 };
        assert_eq!(resultado_para_no(true, held, false, YOUNG, BOUND), ResultadoPortao::Retido { preso: false });
        assert_eq!(resultado_para_no(true, held, false, OLD, BOUND), ResultadoPortao::Retido { preso: true });
    }

    /// Handing a node back leaves the taint ON — nothing may schedule onto a
    /// node being reclaimed — so it is never confused with a release.
    #[test]
    fn handing_back_is_not_a_release_at_any_age() {
        for m in [MotivoDevolucao::Rejeitado, MotivoDevolucao::Expirado] {
            for age in [YOUNG, OLD] {
                assert_eq!(
                    resultado_para_no(true, AcaoPortao::Devolver { motivo: m }, false, age, BOUND),
                    ResultadoPortao::Devolvido
                );
            }
        }
    }

    #[test]
    fn every_outcome_has_a_distinct_label() {
        let all = [
            ResultadoPortao::NaoGuardado,
            ResultadoPortao::Liberaria,
            ResultadoPortao::Liberado,
            ResultadoPortao::Retido { preso: false },
            ResultadoPortao::Retido { preso: true },
            ResultadoPortao::Devolvido,
            ResultadoPortao::FalhouAoLiberar,
        ];
        let mut seen: Vec<&str> = all.iter().map(|r| r.as_str()).collect();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), all.len(), "metric labels must not collide");
    }
}
