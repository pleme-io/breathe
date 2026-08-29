use super::{
    Admitido, Allocatable, CapacidadeProof, Descoberto, FaseRecurso, Forma, GateDecision, Portao,
    PortaoKind, ReciboGate, Recurso, ResourceId, StubGate, Validando, ValidationStep, Viveiro,
    classify,
};

/// A minimal no-dependency executor — the gates never await a pending future, so
/// a busy-poll to the first `Ready` is sufficient. Keeps breathe-admission
/// tokio-free for M1.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::pin::pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VT)
    }
    fn noop(_: *const ()) {}
    static VT: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VT)) };
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

#[derive(Debug, Clone)]
struct NodeRef {
    #[allow(dead_code)]
    name: String,
    allocatable: u64,
}
impl Allocatable for NodeRef {
    fn allocatable(&self) -> u64 {
        self.allocatable
    }
}

fn candidate() -> Recurso<Validando> {
    Recurso::<Descoberto>::discover(ResourceId::new("node-1"), Forma::NodeOnDemand)
        .begin_provision()
        .provisioned()
        .begin_validation()
}

#[test]
fn happy_path_admits_into_viveiro() {
    let node = NodeRef {
        name: "node-1".into(),
        allocatable: 64,
    };
    let cand = candidate();
    let gate = CapacidadeProof { required_floor: 8 };
    let recibo = block_on(gate.check(&cand, &node));
    assert_eq!(recibo.decision, GateDecision::Pass);
    match classify(cand, &[recibo], 3) {
        ValidationStep::Ready(pronto) => {
            let cert = pronto.admit(node);
            assert_eq!(cert.fase(), FaseRecurso::Admitido);
            assert!(
                cert.evidence()
                    .iter()
                    .any(|r| r.kind == PortaoKind::CapacidadeProof)
            );
            let mut pool: Viveiro<NodeRef> = Viveiro::new();
            assert!(pool.is_empty());
            pool.admit(cert);
            assert_eq!(pool.len(), 1);
            assert!(pool.get(&ResourceId::new("node-1")).is_some());
            // retire = the decommission terminal
            assert!(pool.retire(&ResourceId::new("node-1")).is_some());
            assert!(pool.is_empty());
        }
        _ => panic!("expected Ready"),
    }
}

#[test]
fn capacidade_proof_rejects_undersized_node() {
    let node = NodeRef {
        name: "tiny".into(),
        allocatable: 2,
    };
    let cand = candidate();
    let gate = CapacidadeProof { required_floor: 8 };
    let recibo = block_on(gate.check(&cand, &node));
    assert!(matches!(recibo.decision, GateDecision::Reject { .. }));
    match classify(cand, &[recibo], 3) {
        ValidationStep::Rejected(r) => assert_eq!(r.fase(), FaseRecurso::Rejeitado),
        _ => panic!("expected Rejected"),
    }
}

#[test]
fn stub_gate_defers_fail_safe() {
    let node = NodeRef {
        name: "n".into(),
        allocatable: 64,
    };
    let cand = candidate();
    let gate = StubGate(PortaoKind::QuotaCheck);
    let recibo = block_on(Portao::<NodeRef>::check(&gate, &cand, &node));
    assert!(matches!(recibo.decision, GateDecision::Defer { .. }));
}

#[test]
fn classify_reject_beats_defer_and_pass() {
    let receipts = vec![
        ReciboGate::pass(PortaoKind::CapacidadeProof),
        ReciboGate::defer(PortaoKind::QuotaCheck, "stub"),
        ReciboGate::reject(PortaoKind::CostEnvelope, "over budget"),
    ];
    assert!(matches!(
        classify(candidate(), &receipts, 5),
        ValidationStep::Rejected(_)
    ));
}

#[test]
fn classify_defer_requeues_until_budget_then_expires() {
    let receipts = vec![ReciboGate::defer(PortaoKind::QuotaCheck, "stub")];
    match classify(candidate(), &receipts, 2) {
        ValidationStep::Deferred(_, rem) => assert_eq!(rem, 1),
        _ => panic!("expected Deferred with budget"),
    }
    // budget exhausted → Expired (the FSM cannot wedge in Validando)
    match classify(candidate(), &receipts, 0) {
        ValidationStep::Expired(e) => assert_eq!(e.fase(), FaseRecurso::Expirado),
        _ => panic!("expected Expired at budget 0"),
    }
}

#[test]
fn classify_all_pass_is_ready() {
    let receipts: Vec<_> = PortaoKind::ALL
        .iter()
        .map(|&k| ReciboGate::pass(k))
        .collect();
    assert!(matches!(
        classify(candidate(), &receipts, 3),
        ValidationStep::Ready(_)
    ));
}

// ── FSM convergence (via the shared shigoto-fsm forcing-function) ──
// The hand-rolled BFS reachability + terminal-soundness + closed-graph walks were
// replaced by the fleet `shigoto_fsm` harness (FaseRecurso impls ConvergentFsm).
// One call proves: closed graph, terminals-have-no-successors-and-are-good, no
// dead-end traps, AND every reachable phase reaches a good terminal — the same
// guarantees, now from the shared primitive instead of four copies fleet-wide.

#[test]
fn admission_fsm_is_convergent() {
    shigoto_fsm::assert_convergent_fsm::<FaseRecurso>().expect(
        "the resource-admission FSM must be convergent (every phase reaches a good terminal)",
    );
}

/// The domain-specific count still pinned locally: exactly three good terminals
/// (Aposentado retire / Rejeitado refuse / Expirado timeout) — a property of THIS
/// FSM, not the generic convergence harness.
#[test]
fn exactly_three_terminals() {
    let terminals = FaseRecurso::ALL
        .into_iter()
        .filter(|f| f.is_terminal())
        .count();
    assert_eq!(terminals, 3, "expected exactly 3 terminals");
}

#[test]
fn nine_gate_kinds() {
    assert_eq!(PortaoKind::ALL.len(), 9);
}

// `Admitido<T>` is not constructible outside this crate — proven by the
// `compile_fail` doctests in lib.rs. Here we just confirm a minted one round-trips.
#[test]
fn admitido_round_trips_inner() {
    let cert: Admitido<u32> = candidate().ready().admit(42u32);
    assert_eq!(*cert.get(), 42);
    assert_eq!(cert.into_inner(), 42);
}

// ── ConformanceBinding — no node is usable before it is breathed ───────────
//
// One test per Gate-0 illegal state the gate was built to corner. Mock-green:
// the whole gate runs against a fake node handle, with zero kube linkage.

use super::{ComponenteExigido, ConformanceBinding, Conformant, EstadoComponente};
use core::time::Duration;

/// A node's self-report, entirely in memory.
struct FakeNode(std::collections::BTreeMap<ComponenteExigido, EstadoComponente>);

impl FakeNode {
    /// Every component healthy — the baseline the other cases perturb.
    fn breathed() -> Self {
        Self(
            ComponenteExigido::ALL
                .into_iter()
                .map(|c| {
                    (
                        c,
                        EstadoComponente::Present {
                            ready_for: Duration::from_secs(600),
                        },
                    )
                })
                .collect(),
        )
    }
    fn with(mut self, c: ComponenteExigido, s: EstadoComponente) -> Self {
        self.0.insert(c, s);
        self
    }
}

impl Conformant for FakeNode {
    fn component_state(&self, c: ComponenteExigido) -> EstadoComponente {
        self.0
            .get(&c)
            .copied()
            .unwrap_or(EstadoComponente::Indeterminate)
    }
}

fn verdict<T: Conformant + Send + Sync>(node: &T, gate: &ConformanceBinding) -> GateDecision {
    block_on(gate.check(&candidate(), node)).decision
}

#[test]
fn a_fully_breathed_node_passes() {
    assert_eq!(
        verdict(&FakeNode::breathed(), &ConformanceBinding::fleet_default()),
        GateDecision::Pass
    );
}

/// Gate-0 state 1 — the whole point. A node without the host agent must not
/// become schedulable, however healthy Kubernetes thinks it is.
#[test]
fn a_node_without_the_host_agent_never_passes() {
    let node = FakeNode::breathed().with(
        ComponenteExigido::BreatheHostAgent,
        EstadoComponente::Absent,
    );
    assert!(
        !matches!(
            verdict(&node, &ConformanceBinding::fleet_default()),
            GateDecision::Pass
        ),
        "an unbreathed node reaching Pass is the gap this gate exists to close"
    );
}

/// Gate-0 state 3 — present is not reporting. An agent that registered at boot
/// and died leaves the node ungoverned while looking identical to a healthy one.
#[test]
fn an_agent_that_stopped_reporting_does_not_pass() {
    let node = FakeNode::breathed().with(
        ComponenteExigido::BreatheHostAgent,
        EstadoComponente::Reporting {
            last_report_age: Duration::from_secs(3600),
        },
    );
    assert!(!matches!(
        verdict(&node, &ConformanceBinding::fleet_default()),
        GateDecision::Pass
    ));
}

/// ★ THE REGRESSION. Measured on one cluster 2026-08-08: every healthy host agent
/// had been Ready for 6 004–20 959 s, because the DaemonSet carries no
/// readinessProbe so `Ready` never re-transitions. The first cut compared that
/// against a 90 s staleness ceiling and DEFERRED all of them — a stable agent
/// failed while a freshly restarted one passed. The old suite could not catch
/// it: every case used `ready_for(5)`.
#[test]
fn a_long_stable_agent_passes_rather_than_deferring() {
    for secs in [600u64, 6_004, 20_959, 86_400] {
        let node = ComponenteExigido::ALL
            .into_iter()
            .fold(VistaNo::new(), |v, c| v.observando(c, &ready_for(secs)));
        assert_eq!(
            verdict(&node, &ConformanceBinding::fleet_default()),
            GateDecision::Pass,
            "an agent ready for {secs}s is HEALTHIER, not staler"
        );
    }
}

/// The stability bound is a FLOOR, so the two arms cannot be merged again:
/// raising readiness time can only ever help.
#[test]
fn presence_is_bounded_from_below_not_above() {
    let gate = ConformanceBinding::fleet_default();
    let at = |secs: u64| {
        let n = ComponenteExigido::ALL
            .into_iter()
            .fold(VistaNo::new(), |v, c| v.observando(c, &ready_for(secs)));
        verdict(&n, &gate)
    };
    assert!(
        matches!(at(5), GateDecision::Defer { .. }),
        "under min_stable"
    );
    assert_eq!(at(31), GateDecision::Pass);
    assert_eq!(
        at(999_999),
        GateDecision::Pass,
        "more stability never hurts"
    );
}

/// `ApenasReportando` refuses presence however stable — stability is not
/// liveness. This is the one-field flip that lands when the agent grows a Lease.
#[test]
fn a_component_demanding_a_heartbeat_refuses_mere_presence() {
    let gate = ConformanceBinding {
        required: vec![(
            ComponenteExigido::BreatheHostAgent,
            ProvaExigida::ApenasReportando,
        )],
        ..ConformanceBinding::fleet_default()
    };
    let stable = VistaNo::new().observando(ComponenteExigido::BreatheHostAgent, &ready_for(86_400));
    assert!(matches!(
        verdict(&stable, &gate),
        GateDecision::Defer { .. }
    ));

    let mut beating = VistaNo::new();
    beating = beating.com_estado(
        ComponenteExigido::BreatheHostAgent,
        EstadoComponente::Reporting {
            last_report_age: Duration::from_secs(5),
        },
    );
    assert_eq!(verdict(&beating, &gate), GateDecision::Pass);
}

/// Gate-0 state 5 — "can't tell" must not round up to "fine".
#[test]
fn an_unobservable_component_defers_rather_than_passing() {
    let node = FakeNode::breathed().with(
        ComponenteExigido::ContainerNetwork,
        EstadoComponente::Indeterminate,
    );
    assert!(matches!(
        verdict(&node, &ConformanceBinding::fleet_default()),
        GateDecision::Defer { .. }
    ));
}

/// A node still coming up DEFERS — it must not be rejected, because the
/// DaemonSet is landing and the defer budget is what bounds the wait.
#[test]
fn a_booting_node_defers_and_is_not_rejected() {
    let node =
        FakeNode::breathed().with(ComponenteExigido::StorageDriver, EstadoComponente::Absent);
    assert!(matches!(
        verdict(&node, &ConformanceBinding::fleet_default()),
        GateDecision::Defer { .. }
    ));
}

/// The vacuous guard, refused explicitly: a gate configured to require nothing
/// would report green over every node in the fleet.
#[test]
fn a_gate_requiring_nothing_is_a_reject_not_a_pass() {
    let gate = ConformanceBinding {
        required: vec![],
        ..ConformanceBinding::fleet_default()
    };
    assert!(matches!(
        verdict(&FakeNode::breathed(), &gate),
        GateDecision::Reject { .. }
    ));
}

/// Freshness is the gate's policy, not the observer's — the same observation
/// passes or defers purely on the configured bound.
#[test]
fn the_freshness_bound_belongs_to_the_gate() {
    let node = FakeNode::breathed().with(
        ComponenteExigido::BreatheHostAgent,
        EstadoComponente::Reporting {
            last_report_age: Duration::from_secs(120),
        },
    );
    let only_agent = vec![(
        ComponenteExigido::BreatheHostAgent,
        ProvaExigida::PresenteOuMelhor,
    )];
    let strict = ConformanceBinding {
        required: only_agent.clone(),
        max_report_age: Duration::from_secs(90),
        ..ConformanceBinding::fleet_default()
    };
    let lax = ConformanceBinding {
        required: only_agent,
        max_report_age: Duration::from_secs(300),
        ..ConformanceBinding::fleet_default()
    };
    assert!(matches!(
        verdict(&node, &strict),
        GateDecision::Defer { .. }
    ));
    assert_eq!(verdict(&node, &lax), GateDecision::Pass);
}

/// The catalog is enumerable, so "everything the node must carry" is readable
/// back rather than remembered (★★ CATALOG REFLECTION).
#[test]
fn every_required_component_is_enumerated_and_named() {
    assert_eq!(ComponenteExigido::ALL.len(), 3);
    for c in ComponenteExigido::ALL {
        assert!(!c.as_str().is_empty());
    }
    assert_eq!(
        ConformanceBinding::fleet_default().required.len(),
        ComponenteExigido::ALL.len()
    );
}

// ── acao_para — the join between the seal and the cluster ──────────────────

use super::{AcaoPortao, MotivoDevolucao, acao_para};

/// Verdicts are built through the REAL `classify` path, never by constructing
/// a `ValidationStep` arm directly — the point is the join, not the match.
fn acao(receipts: &[ReciboGate], budget: u32) -> AcaoPortao {
    acao_para(&classify(candidate(), receipts, budget))
}

#[test]
fn every_gate_passing_releases_the_node() {
    let all_pass: Vec<_> = PortaoKind::ALL.into_iter().map(ReciboGate::pass).collect();
    assert_eq!(acao(&all_pass, 3), AcaoPortao::Liberar);
}

#[test]
fn a_deferred_gate_holds_the_taint_and_keeps_the_budget() {
    let r = vec![
        ReciboGate::pass(PortaoKind::CapacidadeProof),
        ReciboGate::defer(PortaoKind::ConformanceBinding, "agent absent"),
    ];
    assert_eq!(
        acao(&r, 3),
        AcaoPortao::Reter {
            orcamento_restante: 2
        }
    );
}

#[test]
fn an_exhausted_defer_budget_hands_the_node_back() {
    let r = vec![ReciboGate::defer(
        PortaoKind::ConformanceBinding,
        "still absent",
    )];
    assert_eq!(
        acao(&r, 0),
        AcaoPortao::Devolver {
            motivo: MotivoDevolucao::Expirado
        }
    );
}

#[test]
fn a_rejecting_gate_hands_the_node_back() {
    let r = vec![
        ReciboGate::pass(PortaoKind::ConformanceBinding),
        ReciboGate::reject(PortaoKind::CapacidadeProof, "allocatable below floor"),
    ];
    assert_eq!(
        acao(&r, 9),
        AcaoPortao::Devolver {
            motivo: MotivoDevolucao::Rejeitado
        }
    );
}

/// **The load-bearing property.** Nothing but a full pass makes a node
/// schedulable — checked over every single-gate failure, not argued.
#[test]
fn only_a_full_pass_releases_a_node() {
    for spoiler in PortaoKind::ALL {
        for bad in [
            ReciboGate::defer(spoiler, "not yet"),
            ReciboGate::reject(spoiler, "no"),
        ] {
            let mut receipts: Vec<_> = PortaoKind::ALL
                .into_iter()
                .filter(|k| *k != spoiler)
                .map(ReciboGate::pass)
                .collect();
            receipts.push(bad);
            for budget in [0, 1, 7] {
                assert!(
                    !acao(&receipts, budget).releases_node(),
                    "{spoiler:?} failing must never release the node"
                );
            }
        }
    }
}

/// **Operational invariant, and the reason the taint must land last.** A stub
/// gate ALWAYS defers, so any chain containing one can never reach `Liberar` —
/// it burns the defer budget and hands every node back. Seven of the nine kinds
/// are stubs today, so the deployed chain must run the REAL gates only. Adding
/// a stub to a live chain would make every new node unschedulable and then
/// reclaimed, which is a cluster-wide outage rather than a degraded check.
#[test]
fn a_chain_containing_a_stub_can_never_release_a_node() {
    let stub = StubGate(PortaoKind::AttestationBinding);
    let inner = 0u32;
    let receipt = block_on(stub.check(&candidate(), &inner));
    let mut receipts: Vec<_> = PortaoKind::ALL
        .into_iter()
        .filter(|k| *k != PortaoKind::AttestationBinding)
        .map(ReciboGate::pass)
        .collect();
    receipts.push(receipt);
    for budget in [0, 1, 100] {
        assert!(
            !acao(&receipts, budget).releases_node(),
            "a stub in the chain must never release — it defers forever"
        );
    }
}

/// `releases_node` is the single reader of the release question, so a future
/// variant cannot be misclassified by a caller's own re-derived match.
#[test]
fn exactly_one_action_releases_the_node() {
    let all = [
        AcaoPortao::Liberar,
        AcaoPortao::Reter {
            orcamento_restante: 1,
        },
        AcaoPortao::Devolver {
            motivo: MotivoDevolucao::Rejeitado,
        },
        AcaoPortao::Devolver {
            motivo: MotivoDevolucao::Expirado,
        },
    ];
    assert_eq!(all.iter().filter(|a| a.releases_node()).count(), 1);
}

// ── estado_de_pod — a reading becomes a gate input ─────────────────────────

use super::{ObservacaoPod, ProvaExigida, VistaNo, estado_de_pod};

/// A pod continuously Ready for `secs`. Defaults well above `min_stable` (30s)
/// so callers that do not care about the stability bound get a healthy node.
fn ready_for(secs: u64) -> ObservacaoPod {
    ObservacaoPod {
        found: true,
        ready: true,
        ready_for: Some(Duration::from_secs(secs)),
        lookup_failed: false,
    }
}

/// A kubelet Ready condition is PRESENCE, never a heartbeat.
#[test]
fn a_ready_pod_is_present_not_reporting() {
    assert_eq!(
        estado_de_pod(&ready_for(5)),
        EstadoComponente::Present {
            ready_for: Duration::from_secs(5)
        }
    );
}

#[test]
fn a_missing_pod_is_absent() {
    assert_eq!(
        estado_de_pod(&ObservacaoPod::default()),
        EstadoComponente::Absent
    );
}

#[test]
fn a_found_but_unready_pod_is_absent_not_reporting() {
    let o = ObservacaoPod {
        found: true,
        ready: false,
        ..Default::default()
    };
    assert_eq!(estado_de_pod(&o), EstadoComponente::Absent);
}

/// **The distinction that matters.** A failed read and an empty read produce
/// the same empty result set from a list call. Treating them alike would let a
/// broken watch reclaim healthy nodes wholesale.
#[test]
fn a_failed_lookup_is_indeterminate_never_absent() {
    let o = ObservacaoPod {
        lookup_failed: true,
        ..Default::default()
    };
    assert_eq!(estado_de_pod(&o), EstadoComponente::Indeterminate);
    assert_ne!(estado_de_pod(&o), EstadoComponente::Absent);
}

/// Ready but undatable cannot honour a freshness bound, so it must not pretend
/// to by reporting age zero.
#[test]
fn a_ready_pod_with_no_timestamp_does_not_become_age_zero() {
    let o = ObservacaoPod {
        found: true,
        ready: true,
        ready_for: None,
        lookup_failed: false,
    };
    assert_eq!(estado_de_pod(&o), EstadoComponente::Indeterminate);
    assert_ne!(
        estado_de_pod(&o),
        EstadoComponente::Present {
            ready_for: Duration::ZERO
        }
    );
}

/// A component the gatherer forgot is indeterminate, not absent — the safe
/// reading of a bug in the gatherer.
#[test]
fn an_unobserved_component_is_indeterminate() {
    let vista = VistaNo::new().observando(ComponenteExigido::BreatheHostAgent, &ready_for(1));
    assert_eq!(
        vista.component_state(ComponenteExigido::StorageDriver),
        EstadoComponente::Indeterminate
    );
}

/// End to end over the real gate: a gathered view of a healthy node passes,
/// and the same view with the agent's pod gone does not.
#[test]
fn a_gathered_view_drives_the_real_gate() {
    let gate = ConformanceBinding::fleet_default();
    let healthy = ComponenteExigido::ALL
        .into_iter()
        .fold(VistaNo::new(), |v, c| v.observando(c, &ready_for(600)));
    assert_eq!(
        block_on(gate.check(&candidate(), &healthy)).decision,
        GateDecision::Pass
    );

    let unbreathed = ComponenteExigido::ALL
        .into_iter()
        .fold(VistaNo::new(), |v, c| {
            let o = if c == ComponenteExigido::BreatheHostAgent {
                ObservacaoPod::default()
            } else {
                ready_for(600)
            };
            v.observando(c, &o)
        });
    assert!(!matches!(
        block_on(gate.check(&candidate(), &unbreathed)).decision,
        GateDecision::Pass
    ));
}

// ── bloqueador — the typed "who is blocking" for telemetry ────────────────

/// It must agree with the gate itself. Two answers to one question is how a
/// dashboard ends up contradicting the thing it is watching.
#[test]
fn the_blocker_agrees_with_the_gates_own_verdict() {
    let gate = ConformanceBinding::fleet_default();
    let healthy = ComponenteExigido::ALL
        .into_iter()
        .fold(VistaNo::new(), |v, c| v.observando(c, &ready_for(600)));
    assert!(gate.bloqueador(&healthy).is_none());
    assert_eq!(verdict(&healthy, &gate), GateDecision::Pass);

    for missing in ComponenteExigido::ALL {
        let node = ComponenteExigido::ALL
            .into_iter()
            .fold(VistaNo::new(), |v, c| {
                v.observando(
                    c,
                    &if c == missing {
                        ObservacaoPod::default()
                    } else {
                        ready_for(600)
                    },
                )
            });
        assert_eq!(
            gate.bloqueador(&node),
            Some((missing, EstadoComponente::Absent)),
            "the blocker must name the component the gate is actually waiting on"
        );
        assert!(!matches!(verdict(&node, &gate), GateDecision::Pass));
    }
}

/// It reports the STATE too, so a metric can distinguish "not there yet" from
/// "cannot observe" — different remedies, and the whole reason the enum has
/// both arms.
#[test]
fn the_blocker_carries_the_state_not_just_the_name() {
    let gate = ConformanceBinding::fleet_default();
    let unobservable = ComponenteExigido::ALL
        .into_iter()
        .fold(VistaNo::new(), |v, c| {
            if c == ComponenteExigido::ContainerNetwork {
                v.com_estado(c, EstadoComponente::Indeterminate)
            } else {
                v.observando(c, &ready_for(600))
            }
        });
    assert_eq!(
        gate.bloqueador(&unobservable),
        Some((
            ComponenteExigido::ContainerNetwork,
            EstadoComponente::Indeterminate
        ))
    );

    let too_fresh = ComponenteExigido::ALL
        .into_iter()
        .fold(VistaNo::new(), |v, c| {
            v.observando(
                c,
                &ready_for(if c == ComponenteExigido::StorageDriver {
                    5
                } else {
                    600
                }),
            )
        });
    assert_eq!(
        gate.bloqueador(&too_fresh),
        Some((
            ComponenteExigido::StorageDriver,
            EstadoComponente::Present {
                ready_for: Duration::from_secs(5)
            }
        ))
    );
}
