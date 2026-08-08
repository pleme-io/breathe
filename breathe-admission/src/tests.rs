use super::{
    classify, Admitido, Allocatable, CapacidadeProof, Descoberto, FaseRecurso, GateDecision, Forma, Portao,
    PortaoKind, ReciboGate, Recurso, ResourceId, StubGate, Validando, ValidationStep, Viveiro,
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
    let node = NodeRef { name: "node-1".into(), allocatable: 64 };
    let cand = candidate();
    let gate = CapacidadeProof { required_floor: 8 };
    let recibo = block_on(gate.check(&cand, &node));
    assert_eq!(recibo.decision, GateDecision::Pass);
    match classify(cand, &[recibo], 3) {
        ValidationStep::Ready(pronto) => {
            let cert = pronto.admit(node);
            assert_eq!(cert.fase(), FaseRecurso::Admitido);
            assert!(cert.evidence().iter().any(|r| r.kind == PortaoKind::CapacidadeProof));
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
    let node = NodeRef { name: "tiny".into(), allocatable: 2 };
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
    let node = NodeRef { name: "n".into(), allocatable: 64 };
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
    assert!(matches!(classify(candidate(), &receipts, 5), ValidationStep::Rejected(_)));
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
    let receipts: Vec<_> = PortaoKind::ALL.iter().map(|&k| ReciboGate::pass(k)).collect();
    assert!(matches!(classify(candidate(), &receipts, 3), ValidationStep::Ready(_)));
}

// ── FSM convergence (via the shared shigoto-fsm forcing-function) ──
// The hand-rolled BFS reachability + terminal-soundness + closed-graph walks were
// replaced by the fleet `shigoto_fsm` harness (FaseRecurso impls ConvergentFsm).
// One call proves: closed graph, terminals-have-no-successors-and-are-good, no
// dead-end traps, AND every reachable phase reaches a good terminal — the same
// guarantees, now from the shared primitive instead of four copies fleet-wide.

#[test]
fn admission_fsm_is_convergent() {
    shigoto_fsm::assert_convergent_fsm::<FaseRecurso>()
        .expect("the resource-admission FSM must be convergent (every phase reaches a good terminal)");
}

/// The domain-specific count still pinned locally: exactly three good terminals
/// (Aposentado retire / Rejeitado refuse / Expirado timeout) — a property of THIS
/// FSM, not the generic convergence harness.
#[test]
fn exactly_three_terminals() {
    let terminals = FaseRecurso::ALL.into_iter().filter(|f| f.is_terminal()).count();
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
                .map(|c| (c, EstadoComponente::Reporting { last_report_age: Duration::from_secs(5) }))
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
        self.0.get(&c).copied().unwrap_or(EstadoComponente::Indeterminate)
    }
}

fn verdict(node: &FakeNode, gate: &ConformanceBinding) -> GateDecision {
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
    let node = FakeNode::breathed()
        .with(ComponenteExigido::BreatheHostAgent, EstadoComponente::Absent);
    assert!(
        !matches!(verdict(&node, &ConformanceBinding::fleet_default()), GateDecision::Pass),
        "an unbreathed node reaching Pass is the gap this gate exists to close"
    );
}

/// Gate-0 state 3 — present is not reporting. An agent that registered at boot
/// and died leaves the node ungoverned while looking identical to a healthy one.
#[test]
fn an_agent_that_stopped_reporting_does_not_pass() {
    let node = FakeNode::breathed().with(
        ComponenteExigido::BreatheHostAgent,
        EstadoComponente::Reporting { last_report_age: Duration::from_secs(3600) },
    );
    assert!(!matches!(
        verdict(&node, &ConformanceBinding::fleet_default()),
        GateDecision::Pass
    ));
}

/// Gate-0 state 5 — "can't tell" must not round up to "fine".
#[test]
fn an_unobservable_component_defers_rather_than_passing() {
    let node = FakeNode::breathed()
        .with(ComponenteExigido::ContainerNetwork, EstadoComponente::Indeterminate);
    assert!(matches!(
        verdict(&node, &ConformanceBinding::fleet_default()),
        GateDecision::Defer { .. }
    ));
}

/// A node still coming up DEFERS — it must not be rejected, because the
/// DaemonSet is landing and the defer budget is what bounds the wait.
#[test]
fn a_booting_node_defers_and_is_not_rejected() {
    let node = FakeNode::breathed()
        .with(ComponenteExigido::StorageDriver, EstadoComponente::Absent);
    assert!(matches!(
        verdict(&node, &ConformanceBinding::fleet_default()),
        GateDecision::Defer { .. }
    ));
}

/// The vacuous guard, refused explicitly: a gate configured to require nothing
/// would report green over every node in the fleet.
#[test]
fn a_gate_requiring_nothing_is_a_reject_not_a_pass() {
    let gate = ConformanceBinding { required: vec![], max_report_age: Duration::from_secs(90) };
    assert!(matches!(verdict(&FakeNode::breathed(), &gate), GateDecision::Reject { .. }));
}

/// Freshness is the gate's policy, not the observer's — the same observation
/// passes or defers purely on the configured bound.
#[test]
fn the_freshness_bound_belongs_to_the_gate() {
    let node = FakeNode::breathed().with(
        ComponenteExigido::BreatheHostAgent,
        EstadoComponente::Reporting { last_report_age: Duration::from_secs(120) },
    );
    let strict = ConformanceBinding {
        required: vec![ComponenteExigido::BreatheHostAgent],
        max_report_age: Duration::from_secs(90),
    };
    let lax = ConformanceBinding {
        required: vec![ComponenteExigido::BreatheHostAgent],
        max_report_age: Duration::from_secs(300),
    };
    assert!(matches!(verdict(&node, &strict), GateDecision::Defer { .. }));
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
    assert_eq!(ConformanceBinding::fleet_default().required.len(), ComponenteExigido::ALL.len());
}
