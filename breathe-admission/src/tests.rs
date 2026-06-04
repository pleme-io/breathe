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

// ── FSM reflection / convergence (the BFS reachability forcing-function) ──

#[test]
fn terminals_are_exactly_three_and_have_no_successors() {
    let terminals: Vec<_> = FaseRecurso::ALL.into_iter().filter(|f| f.is_terminal()).collect();
    assert_eq!(terminals.len(), 3, "expected exactly 3 terminals, got {terminals:?}");
    for f in terminals {
        assert!(f.legal_successors().is_empty(), "{f} is terminal but has successors");
        assert!(f.is_good_terminal(), "{f} terminal must be good");
    }
}

#[test]
fn every_reachable_phase_reaches_a_good_terminal() {
    // The convergence claim made mechanical: from EVERY phase, a good terminal is
    // reachable over the legal edges. This is non-vacuous only because Validando
    // has the Rejeitado/Expirado exits (the FSM-completeness fix).
    for start in FaseRecurso::ALL {
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![start];
        let mut reached_good = false;
        while let Some(f) = stack.pop() {
            if !seen.insert(f) {
                continue;
            }
            if f.is_good_terminal() {
                reached_good = true;
            }
            stack.extend(f.legal_successors().iter().copied());
        }
        assert!(reached_good, "phase {start} cannot reach a good terminal — stuck state!");
    }
}

#[test]
fn legal_successors_reference_only_valid_phases() {
    let all: std::collections::HashSet<_> = FaseRecurso::ALL.into_iter().collect();
    for f in FaseRecurso::ALL {
        for &s in f.legal_successors() {
            assert!(all.contains(&s), "{f} → {s} references an unknown phase");
        }
    }
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
