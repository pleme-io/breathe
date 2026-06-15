use super::{reconcile_forma, FormaTick};
use breathe_admission::{Allocatable, CapacidadeProof, Portao, PortaoKind, StubGate, Viveiro};
use breathe_auction::{BandLeiloeiro, ReactivePrevisor};
use breathe_control::BandConfig;
use breathe_provider::{Forma, FormaSample, ProvisionReceipt, Provedor, ProviderError};

/// A minimal no-dependency executor (the loop's awaited futures are immediately
/// ready in tests — the mocks never block). Keeps the crate runtime-free.
pub(crate) fn block_on<F: std::future::Future>(fut: F) -> F::Output {
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

/// An observe-only `Provedor` double — reports a fixed `(used, capacity)` and
/// DryRuns every provision (the M0 shadow behaviour).
struct MockProvedor {
    used: u64,
    capacity: u64,
}
#[async_trait::async_trait]
impl Provedor for MockProvedor {
    async fn observe(&self) -> Result<FormaSample, ProviderError> {
        Ok(FormaSample { used: self.used, capacity: self.capacity })
    }
    async fn provision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        Ok(ProvisionReceipt::DryRun { would: n as i64 })
    }
    async fn deprovision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        Ok(ProvisionReceipt::DryRun { would: -(n as i64) })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NodeRef {
    pub(crate) allocatable: u64,
}
impl Allocatable for NodeRef {
    fn allocatable(&self) -> u64 {
        self.allocatable
    }
}

pub(crate) fn open_cfg() -> BandConfig {
    BandConfig {
        grow_above: 0.85,
        shrink_below: 0.70,
        setpoint: 0.80,
        grow_factor: 1.25,
        shrink_factor: 0.90,
        floor_bytes: 1,
        ceiling_bytes: 1_000_000,
    }
}

pub(crate) fn capacidade_gate(floor: u64) -> Vec<Box<dyn Portao<NodeRef>>> {
    vec![Box::new(CapacidadeProof { required_floor: floor })]
}

#[test]
fn grow_provisions_and_admits_into_the_viveiro() {
    // util 0.90 > grow_above → Crescer; the new units pass CapacidadeProof and
    // land in the Viveiro VALIDATED (never raw).
    let provedor = MockProvedor { used: 90, capacity: 100 };
    let mut viveiro: Viveiro<NodeRef> = Viveiro::new();
    let tick = block_on(reconcile_forma(
        Forma::NodeOnDemand,
        &provedor,
        &ReactivePrevisor,
        &BandLeiloeiro,
        &open_cfg(),
        &capacidade_gate(8),
        3,
        &mut viveiro,
        |_id| NodeRef { allocatable: 64 },
    ));
    match tick {
        FormaTick::Grew { forma, requested, admitted, rejected } => {
            assert_eq!(forma, Forma::NodeOnDemand);
            assert!(requested > 0);
            assert_eq!(admitted, requested, "every provisioned unit cleared the gate");
            assert_eq!(rejected, 0);
            assert_eq!(viveiro.len() as u64, admitted, "the pool holds exactly the admitted units");
        }
        other => panic!("expected Grew, got {other:?}"),
    }
}

#[test]
fn an_undersized_node_is_provisioned_but_never_admitted() {
    // The grow happens, but CapacidadeProof rejects (floor 999 > allocatable 64),
    // so NOTHING enters the pool — a provisioned-but-unvalidated unit is not usable.
    let provedor = MockProvedor { used: 90, capacity: 100 };
    let mut viveiro: Viveiro<NodeRef> = Viveiro::new();
    let tick = block_on(reconcile_forma(
        Forma::NodeOnDemand,
        &provedor,
        &ReactivePrevisor,
        &BandLeiloeiro,
        &open_cfg(),
        &capacidade_gate(999),
        3,
        &mut viveiro,
        |_id| NodeRef { allocatable: 64 },
    ));
    match tick {
        FormaTick::Grew { admitted, rejected, requested, .. } => {
            assert_eq!(admitted, 0, "an undersized node must NOT be admitted");
            assert_eq!(rejected, requested);
            assert!(viveiro.is_empty(), "the pool stays empty — no unvalidated unit");
        }
        other => panic!("expected Grew, got {other:?}"),
    }
}

#[test]
fn a_stub_gate_keeps_everything_out_fail_safe() {
    // An unimplemented gate Defers; the unit never reaches Pronto, so nothing is
    // admitted while a gate is a stub (fail-safe — never a silent admit).
    let provedor = MockProvedor { used: 90, capacity: 100 };
    let mut viveiro: Viveiro<NodeRef> = Viveiro::new();
    let gates: Vec<Box<dyn Portao<NodeRef>>> = vec![Box::new(StubGate(PortaoKind::QuotaCheck))];
    let tick = block_on(reconcile_forma(
        Forma::NodeOnDemand,
        &provedor,
        &ReactivePrevisor,
        &BandLeiloeiro,
        &open_cfg(),
        &gates,
        3,
        &mut viveiro,
        |_id| NodeRef { allocatable: 64 },
    ));
    assert!(matches!(tick, FormaTick::Grew { admitted: 0, .. }));
    assert!(viveiro.is_empty());
}

#[test]
fn in_band_demand_holds() {
    let provedor = MockProvedor { used: 75, capacity: 100 };
    let mut viveiro: Viveiro<NodeRef> = Viveiro::new();
    let tick = block_on(reconcile_forma(
        Forma::NodeOnDemand,
        &provedor,
        &ReactivePrevisor,
        &BandLeiloeiro,
        &open_cfg(),
        &capacidade_gate(8),
        3,
        &mut viveiro,
        |_id| NodeRef { allocatable: 64 },
    ));
    assert_eq!(tick, FormaTick::Held);
    assert!(viveiro.is_empty());
}

#[test]
fn demand_beyond_the_envelope_escalates() {
    // capped ceiling (capacity == ceiling == 100) + demand 200 → EnvelopeExhausted,
    // never a silent under-provision.
    let provedor = MockProvedor { used: 200, capacity: 100 };
    let capped = BandConfig { ceiling_bytes: 100, ..open_cfg() };
    let mut viveiro: Viveiro<NodeRef> = Viveiro::new();
    let tick = block_on(reconcile_forma(
        Forma::NodeOnDemand,
        &provedor,
        &ReactivePrevisor,
        &BandLeiloeiro,
        &capped,
        &capacidade_gate(8),
        3,
        &mut viveiro,
        |_id| NodeRef { allocatable: 64 },
    ));
    match tick {
        FormaTick::EnvelopeExhausted { forma, shortfall } => {
            assert_eq!(forma, Forma::NodeOnDemand);
            assert_eq!(shortfall, 150); // ⌈200/0.8⌉ − 100
        }
        other => panic!("expected EnvelopeExhausted, got {other:?}"),
    }
}
