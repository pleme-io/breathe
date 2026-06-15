//! `sim` — a deterministic, in-memory node-provisioning test bed.
//!
//! [`SimProvedor`] is the **go-live gate** for the node tier. The shipped
//! [`MockProvedor`](crate::tests) double is observe-only: it reports a fixed
//! `(used, capacity)` and `DryRun`s every action, so it can prove the loop's
//! *single-tick* decision but not that the loop CONVERGES. Real node
//! provisioning has a **dead-time** — a freshly provisioned node is not usable
//! capacity until it boots and joins (`relief_latency_secs`) — and graceful
//! removal has a drain window. A loop that ignores either oscillates or
//! under-provisions.
//!
//! `SimProvedor` models the full node lifecycle with a configurable boot/drain
//! latency, so [`reconcile_forma`](crate::reconcile_forma) can be proven
//! convergent over many ticks BEFORE any real cloud `Provedor` (hcloud, AWS via
//! magma `Plan`) ships. It is a simulator, not a production provider — but it is
//! real code (not `#[cfg(test)]`), so integration tests, examples, and a future
//! `KwokProvedor` (kwok-backed test cluster) share exactly one lifecycle model.
//!
//! The lifecycle mirrors `theory/BREATHABILITY-NODE-LIFECYCLE.md`:
//!
//! ```text
//!   provision(n) ─► Provisioning(boot) ──advance×boot──► Ready ──(counts as capacity)
//!   deprovision(n) ─► Ready → Draining(drain) ──advance×drain──► Terminated (removed)
//! ```
//!
//! `capacity` = count of `Ready` nodes (the usable, provisioned ceiling).
//! Provisioning nodes are *booked but not yet usable* — the dead-time made
//! concrete. Draining nodes have been committed for removal and no longer count
//! as capacity. `used` (demand) is set by the scenario and models workload
//! shifting independently of node phase.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::doc_markdown
)]

use breathe_provider::{FormaSample, Provedor, ProvisionReceipt, ProviderError};
use std::sync::Mutex;

/// A simulated node's lifecycle phase (the node-tier peer of a pod's phase).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimNodePhase {
    /// Requested + booting. NOT usable capacity until it reaches [`Ready`](Self::Ready).
    /// `ticks_remaining` counts down the boot dead-time on each [`SimProvedor::advance`].
    Provisioning { ticks_remaining: u64 },
    /// Joined + schedulable — counts toward `capacity`.
    Ready,
    /// Cordoned + draining. Committed for removal, so it no longer counts as
    /// capacity; survives `ticks_remaining` more advances, then is dropped.
    Draining { ticks_remaining: u64 },
}

impl SimNodePhase {
    /// Is this node usable, provisioned capacity right now?
    #[must_use]
    pub fn is_capacity(self) -> bool {
        matches!(self, Self::Ready)
    }
}

#[derive(Debug, Default)]
struct SimState {
    nodes: Vec<SimNodePhase>,
    /// Current demand (the band law's `used`), in node-equivalents.
    demand: u64,
    /// Monotonic provision-action counter — the synthetic `plan_id` source.
    seq: u64,
}

/// A stateful, deterministic node-pool simulator behind the [`Provedor`] seam.
///
/// Construct with [`SimProvedor::new`], drive the loop with
/// [`reconcile_forma`](crate::reconcile_forma), and age the sim one step per
/// reconcile interval with [`advance`](Self::advance). `boot_ticks` / `drain_ticks`
/// model the provider's node-boot and graceful-drain windows in advance-units.
pub struct SimProvedor {
    state: Mutex<SimState>,
    boot_ticks: u64,
    drain_ticks: u64,
}

impl SimProvedor {
    /// A pool seeded with `ready` Ready nodes and a starting `demand`, with the
    /// given boot/drain dead-times in advance-units: a node booting in
    /// `boot_ticks` is `Ready` on the `boot_ticks`-th [`advance`](Self::advance)
    /// (`0` = usable immediately); a node draining in `drain_ticks` is removed on
    /// the `drain_ticks`-th advance (`0` = removed outright, no drain window).
    #[must_use]
    pub fn new(ready: u64, demand: u64, boot_ticks: u64, drain_ticks: u64) -> Self {
        let nodes = vec![SimNodePhase::Ready; ready as usize];
        Self {
            state: Mutex::new(SimState { nodes, demand, seq: 0 }),
            boot_ticks,
            drain_ticks,
        }
    }

    /// Age the simulation one step: a `Provisioning` node booting in `boot_ticks`
    /// advances reaches `Ready` on the `boot_ticks`-th advance; a `Draining` node
    /// is dropped on the `drain_ticks`-th advance. Call once per reconcile interval.
    pub fn advance(&self) {
        let mut s = self.state.lock().expect("sim poisoned");
        s.nodes.retain_mut(|phase| match phase {
            SimNodePhase::Provisioning { ticks_remaining } => {
                if *ticks_remaining <= 1 {
                    *phase = SimNodePhase::Ready;
                } else {
                    *ticks_remaining -= 1;
                }
                true
            }
            SimNodePhase::Draining { ticks_remaining } => {
                if *ticks_remaining <= 1 {
                    false // terminated — removed from the pool
                } else {
                    *ticks_remaining -= 1;
                    true
                }
            }
            SimNodePhase::Ready => true,
        });
    }

    /// Model workload shifting / load change: set the current demand (`used`).
    pub fn set_demand(&self, used: u64) {
        self.state.lock().expect("sim poisoned").demand = used;
    }

    /// Count of nodes in a given predicate — test/observability helper.
    fn count(&self, pred: impl Fn(SimNodePhase) -> bool) -> u64 {
        self.state.lock().expect("sim poisoned").nodes.iter().filter(|p| pred(**p)).count() as u64
    }

    /// Usable, provisioned capacity right now (Ready nodes).
    #[must_use]
    pub fn ready_count(&self) -> u64 {
        self.count(SimNodePhase::is_capacity)
    }

    /// Nodes booked + booting (the relief-latency dead-time in flight).
    #[must_use]
    pub fn provisioning_count(&self) -> u64 {
        self.count(|p| matches!(p, SimNodePhase::Provisioning { .. }))
    }

    /// Nodes cordoned + draining (committed for removal).
    #[must_use]
    pub fn draining_count(&self) -> u64 {
        self.count(|p| matches!(p, SimNodePhase::Draining { .. }))
    }
}

#[async_trait::async_trait]
impl Provedor for SimProvedor {
    async fn observe(&self) -> Result<FormaSample, ProviderError> {
        let s = self.state.lock().expect("sim poisoned");
        let capacity = s.nodes.iter().filter(|p| p.is_capacity()).count() as u64;
        Ok(FormaSample { used: s.demand, capacity })
    }

    async fn provision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        if n == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        let mut s = self.state.lock().expect("sim poisoned");
        for _ in 0..n {
            // boot_ticks == 0 ⇒ usable immediately (no boot dead-time).
            let phase = if self.boot_ticks == 0 {
                SimNodePhase::Ready
            } else {
                SimNodePhase::Provisioning { ticks_remaining: self.boot_ticks }
            };
            s.nodes.push(phase);
        }
        s.seq += 1;
        let plan_id = format!("sim:provision:{}", s.seq);
        Ok(ProvisionReceipt::Applied { delta: n as i64, plan_id })
    }

    async fn deprovision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        if n == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        let mut s = self.state.lock().expect("sim poisoned");
        // Graceful: cordon+drain Ready nodes first (never yank a booting node).
        // A cordoned node leaves usable capacity immediately (it stops being
        // `Ready`); drain_ticks == 0 removes it outright (no drain window).
        let mut released = 0u64;
        if self.drain_ticks == 0 {
            let mut budget = n;
            s.nodes.retain(|phase| {
                if budget > 0 && matches!(phase, SimNodePhase::Ready) {
                    budget -= 1;
                    released += 1;
                    false
                } else {
                    true
                }
            });
        } else {
            for phase in s.nodes.iter_mut() {
                if released == n {
                    break;
                }
                if matches!(phase, SimNodePhase::Ready) {
                    *phase = SimNodePhase::Draining { ticks_remaining: self.drain_ticks };
                    released += 1;
                }
            }
        }
        if released == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        s.seq += 1;
        let plan_id = format!("sim:deprovision:{}", s.seq);
        Ok(ProvisionReceipt::Applied { delta: -(released as i64), plan_id })
    }
}

#[cfg(test)]
mod sim_tests {
    use super::SimProvedor;
    use crate::reconcile_forma;
    use crate::tests::{block_on as run, capacidade_gate, open_cfg, NodeRef};
    use breathe_admission::Viveiro;
    use breathe_auction::{BandLeiloeiro, LinearTrendPrevisor, Previsor, ReactivePrevisor};
    use breathe_provider::{Forma, Provedor, ProvisionReceipt};

    #[test]
    fn provisioned_node_is_not_capacity_until_it_boots() {
        // The relief-latency dead-time made concrete: provision(2) with a 3-tick
        // boot does NOT raise capacity until 3 advances have elapsed.
        let sim = SimProvedor::new(1, 0, 3, 0);
        run(sim.provision(2));
        assert_eq!(sim.ready_count(), 1, "booting nodes are not yet capacity");
        assert_eq!(sim.provisioning_count(), 2);
        sim.advance();
        sim.advance();
        assert_eq!(sim.ready_count(), 1, "still booting at tick 2 of 3");
        sim.advance();
        assert_eq!(sim.ready_count(), 3, "now Ready — capacity is usable");
        assert_eq!(sim.provisioning_count(), 0);
    }

    #[test]
    fn deprovisioned_node_drains_before_it_is_gone() {
        let sim = SimProvedor::new(3, 0, 0, 2);
        let r = run(sim.deprovision(2));
        assert!(matches!(r, Ok(ProvisionReceipt::Applied { delta: -2, .. })));
        // Drained nodes leave capacity immediately but survive the drain window.
        assert_eq!(sim.ready_count(), 1);
        assert_eq!(sim.draining_count(), 2);
        sim.advance();
        sim.advance();
        assert_eq!(sim.draining_count(), 0, "drained + terminated after 2 ticks");
    }

    #[test]
    fn deprovision_is_graceful_never_yanks_a_booting_node() {
        // 1 Ready + 2 booting; deprovision(3) can only drain the 1 Ready node.
        let sim = SimProvedor::new(1, 0, 5, 1);
        run(sim.provision(2));
        let r = run(sim.deprovision(3));
        assert!(matches!(r, Ok(ProvisionReceipt::Applied { delta: -1, .. })), "only the Ready node drains");
        assert_eq!(sim.provisioning_count(), 2, "booting nodes untouched");
    }

    #[test]
    fn the_loop_converges_up_into_the_deadband_with_boot_latency() {
        // Keystone (node tier, WITH dead-time): demand 90, start 100 Ready →
        // util 0.90 > grow_above. The loop provisions; nodes boot over 2 ticks;
        // capacity rises until util re-enters [0.70, 0.85]; then it Holds.
        let sim = SimProvedor::new(100, 90, 2, 0);
        let previsor = ReactivePrevisor;
        let leiloeiro = BandLeiloeiro;
        let cfg = open_cfg();
        let gates = capacidade_gate(0);

        let mut held = false;
        for _ in 0..40 {
            let mut viveiro = Viveiro::new();
            let _tick = run(reconcile_forma(
                Forma::NodeOnDemand,
                &sim,
                &previsor,
                &leiloeiro,
                &cfg,
                &gates,
                3,
                &mut viveiro,
                |_id| NodeRef { allocatable: 1 },
            ));
            sim.advance();
            let cap = sim.ready_count();
            let util = 90.0 / cap as f64;
            if (0.70..=0.85).contains(&util) {
                held = true;
                break;
            }
        }
        let cap = sim.ready_count();
        let util = 90.0 / cap as f64;
        assert!(held, "loop must converge into the deadband; ended at util={util:.3} cap={cap}");
        assert!((0.70..=0.85).contains(&util), "settled out of band: util={util:.3}");
        assert_eq!(sim.draining_count(), 0, "a growing pool drains nothing");
    }

    #[test]
    fn the_loop_converges_down_into_the_deadband_with_drain() {
        // Reverse: demand 40, start 100 Ready → util 0.40 < shrink_below. The
        // loop deprovisions; nodes drain over 1 tick; capacity falls until util
        // re-enters the band; then it Holds.
        let sim = SimProvedor::new(100, 40, 0, 1);
        let previsor = ReactivePrevisor;
        let leiloeiro = BandLeiloeiro;
        let cfg = open_cfg();
        let gates = capacidade_gate(0);

        let mut held = false;
        for _ in 0..60 {
            let mut viveiro = Viveiro::new();
            let _ = run(reconcile_forma(
                Forma::NodeOnDemand,
                &sim,
                &previsor,
                &leiloeiro,
                &cfg,
                &gates,
                3,
                &mut viveiro,
                |_id| NodeRef { allocatable: 1 },
            ));
            sim.advance();
            let cap = sim.ready_count();
            if cap > 0 {
                let util = 40.0 / cap as f64;
                if (0.70..=0.85).contains(&util) {
                    held = true;
                    break;
                }
            }
        }
        let cap = sim.ready_count();
        let util = 40.0 / cap as f64;
        assert!(held, "loop must converge down into the deadband; ended at util={util:.3} cap={cap}");
        assert!((0.70..=0.85).contains(&util), "settled out of band: util={util:.3}");
    }

    #[test]
    fn workload_shift_reconverges_the_pool() {
        // A pool settled in band, then demand jumps (workload shifting onto it):
        // the loop must re-grow back into the band. Proves the sim models load
        // change + the loop tracks it.
        let sim = SimProvedor::new(50, 40, 1, 0);
        let previsor = ReactivePrevisor;
        let leiloeiro = BandLeiloeiro;
        let cfg = open_cfg();
        let gates = capacidade_gate(0);
        let settle = |sim: &SimProvedor, demand: f64| -> bool {
            for _ in 0..40 {
                let mut viveiro = Viveiro::new();
                let _ = run(reconcile_forma(
                    Forma::NodeOnDemand, sim, &previsor, &leiloeiro, &cfg, &gates, 3,
                    &mut viveiro, |_id| NodeRef { allocatable: 1 },
                ));
                sim.advance();
                let cap = sim.ready_count();
                if cap > 0 && (0.70..=0.85).contains(&(demand / cap as f64)) {
                    return true;
                }
            }
            false
        };
        assert!(settle(&sim, 40.0), "initial settle");
        let before = sim.ready_count();
        sim.set_demand(120); // workload shifts onto this pool
        assert!(settle(&sim, 120.0), "re-converge after the load jump");
        assert!(sim.ready_count() > before, "the pool grew to absorb the shifted load");
    }

    /// Drive one pool through a demand ramp, return the PEAK utilisation seen.
    /// Starts comfortably in-band (util ~0.75, a wide Hold zone) and ramps
    /// GENTLY (+1/tick): reactive Holds for many ticks before util crosses the
    /// band, so the warmed-up forecaster's earlier Hold→Grow flip leads it by
    /// ~`horizon` ticks. A steep ramp would pin BOTH in permanent-Grow and tie
    /// (the band law grows by a fixed factor, not by overshoot magnitude).
    fn peak_util_on_ramp<P: Previsor>(boot_ticks: u64, previsor: &P) -> f64 {
        let sim = SimProvedor::new(56, 42, boot_ticks, 0); // 42/56 = 0.75, in-band
        let leiloeiro = BandLeiloeiro;
        let cfg = open_cfg();
        let gates = capacidade_gate(0);
        let mut demand = 42u64;
        let mut peak = 0.0f64;
        for _ in 0..40 {
            sim.set_demand(demand);
            let mut viveiro = Viveiro::new();
            let _ = run(reconcile_forma(
                Forma::NodeOnDemand, &sim, previsor, &leiloeiro, &cfg, &gates, 3,
                &mut viveiro, |_id| NodeRef { allocatable: 1 },
            ));
            sim.advance();
            let cap = sim.ready_count().max(1);
            peak = peak.max(demand as f64 / cap as f64);
            demand += 1; // gentle ramp
        }
        peak
    }

    #[test]
    fn forecasting_beats_reactive_under_boot_latency() {
        // The BU8 keystone: on a gentle demand ramp with a 5-tick boot dead-time,
        // the reactive previsor flips to Grow only AFTER util crosses the band,
        // and the new capacity lands 5 ticks late — so util overshoots. The
        // monotone-safe forecaster projects 5 ticks ahead, flips Grow earlier,
        // and the capacity lands in time — holding a STRICTLY lower peak util.
        let boot = 5;
        let reactive_peak = peak_util_on_ramp(boot, &ReactivePrevisor);
        let forecast_peak = peak_util_on_ramp(boot, &LinearTrendPrevisor::new(4, boot));
        assert!(
            forecast_peak < reactive_peak,
            "forecaster peak {forecast_peak:.3} must beat reactive peak {reactive_peak:.3} under boot latency"
        );
    }
}
