//! `breathe-auction` — the predict → optimize → auction decision layer
//! (docs/PROVISIONING.md §2.4).
//!
//! **What is solved vs deferred — stated honestly (the critique's A1/B1).** The
//! *single-forma scalar* decision is the proven band law lifted one level: run
//! `breathe_control::decide` on a forma's `(used, capacity)` and map the
//! [`Decision`] to a [`DecisaoForma`]. That is [`BandLeiloeiro`], and it inherits
//! the band law's safety-clamp proof (the K2 reuse). The *cross-forma auction*
//! — choosing among spot/on-demand/accelerator/region under a cost+latency+risk
//! Pareto frontier, with bid strategy, spot-interruption handling, and
//! multi-cloud arbitrage — is **newly-authored arbitration the band-law proof
//! does NOT cover**, and its mechanics are genuinely unsolved. This crate ships
//! the trait ([`Otimizador`]) but **no general impl**; that is the project's
//! center of gravity (M4), not a milestone of equal weight to a forma. Likewise
//! [`Previsor`] ships with only a reactive (no-forecast) reference impl; a real
//! forecaster has its own stability concerns the band-law proof does not transfer
//! to (§5.3 — horizon ≥ relief-latency, mispredict asymmetry, cost-thrash).

use breathe_control::{decide, BandConfig, Decision};
use breathe_provider::Forma;

// ============================================================================
// DecisaoForma — the auctioneer's typed verdict.
// ============================================================================

/// What the auction decides for one shape this tick. Routed (in the controller)
/// through breathe's existing `RemediationPolicy` lattice — `EnvelopeExausto`
/// only ever escalates (never auto-corrects past the budget).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecisaoForma {
    /// In-band — hold; provision nothing.
    Manter,
    /// Grow this shape by `delta` units (the band law said low headroom).
    Crescer { forma: Forma, delta: u64 },
    /// Shrink this shape by `delta` units; `drain` = cordon→drain first (PDB-aware).
    Encolher { forma: Forma, delta: u64, drain: bool },
    /// Replace one shape with another (e.g. spot → on-demand on interruption).
    /// Reserved for M3+; the single-forma `BandLeiloeiro` never emits it.
    Reformar { from: Forma, to: Forma, delta: u64 },
    /// Demand exceeds the envelope — escalate (never silently under-provision).
    /// `shortfall` is the units the envelope could not cover.
    EnvelopeExausto { forma: Forma, shortfall: u64 },
}

// ============================================================================
// Previsor — demand prediction (reactive reference impl; forecaster deferred).
// ============================================================================

/// A demand forecast for one shape. M-step ships only the immediate term; the
/// `near`/`medium` horizons (and their own stability story) land with the real
/// forecaster (§5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Previsao {
    /// Current demand (scheduled + pending), in the forma's unit.
    pub immediate_used: u64,
    /// Current provisioned ceiling (the `Densa` envelope), in the forma's unit.
    pub capacity: u64,
}

/// Predicts demand for a shape. Pure — no I/O (so the convergence proof holds).
pub trait Previsor: Send + Sync {
    fn predict(&self, sample_used: u64, sample_capacity: u64) -> Previsao;
}

/// The reference predictor: **reactive — no forecast.** It echoes the current
/// sample. A forecasting `Previsor` (the predictive-grow successor) is deferred
/// because a forecaster is a dynamical system with its own lag/oscillation
/// concerns the band-law static-plant proof does NOT cover (§5.3).
#[derive(Debug, Clone, Copy, Default)]
pub struct ReactivePrevisor;

impl Previsor for ReactivePrevisor {
    fn predict(&self, sample_used: u64, sample_capacity: u64) -> Previsao {
        Previsao { immediate_used: sample_used, capacity: sample_capacity }
    }
}

// ============================================================================
// Leiloeiro — the auctioneer. Single-forma = SOLVED (the band law lifted).
// ============================================================================

/// The auctioneer: decide what to do for a shape given a forecast. Pure.
pub trait Leiloeiro: Send + Sync {
    fn decide(&self, forma: Forma, previsao: &Previsao, cfg: &BandConfig) -> DecisaoForma;
}

/// The **single-forma** auctioneer — the SOLVED case. It runs the proven band law
/// on the forma's `(used, capacity)` and maps the [`Decision`] to a
/// [`DecisaoForma`], inheriting the band law's safety-clamp guarantee verbatim
/// (the K2 reuse; cf. `breathe-provider`'s shape-blindness proof). No cost, no
/// alternatives, no contention — those need [`Otimizador`], which is deferred.
#[derive(Debug, Clone, Copy, Default)]
pub struct BandLeiloeiro;

impl Leiloeiro for BandLeiloeiro {
    fn decide(&self, forma: Forma, previsao: &Previsao, cfg: &BandConfig) -> DecisaoForma {
        match decide(previsao.immediate_used, previsao.capacity, cfg) {
            Decision::Hold | Decision::NoSafeShrink { .. } | Decision::NoLimit => DecisaoForma::Manter,
            Decision::Grow { from, to } => DecisaoForma::Crescer { forma, delta: to.saturating_sub(from) },
            Decision::Shrink { from, to } => {
                DecisaoForma::Encolher { forma, delta: from.saturating_sub(to), drain: true }
            }
            // At the ceiling and still wanting more ⇒ the envelope is exhausted:
            // escalate, never silently under-provision (the never-over-commit peer
            // of never-OOM). The shortfall is the demand the envelope can't cover:
            // `need = ⌈used / setpoint⌉` (the capacity to keep util ≤ setpoint),
            // minus the ceiling we're stuck at.
            Decision::AtCeiling { current } => {
                let setpoint = if cfg.setpoint <= 0.0 { 1.0 } else { cfg.setpoint };
                #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
                let need = ((previsao.immediate_used as f64) / setpoint).ceil() as u64;
                let need = need.max(current);
                DecisaoForma::EnvelopeExausto { forma, shortfall: need.saturating_sub(current) }
            }
        }
    }
}

// ============================================================================
// Otimizador — the multi-forma joint planner. DEFERRED (no general impl).
// ============================================================================

/// A scored candidate shape on the cost/latency/risk frontier.
#[derive(Debug, Clone, PartialEq)]
pub struct Proposta {
    pub forma: Forma,
    pub delta: u64,
    /// Estimated cost (cents) of this proposal — sourced from the fleet
    /// `attribution-forge`/`commitment-forge` plane, NOT a parallel cost model.
    pub cost_cents: u64,
    /// Provisioning dead-time (seconds) — the predictor's look-ahead floor.
    pub relief_latency_secs: u64,
    /// Why this proposal (for the operator-facing receipt).
    pub rationale: String,
}

/// The **cross-forma joint planner** — the DEFERRED hard arbitration (thesis
/// P3/P7). Given a forecast, the live inventory, and the envelope, rank the
/// candidate shapes on a `(cost, -latency, buffer)` Pareto frontier. **No general
/// impl ships** — coupled dimensions have no derivable joint safe-set (the thesis
/// §154 scope line), so this is authored arbitration with its own (weaker, named)
/// safety story, landing at M4. The trait exists so the loop has the right shape;
/// implementing it well is the project's center of gravity, not a footnote.
pub trait Otimizador: Send + Sync {
    fn optimize(&self, previsoes: &[(Forma, Previsao)]) -> Vec<Proposta>;
}

#[cfg(test)]
mod tests;
