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

/// `quinhão` — the hierarchical, vector-valued, dynamically-rebalancing
/// even-fair-share allocator (the dual of [`BandLeiloeiro`]: the band law DIVIDES
/// a fixed pool among N weighted claimants, instead of sizing one limit). See
/// [`quinhao`] for the full model. Composes the existing band law (the pool's
/// 80/20 band) — it adds no new safety surface.
pub mod quinhao;

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

/// A **monotone-safe linear-trend forecaster** — the predictive successor to
/// [`ReactivePrevisor`], and the answer to §5.3's "horizon ≥ relief-latency".
///
/// A reactive previsor echoes the current demand, so the loop only reacts AFTER
/// demand has already risen — and a node takes `relief_latency` to boot, so a
/// reactively-grown pool is chronically behind during a demand ramp (util
/// overshoots the band until the late capacity finally lands). This previsor
/// keeps a short window of recent `used` samples, estimates the per-tick slope,
/// and projects demand `horizon_ticks` ahead (set ≥ `relief_latency / interval`),
/// so the loop provisions BEFORE the spike and the late capacity is already
/// Ready when demand arrives.
///
/// **The one safety invariant (tier-honest):** the forecast is
/// `max(current, projected)` — it is **never below the current sample**. So this
/// previsor can only ever make the loop provision EARLIER; it can NEVER cause a
/// premature shrink. That removes the §5.3 mispredict-asymmetry + cost-thrash
/// hazard *by construction* (a falling trend collapses to the reactive echo —
/// it never bets on demand dropping), and it means the band law's safety-clamp
/// proof still bounds every action (the previsor only ever raises `used`, and
/// `decide`'s grow path is already proven safe). It is the demand-side peer of
/// the `PredictiveGrow<BandLaw>` limit-side predictor (memory:
/// pangea-database `predictive: true`).
///
/// Pure in the sense that matters for the convergence proof: deterministic given
/// the sample sequence, no I/O, no wall clock (it assumes the controller's fixed
/// tick cadence — `horizon_ticks` is in ticks, not seconds). History lives
/// behind a `Mutex` for interior mutability under the `&self` trait signature.
#[derive(Debug)]
pub struct LinearTrendPrevisor {
    window: usize,
    horizon_ticks: u64,
    history: std::sync::Mutex<std::collections::VecDeque<u64>>,
}

impl LinearTrendPrevisor {
    /// `window` = how many recent samples to keep for the slope estimate (≥ 2 to
    /// forecast at all); `horizon_ticks` = how many ticks ahead to project (set
    /// ≥ `relief_latency_secs / reconcile_interval_secs` or provisioning is
    /// always late — §5.3, thesis P8).
    #[must_use]
    pub fn new(window: usize, horizon_ticks: u64) -> Self {
        Self {
            window: window.max(2),
            horizon_ticks,
            history: std::sync::Mutex::new(std::collections::VecDeque::new()),
        }
    }
}

impl Previsor for LinearTrendPrevisor {
    fn predict(&self, sample_used: u64, sample_capacity: u64) -> Previsao {
        let mut h = self.history.lock().expect("previsor history poisoned");
        h.push_back(sample_used);
        while h.len() > self.window {
            h.pop_front();
        }
        // Need ≥ 2 samples to estimate a slope; until then, echo (reactive).
        let projected = if h.len() < 2 {
            sample_used
        } else {
            let oldest = *h.front().expect("len ≥ 2");
            let newest = *h.back().expect("len ≥ 2");
            let span = (h.len() - 1) as i64; // ticks between oldest and newest
            #[allow(clippy::cast_possible_wrap)]
            let slope_num = newest as i64 - oldest as i64; // per-`span`-ticks delta
            #[allow(clippy::cast_possible_wrap)]
            let proj = newest as i64 + slope_num * self.horizon_ticks as i64 / span;
            #[allow(clippy::cast_sign_loss)]
            let proj = proj.max(0) as u64;
            // MONOTONE-SAFE: never forecast below the current sample. A falling
            // trend collapses to the reactive echo — never a premature shrink.
            proj.max(sample_used)
        };
        Previsao { immediate_used: projected, capacity: sample_capacity }
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
            // Warmup + Throttled are unreachable here — the free `decide()` has no
            // warmup/throttle gate (those live in `plan_tick`); a node Forma has no
            // restart/throttle/warmup concept anyway. Mapped to Manter (hold) for
            // exhaustiveness, never silent.
            Decision::Hold | Decision::NoSafeShrink { .. } | Decision::NoLimit | Decision::Warmup { .. } | Decision::Throttled { .. } => DecisaoForma::Manter,
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

// ============================================================================
// ParetoOtimizador — the first real Otimizador impl (Pareto-frontier scan).
// ============================================================================

/// Cost source for a candidate `Forma`, injected rather than computed here —
/// per [`Proposta`]'s own rule, cost is "sourced from the fleet
/// `attribution-forge`/`commitment-forge` plane, NOT a parallel cost model."
/// This is this crate's `Previsor`/`Leiloeiro` pattern (a pure trait, real
/// impls injected, tests mock) applied to price data instead of demand data.
pub trait PriceOracle: Send + Sync {
    /// Cost in cents to run `forma` for `duration_secs`.
    fn cost_cents(&self, forma: Forma, duration_secs: u64) -> u64;
}

/// Interruption-frequency source for a candidate `Forma` — the same
/// injected-not-computed seam as [`PriceOracle`], applied to risk data instead
/// of price data.
///
/// **Where a real impl comes from (grounded, not hypothetical).** The sibling
/// `pleme-io/leilao` crate already ships a sealed, tested model for exactly
/// this number: `leilao::aws::InterruptionBucket` types AWS's spot-advisor
/// frequency-of-interruption buckets (`spot-advisor-data.json` `r=0..4`) and
/// `InterruptionBucket::representative_ppm()` maps them to a monotone
/// parts-per-million hazard (`leilao/src/aws.rs`); `leilao::refined::Hazard`
/// and `leilao::survival` go further, sealing the bucket into a Poisson
/// reclaim rate + a survival probability. **Tier-honest:** leilao's own docs
/// (`leilao/src/aws.rs` module comment, `AWS_SIGNAL_LEDGER`) mark the *live*
/// AWS fetch (the SDK call / published-dataset read) as `source-design` — "the
/// seam is present, no `aws-sdk-ec2` is compiled in" — so this is a real,
/// sealed, unit-tested CONVERSION model, not yet a live DATA FEED. Nothing in
/// `leilao` or `breathe` calls the AWS spot-advisor API today.
///
/// This trait deliberately does NOT take a dependency on `leilao` (a separate
/// repo/crate, not a `breathe` workspace member) — pulling in a cross-repo
/// dependency is a bigger architectural decision than "wire a PickPolicy
/// variant," and `PriceOracle` itself sets the precedent that this crate
/// defines the SEAM, not the data source. A real impl can trivially wrap
/// `leilao::aws::InterruptionBucket::representative_ppm()` (same ppm unit,
/// chosen for exactly that reason) once leilao's live fetch lands, or wrap
/// any other calibrated source in the meantime — this crate only needs the
/// number, never how it was produced.
pub trait InterruptionOracle: Send + Sync {
    /// Monthly interruption frequency for `forma`, in parts-per-million
    /// (matching `leilao::aws::InterruptionBucket::representative_ppm()`'s
    /// unit convention 1:1, so a real impl needs no conversion).
    fn interruption_ppm(&self, forma: Forma) -> u64;
}

/// How [`ParetoOtimizador`] resolves a single winner from the Pareto frontier.
/// This is where the `CostFloor`/`TimeFloor` duality
/// (`theory/BREATHABILITY.md`'s auction-objective section) becomes an actual
/// decision instead of a static enum: `MinCost` auctions hard on price;
/// `MinCostUnderDeadline` auctions hard on price *subject to* a latency floor
/// — the mechanism "choose performance when latency is the binding constraint"
/// resolves to, not a separate code path.
// `Eq` dropped (not `PartialEq, Eq`): `MinCostRiskAdjusted`'s `risk_weight: f64`
// has no total `Eq` — `f64` implements only `PartialEq`. No caller compares
// `PickPolicy` for equality today (grep-verified), so this is a pure widening.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PickPolicy {
    /// Cheapest point on the frontier, latency unconstrained (`CostFloor`).
    MinCost,
    /// Cheapest point on the frontier whose `relief_latency_secs` fits the
    /// deadline (`TimeFloor`'s actual selection mechanism — a build-burst
    /// workload's deadline is short, so this naturally rules out the
    /// cheap-but-slow candidates a plain `MinCost` would otherwise pick).
    MinCostUnderDeadline { deadline_secs: u64 },
    /// Risk-adjusted cost: pick the frontier candidate minimizing
    /// `cost_cents + risk_weight * interruption_ppm` (interruption sourced
    /// from [`ParetoOtimizador::interruption`], parts-per-million per
    /// [`InterruptionOracle`]). `risk_weight` is a cents-per-ppm tradeoff
    /// knob — 0.0 degrades exactly to [`PickPolicy::MinCost`].
    ///
    /// **Deliberately a linear penalty at the PICK step, not a 3rd Pareto
    /// dominance axis and not a Markowitz mean-variance portfolio.** A real
    /// variance-aware allocator (covariance across candidates, portfolio
    /// apportionment) already exists, sealed and tested, in the sibling
    /// `pleme-io/leilao` crate's `portfolio.rs` — that is a different tier of
    /// machinery (a joint allocation across MANY pools at once) than this
    /// auctioneer's job (pick ONE winning `Forma` per tick). Reaching for
    /// Markowitz here would be exactly the over-build this crate's own
    /// scope discipline forbids; this variant is the smallest sufficient
    /// step that makes interruption risk a first-class input to the pick.
    ///
    /// **Honest scope today (found while implementing, not assumed up
    /// front — see `breathe-auction/src/tests.rs`'s `MinCostRiskAdjusted`
    /// section).** `relief_latency_secs` is the SAME `cfg.warmup_seconds`
    /// for every candidate within one [`Otimizador::optimize`] call (a
    /// pre-existing, already-documented simplification). So whenever two
    /// candidates' `cost_cents` differ, [`ParetoOtimizador::dominates`]
    /// already collapses the frontier to the single cheapest one BEFORE
    /// this pick policy ever runs — a risk-adjusted pick cannot resurrect a
    /// pricier candidate the frontier already dropped. Today this variant
    /// therefore only has observable effect when candidates tie on cost
    /// (it then breaks the tie toward the lower-risk one). Once
    /// `relief_latency_secs` is genuinely per-forma (letting a slower-but-
    /// safer candidate legitimately survive dominance), this exact same
    /// arm re-ranks the wider resulting frontier for free — no change
    /// needed here when that lands.
    MinCostRiskAdjusted { risk_weight: f64 },
}

/// The **Pareto-frontier auctioneer** — classical multi-objective optimization
/// (skyline / dominance filtering), no ML, fully deterministic: a [`Proposta`]
/// is dominated when another candidate is no worse on *both* axes (cost,
/// latency) and strictly better on at least one — a rational chooser under
/// *any* weighting of the two axes never picks a dominated candidate, so
/// dropping them is lossless, not a heuristic approximation. What survives is
/// the true cost/latency trade-off frontier; [`PickPolicy`] then resolves the
/// single winner the controller acts on.
///
/// **Tier-honest scope (do not round up).** The FRONTIER itself still ranks
/// on exactly the two axes [`Proposta`] carries (`cost_cents`,
/// `relief_latency_secs`) — dominance filtering stays 2-axis. The doc's own
/// three-axis frontier (`cost, -latency, buffer`) and a real Markowitz-style
/// mean-variance risk adjustment under the auction's `$/mo` cost-variance
/// budget (`theory/BREATHABILITY.md` §100% spot reliability) are NOT what
/// [`PickPolicy::MinCostRiskAdjusted`] is — see that variant's own doc
/// comment for why a full covariance-aware portfolio is out of scope here
/// (it exists, sealed and tested, one layer up in `pleme-io/leilao`'s
/// `portfolio.rs`). What ships here is smaller and honestly-scoped: an
/// OPTIONAL, injected [`InterruptionOracle`] the `MinCostRiskAdjusted` pick
/// policy reads to apply a linear risk penalty when choosing the single
/// winner off the (still 2-axis) frontier — [`interruption`](Self::interruption)
/// is `None` by default, in which case that policy degrades to plain
/// [`PickPolicy::MinCost`] rather than fabricating a risk number (see
/// [`InterruptionOracle`]'s doc comment for exactly which real source, and
/// at which tier, exists today).
pub struct ParetoOtimizador<P: PriceOracle> {
    pub price: P,
    pub cfg: BandConfig,
    pub pick: PickPolicy,
    /// Interruption-frequency source for [`PickPolicy::MinCostRiskAdjusted`].
    /// `None` (the [`Self::new`] default) ⇒ that policy treats every
    /// candidate as zero risk, degrading cleanly to [`PickPolicy::MinCost`]
    /// — never a silent wrong answer, never a fabricated number. Set via
    /// [`Self::with_interruption_oracle`].
    pub interruption: Option<Box<dyn InterruptionOracle>>,
}

/// Typed rationale renderer for a grow candidate — the one allowed `write!()`
/// surface for composed text (★★ TYPED EMISSION: a `Display` impl, never a
/// bare `format!()` string composition).
struct GrowRationale {
    delta: u64,
    forma: Forma,
    cost_cents: u64,
    duration_secs: u64,
}

impl std::fmt::Display for GrowRationale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "grow {} units of {:?}: {}c over {}s",
            self.delta, self.forma, self.cost_cents, self.duration_secs
        )
    }
}

impl<P: PriceOracle> ParetoOtimizador<P> {
    #[must_use]
    pub fn new(price: P, cfg: BandConfig, pick: PickPolicy) -> Self {
        Self { price, cfg, pick, interruption: None }
    }

    /// Attach an [`InterruptionOracle`] — required for
    /// [`PickPolicy::MinCostRiskAdjusted`] to actually weigh risk (without
    /// one it degrades to [`PickPolicy::MinCost`]). Builder-style so the
    /// common no-risk-data construction (`Self::new`) stays a 3-argument
    /// call.
    #[must_use]
    pub fn with_interruption_oracle(mut self, oracle: impl InterruptionOracle + 'static) -> Self {
        self.interruption = Some(Box::new(oracle));
        self
    }

    /// `a` Pareto-dominates `b`: no worse on both axes, strictly better on at
    /// least one. O(n²) dominance scan — the candidate set is a bounded
    /// enumeration of instance pools (families × sizes × AZs), never large
    /// enough to need a sweep-line skyline algorithm.
    fn dominates(a: &Proposta, b: &Proposta) -> bool {
        let no_worse = a.cost_cents <= b.cost_cents && a.relief_latency_secs <= b.relief_latency_secs;
        let strictly_better = a.cost_cents < b.cost_cents || a.relief_latency_secs < b.relief_latency_secs;
        no_worse && strictly_better
    }

    /// The Pareto frontier of `candidates` — every proposal not dominated by
    /// another.
    fn frontier(candidates: &[Proposta]) -> Vec<Proposta> {
        candidates
            .iter()
            .filter(|p| !candidates.iter().any(|q| Self::dominates(q, p)))
            .cloned()
            .collect()
    }
}

impl<P: PriceOracle> Otimizador for ParetoOtimizador<P> {
    fn optimize(&self, previsoes: &[(Forma, Previsao)]) -> Vec<Proposta> {
        // Reuse the same proven band law every single-forma decision already
        // goes through (`BandLeiloeiro`'s K2 reuse) to size each candidate's
        // delta — this crate has exactly one grow/shrink-sizing algorithm,
        // never two. Only `Grow` candidates enter the auction: `Otimizador`
        // arbitrates COST among ways to add capacity, it does not duplicate
        // `BandLeiloeiro`'s hold/shrink/escalate logic for a single forma.
        let candidates: Vec<Proposta> = previsoes
            .iter()
            .filter_map(|(forma, previsao)| {
                match decide(previsao.immediate_used, previsao.capacity, &self.cfg) {
                    Decision::Grow { from, to } => {
                        let delta = to.saturating_sub(from);
                        // relief_latency_secs is this Forma's own provisioning
                        // dead-time (time-to-Ready), sourced from breathe's
                        // existing per-forma cold-start data. A genuinely
                        // wired impl reads this from the fleet's provisioning
                        // telemetry, same as cost from PriceOracle -- both
                        // are injected via the same seam a real caller
                        // supplies (see PriceOracle's own doc comment); this
                        // reference impl folds it through the price call
                        // below for the same reason: no parallel data model.
                        let duration_secs = self.cfg.warmup_seconds.max(1);
                        let cost_cents = self.price.cost_cents(*forma, duration_secs);
                        Some(Proposta {
                            forma: *forma,
                            delta,
                            cost_cents,
                            relief_latency_secs: duration_secs,
                            rationale: GrowRationale { delta, forma: *forma, cost_cents, duration_secs }
                                .to_string(),
                        })
                    }
                    _ => None,
                }
            })
            .collect();

        if candidates.is_empty() {
            return Vec::new();
        }

        let frontier = Self::frontier(&candidates);
        let winner = match self.pick {
            PickPolicy::MinCost => frontier.iter().min_by_key(|p| p.cost_cents),
            PickPolicy::MinCostUnderDeadline { deadline_secs } => frontier
                .iter()
                .filter(|p| p.relief_latency_secs <= deadline_secs)
                .min_by_key(|p| p.cost_cents)
                // No candidate meets the deadline: fall back to the fastest
                // frontier point rather than returning nothing — an auction
                // that silently under-provisions on a missed deadline is the
                // same "never silently under-provision" invariant
                // `BandLeiloeiro::EnvelopeExausto` already enforces.
                .or_else(|| frontier.iter().min_by_key(|p| p.relief_latency_secs)),
            PickPolicy::MinCostRiskAdjusted { risk_weight } => {
                // score = cost_cents + risk_weight * interruption_ppm. No
                // oracle attached ⇒ every candidate scores as zero-risk, so
                // this is byte-identical to MinCost (never a fabricated
                // number standing in for a real one — see
                // ParetoOtimizador::interruption's doc comment).
                #[allow(clippy::cast_precision_loss, reason = "cost_cents/ppm are bounded fleet-scale counters; a ranking score tolerates f64 rounding")]
                let score = |p: &Proposta| -> f64 {
                    let ppm = self.interruption.as_deref().map_or(0, |o| o.interruption_ppm(p.forma));
                    p.cost_cents as f64 + risk_weight * ppm as f64
                };
                frontier.iter().min_by(|a, b| score(a).total_cmp(&score(b)))
            }
        };

        winner.into_iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests;
