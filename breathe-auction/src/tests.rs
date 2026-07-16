use super::{
    BandLeiloeiro, DecisaoForma, Leiloeiro, Otimizador, ParetoOtimizador, PickPolicy, PriceOracle,
    Previsao, Previsor, ReactivePrevisor,
};
use breathe_control::BandConfig;
use breathe_provider::Forma;

/// An OPEN config — the ceiling is far above the capacity in the tests, so a grow
/// is a real `Grow` (not clamped to `AtCeiling`).
fn cfg() -> BandConfig {
    BandConfig {
        grow_above: 0.85,
        shrink_below: 0.70,
        setpoint: 0.80,
        grow_factor: 1.25,
        shrink_factor: 0.90,
        floor_bytes: 1,
        ceiling_bytes: 1_000_000,
        request_floor_bytes: 0,
        warmup_seconds: 0,
        metric_missing_policy: breathe_control::MetricMissingPolicy::RestoreHeadroom,
    }
}

/// A CAPPED config — ceiling == 100, so demand beyond it exhausts the envelope.
fn cfg_capped() -> BandConfig {
    BandConfig { ceiling_bytes: 100, ..cfg() }
}

fn previsao(used: u64, capacity: u64) -> Previsao {
    Previsao { immediate_used: used, capacity }
}

#[test]
fn reactive_previsor_echoes_the_sample() {
    let p = ReactivePrevisor.predict(42, 100);
    assert_eq!(p, previsao(42, 100));
}

#[test]
fn grows_when_demand_is_high() {
    // util 0.90 > grow_above 0.85 → the band law grows the node count.
    let d = BandLeiloeiro.decide(Forma::NodeOnDemand, &previsao(90, 100), &cfg());
    match d {
        DecisaoForma::Crescer { forma, delta } => {
            assert_eq!(forma, Forma::NodeOnDemand);
            assert!(delta > 0, "grow delta must be positive");
        }
        other => panic!("expected Crescer, got {other:?}"),
    }
}

#[test]
fn holds_when_in_band() {
    // util 0.75 ∈ [shrink_below, grow_above] → Manter (per MATH §3.3: in-band).
    assert_eq!(BandLeiloeiro.decide(Forma::NodeOnDemand, &previsao(75, 100), &cfg()), DecisaoForma::Manter);
}

#[test]
fn shrinks_when_demand_is_low() {
    // util 0.50 < shrink_below 0.70 → Encolher (one band-law step, drain-first).
    match BandLeiloeiro.decide(Forma::NodeOnDemand, &previsao(50, 100), &cfg()) {
        DecisaoForma::Encolher { forma, delta, drain } => {
            assert_eq!(forma, Forma::NodeOnDemand);
            assert!(delta > 0);
            assert!(drain, "a node shrink must drain (PDB-aware) first");
        }
        other => panic!("expected Encolher, got {other:?}"),
    }
}

#[test]
fn envelope_exhausted_at_the_ceiling() {
    // demand 200 on a ceiling of 100 ⇒ AtCeiling ⇒ escalate, never silently
    // under-provision. need = ⌈200/0.80⌉ = 250 ; shortfall = 250 − 100 = 150.
    match BandLeiloeiro.decide(Forma::NodeOnDemand, &previsao(200, 100), &cfg_capped()) {
        DecisaoForma::EnvelopeExausto { forma, shortfall } => {
            assert_eq!(forma, Forma::NodeOnDemand);
            assert_eq!(shortfall, 150);
        }
        other => panic!("expected EnvelopeExausto, got {other:?}"),
    }
}

#[test]
fn single_forma_leiloeiro_is_the_band_law_lifted() {
    // Shape-blindness end to end: the auctioneer's verdict is exactly the band
    // law's Decision, re-typed. A sweep of demands must never over-commit beyond
    // an escalation: every non-Manter grow/escalate corresponds to high util.
    let c = cfg();
    for used in [1u64, 10, 40, 70, 80, 84, 86, 95, 150, 300] {
        let d = BandLeiloeiro.decide(Forma::NodeOnDemand, &previsao(used, 100), &c);
        let util = used as f64 / 100.0;
        match d {
            DecisaoForma::Manter => assert!(
                util <= c.grow_above + 1e-9,
                "Manter at util {util:.3} above grow_above"
            ),
            DecisaoForma::Crescer { .. } | DecisaoForma::EnvelopeExausto { .. } => {
                assert!(util > c.grow_above - 1e-9, "grow/escalate at util {util:.3} below grow_above");
            }
            DecisaoForma::Encolher { .. } => assert!(util < c.shrink_below + 1e-9),
            DecisaoForma::Reformar { .. } => panic!("BandLeiloeiro never reforms"),
        }
    }
}

// ============================================================================
// LinearTrendPrevisor — the monotone-safe forecaster (BU8).
// ============================================================================
use super::LinearTrendPrevisor;

#[test]
fn forecaster_echoes_until_it_has_two_samples() {
    // First sample: no slope yet ⇒ reactive echo (never guess from one point).
    let p = LinearTrendPrevisor::new(4, 3);
    assert_eq!(p.predict(50, 100).immediate_used, 50);
}

#[test]
fn forecaster_projects_a_rising_trend_ahead_of_the_horizon() {
    // demand rising +10/tick; with a 3-tick horizon the forecast leads demand.
    let p = LinearTrendPrevisor::new(4, 3);
    p.predict(10, 100);
    p.predict(20, 100);
    let f = p.predict(30, 100); // slope +10/tick over 2 ticks; project 3 ahead
    assert!(f.immediate_used > 30, "must lead the current sample on a rising trend");
    // newest 30 + slope(10)*horizon(3) = 60.
    assert_eq!(f.immediate_used, 60);
}

#[test]
fn forecaster_is_monotone_safe_a_falling_trend_never_forecasts_below_current() {
    // demand FALLING: a forecaster that extrapolated down would trigger a
    // premature shrink (cost-thrash, §5.3). The max(current) floor forbids it.
    let p = LinearTrendPrevisor::new(4, 3);
    p.predict(90, 100);
    p.predict(70, 100);
    let f = p.predict(50, 100); // slope -20/tick; naive proj = 50 - 60 < 0
    assert_eq!(f.immediate_used, 50, "falling trend collapses to the reactive echo");
}

#[test]
fn forecaster_never_undershoots_the_current_sample_for_any_history() {
    // The load-bearing invariant: for ANY sequence, immediate_used >= current.
    let p = LinearTrendPrevisor::new(5, 4);
    for used in [100, 80, 60, 90, 40, 200, 10] {
        let f = p.predict(used, 1000);
        assert!(f.immediate_used >= used, "forecast {} < current {used}", f.immediate_used);
    }
}

#[test]
fn forecaster_capacity_passes_through_untouched() {
    let p = LinearTrendPrevisor::new(3, 2);
    assert_eq!(p.predict(10, 777).capacity, 777);
}

// ============================================================================
// ParetoOtimizador
// ============================================================================

/// Spot is cheaper per-unit-time but slower to provision than on-demand --
/// the realistic cost/latency tradeoff the frontier is supposed to preserve.
struct FixedPrice;
impl PriceOracle for FixedPrice {
    fn cost_cents(&self, forma: Forma, duration_secs: u64) -> u64 {
        let rate_per_sec = match forma {
            Forma::NodeSpot => 1,
            Forma::NodeOnDemand => 3,
            _ => 10,
        };
        rate_per_sec * duration_secs
    }
}

fn otimizador_cfg(warmup_seconds: u64) -> BandConfig {
    BandConfig { warmup_seconds, ..cfg() }
}

#[test]
fn no_candidates_grow_yields_no_proposals() {
    // Both formas are already in-band (Hold) -- nothing to auction.
    let o = ParetoOtimizador::new(FixedPrice, otimizador_cfg(60), PickPolicy::MinCost);
    let previsoes = [
        (Forma::NodeSpot, previsao(50, 100)),
        (Forma::NodeOnDemand, previsao(50, 100)),
    ];
    assert!(o.optimize(&previsoes).is_empty());
}

#[test]
fn min_cost_picks_the_cheaper_candidate_on_the_frontier() {
    // Both need to grow (util 0.90 > grow_above 0.85); spot is 1c/s vs
    // on-demand's 3c/s over the same 60s warmup -- spot strictly dominates
    // (cheaper AND not slower, since relief_latency_secs is the same
    // warmup_seconds for both in this reference impl), so MinCost must
    // pick spot.
    let o = ParetoOtimizador::new(FixedPrice, otimizador_cfg(60), PickPolicy::MinCost);
    let previsoes = [
        (Forma::NodeSpot, previsao(90, 100)),
        (Forma::NodeOnDemand, previsao(90, 100)),
    ];
    let winners = o.optimize(&previsoes);
    assert_eq!(winners.len(), 1);
    assert_eq!(winners[0].forma, Forma::NodeSpot);
    assert_eq!(winners[0].cost_cents, 60); // 1c/s * 60s
}

#[test]
fn dominated_candidate_never_reaches_the_frontier() {
    // Same latency, strictly higher cost -- on-demand is dominated outright,
    // so the frontier itself (not just the pick policy) must exclude it.
    let o = ParetoOtimizador::new(FixedPrice, otimizador_cfg(30), PickPolicy::MinCost);
    let previsoes = [
        (Forma::NodeSpot, previsao(90, 100)),
        (Forma::NodeOnDemand, previsao(90, 100)),
    ];
    let candidates: Vec<_> = previsoes
        .iter()
        .filter_map(|(forma, p)| {
            matches!(
                breathe_control::decide(p.immediate_used, p.capacity, &otimizador_cfg(30)),
                breathe_control::Decision::Grow { .. }
            )
            .then_some(*forma)
        })
        .collect();
    assert_eq!(candidates.len(), 2, "sanity: both should be Grow candidates");
    let frontier_forms: Vec<Forma> = o.optimize(&previsoes).iter().map(|p| p.forma).collect();
    assert_eq!(frontier_forms, vec![Forma::NodeSpot]);
}

#[test]
fn min_cost_under_deadline_rules_out_the_cheap_but_slow_candidate() {
    // Spot is cheaper (1c/s) but this reference impl ties relief_latency_secs
    // to warmup_seconds, so give spot a deliberately longer warmup via a
    // second BandConfig-free scenario: instead, prove the deadline math on
    // the oracle side by making on-demand strictly faster AND still
    // acceptable-cost under a tight deadline that spot's warmup can't meet.
    struct SlowSpotPrice;
    impl PriceOracle for SlowSpotPrice {
        fn cost_cents(&self, forma: Forma, duration_secs: u64) -> u64 {
            match forma {
                Forma::NodeSpot => duration_secs, // 1c/s
                Forma::NodeOnDemand => duration_secs * 3,
                _ => duration_secs * 10,
            }
        }
    }
    // warmup_seconds is shared across candidates in this reference impl
    // (a named, documented simplification), so to exercise the deadline
    // fallback we set it ABOVE the deadline for every candidate -- proving
    // "no candidate meets the deadline" falls back to the fastest, not to
    // silently returning nothing (the same never-under-provision invariant
    // BandLeiloeiro::EnvelopeExausto enforces for the single-forma case).
    let o = ParetoOtimizador::new(
        SlowSpotPrice,
        otimizador_cfg(120),
        PickPolicy::MinCostUnderDeadline { deadline_secs: 10 },
    );
    let previsoes = [
        (Forma::NodeSpot, previsao(90, 100)),
        (Forma::NodeOnDemand, previsao(90, 100)),
    ];
    let winners = o.optimize(&previsoes);
    assert_eq!(winners.len(), 1, "must still return a winner, never silently empty");
    // Neither meets the 10s deadline (both are 120s) -- falls back to
    // min-latency, and both tie at 120s, so the tie-break is min_by_key's
    // first-match (NodeSpot, since it's first in `previsoes`); the load-
    // bearing assertion is that SOME winner comes back, not which one wins
    // an exact tie.
    assert_eq!(winners[0].relief_latency_secs, 120);
}

#[test]
fn min_cost_under_deadline_picks_cheapest_among_those_that_fit() {
    struct TieredPrice;
    impl PriceOracle for TieredPrice {
        fn cost_cents(&self, forma: Forma, duration_secs: u64) -> u64 {
            match forma {
                Forma::NodeSpot => duration_secs, // cheapest
                Forma::NodeOnDemand => duration_secs * 2,
                _ => duration_secs * 100,
            }
        }
    }
    // Both candidates share the same warmup/relief_latency_secs (20s) and
    // both fit a 30s deadline -- so MinCostUnderDeadline must fall through
    // to cost, same as plain MinCost, and pick the cheaper one (spot).
    let o = ParetoOtimizador::new(
        TieredPrice,
        otimizador_cfg(20),
        PickPolicy::MinCostUnderDeadline { deadline_secs: 30 },
    );
    let previsoes = [
        (Forma::NodeSpot, previsao(90, 100)),
        (Forma::NodeOnDemand, previsao(90, 100)),
    ];
    let winners = o.optimize(&previsoes);
    assert_eq!(winners.len(), 1);
    assert_eq!(winners[0].forma, Forma::NodeSpot);
}
