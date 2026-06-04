use super::{BandLeiloeiro, DecisaoForma, Leiloeiro, Previsao, Previsor, ReactivePrevisor};
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
