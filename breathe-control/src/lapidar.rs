//! `lapidar` — the self-optimization loop every CONTROLLED band runs by default.
//!
//! A band's reactive law (the [`crate::BandConfig`]-parameterized carve toward a
//! fixed setpoint) is a *fixed* policy. `lapidar` is the META-loop layered on
//! top: it watches the band's OWN control quality over time and refines the
//! band's parameters toward their optimal form — the lapidação (gem-cutting) of
//! a controller. It is to a controlled band what the env-discovered tier is to
//! config: a **core, default-on capability that applies to every band**, not a
//! per-band bolt-on.
//!
//! The seven-verb loop — all pure + total HERE (the controller supplies real
//! observations and performs the side-effecting apply/revert against the CR):
//!
//! ```text
//!  ANALYZE       observations -> ControlQuality          (this module)
//!  LEARN+SUGGEST (quality, cfg) -> Option<Suggestion>    (this module)
//!  APPLY         controller writes the suggested param to the band spec
//!  WATCH         controller feeds the post-change observations back
//!  TEST          score(new quality) vs the pre-change baseline             (this module)
//!  OPTIMIZE      Accept (keep) if better, else Revert (rollback)           (this module)
//! ```
//!
//! …then back to ANALYZE. **Small-scale by construction:** one parameter at a
//! time, one trial at a time, every change bounded and *revertible* — careful
//! improvement, never a leap. A band that never leaves shadow never enters the
//! loop; the moment it controls, the loop is its default behavior.

/// A single control snapshot the controller records each reconcile for a band.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BandObservation {
    /// Observed utilization (used / limit), in `[0, ∞)` (can exceed 1 on a spike).
    pub util: f64,
    /// Observed working set (bytes / millicores).
    pub used: u64,
    /// The effective limit at this tick.
    pub limit: u64,
    /// A carve was applied this tick (the controller acted).
    pub carved: bool,
    /// The carve this tick was GROW (`Some(true)`) / SHRINK (`Some(false)`) /
    /// none (`None`) — the direction, for oscillation analysis.
    pub grow: Option<bool>,
    /// A carve was ATTEMPTED but failed (RBAC, QoS-class, API error).
    pub carve_failed: bool,
    /// The workload (re)started this tick (a roll, or an external restart).
    pub restarted: bool,
}

impl BandObservation {
    /// Fraction of the limit left unused this tick, in `[0, 1]` (`0` when the
    /// reading is degenerate). High average waste ⇒ over-provisioned.
    #[must_use]
    pub fn waste_frac(&self) -> f64 {
        if self.limit == 0 {
            return 0.0;
        }
        let used = self.used.min(self.limit) as f64;
        1.0 - used / (self.limit as f64)
    }
}

/// Utilization at or above this fraction is a SAFETY BREACH (too little
/// headroom — restart/OOM risk). The loop treats breaches as the highest-
/// priority signal: it always buys headroom before chasing efficiency.
pub const BREACH_THRESHOLD: f64 = 0.95;

/// The analyzed control quality over a window of [`BandObservation`]s. Every
/// field is "lower is better" so [`ControlQuality::score`] is a simple sum.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ControlQuality {
    /// Mean fraction of the limit left unused — the over-provisioning cost.
    pub mean_waste: f64,
    /// RMS distance of utilization from `setpoint` — control accuracy.
    pub setpoint_rmse: f64,
    /// Carve-direction reversals ÷ carves — thrash (grow→shrink→grow…).
    pub oscillation: f64,
    /// Failed carves ÷ attempted carves — the structural-blocker rate.
    pub carve_failure_rate: f64,
    /// Fraction of ticks at/above [`BREACH_THRESHOLD`] — the unsafe time.
    pub breach_frac: f64,
    /// Number of observations the quality was computed from (confidence).
    pub samples: usize,
}

impl ControlQuality {
    /// The scalar objective the loop MINIMIZES. Safety (breaches) is weighted
    /// hardest, then failures, then thrash, then accuracy + waste. Weights are
    /// the only "policy" knob; they make breaches dominate any waste win.
    #[must_use]
    pub fn score(&self) -> f64 {
        8.0 * self.breach_frac
            + 4.0 * self.carve_failure_rate
            + 2.0 * self.oscillation
            + 1.0 * self.setpoint_rmse
            + 1.0 * self.mean_waste
    }
}

/// Analyze a window of observations into a [`ControlQuality`], relative to the
/// band's current `setpoint`. Pure; empty input yields a zeroed quality with
/// `samples == 0` (the loop refuses to act on no data).
#[must_use]
pub fn analyze(observations: &[BandObservation], setpoint: f64) -> ControlQuality {
    let n = observations.len();
    if n == 0 {
        return ControlQuality {
            mean_waste: 0.0,
            setpoint_rmse: 0.0,
            oscillation: 0.0,
            carve_failure_rate: 0.0,
            breach_frac: 0.0,
            samples: 0,
        };
    }
    let nf = n as f64;
    let mean_waste = observations.iter().map(BandObservation::waste_frac).sum::<f64>() / nf;
    let sq_err = observations
        .iter()
        .map(|o| (o.util - setpoint) * (o.util - setpoint))
        .sum::<f64>()
        / nf;
    let setpoint_rmse = sq_err.sqrt();
    let breach_frac = observations.iter().filter(|o| o.util >= BREACH_THRESHOLD).count() as f64 / nf;

    let carve_attempts = observations.iter().filter(|o| o.carved || o.carve_failed).count();
    let carve_failures = observations.iter().filter(|o| o.carve_failed).count();
    let carve_failure_rate =
        if carve_attempts == 0 { 0.0 } else { carve_failures as f64 / carve_attempts as f64 };

    // Oscillation: among successful directional carves, how often the direction
    // reverses tick-to-tick (grow then shrink then grow = thrash).
    let dirs: Vec<bool> = observations.iter().filter_map(|o| o.grow).collect();
    let oscillation = if dirs.len() < 2 {
        0.0
    } else {
        let reversals = dirs.windows(2).filter(|w| w[0] != w[1]).count();
        reversals as f64 / (dirs.len() - 1) as f64
    };

    ControlQuality { mean_waste, setpoint_rmse, oscillation, carve_failure_rate, breach_frac, samples: n }
}

/// The band parameter a [`Suggestion`] tunes (a subset of [`crate::BandConfig`]
/// — the safe-to-auto-tune scalars).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TunedParam {
    /// `setpoint` — the target utilization.
    Setpoint,
    /// `grow_above` — the grow trigger (raising it widens the deadband).
    GrowAbove,
    /// `shrink_below` — the shrink trigger (lowering it widens the deadband).
    ShrinkBelow,
    /// `warmup_seconds` — the post-restart shrink hold.
    WarmupSeconds,
}

impl TunedParam {
    /// Canonical name (matches the [`crate::BandConfig`] field).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Setpoint => "setpoint",
            Self::GrowAbove => "grow_above",
            Self::ShrinkBelow => "shrink_below",
            Self::WarmupSeconds => "warmup_seconds",
        }
    }
}

/// A single, bounded, revertible parameter change the loop proposes.
#[derive(Debug, Clone, PartialEq)]
pub struct Suggestion {
    /// Which parameter to change.
    pub param: TunedParam,
    /// The current value (so the controller can revert exactly).
    pub from: f64,
    /// The proposed value (already clamped to the safe range).
    pub to: f64,
    /// Why — surfaced on the band status / events for the operator.
    pub rationale: &'static str,
}

/// How much to nudge a fractional threshold per trial (small — careful).
const SETPOINT_STEP: f64 = 0.02;
/// Below this waste there is nothing worth reclaiming.
const WASTE_FLOOR: f64 = 0.15;
/// Above this oscillation the band is thrashing.
const OSC_CEILING: f64 = 0.40;
/// The loop refuses to suggest on fewer than this many samples.
const MIN_SAMPLES: usize = 12;

#[inline]
fn clamp(v: f64, lo: f64, hi: f64) -> f64 {
    v.max(lo).min(hi)
}

/// LEARN + GENERATE: from the analyzed quality + the current config, propose at
/// most ONE bounded parameter change — or `None` when the band is already
/// well-tuned (or there is too little data). Priority is **safety first**:
/// breaches buy headroom before any efficiency move; thrash is calmed before
/// waste is chased.
///
/// Carve FAILURES are deliberately NOT param-tuned here — a 403/QoS-class
/// failure is structural (RBAC, a BestEffort pod), not a setpoint problem; it
/// surfaces in [`ControlQuality::carve_failure_rate`] (and the score) for the
/// controller to escalate, not for the loop to chase with a knob.
#[must_use]
pub fn suggest(q: &ControlQuality, cfg: &crate::BandConfig) -> Option<Suggestion> {
    if q.samples < MIN_SAMPLES {
        return None;
    }

    // 1) SAFETY: any breach → lower the setpoint to buy headroom.
    if q.breach_frac > 0.0 {
        let to = clamp(cfg.setpoint - SETPOINT_STEP, cfg.shrink_below, cfg.grow_above);
        if to < cfg.setpoint {
            return Some(Suggestion {
                param: TunedParam::Setpoint,
                from: cfg.setpoint,
                to,
                rationale: "utilization breached the safety threshold; lowering setpoint to buy headroom",
            });
        }
    }

    // 2) STABILITY: thrash → widen the deadband (raise grow_above) so carves fire less.
    if q.oscillation > OSC_CEILING {
        let to = clamp(cfg.grow_above + SETPOINT_STEP, cfg.setpoint, 0.98);
        if to > cfg.grow_above {
            return Some(Suggestion {
                param: TunedParam::GrowAbove,
                from: cfg.grow_above,
                to,
                rationale: "carve thrash detected; widening the grow threshold to reduce oscillation",
            });
        }
    }

    // 3) EFFICIENCY: persistent waste with NO breaches and calm control →
    //    raise the setpoint toward fuller utilization.
    if q.mean_waste > WASTE_FLOOR && q.breach_frac == 0.0 && q.oscillation <= OSC_CEILING {
        let to = clamp(cfg.setpoint + SETPOINT_STEP, cfg.shrink_below, cfg.grow_above);
        if to > cfg.setpoint {
            return Some(Suggestion {
                param: TunedParam::Setpoint,
                from: cfg.setpoint,
                to,
                rationale: "over-provisioned with no breaches; raising setpoint toward efficiency",
            });
        }
    }

    None
}

/// The loop's state for one band. The controller advances it with [`step`],
/// performing the side effects (`apply` on entering [`Lapidacao::Trial`],
/// `apply`-or-`revert` on entering [`Lapidacao::Settled`]).
#[derive(Debug, Clone, PartialEq)]
pub enum Lapidacao {
    /// No trial in flight — collecting the baseline window.
    Observing,
    /// `suggestion` has been APPLIED; WATCHING its effect against `baseline`.
    Trial { suggestion: Suggestion, baseline: f64 },
    /// The trial concluded. `accepted` ⇒ keep the change; else the controller
    /// reverts `suggestion` (param back to `suggestion.from`).
    Settled { suggestion: Suggestion, accepted: bool, baseline: f64, trial: f64 },
}

/// Advance the loop given the latest analyzed `quality`.
///
/// - From [`Lapidacao::Observing`]: [`suggest`] → if Some, enter `Trial`
///   (the controller then APPLIES `suggestion.to`); else stay observing.
/// - From [`Lapidacao::Trial`]: `quality` is now the POST-change window. TEST:
///   if its score improved on `baseline`, `Settled { accepted: true }` (keep);
///   else `Settled { accepted: false }` (the controller REVERTS). A tie keeps
///   the baseline (revert) — no change without a proven win.
/// - From [`Lapidacao::Settled`]: fold back to `Observing` for the next cycle.
#[must_use]
pub fn step(state: Lapidacao, quality: &ControlQuality, cfg: &crate::BandConfig) -> Lapidacao {
    match state {
        Lapidacao::Observing => match suggest(quality, cfg) {
            Some(suggestion) => Lapidacao::Trial { suggestion, baseline: quality.score() },
            None => Lapidacao::Observing,
        },
        Lapidacao::Trial { suggestion, baseline } => {
            let trial = quality.score();
            Lapidacao::Settled { suggestion, accepted: trial < baseline, baseline, trial }
        }
        Lapidacao::Settled { .. } => Lapidacao::Observing,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BandConfig;

    fn obs(util: f64, used: u64, limit: u64) -> BandObservation {
        BandObservation { util, used, limit, carved: false, grow: None, carve_failed: false, restarted: false }
    }

    #[test]
    fn analyze_waste_and_setpoint_and_breach() {
        // 4 idle ticks at 1Gi limit, ~10% used → high waste; util 0.1 vs setpoint 0.8.
        let win: Vec<_> = (0..4).map(|_| obs(0.10, 100, 1000)).collect();
        let q = analyze(&win, 0.80);
        assert_eq!(q.samples, 4);
        assert!((q.mean_waste - 0.90).abs() < 1e-9, "10% used → 90% waste");
        assert!((q.setpoint_rmse - 0.70).abs() < 1e-9, "|0.1-0.8| = 0.7");
        assert_eq!(q.breach_frac, 0.0);
    }

    #[test]
    fn analyze_counts_breaches_and_oscillation_and_failures() {
        let mut win = vec![obs(0.97, 970, 1000), obs(0.50, 500, 1000)]; // 1 breach of 2
        win[0].carved = true;
        win[0].grow = Some(true);
        win[1].carve_failed = true;
        let mut w2 = obs(0.50, 500, 1000);
        w2.carved = true;
        w2.grow = Some(false); // reversal vs win[0] grow
        win.push(w2);
        let q = analyze(&win, 0.80);
        assert!((q.breach_frac - 1.0 / 3.0).abs() < 1e-9);
        // 3 carve attempts (carved, failed, carved), 1 failed.
        assert!((q.carve_failure_rate - 1.0 / 3.0).abs() < 1e-9, "1 failed of 3 attempts");
        assert_eq!(q.oscillation, 1.0, "grow then shrink = full reversal");
    }

    #[test]
    fn score_weights_breach_over_waste() {
        let breachy = ControlQuality { mean_waste: 0.0, setpoint_rmse: 0.0, oscillation: 0.0, carve_failure_rate: 0.0, breach_frac: 0.5, samples: 20 };
        let wasteful = ControlQuality { mean_waste: 0.9, setpoint_rmse: 0.7, oscillation: 0.0, carve_failure_rate: 0.0, breach_frac: 0.0, samples: 20 };
        assert!(breachy.score() > wasteful.score(), "a breach must outweigh waste");
    }

    #[test]
    fn suggest_lowers_setpoint_on_breach() {
        let q = ControlQuality { mean_waste: 0.0, setpoint_rmse: 0.1, oscillation: 0.0, carve_failure_rate: 0.0, breach_frac: 0.2, samples: 20 };
        let s = suggest(&q, &BandConfig::default()).expect("breach must suggest");
        assert_eq!(s.param, TunedParam::Setpoint);
        assert!(s.to < s.from, "setpoint lowered for headroom");
    }

    #[test]
    fn suggest_raises_setpoint_on_waste_when_safe() {
        let q = ControlQuality { mean_waste: 0.6, setpoint_rmse: 0.2, oscillation: 0.0, carve_failure_rate: 0.0, breach_frac: 0.0, samples: 20 };
        let s = suggest(&q, &BandConfig::default()).expect("waste must suggest");
        assert_eq!(s.param, TunedParam::Setpoint);
        assert!(s.to > s.from, "setpoint raised toward efficiency");
    }

    #[test]
    fn suggest_widens_deadband_on_thrash() {
        let q = ControlQuality { mean_waste: 0.0, setpoint_rmse: 0.1, oscillation: 0.8, carve_failure_rate: 0.0, breach_frac: 0.0, samples: 20 };
        let s = suggest(&q, &BandConfig::default()).expect("thrash must suggest");
        assert_eq!(s.param, TunedParam::GrowAbove);
        assert!(s.to > s.from);
    }

    #[test]
    fn suggest_none_when_well_tuned_or_thin_data() {
        let good = ControlQuality { mean_waste: 0.05, setpoint_rmse: 0.05, oscillation: 0.0, carve_failure_rate: 0.0, breach_frac: 0.0, samples: 20 };
        assert!(suggest(&good, &BandConfig::default()).is_none(), "well-tuned → no change");
        let thin = ControlQuality { mean_waste: 0.9, setpoint_rmse: 0.7, oscillation: 0.0, carve_failure_rate: 0.0, breach_frac: 0.3, samples: 3 };
        assert!(suggest(&thin, &BandConfig::default()).is_none(), "too few samples → no change");
    }

    #[test]
    fn loop_accepts_an_improving_trial() {
        let cfg = BandConfig::default();
        // Observing → a wasteful baseline suggests raising setpoint.
        let baseline_q = ControlQuality { mean_waste: 0.6, setpoint_rmse: 0.2, oscillation: 0.0, carve_failure_rate: 0.0, breach_frac: 0.0, samples: 20 };
        let st = step(Lapidacao::Observing, &baseline_q, &cfg);
        let Lapidacao::Trial { ref suggestion, baseline } = st else { panic!("must enter Trial") };
        assert_eq!(suggestion.param, TunedParam::Setpoint);
        // Post-change window is BETTER (less waste) → accept.
        let better_q = ControlQuality { mean_waste: 0.3, setpoint_rmse: 0.15, oscillation: 0.0, carve_failure_rate: 0.0, breach_frac: 0.0, samples: 20 };
        assert!(better_q.score() < baseline);
        let settled = step(st, &better_q, &cfg);
        let Lapidacao::Settled { accepted, .. } = settled else { panic!("must settle") };
        assert!(accepted, "an improving trial is kept");
        assert_eq!(step(settled, &better_q, &cfg), Lapidacao::Observing, "folds back to Observing");
    }

    #[test]
    fn loop_reverts_a_regressing_trial() {
        let cfg = BandConfig::default();
        let baseline_q = ControlQuality { mean_waste: 0.6, setpoint_rmse: 0.2, oscillation: 0.0, carve_failure_rate: 0.0, breach_frac: 0.0, samples: 20 };
        let st = step(Lapidacao::Observing, &baseline_q, &cfg);
        // Post-change window REGRESSED (a breach appeared) → revert.
        let worse_q = ControlQuality { mean_waste: 0.3, setpoint_rmse: 0.2, oscillation: 0.0, carve_failure_rate: 0.0, breach_frac: 0.2, samples: 20 };
        let settled = step(st, &worse_q, &cfg);
        let Lapidacao::Settled { accepted, .. } = settled else { panic!("must settle") };
        assert!(!accepted, "a regressing trial is reverted — careful, never a leap");
    }
}
