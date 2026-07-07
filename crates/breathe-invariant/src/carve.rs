//! The carve control laws — pure functions, best-fit, NO ML
//! (`/algorithmic-prowess-seal`).
//!
//! A carve is a pure decision: given the observed working set and a sealed
//! [`UtilizationSetpoint`], compute the target limit that seats utilization at
//! the setpoint. These are the algorithmic cores the breathe substrate's
//! `ControlLaw`/`safety_clamp` realize live; here they are the *contract*
//! form — deterministic, total, testable, and the concrete witness that
//! carving-to-setpoint is a real function, not an assertion.
//!
//! The algorithms (each a top-of-ladder classical primitive, no ML):
//! - **multiplicative band** — `target = ⌈used / setpoint⌉`: the exact
//!   seat-at-setpoint law (BandLaw). Reused by memory/cpu/replica.
//! - **grow-only predictive** — a **least-squares linear fit** of fill
//!   velocity + `time-to-full`, the storage trigger (grow early, never
//!   shrink an irreversible actuator).
//! - **the dual-purpose lemma** — the same `target = ⌈used / setpoint⌉`
//!   simultaneously *reduces* the limit when idle (cost) AND *keeps*
//!   `1/setpoint` headroom before saturation (resiliency). Clause 6, proven
//!   numerically.

use crate::setpoint::UtilizationSetpoint;

/// The pluggable carve law — direction + magnitude of one carve. Every
/// dimension's carving is one impl of this trait (the contract peer of the
/// breathe substrate's `ControlLaw`). Pure; no I/O; no ML.
pub trait CarveLaw {
    /// Compute the target limit that seats `observed_used` at `setpoint`.
    /// Units are dimension-agnostic (bytes, millicores, replica count).
    fn carve(&self, observed_used: u64, setpoint: UtilizationSetpoint) -> u64;
}

/// The exact seat-at-setpoint law: `target = ⌈used / setpoint⌉`. After the
/// carve, `used / target ≤ setpoint`, so the setpoint's headroom is preserved
/// by construction. This is the multiplicative BandLaw memory / cpu / replica
/// carve through.
#[derive(Clone, Copy, Debug, Default)]
pub struct MultiplicativeBand;

impl CarveLaw for MultiplicativeBand {
    fn carve(&self, observed_used: u64, setpoint: UtilizationSetpoint) -> u64 {
        carve_to_setpoint(observed_used, setpoint)
    }
}

/// The exact seat-at-setpoint target: the least `target` with
/// `used / target ≤ setpoint`, i.e. `⌈used / setpoint⌉`, computed in integer
/// basis-point arithmetic (no float). `used == 0` seats at 0 (nothing to
/// hold); the reconcile-layer floor is applied separately by the substrate's
/// `safety_clamp` — this is the pure law, not the clamped decision.
#[must_use]
pub fn carve_to_setpoint(observed_used: u64, setpoint: UtilizationSetpoint) -> u64 {
    if observed_used == 0 {
        return 0;
    }
    let bps = u64::from(setpoint.bps());
    // target = ceil(used * 10000 / bps)
    (observed_used * 10_000 + (bps - 1)) / bps
}

/// The realized utilization after seating `used` at `target` — a ratio in
/// (0, 1]. Used to prove the carve holds the setpoint (headroom = resiliency).
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn realized_utilization(observed_used: u64, target: u64) -> f64 {
    if target == 0 {
        return 0.0;
    }
    observed_used as f64 / target as f64
}

/// A least-squares linear fit of `(t_secs, used)` samples → fill velocity in
/// units/sec (the storage grow-only trigger core). Returns `0.0` for < 2
/// samples or a degenerate time span. Classical statistics, no ML.
#[must_use]
pub fn fill_velocity(samples: &[(f64, f64)]) -> f64 {
    let n = samples.len();
    if n < 2 {
        return 0.0;
    }
    #[allow(clippy::cast_precision_loss)]
    let nf = n as f64;
    let sum_t: f64 = samples.iter().map(|&(t, _)| t).sum();
    let sum_u: f64 = samples.iter().map(|&(_, u)| u).sum();
    let sum_tt: f64 = samples.iter().map(|&(t, _)| t * t).sum();
    let sum_tu: f64 = samples.iter().map(|&(t, u)| t * u).sum();
    let denom = nf * sum_tt - sum_t * sum_t;
    if denom.abs() < f64::EPSILON {
        return 0.0;
    }
    (nf * sum_tu - sum_t * sum_u) / denom
}

/// Seconds until a store at `used` fills `capacity` given a fill `velocity`
/// (units/sec). `None` when velocity ≤ 0 (not filling — no deadline). This is
/// the storage predictive trigger: grow NOW when
/// `seconds_to_full < resize_cooldown · safety_k`.
#[must_use]
pub fn seconds_to_full(used: u64, capacity: u64, velocity: f64) -> Option<f64> {
    if velocity <= 0.0 || used >= capacity {
        return if used >= capacity { Some(0.0) } else { None };
    }
    #[allow(clippy::cast_precision_loss)]
    let remaining = (capacity - used) as f64;
    Some(remaining / velocity)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sp(bps: u16) -> UtilizationSetpoint {
        UtilizationSetpoint::from_bps(bps)
    }

    #[test]
    fn carve_seats_utilization_at_or_below_the_setpoint() {
        // 5 GiB used, seat at 80% → target ⌈5/0.8⌉ = 7 (6.25 rounds up).
        let target = carve_to_setpoint(5, sp(8_000));
        assert_eq!(target, 7);
        assert!(realized_utilization(5, target) <= 0.80 + 1e-9);
    }

    #[test]
    fn carve_of_zero_is_zero() {
        assert_eq!(carve_to_setpoint(0, sp(8_000)), 0);
    }

    #[test]
    fn dual_purpose_lemma_cost_and_resiliency_together() {
        // CLAUSE 6 proven numerically: the ONE carve is BOTH a cost control
        // AND a resiliency maximizer, at the same time.
        //
        // A workload provisioned at 155 (GiB) but only using 5 — the 155GB
        // receipt shape. One carve to the 80% setpoint:
        let over_provisioned_limit: u64 = 155;
        let used: u64 = 5;
        let target = carve_to_setpoint(used, sp(8_000));

        // (a) COST: the carved target is far below the over-provisioned limit
        //     — money saved, by construction (right-size-down).
        assert!(
            target < over_provisioned_limit,
            "carve must reduce the over-provisioned limit (cost win): {target} !< {over_provisioned_limit}"
        );
        assert_eq!(target, 7); // 155 → 7 GiB: the 155GB waste is carved away.

        // (b) RESILIENCY: the SAME carved target still holds ≥ 1/setpoint
        //     headroom before saturation — util ≤ setpoint, room to absorb a
        //     burst (availability). Not a tradeoff: both hold at once.
        let util = realized_utilization(used, target);
        assert!(util <= 0.80 + 1e-9, "carve must preserve headroom (resiliency): util {util} > 0.80");
        assert!(util > 0.0, "a carved target is not zero — the workload is served");
    }

    #[test]
    fn fill_velocity_is_a_clean_linear_fit() {
        // used grows 1 unit/sec exactly → velocity 1.0.
        let samples = [(0.0, 10.0), (1.0, 11.0), (2.0, 12.0), (3.0, 13.0)];
        let v = fill_velocity(&samples);
        assert!((v - 1.0).abs() < 1e-9, "expected 1.0 unit/sec, got {v}");
    }

    #[test]
    fn fill_velocity_degenerate_cases_are_zero() {
        assert_eq!(fill_velocity(&[]), 0.0);
        assert_eq!(fill_velocity(&[(0.0, 5.0)]), 0.0);
        // all-same-time → no slope.
        assert_eq!(fill_velocity(&[(1.0, 5.0), (1.0, 9.0)]), 0.0);
    }

    #[test]
    fn seconds_to_full_is_the_storage_deadline() {
        // 90 used of 100, filling 1/sec → 10s to full (the grow trigger).
        assert_eq!(seconds_to_full(90, 100, 1.0), Some(10.0));
        // not filling → no deadline.
        assert_eq!(seconds_to_full(90, 100, 0.0), None);
        // already full → 0s (grow NOW).
        assert_eq!(seconds_to_full(100, 100, 1.0), Some(0.0));
    }

    #[test]
    fn multiplicative_band_law_matches_the_pure_fn() {
        let law = MultiplicativeBand;
        assert_eq!(law.carve(5, sp(8_000)), carve_to_setpoint(5, sp(8_000)));
    }
}
