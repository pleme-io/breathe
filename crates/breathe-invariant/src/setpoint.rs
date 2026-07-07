//! The utilization-setpoint SEAL — breathability clause 2, at the border.
//!
//! `/algorithmic-prowess-seal`: the best-fit construction for "carving is TO
//! a utilization setpoint, a target in the open interval (0, 1)" is a
//! **refined type** whose only constructors reject the out-of-range case:
//!
//! - **Catalog authoring is truly-unrepresentable.** The `const`
//!   [`UtilizationSetpoint::from_bps`] `assert!`s its range, so a bad setpoint
//!   in a `const` catalog entry is a **const-eval compile error** — there is
//!   no expressible path to a `const` setpoint of 0% or 100%.
//! - **The wire boundary is parse-time-rejected.** `Deserialize` routes
//!   through [`UtilizationSetpoint::try_from_ratio`], so `1.5` / `0.0` in JSON
//!   is an `Err` at the border, never a value that flows into a carve.
//!
//! The value is basis points (0–10000) so the type is `Copy`, hashable, and
//! exact — no float in the stored invariant. NO ML: a bounded integer + a
//! refinement, the top of the smallest-sufficient ladder for this invariant.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A sealed utilization setpoint (breathability clause 2). The stored value
/// is basis points in `1..=9999` — the open interval (0, 1). A setpoint of
/// exactly 0% (carve to nothing) or 100% (no headroom) has no code path.
///
/// The field is private; the only ways in are the range-checked constructors,
/// so an out-of-range setpoint is unrepresentable past construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UtilizationSetpoint(u16);

/// Why a candidate ratio is not a valid utilization setpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetpointError {
    /// The ratio was ≤ 0 — carving to zero utilization is forbidden.
    NotPositive,
    /// The ratio was ≥ 1 — a setpoint with no headroom is forbidden (there
    /// would be no room to absorb a burst before saturation).
    AtOrAboveOne,
    /// The ratio was NaN / non-finite.
    NotFinite,
}

impl std::fmt::Display for SetpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetpointError::NotPositive => {
                write!(f, "utilization setpoint must be > 0 (carve-to-zero is forbidden)")
            }
            SetpointError::AtOrAboveOne => {
                write!(f, "utilization setpoint must be < 1 (a setpoint needs headroom)")
            }
            SetpointError::NotFinite => write!(f, "utilization setpoint must be finite"),
        }
    }
}

impl std::error::Error for SetpointError {}

impl UtilizationSetpoint {
    /// The `const` constructor — the catalog-authoring seal. Panics (a
    /// **const-eval compile error** when used in `const` context) if `bps` is
    /// not in `1..=9999`, so a bad setpoint in a `const` catalog entry cannot
    /// compile.
    ///
    /// # Panics
    /// When `bps == 0` or `bps >= 10_000`.
    #[must_use]
    pub const fn from_bps(bps: u16) -> Self {
        assert!(
            bps > 0 && bps < 10_000,
            "utilization setpoint must be in the open interval (0, 1) — basis points 1..=9999"
        );
        Self(bps)
    }

    /// The fallible ratio constructor — the wire seal. Rejects ≤ 0, ≥ 1, and
    /// non-finite at the boundary.
    ///
    /// # Errors
    /// [`SetpointError`] for a non-finite / non-positive / ≥ 1 ratio.
    pub fn try_from_ratio(r: f64) -> Result<Self, SetpointError> {
        if !r.is_finite() {
            return Err(SetpointError::NotFinite);
        }
        if r <= 0.0 {
            return Err(SetpointError::NotPositive);
        }
        if r >= 1.0 {
            return Err(SetpointError::AtOrAboveOne);
        }
        // Round to nearest basis point; the range checks above guarantee
        // 1..=9999 after rounding (a ratio in (0,1) maps into (0,10000)).
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let bps = (r * 10_000.0).round() as u16;
        Ok(Self(bps.clamp(1, 9_999)))
    }

    /// The setpoint as a ratio in (0, 1).
    #[must_use]
    pub fn as_ratio(self) -> f64 {
        f64::from(self.0) / 10_000.0
    }

    /// The setpoint in basis points (1..=9999).
    #[must_use]
    pub fn bps(self) -> u16 {
        self.0
    }

    /// The setpoint rendered as a percent string (e.g. `"80%"`), for reports.
    #[must_use]
    pub fn percent(self) -> u16 {
        self.0 / 100
    }
}

impl Serialize for UtilizationSetpoint {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_f64(self.as_ratio())
    }
}

impl<'de> Deserialize<'de> for UtilizationSetpoint {
    /// Parse-time rejection: an out-of-range setpoint in the wire form is an
    /// `Err` at the boundary, never a value that flows into a carve.
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let r = f64::deserialize(d)?;
        UtilizationSetpoint::try_from_ratio(r).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn const_constructor_accepts_the_canonical_setpoints() {
        // The 80/20 homeostasis defaults — const-constructible.
        const EIGHTY: UtilizationSetpoint = UtilizationSetpoint::from_bps(8_000);
        const TWENTY: UtilizationSetpoint = UtilizationSetpoint::from_bps(2_000);
        assert!((EIGHTY.as_ratio() - 0.80).abs() < 1e-9);
        assert_eq!(EIGHTY.percent(), 80);
        assert_eq!(TWENTY.percent(), 20);
    }

    #[test]
    fn wire_boundary_rejects_out_of_range() {
        // parse-time-rejected: 0.0 / 1.0 / 1.5 cannot deserialize.
        assert!(serde_json::from_str::<UtilizationSetpoint>("0.8").is_ok());
        assert!(serde_json::from_str::<UtilizationSetpoint>("0.0").is_err());
        assert!(serde_json::from_str::<UtilizationSetpoint>("1.0").is_err());
        assert!(serde_json::from_str::<UtilizationSetpoint>("1.5").is_err());
    }

    #[test]
    fn try_from_ratio_enforces_the_open_interval() {
        assert_eq!(UtilizationSetpoint::try_from_ratio(0.0), Err(SetpointError::NotPositive));
        assert_eq!(UtilizationSetpoint::try_from_ratio(-0.1), Err(SetpointError::NotPositive));
        assert_eq!(UtilizationSetpoint::try_from_ratio(1.0), Err(SetpointError::AtOrAboveOne));
        assert_eq!(UtilizationSetpoint::try_from_ratio(f64::NAN), Err(SetpointError::NotFinite));
        assert!(UtilizationSetpoint::try_from_ratio(0.8).is_ok());
    }

    #[test]
    fn round_trips_through_json() {
        let sp = UtilizationSetpoint::from_bps(8_000);
        let js = serde_json::to_string(&sp).unwrap();
        let back: UtilizationSetpoint = serde_json::from_str(&js).unwrap();
        assert_eq!(sp, back);
    }
}
