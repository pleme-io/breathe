//! Synthetic per-dimension [`WorkloadProfile`] fixtures the verification
//! matrix exercises.
//!
//! Each fixture presents one dimension in the state its catalog maturity
//! claims:
//! - a SHIPPED/LANDING dimension → a fully-carved use (Band + setpoint +
//!   default-on + dual-purpose) that the checker passes;
//! - a GAP dimension → an uncarved use carrying its `pending-breathe` note,
//!   so the matrix records the honest gap rather than pretending conformance.
//!
//! The DimensionUse fixtures faithfully mirror the real enrollment shape so
//! the matrix asserts the law, not a tautology.

use crate::catalog;
use crate::check::{DimensionUse, WorkloadProfile};
use crate::dimension::{DimensionId, Maturity};

/// The catalog's default setpoint for `id`, used to build a faithful carve.
fn setpoint_for(id: DimensionId) -> crate::setpoint::UtilizationSetpoint {
    catalog::dimension(id).expect("catalogued dimension").setpoint
}

/// A conformant single-dimension profile: the dimension carved to its
/// catalog setpoint, default-on, dual-purpose. The SHIPPED/LANDING shape.
#[must_use]
pub fn carved_fixture(id: DimensionId) -> WorkloadProfile {
    WorkloadProfile::new(vec![DimensionUse::carved(id, setpoint_for(id))])
}

/// A GAP profile: the dimension consumed but uncarved, carrying the catalog's
/// `pending-breathe` note (a tracked interim gap — honest, not conformant).
#[must_use]
pub fn gap_fixture(id: DimensionId) -> WorkloadProfile {
    let note = catalog::dimension(id)
        .and_then(|d| d.pending)
        .unwrap_or("pending-breathe: unbuilt");
    WorkloadProfile::new(vec![DimensionUse::pending(id, note)])
}

/// The 155GB receipt shape: a claimed dimension consumed but uncarved with NO
/// pending note — the class the models-stay-current gate must catch. Used by
/// the adversarial matrix test to prove the invariant has teeth.
#[must_use]
pub fn uncarved_claim_fixture(id: DimensionId) -> WorkloadProfile {
    WorkloadProfile::new(vec![DimensionUse::uncarved(id)])
}

/// Build the matrix fixture for a dimension based on its catalog maturity:
/// carved for Shipped/Landing, gap for Gap.
#[must_use]
pub fn fixture_for(id: DimensionId) -> WorkloadProfile {
    match catalog::dimension(id).map(|d| d.maturity) {
        Some(Maturity::Shipped | Maturity::Landing) => carved_fixture(id),
        _ => gap_fixture(id),
    }
}
