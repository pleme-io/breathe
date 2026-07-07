//! The UNIVERSAL breathability checker — one law, applied to any workload's
//! consumed-dimension profile.
//!
//! A workload consumes some set of resource dimensions (it uses memory + cpu;
//! a database also uses storage + replica + engine knobs). The breathability
//! invariant is a law over exactly one shape: **for every consumed dimension,
//! a Band carves it, to a setpoint, default-on, dual-purpose.** This module
//! is the checker over that shape — the pure, deterministic verdict the matrix
//! and a `breathe confirm` report both run.
//!
//! `/algorithmic-prowess-seal` — the checks, best-fit, no ML:
//! - clause 1 + 4 (carved-by-a-Band / models-stay-current): a set-membership
//!   test against the catalog's claimed set — a dimension the doctrine claims
//!   but the workload leaves uncarved (and unacknowledged) is the 155GB class.
//! - clause 2 (carve-to-setpoint): the setpoint is already a sealed type, so
//!   the checker only tests presence-when-carved.
//! - clause 3 (default-on): a boolean the checker surfaces.
//! - clause 6 (dual-purpose): both effects must be named on a carved use.

use crate::catalog;
use crate::dimension::DimensionId;
use crate::setpoint::UtilizationSetpoint;

/// One dimension a workload consumes, and how it is carved. This is the
/// normalized projection every real breathe enrollment reduces to.
#[derive(Clone, Debug)]
pub struct DimensionUse {
    /// The consumed dimension.
    pub dimension: DimensionId,
    /// Is this dimension carved by a Band in this workload's enrollment?
    pub carved: bool,
    /// The setpoint the carve holds (clause 2) — `Some` iff carved. Being a
    /// [`UtilizationSetpoint`], an out-of-range value is already
    /// unrepresentable here; the checker only tests presence.
    pub setpoint: Option<UtilizationSetpoint>,
    /// Is the carve on by DEFAULT (clause 3), not opt-in?
    pub default_on: bool,
    /// Does the carve name a cost effect (clause 6)?
    pub cost_named: bool,
    /// Does the carve name a resiliency effect (clause 6)?
    pub resiliency_named: bool,
    /// An explicit `pending-breathe` acknowledgement when this dimension is
    /// claimed but intentionally left uncarved (interim) — the honest escape
    /// hatch that keeps a known gap out of the violation set.
    pub pending: Option<String>,
}

impl DimensionUse {
    /// A fully-conformant carve of `dimension` at `setpoint` (default-on,
    /// dual-purpose). The shape a shipped/landing dimension presents.
    #[must_use]
    pub fn carved(dimension: DimensionId, setpoint: UtilizationSetpoint) -> Self {
        Self {
            dimension,
            carved: true,
            setpoint: Some(setpoint),
            default_on: true,
            cost_named: true,
            resiliency_named: true,
            pending: None,
        }
    }

    /// An UNCARVED consumption of `dimension` — no Band, no pending note. The
    /// 155GB-class shape (a claimed dimension left silently uncarved).
    #[must_use]
    pub fn uncarved(dimension: DimensionId) -> Self {
        Self {
            dimension,
            carved: false,
            setpoint: None,
            default_on: false,
            cost_named: false,
            resiliency_named: false,
            pending: None,
        }
    }

    /// An uncarved consumption with an explicit `pending-breathe` note — a
    /// tracked interim gap (honest, not a violation).
    #[must_use]
    pub fn pending(dimension: DimensionId, note: impl Into<String>) -> Self {
        Self {
            dimension,
            carved: false,
            setpoint: None,
            default_on: false,
            cost_named: false,
            resiliency_named: false,
            pending: Some(note.into()),
        }
    }
}

/// A workload's consumed-dimension profile — ecosystem-agnostic.
#[derive(Clone, Debug, Default)]
pub struct WorkloadProfile {
    pub uses: Vec<DimensionUse>,
}

impl WorkloadProfile {
    #[must_use]
    pub fn new(uses: Vec<DimensionUse>) -> Self {
        Self { uses }
    }
}

/// A universal breathability violation — the law's failure modes, named once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BreatheViolation {
    /// Clause 1 + 4: the doctrine CLAIMS this dimension is breathed, but the
    /// workload leaves it uncarved with no `pending` acknowledgement. **THE
    /// 155GB class** — a claimed-but-uncarved dimension (storage was
    /// doctrine-claimed as "elasticity" but never carved → 155 GB provisioned
    /// / 5 GB used).
    ClaimedButUncarved { dimension: DimensionId },
    /// Clause 2: a carved dimension carries no setpoint (a static request
    /// masquerading as a carve).
    CarveWithoutSetpoint { dimension: DimensionId },
    /// Clause 3: a carved dimension is not default-on (opt-in breathability).
    NotDefaultOn { dimension: DimensionId },
    /// Clause 6: a carved dimension names only one of {cost, resiliency} — a
    /// Band claiming a tradeoff instead of both-at-once.
    DualPurposeIncomplete {
        dimension: DimensionId,
        cost_named: bool,
        resiliency_named: bool,
    },
}

impl BreatheViolation {
    /// A stable kebab-case rule name + optional locus, for a `confirm` report.
    #[must_use]
    pub fn locus(&self) -> (&'static str, Option<String>) {
        match self {
            BreatheViolation::ClaimedButUncarved { dimension } => {
                ("breathe-claimed-but-uncarved", Some(dimension.as_str().to_string()))
            }
            BreatheViolation::CarveWithoutSetpoint { dimension } => {
                ("breathe-carve-without-setpoint", Some(dimension.as_str().to_string()))
            }
            BreatheViolation::NotDefaultOn { dimension } => {
                ("breathe-not-default-on", Some(dimension.as_str().to_string()))
            }
            BreatheViolation::DualPurposeIncomplete { dimension, .. } => {
                ("breathe-dual-purpose-incomplete", Some(dimension.as_str().to_string()))
            }
        }
    }
}

/// Outcome of a breathability check: the violations (empty = well-formed)
/// plus the count of dimensions carved dual-purpose.
#[derive(Clone, Debug)]
pub struct BreatheCheckOutcome {
    pub violations: Vec<BreatheViolation>,
    /// Dimensions carved with BOTH a cost and a resiliency effect (the
    /// dual-purpose count — the both-outcomes-by-construction witness).
    pub dual_purpose_carves: usize,
}

impl BreatheCheckOutcome {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.violations.is_empty()
    }
}

/// Run the universal breathability clauses over one workload's profile.
///
/// A dimension is treated as doctrine-claimed iff the catalog says so. Pure,
/// deterministic, no I/O. Violations AGGREGATE — one call reports every
/// broken clause, not just the first (CLOSED-LOOP MASS-SYNTHESIS rule 1).
#[must_use]
pub fn check(profile: &WorkloadProfile) -> BreatheCheckOutcome {
    let mut violations = Vec::new();
    let mut dual_purpose_carves = 0usize;

    for u in &profile.uses {
        let claimed = catalog::dimension(u.dimension).is_some_and(|d| d.claimed_by_doctrine);

        if u.carved {
            // clause 2 — a carve must carry a setpoint.
            if u.setpoint.is_none() {
                violations.push(BreatheViolation::CarveWithoutSetpoint { dimension: u.dimension });
            }
            // clause 3 — carving is default-on.
            if !u.default_on {
                violations.push(BreatheViolation::NotDefaultOn { dimension: u.dimension });
            }
            // clause 6 — dual-purpose: BOTH effects, never one.
            if u.cost_named && u.resiliency_named {
                dual_purpose_carves += 1;
            } else {
                violations.push(BreatheViolation::DualPurposeIncomplete {
                    dimension: u.dimension,
                    cost_named: u.cost_named,
                    resiliency_named: u.resiliency_named,
                });
            }
        } else if claimed && u.pending.is_none() {
            // clause 1 + 4 — THE 155GB class: claimed, uncarved, unacknowledged.
            violations.push(BreatheViolation::ClaimedButUncarved { dimension: u.dimension });
        }
        // (uncarved + pending = a tracked interim gap; not a violation.)
    }

    BreatheCheckOutcome { violations, dual_purpose_carves }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setpoint::UtilizationSetpoint;

    fn sp() -> UtilizationSetpoint {
        UtilizationSetpoint::from_bps(8_000)
    }

    #[test]
    fn a_fully_carved_workload_is_clean() {
        let profile = WorkloadProfile::new(vec![
            DimensionUse::carved(DimensionId::Memory, sp()),
            DimensionUse::carved(DimensionId::Cpu, sp()),
        ]);
        let out = check(&profile);
        assert!(out.is_valid(), "conformant workload must be clean: {:?}", out.violations);
        assert_eq!(out.dual_purpose_carves, 2);
    }

    #[test]
    fn the_155gb_storage_class_is_caught() {
        // A workload consuming storage but leaving it UNCARVED with no pending
        // note — the exact 155GB receipt shape. Must be a violation.
        let profile = WorkloadProfile::new(vec![
            DimensionUse::carved(DimensionId::Memory, sp()),
            DimensionUse::uncarved(DimensionId::Storage),
        ]);
        let out = check(&profile);
        assert!(!out.is_valid());
        assert!(
            out.violations.iter().any(|v| matches!(
                v,
                BreatheViolation::ClaimedButUncarved { dimension: DimensionId::Storage }
            )),
            "storage-claimed-not-carved must be a ClaimedButUncarved violation, got {:?}",
            out.violations
        );
    }

    #[test]
    fn a_pending_gap_is_tracked_not_a_violation() {
        // The honest escape hatch: an uncarved dimension WITH a pending note
        // is a tracked interim gap, not the 155GB class.
        let profile = WorkloadProfile::new(vec![DimensionUse::pending(
            DimensionId::Database,
            "pending-breathe: DatabaseBand unbuilt",
        )]);
        let out = check(&profile);
        assert!(out.is_valid(), "a pending-acknowledged gap is not a violation: {:?}", out.violations);
    }

    #[test]
    fn a_carve_without_a_setpoint_is_a_violation() {
        // clause 2: a static request masquerading as a carve.
        let mut u = DimensionUse::carved(DimensionId::Cpu, sp());
        u.setpoint = None;
        let out = check(&WorkloadProfile::new(vec![u]));
        assert!(out.violations.iter().any(|v| matches!(v, BreatheViolation::CarveWithoutSetpoint { .. })));
    }

    #[test]
    fn a_single_effect_carve_is_a_dual_purpose_violation() {
        // clause 6: a Band claiming a tradeoff (cost only, no resiliency).
        let mut u = DimensionUse::carved(DimensionId::Memory, sp());
        u.resiliency_named = false;
        let out = check(&WorkloadProfile::new(vec![u]));
        assert!(out.violations.iter().any(|v| matches!(
            v,
            BreatheViolation::DualPurposeIncomplete { resiliency_named: false, .. }
        )));
        assert_eq!(out.dual_purpose_carves, 0);
    }

    #[test]
    fn violations_aggregate_not_short_circuit() {
        // Break three clauses across two dimensions; one call reports all.
        let mut cpu = DimensionUse::carved(DimensionId::Cpu, sp());
        cpu.default_on = false; // clause 3
        cpu.cost_named = false; // clause 6
        let profile = WorkloadProfile::new(vec![
            cpu,
            DimensionUse::uncarved(DimensionId::Storage), // clause 1+4
        ]);
        let out = check(&profile);
        assert!(out.violations.len() >= 3, "expected aggregated violations, got {:?}", out.violations);
    }

    #[test]
    fn violation_locus_is_stable_kebab() {
        let v = BreatheViolation::ClaimedButUncarved { dimension: DimensionId::Storage };
        assert_eq!(v.locus(), ("breathe-claimed-but-uncarved", Some("storage".to_string())));
    }
}
