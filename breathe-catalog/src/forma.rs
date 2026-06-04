//! `Floresta` — the provisioning catalog (docs/PROVISIONING.md §2.2), the
//! infra-scale peer of the dimension [`CATALOG`](crate::CATALOG). A
//! self-describing registry of resource SHAPES (CATALOG REFLECTION) plus the
//! capacity envelope [`Densa`] (the thesis L2 / P7) and its never-swap fits-check
//! lifted to cluster scale (BREATHABILITY-MATH §4.3). Adding a `Forma` *requires*
//! a `FormaSpec` row in the same commit — the reflection tests enforce the
//! bijection.

use breathe_provider::{Directionality, Forma};

use crate::Maturity;

/// One resource SHAPE declared as typed data — the infra-scale peer of
/// [`DimensionSpec`](crate::DimensionSpec).
#[derive(Debug, Clone)]
pub struct FormaSpec {
    pub forma: Forma,
    pub name: &'static str,
    /// The tatara-lisp authoring form this shape exposes.
    pub authoring_keyword: &'static str,
    pub maturity: Maturity,
    pub directionality: Directionality,
    /// How long one provision becomes usable capacity — the predictor's look-ahead
    /// floor (the P8 dead-time; BREATHABILITY-MATH §5.3). Seconds.
    pub relief_latency_secs: u64,
    /// The unit one provision adds (`"node"`, `"gpu"`, `"slot"`).
    pub unit: &'static str,
    pub purpose: &'static str,
    /// The provisioning backend that realizes a `provision()` (always a typed,
    /// attested path — a magma Plan / a JIT-builder wake — never a direct cloud call).
    pub backend: &'static str,
    /// The upstream surface this shape mirrors, if any.
    pub upstream_mirror: Option<&'static str>,
    /// The shapes this one falls back to when it cannot provision (the typed
    /// `Cascata` edges — e.g. `NodeSpot → NodeOnDemand` on quota exhaustion). The
    /// graph MUST be acyclic ([`cascata_is_acyclic`]).
    pub falls_back_to: &'static [Forma],
}

/// The catalog. One row per [`Forma`]; the reflection tests enforce the bijection.
pub const FLORESTA: &[FormaSpec] = &[FormaSpec {
    forma: Forma::NodeOnDemand,
    name: "node-on-demand",
    authoring_keyword: "defforma-node-on-demand",
    // Observe-only at M0; the live provision (a magma Plan) lands at M2.
    maturity: Maturity::M2Typed,
    directionality: Directionality::Bidirectional,
    relief_latency_secs: 180,
    unit: "node",
    purpose: "provision on-demand cloud nodes to host pending pods, carved by the band law",
    backend: "magma plan (ASG desired-count / Karpenter NodeClaim)",
    upstream_mirror: Some("AWS ASG / Karpenter NodePool"),
    falls_back_to: &[],
}];

/// Every shape — the domain side of the catalog bijection.
pub const ALL_FORMAS: [Forma; 1] = [Forma::NodeOnDemand];

/// The `FormaSpec` for a shape (`None` if absent — a reflection-test failure).
#[must_use]
pub fn lookup_forma(f: Forma) -> Option<&'static FormaSpec> {
    FLORESTA.iter().find(|s| s.forma == f)
}

/// The `Cascata` fallback graph is acyclic — a cycle `A → B → A` would loop the
/// auctioneer forever. DFS three-colour cycle detection over `falls_back_to`.
#[must_use]
pub fn cascata_is_acyclic() -> bool {
    fn visit(f: Forma, stack: &mut Vec<Forma>, done: &mut Vec<Forma>) -> bool {
        if stack.contains(&f) {
            return false; // back-edge → cycle
        }
        if done.contains(&f) {
            return true;
        }
        stack.push(f);
        let ok = lookup_forma(f)
            .map(|s| s.falls_back_to.iter().all(|&n| visit(n, stack, done)))
            .unwrap_or(true);
        stack.pop();
        done.push(f);
        ok
    }
    let mut stack = Vec::new();
    let mut done = Vec::new();
    ALL_FORMAS.iter().all(|&f| visit(f, &mut stack, &mut done))
}

// ============================================================================
// Densa — the capacity envelope (the thesis L2 / P7).
// ============================================================================

/// A per-shape `[floor, ceiling]` bound in a [`Densa`] envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormaBound {
    pub forma: Forma,
    /// The provisioned-from-peak floor (must ALWAYS fit — the never-swap base).
    pub floor: u64,
    /// The L2 hard ceiling — the band law's `BandConfig.ceiling`; carving stays ≤ it.
    pub ceiling: u64,
}

/// The capacity envelope for a pool of same-unit shapes — the hard wall the bands
/// carve *within* (L1 ⊂ L2). Cross-unit envelopes (nodes vs GPUs) compose multiple
/// `Densa`s, one per pool; the sum-of-floors check below is meaningful only within
/// a single unit/pool (honest scope — the cross-unit case is the `Otimizador`'s,
/// not a single envelope's).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Densa {
    pub bounds: Vec<FormaBound>,
    /// Units that must stay free (reserve headroom — never provisioned into).
    pub reserve: u64,
    /// The pool's hard capacity (the never-swap denominator), same unit as the bounds.
    pub pool_capacity: u64,
}

/// Why a [`Densa`] is rejected — a typed refusal, never an applied bad envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DensaError {
    /// A floor exceeds its own ceiling — the band could never satisfy it.
    FloorAboveCeiling { forma: Forma, floor: u64, ceiling: u64 },
    /// Σ floors + reserve exceeds the pool capacity — the never-swap breach
    /// (the cluster-scale `OOM`): the floors are not guaranteed to fit.
    DoesNotFit { sum_floors: u64, reserve: u64, capacity: u64 },
}

impl Densa {
    /// The never-swap fits-check (BREATHABILITY-MATH §4.3 / V4) lifted to the pool:
    /// every floor ≤ its ceiling, AND `Σ floors + reserve ≤ pool_capacity`. A
    /// `Densa` that fails is **refused** (a parse/admission error), never applied —
    /// the cluster-scale floor-from-peak proof that the declared floors fit before
    /// any provision runs.
    pub fn fits(&self) -> Result<(), DensaError> {
        for b in &self.bounds {
            if b.floor > b.ceiling {
                return Err(DensaError::FloorAboveCeiling { forma: b.forma, floor: b.floor, ceiling: b.ceiling });
            }
        }
        let sum_floors: u64 = self.bounds.iter().map(|b| b.floor).sum();
        if sum_floors.saturating_add(self.reserve) <= self.pool_capacity {
            Ok(())
        } else {
            Err(DensaError::DoesNotFit { sum_floors, reserve: self.reserve, capacity: self.pool_capacity })
        }
    }

    /// The L2 ceiling for a shape — the `BandConfig.ceiling` the band law carves
    /// within. `None` if the shape is not in this envelope.
    #[must_use]
    pub fn ceiling(&self, forma: Forma) -> Option<u64> {
        self.bounds.iter().find(|b| b.forma == forma).map(|b| b.ceiling)
    }

    /// The provisioned-from-peak floor for a shape.
    #[must_use]
    pub fn floor(&self, forma: Forma) -> Option<u64> {
        self.bounds.iter().find(|b| b.forma == forma).map(|b| b.floor)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        cascata_is_acyclic, lookup_forma, Densa, DensaError, FormaBound, FormaSpec, ALL_FORMAS, FLORESTA,
    };
    use breathe_provider::Forma;

    #[test]
    fn floresta_has_one_row_per_forma() {
        assert_eq!(FLORESTA.len(), ALL_FORMAS.len(), "row count == forma count");
        for f in ALL_FORMAS {
            let n = FLORESTA.iter().filter(|s| s.forma == f).count();
            assert_eq!(n, 1, "exactly one row for {f}");
        }
    }

    #[test]
    fn floresta_authoring_keywords_are_unique() {
        for (i, a) in FLORESTA.iter().enumerate() {
            for b in &FLORESTA[i + 1..] {
                assert_ne!(a.authoring_keyword, b.authoring_keyword, "keyword collision");
            }
        }
    }

    #[test]
    fn floresta_relief_latencies_are_positive() {
        for s in FLORESTA {
            assert!(s.relief_latency_secs > 0, "{}: relief_latency must be > 0 (P8 dead-time)", s.name);
        }
    }

    #[test]
    fn floresta_fallbacks_reference_known_formas() {
        for s in FLORESTA {
            for &fb in s.falls_back_to {
                assert!(lookup_forma(fb).is_some(), "{} falls back to a missing forma", s.name);
            }
        }
    }

    #[test]
    fn cascata_graph_is_acyclic() {
        assert!(cascata_is_acyclic());
    }

    // A synthetic cyclic catalog would be caught — proven by a local DFS on a hand
    // cycle (the production FLORESTA has no fallbacks yet, so we test the detector).
    #[test]
    fn cascata_detector_catches_a_cycle() {
        // a → b → a, detected by the same three-colour walk
        fn cyclic(f: u8, stack: &mut Vec<u8>, edges: &dyn Fn(u8) -> Vec<u8>) -> bool {
            if stack.contains(&f) {
                return false;
            }
            stack.push(f);
            let ok = edges(f).into_iter().all(|n| cyclic(n, stack, edges));
            stack.pop();
            ok
        }
        let edges = |f: u8| if f == 0 { vec![1] } else { vec![0] };
        assert!(!cyclic(0, &mut Vec::new(), &edges), "a→b→a must be detected as cyclic");
    }

    #[test]
    fn densa_fits_accepts_a_valid_envelope() {
        let d = Densa {
            bounds: vec![FormaBound { forma: Forma::NodeOnDemand, floor: 2, ceiling: 10 }],
            reserve: 1,
            pool_capacity: 20,
        };
        assert!(d.fits().is_ok());
        assert_eq!(d.ceiling(Forma::NodeOnDemand), Some(10));
        assert_eq!(d.floor(Forma::NodeOnDemand), Some(2));
    }

    #[test]
    fn densa_refuses_floor_above_ceiling() {
        let d = Densa {
            bounds: vec![FormaBound { forma: Forma::NodeOnDemand, floor: 12, ceiling: 10 }],
            reserve: 0,
            pool_capacity: 100,
        };
        assert!(matches!(d.fits(), Err(DensaError::FloorAboveCeiling { .. })));
    }

    #[test]
    fn densa_refuses_oversubscribed_floors() {
        // Σ floors (8) + reserve (5) = 13 > capacity 10 → never-swap breach.
        let d = Densa {
            bounds: vec![FormaBound { forma: Forma::NodeOnDemand, floor: 8, ceiling: 9 }],
            reserve: 5,
            pool_capacity: 10,
        };
        match d.fits() {
            Err(DensaError::DoesNotFit { sum_floors, reserve, capacity }) => {
                assert_eq!((sum_floors, reserve, capacity), (8, 5, 10));
            }
            other => panic!("expected DoesNotFit, got {other:?}"),
        }
    }

    // The FormaSpec is plain typed data — confirm the seed row round-trips.
    #[test]
    fn node_on_demand_spec_is_present_and_well_formed() {
        let s: &FormaSpec = lookup_forma(Forma::NodeOnDemand).expect("seed row");
        assert_eq!(s.unit, "node");
        assert!(s.backend.contains("magma"));
        assert!(s.upstream_mirror.is_some());
    }
}
