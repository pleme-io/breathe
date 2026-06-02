//! `breathe-catalog` — the self-describing dimensions catalog (CATALOG REFLECTION).
//!
//! Every breathe dimension declares itself here as one typed row. Adding a
//! dimension is no longer just "write a provider" — it is "write a provider +
//! land its catalog row", and the reflection tests below **fail the build** if a
//! [`DimensionId`] variant has no row (or a row names no provider). The catalog
//! IS the inventory: `breathe-inventory` (M3) iterates it; the typed DAG of
//! `depends_on` edges gives the implementation order for free.
//!
//! Mirrors `sui-spec`'s catalog template; the maturity gate is a breathe-local
//! enum (a conscious fork, not a verbatim reuse of sui-spec's `M*TypedOnly`).

use breathe_provider::{DimensionId, Directionality};

/// Mechanical readiness signal — lets tooling plan implementation order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Maturity {
    /// Implemented + tested + shippable now.
    Working,
    /// Typed border + spec authored; interpreter lands at the named milestone.
    M2Typed,
    M3Typed,
    /// Declared for completeness; no mutating interpreter (e.g. ObserveOnly).
    Informational,
}

/// One dimension declared as typed data.
#[derive(Debug, Clone)]
pub struct DimensionSpec {
    pub id: DimensionId,
    pub name: &'static str,
    /// The tatara-lisp authoring form this dimension exposes.
    pub authoring_keyword: &'static str,
    pub maturity: Maturity,
    pub directionality: Directionality,
    pub purpose: &'static str,
    /// The upstream surface this mirrors, if any.
    pub upstream_mirror: Option<&'static str>,
    /// Dimensions this one consumes context from (the typed DAG edges).
    pub depends_on: &'static [DimensionId],
}

/// The catalog. One row per [`DimensionId`]; the reflection tests enforce the
/// bijection.
pub const CATALOG: &[DimensionSpec] = &[
    DimensionSpec {
        id: DimensionId::Memory,
        name: "memory",
        authoring_keyword: "defdimension-memory",
        maturity: Maturity::Working,
        directionality: Directionality::Bidirectional,
        purpose: "hold container memory at the band by carving resources.limits.memory",
        upstream_mirror: None,
        depends_on: &[DimensionId::Replica],
    },
    DimensionSpec {
        id: DimensionId::Storage,
        name: "storage",
        authoring_keyword: "defdimension-storage",
        maturity: Maturity::M2Typed,
        directionality: Directionality::GrowOnly,
        purpose: "grow PVC capacity at 80% (data persists; never shrink)",
        upstream_mirror: None,
        depends_on: &[],
    },
    DimensionSpec {
        id: DimensionId::Cpu,
        name: "cpu",
        authoring_keyword: "defdimension-cpu",
        maturity: Maturity::M2Typed,
        directionality: Directionality::Bidirectional,
        purpose: "hold cpu at the band by carving resources.limits.cpu (millicores)",
        upstream_mirror: None,
        depends_on: &[DimensionId::Replica],
    },
    DimensionSpec {
        id: DimensionId::Replica,
        name: "replica",
        authoring_keyword: "defdimension-replica",
        maturity: Maturity::Informational,
        directionality: Directionality::ObserveOnly,
        purpose: "observe replica count; compose with KEDA via disjoint fields (never write)",
        upstream_mirror: Some("KEDA ScaledObject"),
        depends_on: &[],
    },
];

/// All dimension ids the substrate knows (the partition the catalog must cover).
pub const ALL_DIMENSIONS: [DimensionId; 4] = [
    DimensionId::Memory,
    DimensionId::Storage,
    DimensionId::Cpu,
    DimensionId::Replica,
];

/// Look up a dimension's row.
#[must_use]
pub fn lookup(id: DimensionId) -> Option<&'static DimensionSpec> {
    CATALOG.iter().find(|d| d.id == id)
}

/// True when the `depends_on` DAG is acyclic (topological order solvable).
/// Iterative DFS with a visiting-set; pure, no allocation beyond two small vecs.
#[must_use]
pub fn dependency_graph_is_acyclic() -> bool {
    fn visit(id: DimensionId, stack: &mut Vec<DimensionId>, done: &mut Vec<DimensionId>) -> bool {
        if done.contains(&id) {
            return true;
        }
        if stack.contains(&id) {
            return false; // back-edge ⇒ cycle
        }
        stack.push(id);
        if let Some(spec) = lookup(id) {
            for &dep in spec.depends_on {
                if !visit(dep, stack, done) {
                    return false;
                }
            }
        }
        stack.pop();
        done.push(id);
        true
    }
    let mut done = Vec::new();
    for &id in &ALL_DIMENSIONS {
        let mut stack = Vec::new();
        if !visit(id, &mut stack, &mut done) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Substrate invariant: every dimension id has exactly one catalog row, and
    /// every row names a known id — the bijection that makes the catalog the
    /// inventory. Fails the build if a dimension is added without a row.
    #[test]
    fn catalog_is_a_bijection_with_dimension_ids() {
        assert_eq!(CATALOG.len(), ALL_DIMENSIONS.len(), "row count == dimension count");
        for &id in &ALL_DIMENSIONS {
            let n = CATALOG.iter().filter(|d| d.id == id).count();
            assert_eq!(n, 1, "exactly one row for {id}");
        }
    }

    /// Authoring keywords must be globally unique (no `(defdimension-*)` collision).
    #[test]
    fn authoring_keywords_are_unique() {
        for (i, a) in CATALOG.iter().enumerate() {
            for b in &CATALOG[i + 1..] {
                assert_ne!(a.authoring_keyword, b.authoring_keyword, "keyword collision");
            }
        }
    }

    /// The dependency DAG must be acyclic so M-phase order falls out mechanically.
    #[test]
    fn dependency_dag_has_no_cycle() {
        assert!(dependency_graph_is_acyclic());
    }

    /// Every `depends_on` edge must resolve to a real catalog row (no dangling refs).
    #[test]
    fn dependency_edges_resolve() {
        for d in CATALOG {
            for &dep in d.depends_on {
                assert!(lookup(dep).is_some(), "{} depends on a missing dimension", d.name);
            }
        }
    }

    /// The maturity histogram partitions the catalog (sum == size).
    #[test]
    fn maturity_histogram_partitions_the_catalog() {
        let counts = [Maturity::Working, Maturity::M2Typed, Maturity::M3Typed, Maturity::Informational]
            .iter()
            .map(|m| CATALOG.iter().filter(|d| d.maturity == *m).count())
            .sum::<usize>();
        assert_eq!(counts, CATALOG.len());
    }

    /// The directionality recorded in the catalog must match each provider's
    /// contract (memory/cpu bidirectional, storage grow-only, replica observe-only).
    #[test]
    fn directionality_matches_dimension_semantics() {
        assert_eq!(lookup(DimensionId::Memory).unwrap().directionality, Directionality::Bidirectional);
        assert_eq!(lookup(DimensionId::Storage).unwrap().directionality, Directionality::GrowOnly);
        assert_eq!(lookup(DimensionId::Cpu).unwrap().directionality, Directionality::Bidirectional);
        assert_eq!(lookup(DimensionId::Replica).unwrap().directionality, Directionality::ObserveOnly);
    }
}
