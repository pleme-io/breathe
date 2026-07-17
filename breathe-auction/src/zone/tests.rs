//! Unit tests for `zone` — proven against the REAL `breathe_catalog::CATALOG`
//! data (not a hand-picked hypothetical), so a future catalog edit that
//! removes/adds a `depends_on` edge is caught here, not silently drifted from.

use std::collections::HashSet;

use breathe_provider::{DimensionId, Forma};
use shigoto_types::JobScope;

use crate::quinhao::{Demand, DemandVector, PoolCapacity, Quinhao};

use super::{
    allocate_for_zone, dimension_job, dimension_tick_dag, dimensions_with_structural_dependencies, forma_job,
    zone_tick_dag, AllocationPolicy, BreatheZone, ZoneError,
};

fn scope() -> JobScope {
    JobScope::Workspace("camelot-controllers".into())
}

// ── dimension_tick_dag — the real, catalog-driven within-DimensionId edges ──

#[test]
fn replica_resolves_before_memory_and_cpu_in_the_real_catalog() {
    // Ground truth: breathe_catalog::CATALOG today declares
    // Memory.depends_on = [Replica] and Cpu.depends_on = [Replica]. This test
    // is a canary -- if a future catalog edit changes that, this test names
    // exactly what changed, rather than the Dag silently drifting.
    let dims = [DimensionId::Memory, DimensionId::Cpu, DimensionId::Replica];
    let d = dimension_tick_dag(&scope(), &dims);
    let waves = d.waves(None).unwrap();

    let replica = dimension_job(&scope(), DimensionId::Replica);
    let memory = dimension_job(&scope(), DimensionId::Memory);
    let cpu = dimension_job(&scope(), DimensionId::Cpu);

    assert_eq!(waves[0], vec![replica], "Replica is the only wave-0 job");
    assert!(waves[1].contains(&memory), "Memory is gated on Replica");
    assert!(waves[1].contains(&cpu), "Cpu is gated on Replica");
    assert_eq!(waves.len(), 2, "exactly 2 waves: {{replica}} -> {{memory, cpu}}");
}

#[test]
fn an_edge_to_a_dimension_outside_the_zone_is_dropped_not_dangling() {
    // Memory.depends_on = [Replica], but this zone doesn't enroll Replica at
    // all -- the edge must be DROPPED, never left dangling into a dimension
    // this Dag doesn't even contain.
    let dims = [DimensionId::Memory, DimensionId::Storage];
    let d = dimension_tick_dag(&scope(), &dims);
    let waves = d.waves(None).unwrap();

    assert_eq!(waves.len(), 1, "no Replica enrolled -> no edge -> everything in wave 0");
    assert_eq!(waves[0].len(), 2);
    assert_eq!(d.edge_count(), 0);
}

#[test]
fn dimensions_with_structural_dependencies_matches_the_real_catalog_today() {
    let map = dimensions_with_structural_dependencies();
    assert_eq!(map.get(&DimensionId::Memory), Some(&[DimensionId::Replica].as_slice()));
    assert_eq!(map.get(&DimensionId::Cpu), Some(&[DimensionId::Replica].as_slice()));
    assert_eq!(map.get(&DimensionId::Storage), None, "Storage has no depends_on edge today");
    assert_eq!(map.len(), 2, "exactly 2 dimensions carry a structural dependency in the shipped catalog");
}

// ── BreatheZone / zone_tick_dag — the cross-catalog Forma -> DimensionId bridge ──

const CAMELOT_FORMAS: &[Forma] = &[Forma::NodeSpot];
const CAMELOT_DIMS: &[DimensionId] = &[DimensionId::Cpu, DimensionId::Memory];

#[test]
fn zone_tick_dag_resolves_the_camelot_incident() {
    // The exact worked incident theory/BREATHABILITY.md §II.6.8 names: a
    // node-pool shape decision (Forma::NodeSpot) must resolve before the
    // Cpu/Memory bands that assume a pod-density ceiling it implies.
    let zone = BreatheZone {
        scope: scope(),
        formas: CAMELOT_FORMAS,
        dims: CAMELOT_DIMS,
        gated_dims: CAMELOT_DIMS,
        allocation_policy: AllocationPolicy::PerAxisIndependent,
    };
    let d = zone_tick_dag(&zone).unwrap();
    let waves = d.waves(None).unwrap();

    let node_spot = forma_job(&scope(), Forma::NodeSpot);
    let cpu = dimension_job(&scope(), DimensionId::Cpu);
    let mem = dimension_job(&scope(), DimensionId::Memory);

    assert_eq!(waves[0], vec![node_spot], "NodeSpot alone in wave 0");
    let wave1: HashSet<_> = waves[1].iter().cloned().collect();
    assert_eq!(wave1, HashSet::from([cpu, mem]), "Cpu and Memory both gated into wave 1, concurrent with each other");
    assert_eq!(waves.len(), 2);
}

#[test]
fn zone_tick_dag_composes_both_edge_sources_correctly() {
    // Dims = {Memory, Cpu, Replica}; only Memory+Cpu are Forma-gated (not
    // Replica). Replica has no depends_on edge either, so it should land
    // FREE in wave 0 alongside NodeSpot -- proving the two edge sources
    // (catalog depends_on + zone gated_dims) compose without over-serializing
    // a dimension neither source constrains.
    let dims: &[DimensionId] = &[DimensionId::Memory, DimensionId::Cpu, DimensionId::Replica];
    let zone = BreatheZone {
        scope: scope(),
        formas: CAMELOT_FORMAS,
        dims,
        gated_dims: &[DimensionId::Memory, DimensionId::Cpu],
        allocation_policy: AllocationPolicy::PerAxisIndependent,
    };
    let d = zone_tick_dag(&zone).unwrap();
    let waves = d.waves(None).unwrap();

    let node_spot = forma_job(&scope(), Forma::NodeSpot);
    let replica = dimension_job(&scope(), DimensionId::Replica);
    let memory = dimension_job(&scope(), DimensionId::Memory);
    let cpu = dimension_job(&scope(), DimensionId::Cpu);

    let wave0: HashSet<_> = waves[0].iter().cloned().collect();
    assert_eq!(wave0, HashSet::from([node_spot, replica]), "NodeSpot and Replica both free in wave 0");
    let wave1: HashSet<_> = waves[1].iter().cloned().collect();
    assert_eq!(wave1, HashSet::from([memory, cpu]), "Memory/Cpu gated by BOTH depends_on(Replica, dropped-not-enrolled-as-gate) and the Forma gate");
}

#[test]
fn zone_validate_refuses_a_gate_on_a_dim_this_zone_does_not_enroll() {
    let zone = BreatheZone {
        scope: scope(),
        formas: CAMELOT_FORMAS,
        dims: &[DimensionId::Cpu],
        gated_dims: &[DimensionId::Memory], // NOT in dims
        allocation_policy: AllocationPolicy::PerAxisIndependent,
    };
    assert_eq!(zone.validate(), Err(ZoneError::GatedDimNotEnrolled { dim: DimensionId::Memory }));
    assert!(zone_tick_dag(&zone).is_err(), "zone_tick_dag propagates the refusal, never builds a bad Dag");
}

#[test]
fn a_zone_with_no_gates_is_flat_everything_in_one_wave() {
    // No Forma gates anything -> only the (empty, for this dim subset)
    // depends_on edges apply. Storage has no depends_on -> flat.
    let zone = BreatheZone {
        scope: scope(),
        formas: CAMELOT_FORMAS,
        dims: &[DimensionId::Storage],
        gated_dims: &[],
        allocation_policy: AllocationPolicy::PerAxisIndependent,
    };
    let d = zone_tick_dag(&zone).unwrap();
    let waves = d.waves(None).unwrap();
    assert_eq!(waves.len(), 1, "no edges at all -> everything free in wave 0");
    assert_eq!(waves[0].len(), 2, "NodeSpot + Storage, both ungated");
}

// ── allocate_for_zone — the AllocationPolicy dispatcher ─────────────────────

fn cs(storage: u64, cpu: u64) -> DemandVector {
    DemandVector::new(
        Demand { weight: 1, min: 0, max: u64::MAX, demand: storage },
        Demand { weight: 1, min: 0, max: u64::MAX, demand: cpu },
        Demand::absent(),
        Demand::absent(),
    )
}

#[test]
fn allocate_for_zone_per_axis_independent_matches_allocate_fabric_directly() {
    let zone = BreatheZone {
        scope: scope(),
        formas: CAMELOT_FORMAS,
        dims: CAMELOT_DIMS,
        gated_dims: CAMELOT_DIMS,
        allocation_policy: AllocationPolicy::PerAxisIndependent,
    };
    let claimants = vec![Quinhao::root("a", cs(4, 1)), Quinhao::root("b", cs(1, 3))];
    let via_zone = allocate_for_zone(&zone, PoolCapacity::new(18, 9, 0, 0), 1.0, &claimants).unwrap();
    let direct = crate::quinhao::allocate_fabric(PoolCapacity::new(18, 9, 0, 0), 1.0, &claimants).unwrap();
    assert_eq!(via_zone.get("a"), direct.get("a"));
    assert_eq!(via_zone.get("b"), direct.get("b"));
}

#[test]
fn allocate_for_zone_dominant_resource_fairness_matches_allocate_drf_fabric_directly() {
    let zone = BreatheZone {
        scope: scope(),
        formas: CAMELOT_FORMAS,
        dims: CAMELOT_DIMS,
        gated_dims: CAMELOT_DIMS,
        allocation_policy: AllocationPolicy::DominantResourceFairness,
    };
    let claimants = vec![Quinhao::root("a", cs(4, 1)), Quinhao::root("b", cs(1, 3))];
    let via_zone = allocate_for_zone(&zone, PoolCapacity::new(18, 9, 0, 0), 1.0, &claimants).unwrap();
    let direct = crate::quinhao::allocate_drf_fabric(PoolCapacity::new(18, 9, 0, 0), 1.0, &claimants).unwrap();
    assert_eq!(via_zone.get("a"), direct.get("a"));
    assert_eq!(via_zone.get("b"), direct.get("b"));
    // The hand-verified DRF numbers, reached THROUGH the zone dispatcher this time.
    assert_eq!(via_zone.get_dim("a", crate::quinhao::Dim::Storage), 12);
    assert_eq!(via_zone.get_dim("a", crate::quinhao::Dim::Cpu), 3);
}

#[test]
fn allocate_for_zone_the_two_policies_genuinely_disagree_on_the_same_input() {
    // The whole point of the AllocationPolicy field: it must actually change
    // the result, not just the code path taken to reach an identical one.
    let claimants = vec![Quinhao::root("a", cs(4, 1)), Quinhao::root("b", cs(1, 3))];
    let per_axis = allocate_for_zone(
        &BreatheZone { scope: scope(), formas: CAMELOT_FORMAS, dims: CAMELOT_DIMS, gated_dims: CAMELOT_DIMS, allocation_policy: AllocationPolicy::PerAxisIndependent },
        PoolCapacity::new(18, 9, 0, 0), 1.0, &claimants,
    ).unwrap();
    let drf = allocate_for_zone(
        &BreatheZone { scope: scope(), formas: CAMELOT_FORMAS, dims: CAMELOT_DIMS, gated_dims: CAMELOT_DIMS, allocation_policy: AllocationPolicy::DominantResourceFairness },
        PoolCapacity::new(18, 9, 0, 0), 1.0, &claimants,
    ).unwrap();
    assert_ne!(per_axis.get("a"), drf.get("a"), "PerAxisIndependent and DominantResourceFairness genuinely diverge on the same claimants");
}
