//! Property tests for the `quinhão` allocator — the rebalance theorems proved
//! over RANDOM forests + demand vectors (FABRIC §III.0: balanced at any time).
//!
//! The invariants:
//!  - band-bounded: Σ top-level grants ≤ capacity · setpoint, every axis.
//!  - tree-respecting: Σ children grants ≤ parent grant.
//!  - clamp-respecting: every grant ≤ min(max, demand) (when reachable).
//!  - even: equal-weight equal-demand siblings get equal grants.
//!  - monotone-add: adding a sibling never raises another's grant.
//!  - monotone-remove: removing a sibling never lowers a remaining one's grant.
//!  - deterministic: the allocation is a pure function (same input ⇒ same output).

use super::{allocate_drf, allocate_even, allocate_fabric, Demand, DemandVector, Dim, GrantVector, PoolCapacity, Quinhao};
use proptest::prelude::*;

/// A bounded random `Demand` on the storage axis (values kept small so Σ math
/// stays well inside u64 and the tests run fast).
fn demand_strategy() -> impl Strategy<Value = Demand> {
    (0u32..4, 0u64..50, 1u64..2000, 0u64..2000).prop_map(|(weight, min, max, demand)| {
        // ensure max ≥ min so the clamp window is non-empty
        let max = max.max(min);
        Demand { weight, min, max, demand }
    })
}

/// A random flat pool of 1..8 storage-only even-or-weighted members.
fn flat_members_strategy() -> impl Strategy<Value = Vec<Quinhao>> {
    proptest::collection::vec(demand_strategy(), 1..8).prop_map(|demands| {
        demands
            .into_iter()
            .enumerate()
            .map(|(i, d)| Quinhao::root(format!("m{i}"), DemandVector::storage_only(d)))
            .collect()
    })
}

proptest! {
    /// allocate_even never over-allocates the band, and respects every clamp.
    #[test]
    fn even_kernel_is_band_bounded_and_clamp_respecting(
        band in 0u64..100_000,
        demands in proptest::collection::vec(demand_strategy(), 0..10),
    ) {
        let g = allocate_even(band, &demands);
        prop_assert_eq!(g.len(), demands.len());
        let sum: u128 = g.iter().map(|&x| u128::from(x)).sum();
        prop_assert!(sum <= u128::from(band), "Σ grants {sum} > band {band}");
        for (grant, d) in g.iter().zip(&demands) {
            // a grant never exceeds min(max, demand) UNLESS the floor itself does
            // (a min above the cap is the operator's contradiction; floors win).
            let cap = d.max.min(d.demand).max(d.min);
            prop_assert!(*grant <= cap.max(d.min), "grant {grant} exceeds cap {cap}");
        }
    }

    /// Equal-weight, equal-demand, equal-bound claimants get grants within 1 unit
    /// of each other (the EVEN split; the ±1 is the integer-division crumb).
    #[test]
    fn equal_claimants_get_equal_grants(band in 0u64..100_000, n in 1usize..8) {
        let claims = vec![Demand::even(); n];
        let g = allocate_even(band, &claims);
        let (min, max) = (*g.iter().min().unwrap(), *g.iter().max().unwrap());
        prop_assert!(max - min <= 1, "even grants differ by {} (> 1 crumb)", max - min);
    }

    /// The fabric allocation is band-bounded on every axis (top-level Σ ≤ band).
    #[test]
    fn fabric_top_level_is_band_bounded(
        members in flat_members_strategy(),
        cap in 0u64..1_000_000,
        sp in prop::sample::select(vec![0.5f64, 0.7, 0.8, 1.0]),
    ) {
        let g = allocate_fabric(PoolCapacity::storage_only(cap), sp, &members).unwrap();
        #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let band = (cap as f64 * sp) as u64;
        let total: u128 = members.iter().map(|m| u128::from(g.get_dim(&m.id, Dim::Storage))).sum();
        prop_assert!(total <= u128::from(band), "Σ {total} > band {band}");
    }

    /// Adding a sibling to a flat even pool never RAISES an existing member's
    /// grant (monotone-on-add — the rebalance cap).
    #[test]
    fn adding_a_sibling_never_raises_others(n in 1usize..7, cap in 1u64..1_000_000) {
        let base: Vec<Quinhao> = (0..n)
            .map(|i| Quinhao::root(format!("m{i}"), DemandVector::storage_only(Demand::even())))
            .collect();
        let g0 = allocate_fabric(PoolCapacity::storage_only(cap), 0.80, &base).unwrap();
        let mut grown = base.clone();
        grown.push(Quinhao::root(format!("m{n}"), DemandVector::storage_only(Demand::even())));
        let g1 = allocate_fabric(PoolCapacity::storage_only(cap), 0.80, &grown).unwrap();
        for i in 0..n {
            let before = g0.get_dim(&format!("m{i}"), Dim::Storage);
            let after = g1.get_dim(&format!("m{i}"), Dim::Storage);
            prop_assert!(after <= before, "m{i} rose on add: {before} → {after}");
        }
    }

    /// Removing a sibling never LOWERS a remaining member's grant
    /// (monotone-on-remove — "removing raises remaining").
    #[test]
    fn removing_a_sibling_never_lowers_others(n in 2usize..8, cap in 1u64..1_000_000) {
        let base: Vec<Quinhao> = (0..n)
            .map(|i| Quinhao::root(format!("m{i}"), DemandVector::storage_only(Demand::even())))
            .collect();
        let g0 = allocate_fabric(PoolCapacity::storage_only(cap), 0.80, &base).unwrap();
        let shrunk: Vec<Quinhao> = base.into_iter().filter(|q| q.id != format!("m{}", n - 1)).collect();
        let g1 = allocate_fabric(PoolCapacity::storage_only(cap), 0.80, &shrunk).unwrap();
        for i in 0..(n - 1) {
            let before = g0.get_dim(&format!("m{i}"), Dim::Storage);
            let after = g1.get_dim(&format!("m{i}"), Dim::Storage);
            prop_assert!(after >= before, "m{i} fell on remove: {before} → {after}");
        }
    }

    /// A two-level fabric is tree-respecting: Σ children grants ≤ parent grant on
    /// every axis (a user never gets more than its group).
    #[test]
    fn fabric_is_tree_respecting(
        n_groups in 1usize..4,
        users_per_group in 1usize..4,
        cap in 1u64..1_000_000,
    ) {
        let mut claimants = Vec::new();
        for gi in 0..n_groups {
            claimants.push(Quinhao::root(format!("g{gi}"), DemandVector::storage_only(Demand::even())));
            for ui in 0..users_per_group {
                claimants.push(Quinhao::child(
                    format!("g{gi}u{ui}"),
                    format!("g{gi}"),
                    DemandVector::storage_only(Demand::even()),
                ));
            }
        }
        let g = allocate_fabric(PoolCapacity::storage_only(cap), 0.80, &claimants).unwrap();
        for gi in 0..n_groups {
            let group_grant = g.get_dim(&format!("g{gi}"), Dim::Storage);
            let children_sum: u128 = (0..users_per_group)
                .map(|ui| u128::from(g.get_dim(&format!("g{gi}u{ui}"), Dim::Storage)))
                .sum();
            prop_assert!(
                children_sum <= u128::from(group_grant),
                "group g{gi}: Σ children {children_sum} > group grant {group_grant}"
            );
        }
    }

    /// The allocation is a PURE function — the same input always yields the same
    /// output (the determinism the dynamic-rebalance guarantee rests on).
    #[test]
    fn allocation_is_deterministic(members in flat_members_strategy(), cap in 0u64..1_000_000) {
        let a = allocate_fabric(PoolCapacity::storage_only(cap), 0.80, &members).unwrap();
        let b = allocate_fabric(PoolCapacity::storage_only(cap), 0.80, &members).unwrap();
        prop_assert_eq!(a, b);
    }
}

// ============================================================================
// allocate_drf — envy-freeness (Ghodsi et al.'s own proven DRF property),
// generated-instance coverage. Found missing by an /algorithmic-prowess-seal
// adversarial verify pass (2026-07-17): the only prior proof was one
// hand-picked numeric example (quinhao::tests::drf_equalizes_dominant_share_
// not_raw_units) -- real, but a single fixed case, not a mechanically-checked
// regression gate a future kernel edit could silently violate.
// ============================================================================

/// A bounded random FINITE demand for one axis, matching `allocate_drf`'s own
/// demand model (only `.demand` is read; `weight`/`min`/`max` are ignored, and
/// `Demand::even()`'s `u64::MAX` convention is deliberately NOT used here —
/// see `allocate_drf`'s doc comment for why DRF needs a real, differentiated
/// quantity, not "unbounded").
fn drf_demand_vector_strategy() -> impl Strategy<Value = DemandVector> {
    (1u64..1000, 1u64..1000).prop_map(|(storage, cpu)| {
        DemandVector::new(
            Demand { weight: 1, min: 0, max: u64::MAX, demand: storage },
            Demand { weight: 1, min: 0, max: u64::MAX, demand: cpu },
            Demand::absent(),
            Demand::absent(),
        )
    })
}

/// Claimant `demand`'s utility for `bundle`, evaluated under the claimant's
/// OWN demand ratios — the number of "tasks" `bundle` represents
/// (`min` over the claimant's contended axes of `bundle[r] / demand[r]`).
/// Envy-freeness compares a claimant's utility for its OWN grant against its
/// utility for another claimant's grant.
fn drf_utility(demand: &DemandVector, bundle: GrantVector) -> f64 {
    let mut min_tasks = f64::INFINITY;
    for dim in [Dim::Storage, Dim::Cpu] {
        let d = demand.get(dim).demand;
        if d == 0 {
            continue;
        }
        #[allow(clippy::cast_precision_loss)]
        let tasks = bundle.get(dim) as f64 / d as f64;
        if tasks < min_tasks {
            min_tasks = tasks;
        }
    }
    if min_tasks.is_finite() { min_tasks } else { 0.0 }
}

proptest! {
    /// DRF is envy-free: no claimant would prefer another claimant's ACTUAL
    /// grant, evaluated under its own demand ratios — mechanically
    /// re-verifying (against the shipped implementation, across generated
    /// instances, not just one hand-picked case) the property Ghodsi et
    /// al.'s own paper proves for DRF. Tolerance of 1.0 "task" absorbs the
    /// legitimate per-claimant integer-floor rounding `allocate_drf` performs
    /// (each grant is independently floored from its exact fractional share)
    /// — a real envy-freeness violation is far larger than one rounding unit.
    #[test]
    fn drf_is_envy_free(
        claims in proptest::collection::vec(drf_demand_vector_strategy(), 2..6),
        storage_cap in 10u64..10_000,
        cpu_cap in 10u64..10_000,
    ) {
        let grants = allocate_drf(PoolCapacity::new(storage_cap, cpu_cap, 0, 0), 1.0, &claims);
        for i in 0..claims.len() {
            let own_utility = drf_utility(&claims[i], grants[i]);
            for j in 0..claims.len() {
                if i == j {
                    continue;
                }
                let envy_utility = drf_utility(&claims[i], grants[j]);
                prop_assert!(
                    own_utility >= envy_utility - 1.0,
                    "claimant {i} envies claimant {j}'s bundle: own_utility={own_utility}, envy_utility={envy_utility}"
                );
            }
        }
    }
}
