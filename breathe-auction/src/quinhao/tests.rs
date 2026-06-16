//! Unit tests for the `quinhão` hierarchical-vector fair-share allocator.

use super::{
    allocate_even, allocate_fabric, Demand, DemandVector, Dim, FabricError, PoolCapacity, Quinhao,
};

// ── allocate_even — the single-level kernel ─────────────────────────────────

#[test]
fn even_split_of_a_band_among_equal_claimants() {
    // 4 even claimants split 800 (the 80% band) → 200 each (the operator's ask:
    // 4 users → ~20% of the whole each, summing to the band).
    let claims = vec![Demand::even(); 4];
    let g = allocate_even(800, &claims);
    assert_eq!(g, vec![200, 200, 200, 200]);
    assert_eq!(g.iter().sum::<u64>(), 800, "Σ grants == band (exact)");
}

#[test]
fn even_split_is_exact_with_an_indivisible_band() {
    // 803 / 3 is not integral — the crumb (2) is handed out deterministically so
    // Σ is still exact and the band is never exceeded.
    let g = allocate_even(803, &vec![Demand::even(); 3]);
    assert_eq!(g.iter().sum::<u64>(), 803);
    assert!(g.iter().all(|&x| x == 267 || x == 268));
}

#[test]
fn weights_split_proportionally() {
    // weights 1:2:1 over a band of 800 → 200:400:200.
    let claims = vec![
        Demand { weight: 1, ..Demand::even() },
        Demand { weight: 2, ..Demand::even() },
        Demand { weight: 1, ..Demand::even() },
    ];
    let g = allocate_even(800, &claims);
    assert_eq!(g, vec![200, 400, 200]);
}

#[test]
fn a_max_clamp_redistributes_surplus_to_siblings() {
    // 3 even claimants over 900 would be 300 each, but #0 caps at 100 → its 200
    // surplus redistributes to #1 and #2 (400 each). Σ still 900.
    let claims = vec![
        Demand { max: 100, ..Demand::even() },
        Demand::even(),
        Demand::even(),
    ];
    let g = allocate_even(900, &claims);
    assert_eq!(g[0], 100, "capped claimant gets exactly its cap");
    assert_eq!(g[1] + g[2], 800, "the 800 surplus goes to the hungry siblings");
    assert_eq!(g[1], g[2], "the two uncapped siblings split it evenly");
    assert_eq!(g.iter().sum::<u64>(), 900);
}

#[test]
fn a_low_demand_claimant_frees_surplus() {
    // an idle claimant demanding only 50 frees the rest to its siblings — the
    // "shifts accordingly" property at the kernel.
    let claims = vec![
        Demand { demand: 50, ..Demand::even() },
        Demand::even(),
        Demand::even(),
    ];
    let g = allocate_even(900, &claims);
    assert_eq!(g[0], 50);
    assert_eq!(g[1] + g[2], 850);
}

#[test]
fn floors_are_always_granted_when_they_fit() {
    // every claimant owed a 100 floor; the band 800 covers Σmin=300 with room.
    let claims = vec![
        Demand { min: 100, ..Demand::even() },
        Demand { min: 100, ..Demand::even() },
        Demand { min: 100, ..Demand::even() },
    ];
    let g = allocate_even(800, &claims);
    assert!(g.iter().all(|&x| x >= 100), "every floor honoured");
    assert_eq!(g.iter().sum::<u64>(), 800);
}

#[test]
fn floors_scale_down_proportionally_when_they_do_not_fit() {
    // Σmin = 1200 > band 600 → floors scale to half. The band is the hard wall
    // (never-over-commit), never breached.
    let claims = vec![
        Demand { min: 600, weight: 0, max: 600, demand: 600 },
        Demand { min: 600, weight: 0, max: 600, demand: 600 },
    ];
    let g = allocate_even(600, &claims);
    assert_eq!(g, vec![300, 300]);
    assert!(g.iter().sum::<u64>() <= 600);
}

#[test]
fn weight_zero_claimant_gets_only_its_floor() {
    // an inactive (idle) claimant takes no weighted share — only its reserved min.
    let claims = vec![
        Demand { weight: 0, min: 100, max: u64::MAX, demand: u64::MAX },
        Demand::even(),
    ];
    let g = allocate_even(900, &claims);
    assert_eq!(g[0], 100, "idle claimant gets only its floor");
    assert_eq!(g[1], 800, "the active claimant takes the rest");
}

#[test]
fn empty_and_zero_band_are_handled() {
    assert_eq!(allocate_even(0, &[]), Vec::<u64>::new());
    assert_eq!(allocate_even(0, &vec![Demand::even(); 3]), vec![0, 0, 0]);
    assert_eq!(allocate_even(1000, &[]), Vec::<u64>::new());
}

// ── allocate_fabric — the hierarchical, vector-valued recursion ─────────────

fn storage(weight: u32, min: u64, max: u64, demand: u64) -> DemandVector {
    DemandVector::storage_only(Demand { weight, min, max, demand })
}

#[test]
fn flat_pool_of_even_members_splits_the_80_band_evenly() {
    // The operator's literal ask, vector-valued: 4 members, no groups, even.
    // pool 1000, setpoint 0.80 → band 800 → 200 each on storage.
    let members: Vec<Quinhao> = (0..4)
        .map(|i| Quinhao::root(format!("m{i}"), DemandVector::storage_only(Demand::even())))
        .collect();
    let g = allocate_fabric(PoolCapacity::storage_only(1000), 0.80, &members).unwrap();
    for i in 0..4 {
        assert_eq!(g.get_dim(&format!("m{i}"), Dim::Storage), 200);
    }
    let total: u64 = g.iter().map(|(_, gv)| gv.get(Dim::Storage)).sum();
    assert_eq!(total, 800, "Σ member grants == the 80% band");
}

#[test]
fn groups_split_the_band_then_users_split_their_group() {
    // pool 1000, setpoint 0.80 → band 800. Two even groups → 400 each. Group A
    // has 2 even users → 200 each; group B has 1 user → 400. The hierarchy.
    let claimants = vec![
        Quinhao::root("groupA", DemandVector::storage_only(Demand::even())),
        Quinhao::root("groupB", DemandVector::storage_only(Demand::even())),
        Quinhao::child("a1", "groupA", DemandVector::storage_only(Demand::even())),
        Quinhao::child("a2", "groupA", DemandVector::storage_only(Demand::even())),
        Quinhao::child("b1", "groupB", DemandVector::storage_only(Demand::even())),
    ];
    let g = allocate_fabric(PoolCapacity::storage_only(1000), 0.80, &claimants).unwrap();
    assert_eq!(g.get_dim("groupA", Dim::Storage), 400);
    assert_eq!(g.get_dim("groupB", Dim::Storage), 400);
    assert_eq!(g.get_dim("a1", Dim::Storage), 200);
    assert_eq!(g.get_dim("a2", Dim::Storage), 200);
    assert_eq!(g.get_dim("b1", Dim::Storage), 400, "b1 gets all of group B's grant");
    // tree-respecting: Σ children ≤ parent grant.
    assert_eq!(g.get_dim("a1", Dim::Storage) + g.get_dim("a2", Dim::Storage), g.get_dim("groupA", Dim::Storage));
}

#[test]
fn rebalance_on_join_never_raises_existing_siblings() {
    // 3 even members → 800/3 ≈ 266..267. Add a 4th → 200 each. No existing
    // member rose (the monotone-on-add property — the "shifts accordingly" cap).
    let base: Vec<Quinhao> = (0..3)
        .map(|i| Quinhao::root(format!("m{i}"), DemandVector::storage_only(Demand::even())))
        .collect();
    let g3 = allocate_fabric(PoolCapacity::storage_only(1000), 0.80, &base).unwrap();

    let mut grown = base.clone();
    grown.push(Quinhao::root("m3", DemandVector::storage_only(Demand::even())));
    let g4 = allocate_fabric(PoolCapacity::storage_only(1000), 0.80, &grown).unwrap();

    for i in 0..3 {
        let before = g3.get_dim(&format!("m{i}"), Dim::Storage);
        let after = g4.get_dim(&format!("m{i}"), Dim::Storage);
        assert!(after <= before, "member m{i} rose on a join ({before} → {after})");
    }
    assert_eq!(g4.get_dim("m3", Dim::Storage), 200);
}

#[test]
fn rebalance_on_leave_never_lowers_remaining_siblings() {
    // 4 even members → 200 each. Remove one → the remaining 3 split 800 → ≥200.
    // No remaining member fell (monotone-on-remove — "removing raises remaining").
    let base: Vec<Quinhao> = (0..4)
        .map(|i| Quinhao::root(format!("m{i}"), DemandVector::storage_only(Demand::even())))
        .collect();
    let g4 = allocate_fabric(PoolCapacity::storage_only(1000), 0.80, &base).unwrap();

    let shrunk: Vec<Quinhao> = base.into_iter().filter(|q| q.id != "m3").collect();
    let g3 = allocate_fabric(PoolCapacity::storage_only(1000), 0.80, &shrunk).unwrap();

    for i in 0..3 {
        let before = g4.get_dim(&format!("m{i}"), Dim::Storage);
        let after = g3.get_dim(&format!("m{i}"), Dim::Storage);
        assert!(after >= before, "member m{i} fell on a leave ({before} → {after})");
    }
}

#[test]
fn an_idle_group_frees_its_band_to_active_groups() {
    // group B goes idle (weight 0, demands nothing) → its band frees to group A.
    let claimants = vec![
        Quinhao::root("groupA", DemandVector::storage_only(Demand::even())),
        Quinhao::root("groupB", DemandVector::storage_only(Demand { weight: 0, min: 0, max: 0, demand: 0 })),
        Quinhao::child("a1", "groupA", DemandVector::storage_only(Demand::even())),
    ];
    let g = allocate_fabric(PoolCapacity::storage_only(1000), 0.80, &claimants).unwrap();
    assert_eq!(g.get_dim("groupB", Dim::Storage), 0, "an idle group claims nothing");
    assert_eq!(g.get_dim("groupA", Dim::Storage), 800, "the active group takes the whole band");
    assert_eq!(g.get_dim("a1", Dim::Storage), 800);
}

#[test]
fn dimensions_are_independent_a_storage_split_does_not_touch_cpu() {
    // a member active on storage but absent on cpu gets a storage grant and a
    // zero cpu grant — the axes never couple.
    let claimants = vec![
        Quinhao::root("m0", storage(1, 0, u64::MAX, u64::MAX)),
        Quinhao::root("m1", DemandVector::new(
            Demand::even(),                              // storage: even
            Demand { weight: 1, min: 0, max: u64::MAX, demand: u64::MAX }, // cpu: even
            Demand::absent(),
            Demand::absent(),
        )),
    ];
    let g = allocate_fabric(PoolCapacity::new(1000, 2000, 0, 0), 0.80, &claimants).unwrap();
    // storage band 800 split evenly → 400 each.
    assert_eq!(g.get_dim("m0", Dim::Storage), 400);
    assert_eq!(g.get_dim("m1", Dim::Storage), 400);
    // cpu band 1600: m0 absent (0), m1 takes all 1600.
    assert_eq!(g.get_dim("m0", Dim::Cpu), 0, "m0 is absent on cpu");
    assert_eq!(g.get_dim("m1", Dim::Cpu), 1600, "m1 takes the whole cpu band");
}

#[test]
fn setpoint_controls_the_band_fraction() {
    // setpoint 0.50 of a 1000 pool → a 500 band; two even members → 250 each.
    let members = vec![
        Quinhao::root("m0", DemandVector::storage_only(Demand::even())),
        Quinhao::root("m1", DemandVector::storage_only(Demand::even())),
    ];
    let g = allocate_fabric(PoolCapacity::storage_only(1000), 0.50, &members).unwrap();
    assert_eq!(g.get_dim("m0", Dim::Storage), 250);
    assert_eq!(g.get_dim("m1", Dim::Storage), 250);
}

// ── parse-gate refusals (not a forest) ──────────────────────────────────────

#[test]
fn duplicate_id_is_refused() {
    let claimants = vec![
        Quinhao::root("dup", DemandVector::storage_only(Demand::even())),
        Quinhao::root("dup", DemandVector::storage_only(Demand::even())),
    ];
    assert!(matches!(
        allocate_fabric(PoolCapacity::storage_only(1000), 0.80, &claimants),
        Err(FabricError::DuplicateId { .. })
    ));
}

#[test]
fn unknown_parent_is_refused() {
    let claimants = vec![Quinhao::child("u", "ghost-group", DemandVector::storage_only(Demand::even()))];
    assert!(matches!(
        allocate_fabric(PoolCapacity::storage_only(1000), 0.80, &claimants),
        Err(FabricError::UnknownParent { .. })
    ));
}

#[test]
fn a_parent_cycle_is_refused() {
    // a → b → a : not a forest.
    let claimants = vec![
        Quinhao::child("a", "b", DemandVector::storage_only(Demand::even())),
        Quinhao::child("b", "a", DemandVector::storage_only(Demand::even())),
    ];
    assert!(matches!(
        allocate_fabric(PoolCapacity::storage_only(1000), 0.80, &claimants),
        Err(FabricError::Cycle { .. })
    ));
}

#[test]
fn dim_round_trips_through_its_wire_string() {
    for d in Dim::ALL {
        assert_eq!(Dim::from_str(d.as_str()), Some(d));
    }
    assert_eq!(Dim::from_str("nonsense"), None);
}
