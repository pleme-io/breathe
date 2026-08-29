//! **The request dimension end to end, against a mock.**
//!
//! `breathe-control` decides, `plan_request_carve` narrows, `RequestActuator`
//! writes. Every world-fact is injected, so the whole path is provable with no
//! cluster, no clock and no network — the ★★ TYPED-SPEC + INTERPRETER TRIPLET
//! testability contract: if a test needs a real apiserver, the seam is in the
//! wrong place.
//!
//! The load-bearing assertions here are the NEGATIVE ones. It is easy to show a
//! carve happens; what this dimension has to prove is that a carve does **not**
//! happen when it must not — under a shadow gate, on a class transition, past
//! allocatable, or downward.

use breathe_control::{BandConfig, Decision, Directionality, Reclaim, RequestLaw, decide_with};
use breathe_control::{clamp_to_directionality, clamp_to_reclaim};
use breathe_invariant::isolation::{IsolationPosture, PlacementIsolation, QosClass, WorkloadClass};
use breathe_provider::gate::{authored_write_gate, legacy_two_state_gate};
use breathe_provider::mock::MockRequestActuator;
use breathe_provider::request::{
    ClassPreserved, ContainerResources, Durability, MemoryResizePolicy, PodResources,
    RequestActuator, RequestCarveInput, RequestCarvePlan, RequestResource, plan_request_carve,
};
use breathe_provider::{LimitLayout, Target};

const MI: u64 = 1 << 20;
const GI: u64 = 1 << 30;

fn target() -> Target {
    Target {
        namespace: "isolated-build".into(),
        name: "sui-cache-pg".into(),
        kind: "StatefulSet".into(),
        api_version: "apps/v1".into(),
        container: Some("db".into()),
        pod_selector: None,
    }
}

/// The `sui-cache-pg` shape: a tiny request under a limit that never binds.
fn observed(request: u64, limit: u64) -> PodResources {
    PodResources::new(vec![ContainerResources {
        name: "db".into(),
        cpu_request: Some(100),
        cpu_limit: Some(500),
        memory_request: Some(request),
        memory_limit: Some(limit),
    }])
}

fn open_posture() -> IsolationPosture {
    IsolationPosture::try_seal(
        WorkloadClass::Standard,
        QosClass::Burstable,
        0,
        0,
        PlacementIsolation::CoLocate,
        false,
    )
    .expect("Standard/Burstable with no floor is a legal posture")
}

/// The band's decision for a demand reading — the real `breathe-control` path,
/// with the reclaim + directionality gates the Request dimension composes.
fn decide(demand: u64, current_request: u64, cfg: &BandConfig) -> Decision {
    clamp_to_directionality(
        clamp_to_reclaim(
            // peak == demand: the reservation tracks a PromQL quantile, not
            // `update_peak`'s decaying max, so there is no peak history.
            decide_with(&RequestLaw::default(), demand, demand, current_request, cfg),
            Reclaim::ObserveOnly,
        ),
        Directionality::GrowOnly,
    )
}

fn cfg() -> BandConfig {
    BandConfig {
        floor_bytes: 64 * MI,
        ceiling_bytes: 8 * GI,
        ..BandConfig::default()
    }
}

fn input<'a>(
    t: &'a Target,
    obs: &'a PodResources,
    posture: &'a IsolationPosture,
    decision: Decision,
) -> RequestCarveInput<'a> {
    RequestCarveInput {
        target: t,
        container: "db",
        resource: RequestResource::Memory,
        observed: obs,
        decision,
        posture,
        headroom: breathe_provider::request::AllocatableHeadroom {
            per_node: 8 * GI,
            observed_at_epoch: 1,
        },
        replicas: 3,
        durability: Durability::Committed,
        qos_target: QosClass::Burstable,
        server_minor: 33,
        memory_resize_policy: MemoryResizePolicy::Unset,
        field_manager: "breathe-request",
    }
}

/// **THE RECEIPT.** `sui-cache-pg` took 34 `OOMKills` at a 202.8Mi high-water
/// under a 1Gi limit with cgroup `failcnt = 0` — the limit was never binding, so
/// the kill came from `oom_score_adj`, which is derived from the request.
///
/// End to end: the band decides, the planner narrows, the actuator writes
/// `requests.memory` — and the recorded patch proves it wrote the RESERVATION,
/// not a limit.
#[tokio::test]
async fn the_sui_cache_pg_shape_is_carved_end_to_end() {
    let t = target();
    let obs = observed(128 * MI, GI);
    let posture = open_posture();
    let high_water = 202_800 * 1024;

    let decision = decide(high_water, 128 * MI, &cfg());
    let RequestCarvePlan::InPlace(carve) = plan_request_carve(&input(&t, &obs, &posture, decision))
    else {
        panic!("the band must plan a reservation raise")
    };

    let actuator = MockRequestActuator::new();
    let gate = authored_write_gate("drzzln: request-band end-to-end");
    let witness = gate.witness().expect("an authored write resolves Live");
    actuator
        .resize_in_place(witness, carve.preserved(), carve.patch())
        .await
        .expect("the carve applies");

    let carves = actuator.carves();
    assert_eq!(carves.len(), 1, "exactly one write");
    let (proof, patch) = &carves[0];

    // It wrote the REQUEST — the field the kernel ranks on — via the request
    // layout. A MemoryBand's patch would carry `PodResize` and move a limit.
    assert!(matches!(patch.layout, LimitLayout::PodRequestResize { .. }));
    assert_eq!(patch.resource, "memory");
    assert!(
        patch.value > 128 * MI,
        "the reservation RISES: {} > {}",
        patch.value,
        128 * MI
    );
    assert!(patch.value < GI, "and stays under the never-binding limit");
    assert_eq!(patch.target.name, "sui-cache-pg");
    assert_eq!(patch.field_manager, "breathe-request");

    // The write carried a proof that the class did not move.
    assert_eq!(proof.class(), QosClass::Burstable);
    assert_eq!(proof.to(), patch.value);
    assert_eq!(proof.container(), "db");

    // I5 — and the plan is honest that this value does not survive a rollout.
    assert!(
        carve.durability_gap(),
        "a Committed band with no durable writer must report the gap"
    );
}

/// **A SHADOW TICK WRITES NOTHING.** The band computes a real decision and a
/// real plan; the gate refuses to yield a witness; the actuator records zero.
///
/// This is the assertion that makes the whole dimension safe to land: the
/// default posture is born shadowed, and "born shadowed" has to mean the I/O
/// door was never reached, not that it was reached and declined.
#[tokio::test]
async fn a_shadow_gated_tick_plans_a_carve_and_writes_nothing() {
    let t = target();
    let obs = observed(128 * MI, GI);
    let posture = open_posture();

    let decision = decide(700 * MI, 128 * MI, &cfg());
    let plan = plan_request_carve(&input(&t, &obs, &posture, decision));
    assert!(plan.writes(), "the plan itself is a real, actionable carve");

    let actuator = MockRequestActuator::new();
    let gate = legacy_two_state_gate(/* dry_run */ true, /* frozen */ false);
    assert!(gate.is_shadow(), "the gate resolves shadow");
    assert!(gate.witness().is_none(), "a shadow gate yields NO witness");
    // …and with no witness there is no call to make: `resize_in_place` cannot be
    // invoked without one. The absence of a witness is the absence of a write.
    assert_eq!(
        actuator.write_count(),
        0,
        "a shadow tick must record zero writes"
    );
}

/// A class transition never reaches the actuator — there is no call to make.
#[tokio::test]
async fn a_class_transition_never_reaches_the_in_place_door() {
    let t = target();
    // BestEffort: no requests, no limits anywhere.
    let obs = PodResources::new(vec![ContainerResources {
        name: "db".into(),
        ..Default::default()
    }]);
    let posture = open_posture();

    let plan = plan_request_carve(&input(
        &t,
        &obs,
        &posture,
        Decision::Grow {
            from: 0,
            to: 256 * MI,
        },
    ));
    let RequestCarvePlan::Transition(proposal) = &plan else {
        panic!("expected a Transition, got {plan:?}")
    };
    assert_eq!(proposal.from, QosClass::BestEffort);
    assert_eq!(proposal.to, QosClass::Burstable);
    assert!(!plan.writes());

    // The proposal is a `ClassTransitionProposal`. `RequestActuator` takes an
    // `SsaPatch`, and no conversion exists in either direction — so there is no
    // expression that routes this proposal through the in-place door. The mock
    // stays empty not because it refused, but because the call is unwritable.
    let actuator = MockRequestActuator::new();
    assert_eq!(actuator.write_count(), 0);
}

/// A carve past node allocatable is refused before any I/O — the pod would be
/// permanently unschedulable, and on the durable path it would land silently and
/// kill the next deploy.
#[tokio::test]
async fn a_carve_past_allocatable_never_reaches_the_actuator() {
    let t = target();
    let obs = observed(128 * MI, 8 * GI);
    let posture = open_posture();

    let mut i = input(
        &t,
        &obs,
        &posture,
        Decision::Grow {
            from: 128 * MI,
            to: 4 * GI,
        },
    );
    i.headroom = breathe_provider::request::AllocatableHeadroom {
        per_node: GI,
        observed_at_epoch: 1,
    };
    let plan = plan_request_carve(&i);
    assert!(!plan.writes(), "expected a refusal, got {plan:?}");

    let actuator = MockRequestActuator::new();
    assert_eq!(actuator.write_count(), 0);
}

/// A transport failure is a typed error, never a silent success. The band must
/// be able to tell "carved" from "tried to carve".
#[tokio::test]
async fn an_actuation_failure_is_typed_and_never_a_silent_ok() {
    let t = target();
    let obs = observed(128 * MI, GI);
    let posture = open_posture();
    let decision = decide(600 * MI, 128 * MI, &cfg());
    let RequestCarvePlan::InPlace(carve) = plan_request_carve(&input(&t, &obs, &posture, decision))
    else {
        panic!("expected InPlace")
    };

    let actuator = MockRequestActuator::failing(breathe_provider::ProviderError::ApiTransient(
        "apiserver said no".into(),
    ));
    let gate = authored_write_gate("drzzln: failure path");
    let witness = gate.witness().expect("Live");
    let err = actuator
        .resize_in_place(witness, carve.preserved(), carve.patch())
        .await
        .expect_err("a failed carve must surface as an error");
    assert!(matches!(
        err,
        breathe_provider::ProviderError::ApiTransient(_)
    ));
    assert_eq!(actuator.write_count(), 0, "a failed carve records no write");
}

/// The witness is not decorative: it names the exact change it authorizes, so a
/// mismatched pair is detectable. (The `KubeCluster` actuator enforces this; the
/// property asserted here is that the witness CARRIES enough to enforce it.)
#[test]
fn the_witness_names_the_exact_change_it_authorizes() {
    let obs = observed(128 * MI, GI);
    let w = ClassPreserved::check(&obs, "db", RequestResource::Memory, 512 * MI)
        .expect("class preserved");
    assert_eq!(w.container(), "db");
    assert_eq!(w.resource(), RequestResource::Memory);
    assert_eq!(w.to(), 512 * MI);
    assert_eq!(w.from(), Some(128 * MI));
}
