use crate::breaker::{Admission, BreakerState, Bulkhead, SpendRateBreaker};

// ── Layer 1: Bulkhead ───────────────────────────────────────────────────────

#[test]
fn bulkhead_admits_up_to_capacity_then_refuses() {
    let bh = Bulkhead::new(2);
    let p1 = bh.try_acquire().expect("1st permit");
    assert_eq!(bh.outstanding(), 1);
    let p2 = bh.try_acquire().expect("2nd permit");
    assert_eq!(bh.outstanding(), 2);
    assert!(
        bh.try_acquire().is_none(),
        "3rd acquire must be refused at capacity"
    );
    drop(p1);
    drop(p2);
}

#[test]
fn dropping_a_permit_releases_the_slot() {
    let bh = Bulkhead::new(1);
    let p = bh.try_acquire().expect("permit");
    assert!(bh.try_acquire().is_none(), "capacity 1 is already held");
    drop(p);
    assert_eq!(bh.outstanding(), 0);
    assert!(
        bh.try_acquire().is_some(),
        "the slot must be free again after Drop"
    );
}

#[test]
fn authorized_provision_carries_its_payload_and_is_only_constructible_via_a_permit() {
    let bh = Bulkhead::new(1);
    let permit = bh.try_acquire().unwrap();
    let authorized = permit.authorize(42u32);
    assert_eq!(authorized.payload(), &42);
    assert_eq!(authorized.into_inner(), 42);
    // The permit was consumed by `authorize` and is dropped with the
    // AuthorizedProvision above — the slot is free again.
    assert_eq!(bh.outstanding(), 0);
}

#[test]
fn concurrent_acquire_never_oversubscribes_the_ceiling() {
    // A single-threaded stress of the CAS retry loop: acquire/drop churn must
    // never let `outstanding()` exceed `capacity()`.
    let bh = Bulkhead::new(3);
    let mut held = Vec::new();
    for i in 0..100 {
        if let Some(p) = bh.try_acquire() {
            held.push(p);
        }
        assert!(
            bh.outstanding() <= bh.capacity(),
            "outstanding exceeded capacity at iteration {i}"
        );
        if i % 3 == 0 {
            held.pop();
        }
    }
}

// ── Layer 2: SpendRateBreaker ───────────────────────────────────────────────

#[test]
fn new_rejects_non_positive_or_non_finite_capacity() {
    assert!(SpendRateBreaker::new(0.0, 60).is_err());
    assert!(SpendRateBreaker::new(-5.0, 60).is_err());
    assert!(SpendRateBreaker::new(f64::NAN, 60).is_err());
    assert!(SpendRateBreaker::new(f64::INFINITY, 60).is_err());
    assert!(SpendRateBreaker::new(10.0, 60).is_ok());
}

#[test]
fn admits_while_headroom_covers_the_marginal_rate() {
    let b = SpendRateBreaker::new(10.0, 100).unwrap();
    assert_eq!(b.try_admit(4.0, 0), Admission::Admitted);
    assert!((b.available_dollars_per_hour() - 6.0).abs() < f64::EPSILON);
    assert_eq!(b.state(), BreakerState::Closed);
}

#[test]
fn trips_open_when_the_marginal_rate_exceeds_headroom() {
    let b = SpendRateBreaker::new(10.0, 100).unwrap();
    let admission = b.try_admit(11.0, 0);
    assert_eq!(
        admission,
        Admission::RefusedOpen {
            retry_after_unix: 100
        }
    );
    assert_eq!(b.state(), BreakerState::Open);
    // The refused request was never debited — headroom is untouched.
    assert!((b.available_dollars_per_hour() - 10.0).abs() < f64::EPSILON);
}

#[test]
fn open_refuses_every_request_until_the_cooldown_elapses() {
    let b = SpendRateBreaker::new(10.0, 100).unwrap();
    let _ = b.try_admit(11.0, 0); // trips Open at t=0
    assert_eq!(
        b.try_admit(1.0, 50),
        Admission::RefusedOpen {
            retry_after_unix: 100
        }
    );
    assert_eq!(
        b.state(),
        BreakerState::Open,
        "must stay Open before cooldown elapses"
    );
}

#[test]
fn half_open_samples_exactly_one_trial_after_cooldown() {
    let b = SpendRateBreaker::new(10.0, 100).unwrap();
    let _ = b.try_admit(11.0, 0); // trips Open at t=0

    let trial = b.try_admit(1.0, 100); // cooldown exactly elapsed
    assert_eq!(trial, Admission::AdmittedAsHalfOpenTrial);
    assert_eq!(b.state(), BreakerState::HalfOpen);

    // A second concurrent attempt during the same HalfOpen window is refused
    // — only ONE trial is sampled per Open→HalfOpen cycle.
    let second = b.try_admit(1.0, 100);
    assert_eq!(second, Admission::RefusedHalfOpenTrialInFlight);
}

#[test]
fn a_successful_trial_closes_the_breaker_and_keeps_the_debit() {
    let b = SpendRateBreaker::new(10.0, 100).unwrap();
    let _ = b.try_admit(11.0, 0);
    let _ = b.try_admit(3.0, 100); // AdmittedAsHalfOpenTrial, debits 3.0
    b.record_trial_outcome(true, 100);
    assert_eq!(b.state(), BreakerState::Closed);
    assert!(
        (b.available_dollars_per_hour() - 7.0).abs() < f64::EPSILON,
        "the successful trial's debit stands"
    );
}

#[test]
fn a_failed_trial_reopens_and_refunds_the_speculative_debit() {
    let b = SpendRateBreaker::new(10.0, 100).unwrap();
    let _ = b.try_admit(11.0, 0);
    let _ = b.try_admit(3.0, 100); // debits 3.0 as a trial
    assert!((b.available_dollars_per_hour() - 7.0).abs() < f64::EPSILON);

    b.record_trial_outcome(false, 100);
    assert_eq!(
        b.state(),
        BreakerState::Open,
        "a failed trial must reopen the breaker"
    );
    assert!(
        (b.available_dollars_per_hour() - 10.0).abs() < f64::EPSILON,
        "the failed trial's debit must be refunded — it never actually consumed real budget"
    );
    // A fresh cooldown starts from the failed trial's timestamp.
    assert_eq!(
        b.try_admit(1.0, 150),
        Admission::RefusedOpen {
            retry_after_unix: 200
        }
    );
}

#[test]
fn record_trial_outcome_is_a_no_op_when_no_trial_is_in_flight() {
    let b = SpendRateBreaker::new(10.0, 100).unwrap();
    b.record_trial_outcome(true, 0); // no trial ever admitted — must not panic or corrupt state
    assert_eq!(b.state(), BreakerState::Closed);
    assert!((b.available_dollars_per_hour() - 10.0).abs() < f64::EPSILON);
}

#[test]
fn release_credits_back_headroom_clamped_to_capacity() {
    let b = SpendRateBreaker::new(10.0, 100).unwrap();
    let _ = b.try_admit(6.0, 0);
    b.release(100.0); // way more than was ever debited
    assert!(
        (b.available_dollars_per_hour() - 10.0).abs() < f64::EPSILON,
        "release must never manufacture headroom above capacity"
    );
}

#[test]
fn true_up_headroom_clamps_into_the_valid_range() {
    let b = SpendRateBreaker::new(10.0, 100).unwrap();
    b.true_up_headroom(-5.0);
    assert!((b.available_dollars_per_hour() - 0.0).abs() < f64::EPSILON);
    b.true_up_headroom(999.0);
    assert!((b.available_dollars_per_hour() - 10.0).abs() < f64::EPSILON);
}

#[test]
fn an_over_budget_request_from_closed_trips_open() {
    let b = SpendRateBreaker::new(10.0, 100).unwrap();
    let _ = b.try_admit(9.0, 0); // available = 1.0, still Closed
    assert_eq!(b.state(), BreakerState::Closed);
    let refusal = b.try_admit(9.0, 0);
    assert_eq!(
        b.state(),
        BreakerState::Open,
        "an over-budget request from Closed DOES trip Open"
    );
    assert_eq!(
        refusal,
        Admission::RefusedOpen {
            retry_after_unix: 100
        }
    );
}

#[test]
fn insufficient_headroom_during_half_open_is_backpressure_not_a_second_trip() {
    // Distinct from the Closed→Open trip above: once already HalfOpen, an
    // under-funded trial attempt is ordinary backpressure — it does NOT
    // consume the window's single trial slot and does NOT change state, so a
    // later, cheaper attempt in the same window can still be sampled.
    let b = SpendRateBreaker::new(10.0, 100).unwrap();
    let _ = b.try_admit(11.0, 0); // trips Open at t=0
    let refusal = b.try_admit(50.0, 100); // way over capacity, sampled at HalfOpen
    assert_eq!(
        refusal,
        Admission::RefusedInsufficientHeadroom {
            available_dollars_per_hour: 10.0
        }
    );
    assert_eq!(
        b.state(),
        BreakerState::HalfOpen,
        "an under-funded trial must not silently re-trip or advance"
    );

    // The trial slot was NOT consumed — a cheaper attempt in the same window
    // still gets sampled.
    let ok = b.try_admit(2.0, 100);
    assert_eq!(ok, Admission::AdmittedAsHalfOpenTrial);
}
