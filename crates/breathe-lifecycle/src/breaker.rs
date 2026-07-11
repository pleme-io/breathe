//! The cost/blast-radius circuit-breaker backstop — bulkhead ceiling + a
//! spend-rate Nygard three-state breaker, sitting at credential/client
//! ACQUISITION as a narrow waist neither [`crate::fsm`]'s FSM nor
//! [`crate::drift`]'s reconciler can route around, because there is no code
//! path to a cloud provisioning call that does not first pass both.
//!
//! # Tier
//!
//! **Layer 1 ([`Bulkhead`]/[`Permit`]) — truly-unrepresentable** for "more
//! than `capacity` permits outstanding at once through this API": the sole
//! constructor ([`Bulkhead::try_acquire`]) is atomically capacity-checked,
//! and every provisioning payload requires a `Permit` BY VALUE
//! ([`Permit::authorize`]) — "provision without a permit" has no
//! expressible form (a missing-argument compile error). **Only-mitigated
//! (C5)** for the crash-then-leak case: in-memory accounting, not journaled;
//! a process crash between "acquire" and "persist the acquisition" leaks the
//! slot until the process (and its `Arc` refcount) is gone. Discharge that
//! the eclusa way (a resumable job + typed outcome), never assume atomicity.
//!
//! **Layer 2 ([`SpendRateBreaker`]) — only-mitigated by construction, and
//! CANNOT be otherwise** (a C2/C4 ceiling, not a fixable gap): whether a
//! provisioning attempt is safe to admit is a fact about a shared, external,
//! non-transactional resource (the real cloud spend), so the breaker's trip
//! decision is inherently a RUNTIME judgment over a local approximation, not
//! a compile-time proof. What IS real: the trip signal is computed LOCALLY
//! and for FREE from the bulkhead's own occupancy (never a laggy Cost
//! Explorer/CUR query, which would defeat a backstop's purpose by trailing
//! hours behind); real billing feeds [`SpendRateBreaker::true_up_headroom`]
//! only as a slow, advisory correction — never the primary trip.
//!
//! # Fabrication is unrepresentable (Layer 1)
//!
//! A [`Permit`] cannot be minted except through [`Bulkhead::try_acquire`] —
//! its fields are private, so there is no struct-literal path to one:
//!
//! ```compile_fail
//! use breathe_lifecycle::Permit;
//! use std::sync::atomic::AtomicU32;
//! use std::sync::Arc;
//! let _bad = Permit { counter: Arc::new(AtomicU32::new(0)), _seal: () }; // E0451: fields `counter`/`_seal` are private
//! ```
//!
//! An [`AuthorizedProvision`] cannot be fabricated without consuming a real
//! `Permit` by value — [`Permit::authorize`] is its sole constructor and its
//! own field is private:
//!
//! ```compile_fail
//! use breathe_lifecycle::AuthorizedProvision;
//! let _bad: AuthorizedProvision<u32> = AuthorizedProvision { payload: 7 }; // E0451/E0063: missing/private field `_permit`
//! ```

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

// ============================================================================
// Layer 1 — Bulkhead: a sealed permit pool, hard concurrency ceiling.
// ============================================================================

/// A hard concurrency ceiling on in-flight provisioning attempts, realized as
/// a sealed permit pool (the bulkhead pattern) rather than a bare counter —
/// mirroring `breathe-admission::Admitido<T>`'s "the pool accepts nothing
/// else" shape at the concurrency-limiting layer instead of the
/// resource-pool layer.
#[derive(Debug, Clone)]
pub struct Bulkhead {
    capacity: u32,
    outstanding: Arc<AtomicU32>,
}

impl Bulkhead {
    #[must_use]
    pub fn new(capacity: u32) -> Self {
        Self {
            capacity,
            outstanding: Arc::new(AtomicU32::new(0)),
        }
    }

    #[must_use]
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    #[must_use]
    pub fn outstanding(&self) -> u32 {
        self.outstanding.load(Ordering::Acquire)
    }

    /// The SOLE way to obtain a [`Permit`]. Returns `None` at the ceiling.
    /// Atomically capacity-checked via CAS retry — no lost-update race
    /// between the load and the increment under concurrent callers.
    #[must_use]
    pub fn try_acquire(&self) -> Option<Permit> {
        let mut cur = self.outstanding.load(Ordering::Acquire);
        loop {
            if cur >= self.capacity {
                return None;
            }
            match self.outstanding.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(Permit {
                        counter: Arc::clone(&self.outstanding),
                        _seal: (),
                    });
                }
                Err(observed) => cur = observed,
            }
        }
    }
}

/// A sealed capacity token. Its only constructor is [`Bulkhead::try_acquire`]
/// — the private fields block struct-literal construction from outside this
/// module, so "call the provisioning path without a permit" has no
/// expressible form wherever a signature requires `Permit` by value (see
/// [`Permit::authorize`]). Dropping a `Permit` releases its bulkhead slot —
/// success or failure, the slot is freed exactly once.
pub struct Permit {
    counter: Arc<AtomicU32>,
    _seal: (),
}

impl std::fmt::Debug for Permit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Permit").finish_non_exhaustive()
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Permit {
    /// The sole constructor of [`AuthorizedProvision`] — consumes `self` by
    /// value, so a provisioning payload cannot be authorized without holding
    /// a real, live permit.
    #[must_use]
    pub fn authorize<T>(self, payload: T) -> AuthorizedProvision<T> {
        AuthorizedProvision {
            _permit: self,
            payload,
        }
    }
}

/// A provisioning payload gated by a live [`Permit`]. Holding one over its
/// lifetime IS proof a bulkhead slot was reserved for it; dropping it
/// releases the slot (success or failure — the bulkhead does not care which,
/// only that the attempt concluded).
pub struct AuthorizedProvision<T> {
    _permit: Permit,
    payload: T,
}

impl<T> AuthorizedProvision<T> {
    #[must_use]
    pub fn payload(&self) -> &T {
        &self.payload
    }
    #[must_use]
    pub fn into_inner(self) -> T {
        self.payload
    }
}

// ============================================================================
// Layer 2 — SpendRateBreaker: Nygard 3-state breaker over a $/hr headroom gauge.
// ============================================================================

/// The breaker's current state (Nygard, *Release It!*): `Closed` = admitting
/// normally; `Open` = refusing everything until the cooldown elapses;
/// `HalfOpen` = a single trial admission is being sampled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

/// The outcome of a [`SpendRateBreaker::try_admit`] call.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Admission {
    /// Admitted normally (`Closed` state, headroom covered the marginal rate).
    Admitted,
    /// Admitted as the SINGLE trial sampled this `HalfOpen` window — the
    /// caller MUST report the outcome via [`SpendRateBreaker::record_trial_outcome`].
    AdmittedAsHalfOpenTrial,
    /// Refused — the breaker is `Open` (or just tripped `Open` this call).
    /// `retry_after_unix` is when a trial will next be sampled.
    RefusedOpen { retry_after_unix: u64 },
    /// Refused — `Closed` or `HalfOpen`, but current headroom does not cover
    /// the marginal rate. Not a trip; ordinary backpressure.
    RefusedInsufficientHeadroom { available_dollars_per_hour: f64 },
    /// Refused — `HalfOpen` with a trial already outstanding; only one trial
    /// is sampled per `Open→HalfOpen` window.
    RefusedHalfOpenTrialInFlight,
}

#[derive(Debug)]
struct BreakerCore {
    available_dollars_per_hour: f64,
    state: BreakerState,
    opened_at_unix: Option<u64>,
    /// `Some(amount)` while a `HalfOpen` trial's speculative debit is
    /// outstanding, awaiting `record_trial_outcome`.
    half_open_reserved: Option<f64>,
}

/// A local, occupancy-driven spend-rate circuit breaker. The trip signal is
/// `active_permits × per_instance_hourly_rate` computed by the CALLER from
/// [`Bulkhead::outstanding`] and a known rate table — never a query to a
/// billing API (see the module tier note). One `SpendRateBreaker` instance
/// per cost domain (e.g. one per AWS account/region being provisioned into).
#[derive(Debug)]
pub struct SpendRateBreaker {
    capacity_dollars_per_hour: f64,
    cooldown_seconds: u64,
    core: Mutex<BreakerCore>,
}

/// Construction/usage errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BreakerError {
    Config(String),
}

impl std::fmt::Display for BreakerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(m) => write!(f, "spend-rate breaker config error: {m}"),
        }
    }
}

impl std::error::Error for BreakerError {}

impl SpendRateBreaker {
    /// # Errors
    /// [`BreakerError::Config`] if `capacity_dollars_per_hour` is not finite
    /// and strictly positive.
    pub fn new(
        capacity_dollars_per_hour: f64,
        cooldown_seconds: u64,
    ) -> Result<Self, BreakerError> {
        if !capacity_dollars_per_hour.is_finite() || capacity_dollars_per_hour <= 0.0 {
            return Err(BreakerError::Config(
                "capacity_dollars_per_hour must be finite and > 0".into(),
            ));
        }
        Ok(Self {
            capacity_dollars_per_hour,
            cooldown_seconds,
            core: Mutex::new(BreakerCore {
                available_dollars_per_hour: capacity_dollars_per_hour,
                state: BreakerState::Closed,
                opened_at_unix: None,
                half_open_reserved: None,
            }),
        })
    }

    #[must_use]
    pub fn state(&self) -> BreakerState {
        self.core.lock().expect("breaker core poisoned").state
    }

    #[must_use]
    pub fn available_dollars_per_hour(&self) -> f64 {
        self.core
            .lock()
            .expect("breaker core poisoned")
            .available_dollars_per_hour
    }

    #[must_use]
    pub fn capacity_dollars_per_hour(&self) -> f64 {
        self.capacity_dollars_per_hour
    }

    /// Try to admit a new provisioning request whose ongoing cost is
    /// `marginal_rate_dollars_per_hour`. `now_unix` is caller-supplied (not
    /// `SystemTime::now()`) so tests are deterministic and never race the
    /// wall clock.
    pub fn try_admit(&self, marginal_rate_dollars_per_hour: f64, now_unix: u64) -> Admission {
        let mut core = self.core.lock().expect("breaker core poisoned");
        match core.state {
            BreakerState::Open => {
                let opened_at = core.opened_at_unix.unwrap_or(now_unix);
                if now_unix.saturating_sub(opened_at) < self.cooldown_seconds {
                    return Admission::RefusedOpen {
                        retry_after_unix: opened_at + self.cooldown_seconds,
                    };
                }
                // Cooldown elapsed: transition to HalfOpen and sample one trial.
                core.state = BreakerState::HalfOpen;
                core.half_open_reserved = None;
                Self::admit_half_open(&mut core, marginal_rate_dollars_per_hour)
            }
            BreakerState::HalfOpen => {
                Self::admit_half_open(&mut core, marginal_rate_dollars_per_hour)
            }
            BreakerState::Closed => {
                if marginal_rate_dollars_per_hour > core.available_dollars_per_hour {
                    core.state = BreakerState::Open;
                    core.opened_at_unix = Some(now_unix);
                    return Admission::RefusedOpen {
                        retry_after_unix: now_unix + self.cooldown_seconds,
                    };
                }
                core.available_dollars_per_hour -= marginal_rate_dollars_per_hour;
                Admission::Admitted
            }
        }
    }

    fn admit_half_open(core: &mut BreakerCore, marginal_rate: f64) -> Admission {
        if core.half_open_reserved.is_some() {
            return Admission::RefusedHalfOpenTrialInFlight;
        }
        if marginal_rate > core.available_dollars_per_hour {
            return Admission::RefusedInsufficientHeadroom {
                available_dollars_per_hour: core.available_dollars_per_hour,
            };
        }
        core.available_dollars_per_hour -= marginal_rate;
        core.half_open_reserved = Some(marginal_rate);
        Admission::AdmittedAsHalfOpenTrial
    }

    /// Report the outcome of a `HalfOpen` trial admission (the caller learns
    /// this downstream — did the actual provisioning attempt succeed).
    /// `success = true` closes the breaker, the debit stands (that capacity
    /// is now genuinely spent). `success = false` reopens it AND refunds the
    /// speculative debit (the attempt never actually consumed real budget).
    /// A no-op if no trial is currently in flight.
    pub fn record_trial_outcome(&self, success: bool, now_unix: u64) {
        let mut core = self.core.lock().expect("breaker core poisoned");
        let Some(reserved) = core.half_open_reserved.take() else {
            return;
        };
        if success {
            core.state = BreakerState::Closed;
            core.opened_at_unix = None;
        } else {
            core.available_dollars_per_hour =
                (core.available_dollars_per_hour + reserved).min(self.capacity_dollars_per_hour);
            core.state = BreakerState::Open;
            core.opened_at_unix = Some(now_unix);
        }
    }

    /// Credit back `amount_dollars_per_hour` of headroom — call when a
    /// provisioned resource this breaker gated is released/terminated (its
    /// ongoing spend stops). Always allowed in any state (a release can
    /// never be harmful); clamped to the configured ceiling so repeated
    /// releases can never manufacture headroom above capacity.
    pub fn release(&self, amount_dollars_per_hour: f64) {
        let mut core = self.core.lock().expect("breaker core poisoned");
        core.available_dollars_per_hour = (core.available_dollars_per_hour
            + amount_dollars_per_hour)
            .min(self.capacity_dollars_per_hour);
    }

    /// A slow, ADVISORY correction from a real billing signal (Cost
    /// Explorer/CUR), typically hours-laggy (C2) — never the primary trip
    /// signal (the local occupancy debit/credit above is). This crate ships
    /// no billing-API client; the seam exists so a caller with one can true
    /// the gauge up periodically. Clamped to `[0, capacity]`.
    pub fn true_up_headroom(&self, observed_available_dollars_per_hour: f64) {
        let mut core = self.core.lock().expect("breaker core poisoned");
        core.available_dollars_per_hour =
            observed_available_dollars_per_hour.clamp(0.0, self.capacity_dollars_per_hour);
    }
}

#[cfg(test)]
mod tests;
