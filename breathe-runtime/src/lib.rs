//! `breathe-runtime` — the controller-runtime glue shared by breathe's two
//! reconcile binaries: the **brain** (`breathe-controller`, k8s dimensions via
//! `KubeCluster`) and the **hands** (`breathe-host-agent`, host dimensions via
//! `HostCluster`). The decision math lives in `breathe-control`; the I/O lives in
//! the `Cluster` impls; this crate owns only the two things both processes must
//! do *identically* — map a `TickReceipt` to a `BandStatus`, and patch it onto
//! the band CR. Sharing it means the brain and the hands can never drift in how a
//! decision is reported (a `ShadowWouldApply` means the same thing on both).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use breathe_control::{BandConfig, Decision, Observation};
use breathe_control::replica::{ReplicaDecision, ReplicaTickPlan};
use breathe_core::{TickOutcome, TickReceipt};
use breathe_crd::{Band, BandStatus, Condition, TrendSample};
use breathe_provider::{ClassCooldowns, DisruptionPolicy, EdgeTier, ProviderError};

// The durable-store seam (M0 of the Urdume-microservice refactor;
// docs/BREATHE-MICROSERVICE.md). `CumulativeCounters` is the single counter
// fold; `DecisionEntry` is the per-tick classified decision. Re-exported so the
// controller + agent can name them via `breathe_runtime::…` without a direct
// breathe-store dependency.
pub use breathe_store::{CounterClass, CumulativeCounters, DecisionEntry};
use metrics::{counter, gauge, Label};
use kube::{
    api::{Api, Patch, PatchParams},
    Client,
};
use serde_json::json;

/// Unix epoch seconds (monotonic enough for cooldown bookkeeping; 0 on error).
#[must_use]
pub fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

/// The current time as an RFC3339 string (condition/sample/overview timestamps).
#[must_use]
pub fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// True if an RFC3339 timestamp is in the FUTURE (a forceLimit pin is still active).
/// An unparseable expiry is treated as no-expiry (active) — a malformed string must
/// not silently disable a break-glass pin.
#[must_use]
pub fn rfc3339_in_future(s: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(s).map_or(true, |t| t > chrono::Utc::now())
}

/// Observed utilization (`used / capacity`) as a ratio, or `None` when there is
/// no denominator (capacity == 0 ⇒ no limit set).
#[must_use]
pub fn util_of(obs: &Observation) -> Option<f64> {
    (obs.capacity() > 0).then(|| obs.used as f64 / obs.capacity() as f64)
}

/// The DisruptionPolicy as its camelCase wire string (matches the CRD enum).
fn policy_str(p: DisruptionPolicy) -> String {
    match p {
        DisruptionPolicy::RestartFreeOnly => "restartFreeOnly",
        DisruptionPolicy::AllowConditional => "allowConditional",
        DisruptionPolicy::AllowRestart => "allowRestart",
    }
    .into()
}

/// Where the tick sat on the golden/ceiling line, as a short status string.
fn edge_tier_str(t: EdgeTier) -> String {
    match t {
        EdgeTier::GoldenPreserving => "golden".into(),
        EdgeTier::CeilingCrossing(c) => format!("crossing:{c:?}"),
    }
}

/// The k8s Event severity for a tick. Kept dep-free of `kube::runtime::events`
/// (the binaries map it to `EventType`) so breathe-runtime stays a pure mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Normal,
    Warning,
}

/// Map a reconcile receipt to a k8s Event `(severity, reason, note)`, or `None`
/// when nothing should be emitted (a resting `Hold`, a transient `Cooldown`). The
/// `reason` is a stable PascalCase token for `kubectl get events --field-selector
/// reason=…`; the `note` is the human message. The binaries bind this to a
/// `kube::runtime::events::Recorder` and gate it with [`should_emit_event`].
#[must_use]
pub fn event_for(receipt: &TickReceipt) -> Option<(EventKind, &'static str, String)> {
    use EventKind::{Normal, Warning};
    Some(match receipt {
        TickReceipt::Applied { from, to, class } => (
            Normal,
            if to > from { "Grew" } else { "Shrank" },
            format!("carved {from} -> {to} ({class:?})"),
        ),
        TickReceipt::DeferredWouldRestart { from, to, class } => (
            Warning,
            "DeferredCrossing",
            format!("deferred {from} -> {to}: {class:?} crossing blocked by DisruptionPolicy (widen to AllowConditional/AllowRestart to permit)"),
        ),
        TickReceipt::Stale { staleness_secs } => {
            (Warning, "StaleMetric", format!("metric {staleness_secs}s stale — held (never carve on a stale sample)"))
        }
        TickReceipt::Conflict { manager } => (Warning, "Yielded", format!("yielded the field to {manager}")),
        TickReceipt::MetricUnrepresentable { used, capacity } => (
            Warning,
            "MetricUnrepresentable",
            format!("metric reports used {used} > capacity {capacity} — not a per-entity gauge (e.g. local-path PVC stats the whole node fs); held, never carved"),
        ),
        TickReceipt::CapabilityMissing { volume_expansion, per_volume_metrics, provisioner } => (
            Warning,
            "Unsupported",
            format!(
                "StorageClass ({provisioner}) can never converge — allowVolumeExpansion={volume_expansion}, perVolumeMetrics={per_volume_metrics}; provision an elastic StorageClass (e.g. ebs-gp3) or accept the fixed size"
            ),
        ),
        // A dangling targetRef is a distinct, honest arm — never a generic
        // "ReconcileError" (which reads as a breathe-side fault). It self-heals the
        // instant the real object appears (re-derived fresh every tick); mirrors the
        // `CapabilityMissing`->`Unsupported` pattern for storage (task #217).
        TickReceipt::Error { error: ProviderError::TargetNotFound } => (
            Warning,
            "TargetNotFound",
            "targetRef does not exist (namespace/name/kind) — held; will self-heal automatically once the object is created".to_string(),
        ),
        TickReceipt::Error { error } => (Warning, "ReconcileError", error.to_string()),
        // The reason, not "dryRun". `spec.dryRun` has been unread for every k8s
        // band kind since 76924b0, so rendering it as the cause taught the wrong
        // model at the exact moment an operator was debugging — and it could not
        // distinguish an authored hold from an accidental one.
        TickReceipt::ShadowWouldApply { from, to, reason } => (
            Normal,
            "ShadowWouldApply",
            format!("shadow: would carve {from} -> {to} — held: {}", shadow_reason_note(*reason)),
        ),
        TickReceipt::Warmup { observed_for, warmup } => (
            Normal,
            "Warmup",
            format!("warming up ({observed_for}s of {warmup}s) — shrink held until a full duty cycle is observed (boot-spike guard)"),
        ),
        TickReceipt::Throttled { restarting } => (
            Normal,
            "ThrottledHold",
            if *restarting {
                "recently restarted / crash-looping — shrink held (low usage is a symptom, not safe slack)".to_string()
            } else {
                "actively throttled — shrink held + growing out of the cap (usage is CFS-capped; throttle reveals the suppressed demand)".to_string()
            },
        ),
        TickReceipt::Observed { decision } => match decision {
            Decision::AtCeiling { current } => (Normal, "AtCeiling", format!("at ceiling {current} — would grow but capped")),
            Decision::NoSafeShrink { current } => (Normal, "AtFloor", format!("at floor {current} — no safe shrink")),
            Decision::NoLimit => (
                Warning,
                "NoLimit",
                "the target declares NO limit for this dimension and this band may not introduce one — set spec.boundIntroduction: allowed to let breathe seed a bound, or declare a limit upstream".into(),
            ),
            // NOT `AtFloor`: there IS slack, policy declined to take it.
            Decision::ReclaimWithheld { current, reclaimable } => (
                Normal,
                "ReclaimWithheld",
                format!("holding {current} — {reclaimable} reclaimable, withheld by the band's reclaim policy"),
            ),
            Decision::Grow { from, to } | Decision::Shrink { from, to } => {
                (Normal, "ObservedNoAct", format!("observed {from} -> {to} (directionality/observe-only — not applied)"))
            }
            // a Warmup/Throttled decision never reaches Observed (it maps to its own
            // TickReceipt above); kept exhaustive + silent, never a panic.
            Decision::Hold | Decision::Warmup { .. } | Decision::Throttled { .. } => return None, // resting, no event
        },
        TickReceipt::Cooldown => return None, // transient post-carve wait — no event
        TickReceipt::Dormant => return None, // no pods in the group — resting, no event
    })
}

/// Transition-gate for events: a carve (`Applied`) ALWAYS emits (each is a
/// distinct, meaningful event); every other emittable receipt emits ONLY when the
/// phase changed from the prior tick — so a band resting in `Holding`/`AtFloor`
/// produces ~0 events instead of one per 15s tick (no etcd flood).
#[must_use]
pub fn should_emit_event(receipt: &TickReceipt, new_phase: Option<&str>, prior_phase: Option<&str>) -> bool {
    matches!(receipt, TickReceipt::Applied { .. }) || new_phase != prior_phase
}

/// Upsert one condition into `out`, keeping `last_transition_time` STABLE while the
/// status holds (only stamped `now` when the True↔False status actually flips).
fn upsert_condition(
    out: &mut Vec<Condition>,
    prior: &[Condition],
    now: &str,
    type_: &str,
    ok: bool,
    reason: &str,
    message: &str,
    generation: Option<i64>,
) {
    let status = if ok { "True" } else { "False" };
    let last_transition_time = prior
        .iter()
        .find(|c| c.type_ == type_ && c.status == status)
        .map_or_else(|| now.to_string(), |c| c.last_transition_time.clone());
    out.push(Condition {
        type_: type_.into(),
        status: status.into(),
        reason: reason.into(),
        message: message.into(),
        last_transition_time,
        observed_generation: generation,
    });
}

/// Derive the standard k8s conditions (Ready/Converged/Throttled/Stale/Conflict)
/// from the SAME receipt the status + events + metrics read. The FULL array is
/// always returned (a `Patch::Merge` cannot delete a stale element). `kubectl wait
/// --for=condition=Converged` and Flux/Argo health-gating key off these.
#[must_use]
pub fn conditions_for(outcome: &TickOutcome, prior: &[Condition], generation: Option<i64>) -> Vec<Condition> {
    let now = chrono::Utc::now().to_rfc3339();
    let r = &outcome.receipt;
    // OBSERVABLE = "there is a metric/limit to reason on THIS TICK". `NoLimit` is
    // DELIBERATELY NOT in this exclusion set (it was, until 2026-07-26): the
    // bound-introduction guard only fires AFTER a clean observe — breathe read the
    // target, read its usage, and found no declared bound. That is an OBSERVATION,
    // not an observability failure, and calling it `Ready=False` started a
    // 1800s timer that ended in `HealthVerdict::Stuck` FOREVER for every target
    // that legitimately declares no limit (`coredns` / `ebs-csi-controller` on
    // camelot-eks: no cpu limit in their manifests, which is exactly the case the
    // guard exists to respect). `NoLimit` is carried by `Supported=False` below —
    // the condition purpose-built for "correct, permanent, needs operator action".
    //
    // ── This used to BE `Ready`, and that was the bug. ────────────────────────
    // The 2026-07-26 fix above pulled ONE arm (`NoLimit`) out of `Ready` for
    // exactly the reason that applies to the whole set: an absent INPUT is not a
    // failed RECONCILE. `Error { MetricsMissing }` is the arm it did not reach, and
    // on 2026-08-07 that cost a live release. `Ready` is what kstatus reads, so
    // helm-controller/Flux/Argo treat it as "did this object come up?" — with
    // `Ready = observable`, a metrics-server outage made every band report
    // NOT-CAME-UP. Measured on camelot-eks: 115/115 bands `Ready=False` while
    // `TargetFound`/`Supported`/`Conflict` were all green, i.e. every band was
    // ACCEPTED and merely blind. A forced `helm upgrade` of `camelot-build/sui`
    // then blocked the full 5m timeout and Flux's `remediation.strategy:
    // uninstall` DELETED AND REINSTALLED the release (history reset to `.v1`).
    // The reinstall took 6s, because freshly-created bands have no status yet —
    // which is why this only ever bites on UPGRADE, never on install.
    //
    // So: an observability declaration must never be able to become an
    // availability precondition. `Ready` answers "have I accepted and taken
    // ownership of this spec?"; `Observable` answers "do I have data right now?".
    // Two facts, two conditions — never one bit.
    let observable = !matches!(
        r,
        TickReceipt::Error { .. } | TickReceipt::MetricUnrepresentable { .. } | TickReceipt::CapabilityMissing { .. }
    );
    // SUPPORTED (the design's point-3(b) "will never converge without operator
    // action" signal, distinct from "waiting"): `false` for the two receipts that
    // are permanent-by-construction until a human acts —
    //   * `CapabilityMissing` — the StorageClass cannot expand (task #167), and
    //   * `NoLimit` — the target declares no bound and `boundIntroduction:
    //     forbidden` says breathe may not invent one.
    // Every other receipt (including `Conflict`/`MetricUnrepresentable`, which MAY
    // be transient) stays `true`.
    //
    // Why `Supported` and not a shadow-only special case: the state is
    // GATE-INDEPENDENT. A LIVE band pointed at a limitless target is just as
    // permanently non-convergent as a shadowed one, so keying the fix on the
    // authored gate would leave the identical bug one `writeIntent` away. And
    // `health_verdict` already checks `Supported` FIRST and documents
    // `Unsupported` as taking priority over `Stuck` — "an unsupported band isn't
    // waiting, it structurally can't" is precisely this band's situation.
    let supported = !matches!(
        r,
        TickReceipt::CapabilityMissing { .. } | TickReceipt::Observed { decision: Decision::NoLimit }
    );
    // Which of the two unsupported causes this is, for an honest reason+message
    // (the message is what `HealthVerdict::Unsupported` carries to the operator).
    let no_limit = matches!(r, TickReceipt::Observed { decision: Decision::NoLimit });
    let converged = matches!(
        r,
        TickReceipt::Observed {
            decision: Decision::Hold
                | Decision::AtCeiling { .. }
                | Decision::NoSafeShrink { .. }
                // withheld-by-policy is a RESTING state, not an unconverged one:
                // the band has decided, and the decision is "leave it".
                | Decision::ReclaimWithheld { .. }
        } | TickReceipt::Dormant // an empty (scaled-to-zero) target is trivially at rest
    );
    let throttled = matches!(
        r,
        TickReceipt::Cooldown | TickReceipt::DeferredWouldRestart { .. } | TickReceipt::Stale { .. } | TickReceipt::Warmup { .. } | TickReceipt::Throttled { .. }
    );
    let stale = matches!(r, TickReceipt::Stale { .. });
    let conflict = matches!(r, TickReceipt::Conflict { .. });
    // TARGET FOUND (task #217's honest-arm fix): `false` ONLY when the target
    // object itself does not exist. Distinct from `Supported=False`
    // ("can never converge without operator action") — a dangling targetRef is
    // self-healing (re-derived fresh every tick; the moment the real object
    // appears this flips back to `true` with no accumulated state).
    let target_found = !matches!(r, TickReceipt::Error { error: ProviderError::TargetNotFound });
    // READY = ACCEPTANCE, never achievement — see the `observable` note above.
    // The band is enrolled, its config parses, and its targetRef resolves to a
    // live object, so the controller has taken ownership and is running its loop.
    // Deliberately NOT gated on:
    //   * `observable` — an absent input is `Observable=False`, not a failed
    //     reconcile (the whole point of this split);
    //   * `supported`  — `Supported=False` already means "correct, permanent,
    //     needs operator action", and the 2026-07-26 `NoLimit` fix established
    //     that such a band stays Ready. Gating here would re-break it;
    //   * `conflict`   — carried by `Conflict` below, and unchanged from before.
    // `TargetFound=False` is the one honest not-ready: there is no object to own.
    let ready = target_found;

    let mut out = Vec::with_capacity(8);
    upsert_condition(&mut out, prior, &now, "Ready", ready,
        if ready { "Accepted" } else { "TargetMissing" },
        if ready { "enrolled, config parses, targetRef resolved — the controller owns this band" } else { "targetRef does not resolve — nothing to take ownership of" }, generation);
    upsert_condition(&mut out, prior, &now, "Observable", observable,
        if observable { "MetricObservable" } else { "NotObservable" },
        if observable { "a metric/limit is available to reason on" } else { "no metric/limit to reason on — the band is accepted and idle, NOT failed" }, generation);
    upsert_condition(&mut out, prior, &now, "Converged", converged,
        if converged { "WithinBand" } else { "Adjusting" },
        if converged { "utilization is within the deadband" } else { "carving/waiting toward the setpoint" }, generation);
    upsert_condition(&mut out, prior, &now, "Throttled", throttled,
        if throttled { "Throttled" } else { "Free" }, "in cooldown / deferred crossing / stale metric", generation);
    upsert_condition(&mut out, prior, &now, "Stale", stale,
        if stale { "StaleMetric" } else { "Fresh" }, "driving metric sample age vs maxStaleness", generation);
    upsert_condition(&mut out, prior, &now, "Conflict", conflict,
        if conflict { "FieldOwnedElsewhere" } else { "SoleWriter" }, "single-writer guard", generation);
    upsert_condition(&mut out, prior, &now, "Supported", supported,
        if supported { "CapabilityOk" } else if no_limit { "NoBoundDeclared" } else { "StorageClassUnsupported" },
        if no_limit {
            "the target declares NO bound for this dimension and spec.boundIntroduction is `forbidden` — this band can NEVER converge until a limit is declared upstream or boundIntroduction is set to `allowed`"
        } else {
            "StorageClass allowVolumeExpansion + per-volume metrics — False means this band can NEVER converge without operator action"
        }, generation);
    upsert_condition(&mut out, prior, &now, "TargetFound", target_found,
        if target_found { "Resolved" } else { "TargetMissing" },
        if target_found { "targetRef resolves to a live object" } else { "targetRef does not exist — self-heals automatically once the object is created" },
        generation);
    out
}

/// Below this, `Ready=False` / `Converged=False` (while not `Throttled`) /
/// `Conflict=True` graduate from "waiting" to "stuck" — long enough to clear a
/// warmup window, a cooldown, or a transient field-manager race, short enough
/// that a genuinely wedged band surfaces inside one operator session rather than
/// requiring a human to notice by polling.
pub const STUCK_AFTER_SECS: i64 = 1800;

/// Cross-dimension health rollup — the generalization of the storage-only
/// `Supported=False` terminal (task #167) into a single verdict every band kind
/// gets for free the moment it carries [`conditions_for`]'s output. Computed
/// purely from the conditions array + `now`; no new per-band state (each
/// condition's `last_transition_time` is already stable-while-held).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthVerdict {
    /// Resting (`Converged`/`Throttled`) or freshly adjusting — nothing to do.
    Healthy,
    /// `Supported=False` — will NEVER converge without operator action. Two
    /// producers today: StorageBand's `CapabilityMissing` (the StorageClass
    /// cannot expand) and any band's `Decision::NoLimit` (the target declares no
    /// bound and `boundIntroduction: forbidden`). Takes priority over `Stuck` —
    /// an unsupported band isn't "waiting", it structurally can't.
    Unsupported { reason: String },
    /// `TargetFound=False` — the band's targetRef points at an object that does
    /// not (yet) exist (task #217). Distinct from `Stuck`: this is re-derived
    /// fresh every tick (no accumulated state), so it self-heals the INSTANT the
    /// real object appears, and is surfaced immediately rather than waiting
    /// [`STUCK_AFTER_SECS`] behind the generic Ready/Converged timer.
    TargetNotFound { since_secs: i64 },
    /// `Observable=False` — the band is ACCEPTED and owned, but has no metric to
    /// reason on (the metrics API is down, or this target's usage is unreadable).
    /// Distinct from `Stuck` in the same way `TargetNotFound` is: nothing is
    /// wedged, an INPUT is absent, and it self-heals the instant the metric
    /// returns — so it must not sit behind the generic [`STUCK_AFTER_SECS`] timer.
    ///
    /// Checked BEFORE the `Converged` timer on purpose. A blind band can never be
    /// converged either, so without this arm the metrics outage would simply
    /// reappear as `Stuck { condition: "Converged" }` — the same false alarm one
    /// condition to the left. Receipt: 115/115 camelot-eks bands sat in exactly
    /// this state on 2026-08-07 while metrics-server was floored at 0.
    Unobservable { since_secs: i64 },
    /// A permanently-shadowed band (`dryRun:true`) whose target sits outside the
    /// deadband: `Converged=False` has held past [`STUCK_AFTER_SECS`], but a
    /// shadow band computes what it WOULD do and never actually resizes, so it
    /// can STRUCTURALLY never converge. Distinct from `Stuck` — this is the band
    /// working exactly as designed, not wedged; never alarming.
    ShadowPending { since_secs: i64 },
    /// A condition that should be transient has held past [`STUCK_AFTER_SECS`] —
    /// no longer "waiting on a warmup/cooldown/race", now needs attention.
    Stuck { condition: String, since_secs: i64, reason: String },
}

impl HealthVerdict {
    /// The `BandStatus.health` string — stable, PascalCase, jsonpath-queryable
    /// (`kubectl get bands -o jsonpath='{.items[?(@.status.health!="Healthy")]}'`).
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Healthy => "Healthy",
            Self::Unsupported { .. } => "Unsupported",
            Self::TargetNotFound { .. } => "TargetNotFound",
            Self::Unobservable { .. } => "Unobservable",
            Self::ShadowPending { .. } => "ShadowPending",
            Self::Stuck { .. } => "Stuck",
        }
    }
}

fn find_condition<'a>(conditions: &'a [Condition], type_: &str) -> Option<&'a Condition> {
    conditions.iter().find(|c| c.type_ == type_)
}

/// Seconds between two RFC3339 timestamps (`now - since`), floored at 0. `None`
/// if either fails to parse — a malformed timestamp must never panic or produce
/// a bogus (possibly negative-then-huge) duration.
#[must_use]
pub fn seconds_since(now: &str, since: &str) -> Option<i64> {
    let now = chrono::DateTime::parse_from_rfc3339(now).ok()?;
    let since = chrono::DateTime::parse_from_rfc3339(since).ok()?;
    Some((now - since).num_seconds().max(0))
}

/// Classify [`HealthVerdict`] from a band's own conditions array. `now` is an
/// RFC3339 timestamp (injected, not read internally, so this stays pure/testable
/// — matches this file's existing `conditions_for`/`upsert_condition` style).
/// `effective_dry_run` is the tick's shadow mode — a permanently-shadowed band
/// can structurally never clear `Converged=False` (it computes what it WOULD do
/// and never applies), so the age-based `Converged` → `Stuck` classification is
/// downgraded to the distinct, non-alarming [`HealthVerdict::ShadowPending`].
/// The `Ready`-based check is UNCHANGED by `effective_dry_run` — an
/// observability/metrics failure is meaningful even in shadow mode.
#[must_use]
pub fn health_verdict(conditions: &[Condition], now: &str, stuck_after_secs: i64, effective_dry_run: bool) -> HealthVerdict {
    if let Some(c) = find_condition(conditions, "Supported") {
        if c.status != "True" {
            return HealthVerdict::Unsupported { reason: c.message.clone() };
        }
    }
    // TARGET NOT FOUND (task #217): checked early, exactly like `Supported` above
    // — a dangling targetRef is a distinct, honest fact, not a generic timer-based
    // `Stuck`. Re-derived fresh every tick, so it self-heals the instant the real
    // object appears (no accumulated state to unwind).
    if let Some(c) = find_condition(conditions, "TargetFound") {
        if c.status != "True" {
            let since_secs = seconds_since(now, &c.last_transition_time).unwrap_or(0);
            return HealthVerdict::TargetNotFound { since_secs };
        }
    }
    // UNOBSERVABLE: the same family as `TargetFound` above — an absent INPUT, not a
    // wedged band — so it is surfaced immediately and honestly rather than behind
    // the generic timer. Placed before `Converged` deliberately: a blind band is
    // never converged either, so leaving it to fall through would just re-emit the
    // identical false alarm as `Stuck { condition: "Converged" }`.
    if let Some(c) = find_condition(conditions, "Observable") {
        if c.status != "True" {
            let since_secs = seconds_since(now, &c.last_transition_time).unwrap_or(0);
            return HealthVerdict::Unobservable { since_secs };
        }
    }
    // Throttled=True covers warmup / cooldown / deferred-crossing / stale-metric —
    // all EXPECTED, self-resolving holds. A throttled band is never "stuck", even
    // if Converged has been False the entire time it's been throttled.
    if find_condition(conditions, "Throttled").is_some_and(|c| c.status == "True") {
        return HealthVerdict::Healthy;
    }
    // READY: unaffected by dry-run — an observability/metrics failure is
    // meaningful even in shadow mode.
    if let Some(c) = find_condition(conditions, "Ready") {
        if c.status == "False" {
            if let Some(secs) = seconds_since(now, &c.last_transition_time) {
                if secs >= stuck_after_secs {
                    return HealthVerdict::Stuck { condition: "Ready".into(), since_secs: secs, reason: c.message.clone() };
                }
            }
        }
    }
    // CONVERGED: dry-run aware. A band permanently in shadow mode whose target
    // sits outside the deadband can never actually converge (it never applies) —
    // that is the band working as designed, not wedged, so the age-based Stuck
    // classification is downgraded to `ShadowPending` instead of alarming forever.
    if let Some(c) = find_condition(conditions, "Converged") {
        if c.status == "False" {
            if let Some(secs) = seconds_since(now, &c.last_transition_time) {
                if secs >= stuck_after_secs {
                    if effective_dry_run {
                        return HealthVerdict::ShadowPending { since_secs: secs };
                    }
                    return HealthVerdict::Stuck { condition: "Converged".into(), since_secs: secs, reason: c.message.clone() };
                }
            }
        }
    }
    if let Some(c) = find_condition(conditions, "Conflict") {
        if c.status == "True" {
            if let Some(secs) = seconds_since(now, &c.last_transition_time) {
                if secs >= stuck_after_secs {
                    return HealthVerdict::Stuck { condition: "Conflict".into(), since_secs: secs, reason: c.message.clone() };
                }
            }
        }
    }
    HealthVerdict::Healthy
}

/// Map a [`HealthVerdict`] to a k8s Event `(severity, reason, note)`, or `None`
/// for `Healthy` (a healthy band emits nothing — no event flood on every tick).
/// Mirrors [`event_for`]'s shape so the controller's existing `emit_event` call
/// pattern extends to health with no new plumbing concept.
#[must_use]
pub fn health_event_for(verdict: &HealthVerdict) -> Option<(EventKind, &'static str, String)> {
    match verdict {
        HealthVerdict::Healthy => None,
        HealthVerdict::Unsupported { reason } => {
            Some((EventKind::Warning, "BandUnsupported", format!("band can never converge without operator action: {reason}")))
        }
        HealthVerdict::TargetNotFound { since_secs } => Some((
            EventKind::Warning,
            "TargetNotFound",
            format!("targetRef has not resolved for {since_secs}s — will self-heal automatically once the object is created; verify the targetRef name/kind/namespace if this persists"),
        )),
        // Warning, but explicitly NOT a band fault: the band is accepted and owned,
        // its input is gone. The note points at the metrics pipeline because that is
        // where the fix is — chasing the band itself is the wrong hunt (2026-08-07:
        // 115 bands read as broken when metrics-server was simply floored at 0).
        HealthVerdict::Unobservable { since_secs } => Some((
            EventKind::Warning,
            "BandUnobservable",
            format!(
                "no metric to reason on for {since_secs}s — the band is ACCEPTED and idle, not failed; check the metrics pipeline (metrics-server / the metric source), not this band"
            ),
        )),
        // non-alarming by design: a permanently-shadowed band that never converges
        // is working exactly as configured, not wedged — Normal, not Warning.
        HealthVerdict::ShadowPending { since_secs } => Some((
            EventKind::Normal,
            "ShadowPending",
            format!("shadow mode (dryRun) — {since_secs}s outside the deadband; would-carve only, never applied (flip dryRun to converge)"),
        )),
        HealthVerdict::Stuck { condition, since_secs, reason } => Some((
            EventKind::Warning,
            "BandStuck",
            format!("{condition} has held for {since_secs}s (past the {STUCK_AFTER_SECS}s stuck threshold): {reason}"),
        )),
    }
}

/// Transition-gate for health events, mirroring [`should_emit_event`]: emit only
/// when the health LABEL changed since the prior tick, so a band parked in
/// `Stuck` for hours emits one event, not one per reconcile tick.
#[must_use]
pub fn should_emit_health_event(verdict: &HealthVerdict, prior_health: Option<&str>) -> bool {
    Some(verdict.label()) != prior_health
}

/// Map one reconcile OUTCOME to the typed CR status — every branch observable,
/// none silent. This is the single source of truth for band status semantics
/// across both reconcile processes. It reports not just *what happened* (phase +
/// legible last_decision) but the OBSERVED inputs that drove it (util/used/capacity/
/// freshness), the effective mode (dry-run/policy), the golden/ceiling edge tier,
/// the cooldown remaining, and cumulative carve/deferral/conflict counters —
/// everything `kubectl get/describe` and Grafana need, all from the one TickOutcome.
///
/// `prior` is the band's CURRENT status (read before reconcile) — used to compute
/// the cooldown remaining from the last carve epoch and to carry forward history.
/// `cooldown_seconds` is the band's configured cooldown window. `counters` is the
/// cumulative carve/deferral/conflict count, sourced from the `DecisionLog` (the
/// single accumulation point — see [`entry_for`] / [`CumulativeCounters::fold`]);
/// `status_for` no longer increments counters itself (the dual-source-of-truth the
/// Urdume-microservice refactor removed).
/// Attach the tick's TYPED authorization verdict to a status, alongside the
/// legacy `effectiveDryRun` bool.
///
/// Kept as a separate call rather than a new `status_for` parameter so this is
/// a purely additive change at five existing call sites — and so the two
/// fields are written from ONE value and can never disagree: the bool is
/// literally `gate.is_shadow()`, re-derived here rather than passed in.
///
/// A band whose status carries `effectiveGate` but whose `effectiveDryRun`
/// contradicts it is therefore unrepresentable through this function.
pub fn set_effective_gate(s: &mut BandStatus, gate: &breathe_provider::EffectiveGate) {
    s.effective_dry_run = Some(gate.is_shadow());
    s.effective_gate = Some(gate.report());
}

#[must_use]
#[allow(clippy::too_many_lines)] // one exhaustive receipt→status match; the +1 Throttled arm pushed it over
pub fn status_for(
    outcome: &TickOutcome,
    prior: Option<&BandStatus>,
    cooldown_seconds: u64,
    generation: Option<i64>,
    counters: CumulativeCounters,
) -> BandStatus {
    let mut s = BandStatus::default();
    let receipt = &outcome.receipt;

    // ── COMMON: the observed inputs + effective mode + edge tier (from the
    //    outcome, available on every non-pre-observe-error tick). ──────────────
    s.effective_dry_run = Some(outcome.dry_run());
    s.effective_policy = Some(policy_str(outcome.policy));
    s.edge_tier = Some(edge_tier_str(receipt.edge_tier()));

    // ── EVER-GOVERNED (sticky). Carried on EVERY path, including the error and
    //    Dormant arms below, because `s` starts from `BandStatus::default()` —
    //    anything not explicitly carried is silently dropped each tick, and a
    //    latch that resets is not a latch.
    //
    //    Set on the first tick that observes a live pod; never cleared. This is
    //    what separates a band resting at zero (proven: it governed something
    //    once, so an empty tick is genuinely at-rest) from a band whose selector
    //    has never matched anything (unproven: it may be governing nothing,
    //    forever, while reporting Dormant + converged). See the field's own doc
    //    on `BandStatus` for why this is a latch and not a timeout.
    let prior_first_observed = prior.and_then(|p| p.first_observed_epoch);
    s.first_observed_epoch = if outcome.observed.is_some() {
        prior_first_observed.or_else(|| Some(now_secs()))
    } else {
        prior_first_observed
    };

    if let Some(obs) = &outcome.observed {
        s.observed_used = Some(obs.used as i64);
        // The trailing-window peak that drove this tick's never-OOM shrink floor —
        // persisted so the next reconcile folds the current sample into it (the
        // cross-tick peak carry). `reconcile_one` guarantees `peak_used ≥ used`.
        s.observed_peak_used = Some(obs.peak_used as i64);
        s.observed_capacity = Some(obs.capacity() as i64);
        s.freshness_seconds = Some(obs.staleness_secs as i64);
        if let Some(u) = util_of(obs) {
            s.observed_util = Some(u);
            s.last_util = Some(format!("{:.0}%", u * 100.0)); // the headline Util column
        }
    }

    // ── PER-RECEIPT: phase, legible decision, current_limit, action class. ────
    match receipt {
        TickReceipt::Conflict { manager } => {
            s.phase = Some("Conflict".into());
            s.conflict_manager = Some(manager.clone());
            s.last_decision = Some(format!("yielded to {manager}"));
        }
        TickReceipt::MetricUnrepresentable { used, capacity } => {
            s.phase = Some("MetricUnrepresentable".into());
            s.last_decision = Some(format!(
                "used {used} > capacity {capacity} — metric not per-entity (e.g. local-path PVC = whole-node fs); held"
            ));
        }
        TickReceipt::CapabilityMissing { volume_expansion, per_volume_metrics, provisioner } => {
            // the fail-fast terminal: checked BEFORE the single-writer guard, so this
            // is reached on the very first tick regardless of who else owns the
            // field — never `Conflict`/`MetricUnrepresentable` for the same root cause.
            s.phase = Some("Unsupported".into());
            s.last_decision = Some(format!(
                "StorageClass ({provisioner}) can never converge — allowVolumeExpansion={volume_expansion}, perVolumeMetrics={per_volume_metrics}; provision an elastic StorageClass (e.g. ebs-gp3) or accept the fixed size"
            ));
        }
        TickReceipt::Warmup { observed_for, warmup } => {
            // the workload is still warming up (restarted < warmup ago) — a shrink is
            // HELD so an un-observed boot spike can be seen before any carve. The limit
            // is left exactly as-is (the comfortable berth: undisturbed, golden).
            s.phase = Some("Warmup".into());
            s.last_decision = Some(format!(
                "warming up ({observed_for}s of {warmup}s) — shrink held until a full duty cycle is observed"
            ));
        }
        TickReceipt::Throttled { restarting } => {
            // the no-starve hold: the workload is throttled / crash-looping, so its
            // (CFS-capped) low usage is a symptom, not safe slack — the shrink is HELD
            // and the band grows OUT of the throttle (the limit only ever rises). The
            // comfortable berth: undisturbed, never starved. Closes the CPU ratchet.
            s.phase = Some("Throttled".into());
            s.last_decision = Some(if *restarting {
                "recently restarted / crash-looping — shrink held (low usage is not safe slack)".into()
            } else {
                "actively throttled — shrink held, growing out of the cap (usage is CFS-capped)".into()
            });
        }
        TickReceipt::Stale { staleness_secs } => {
            s.phase = Some("Stale".into());
            s.last_decision = Some(format!("metric {staleness_secs}s stale — held"));
        }
        TickReceipt::Cooldown => {
            s.phase = Some("Cooldown".into());
            s.last_decision = Some("cooling down after a carve".into());
        }
        TickReceipt::Applied { from, to, class } => {
            s.phase = Some(if to > from { "Growing" } else { "Shrinking" }.into());
            s.current_limit = Some(to.to_string());
            s.last_decision = Some(format!("{from} -> {to} ({class:?})"));
            s.last_action_class = Some(format!("{class:?}"));
            s.last_change_epoch = Some(now_secs());
        }
        TickReceipt::ShadowWouldApply { from, to, reason } => {
            s.phase = Some("ShadowWouldApply".into());
            s.current_limit = Some(from.to_string()); // shadow mutates nothing — the UNCHANGED limit
            s.last_decision = Some(format!("shadow: {from} -> {to} — held: {}", shadow_reason_note(*reason)));
        }
        TickReceipt::DeferredWouldRestart { from, to, class } => {
            // the comfortable berth: breathe REFUSED a ceiling crossing — the
            // workload stays golden (undisturbed), un-converged, limit unchanged.
            s.phase = Some("DeferredWouldRestart".into());
            s.current_limit = Some(from.to_string()); // the crossing was refused — limit unchanged
            s.last_decision = Some(format!("{from} -> {to} deferred: {class:?} crossing blocked by DisruptionPolicy (set AllowConditional/AllowRestart to permit)"));
            s.last_action_class = Some(format!("{class:?}"));
        }
        TickReceipt::Observed { decision } => {
            let (phase, note) = match decision {
                Decision::Hold => ("Holding", "within band — held".to_string()),
                Decision::AtCeiling { current } => ("AtCeiling", format!("at ceiling {current} — would grow")),
                Decision::NoSafeShrink { current } => ("AtFloor", format!("at floor {current} — no safe shrink")),
                Decision::NoLimit => (
                    "NoLimit",
                    "the target declares no limit for this dimension — breathe will not introduce one (set spec.boundIntroduction: allowed to permit it)".to_string(),
                ),
                // The honest phase for a band whose slack is real but withheld —
                // the `AtFloor` this used to report was a lie (it is nowhere near
                // a floor), and it made 36 idle camelot-eks MemoryBands look
                // converged.
                Decision::ReclaimWithheld { current, reclaimable } => (
                    "ReclaimWithheld",
                    format!("holding {current} — {reclaimable} reclaimable, withheld by policy"),
                ),
                Decision::Grow { from, to } | Decision::Shrink { from, to } => {
                    ("Observed", format!("observed {from} -> {to} (not applied)"))
                }
                // a Warmup decision is surfaced via TickReceipt::Warmup, never here;
                // kept exhaustive (no panic) in case a future path routes it through.
                Decision::Warmup { observed_for, warmup, .. } => {
                    ("Warmup", format!("warming up ({observed_for}s of {warmup}s) — shrink held"))
                }
                // a Throttled decision is surfaced via TickReceipt::Throttled, never
                // here; kept exhaustive (no panic) in case a future path routes it.
                Decision::Throttled { restarting, .. } => {
                    ("Throttled", format!("throttled/restarting={restarting} — shrink held (no-starve)"))
                }
            };
            s.phase = Some(phase.into());
            s.last_decision = Some(note);
        }
        TickReceipt::Dormant => {
            // benign resting state: the label-selected pod group is empty (the
            // ephemeral target is scaled to zero). Nothing to observe or carve; the
            // band waits. NOT an error — counted at-rest (converged) in the overview.
            s.phase = Some("Dormant".into());
            s.last_decision = Some("no pods in the label group — waiting (target scaled to zero)".into());
        }
        TickReceipt::Error { error: ProviderError::TargetNotFound } => {
            // the honest, distinct arm (task #217) — never the generic "Error",
            // which reads as a breathe-side fault. Self-heals automatically once the
            // targetRef resolves; mirrors CapabilityMissing's own dedicated phase.
            s.phase = Some("TargetNotFound".into());
            s.last_decision =
                Some("targetRef does not exist — held; will self-heal automatically once the object is created".into());
        }
        TickReceipt::Error {
            error: ProviderError::MetricsMissing,
        } => {
            // Same honest-arm treatment as `TargetNotFound` above, for the same
            // reason: the generic "Error" reads as a breathe-side fault, and this
            // is not one — the band is accepted and owned, its metric source is
            // simply not answering. Self-heals the instant the metric returns.
            // Receipt (2026-08-07): every band on camelot-eks sat in phase `Error`
            // with a null message because metrics-server was floored at 0, which
            // made `Error` the RESTING state and therefore worth nothing as a
            // signal — a real fault would have been indistinguishable from it.
            s.phase = Some("Unobservable".into());
            s.last_decision = Some(
                "no metric to reason on — held; check the metrics pipeline, not this band".into(),
            );
        }
        TickReceipt::Error { error } => {
            s.phase = Some("Error".into());
            s.last_decision = Some(error.to_string());
        }
    }

    // current_limit on EVERY arm: any non-carve tick reports the LIVE limit (the
    // observed capacity) rather than a stale value; Applied set its own `to` above.
    if s.current_limit.is_none() {
        if let Some(obs) = &outcome.observed {
            s.current_limit = Some(obs.capacity().to_string());
        }
    }

    // ── CUMULATIVE COUNTERS — the single fold lives in the DecisionLog; this is
    //    purely a projection of the count the caller already accumulated. ───────
    s.carves_total = Some(counters.carves);
    s.deferrals_total = Some(counters.deferrals);
    s.conflicts_total = Some(counters.conflicts);

    // ── COOLDOWN REMAINING — from the last carve epoch (this tick's, or prior's). ─
    let last_carve = s.last_change_epoch.or_else(|| prior.and_then(|p| p.last_change_epoch)).unwrap_or(0);
    let remaining = (last_carve + cooldown_seconds as i64 - now_secs()).max(0);
    s.cooldown_remaining_seconds = Some(remaining);

    // ── M4: observedGeneration + standard conditions (kubectl wait / health). ──
    s.observed_generation = generation;
    s.conditions = conditions_for(outcome, prior.map_or(&[][..], |p| p.conditions.as_slice()), generation);

    // ── HEALTH ROLLUP — derived purely from the conditions just computed above
    //    (no new tracked state; last_transition_time is already stable-while-held
    //    per condition). Same across all 5 band kinds for free.
    let now = now_rfc3339();
    s.health = Some(health_verdict(&s.conditions, &now, STUCK_AFTER_SECS, outcome.dry_run()).label().to_string());

    // ── B: per-band TREND (the over-time view as a k8s object, no Grafana) —
    //    append on a carve or a phase change, cap to the last N. A resting band's
    //    history stays put, so `kubectl get <band> -o yaml` shows the trajectory. ─
    const HISTORY_MAX: usize = 16;
    let phase_changed = prior.and_then(|p| p.phase.as_deref()) != s.phase.as_deref();
    let carved = matches!(receipt, TickReceipt::Applied { .. });
    let mut history = prior.map_or_else(Vec::new, |p| p.history.clone());
    if carved || phase_changed {
        history.push(TrendSample {
            time: chrono::Utc::now().to_rfc3339(),
            util: s.observed_util,
            limit: s.current_limit.as_deref().and_then(|l| l.parse().ok()),
            phase: s.phase.clone().unwrap_or_default(),
            decision: s.last_decision.clone(),
        });
        if history.len() > HISTORY_MAX {
            history.drain(0..history.len() - HISTORY_MAX);
        }
    }
    s.history = history;

    s
}

/// The WARMUP state for this tick: `(observed_for_secs, warmup_start_epoch)`. Pure
/// + testable; the single source of truth both reconcile binaries use to drive the
/// warmup gate and persist the warmup-start epoch.
///
/// - `observed_for_secs = now - warmup_start_epoch` — how long the workload has been
///   observed since its last (re)start. Fed into `ReconcileInput.observed_for_secs`
///   so a shrink is held while it is below the band's `warmup_seconds`.
/// - `warmup_start_epoch` — carried forward in status. It is RESET to `now` when a
///   RESTART is detected: the live limit (`observed_capacity`) dropped vs the prior
///   tick (a re-created pod fell back to its template default), which means a fresh
///   boot — and therefore a fresh boot spike — is incoming, so the warmup clock must
///   restart. Absent prior epoch ⇒ this is the first observation ⇒ start the clock now.
///
/// `warmup_seconds == 0` short-circuits to `(u64::MAX, now)` (gate disabled — always
/// past warmup), so a band that opts out is byte-identical to the pre-warmup path.
#[must_use]
pub fn warmup_state(prior: Option<&BandStatus>, observed_capacity: Option<u64>, warmup_seconds: u64, now: i64) -> (u64, i64) {
    if warmup_seconds == 0 {
        return (u64::MAX, now);
    }
    let prior_epoch = prior.and_then(|p| p.warmup_start_epoch);
    let prior_cap = prior.and_then(|p| p.observed_capacity).and_then(|c| u64::try_from(c).ok());
    // RESTART DETECTION: a strictly-lower live limit than last tick ⇒ a re-created pod
    // fell back to its template default ⇒ a fresh boot ⇒ restart the warmup clock so
    // the (un-observed) boot spike is seen before any carve resumes.
    let restarted = matches!((observed_capacity, prior_cap), (Some(now_cap), Some(was)) if now_cap < was);
    let start = match prior_epoch {
        Some(e) if !restarted => e,
        _ => now, // first observation, or a detected restart ⇒ (re)start the clock
    };
    let observed_for = u64::try_from((now - start).max(0)).unwrap_or(0);
    (observed_for, start)
}

/// Render WHY a band is held, for the status line and the k8s Event.
///
/// A typed [`Display`](std::fmt::Display)-shaped rendering of the reason rather
/// than the flat string "dryRun", which every surface used to print. The
/// distinction it restores is the one that matters operationally: an operator
/// who WROTE `writeIntent: observe` versus a band that FELL into shadow because
/// its metric went stale or another manager took the field.
#[must_use]
pub fn shadow_reason_note(reason: breathe_provider::ShadowReason) -> String {
    use breathe_provider::ShadowReason as R;
    match reason {
        R::Frozen => "an external freeze (pool / fleet write switch) is engaged".into(),
        R::ModeShadow => "authored hold (writeIntent: observe)".into(),
        R::Suspended => "authored freeze (writeIntent: frozen)".into(),
        // The gate's own name predates the Ready/Observable split and is now the
        // narrower of the two facts: the write was withheld because there was no
        // metric, which is `Observable=False`. The band itself stays Ready
        // (accepted + owned). Worded from the condition an operator will actually
        // see, not from the enum variant.
        R::NotReady => "NOT AUTHORED — no observable metric yet (Observable=False; the band itself is accepted)".into(),
        R::Stale => "NOT AUTHORED — the driving metric sample is too old to trust".into(),
        R::Conflict => "NOT AUTHORED — another field manager owns the target".into(),
        R::IntentMalformed => {
            "writeIntent does not parse (an `intent: write` naming no author) — failing safe".into()
        }
        R::ConfirmPending { held_secs, need_secs } => {
            format!("calibrating — {held_secs}s of {need_secs}s of clean observation held")
        }
    }
}

/// A LIVE gate for the test fixtures in this crate. Obtained the only way any
/// caller can obtain one — through `resolve_gate`. There is deliberately no test
/// back door: `LiveWitness` has no public constructor, so even fixtures go
/// through the real resolver.
#[cfg(test)]
fn test_live_gate() -> breathe_provider::EffectiveGate {
    use breathe_provider::gate::{resolve_gate, ConfirmVerdict, GateInputs, LegacyDecision, ShadowReason, WriteIntent};
    resolve_gate(&GateInputs {
        intent: Some(Ok(WriteIntent::Write { authorized_by: "breathe-runtime tests".into() })),
        frozen: false,
        confirm: ConfirmVerdict::NotEvaluated,
        legacy: LegacyDecision::Shadow(ShadowReason::NotReady),
    })
}

/// A short, stable tag for a receipt kind — the `decision_log` row's `receipt_kind`.
fn receipt_kind_str(r: &TickReceipt) -> &'static str {
    match r {
        TickReceipt::Conflict { .. } => "Conflict",
        TickReceipt::MetricUnrepresentable { .. } => "MetricUnrepresentable",
        TickReceipt::CapabilityMissing { .. } => "CapabilityMissing",
        TickReceipt::Stale { .. } => "Stale",
        TickReceipt::Cooldown => "Cooldown",
        TickReceipt::Applied { .. } => "Applied",
        TickReceipt::ShadowWouldApply { .. } => "ShadowWouldApply",
        TickReceipt::DeferredWouldRestart { .. } => "DeferredWouldRestart",
        TickReceipt::Observed { .. } => "Observed",
        TickReceipt::Warmup { .. } => "Warmup",
        TickReceipt::Throttled { .. } => "Throttled",
        TickReceipt::Dormant => "Dormant",
        TickReceipt::Error { .. } => "Error",
    }
}

/// Classify a reconcile outcome into a [`DecisionEntry`] — the **4th consumer**
/// of the `TickOutcome` keystone (alongside [`status_for`], [`event_for`],
/// [`metrics_for`]), so the counter fold and the append-only decision feed are
/// driven by the SAME outcome with zero drift. The boolean classifications are
/// byte-identical to the predicates the old inline counter block used
/// (`matches!(receipt, Applied/DeferredWouldRestart/Conflict)`), so folding them
/// reproduces the previous counter sequence exactly.
#[must_use]
pub fn entry_for(outcome: &TickOutcome) -> DecisionEntry {
    let r = &outcome.receipt;
    let (from_limit, to_limit) = match r {
        TickReceipt::Applied { from, to, .. }
        | TickReceipt::ShadowWouldApply { from, to, .. }
        | TickReceipt::DeferredWouldRestart { from, to, .. } => (Some(*from), Some(*to)),
        _ => (None, None),
    };
    // Exactly the receipt→counter mapping the old inline `matches!` block used —
    // Applied⇒carve, DeferredWouldRestart⇒deferral, Conflict⇒conflict, else none.
    let class = match r {
        TickReceipt::Applied { .. } => CounterClass::Carve,
        TickReceipt::DeferredWouldRestart { .. } => CounterClass::Deferral,
        TickReceipt::Conflict { .. } => CounterClass::Conflict,
        _ => CounterClass::NoCount,
    };
    DecisionEntry {
        receipt_kind: receipt_kind_str(r).to_string(),
        class,
        from_limit,
        to_limit,
        dry_run: outcome.dry_run(),
    }
}

/// Read the cumulative counters off a band's prior status — the seed the
/// in-memory `DecisionLog` folds the new decision onto (the CRD status is the
/// durability projection in the very-small tier). The M2 Postgres tier reads its
/// authoritative `band_registry` row instead and treats this as advisory.
#[must_use]
pub fn counters_from_status(prior: Option<&BandStatus>) -> CumulativeCounters {
    CumulativeCounters {
        carves: prior.and_then(|s| s.carves_total).unwrap_or(0),
        deferrals: prior.and_then(|s| s.deferrals_total).unwrap_or(0),
        conflicts: prior.and_then(|s| s.conflicts_total).unwrap_or(0),
    }
}

/// The backoff for `TickReceipt::CapabilityMissing` — deliberately far past
/// every other class's cooldown (`ClassCooldowns::restart_requiring` tops out
/// at minutes). A StorageClass gap is a STRUCTURAL fact that does not clear on
/// its own; re-checking every few seconds forever (the never-silently-stuck
/// escalation's whole point is to STOP hammering a terminal that needs
/// operator action, not a transient condition) wastes API calls and etcd
/// writes for no gain. One hour still re-observes promptly enough that fixing
/// the StorageClass (or migrating the PVC) converges within a session.
const CAPABILITY_MISSING_REQUEUE_SECS: u64 = 3600;

/// The requeue interval for the NEXT tick, keyed on what just happened — the
/// real-time corollary of the restart-cost axis. A permitted carve (golden under
/// the default policy) or a shadow requeues at the fast restart-free cadence
/// (track the band near-real-time); a deferred ceiling crossing backs off by the
/// blocked class (damp the crossing); everything else takes the mid window. The
/// band's own `cooldownSeconds` still bounds change frequency — this only
/// controls how often breathe LOOKS.
#[must_use]
pub fn next_requeue(receipt: &TickReceipt, cooldowns: &ClassCooldowns) -> Duration {
    let secs = match receipt {
        // a carve that PASSED the policy gate is golden-cadence under the default;
        // a shadow likewise looks fast (it is observing the live band). A dormant
        // (empty) target re-checks at the golden cadence too, so a pod that appears
        // (a runner starting a build) is picked up within one fast tick.
        TickReceipt::Applied { .. } | TickReceipt::ShadowWouldApply { .. } | TickReceipt::Dormant => {
            cooldowns.restart_free
        }
        // a refused crossing: back off by exactly the blocked class.
        TickReceipt::DeferredWouldRestart { class, .. } => cooldowns.for_class(*class),
        // warming up OR throttled/restarting: re-look at the FAST cadence. A warming-up
        // workload needs its boot spike sampled promptly (folds into the peak so it can
        // carve the moment warmup elapses); a throttled/restarting workload is the one
        // we most want to track closely (it is being starved RIGHT NOW) so we observe
        // the throttle clearing + grow it out of the cap promptly. Never the slow window.
        TickReceipt::Warmup { .. } | TickReceipt::Throttled { .. } => cooldowns.restart_free,
        // TERMINAL, structural, never-clears-on-its-own: back off FAR past every
        // other class (see the const doc) — this is the never-silently-stuck
        // escalation for a StorageClass gap, not a transient condition.
        TickReceipt::CapabilityMissing { .. } => return Duration::from_secs(CAPABILITY_MISSING_REQUEUE_SECS),
        // non-mutating / transient: the mid window.
        TickReceipt::Observed { .. }
        | TickReceipt::Cooldown
        | TickReceipt::Conflict { .. }
        | TickReceipt::MetricUnrepresentable { .. }
        | TickReceipt::Stale { .. }
        | TickReceipt::Error { .. } => cooldowns.restart_conditional,
    };
    Duration::from_secs(secs)
}

/// The label set identifying one band's Prometheus series.
pub struct BandLabels {
    pub dim: String,
    pub namespace: String,
    pub name: String,
}

/// Record this tick's Prometheus series — the over-time view of breathe's behavior
/// (`util` oscillating inside the band, the carved limit, carve/defer/conflict
/// rates). The scrape endpoint is installed by each binary's exporter; this records
/// into the global recorder. Driven by the SAME `TickOutcome` as `status_for` /
/// `event_for`, so status, events, and metrics never disagree about a tick.
#[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
pub fn metrics_for(l: &BandLabels, outcome: &TickOutcome, cfg: &BandConfig, cooldown_remaining_s: i64) {
    let base = || {
        vec![
            Label::new("dim", l.dim.clone()),
            Label::new("namespace", l.namespace.clone()),
            Label::new("name", l.name.clone()),
        ]
    };
    // band-shape gauges — the green band the operator watches util oscillate inside.
    gauge!("breathe_band_setpoint_ratio", base()).set(cfg.setpoint);
    gauge!("breathe_band_grow_above_ratio", base()).set(cfg.grow_above);
    gauge!("breathe_band_shrink_below_ratio", base()).set(cfg.shrink_below);
    gauge!("breathe_band_floor", base()).set(cfg.floor_bytes as f64);
    gauge!("breathe_band_ceiling", base()).set(cfg.ceiling_bytes as f64);
    gauge!("breathe_band_dry_run", base()).set(f64::from(u8::from(outcome.dry_run())));
    gauge!("breathe_band_cooldown_remaining_seconds", base()).set(cooldown_remaining_s as f64);

    // observed gauges — the live signal driving the loop.
    if let Some(obs) = &outcome.observed {
        gauge!("breathe_band_used", base()).set(obs.used as f64);
        gauge!("breathe_band_capacity", base()).set(obs.capacity() as f64);
        gauge!("breathe_band_staleness_seconds", base()).set(obs.staleness_secs as f64);
        if let Some(u) = util_of(obs) {
            gauge!("breathe_band_util_ratio", base()).set(u);
        }
    }

    // the carved limit, tracked over time.
    let limit = match &outcome.receipt {
        TickReceipt::Applied { to, .. } => Some(*to),
        TickReceipt::ShadowWouldApply { from, .. } | TickReceipt::DeferredWouldRestart { from, .. } => Some(*from),
        _ => outcome.observed.as_ref().map(Observation::capacity),
    };
    if let Some(v) = limit {
        gauge!("breathe_band_current_limit", base()).set(v as f64);
    }

    // counters — one reconcile per tick + the outcome class.
    counter!("breathe_reconciles_total", base()).increment(1);
    match &outcome.receipt {
        TickReceipt::Applied { from, to, class } => {
            let mut ls = base();
            ls.push(Label::new("dir", if to > from { "grow" } else { "shrink" }));
            ls.push(Label::new("class", format!("{class:?}")));
            counter!("breathe_carves_total", ls).increment(1);
        }
        TickReceipt::DeferredWouldRestart { class, .. } => {
            let mut ls = base();
            ls.push(Label::new("class", format!("{class:?}")));
            counter!("breathe_deferred_total", ls).increment(1);
        }
        TickReceipt::Conflict { .. } => counter!("breathe_conflicts_total", base()).increment(1),
        TickReceipt::Stale { .. } => counter!("breathe_stale_total", base()).increment(1),
        TickReceipt::Error { .. } => counter!("breathe_errors_total", base()).increment(1),
        _ => {}
    }
}

/// The ephemeral-env context for a band's namespace (Dev Loop M3) — the
/// `EphemeralEnvId` + the namespace `Densa`'s cost-remaining (the cost-guard).
/// Read-only: a controller fetches it (namespace label + the namespace Densa's
/// status) and folds it into the band status via [`apply_env_context`]. Both
/// absent ⇒ the namespace is not an ephemeral env / has no Densa (the rio default
/// — zero behavior change there).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnvContext {
    pub env_id: Option<String>,
    pub cost_remaining_cents: Option<i64>,
}

impl EnvContext {
    /// Is there anything to surface? (skip the patch entirely when empty.)
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.env_id.is_none() && self.cost_remaining_cents.is_none()
    }
}

/// Fold the ephemeral-env context into a band status (read-only surfacing). Only
/// overwrites a field the context actually carries, so a band in a non-ephemeral
/// namespace keeps `None` (no churn — the determinism discipline).
pub fn apply_env_context(status: &mut BandStatus, ctx: &EnvContext) {
    if ctx.env_id.is_some() {
        status.observed_env_id = ctx.env_id.clone();
    }
    if ctx.cost_remaining_cents.is_some() {
        status.observed_cost_remaining_cents = ctx.cost_remaining_cents;
    }
}

/// The status for a SUSPENDED band — frozen (the controller skips observe/plan/act;
/// the limit is left exactly as-is). Resume by setting `spec.suspend:false`.
#[must_use]
pub fn suspended_status(prior: Option<&BandStatus>) -> BandStatus {
    // Preserve the transition time of an existing Ready=False condition so a band
    // that STAYS suspended yields a byte-identical status tick after tick (no
    // churn); only stamp `now` on the first transition into suspension.
    let last_transition_time = prior
        .and_then(|p| p.conditions.iter().find(|c| c.type_ == "Ready" && c.status == "False"))
        .map_or_else(now_rfc3339, |c| c.last_transition_time.clone());
    let mut s = BandStatus::default();
    // The ever-governed latch survives suspension: suspending a band does not
    // un-prove that it once governed pods. `prior` is already in hand here for
    // last_transition_time, so this costs nothing.
    carry_latch(&mut s, prior);
    s.phase = Some("Suspended".into());
    s.last_decision = Some("suspended — set spec.suspend:false to resume".into());
    s.conditions = vec![Condition {
        type_: "Ready".into(),
        status: "False".into(),
        reason: "Suspended".into(),
        message: "band is suspended (spec.suspend:true)".into(),
        last_transition_time,
        observed_generation: None,
    }];
    s
}

/// A short typed error status (band-config parse failures, enrollment gaps).
#[must_use]
/// `prior` exists ONLY to carry the sticky `first_observed_epoch` latch across
/// an error tick. See `carry_latch` — an error must never un-prove a band that
/// has genuinely governed pods.
pub fn error_status(prior: Option<&BandStatus>, decision: impl Into<String>) -> BandStatus {
    let mut s = BandStatus::default();
    s.phase = Some("Error".into());
    s.last_decision = Some(decision.into());
    carry_latch(&mut s, prior);
    s
}

/// Carry the sticky ever-governed latch from the prior status onto a freshly
/// built one.
///
/// WHY THIS IS EXPLICIT RATHER THAN LEFT TO THE PATCH. Every status producer
/// starts from `BandStatus::default()`, so `first_observed_epoch` is `None`
/// unless something copies it. Until 2026-07-28 `error_status` and
/// `suspended_status` did not, and the latch survived those ticks only by
/// ACCIDENT: the field carries `skip_serializing_if = "Option::is_none"`, so a
/// `None` is omitted from the JSON entirely and `Patch::Merge` leaves the
/// stored value alone.
///
/// That accident holds today and is three unrelated edits away from breaking:
/// dropping the serde attribute, switching any caller to Apply/Replace, or a
/// new producer building the status a different way. None of those would fail
/// a test, because no test covered it.
///
/// TIER-HONEST about the blast radius, so this is not oversold: if it DID
/// break, a proven band would read as never-proven after an error or suspend
/// tick and be counted `unproven` — a FALSE ALARM, not a false green. The safe
/// direction. This is correctness-of-mechanism, not a live safety hole; it is
/// fixed because a guarantee that rests on an omission is not a guarantee.
pub fn carry_latch(s: &mut BandStatus, prior: Option<&BandStatus>) {
    if s.first_observed_epoch.is_none() {
        s.first_observed_epoch = prior.and_then(|p| p.first_observed_epoch);
    }
}

/// Patch a band CR's `status` subresource (merge — only the fields we set).
pub async fn patch_status<B: Band>(
    client: &Client,
    ns: &str,
    name: &str,
    status: &BandStatus,
) -> Result<(), kube::Error> {
    let api: Api<B> = Api::namespaced(client.clone(), ns);
    let patch = json!({ "status": status });
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch)).await?;
    Ok(())
}

/// [`patch_status`], but DIFF-GATED: skip the write entirely when `status` is
/// byte-identical to `prior` (task #220). Every reconcile — default
/// `BREATHE_REQUEUE_SECONDS=60`, more often under NATS-reactive triggering —
/// otherwise wrote to etcd via the apiserver even when nothing about the band
/// changed. `BandStatus`'s `PartialEq` makes this a real structural
/// comparison (mirrors the field-by-field DIFF-GATE `reconcile_overview`
/// already hand-rolls in `breathe-controller/src/main.rs` for
/// `OverviewStatus`, generalized here via the derive since `BandStatus`
/// carries far more fields).
///
/// Returns whether a patch was actually issued — the test seam that lets a
/// caller (or a test) prove zero-vs-one API calls without inspecting the
/// client's transport directly.
pub async fn patch_status_if_changed<B: Band>(
    client: &Client,
    ns: &str,
    name: &str,
    prior: Option<&BandStatus>,
    status: &BandStatus,
) -> Result<bool, kube::Error> {
    if prior == Some(status) {
        return Ok(false);
    }
    patch_status::<B>(client, ns, name, status).await?;
    Ok(true)
}

// ═════════════════════ HORIZONTAL (ReplicaBand) status mapping ═════════════════════
//
// The ReplicaBand does NOT produce a `TickOutcome` (that keystone models the
// vertical (used,capacity) band); its typed tick is a `ReplicaReceipt`, and this
// section is its `status_for`/`event_for`/`entry_for`/`next_requeue` peer. It reuses
// the SAME condition semantics (`upsert_condition`) + the SAME phase strings
// (Growing/Shrinking/ShadowWouldApply/DeferredWouldRestart/Cooldown/Holding/AtFloor/
// AtCeiling/Stale/Conflict) so `kubectl wait --for=condition=Ready` AND the
// `ShadowConfirmEffect` confirm gate (which reads status.conditions Ready∧¬Stale∧
// ¬Conflict) work IDENTICALLY for a ReplicaBand — the whole point of riding the same
// gate. Adding it here (not in the controller) keeps status mapping the runtime's one
// job, so the brain can never drift in how a horizontal decision is reported.

/// What ONE horizontal (replica) tick did — the `ReplicaBand` peer of
/// [`TickReceipt`]. The controller folds the pure [`ReplicaTickPlan`] + the actuator
/// result into this via [`ReplicaReceipt::resolve`] (or builds `Stale` directly when
/// the driving sample is too old); [`replica_status_for`] renders it to a
/// [`BandStatus`]. `from`/`to` are replica counts.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReplicaReceipt {
    /// A carve was APPLIED to `.spec.replicas` (`from -> to`). `to > from` is a
    /// RestartFree scale-OUT; `to < from` a RestartRequiring scale-IN.
    Applied { from: u32, to: u32 },
    /// SHADOW: the band would carve (`from -> to`) but nothing was written (the
    /// effective dry-run — `ShadowConfirmEffect` before its confirm window, or
    /// explicit `mode: shadow`).
    ShadowWouldApply { from: u32, to: u32 },
    /// A scale-IN the band law wanted but the `DisruptionPolicy` REFUSED (a
    /// pod-shedding crossing under `restartFreeOnly`) — reported, never written.
    DeferredScaleIn { from: u32, to: u32 },
    /// A carve is due but HELD in the post-carve cooldown window.
    Cooldown { from: u32, to: u32 },
    /// RESTING: within band / at the HA floor / at the ceiling — nothing to do.
    Observed { decision: ReplicaDecision },
    /// The driving metric sample was too STALE to act on — held.
    Stale { staleness_secs: u64, current: u32 },
    /// Yielded `.spec.replicas` to a competing writer (KEDA/HPA) on a 409 — the
    /// cooperative-yield of the no-`.force()` SSA (the horizontal single-writer guard).
    Conflict { current: u32 },
}

impl ReplicaReceipt {
    /// Fold the pure [`ReplicaTickPlan`] + the actuator/observe results into a typed
    /// receipt. Exhaustive, no panic. Precedence: conflict ▸ applied ▸ deferred ▸
    /// resting ▸ shadow ▸ cooldown. (`Stale` is built by the caller BEFORE planning,
    /// so it never reaches here.)
    #[must_use]
    pub fn resolve(plan: &ReplicaTickPlan, applied: bool, conflict: bool, dry_run: bool, in_cooldown: bool) -> Self {
        let d = plan.decision;
        let (from, to) = (d.current(), d.target());
        if conflict {
            return Self::Conflict { current: from };
        }
        if applied {
            return Self::Applied { from, to };
        }
        if plan.deferred {
            return Self::DeferredScaleIn { from, to };
        }
        if !d.is_carve() {
            return Self::Observed { decision: d };
        }
        // a carve that was neither applied nor deferred was withheld by the gate.
        if dry_run {
            Self::ShadowWouldApply { from, to }
        } else if in_cooldown {
            Self::Cooldown { from, to }
        } else {
            // no remaining reason (defensive — not expected once actuation ran).
            Self::Observed { decision: d }
        }
    }

    /// A short, stable tag — the `decision_log` row's `receipt_kind`.
    #[must_use]
    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::Applied { .. } => "Applied",
            Self::ShadowWouldApply { .. } => "ShadowWouldApply",
            Self::DeferredScaleIn { .. } => "DeferredScaleIn",
            Self::Cooldown { .. } => "Cooldown",
            Self::Observed { .. } => "Observed",
            Self::Stale { .. } => "Stale",
            Self::Conflict { .. } => "Conflict",
        }
    }
}

/// Render a horizontal [`ReplicaReceipt`] to a [`BandStatus`] — the `ReplicaBand`
/// peer of [`status_for`]. `metric_ratio` is `currentMetric/targetMetric` (the
/// headline "how far from setpoint", surfaced as `lastUtil`); `staleness_secs` is the
/// driving sample age; `dry_run`/`policy` are the effective tick mode. Conditions +
/// counters + cooldown-remaining + history are built exactly as the vertical path.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn replica_status_for(
    receipt: &ReplicaReceipt,
    metric_ratio: f64,
    staleness_secs: u64,
    dry_run: bool,
    policy: DisruptionPolicy,
    prior: Option<&BandStatus>,
    cooldown_seconds: u64,
    generation: Option<i64>,
    counters: CumulativeCounters,
) -> BandStatus {
    let mut s = BandStatus::default();
    s.effective_dry_run = Some(dry_run);
    s.effective_policy = Some(policy_str(policy));
    s.freshness_seconds = Some(staleness_secs as i64);
    if metric_ratio.is_finite() {
        // the metric ratio IS the horizontal "utilization" (1.0 == on setpoint).
        s.observed_util = Some(metric_ratio);
        s.last_util = Some(format!("{:.0}%", metric_ratio * 100.0));
    }

    // Observability booleans — the SAME five the vertical `conditions_for` derives.
    // Every rendered receipt is observable (Ready=True); a pre-observe error takes
    // the `error_status` path, not this one.
    let mut converged = false;
    let mut throttled = false;
    let mut stale_c = false;
    let mut conflict_c = false;

    match receipt {
        ReplicaReceipt::Applied { from, to } => {
            let growing = to > from;
            s.phase = Some(if growing { "Growing" } else { "Shrinking" }.into());
            s.current_limit = Some(to.to_string());
            s.observed_used = Some(i64::from(*to));
            s.last_decision =
                Some(format!("{from} -> {to} replicas ({})", if growing { "scale-out" } else { "scale-in" }));
            s.last_action_class = Some(if growing { "RestartFree" } else { "RestartRequiring" }.into());
            s.last_change_epoch = Some(now_secs());
        }
        ReplicaReceipt::ShadowWouldApply { from, to } => {
            s.phase = Some("ShadowWouldApply".into());
            s.current_limit = Some(from.to_string()); // shadow mutates nothing
            s.observed_used = Some(i64::from(*from));
            s.last_decision = Some(format!("shadow: would scale {from} -> {to} replicas (dryRun — nothing written)"));
        }
        ReplicaReceipt::DeferredScaleIn { from, to } => {
            s.phase = Some("DeferredWouldRestart".into());
            s.current_limit = Some(from.to_string()); // the crossing was refused
            s.observed_used = Some(i64::from(*from));
            s.last_decision = Some(format!(
                "{from} -> {to} deferred: a scale-in sheds a pod (RestartRequiring) blocked by DisruptionPolicy (set allowRestart to permit)"
            ));
            s.last_action_class = Some("RestartRequiring".into());
            throttled = true;
        }
        ReplicaReceipt::Cooldown { from, to } => {
            s.phase = Some("Cooldown".into());
            s.current_limit = Some(from.to_string());
            s.observed_used = Some(i64::from(*from));
            s.last_decision = Some(format!("cooling down after a carve (would scale {from} -> {to})"));
            throttled = true;
        }
        ReplicaReceipt::Observed { decision } => {
            let current = decision.current();
            let (phase, note) = match decision {
                ReplicaDecision::Hold { .. } => ("Holding", format!("within band — held at {current} replicas")),
                ReplicaDecision::AtFloor { .. } => ("AtFloor", format!("at HA floor {current} — no safe scale-in")),
                ReplicaDecision::AtCeiling { .. } => ("AtCeiling", format!("at ceiling {current} — would scale out")),
                // Persistent (stateful): a scale-in is HELD pending drain/rebalance of
                // the ordinal's data — the reactive shrink is not written directly.
                ReplicaDecision::HeldForRebalance { would_shrink_to, .. } => (
                    "PendingRebalance",
                    format!("scale-in to {would_shrink_to} held — drain/rebalance the ordinal's data first (stateful)"),
                ),
                // a carve routed through Observed only defensively (resolve covers the
                // real cases above); keep exhaustive, never panic.
                other => ("Observed", other.to_string()),
            };
            s.phase = Some(phase.into());
            s.last_decision = Some(note);
            s.current_limit = Some(current.to_string());
            s.observed_used = Some(i64::from(current));
            // a resting horizontal decision is at rest → Converged.
            converged = matches!(
                decision,
                ReplicaDecision::Hold { .. } | ReplicaDecision::AtFloor { .. } | ReplicaDecision::AtCeiling { .. }
            );
        }
        ReplicaReceipt::Stale { staleness_secs, current } => {
            s.phase = Some("Stale".into());
            s.current_limit = Some(current.to_string());
            s.observed_used = Some(i64::from(*current));
            s.last_decision = Some(format!("metric {staleness_secs}s stale — held (never scale on a stale sample)"));
            stale_c = true;
            throttled = true;
        }
        ReplicaReceipt::Conflict { current } => {
            s.phase = Some("Conflict".into());
            s.current_limit = Some(current.to_string());
            s.observed_used = Some(i64::from(*current));
            s.last_decision =
                Some("yielded .spec.replicas to a competing writer (KEDA/HPA) — will re-observe".into());
            conflict_c = true;
        }
    }

    // ── conditions: the SAME five the vertical path derives (so the confirm gate +
    //    `kubectl wait` behave identically). ────────────────────────────────────
    let now = now_rfc3339();
    let prior_conds = prior.map_or(&[][..], |p| p.conditions.as_slice());
    let mut conds = Vec::with_capacity(6);
    // Ready/Observable carry the SAME split as the vertical path (`conditions_for`)
    // — both `true` here because the replica path only reaches this point after a
    // clean observe. Emitting `Observable` unconditionally matters anyway: a band
    // whose condition array LACKS it would be read as "not unobservable" by
    // `health_verdict`, so an absent condition must never be the quiet default.
    upsert_condition(&mut conds, prior_conds, &now, "Ready", true, "Accepted", "enrolled, config parses, targetRef resolved — the controller owns this band", generation);
    upsert_condition(&mut conds, prior_conds, &now, "Observable", true, "MetricObservable", "a metric/limit is available to reason on", generation);
    upsert_condition(&mut conds, prior_conds, &now, "Converged", converged,
        if converged { "WithinBand" } else { "Adjusting" },
        if converged { "replica count is within the deadband" } else { "scaling/waiting toward the setpoint" }, generation);
    upsert_condition(&mut conds, prior_conds, &now, "Throttled", throttled,
        if throttled { "Throttled" } else { "Free" }, "in cooldown / deferred scale-in / stale metric", generation);
    upsert_condition(&mut conds, prior_conds, &now, "Stale", stale_c,
        if stale_c { "StaleMetric" } else { "Fresh" }, "driving metric sample age vs maxStaleness", generation);
    upsert_condition(&mut conds, prior_conds, &now, "Conflict", conflict_c,
        if conflict_c { "FieldOwnedElsewhere" } else { "SoleWriter" }, "single-writer guard", generation);
    s.conditions = conds;

    // ── counters (projection), cooldown remaining, observedGeneration, history —
    //    identical tail to `status_for`. ─────────────────────────────────────────
    s.carves_total = Some(counters.carves);
    s.deferrals_total = Some(counters.deferrals);
    s.conflicts_total = Some(counters.conflicts);
    let last_carve = s.last_change_epoch.or_else(|| prior.and_then(|p| p.last_change_epoch)).unwrap_or(0);
    s.cooldown_remaining_seconds = Some((last_carve + cooldown_seconds as i64 - now_secs()).max(0));
    s.observed_generation = generation;

    const HISTORY_MAX: usize = 16;
    let phase_changed = prior.and_then(|p| p.phase.as_deref()) != s.phase.as_deref();
    let carved = matches!(receipt, ReplicaReceipt::Applied { .. });
    let mut history = prior.map_or_else(Vec::new, |p| p.history.clone());
    if carved || phase_changed {
        history.push(TrendSample {
            time: now_rfc3339(),
            util: s.observed_util,
            limit: s.current_limit.as_deref().and_then(|l| l.parse().ok()),
            phase: s.phase.clone().unwrap_or_default(),
            decision: s.last_decision.clone(),
        });
        if history.len() > HISTORY_MAX {
            history.drain(0..history.len() - HISTORY_MAX);
        }
    }
    s.history = history;
    s
}

/// Classify a horizontal receipt into a [`DecisionEntry`] — the `ReplicaBand` peer of
/// [`entry_for`], so the cumulative carve/deferral/conflict fold is driven the SAME
/// way for the horizontal path. Applied ⇒ carve, `DeferredScaleIn` ⇒ deferral,
/// Conflict ⇒ conflict, else no count.
#[must_use]
pub fn replica_entry_for(receipt: &ReplicaReceipt, dry_run: bool) -> DecisionEntry {
    let (from_limit, to_limit) = match receipt {
        ReplicaReceipt::Applied { from, to }
        | ReplicaReceipt::ShadowWouldApply { from, to }
        | ReplicaReceipt::DeferredScaleIn { from, to }
        | ReplicaReceipt::Cooldown { from, to } => (Some(u64::from(*from)), Some(u64::from(*to))),
        _ => (None, None),
    };
    let class = match receipt {
        ReplicaReceipt::Applied { .. } => CounterClass::Carve,
        ReplicaReceipt::DeferredScaleIn { .. } => CounterClass::Deferral,
        ReplicaReceipt::Conflict { .. } => CounterClass::Conflict,
        _ => CounterClass::NoCount,
    };
    DecisionEntry { receipt_kind: receipt.kind_str().to_string(), class, from_limit, to_limit, dry_run }
}

/// The next-tick requeue for the horizontal path, keyed on the receipt — the peer of
/// [`next_requeue`]. A carve/shadow re-ticks at the fast RestartFree cadence (track
/// the live band); a deferred scale-in backs off by the RestartRequiring class;
/// everything else takes the mid window.
#[must_use]
pub fn replica_next_requeue(receipt: &ReplicaReceipt, cooldowns: &ClassCooldowns) -> Duration {
    let secs = match receipt {
        ReplicaReceipt::Applied { .. } | ReplicaReceipt::ShadowWouldApply { .. } => cooldowns.restart_free,
        ReplicaReceipt::DeferredScaleIn { .. } => cooldowns.restart_requiring,
        ReplicaReceipt::Observed { .. }
        | ReplicaReceipt::Cooldown { .. }
        | ReplicaReceipt::Stale { .. }
        | ReplicaReceipt::Conflict { .. } => cooldowns.restart_conditional,
    };
    Duration::from_secs(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_env_context_surfaces_only_present_fields() {
        let mut st = BandStatus::default();
        // empty context ⇒ band keeps None (the rio / non-ephemeral default)
        apply_env_context(&mut st, &EnvContext::default());
        assert_eq!(st.observed_env_id, None);
        assert_eq!(st.observed_cost_remaining_cents, None);
        assert!(EnvContext::default().is_empty());

        // env id only
        apply_env_context(&mut st, &EnvContext { env_id: Some("deadbeef".into()), cost_remaining_cents: None });
        assert_eq!(st.observed_env_id.as_deref(), Some("deadbeef"));
        assert_eq!(st.observed_cost_remaining_cents, None);

        // cost remaining (incl. negative = over budget)
        apply_env_context(&mut st, &EnvContext { env_id: None, cost_remaining_cents: Some(-250) });
        assert_eq!(st.observed_env_id.as_deref(), Some("deadbeef"), "env id preserved");
        assert_eq!(st.observed_cost_remaining_cents, Some(-250));
        assert!(!EnvContext { env_id: None, cost_remaining_cents: Some(-250) }.is_empty());
    }

    /// Wrap a bare receipt in a minimal TickOutcome (no observation; the status
    /// per-arm fields under test don't need one).
    fn out(receipt: TickReceipt) -> TickOutcome {
        TickOutcome { receipt, observed: None, policy: DisruptionPolicy::RestartFreeOnly, gate: test_live_gate() }
    }

    /// Build a status from an outcome with the counters the DecisionLog would
    /// produce from a zero prior — i.e. `fold(ZERO, entry_for(outcome))`. Keeps
    /// these per-receipt tests asserting the counter values they always did
    /// (Applied ⇒ carves 1, Conflict ⇒ conflicts 1, …) now that `status_for`
    /// consumes the count instead of computing it.
    fn status_of(o: &TickOutcome) -> BandStatus {
        status_for(o, None, 0, None, CumulativeCounters::ZERO.fold(&entry_for(o)))
    }

    /// **The shadow REASON reaches the operator, and "dryRun" never does.**
    ///
    /// Every surface used to render a shadowed tick as "dryRun — nothing written".
    /// That string was false for every k8s band kind after `76924b0` retired the
    /// field, and it could not distinguish an operator's authored hold from a band
    /// that FELL into shadow — the exact confusion that left six camelot-eks bands
    /// looking deliberately parked when they were actually broken.
    #[test]
    fn a_shadowed_status_names_why_and_never_says_dry_run() {
        use breathe_provider::ShadowReason as R;
        for (reason, must_contain) in [
            (R::ModeShadow, "authored"),
            (R::NotReady, "NOT AUTHORED"),
            (R::Stale, "NOT AUTHORED"),
            (R::Conflict, "NOT AUTHORED"),
            (R::ConfirmPending { held_secs: 400, need_secs: 1800 }, "400s of 1800s"),
            (R::Frozen, "freeze"),
            (R::IntentMalformed, "naming no author"),
        ] {
            let s = status_of(&out(TickReceipt::ShadowWouldApply { from: 100, to: 250, reason }));
            assert_eq!(s.phase.as_deref(), Some("ShadowWouldApply"));
            assert_eq!(s.current_limit.as_deref(), Some("100"), "shadow mutates nothing");
            let note = s.last_decision.unwrap_or_default();
            assert!(note.contains(must_contain), "{reason:?} must explain itself, got {note:?}");
            assert!(!note.contains("dryRun") && !note.contains("dry-run"), "the retired field is never the reason");

            let (_, ev_reason, msg) = event_for(&TickReceipt::ShadowWouldApply { from: 100, to: 250, reason }).unwrap();
            assert_eq!(ev_reason, "ShadowWouldApply");
            assert!(msg.contains(must_contain), "the k8s Event carries the same reason");
        }
    }

    /// An idle band whose reclaim is withheld by policy reports `ReclaimWithheld`
    /// with the slack it declined — NOT the `AtFloor` it is nowhere near — and is
    /// still counted as CONVERGED (it decided; the decision was "leave it").
    #[test]
    fn withheld_reclaim_is_reported_honestly_and_counts_as_converged() {
        let o = out(TickReceipt::Observed { decision: Decision::ReclaimWithheld { current: 2048, reclaimable: 512 } });
        let s = status_of(&o);
        assert_eq!(s.phase.as_deref(), Some("ReclaimWithheld"));
        let note = s.last_decision.unwrap_or_default();
        assert!(note.contains("512"), "the amount NOT taken is named, got {note:?}");
        assert!(!note.contains("floor"), "and it is not dressed up as a floor");
        let converged = conditions_for(&o, &[], None)
            .into_iter()
            .find(|c| c.type_ == "Converged")
            .expect("Converged condition");
        assert_eq!(converged.status, "True", "a withheld reclaim is a RESTING state");
    }

    /// **The 2026-07-26 deploy hazard, pinned.** `coredns-cpu` and
    /// `ebs-csi-controller-cpu` on camelot-eks target workloads that declare NO
    /// cpu limit — precisely the case the bound-introduction guard exists to
    /// respect. `Decision::NoLimit` used to drive `Ready=False`, whose
    /// [`STUCK_AFTER_SECS`] timer then classified both bands
    /// [`HealthVerdict::Stuck`] **permanently**, with a `BandStuck` Warning, for
    /// doing exactly the right thing.
    ///
    /// The honest classification is `Supported=False` ⇒
    /// [`HealthVerdict::Unsupported`] — the condition already documented as
    /// "will NEVER converge without operator action" and already checked ahead of
    /// (and in priority over) `Stuck`.
    ///
    /// Note the two assertions this test deliberately makes TOGETHER: the verdict
    /// must be `Unsupported` **regardless of the gate**. A shadow-only fix would
    /// leave a live band on a limitless target Stuck forever — the same bug one
    /// `writeIntent` away.
    #[test]
    fn no_limit_is_unsupported_not_stuck_in_either_gate() {
        let o = out(TickReceipt::Observed { decision: Decision::NoLimit });
        let conds = conditions_for(&o, &[], None);
        let find = |t: &str| conds.iter().find(|c| c.type_ == t).expect("condition").clone();

        // READY stays TRUE: the guard fires only after a clean observe.
        assert_eq!(find("Ready").status, "True", "a NoLimit tick observed the target fine");
        // SUPPORTED carries the verdict, with a reason naming the actual lever.
        let supported = find("Supported");
        assert_eq!(supported.status, "False");
        assert_eq!(supported.reason, "NoBoundDeclared");
        assert!(supported.message.contains("boundIntroduction"), "the message names the lever: {:?}", supported.message);
        // …and does NOT claim a StorageClass problem, which this is not.
        assert!(!supported.message.contains("StorageClass"), "wrong cause: {:?}", supported.message);

        // The verdict, far past the stuck threshold, in BOTH gate modes.
        let far_future = "2999-01-01T00:00:00Z";
        for dry_run in [true, false] {
            match health_verdict(&conds, far_future, STUCK_AFTER_SECS, dry_run) {
                HealthVerdict::Unsupported { reason } => {
                    assert!(reason.contains("boundIntroduction"), "reason: {reason:?}");
                }
                other => panic!("dry_run={dry_run}: expected Unsupported, got {other:?}"),
            }
        }
    }

    /// The sibling arm must not regress: a StorageClass that cannot expand is
    /// STILL `Unsupported`, and still says so in StorageClass terms.
    #[test]
    fn capability_missing_keeps_its_own_unsupported_reason() {
        let o = out(TickReceipt::CapabilityMissing {
            volume_expansion: false,
            per_volume_metrics: true,
            provisioner: "rancher.io/local-path".into(),
        });
        let conds = conditions_for(&o, &[], None);
        let supported = conds.iter().find(|c| c.type_ == "Supported").expect("Supported");
        assert_eq!(supported.status, "False");
        assert_eq!(supported.reason, "StorageClassUnsupported");
        assert!(supported.message.contains("StorageClass"));
        // …and this arm keeps "there is genuinely nothing to reason on" — which is
        // `Observable=False`. It asserted that on `Ready` until 2026-08-07, because
        // `Ready` WAS observability; the comment was already describing the right
        // fact, the condition was just the wrong one to carry it.
        assert_eq!(conds.iter().find(|c| c.type_ == "Observable").expect("Observable").status, "False");
        // Ready stays TRUE: the band is accepted and owned, and "can never converge
        // without operator action" is `Supported`'s job — exactly as the 2026-07-26
        // `NoLimit` fix established. Those two arms disagreed on Ready until now.
        assert_eq!(conds.iter().find(|c| c.type_ == "Ready").expect("Ready").status, "True");
    }

    #[test]
    fn events_are_typed_and_transition_gated() {
        use breathe_provider::DisruptionClass::{RestartFree, RestartRequiring};
        // a carve is a Normal Grew/Shrank event…
        let (k, reason, _) = event_for(&TickReceipt::Applied { from: 1, to: 2, class: RestartFree }).unwrap();
        assert_eq!((k, reason), (EventKind::Normal, "Grew"));
        // …and ALWAYS emits, even when the phase didn't change (each carve is an event).
        assert!(should_emit_event(&TickReceipt::Applied { from: 1, to: 2, class: RestartFree }, Some("Growing"), Some("Growing")));
        // a deferred crossing is a Warning.
        let (k, reason, _) = event_for(&TickReceipt::DeferredWouldRestart { from: 1, to: 2, class: RestartRequiring }).unwrap();
        assert_eq!((k, reason), (EventKind::Warning, "DeferredCrossing"));
        // a resting Hold emits NOTHING; Cooldown likewise.
        assert!(event_for(&TickReceipt::Observed { decision: Decision::Hold }).is_none());
        assert!(event_for(&TickReceipt::Cooldown).is_none());
        // a non-carve at the SAME phase is suppressed; a phase CHANGE emits.
        let atfloor = TickReceipt::Observed { decision: Decision::NoSafeShrink { current: 9 } };
        assert!(!should_emit_event(&atfloor, Some("AtFloor"), Some("AtFloor")));
        assert!(should_emit_event(&atfloor, Some("AtFloor"), Some("Holding")));
    }

    #[test]
    fn applied_growth_vs_shrink_is_reported_directionally() {
        use breathe_provider::DisruptionClass::RestartFree;
        let grow = status_of(&out(TickReceipt::Applied { from: 100, to: 200, class: RestartFree }));
        assert_eq!(grow.phase.as_deref(), Some("Growing"));
        assert_eq!(grow.current_limit.as_deref(), Some("200"));
        assert_eq!(grow.carves_total, Some(1));
        let shrink = status_of(&out(TickReceipt::Applied { from: 200, to: 100, class: RestartFree }));
        assert_eq!(shrink.phase.as_deref(), Some("Shrinking"));
    }

    #[test]
    fn shadow_reports_what_would_have_happened_without_changing_the_limit() {
        let s = status_of(&out(TickReceipt::ShadowWouldApply { from: 100, to: 250, reason: breathe_provider::ShadowReason::ModeShadow }));
        assert_eq!(s.phase.as_deref(), Some("ShadowWouldApply"));
        // the reported current limit is the UNCHANGED value — shadow mutates nothing.
        assert_eq!(s.current_limit.as_deref(), Some("100"));
        assert!(s.last_decision.as_deref().unwrap().contains("250"));
    }

    #[test]
    fn conflict_records_the_yielded_to_manager() {
        let s = status_of(&out(TickReceipt::Conflict { manager: "helm".into() }));
        assert_eq!(s.conflicts_total, Some(1));
        assert_eq!(s.phase.as_deref(), Some("Conflict"));
        assert_eq!(s.conflict_manager.as_deref(), Some("helm"));
    }

    #[test]
    fn capability_missing_maps_to_the_unsupported_phase_and_a_false_supported_condition() {
        // the fail-fast fix's CRD-visible shape: phase="Unsupported" (never
        // Conflict/MetricUnrepresentable for the same StorageClass gap), and the
        // new Supported condition flips False — distinct from every other
        // "waiting" state, which stays True.
        let s = status_of(&out(TickReceipt::CapabilityMissing {
            volume_expansion: false,
            per_volume_metrics: false,
            provisioner: "rancher.io/local-path".into(),
        }));
        assert_eq!(s.phase.as_deref(), Some("Unsupported"));
        assert!(s.last_decision.as_deref().unwrap().contains("rancher.io/local-path"));
        let supported = s.conditions.iter().find(|c| c.type_ == "Supported").expect("Supported condition present");
        assert_eq!(supported.status, "False");
        assert_eq!(supported.reason, "StorageClassUnsupported");
        // not observable either — there is nothing further to reason on. (Asserted
        // on `Ready` until 2026-08-07, when Ready/Observable split; the comment
        // already named the right fact.)
        let observable = s
            .conditions
            .iter()
            .find(|c| c.type_ == "Observable")
            .unwrap();
        assert_eq!(observable.status, "False");
        let ready = s.conditions.iter().find(|c| c.type_ == "Ready").unwrap();
        assert_eq!(ready.status, "True");
    }

    #[test]
    fn a_normal_receipt_keeps_the_supported_condition_true() {
        use breathe_provider::DisruptionClass::RestartFree;
        let s = status_of(&out(TickReceipt::Applied { from: 1, to: 2, class: RestartFree }));
        let supported = s.conditions.iter().find(|c| c.type_ == "Supported").expect("Supported condition present");
        assert_eq!(supported.status, "True");
        assert_eq!(supported.reason, "CapabilityOk");
    }

    #[test]
    fn target_not_found_maps_to_its_own_honest_phase_not_generic_error() {
        // task #217's fix: a dangling targetRef gets a DISTINCT, legible phase +
        // a false TargetFound condition — never the generic "Error" (which reads
        // as a breathe-side fault) and never silently indistinguishable from a
        // genuine MetricsMissing/ApiTransient outage.
        use breathe_provider::ProviderError;
        let s = status_of(&out(TickReceipt::Error { error: ProviderError::TargetNotFound }));
        assert_eq!(s.phase.as_deref(), Some("TargetNotFound"));
        assert!(s.last_decision.as_deref().unwrap().contains("self-heal"));
        let target_found = s.conditions.iter().find(|c| c.type_ == "TargetFound").expect("TargetFound condition present");
        assert_eq!(target_found.status, "False");
        assert_eq!(target_found.reason, "TargetMissing");
        // Supported stays True — this is not a "can never converge" verdict, it is
        // self-healing (distinct from CapabilityMissing's structural gap).
        let supported = s.conditions.iter().find(|c| c.type_ == "Supported").unwrap();
        assert_eq!(supported.status, "True");
    }

    #[test]
    fn a_metrics_outage_gets_its_own_honest_phase_not_the_generic_error() {
        // ⚠️ THIS REVERSES A DELIBERATE EARLIER DECISION, and says so on purpose.
        // Until 2026-08-07 this test was named `..._keeps_the_generic_error_phase_...`
        // and pinned MetricsMissing to phase "Error", reasoning that "only the
        // specific TargetNotFound provider error gets the distinct treatment".
        //
        // What that missed: `Error` then became the RESTING state. Measured on
        // camelot-eks 2026-08-07 — 115 of 115 bands in phase `Error` with a null
        // message, because metrics-server was floored at 0 by the park layer. A
        // phase every band is always in carries no signal at all, so a genuinely
        // broken band would have been indistinguishable from the resting fleet.
        // That is the same "failure mode identical to the success mode" defect the
        // TargetNotFound arm was itself introduced to fix.
        use breathe_provider::ProviderError;
        let s = status_of(&out(TickReceipt::Error { error: ProviderError::MetricsMissing }));
        assert_eq!(s.phase.as_deref(), Some("Unobservable"));
        assert!(s.last_decision.as_deref().unwrap().contains("metrics pipeline"));
        // The contrast this test was originally written to hold STILL holds, and is
        // why the arm is distinct from TargetNotFound rather than merged with it:
        // the pods exist, the targetRef resolved, only the usage is unreadable.
        let target_found = s.conditions.iter().find(|c| c.type_ == "TargetFound").expect("TargetFound condition present");
        assert_eq!(target_found.status, "True");
        // And the generic arm is NOT hollowed out — a real API fault still says Error.
        let api = status_of(&out(TickReceipt::Error { error: ProviderError::ApiPermanent("boom".into()) }));
        assert_eq!(api.phase.as_deref(), Some("Error"));
    }

    /// ★ THE regression test for the 2026-08-07 defect. Goes red the instant
    /// `Ready` is re-derived from observability.
    ///
    /// Why this one assertion is worth a named test: `Ready` is the condition
    /// kstatus reads, so it is what decides whether helm-controller / Flux / Argo
    /// consider the object to have come up. With `Ready = observable`, a
    /// metrics-server outage made every band report NOT-CAME-UP, a forced
    /// `helm upgrade` of `camelot-build/sui` blocked its full 5m timeout, and
    /// Flux's `remediation.strategy: uninstall` deleted and reinstalled the
    /// release. An observability declaration must never be able to become an
    /// availability precondition.
    #[test]
    fn a_blind_band_is_accepted_not_failed() {
        use breathe_provider::ProviderError;
        let o = out(TickReceipt::Error { error: ProviderError::MetricsMissing });
        let conds = conditions_for(&o, &[], None);
        let find = |t: &str| conds.iter().find(|c| c.type_ == t).expect("condition").clone();

        assert_eq!(find("Ready").status, "True", "a band with no metric is ACCEPTED, not failed");
        assert_eq!(find("Ready").reason, "Accepted");
        // …and the real fact is carried losslessly by the condition built for it.
        assert_eq!(find("Observable").status, "False");
        assert_eq!(find("Observable").reason, "NotObservable");
        // the three acceptance proofs that JUSTIFY Ready=True are all green — this
        // is what makes "accepted" a derivation rather than an assertion.
        assert_eq!(find("TargetFound").status, "True");
        assert_eq!(find("Supported").status, "True");
        assert_eq!(find("Conflict").status, "False");
    }

    /// A blind band, far past the stuck window, is `Unobservable` — and crucially
    /// NOT `Stuck { condition: "Converged" }` either.
    ///
    /// That second half is the subtle one, and the reason `Unobservable` is an
    /// early return rather than a label swap: a band with no metric can never be
    /// converged, so a fix that only cleared `Ready` would have the identical
    /// false alarm reappear one condition to the left, 1800s later.
    #[test]
    fn a_blind_band_never_reads_as_stuck_by_either_route() {
        use breathe_provider::ProviderError;
        let o = out(TickReceipt::Error { error: ProviderError::MetricsMissing });
        let conds = conditions_for(&o, &[], None);
        assert_eq!(
            conds.iter().find(|c| c.type_ == "Converged").expect("Converged").status,
            "False",
            "precondition: a blind band really is un-converged — which is exactly why the \
             Converged fall-through needs guarding, and why this test is not redundant"
        );
        let far_future = "2999-01-01T00:00:00Z";
        for dry_run in [true, false] {
            match health_verdict(&conds, far_future, STUCK_AFTER_SECS, dry_run) {
                HealthVerdict::Unobservable { since_secs } => assert!(since_secs > 0),
                other => panic!("dry_run={dry_run}: expected Unobservable, got {other:?}"),
            }
        }
        // and the operator-facing event names the metrics pipeline, not the band —
        // sending the hunt to the right place is the whole point of the arm.
        let (kind, reason, note) =
            health_event_for(&HealthVerdict::Unobservable { since_secs: 9000 }).expect("event");
        assert_eq!(kind, EventKind::Warning);
        assert_eq!(reason, "BandUnobservable");
        assert_ne!(reason, "BandStuck");
        assert!(note.contains("metrics pipeline"), "note: {note:?}");
    }

    /// `TargetFound=False` is the ONE arm that legitimately clears `Ready`: there
    /// is no object to take ownership of, so "accepted" would be a lie. This is
    /// the positive control for the test above — proof that `Ready` can still go
    /// False, i.e. that the fix did not simply nail it to `True`.
    #[test]
    fn target_not_found_is_the_only_arm_that_clears_ready() {
        use breathe_provider::ProviderError;
        let conds = conditions_for(&out(TickReceipt::Error { error: ProviderError::TargetNotFound }), &[], None);
        let find = |t: &str| conds.iter().find(|c| c.type_ == t).expect("condition").clone();
        assert_eq!(find("Ready").status, "False");
        assert_eq!(find("Ready").reason, "TargetMissing");

        // Every other non-observable receipt keeps Ready=True. If this loop ever
        // goes red, someone has re-coupled acceptance to achievement.
        for (label, r) in [
            ("MetricsMissing", TickReceipt::Error { error: ProviderError::MetricsMissing }),
            ("ApiTransient", TickReceipt::Error { error: ProviderError::ApiTransient("x".into()) }),
            ("ApiPermanent", TickReceipt::Error { error: ProviderError::ApiPermanent("x".into()) }),
            ("MetricUnrepresentable", TickReceipt::MetricUnrepresentable { used: 9, capacity: 1 }),
            (
                "CapabilityMissing",
                TickReceipt::CapabilityMissing {
                    volume_expansion: false,
                    per_volume_metrics: false,
                    provisioner: "p".into(),
                },
            ),
            ("NoLimit", TickReceipt::Observed { decision: Decision::NoLimit }),
        ] {
            let c = conditions_for(&out(r), &[], None);
            let ready = c.iter().find(|c| c.type_ == "Ready").expect("Ready");
            assert_eq!(ready.status, "True", "{label} must stay Ready — it is accepted, merely blind or unsupported");
        }
    }

    #[test]
    fn target_not_found_event_is_a_distinct_warning_not_reconcile_error() {
        use breathe_provider::ProviderError;
        let (kind, reason, note) = event_for(&TickReceipt::Error { error: ProviderError::TargetNotFound }).unwrap();
        assert_eq!(kind, EventKind::Warning);
        assert_eq!(reason, "TargetNotFound");
        assert_ne!(reason, "ReconcileError");
        assert!(note.contains("self-heal"));
    }

    #[test]
    fn capability_missing_backs_off_far_longer_than_every_other_class() {
        use breathe_provider::ClassCooldowns;
        let cd = ClassCooldowns::default();
        let backoff = next_requeue(
            &TickReceipt::CapabilityMissing { volume_expansion: false, per_volume_metrics: false, provisioner: "x".into() },
            &cd,
        );
        assert!(backoff > Duration::from_secs(cd.restart_requiring), "must back off PAST every existing class's cooldown");
        assert_eq!(backoff, Duration::from_secs(CAPABILITY_MISSING_REQUEUE_SECS));
    }

    #[test]
    fn capability_missing_emits_a_warning_event_naming_the_provisioner() {
        let (kind, reason, note) = event_for(&TickReceipt::CapabilityMissing {
            volume_expansion: false,
            per_volume_metrics: true,
            provisioner: "rancher.io/local-path".into(),
        })
        .unwrap();
        assert_eq!(kind, EventKind::Warning);
        assert_eq!(reason, "Unsupported");
        assert!(note.contains("rancher.io/local-path"));
    }

    #[test]
    fn deferred_crossing_maps_to_a_first_class_phase() {
        use breathe_provider::DisruptionClass;
        let s = status_of(&out(TickReceipt::DeferredWouldRestart { from: 1 << 30, to: 2 << 30, class: DisruptionClass::RestartRequiring }));
        assert_eq!(s.phase.as_deref(), Some("DeferredWouldRestart"));
        // the limit is UNCHANGED — the crossing was refused.
        assert_eq!(s.current_limit.as_deref(), Some((1u64 << 30).to_string().as_str()));
        assert!(s.last_decision.as_deref().unwrap().contains("RestartRequiring"));
    }

    #[test]
    fn requeue_is_fast_for_carves_and_damped_for_crossings() {
        use breathe_provider::{ClassCooldowns, DisruptionClass};
        let cd = ClassCooldowns::default();
        assert!(cd.well_ordered());
        // a permitted carve looks again at the fast restart-free cadence.
        assert_eq!(next_requeue(&TickReceipt::Applied { from: 1, to: 2, class: DisruptionClass::RestartFree }, &cd), Duration::from_secs(cd.restart_free));
        // a refused full-roll crossing backs off the longest.
        assert_eq!(
            next_requeue(&TickReceipt::DeferredWouldRestart { from: 1, to: 2, class: DisruptionClass::RestartRequiring }, &cd),
            Duration::from_secs(cd.restart_requiring)
        );
    }

    #[test]
    fn dormant_is_a_benign_at_rest_state_not_an_error() {
        use breathe_provider::ClassCooldowns;
        // A scaled-to-zero label group (an ARC runner between builds) is DORMANT:
        // a first-class resting phase, Ready=True, Converged=True (at rest), no
        // event, and a fast re-check so a runner that appears is picked up promptly.
        let s = status_of(&out(TickReceipt::Dormant));
        assert_eq!(s.phase.as_deref(), Some("Dormant"));
        assert!(s.last_decision.as_deref().unwrap().contains("no pods"));
        let ready = s.conditions.iter().find(|c| c.type_ == "Ready").unwrap();
        let converged = s.conditions.iter().find(|c| c.type_ == "Converged").unwrap();
        assert_eq!(ready.status, "True", "a dormant target is healthy, not failed");
        assert_eq!(converged.status, "True", "an empty target is trivially at rest");
        // no event spam for a resting state.
        assert!(event_for(&TickReceipt::Dormant).is_none());
        // re-checks at the fast cadence (snappy dormant→active transition).
        let cd = ClassCooldowns::default();
        assert_eq!(next_requeue(&TickReceipt::Dormant, &cd), Duration::from_secs(cd.restart_free));
        // never counts as a carve / deferral / conflict.
        assert_eq!(s.carves_total, Some(0));
        assert_eq!(s.deferrals_total, Some(0));
    }
}

#[cfg(test)]
mod replica_tests {
    use super::*;
    use breathe_control::replica::{ReplicaDecision, ReplicaTickPlan};

    fn plan(decision: ReplicaDecision, actuate: Option<u32>, deferred: bool) -> ReplicaTickPlan {
        ReplicaTickPlan { decision, actuate, deferred }
    }

    #[test]
    fn resolve_precedence_conflict_applied_deferred_shadow_cooldown() {
        let d = ReplicaDecision::ScaleUp { from: 4, to: 5 };
        // conflict wins even if applied was attempted.
        assert_eq!(
            ReplicaReceipt::resolve(&plan(d, Some(5), false), true, true, false, false),
            ReplicaReceipt::Conflict { current: 4 }
        );
        // applied.
        assert_eq!(
            ReplicaReceipt::resolve(&plan(d, Some(5), false), true, false, false, false),
            ReplicaReceipt::Applied { from: 4, to: 5 }
        );
        // deferred scale-in.
        let din = ReplicaDecision::ScaleDown { from: 10, to: 9 };
        assert_eq!(
            ReplicaReceipt::resolve(&plan(din, None, true), false, false, false, false),
            ReplicaReceipt::DeferredScaleIn { from: 10, to: 9 }
        );
        // shadow (would carve, dry_run).
        assert_eq!(
            ReplicaReceipt::resolve(&plan(d, None, false), false, false, true, false),
            ReplicaReceipt::ShadowWouldApply { from: 4, to: 5 }
        );
        // cooldown (would carve, live, in cooldown).
        assert_eq!(
            ReplicaReceipt::resolve(&plan(d, None, false), false, false, false, true),
            ReplicaReceipt::Cooldown { from: 4, to: 5 }
        );
        // resting → Observed.
        let hold = ReplicaDecision::Hold { current: 3 };
        assert_eq!(
            ReplicaReceipt::resolve(&plan(hold, None, false), false, false, false, false),
            ReplicaReceipt::Observed { decision: hold }
        );
    }

    #[test]
    fn applied_status_reports_growing_and_stamps_a_carve() {
        let r = ReplicaReceipt::Applied { from: 4, to: 6 };
        let s = replica_status_for(&r, 1.3, 0, false, DisruptionPolicy::AllowRestart, None, 60, Some(2), CumulativeCounters::default());
        assert_eq!(s.phase.as_deref(), Some("Growing"));
        assert_eq!(s.current_limit.as_deref(), Some("6"));
        assert_eq!(s.last_action_class.as_deref(), Some("RestartFree"));
        assert!(s.last_change_epoch.is_some(), "an applied carve stamps the change epoch");
        assert_eq!(s.observed_generation, Some(2));
        // Ready=True so kubectl wait / the confirm gate see an observable band.
        assert_eq!(s.conditions.iter().find(|c| c.type_ == "Ready").map(|c| c.status.as_str()), Some("True"));
    }

    #[test]
    fn holding_status_is_confirm_gate_passable() {
        // A resting Holding tick must present exactly the shape the ShadowConfirmEffect
        // confirm gate keys on: Ready=True ∧ Stale=False ∧ Conflict=False.
        let r = ReplicaReceipt::Observed { decision: ReplicaDecision::Hold { current: 3 } };
        let s = replica_status_for(&r, 1.0, 0, true, DisruptionPolicy::RestartFreeOnly, None, 60, None, CumulativeCounters::default());
        assert_eq!(s.phase.as_deref(), Some("Holding"));
        let cond = |t: &str| s.conditions.iter().find(|c| c.type_ == t).map(|c| c.status.as_str());
        assert_eq!(cond("Ready"), Some("True"));
        assert_eq!(cond("Converged"), Some("True"));
        assert_eq!(cond("Stale"), Some("False"));
        assert_eq!(cond("Conflict"), Some("False"));
        assert_eq!(s.effective_dry_run, Some(true));
    }

    #[test]
    fn stale_status_holds_and_marks_stale() {
        let r = ReplicaReceipt::Stale { staleness_secs: 120, current: 4 };
        let s = replica_status_for(&r, 1.0, 120, false, DisruptionPolicy::AllowRestart, None, 60, None, CumulativeCounters::default());
        assert_eq!(s.phase.as_deref(), Some("Stale"));
        assert_eq!(s.current_limit.as_deref(), Some("4"), "a stale tick reports the live count, unchanged");
        assert_eq!(s.conditions.iter().find(|c| c.type_ == "Stale").map(|c| c.status.as_str()), Some("True"));
    }

    #[test]
    fn deferred_scale_in_reports_deferred_would_restart() {
        let r = ReplicaReceipt::DeferredScaleIn { from: 10, to: 9 };
        let s = replica_status_for(&r, 0.5, 0, false, DisruptionPolicy::RestartFreeOnly, None, 60, None, CumulativeCounters::default());
        assert_eq!(s.phase.as_deref(), Some("DeferredWouldRestart"));
        assert_eq!(s.current_limit.as_deref(), Some("10"), "the crossing was refused — count unchanged");
        assert_eq!(s.conditions.iter().find(|c| c.type_ == "Throttled").map(|c| c.status.as_str()), Some("True"));
    }

    #[test]
    fn entry_for_maps_receipts_to_counter_classes() {
        assert_eq!(replica_entry_for(&ReplicaReceipt::Applied { from: 2, to: 3 }, false).class, CounterClass::Carve);
        assert_eq!(replica_entry_for(&ReplicaReceipt::DeferredScaleIn { from: 3, to: 2 }, false).class, CounterClass::Deferral);
        assert_eq!(replica_entry_for(&ReplicaReceipt::Conflict { current: 3 }, false).class, CounterClass::Conflict);
        assert_eq!(replica_entry_for(&ReplicaReceipt::Stale { staleness_secs: 1, current: 3 }, false).class, CounterClass::NoCount);
    }

    #[test]
    fn next_requeue_is_fast_for_carves_and_backs_off_a_deferral() {
        let cd = ClassCooldowns::default();
        assert_eq!(replica_next_requeue(&ReplicaReceipt::Applied { from: 2, to: 3 }, &cd), Duration::from_secs(cd.restart_free));
        assert_eq!(replica_next_requeue(&ReplicaReceipt::DeferredScaleIn { from: 3, to: 2 }, &cd), Duration::from_secs(cd.restart_requiring));
        assert_eq!(replica_next_requeue(&ReplicaReceipt::Stale { staleness_secs: 9, current: 2 }, &cd), Duration::from_secs(cd.restart_conditional));
    }
}

#[cfg(test)]
mod health_tests {
    use super::*;

    fn cond(type_: &str, status: &str, last_transition_time: &str) -> Condition {
        Condition {
            type_: type_.into(),
            status: status.into(),
            reason: "R".into(),
            message: format!("{type_} is {status}"),
            last_transition_time: last_transition_time.into(),
            observed_generation: None,
        }
    }

    const T0: &str = "2026-01-01T00:00:00Z"; // an old transition time
    const NOW_FAR: &str = "2026-01-01T01:00:00Z"; // T0 + 3600s (past the 1800s threshold)
    const NOW_NEAR: &str = "2026-01-01T00:10:00Z"; // T0 + 600s (under the threshold)

    #[test]
    fn seconds_since_computes_a_positive_duration() {
        assert_eq!(seconds_since(NOW_FAR, T0), Some(3600));
        assert_eq!(seconds_since(NOW_NEAR, T0), Some(600));
    }

    #[test]
    fn seconds_since_returns_none_on_unparsable_input() {
        assert_eq!(seconds_since("not-a-time", T0), None);
        assert_eq!(seconds_since(NOW_FAR, "not-a-time"), None);
    }

    #[test]
    fn healthy_when_ready_and_converged() {
        let conditions = vec![
            cond("Ready", "True", T0),
            cond("Converged", "True", T0),
            cond("Throttled", "False", T0),
            cond("Conflict", "False", T0),
            cond("Supported", "True", T0),
        ];
        assert_eq!(health_verdict(&conditions, NOW_FAR, STUCK_AFTER_SECS, false), HealthVerdict::Healthy);
    }

    #[test]
    fn unsupported_takes_priority_regardless_of_other_conditions() {
        let conditions = vec![
            cond("Ready", "False", T0), // would ALSO be Stuck-eligible
            cond("Supported", "False", T0),
        ];
        assert_eq!(
            health_verdict(&conditions, NOW_FAR, STUCK_AFTER_SECS, false),
            HealthVerdict::Unsupported { reason: "Supported is False".into() }
        );
    }

    #[test]
    fn target_not_found_is_immediate_never_waits_for_the_stuck_timer() {
        // TargetFound=False, well UNDER the stuck threshold — still reported as
        // TargetNotFound immediately (task #217), never Healthy-until-stuck.
        let conditions = vec![cond("Ready", "False", T0), cond("Supported", "True", T0), cond("TargetFound", "False", T0)];
        assert_eq!(
            health_verdict(&conditions, NOW_NEAR, STUCK_AFTER_SECS, false),
            HealthVerdict::TargetNotFound { since_secs: 600 }
        );
    }

    #[test]
    fn target_not_found_takes_priority_over_a_stuck_ready() {
        // Ready=False would ALSO be Stuck-eligible past the threshold — TargetFound
        // takes priority, exactly like Supported does, so the honest arm wins.
        let conditions = vec![cond("Ready", "False", T0), cond("Supported", "True", T0), cond("TargetFound", "False", T0)];
        assert_eq!(
            health_verdict(&conditions, NOW_FAR, STUCK_AFTER_SECS, false),
            HealthVerdict::TargetNotFound { since_secs: 3600 }
        );
    }

    #[test]
    fn target_found_true_is_a_no_op_on_the_verdict() {
        let conditions = vec![
            cond("Ready", "True", T0),
            cond("Converged", "True", T0),
            cond("Throttled", "False", T0),
            cond("Supported", "True", T0),
            cond("TargetFound", "True", T0),
        ];
        assert_eq!(health_verdict(&conditions, NOW_FAR, STUCK_AFTER_SECS, false), HealthVerdict::Healthy);
    }

    #[test]
    fn not_yet_stuck_before_the_threshold_elapses() {
        let conditions = vec![cond("Ready", "False", T0), cond("Supported", "True", T0)];
        assert_eq!(health_verdict(&conditions, NOW_NEAR, STUCK_AFTER_SECS, false), HealthVerdict::Healthy);
    }

    #[test]
    fn ready_false_past_threshold_is_stuck() {
        let conditions = vec![cond("Ready", "False", T0), cond("Supported", "True", T0)];
        assert_eq!(
            health_verdict(&conditions, NOW_FAR, STUCK_AFTER_SECS, false),
            HealthVerdict::Stuck { condition: "Ready".into(), since_secs: 3600, reason: "Ready is False".into() }
        );
    }

    #[test]
    fn ready_false_past_threshold_is_stuck_even_in_dry_run() {
        // task 2's carveout is scoped to Converged ONLY — an observability/metrics
        // failure (Ready=False) is meaningful even in shadow mode.
        let conditions = vec![cond("Ready", "False", T0), cond("Supported", "True", T0)];
        assert_eq!(
            health_verdict(&conditions, NOW_FAR, STUCK_AFTER_SECS, true),
            HealthVerdict::Stuck { condition: "Ready".into(), since_secs: 3600, reason: "Ready is False".into() }
        );
    }

    #[test]
    fn converged_false_past_threshold_is_stuck() {
        let conditions = vec![
            cond("Ready", "True", T0),
            cond("Converged", "False", T0),
            cond("Throttled", "False", T0),
            cond("Supported", "True", T0),
        ];
        assert_eq!(
            health_verdict(&conditions, NOW_FAR, STUCK_AFTER_SECS, false),
            HealthVerdict::Stuck { condition: "Converged".into(), since_secs: 3600, reason: "Converged is False".into() }
        );
    }

    #[test]
    fn converged_false_past_threshold_in_dry_run_is_shadow_pending_not_stuck() {
        // task 2: a permanently-shadowed band (dryRun:true) structurally never
        // converges — the age-based Stuck classification downgrades to the
        // distinct, non-alarming ShadowPending instead of a false Stuck.
        let conditions = vec![
            cond("Ready", "True", T0),
            cond("Converged", "False", T0),
            cond("Throttled", "False", T0),
            cond("Supported", "True", T0),
        ];
        assert_eq!(
            health_verdict(&conditions, NOW_FAR, STUCK_AFTER_SECS, true),
            HealthVerdict::ShadowPending { since_secs: 3600 }
        );
    }

    #[test]
    fn throttled_shields_a_long_unconverged_hold_from_stuck() {
        // warmup/cooldown/deferred-crossing/stale-metric can legitimately hold
        // Converged=False far past the threshold — Throttled=True must shield it.
        let conditions = vec![
            cond("Ready", "True", T0),
            cond("Converged", "False", T0),
            cond("Throttled", "True", T0),
            cond("Supported", "True", T0),
        ];
        assert_eq!(health_verdict(&conditions, NOW_FAR, STUCK_AFTER_SECS, false), HealthVerdict::Healthy);
    }

    #[test]
    fn conflict_true_past_threshold_is_stuck() {
        let conditions = vec![
            cond("Ready", "True", T0),
            cond("Converged", "True", T0),
            cond("Throttled", "False", T0),
            cond("Conflict", "True", T0),
            cond("Supported", "True", T0),
        ];
        assert_eq!(
            health_verdict(&conditions, NOW_FAR, STUCK_AFTER_SECS, false),
            HealthVerdict::Stuck { condition: "Conflict".into(), since_secs: 3600, reason: "Conflict is True".into() }
        );
    }

    #[test]
    fn missing_conditions_never_panics_and_defaults_healthy() {
        assert_eq!(health_verdict(&[], NOW_FAR, STUCK_AFTER_SECS, false), HealthVerdict::Healthy);
    }

    #[test]
    fn label_is_stable_pascal_case() {
        assert_eq!(HealthVerdict::Healthy.label(), "Healthy");
        assert_eq!(HealthVerdict::Unsupported { reason: String::new() }.label(), "Unsupported");
        assert_eq!(HealthVerdict::TargetNotFound { since_secs: 0 }.label(), "TargetNotFound");
        assert_eq!(HealthVerdict::ShadowPending { since_secs: 0 }.label(), "ShadowPending");
        assert_eq!(HealthVerdict::Stuck { condition: String::new(), since_secs: 0, reason: String::new() }.label(), "Stuck");
    }

    #[test]
    fn health_event_for_is_none_only_for_healthy() {
        assert!(health_event_for(&HealthVerdict::Healthy).is_none());
        assert!(health_event_for(&HealthVerdict::Unsupported { reason: "x".into() }).is_some());
        assert!(health_event_for(&HealthVerdict::Stuck { condition: "Ready".into(), since_secs: 99, reason: "x".into() }).is_some());
    }

    #[test]
    fn health_event_for_target_not_found_is_a_warning() {
        let (kind, reason, note) = health_event_for(&HealthVerdict::TargetNotFound { since_secs: 42 }).unwrap();
        assert_eq!(kind, EventKind::Warning);
        assert_eq!(reason, "TargetNotFound");
        assert!(note.contains("42s"));
    }

    #[test]
    fn health_event_for_shadow_pending_is_normal_never_a_warning() {
        // non-alarming by design — a permanently-shadowed band that never
        // converges is working exactly as configured.
        let (kind, reason, _) = health_event_for(&HealthVerdict::ShadowPending { since_secs: 42 }).unwrap();
        assert_eq!(kind, EventKind::Normal);
        assert_eq!(reason, "ShadowPending");
    }

    #[test]
    fn should_emit_health_event_dedupes_on_unchanged_label() {
        let stuck = HealthVerdict::Stuck { condition: "Ready".into(), since_secs: 99, reason: "x".into() };
        assert!(should_emit_health_event(&stuck, None), "first observation always emits");
        assert!(should_emit_health_event(&stuck, Some("Healthy")), "transition into Stuck emits");
        assert!(!should_emit_health_event(&stuck, Some("Stuck")), "unchanged label does not re-emit every tick");
        assert!(should_emit_health_event(&HealthVerdict::Healthy, Some("Stuck")), "recovering out of Stuck is a transition too");
    }

    // ── the ever-governed latch (task #50) ────────────────────────────────
    //
    // A band matching zero pods reports `Dormant`, and `Dormant` was counted as
    // converged. That is correct for a scale-to-zero workload and WRONG for a
    // selector that matches nothing and never will — and the two were
    // indistinguishable, so a fleet with stale selectors reported 100%
    // converged. `first_observed_epoch` is the threshold-free discriminator.

    fn obs(used: u64) -> breathe_control::Observation {
        breathe_control::Observation {
            used,
            peak_used: used,
            bound: breathe_control::Capacity::Declared(used * 2),
            owners: vec![],
            staleness_secs: 0,
            observed_for_secs: 0,
            memory_shrink_restart_free: false,
            request_floor: 0,
            throttle_signal: 0,
            restarting: false,
            storage_capability: None,
        }
    }

    fn out_observed(receipt: TickReceipt, observed: Option<breathe_control::Observation>) -> TickOutcome {
        TickOutcome { receipt, observed, policy: DisruptionPolicy::RestartFreeOnly, gate: test_live_gate() }
    }

    #[test]
    fn a_band_that_has_never_seen_a_pod_has_no_first_observed_epoch() {
        // The pathology: Dormant on the very first tick, nothing ever observed.
        //
        // THIS TEST WAS STRUCTURALLY VACUOUS UNTIL 2026-07-28. It asserted
        // `first_observed_epoch == None` on a field whose Default is already
        // None, so it passed with the latch deleted — it could not fail. The
        // red-run audit that "verified all four in both directions" counted
        // four tests and never noticed there were five. That is the same
        // vacuous-guard class this whole feature exists to close, found for the
        // THIRD time in my own work on it, and it is why the standing rule is
        // "make it fail" rather than "read it carefully".
        //
        // Now non-vacuous: it pins the DISCRIMINATION (unproven Dormant vs
        // proven Dormant), which no other test covers and which cannot hold
        // without the latch.
        let never = out_observed(TickReceipt::Dormant, None);
        let s_never = status_for(&never, None, 0, None, CumulativeCounters::ZERO.fold(&entry_for(&never)));
        assert_eq!(s_never.phase.as_deref(), Some("Dormant"));
        assert_eq!(s_never.first_observed_epoch, None, "never-observed band carries no epoch");

        // The contrast that gives the assertion teeth: a band that DID observe,
        // then went Dormant, must be distinguishable from the one above.
        let first = out_observed(TickReceipt::Observed { decision: Decision::Hold }, Some(obs(100)));
        let proven = status_for(&first, None, 0, None, CumulativeCounters::ZERO.fold(&entry_for(&first)));
        let later = out_observed(TickReceipt::Dormant, None);
        let s_proven = status_for(&later, Some(&proven), 0, None, CumulativeCounters::ZERO.fold(&entry_for(&later)));

        assert_eq!(s_proven.phase.as_deref(), Some("Dormant"), "both are Dormant — phase alone cannot tell them apart");
        assert!(s_proven.first_observed_epoch.is_some(), "the proven one MUST carry an epoch");
        assert_ne!(
            s_never.first_observed_epoch, s_proven.first_observed_epoch,
            "two Dormant bands, one proven and one never-proven, MUST be distinguishable — \
             this inequality is the entire point of the field and fails without the latch"
        );
    }

    #[test]
    fn the_first_observation_sets_the_latch() {
        let o = out_observed(TickReceipt::Observed { decision: Decision::Hold }, Some(obs(100)));
        let s = status_for(&o, None, 0, None, CumulativeCounters::ZERO.fold(&entry_for(&o)));
        assert!(s.first_observed_epoch.is_some(), "observing a pod proves the band governs something");
    }

    #[test]
    fn the_latch_survives_a_later_dormant_tick() {
        // THE CASE THAT MAKES `Dormant` LEGITIMATE: a real workload that scaled to
        // zero. It observed pods before, so its later empty ticks are genuinely
        // at-rest and must keep counting as converged.
        let first = out_observed(TickReceipt::Observed { decision: Decision::Hold }, Some(obs(100)));
        let proven = status_for(&first, None, 0, None, CumulativeCounters::ZERO.fold(&entry_for(&first)));
        let stamped = proven.first_observed_epoch.expect("set by the first observation");

        let later = out_observed(TickReceipt::Dormant, None);
        let s = status_for(&later, Some(&proven), 0, None, CumulativeCounters::ZERO.fold(&entry_for(&later)));
        assert_eq!(s.phase.as_deref(), Some("Dormant"));
        assert_eq!(
            s.first_observed_epoch,
            Some(stamped),
            "the latch must be STICKY and keep its ORIGINAL timestamp — `s` starts from \
             BandStatus::default() each tick, so a latch that is not explicitly carried is \
             silently dropped, and a latch that resets is not a latch"
        );
    }

    #[test]
    fn the_latch_does_not_move_on_a_second_observation() {
        // It records the FIRST proof, not the most recent one — otherwise it would
        // drift into being a last-seen timestamp, which is a different (and
        // threshold-requiring) signal.
        let a = out_observed(TickReceipt::Observed { decision: Decision::Hold }, Some(obs(100)));
        let s1 = status_for(&a, None, 0, None, CumulativeCounters::ZERO.fold(&entry_for(&a)));
        let b = out_observed(TickReceipt::Observed { decision: Decision::Hold }, Some(obs(200)));
        let s2 = status_for(&b, Some(&s1), 0, None, CumulativeCounters::ZERO.fold(&entry_for(&b)));
        // NON-VACUITY GUARD, and it is not decoration: the red run proved this
        // test PASSES with the latch deleted, because `None == None` holds. It
        // would have sat green forever while testing nothing — the exact class
        // this whole change closes, found in its own test suite. Pin that both
        // sides are real before comparing them.
        assert!(s1.first_observed_epoch.is_some(), "precondition: the latch must actually be set");
        assert_eq!(s1.first_observed_epoch, s2.first_observed_epoch);
    }

    #[test]
    fn the_latch_is_carried_across_an_error_tick_too() {
        // Carried in the COMMON section, before the per-receipt match, so a
        // transient provider error cannot un-prove a band that has genuinely
        // governed pods. Regression guard for the "carry it in one arm only"
        // mistake this shape invites.
        let first = out_observed(TickReceipt::Observed { decision: Decision::Hold }, Some(obs(100)));
        let proven = status_for(&first, None, 0, None, CumulativeCounters::ZERO.fold(&entry_for(&first)));
        let err = out_observed(TickReceipt::Error { error: ProviderError::MetricsMissing }, None);
        let s = status_for(&err, Some(&proven), 0, None, CumulativeCounters::ZERO.fold(&entry_for(&err)));
        // Same non-vacuity guard as the sibling above — without it this passes on
        // `None == None` when the latch is absent (proven by the red run).
        assert!(proven.first_observed_epoch.is_some(), "precondition: the band was genuinely proven first");
        assert_eq!(s.first_observed_epoch, proven.first_observed_epoch, "an error tick must not un-prove the band");
    }

    #[test]
    fn the_latch_survives_error_status_and_suspended_status() {
        // THE GAP AN ADVERSARIAL PASS FOUND (2026-07-28). `status_for` carried
        // the latch; its two SIBLING producers did not. It survived them only
        // because `skip_serializing_if = "Option::is_none"` omits a None from
        // the JSON and `Patch::Merge` then leaves the stored value alone —
        // three unrelated edits away from breaking, and covered by nothing.
        //
        // These assertions are on the STRUCT, deliberately, not through a
        // patch: the whole point is that the guarantee no longer depends on
        // serde behaviour or on the patch strategy staying Merge.
        let first = out_observed(TickReceipt::Observed { decision: Decision::Hold }, Some(obs(100)));
        let proven = status_for(&first, None, 0, None, CumulativeCounters::ZERO.fold(&entry_for(&first)));
        let stamped = proven.first_observed_epoch.expect("precondition: the band is proven");

        assert_eq!(
            super::error_status(Some(&proven), "boom").first_observed_epoch,
            Some(stamped),
            "an error tick must not un-prove a band that has governed pods"
        );
        assert_eq!(
            super::suspended_status(Some(&proven)).first_observed_epoch,
            Some(stamped),
            "suspending a band does not un-prove that it once governed pods"
        );

        // The honest negative: with no prior there is nothing to carry, so both
        // correctly report unproven rather than inventing an epoch.
        assert_eq!(super::error_status(None, "boom").first_observed_epoch, None);
        assert_eq!(super::suspended_status(None).first_observed_epoch, None);
    }

    #[test]
    fn status_for_sets_health_field_end_to_end() {
        let outcome = TickOutcome {
            receipt: TickReceipt::Conflict { manager: "someone-else".into() },
            observed: None,
            policy: DisruptionPolicy::RestartFreeOnly,
            gate: test_live_gate(),
        };
        let s = status_for(&outcome, None, 0, None, CumulativeCounters::ZERO.fold(&entry_for(&outcome)));
        // fresh band, first tick — under the stuck threshold, so Healthy despite Conflict.
        assert_eq!(s.health.as_deref(), Some("Healthy"));
    }
}

/// Task #220 — the diff-gate that keeps a resting band from writing to etcd
/// on every tick. Mocks the k8s apiserver transport the same way
/// `kube-client`'s own test suite does (`tower_test::mock::pair` stood in as
/// the service under `kube::Client::new` — see `kube-client-0.96.0/src/
/// client/mod.rs`'s `test_mock`), so these prove REAL zero-vs-one HTTP calls
/// through `patch_status_if_changed`, not just the pure `BandStatus`
/// comparison it's built on.
#[cfg(test)]
mod patch_status_diff_gate_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use breathe_crd::MemoryBand;
    use http::{Request, Response};
    use kube::client::Body;
    use tokio::time::timeout;
    use tower_test::mock;

    /// Spawn a background responder that answers every request it receives
    /// with a minimal-but-valid `MemoryBand` (so `Api::patch_status`'s
    /// response deserialization succeeds) and counts how many requests
    /// actually arrived. The count is the test's "did we call the
    /// apiserver" oracle — the thing task #220 is about avoiding.
    fn spawn_counting_responder(mut handle: mock::Handle<Request<Body>, Response<Body>>) -> Arc<AtomicUsize> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_task = calls.clone();
        tokio::spawn(async move {
            while let Some((_req, send)) = handle.next_request().await {
                calls_task.fetch_add(1, Ordering::SeqCst);
                let body = serde_json::to_vec(&serde_json::json!({
                    "apiVersion": "breathe.pleme.io/v1",
                    "kind": "MemoryBand",
                    "metadata": { "name": "demo", "namespace": "default" },
                    "spec": { "targetRef": { "kind": "Deployment", "name": "demo-app" } },
                }))
                .expect("fixture MemoryBand serializes");
                send.send_response(Response::builder().status(200).body(Body::from(body)).unwrap());
            }
        });
        calls
    }

    fn sample_status(util: f64) -> BandStatus {
        BandStatus { phase: Some("Holding".into()), observed_util: Some(util), ..Default::default() }
    }

    #[tokio::test]
    async fn unchanged_status_skips_the_patch_call() {
        let (mock_service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let calls = spawn_counting_responder(handle);
        let client = Client::new(mock_service, "default");

        // Byte-identical to the CR's current live status — the resting-band case.
        let live = sample_status(0.42);
        let recomputed = live.clone();

        let patched = timeout(
            Duration::from_millis(500),
            patch_status_if_changed::<MemoryBand>(&client, "default", "demo", Some(&live), &recomputed),
        )
        .await
        .expect("patch_status_if_changed hung — it must never wait on the apiserver when status is unchanged")
        .expect("patch_status_if_changed returned an error");

        assert!(!patched, "an unchanged status must report that no patch was issued");
        assert_eq!(calls.load(Ordering::SeqCst), 0, "an unchanged status must never call the apiserver");
    }

    #[tokio::test]
    async fn changed_status_still_patches_exactly_once() {
        let (mock_service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let calls = spawn_counting_responder(handle);
        let client = Client::new(mock_service, "default");

        let live = sample_status(0.10);
        let recomputed = sample_status(0.90); // genuinely different observed_util

        let patched = timeout(
            Duration::from_millis(500),
            patch_status_if_changed::<MemoryBand>(&client, "default", "demo", Some(&live), &recomputed),
        )
        .await
        .expect("patch_status_if_changed timed out waiting on the mock apiserver")
        .expect("patch_status_if_changed returned an error");

        assert!(patched, "a genuinely changed status must report that a patch was issued");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "a genuinely changed status must patch exactly once");
    }

    #[tokio::test]
    async fn no_prior_status_always_patches() {
        // The very first reconcile of a fresh CR has no prior status at all
        // (`obj.status()` is `None`) — must still patch, never silently hold
        // the band's status unset forever just because there's nothing to diff.
        let (mock_service, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let calls = spawn_counting_responder(handle);
        let client = Client::new(mock_service, "default");

        let recomputed = sample_status(0.5);

        let patched = timeout(
            Duration::from_millis(500),
            patch_status_if_changed::<MemoryBand>(&client, "default", "demo", None, &recomputed),
        )
        .await
        .expect("patch_status_if_changed timed out waiting on the mock apiserver")
        .expect("patch_status_if_changed returned an error");

        assert!(patched, "the first-ever status write (no prior) must always patch");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
