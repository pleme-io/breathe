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
use breathe_provider::{ClassCooldowns, DisruptionPolicy, EdgeTier};

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
    (obs.capacity > 0).then(|| obs.used as f64 / obs.capacity as f64)
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
        TickReceipt::Error { error } => (Warning, "ReconcileError", error.to_string()),
        TickReceipt::DryRunWouldApply { from, to } => {
            (Normal, "ShadowWouldApply", format!("shadow: would carve {from} -> {to} (dryRun — nothing written)"))
        }
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
            Decision::NoLimit => (Warning, "NoLimit", "no limit set — cannot reason on utilization".into()),
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
    let observable = !matches!(
        r,
        TickReceipt::Error { .. }
            | TickReceipt::MetricUnrepresentable { .. }
            | TickReceipt::CapabilityMissing { .. }
            | TickReceipt::Observed { decision: Decision::NoLimit }
    );
    // SUPPORTED (the design's point-3(b) "will never converge without operator
    // action" signal, distinct from "waiting"): `false` ONLY for
    // `CapabilityMissing` — every other receipt (including `Conflict`/
    // `MetricUnrepresentable`, which MAY be transient) stays `true`.
    let supported = !matches!(r, TickReceipt::CapabilityMissing { .. });
    let converged = matches!(
        r,
        TickReceipt::Observed { decision: Decision::Hold | Decision::AtCeiling { .. } | Decision::NoSafeShrink { .. } }
            | TickReceipt::Dormant // an empty (scaled-to-zero) target is trivially at rest
    );
    let throttled = matches!(
        r,
        TickReceipt::Cooldown | TickReceipt::DeferredWouldRestart { .. } | TickReceipt::Stale { .. } | TickReceipt::Warmup { .. } | TickReceipt::Throttled { .. }
    );
    let stale = matches!(r, TickReceipt::Stale { .. });
    let conflict = matches!(r, TickReceipt::Conflict { .. });

    let mut out = Vec::with_capacity(6);
    upsert_condition(&mut out, prior, &now, "Ready", observable,
        if observable { "Reconciling" } else { "NotObservable" },
        if observable { "enrolled, config parses, metric observable" } else { "no metric/limit to reason on" }, generation);
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
        if supported { "CapabilityOk" } else { "StorageClassUnsupported" },
        "StorageClass allowVolumeExpansion + per-volume metrics — False means this band can NEVER converge without operator action", generation);
    out
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
    s.effective_dry_run = Some(outcome.dry_run);
    s.effective_policy = Some(policy_str(outcome.policy));
    s.edge_tier = Some(edge_tier_str(receipt.edge_tier()));
    if let Some(obs) = &outcome.observed {
        s.observed_used = Some(obs.used as i64);
        // The trailing-window peak that drove this tick's never-OOM shrink floor —
        // persisted so the next reconcile folds the current sample into it (the
        // cross-tick peak carry). `reconcile_one` guarantees `peak_used ≥ used`.
        s.observed_peak_used = Some(obs.peak_used as i64);
        s.observed_capacity = Some(obs.capacity as i64);
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
        TickReceipt::DryRunWouldApply { from, to } => {
            s.phase = Some("ShadowWouldApply".into());
            s.current_limit = Some(from.to_string()); // shadow mutates nothing — the UNCHANGED limit
            s.last_decision = Some(format!("dry-run: {from} -> {to}"));
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
                Decision::NoLimit => ("NoLimit", "no limit set — cannot reason on utilization".to_string()),
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
        TickReceipt::Error { error } => {
            s.phase = Some("Error".into());
            s.last_decision = Some(error.to_string());
        }
    }

    // current_limit on EVERY arm: any non-carve tick reports the LIVE limit (the
    // observed capacity) rather than a stale value; Applied set its own `to` above.
    if s.current_limit.is_none() {
        if let Some(obs) = &outcome.observed {
            s.current_limit = Some(obs.capacity.to_string());
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

/// A short, stable tag for a receipt kind — the `decision_log` row's `receipt_kind`.
fn receipt_kind_str(r: &TickReceipt) -> &'static str {
    match r {
        TickReceipt::Conflict { .. } => "Conflict",
        TickReceipt::MetricUnrepresentable { .. } => "MetricUnrepresentable",
        TickReceipt::CapabilityMissing { .. } => "CapabilityMissing",
        TickReceipt::Stale { .. } => "Stale",
        TickReceipt::Cooldown => "Cooldown",
        TickReceipt::Applied { .. } => "Applied",
        TickReceipt::DryRunWouldApply { .. } => "DryRunWouldApply",
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
        | TickReceipt::DryRunWouldApply { from, to }
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
        dry_run: outcome.dry_run,
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
        TickReceipt::Applied { .. } | TickReceipt::DryRunWouldApply { .. } | TickReceipt::Dormant => {
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
    gauge!("breathe_band_dry_run", base()).set(f64::from(u8::from(outcome.dry_run)));
    gauge!("breathe_band_cooldown_remaining_seconds", base()).set(cooldown_remaining_s as f64);

    // observed gauges — the live signal driving the loop.
    if let Some(obs) = &outcome.observed {
        gauge!("breathe_band_used", base()).set(obs.used as f64);
        gauge!("breathe_band_capacity", base()).set(obs.capacity as f64);
        gauge!("breathe_band_staleness_seconds", base()).set(obs.staleness_secs as f64);
        if let Some(u) = util_of(obs) {
            gauge!("breathe_band_util_ratio", base()).set(u);
        }
    }

    // the carved limit, tracked over time.
    let limit = match &outcome.receipt {
        TickReceipt::Applied { to, .. } => Some(*to),
        TickReceipt::DryRunWouldApply { from, .. } | TickReceipt::DeferredWouldRestart { from, .. } => Some(*from),
        _ => outcome.observed.as_ref().map(|o| o.capacity),
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
pub fn error_status(decision: impl Into<String>) -> BandStatus {
    let mut s = BandStatus::default();
    s.phase = Some("Error".into());
    s.last_decision = Some(decision.into());
    s
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
    let mut conds = Vec::with_capacity(5);
    upsert_condition(&mut conds, prior_conds, &now, "Ready", true, "Reconciling", "enrolled, config parses, signal observable", generation);
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
        TickOutcome { receipt, observed: None, policy: DisruptionPolicy::RestartFreeOnly, dry_run: false }
    }

    /// Build a status from an outcome with the counters the DecisionLog would
    /// produce from a zero prior — i.e. `fold(ZERO, entry_for(outcome))`. Keeps
    /// these per-receipt tests asserting the counter values they always did
    /// (Applied ⇒ carves 1, Conflict ⇒ conflicts 1, …) now that `status_for`
    /// consumes the count instead of computing it.
    fn status_of(o: &TickOutcome) -> BandStatus {
        status_for(o, None, 0, None, CumulativeCounters::ZERO.fold(&entry_for(o)))
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
        let s = status_of(&out(TickReceipt::DryRunWouldApply { from: 100, to: 250 }));
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
        // not observable either — there is nothing further to reason on.
        let ready = s.conditions.iter().find(|c| c.type_ == "Ready").unwrap();
        assert_eq!(ready.status, "False");
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
