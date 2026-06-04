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
use breathe_core::{TickOutcome, TickReceipt};
use breathe_crd::{Band, BandStatus};
use breathe_provider::{ClassCooldowns, DisruptionPolicy, EdgeTier};
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
        TickReceipt::Error { error } => (Warning, "ReconcileError", error.to_string()),
        TickReceipt::DryRunWouldApply { from, to } => {
            (Normal, "ShadowWouldApply", format!("shadow: would carve {from} -> {to} (dryRun — nothing written)"))
        }
        TickReceipt::Observed { decision } => match decision {
            Decision::AtCeiling { current } => (Normal, "AtCeiling", format!("at ceiling {current} — would grow but capped")),
            Decision::NoSafeShrink { current } => (Normal, "AtFloor", format!("at floor {current} — no safe shrink")),
            Decision::NoLimit => (Warning, "NoLimit", "no limit set — cannot reason on utilization".into()),
            Decision::Grow { from, to } | Decision::Shrink { from, to } => {
                (Normal, "ObservedNoAct", format!("observed {from} -> {to} (directionality/observe-only — not applied)"))
            }
            Decision::Hold => return None, // within the deadband — resting, no event
        },
        TickReceipt::Cooldown => return None, // transient post-carve wait — no event
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

/// Map one reconcile OUTCOME to the typed CR status — every branch observable,
/// none silent. This is the single source of truth for band status semantics
/// across both reconcile processes. It reports not just *what happened* (phase +
/// legible last_decision) but the OBSERVED inputs that drove it (util/used/capacity/
/// freshness), the effective mode (dry-run/policy), the golden/ceiling edge tier,
/// the cooldown remaining, and cumulative carve/deferral/conflict counters —
/// everything `kubectl get/describe` and Grafana need, all from the one TickOutcome.
///
/// `prior` is the band's CURRENT status (read before reconcile) — used to carry the
/// cumulative counters forward (reconciles are serialized per-object, so a
/// read-then-increment is race-free) and to compute the cooldown remaining from the
/// last carve epoch. `cooldown_seconds` is the band's configured cooldown window.
#[must_use]
pub fn status_for(outcome: &TickOutcome, prior: Option<&BandStatus>, cooldown_seconds: u64) -> BandStatus {
    let mut s = BandStatus::default();
    let receipt = &outcome.receipt;

    // ── COMMON: the observed inputs + effective mode + edge tier (from the
    //    outcome, available on every non-pre-observe-error tick). ──────────────
    s.effective_dry_run = Some(outcome.dry_run);
    s.effective_policy = Some(policy_str(outcome.policy));
    s.edge_tier = Some(edge_tier_str(receipt.edge_tier()));
    if let Some(obs) = &outcome.observed {
        s.observed_used = Some(obs.used as i64);
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
            };
            s.phase = Some(phase.into());
            s.last_decision = Some(note);
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

    // ── CUMULATIVE COUNTERS — read prior + increment (serialized per object). ─
    let prior_n = |get: fn(&BandStatus) -> Option<i64>| prior.and_then(get).unwrap_or(0);
    s.carves_total = Some(prior_n(|p| p.carves_total) + i64::from(matches!(receipt, TickReceipt::Applied { .. })));
    s.deferrals_total =
        Some(prior_n(|p| p.deferrals_total) + i64::from(matches!(receipt, TickReceipt::DeferredWouldRestart { .. })));
    s.conflicts_total =
        Some(prior_n(|p| p.conflicts_total) + i64::from(matches!(receipt, TickReceipt::Conflict { .. })));

    // ── COOLDOWN REMAINING — from the last carve epoch (this tick's, or prior's). ─
    let last_carve = s.last_change_epoch.or_else(|| prior.and_then(|p| p.last_change_epoch)).unwrap_or(0);
    let remaining = (last_carve + cooldown_seconds as i64 - now_secs()).max(0);
    s.cooldown_remaining_seconds = Some(remaining);

    s
}

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
        // a shadow likewise looks fast (it is observing the live band).
        TickReceipt::Applied { .. } | TickReceipt::DryRunWouldApply { .. } => cooldowns.restart_free,
        // a refused crossing: back off by exactly the blocked class.
        TickReceipt::DeferredWouldRestart { class, .. } => cooldowns.for_class(*class),
        // non-mutating / transient: the mid window.
        TickReceipt::Observed { .. }
        | TickReceipt::Cooldown
        | TickReceipt::Conflict { .. }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Wrap a bare receipt in a minimal TickOutcome (no observation; the status
    /// per-arm fields under test don't need one).
    fn out(receipt: TickReceipt) -> TickOutcome {
        TickOutcome { receipt, observed: None, policy: DisruptionPolicy::RestartFreeOnly, dry_run: false }
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
        let grow = status_for(&out(TickReceipt::Applied { from: 100, to: 200, class: RestartFree }), None, 0);
        assert_eq!(grow.phase.as_deref(), Some("Growing"));
        assert_eq!(grow.current_limit.as_deref(), Some("200"));
        assert_eq!(grow.carves_total, Some(1));
        let shrink = status_for(&out(TickReceipt::Applied { from: 200, to: 100, class: RestartFree }), None, 0);
        assert_eq!(shrink.phase.as_deref(), Some("Shrinking"));
    }

    #[test]
    fn shadow_reports_what_would_have_happened_without_changing_the_limit() {
        let s = status_for(&out(TickReceipt::DryRunWouldApply { from: 100, to: 250 }), None, 0);
        assert_eq!(s.phase.as_deref(), Some("ShadowWouldApply"));
        // the reported current limit is the UNCHANGED value — shadow mutates nothing.
        assert_eq!(s.current_limit.as_deref(), Some("100"));
        assert!(s.last_decision.as_deref().unwrap().contains("250"));
    }

    #[test]
    fn conflict_records_the_yielded_to_manager() {
        let s = status_for(&out(TickReceipt::Conflict { manager: "helm".into() }), None, 0);
        assert_eq!(s.conflicts_total, Some(1));
        assert_eq!(s.phase.as_deref(), Some("Conflict"));
        assert_eq!(s.conflict_manager.as_deref(), Some("helm"));
    }

    #[test]
    fn deferred_crossing_maps_to_a_first_class_phase() {
        use breathe_provider::DisruptionClass;
        let s = status_for(&out(TickReceipt::DeferredWouldRestart { from: 1 << 30, to: 2 << 30, class: DisruptionClass::RestartRequiring }), None, 0);
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
}
