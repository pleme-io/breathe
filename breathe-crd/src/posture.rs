//! `BreathePosture` — a cluster-scoped, named default policy for the 8
//! behavioral fields every Band CRD kind shares (setpoint / growAbove /
//! growFactor / shrinkBelow / shrinkFactor / cooldownSeconds /
//! maxStalenessSeconds / disruptionPolicy). A Band references one by name via
//! its own `spec.postureRef`; the 3-tier fold (an explicit per-CR value ALWAYS
//! wins > the referenced posture's value > the compiled default) lives on the
//! `Band` trait's `*_with_posture` methods in `lib.rs` (see
//! `Band::band_config_with_posture` and its three siblings), which the
//! `band_kind!` macro implements for real over each kind's own raw
//! `Option<T>` spec fields.
//!
//! **Measured real problem this exists to kill:** 47 of ~50 live band CRs on
//! Camelot hand-duplicate the identical 7-value tuple
//! (0.8/0.85/1.25/0.7/0.9/600/120) verbatim. A `BreathePosture` names that
//! tuple once; a band opts in with one `postureRef` line instead of
//! copy-pasting seven.
//!
//! STRUCTURALLY ABSENT from this spec, on purpose: `floor`, `ceiling`,
//! `requestFloor`, `targetRef`, `dryRun`, `mode`. A `BreathePosture` has no
//! field to carry a capacity bound or a promotion-lifecycle flag, so it is
//! IMPOSSIBLE — not merely disciplined — for a posture edit to widen N
//! workloads' ceilings or flip N workloads from shadow to live in one patch
//! (the ★ Core rule in `BREATHABILITY.md`; the org's own `maxRunners`
//! 3×-stale incident, named in
//! `theory/INVARIANT-BY-CONSISTENCY-AND-CONTROLLER.md`, is exactly the
//! failure class this schema split forecloses).
//!
//! **Naming note:** `BreathePosture` is proposed, not yet `/naming`-skill
//! ratified (per the `theory/VIVEIRO.md` precedent for an unratified working
//! name). "Posture" reuses established vocabulary already load-bearing in
//! this exact doctrine (`IsolationPosture::for_class` in
//! `breathe-invariant/src/isolation.rs`; `BREATHABILITY.md` §II.7.4's own
//! phrase "the BEST DEFAULT POSTURE per class+context") rather than adding a
//! fourth name for a concept two names already own — "Class" was rejected
//! because `WorkloadClass` already names two unrelated, colliding concepts in
//! this exact workspace (`breathe-catalog::preset::WorkloadClass` — the
//! topology axis — vs `breathe-invariant::isolation::WorkloadClass` — the
//! criticality axis).

use crate::{DemandSignalSpec, QosClassSpec, WorkloadClassSpec};
use breathe_provider::DisruptionPolicy;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A named, complete default policy for the 8 behavioral band fields. A
/// `BreathePosture` is a COMPLETE tuple by construction — every field is
/// required (non-`Option`): a posture is either fully specified or it doesn't
/// exist. This is deliberate: it collapses what would otherwise be a
/// 3-tier-per-field fallback (override > posture-if-set > compiled-default)
/// into a clean 2-real-tier fold below the override (posture, then compiled
/// default) — a posture can never itself be "partially unset".
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "BreathePosture",
    shortname = "bpost",
    category = "breathe",
    status = "BreathePostureStatus",
    printcolumn = r#"{"name":"Setpoint","type":"number","jsonPath":".spec.setpoint"}"#,
    printcolumn = r#"{"name":"DisruptionPolicy","type":"string","jsonPath":".spec.disruptionPolicy"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BreathePostureSpec {
    /// Human-readable rationale for this class — carries the "why" WITH the
    /// tuple instead of it living only in a copy-pasted CR's comment (or, as
    /// found live, only in a since-retired sibling file's history).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub setpoint: f64,
    pub grow_above: f64,
    pub grow_factor: f64,
    pub shrink_below: f64,
    pub shrink_factor: f64,
    pub cooldown_seconds: u32,
    pub max_staleness_seconds: u32,
    pub disruption_policy: DisruptionPolicy,
    // Deliberately ABSENT: floor, ceiling, requestFloor, targetRef, dryRun,
    // mode. See the module doc — this is a type-level safety invariant, not
    // an omission.

    // ── The REQUEST-POLICY axis (added with `RequestBand`, 2026-07-26) ──
    //
    // These three are `Option`, which visibly breaks the "a posture is a
    // COMPLETE tuple by construction" rule stated above. That is a deliberate
    // exception with two reasons, both stated rather than glossed:
    //
    //  1. **Live CRs.** Five `BreathePosture` objects exist on camelot-eks
    //     (critical, critical-stateful, standard, batch, storage-volume).
    //     Adding REQUIRED fields would make every one of them fail to
    //     deserialize, i.e. a schema change that breaks the running controller
    //     on the postures it is currently serving. A dimension about not
    //     killing workloads must not ship by killing the controller.
    //  2. **They are a different axis.** The 8 fields above are the BAND-LAW
    //     tuple — one control loop's constants. These are ISOLATION policy, and
    //     they are meaningless to the other ten kinds. `storage-volume` should
    //     carry no request policy at all, and `None` says exactly that; a
    //     required field would force it to invent one.
    //
    // The safety invariant is untouched: none of these is a capacity bound or a
    // promotion flag, so a posture edit still cannot widen a ceiling or flip a
    // band live. A posture CAN now change a referencing band's QoS *target* —
    // but a target only ever produces a PROPOSAL for the durable door, never an
    // in-place write, so the blast radius of that edit is a git diff somebody
    // reviews, not N workloads mutated.
    /// The criticality class bands referencing this posture inherit. `None` ⇒
    /// this posture carries no request policy; a band falls through to its own
    /// value, then to `standard`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_class: Option<WorkloadClassSpec>,
    // "QoS" below is an English acronym, not a code item — same false positive
    // the crate already allows on `PlacementIsolationKind`.
    #[allow(clippy::doc_markdown)]
    /// The desired QoS class. `None` ⇒ the resolved `workloadClass`'s default
    /// (`critical → guaranteed`, `standard|noisy → burstable`, `batch →
    /// best-effort`) — delegated to `WorkloadClass::default_qos` upstream, never
    /// re-decided here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qos_target: Option<QosClassSpec>,
    /// The demand statistic a reservation tracks. `None` ⇒ the compiled default
    /// (p95 over 7d, +15%).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub demand: Option<DemandSignalSpec>,
}

/// `BreathePosture` status — deliberately NOT a maintained aggregate (no
/// dedicated actuator writes it). "Which bands reference posture X" is
/// answered live by filtering `breathe_band_list()`'s output client-side (the
/// `breathe-mcp` `breathe_posture_get` tool), never a status field that would
/// need its own writer to stay fresh.
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BreathePostureStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_observed: Option<String>,
}
