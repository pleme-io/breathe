//! The breathe band CRDs (breathe.pleme.io/v1) — the typed per-target enrollment
//! contracts. **Per-dimension kinds sharing one spec shape**, stamped from one
//! `band_kind!` macro: the k8s dimensions `MemoryBand` / `StorageBand` / `CpuBand`
//! AND the HOST dimensions `ArcBand` / `CgroupBand` — the host bands fall out of
//! the *same* macro (the descriptor encodes the host addressing; the CRD shape is
//! identical), so "solve once" holds: a host dimension is not a new CRD shape.
//! The controller/agent reconciles only declared bands; `kubectl get
//! memoryband,cpuband,storageband,arcband,cgroupband -A` is the complete,
//! auditable answer to "what is being managed, in which dimension".
//!
//! [`BreatheNodePool`] is the cluster-scoped enrollment charter: it names the
//! node breathe manages and carries the static L2 ceilings (mirrored from
//! `pleme.nixos.nodeBudget`) that the host agent enforces as its second safety
//! wall, plus the node-level master `writeEnabled` switch (false = whole node in
//! shadow).
//!
//! The [`Band`] trait is the dimension-agnostic accessor the generic controller
//! dispatches on — one reconcile body for every kind, host or k8s.

use std::collections::BTreeMap;

use breathe_control::replica::{ReplicaBandConfig, ReplicaSignal, Topology};
use breathe_control::{BandConfig, BoundIntroduction, MetricMissingPolicy, Unit};
use breathe_provider::gate::{
    self, ConfirmVerdict, EffectiveGate, EffectiveGateReport, GateInputs, LegacyDecision, LegacyPath, WriteIntent,
    WriteIntentSpec,
};
use breathe_provider::{DisruptionPolicy, LimitLayout, MetricSource};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

mod posture;
pub use posture::{BreathePosture, BreathePostureSpec, BreathePostureStatus};

/// The workload owner whose limit a band controls. For CNPG the kind is
/// `Cluster` (the patched field lives on the `Cluster` CR); for storage the kind
/// is `PersistentVolumeClaim`.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TargetRef {
    pub kind: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    /// When set, breathe resolves the band's pods DIRECTLY by this k8s label
    /// selector (`k=v,k2=v2`) instead of via an owner's `spec.selector.matchLabels`.
    /// The path for **ephemeral / owner-less pod groups** whose name is not stable
    /// and which have no single resolvable workload owner — GitHub ARC
    /// `EphemeralRunner`s (`actions.github.com/scale-set-name=<set>`), bare pods, Job
    /// pods. A selector ALWAYS carves in-place (`PodResize`, zero restart) within
    /// `targetRef`'s namespace; `name` then serves only as the metrics pod-name
    /// prefix + the human label. Omit it for Deployment/StatefulSet/CNPG owners.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_selector: Option<String>,
}

/// A standard k8s `metav1.Condition` (schemars-derivable — k8s_openapi's own
/// `Condition` is not `JsonSchema`). Enables `kubectl wait --for=condition=…` and
/// Flux/Argo health assessment off breathe bands.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Condition type, e.g. `Ready` / `Converged` / `Throttled` / `Stale` / `Conflict`.
    #[serde(rename = "type")]
    pub type_: String,
    /// `True` | `False` | `Unknown`.
    pub status: String,
    /// Machine-readable PascalCase reason.
    pub reason: String,
    /// Human-readable message.
    pub message: String,
    /// RFC3339 time the condition last flipped status (stable while status holds).
    pub last_transition_time: String,
    /// The `metadata.generation` the controller observed when setting this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

/// A point in a band's recent trajectory — the OVER-TIME view as a k8s object (no
/// Grafana needed). Appended on a carve or a phase change, capped to the last N, so
/// `kubectl get <band> -o yaml` shows how the adjustments have been going inline.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TrendSample {
    /// RFC3339 time of this sample.
    pub time: String,
    /// Observed utilization ratio at this point.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub util: Option<f64>,
    /// The limit at this point.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
    /// The phase at this point.
    pub phase: String,
    /// The decision that produced this sample (carve / transition).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<String>,
}

/// The per-cycle typed status receipt — shared across all band kinds.
///
/// Derives `PartialEq` so the controller can diff a freshly-computed status
/// against the CR's current live status and skip the `patch_status` write
/// entirely when they're byte-identical (task #220) — every field here is
/// itself `PartialEq` (`Condition`/`TrendSample` included), so the derive is
/// a real structural comparison, not a stand-in.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BandStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Cross-dimension health rollup, derived purely from `conditions` (no new
    /// per-band state) — `"Healthy"` | `"Stuck"` | `"Unsupported"`. See
    /// `breathe_runtime::health_verdict`. Generalizes the storage-only
    /// `Supported=False` terminal into a single normalized signal every band
    /// kind carries, so a reactive consumer (NATS/escuta, a dashboard, an
    /// operator) never has to interpret `phase` strings per-dimension to answer
    /// "is this band OK right now".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_util: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_limit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_decision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_change_epoch: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_manager: Option<String>,

    // ── M1 typed observability (jsonpath-queryable; the data already existed at
    //    decision time and was previously discarded after plan_tick). ──────────
    /// The observed utilization that drove this tick, as a ratio (`used/capacity`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_util: Option<f64>,
    /// The observed `used` scalar (bytes for memory/arc/cgroup; millicores for cpu).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_used: Option<i64>,
    /// The trailing-window PEAK working set (max RSS, with slow decay) — the
    /// never-OOM shrink floor is keyed on THIS, not the instantaneous `observed_used`
    /// (the authentik-Celery-worker OOM fix). Carried across ticks: each tick folds
    /// the current `used` into the prior peak via `breathe_control::update_peak`, so
    /// a recently-demonstrated spike holds the floor up for a meaningful window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_peak_used: Option<i64>,
    /// Epoch of the FIRST tick on which this band ever observed a live pod —
    /// `None` means it never has, in its entire lifetime. STICKY: once set it
    /// is carried forward across every subsequent tick, including ticks that
    /// observe nothing.
    ///
    /// WHY THIS EXISTS. `TickReceipt::Dormant` (the label-selected pod group is
    /// empty) is documented as benign, and for a genuinely scale-to-zero target
    /// — an ephemeral runner between builds, a Job — it IS benign, and it is
    /// counted at-rest/converged in the fleet overview. But it is produced by a
    /// pure emptiness check (`breathe-kube`: `selector.is_some() &&
    /// list.items.is_empty()`), so it cannot distinguish:
    ///
    ///   (a) empty because the workload is resting  -> genuinely converged
    ///   (b) empty because the selector matches nothing that exists, or ever
    ///       will — a typo'd label, a renamed workload, a deleted Deployment
    ///       -> the band governs NOTHING, silently, forever
    ///
    /// Both report `Dormant`, both count as converged, so a fleet where a third
    /// of the bands have stale selectors still reports 100% converged. That is
    /// the vacuous-guard class (`UNREPRESENTABILITY.md` §II.3): a mechanism
    /// reporting success having evaluated zero subjects.
    ///
    /// THIS FIELD IS THE THRESHOLD-FREE DISCRIMINATOR. Deliberately not a
    /// grace window or an idle timeout — inventing a "suspicious after N
    /// seconds" constant would be exactly the re-frozen static value
    /// `BREATHABILITY.md` §II forbids, and it would need re-tuning every time a
    /// workload's cadence changed. `None` is a crisp, tuning-free predicate:
    /// a band that has NEVER, not once, seen a pod is unproven. The instant it
    /// sees one, it is proven for the rest of its life and every later empty
    /// tick is legitimately at rest.
    ///
    /// DISTINCT FROM the `TargetFound` condition, which asks whether the
    /// *targetRef object* resolves. A Deployment scaled to zero resolves fine,
    /// and a raw label selector has no targetRef to resolve at all — so
    /// `TargetFound` is vacuously true in exactly case (b). This asks the
    /// different question: has this band ever actually governed anything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_observed_epoch: Option<i64>,
    /// The observed `capacity` (the current limit the util is measured against).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_capacity: Option<i64>,
    /// The epoch (unix secs) the band's WARMUP window started — the last observed
    /// (re)start of the target, or the band's first successful observation. The
    /// reconcile layer derives `observed_for_secs = now - warmup_start_epoch` to drive
    /// the warmup gate (a shrink is held until this exceeds `warmup_seconds`). Reset
    /// to `now` whenever a target restart is detected (the live limit dropped back to
    /// the template default / the observed capacity collapsed), so a fresh boot spike
    /// is always observed before any carve resumes. `None` until first observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warmup_start_epoch: Option<i64>,
    /// Age of the driving metric sample, in seconds (the freshness gate input).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness_seconds: Option<i64>,
    /// The restart cost of the last carve/decision (`RestartFree` / `RestartConditional`
    /// / `RestartRequiring`) — the per-tick attestation evidence, now typed in status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_action_class: Option<String>,
    /// Where the last tick sat on the golden/ceiling line (`GoldenPreserving` /
    /// `CeilingCrossing`) — the K4 continuity evidence, surfaced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge_tier: Option<String>,

    // ── The REQUEST dimension's projection. `None` on every other band kind. ──
    //
    // Added to the SHARED `BandStatus` rather than forking a `RequestBandStatus`
    // deliberately: three optional fields cost every other kind exactly nothing
    // (they serialize away), whereas a second status type would ripple through
    // `breathe-runtime`'s status mapping, the facade, and the gate matrix to buy
    // only tidiness. If a fourth request-only field ever appears, revisit.
    /// The quality-of-service class breathe **observed**, derived from the live
    /// pod. breathe never declares this — k8s computes it from (requests, limits) across
    /// every resource of every container, and a second source of truth for a
    /// derived value is exactly the drift this dimension exists to end.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qos_observed: Option<String>,
    /// The typed gap between `qosObserved` and the resolved `qosTarget` —
    /// `held` | `promotionProposed` | `blocked(<why>)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "open_json_schema")]
    pub qos_gap: Option<serde_json::Value>,
    /// A converged value that has **not yet reached git**: the content address
    /// of the durable write this band would make, plus where it would land.
    ///
    /// This field is the honest face of the M0 boundary. While no
    /// `ManifestWriter` transport is injected, a `durability: committed` band
    /// publishes a real, content-addressed, byte-identical-to-what-would-commit
    /// proposal here and commits nothing — so the gap between "breathe knows
    /// the right value" and "git carries it" is *visible in status*, not buried
    /// in a controller log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "open_json_schema")]
    pub pending_proposal: Option<serde_json::Value>,
    /// The DisruptionPolicy in effect for this band (`restartFreeOnly` / …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_policy: Option<String>,
    /// The effective mode: `true` = SHADOW (observe + attest, never carve).
    ///
    /// **Kept for compatibility; superseded by [`BandStatus::effective_gate`],**
    /// which carries the same verdict plus the one thing a bare bool never
    /// could: *why*. Both are written every tick and can never disagree —
    /// this bool is literally `effective_gate.state == shadow`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_dry_run: Option<bool>,
    /// **The typed authorization verdict for the last tick** — shadow with a
    /// named reason, or live with a named witness.
    ///
    /// This is the field that answers, from the CR alone, the two questions
    /// `effectiveDryRun` could not: *why is this band held?* (an authored
    /// `observe`, or an accidental `NotReady`/`Stale`/`Conflict` — a
    /// distinction that mattered on camelot-eks, where six bands were shadowed
    /// by accident while every surface reported "dryRun") and *what authorizes
    /// this band to write?* A `witness` of `legacyDefault` means the write
    /// rests on a pre-2026-07 resolution path rather than an authored
    /// `spec.writeIntent` — i.e. migration debt, and the burn-down metric.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_gate: Option<EffectiveGateReport>,
    /// Seconds remaining in the post-carve cooldown (0 = ready to carve).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cooldown_remaining_seconds: Option<i64>,
    /// Cumulative count of carves (Applied) over this controller's lifetime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carves_total: Option<i64>,
    /// Cumulative count of deferred ceiling crossings (policy refused a restart).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deferrals_total: Option<i64>,
    /// Cumulative count of single-writer conflicts (yielded to another manager).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflicts_total: Option<i64>,

    // ── M4: standard k8s conditions + observedGeneration (kubectl wait / health). ─
    /// `metadata.generation` the controller last reconciled — the "controller has
    /// seen my latest spec edit" signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Standard conditions (Ready / Converged / Throttled / Stale / Conflict).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    /// The recent TRAJECTORY (over-time view as a k8s object) — appended on a carve
    /// or a phase change, capped to the last N samples.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<TrendSample>,
    // ── M3 (Dev Loop): the ephemeral-env cost-guard readout (read-only). ──────
    /// The `EphemeralEnvId` of the band's namespace, if it carries one (the
    /// ephemeral-env binding) — read from the namespace label, never written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_env_id: Option<String>,
    /// Cost remaining (cents) under the namespace's `Densa` envelope SLA; negative
    /// ⇒ over budget. Read from the namespace `Densa`'s status — the Dev-Loop
    /// cost-guard surfaced on the band, so `kubectl get <band>` shows the env's
    /// budget headroom. Read-only (breathe never writes the Densa from a band).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_cost_remaining_cents: Option<i64>,
}

/// The CRD wire form of [`BoundIntroduction`] — **may breathe create a bound the
/// target's author never declared?**
///
/// A separate type from the `breathe-control` enum for the same, already-blessed
/// reason [`PromotionMode`] is separate from `outorga::PromotionMode`: a CRD field
/// needs the `JsonSchema` derive, and `breathe-control` is deliberately
/// dependency-free (its whole thesis is that the band law is solved once, in std).
/// The two are pinned one-to-one by an exhaustive round-trip test.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum BoundIntroductionSpec {
    /// A target with no declared bound is reported (phase `NoLimit`) and left
    /// alone. **The default** — the two directions are not symmetric: failing to
    /// right-size a declared limit wastes quota, while capping a deliberately
    /// uncapped component can wedge a cluster.
    #[default]
    Forbidden,
    /// breathe may seed a bound onto a target that declares none — the ceded-field
    /// takeover (CNPG/Flux relinquishing `limits.memory`) and the deliberate "cap
    /// this noisy neighbour" case.
    Allowed,
}

impl BoundIntroductionSpec {
    /// Project onto the pure `breathe-control` policy atom.
    #[must_use]
    pub fn to_control(self) -> BoundIntroduction {
        match self {
            Self::Forbidden => BoundIntroduction::Forbidden,
            Self::Allowed => BoundIntroduction::Allowed,
        }
    }
}

/// The band's PROMOTION LIFECYCLE — the typed state controlling whether (and
/// when) breathe moves from observing (SHADOW) to carving (EFFECT).
///
/// **SUPERSEDED by [`WriteIntent`], retired-not-deleted.** `spec.mode` is still
/// read (it is the second link in the resolution chain `writeIntent` >
/// `mode` > the compiled `ShadowConfirmEffect`) so every already-authored CR
/// keeps working unchanged. New CRs should author `spec.writeIntent`, whose
/// arms map one-to-one: `shadow` → `observe`, `effect` → `write` (which
/// additionally requires naming an author), `shadowConfirmEffect` →
/// `calibrateThenWrite`, `suspended` → `frozen`.
///
/// When neither `writeIntent` nor `mode` is authored, the default remains
/// `ShadowConfirmEffect` — no band is parked in permanent shadow with no exit,
/// and none goes live unconfirmed. Note this is a real, load-bearing default,
/// **not** a reading of `spec.dryRun`: that field has been unread for every
/// k8s band kind since 2026-06-19 (`76924b0`) and is now explicitly retired.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum PromotionMode {
    /// Observe + attest forever; never carve. For deliberate critical-path holds
    /// (flux/cnpg/etc. — annotated `shadow-hold-critical-path`).
    Shadow,
    /// Carve immediately — skip the confirm gate. Explicit, eyes-open go-live.
    Effect,
    /// DEFAULT. Shadow until the confirm gate passes, then auto-begin carving.
    /// Gate = a clean-observation window (Ready ∧ ¬Stale ∧ ¬Conflict held
    /// continuously for `confirmAfterSeconds`), OR the operator fast-path
    /// annotation `breathe.pleme.io/confirmed: "true"`. One-way: once live it
    /// stays live (unless the metric is lost — then it safely re-shadows).
    #[default]
    ShadowConfirmEffect,
    /// Frozen — never carve AND stop deciding (the `suspend` companion).
    Suspended,
}

/// The operator fast-path annotation: setting `breathe.pleme.io/confirmed: "true"`
/// satisfies a calibrating band's confirm gate immediately.
///
/// Re-exported from `breathe_provider::gate` (its home since the authorization
/// axis was typed) so this path keeps working for every existing consumer.
pub use breathe_provider::gate::CONFIRMED_ANNOTATION;

/// Map `outorga`'s typed shadow reason onto the CRD-facing mirror.
///
/// A `From` impl is impossible here — both types are foreign to this crate, so
/// the orphan rule forbids it. This free function is the one conversion point;
/// it is exhaustive, so an arm added upstream is a compile error here rather
/// than a silently-dropped reason.
#[must_use]
pub fn map_shadow_reason(r: outorga::ShadowReason) -> gate::ShadowReason {
    match r {
        outorga::ShadowReason::Frozen => gate::ShadowReason::Frozen,
        outorga::ShadowReason::ModeShadow => gate::ShadowReason::ModeShadow,
        outorga::ShadowReason::Suspended => gate::ShadowReason::Suspended,
        outorga::ShadowReason::NotReady => gate::ShadowReason::NotReady,
        outorga::ShadowReason::Stale => gate::ShadowReason::Stale,
        outorga::ShadowReason::Conflict => gate::ShadowReason::Conflict,
        outorga::ShadowReason::ConfirmPending { held_secs, need_secs } => {
            gate::ShadowReason::ConfirmPending { held_secs, need_secs }
        }
    }
}

fn d_confirm_after() -> u64 {
    1800
}

/// The dimension-agnostic accessor the generic controller reconciles through.
/// Implemented by every band kind via the macro — one reconcile body, N kinds.
pub trait Band:
    Clone
    + std::fmt::Debug
    + serde::de::DeserializeOwned
    + kube::Resource<DynamicType = (), Scope = kube::core::NamespaceResourceScope>
    + Send
    + Sync
    + Sized
    + 'static
{
    fn target_ref(&self) -> &TargetRef;
    fn band_config(&self) -> anyhow::Result<BandConfig>;
    fn max_staleness_seconds(&self) -> u64;
    fn cooldown_seconds(&self) -> u64;
    /// The RETIRED `spec.dryRun` boolean, verbatim.
    ///
    /// **This is not the carve gate and has not been since 2026-06-19
    /// (`76924b0`).** It is read at exactly two sites in the whole workspace —
    /// `HostParamBand`'s and `KubeParamBand`'s two-state `promotion_mode`
    /// overrides — and by no other band kind. For every k8s / app / replica
    /// kind it is inert. Use [`Band::resolve_gate`] (or its bool projection
    /// [`Band::effective_dry_run`]) to ask whether a write is authorized.
    ///
    /// The field is kept, not dropped: it is the record of a decision an
    /// operator authored, and silently deleting authored intent is how this
    /// defect class propagates.
    /// **Which dimension this kind carves** — the one vocabulary shared with
    /// every operator surface (`breathe-facade` dispatches its `kube::Api<T>` on
    /// this exact enum, so a kind that ships without an id cannot be reached by
    /// the MCP, REST, GraphQL or gRPC).
    ///
    /// Deliberately a **required** method with no default: a new band kind must
    /// state its dimension, because the alternative — a default that silently
    /// picks one — is how five shipped kinds went invisible to every surface in
    /// the first place.
    fn dimension_id(&self) -> breathe_provider::DimensionId;
    fn dry_run(&self) -> bool;
    /// The band's authored [`WriteIntent`], if any — **the first and highest
    /// link** in the resolution chain (`writeIntent` > `mode` > the compiled
    /// `ShadowConfirmEffect`).
    ///
    /// Defaults to `None` so every band kind compiles unchanged; each kind
    /// overrides it to read its own `spec.writeIntent`.
    fn write_intent_spec(&self) -> Option<&WriteIntentSpec> {
        None
    }
    /// The band's explicit, RETIRED `spec.mode`, if authored — the second link
    /// in the resolution chain, below [`Band::write_intent`]. `None` ⇒ fall
    /// through to the compiled `ShadowConfirmEffect` default (or, for the two
    /// param kinds that override [`Band::promotion_mode`], to their two-state
    /// `dryRun` reading).
    fn mode_spec(&self) -> Option<PromotionMode>;
    /// The band's authored `spec.boundIntroduction`, if any.
    ///
    /// Defaults to `None` so every band kind compiles unchanged; the six
    /// `band_kind!` kinds override it to read their own field. The kinds that do
    /// NOT (host-param / kube-param / app / replica) address knobs whose absence
    /// is not "unconstrained", so the gate cannot fire for them regardless — see
    /// `LimitLayout::absence_is_unconstrained`.
    fn bound_introduction_spec(&self) -> Option<BoundIntroductionSpec> {
        None
    }
    /// **May this band create a bound the target's author never declared?**
    /// Resolves the authored `spec.boundIntroduction` against the compiled
    /// default, which is `Forbidden` — capping a deliberately-uncapped workload
    /// is a new constraint, and the two directions are not symmetric (failing to
    /// right-size wastes quota; capping cluster DNS can wedge a cluster).
    fn bound_introduction(&self) -> BoundIntroduction {
        self.bound_introduction_spec().map_or(BoundIntroduction::Forbidden, BoundIntroductionSpec::to_control)
    }
    /// The clean-observation window (seconds) a `ShadowConfirmEffect` band holds
    /// Ready-and-healthy before it auto-promotes to carving.
    fn confirm_after_seconds(&self) -> u64;
    fn last_change_epoch(&self) -> Option<i64>;
    /// The band's restart policy — the golden/ceiling gate (default golden
    /// `RestartFreeOnly`). A carve whose class this forbids is deferred, not rolled.
    fn disruption_policy(&self) -> DisruptionPolicy;
    /// `true` ⇒ the band is FROZEN (skip observe/plan/act; leave the limit as-is).
    fn suspended(&self) -> bool;
    /// A break-glass forced limit value (parsed in the band's unit), or `None`.
    fn force_limit_value(&self) -> Option<u64>;
    /// RFC3339 expiry of the forced limit, if any.
    fn force_limit_expiry(&self) -> Option<&str>;
    /// The band's CURRENT status (read before reconcile) — the `prior` that
    /// `status_for` carries cumulative counters + the cooldown epoch forward from.
    fn status(&self) -> Option<&BandStatus>;

    // ── BreathePosture (default methods; one 3-tier fold for every band kind) ──
    //
    // A `BreathePosture` names a default policy for the 8 behavioral fields
    // (setpoint/growAbove/growFactor/shrinkBelow/shrinkFactor/cooldownSeconds/
    // maxStalenessSeconds/disruptionPolicy) so N bands can share one tuple by
    // reference instead of copy-pasting it. The fold is always THREE tiers,
    // resolved fresh on every call (never cached/baked once — see
    // `theory/INVARIANT-BY-CONSISTENCY-AND-CONTROLLER.md`): an EXPLICIT value
    // on this band's own spec always wins; else the referenced posture's
    // value; else the crate's existing compiled default. `floor`/`ceiling`/
    // `requestFloor`/`targetRef`/`dryRun`/`mode` are NEVER posture-derived —
    // see `posture.rs`'s module doc for why that's a structural, not just
    // disciplined, invariant.
    //
    // The default implementations below simply ignore `posture` and echo back
    // each kind's own already-resolved 2-tier (override > compiled-default)
    // accessor — correct, zero-behavior-change, for every `Band` impl that
    // carries no `postureRef` field (`HostParamBand`/`KubeParamBand`/
    // `AppBand`/`ReplicaBand` today). `band_kind!`'s six macro-stamped kinds
    // (MemoryBand/CpuBand/StorageBand/ArcBand/CgroupBand/CgroupCpuBand)
    // override every one of these with the real fold over their raw
    // `Option<T>` spec fields.

    /// The named `BreathePosture` this band's unset behavioral fields fall
    /// back to, before the compiled default. `None` ⇒ no posture tier (2-tier
    /// fold only: override then compiled default) — either because the field
    /// is genuinely unset, or because this `Band` kind doesn't carry a
    /// `postureRef` at all.
    fn posture_ref(&self) -> Option<&str> {
        None
    }
    /// The posture-aware [`BandConfig`] for this tick — identical to
    /// [`Band::band_config`] except the 5 tunable numeric fields
    /// (setpoint/growAbove/shrinkBelow/growFactor/shrinkFactor) additionally
    /// fall through `posture` (when this band references one) before the
    /// compiled default. `floor`/`ceiling`/`requestFloor`/`warmupSeconds` are
    /// read straight from this band's own spec regardless of `posture` — see
    /// the module-level safety invariant.
    fn band_config_with_posture(&self, posture: Option<&BreathePostureSpec>) -> anyhow::Result<BandConfig> {
        let _ = posture;
        self.band_config()
    }
    /// Posture-aware [`Band::cooldown_seconds`].
    fn cooldown_seconds_with_posture(&self, posture: Option<&BreathePostureSpec>) -> u64 {
        let _ = posture;
        self.cooldown_seconds()
    }
    /// Posture-aware [`Band::max_staleness_seconds`].
    fn max_staleness_seconds_with_posture(&self, posture: Option<&BreathePostureSpec>) -> u64 {
        let _ = posture;
        self.max_staleness_seconds()
    }
    /// Posture-aware [`Band::disruption_policy`].
    fn disruption_policy_with_posture(&self, posture: Option<&BreathePostureSpec>) -> DisruptionPolicy {
        let _ = posture;
        self.disruption_policy()
    }

    // ── Promotion lifecycle (default methods; one law for every band kind) ─────

    /// Resolve the effective promotion lifecycle. Permanent shadow (never carve)
    /// is reachable ONLY through the EXPLICIT `mode: shadow` — a deliberate,
    /// eyes-open critical-path hold. It is NOT reachable through the legacy
    /// `dryRun:true` boolean: a band parked never-live by a bare boolean is the
    /// anti-pattern (it backs into a state with no exit). So when no explicit
    /// `mode` is set, the lifecycle is always the bounded `ShadowConfirmEffect`
    /// — start shadowed, then auto-promote to carving once the clean-observation
    /// window proves it safe — regardless of `dryRun`. `dryRun:true` with no
    /// `mode` therefore means exactly "start shadowed and calibrate" (which the
    /// default already does), never "shadow forever". This makes "the band never
    /// goes live" a state that is unrepresentable without explicit operator intent.
    fn promotion_mode(&self) -> PromotionMode {
        self.mode_spec().unwrap_or(PromotionMode::ShadowConfirmEffect)
    }

    /// The operator fast-path: `breathe.pleme.io/confirmed: "true"` promotes now.
    fn operator_confirmed(&self) -> bool {
        self.meta()
            .annotations
            .as_ref()
            .and_then(|a| a.get(CONFIRMED_ANNOTATION))
            .is_some_and(|v| v == "true")
    }

    /// Has the `ShadowConfirmEffect` confirm gate passed? True iff the operator
    /// confirmed, OR the band has held Ready ∧ ¬Stale ∧ ¬Conflict continuously
    /// for `confirmAfterSeconds`. Reads the prior status conditions — a band that
    /// loses its metric (Ready=False) safely falls back to shadow.
    ///
    /// Delegates to `outorga::PromotionPolicy::confirm_gate` — the k8s-free lift
    /// of this exact algebra ([`formigueiro`](https://github.com/pleme-io/formigueiro)),
    /// extracted from this trait on 2026-06-30. `mode` is fixed to
    /// `ShadowConfirmEffect` here because this method IS the confirm-gate check;
    /// `outorga`'s `confirm_gate` itself never consults the policy's mode.
    fn confirm_gate_passed(&self, now_epoch: i64) -> bool {
        let policy = outorga::PromotionPolicy::new(outorga::PromotionMode::ShadowConfirmEffect)
            .confirm_after(self.confirm_after_seconds());
        matches!(policy.confirm_gate(&BandObservation(self), now_epoch), outorga::ConfirmGate::Passed)
    }

    /// Which pre-`writeIntent` path authorized a write, for attribution in
    /// status. Only meaningful when the legacy chain actually applied; every
    /// arm is migration debt whose burn-down is the definition of done for the
    /// authorization refactor.
    fn legacy_path(&self) -> LegacyPath {
        match (self.mode_spec(), self.promotion_mode()) {
            // Authored through the retired field.
            (Some(PromotionMode::Effect), _) => LegacyPath::ModeEffect,
            // No `mode`, yet the kind resolved to Effect ⇒ it overrode
            // `promotion_mode` with the two-state `dryRun` reading. Exactly two
            // kinds do this: `HostParamBand` and `KubeParamBand`.
            (None, PromotionMode::Effect) => LegacyPath::TwoStateDryRun,
            _ if self.operator_confirmed() => LegacyPath::OperatorAnnotation,
            _ => LegacyPath::ConfirmGate {
                required_secs: i64::try_from(self.confirm_after_seconds()).unwrap_or(i64::MAX),
            },
        }
    }

    /// **Resolve this tick's authorization verdict** — the typed replacement
    /// for the bare `effectiveDryRun` bool.
    ///
    /// Precedence, highest first: an external `frozen` key (the pool/fleet
    /// master switch) ⇒ the authored [`WriteIntent`] ⇒ the legacy chain
    /// (`mode`, else the compiled `ShadowConfirmEffect`). `spec.dryRun` is
    /// **not** in that list — see [`Band::dry_run`].
    ///
    /// The confirm-gate math is NOT re-implemented here: it is
    /// `outorga::PromotionPolicy`'s, computed below and handed to
    /// `breathe_provider::gate::resolve_gate` as an input, so the fleet keeps
    /// exactly one tested FSM.
    ///
    /// **Additive by construction:** when no `writeIntent` is authored — which
    /// is every CR in existence as of this change — the verdict is derived from
    /// the very same `outorga::PromotionPolicy::decide` call the previous
    /// implementation made, with the same arguments. Behaviour is byte-identical;
    /// only the verdict's *type* (and hence its legibility) changes.
    fn resolve_gate(&self, now_epoch: i64, frozen: bool) -> EffectiveGate {
        // Parse the authored wire value at the border. A malformed intent
        // (`{intent: write}` naming no author) resolves to a fail-safe,
        // clearly-named shadow rather than a granted anonymous write.
        let intent = self.write_intent_spec().map(WriteIntentSpec::parse);

        // The confirm gate is consulted only for a calibrating intent; the
        // legacy path runs its own gate inside `decide` below.
        let confirm = match &intent {
            Some(Ok(WriteIntent::CalibrateThenWrite { confirm_after_seconds })) => {
                let obs = BandObservation(self);
                let policy = outorga::PromotionPolicy::new(outorga::PromotionMode::ShadowConfirmEffect)
                    .confirm_after(*confirm_after_seconds);
                let required_secs = i64::try_from(*confirm_after_seconds).unwrap_or(i64::MAX);
                match policy.confirm_gate(&obs, now_epoch) {
                    outorga::ConfirmGate::Passed => {
                        if outorga::Observation::operator_confirmed(&obs) {
                            ConfirmVerdict::OperatorConfirmed
                        } else {
                            let since = outorga::Observation::ready_since(&obs).unwrap_or(now_epoch);
                            ConfirmVerdict::Passed {
                                ready_since_epoch: since,
                                held_secs: (now_epoch - since).max(0),
                                required_secs,
                            }
                        }
                    }
                    outorga::ConfirmGate::Pending { held_secs, need_secs } => {
                        ConfirmVerdict::Pending { held_secs, required_secs: need_secs }
                    }
                    outorga::ConfirmGate::Blocked(r) => ConfirmVerdict::Blocked(map_shadow_reason(r)),
                }
            }
            _ => ConfirmVerdict::NotEvaluated,
        };

        // The legacy chain, EXACTLY as before. `frozen` is passed as `false`
        // here because `resolve_gate` applies the freeze key itself, ahead of
        // everything — the composed result is identical either way (outorga
        // also short-circuits on `frozen`), and this keeps the legacy path's
        // attribution meaningful rather than always reading `Frozen`.
        let legacy = match outorga::PromotionPolicy::new(self.promotion_mode().to_outorga())
            .confirm_after(self.confirm_after_seconds())
            .decide(&BandObservation(self), now_epoch, false)
        {
            outorga::PromotionDecision::Apply => LegacyDecision::Apply(self.legacy_path()),
            outorga::PromotionDecision::Shadow(r) => LegacyDecision::Shadow(map_shadow_reason(r)),
        };

        gate::resolve_gate(&GateInputs { intent, frozen, confirm, legacy })
    }

    /// The EFFECTIVE dry-run for this tick, derived from the promotion lifecycle.
    /// THIS — not the raw `dryRun` field — is what gates the carve. Equivalent to
    /// [`Band::effective_dry_run_frozen`] with `frozen = false` (no external
    /// freeze key applies at this trait's own call sites).
    fn effective_dry_run(&self, now_epoch: i64) -> bool {
        self.effective_dry_run_frozen(now_epoch, false)
    }

    /// The full TWO-KEY effective dry-run: this band's own promotion gate AND an
    /// external FREEZE (a pool/fleet master write switch) — `outorga`'s two-key
    /// rule (`PromotionPolicy::decide`'s `frozen` parameter) threaded through
    /// explicitly instead of folded into the raw `dry_run()` field. A blind
    /// `dry_run() || !some_switch` composition is exactly the bug this closes:
    /// `breathe-host-agent`'s generic host reconcile (ArcBand/CgroupBand/
    /// CgroupCpuBand/HostParamBand) used to compose the RAW `dry_run()` field
    /// with the node's `BreatheNodePool.writeEnabled`, bypassing the
    /// confirm-gate/auto-promote lifecycle the k8s-plane controller already
    /// gives the SAME CRD kinds — call this instead, with
    /// `frozen = !pool.spec.write_enabled`.
    ///
    /// Now a projection of [`Band::resolve_gate`] rather than a second,
    /// independently-drifting derivation of the same rule — the bool an
    /// existing consumer wants, folded out of the typed verdict.
    fn effective_dry_run_frozen(&self, now_epoch: i64, frozen: bool) -> bool {
        self.resolve_gate(now_epoch, frozen).is_shadow()
    }
    /// M0 PREDICTIVE: `Some(lookahead_secs)` when the band opts into preemptive
    /// carving (`predictive: true`) — the controller measures the working-set
    /// velocity and feeds `PredictiveGrow` so the limit pre-grows for the burst
    /// the instantaneous reading misses. `None` (default) ⇒ plain reactive carving.
    fn predictive(&self) -> Option<f64>;
    /// The trailing-window PEAK decay per tick `∈ [0,1)` — the never-OOM shrink
    /// floor is keyed on the demonstrated peak working set, which decays by this
    /// each tick so a real spike holds the floor for a meaningful window (the
    /// authentik-Celery OOM fix). Default 0.98; band kinds override from their spec.
    fn peak_decay(&self) -> f64 {
        0.98
    }
    /// WARMUP HOLD (seconds) — the minimum observed-since-restart age before a SHRINK
    /// is permitted (the un-observed-boot-spike gate). `0` disables. Default 600s;
    /// band kinds override from their spec. Host dimensions (no restart concept)
    /// keep the default but the reconcile layer feeds `observed_for_secs = u64::MAX`
    /// so the gate never fires for them.
    fn warmup_seconds(&self) -> u64 {
        600
    }
    /// `metadata.generation` — set as `status.observedGeneration` so an operator can
    /// confirm the controller reconciled their latest spec edit.
    fn generation(&self) -> Option<i64> {
        self.meta().generation
    }
}

// ─────────────── outorga — the shared promotion-lifecycle FSM ───────────────
//
// `outorga` (https://github.com/pleme-io/formigueiro) is the k8s-free lift of
// THIS trait's own shadow→confirm→effect algebra, extracted from this crate
// on 2026-06-30 so formigueiro (fleet updates) and breathe (resource
// homeostasis) stand on one tested FSM. Since extraction, `Band`'s own
// `confirm_gate_passed`/`effective_dry_run` had never migrated onto their own
// extracted copy — the two crates carried byte-identical, driftable logic.
// The methods above now delegate here; the two helpers below are what makes
// that delegation possible.

impl PromotionMode {
    /// The `outorga`-side mirror of this mode (identical variant set). This
    /// crate's [`PromotionMode`] stays a distinct local type because it needs
    /// `JsonSchema` for the CRD surface, which `outorga::PromotionMode`
    /// deliberately does not carry (it has no k8s dependency at all).
    #[must_use]
    fn to_outorga(self) -> outorga::PromotionMode {
        match self {
            Self::Shadow => outorga::PromotionMode::Shadow,
            Self::Effect => outorga::PromotionMode::Effect,
            Self::ShadowConfirmEffect => outorga::PromotionMode::ShadowConfirmEffect,
            Self::Suspended => outorga::PromotionMode::Suspended,
        }
    }
}

/// Adapts any [`Band`] to `outorga::Observation` — reads the Ready/Stale/
/// Conflict conditions from the band's own [`BandStatus`] (via [`Band::status`]),
/// the exact signal the confirm-gate needs. Generic over `B: Band` rather than
/// implemented once per band kind: `Observation` is `outorga`'s foreign trait,
/// and every band kind's `status()`/`operator_confirmed()` already expose
/// everything it needs uniformly through the `Band` trait itself.
struct BandObservation<'a, B: Band>(&'a B);

impl<B: Band> outorga::Observation for BandObservation<'_, B> {
    fn ready(&self) -> bool {
        self.ready_since().is_some()
    }
    fn stale(&self) -> bool {
        self.0
            .status()
            .is_some_and(|st| st.conditions.iter().any(|c| c.type_ == "Stale" && c.status == "True"))
    }
    fn conflict(&self) -> bool {
        self.0
            .status()
            .is_some_and(|st| st.conditions.iter().any(|c| c.type_ == "Conflict" && c.status == "True"))
    }
    fn ready_since(&self) -> Option<i64> {
        let st = self.0.status()?;
        let cond = st.conditions.iter().find(|c| c.type_ == "Ready")?;
        if cond.status != "True" {
            return None;
        }
        chrono::DateTime::parse_from_rfc3339(&cond.last_transition_time).ok().map(|t| t.timestamp())
    }
    fn operator_confirmed(&self) -> bool {
        self.0.operator_confirmed()
    }
}

/// A trivially-ready `outorga::Observation` — always ready (since epoch 0),
/// never stale, never conflicted, never operator-confirmed. Used by
/// [`legacy_effective_dry_run`] for the Tier-B CRD kinds
/// (`BreatheCloudPool`/`IsolationBand`/the `PodMemoryHigh` dispatch) that carry
/// a bare `dryRun` boolean and no `mode` field yet, and whose status has no
/// Ready/Stale/Conflict conditions to observe at all. Because
/// [`legacy_effective_dry_run`] only ever constructs `outorga::PromotionMode::
/// Shadow` or `::Effect` (never `ShadowConfirmEffect`), this observation is
/// never actually consulted for its readiness window — it exists purely so
/// the SAME typed `outorga::PromotionPolicy::decide` two-key call handles
/// these CRD kinds too, rather than a hand re-derived `a || !b` boolean
/// expression at each call site.
struct AlwaysReady;

impl outorga::Observation for AlwaysReady {
    fn ready(&self) -> bool {
        true
    }
    fn stale(&self) -> bool {
        false
    }
    fn conflict(&self) -> bool {
        false
    }
    fn ready_since(&self) -> Option<i64> {
        Some(0)
    }
    fn operator_confirmed(&self) -> bool {
        false
    }
}

/// The TWO-KEY promotion decision for a LEGACY-boolean Tier-B CRD kind — one
/// that carries a bare `dryRun`/`writeEnabled` pair and no `mode` field or
/// Ready/Stale/Conflict status yet (`BreatheCloudPool`, `IsolationBand`, the
/// `PodMemoryHigh` dispatch). `dry_run` selects Shadow-vs-Effect directly (pure
/// two-state — exactly [`HostParamBand`]'s own established pattern for
/// pre-FSM CRDs); `frozen` is the pool/node master write switch (mirrors
/// `BreatheNodePool.spec.writeEnabled` / `BreatheCloudPool.spec.writeEnabled` /
/// `IsolationBand.spec.writeEnabled`). Threads BOTH keys through the SAME
/// `outorga::PromotionPolicy::decide` two-key rule every `Band` uses, so every
/// "is this thing allowed to write" decision in the fleet is one tested
/// function, never three-plus hand re-derived `a || !b` expressions — and the
/// caller gets a typed [`outorga::ShadowReason`] back (was it explicitly
/// `dryRun`? frozen by the pool switch?) instead of a bare bool.
///
/// A genuine `ShadowConfirmEffect` lifecycle (calibrate, then auto-promote)
/// for these CRD kinds is a real, NAMED follow-on, not silently dropped: it
/// needs a `mode` spec field plus real Ready/Stale/Conflict status conditions
/// on `CloudPoolStatus`/`IsolationBandStatus`/`PodMemoryHighStatus` (they carry
/// none today), at which point this function's `AlwaysReady` observation is
/// swapped for a real one and `dry_run` becomes just the fallback when `mode`
/// is unset — mirroring exactly how `Band::promotion_mode`'s own
/// `mode_spec().unwrap_or(ShadowConfirmEffect)` already works. Deliberately not
/// done here: it would add CRD schema fields to CRD kinds live on rio, out of
/// scope for a same-behavior migration.
#[must_use]
pub fn legacy_effective_dry_run(dry_run: bool, frozen: bool) -> outorga::PromotionDecision {
    let mode = if dry_run { outorga::PromotionMode::Shadow } else { outorga::PromotionMode::Effect };
    outorga::PromotionPolicy::new(mode).decide(&AlwaysReady, 0, frozen)
}

#[allow(clippy::too_many_arguments)]
fn band_config_of(
    setpoint: f64,
    grow_above: f64,
    shrink_below: f64,
    grow_factor: f64,
    shrink_factor: f64,
    floor: &str,
    ceiling: &str,
    request_floor: &str,
    warmup_seconds: u64,
    unit: Unit,
) -> anyhow::Result<BandConfig> {
    let parse = |q: &str| -> anyhow::Result<u64> {
        unit.parse(q)
            .ok_or_else(|| anyhow::anyhow!("invalid {unit:?} quantity {q:?}"))
    };
    // An empty request_floor ⇒ no declared request floor (0). A malformed one is a
    // typed parse error (never silently a wrong floor).
    let request_floor_bytes = if request_floor.is_empty() { 0 } else { parse(request_floor)? };
    Ok(BandConfig {
        grow_above,
        shrink_below,
        setpoint,
        grow_factor,
        shrink_factor,
        floor_bytes: parse(floor)?,
        ceiling_bytes: parse(ceiling)?,
        request_floor_bytes,
        warmup_seconds,
        // Default to the safe split-brain policy; a band CRD knob can override it
        // when the field is added to the spec (currently the proven default).
        metric_missing_policy: breathe_control::MetricMissingPolicy::default(),
    })
}

/// Stamp one band CRD kind + its [`Band`] impl from the shared field set. Each
/// kind carries its own [`Unit`] (so cpu parses millicores, memory/storage
/// bytes) and its own unit-appropriate floor/ceiling defaults (passed as
/// `serde(default = …)` fn names so an omitted floor on a `CpuBand` defaults to
/// `250m`, not the byte default `256Mi` which would fail to parse as cpu).
macro_rules! band_kind {
    ($spec:ident, $kind:ident, $kindlit:literal, $short:literal, $unit:expr, $dfloor:literal, $dceiling:literal, $dim:expr) => {
        #[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
        #[kube(
            group = "breathe.pleme.io",
            version = "v1",
            kind = $kindlit,
            namespaced,
            status = "BandStatus",
            shortname = $short,
            category = "breathe",
            printcolumn = r#"{"name":"Target","type":"string","jsonPath":".spec.targetRef.kind"}"#,
            printcolumn = r#"{"name":"Name","type":"string","jsonPath":".spec.targetRef.name"}"#,
            printcolumn = r#"{"name":"Util","type":"string","jsonPath":".status.lastUtil"}"#,
            printcolumn = r#"{"name":"Limit","type":"string","jsonPath":".status.currentLimit"}"#,
            printcolumn = r#"{"name":"Last","type":"string","jsonPath":".status.lastDecision"}"#,
            printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
            // The question `kubectl get` could not answer before: is this band
            // WRITING to my cluster, and why. `dryRun` was never the answer.
            printcolumn = r#"{"name":"Gate","type":"string","jsonPath":".status.effectiveGate.state"}"#,
            printcolumn = r#"{"name":"Why","type":"string","jsonPath":".status.effectiveGate.reason"}"#,
            printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.conditions[?(@.type=='Ready')].status"}"#
        )]
        #[serde(rename_all = "camelCase")]
        pub struct $spec {
            pub target_ref: TargetRef,
            /// The named [`BreathePosture`] this band's UNSET behavioral fields (the
            /// 8 below) fall back to before the compiled default. `None` ⇒ compiled
            /// defaults only. Structurally cannot carry `floor`/`ceiling`/
            /// `targetRef`/`dryRun`/`mode` (those live only on this band's own
            /// spec) — a posture patch can never widen a capacity bound or flip a
            /// promotion state fleet-wide. See `posture.rs`'s module doc.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub posture_ref: Option<String>,
            /// EXPLICIT override. `None` ⇒ resolve via the 3-tier fold (the
            /// referenced posture, then the compiled default) — see
            /// `Band::band_config_with_posture`.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub setpoint: Option<f64>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub grow_above: Option<f64>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub shrink_below: Option<f64>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub grow_factor: Option<f64>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub shrink_factor: Option<f64>,
            #[serde(default = $dfloor)]
            pub floor: String,
            #[serde(default = $dceiling)]
            pub ceiling: String,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub cooldown_seconds: Option<u64>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub max_staleness_seconds: Option<u64>,
            /// RETIRED 2026-06-19 (breathe@76924b0) — **this field has NO
            /// effect on this band kind.** It is not read by the carve gate;
            /// setting it to `true` does not hold the band in shadow, and a
            /// band with `dryRun: true` and no `mode` carves for real once its
            /// confirm window elapses.
            ///
            /// It is kept, not deleted, because it is the record of a decision
            /// an operator authored — but it decides nothing. Authorization is
            /// `spec.writeIntent`; the live verdict (and its reason or witness)
            /// is `status.effectiveGate`.
            #[serde(default)]
            pub dry_run: bool,
            /// **The authorization intent — what this band is permitted to do
            /// and who says so.** The first and highest link in the resolution
            /// chain `writeIntent` > `mode` > the compiled `shadowConfirmEffect`.
            ///
            /// * `{intent: observe}` — decide, report, attest; never write.
            /// * `{intent: calibrateThenWrite, confirmAfterSeconds: 1800}` —
            ///   shadow until a clean-observation window proves the band safe,
            ///   then write.
            /// * `{intent: write, authorizedBy: "…"}` — write now.
            /// * `{intent: frozen}` — never write, but keep observing.
            ///
            /// `authorizedBy` is REQUIRED on `write`: an `{intent: write}`
            /// naming no `authorizedBy` never goes live: it is held in shadow
            /// as `intentMalformed`. NOTE the tier — that is a runtime
            /// mitigation, not an apiserver rejection: a k8s structural schema
            /// cannot express "this property is required only when another
            /// property has this value", so the API accepts the object and the
            /// controller refuses to act on it. (This description said
            /// "rejected at parse time" until 2026-07-26; it was not.)
            ///
            /// Unset ⇒ the retired `mode`/default chain decides, and
            /// `status.effectiveGate.witness` reports `legacyDefault` so an
            /// unauthored live band is visible as such.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub write_intent: Option<WriteIntentSpec>,
            /// The RETIRED promotion lifecycle. Still read (below
            /// `writeIntent`), so an already-authored CR keeps working; new CRs
            /// should author `writeIntent` instead. Unset ⇒ the compiled fleet
            /// default `shadowConfirmEffect`.
            ///
            /// NOTE: unset does **not** mean "derived from `dryRun`" — that
            /// resolution was removed on 2026-06-19 (`76924b0`) and this
            /// description said otherwise until 2026-07-26. Values:
            /// `shadow` | `effect` | `shadowConfirmEffect` | `suspended`.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub mode: Option<PromotionMode>,
            /// Clean-observation window (seconds) a `ShadowConfirmEffect` band holds
            /// Ready-and-healthy before it auto-promotes to carving (default 1800).
            #[serde(default = "d_confirm_after")]
            pub confirm_after_seconds: u64,
            /// **May breathe create a bound this target's author never declared?**
            /// Default `forbidden`.
            ///
            /// Right-sizing an existing limit and newly capping a workload that was
            /// deliberately left uncapped look identical to a controller reading
            /// `limit == 0` — and breathe was doing the second while believing it did
            /// the first. On camelot-eks it introduced a cpu limit onto `coredns` and
            /// `ebs-csi-controller`, neither of which declares one.
            ///
            /// * `forbidden` (default) — a target with no declared bound reports
            ///   phase `NoLimit` and is left alone.
            /// * `allowed` — breathe may seed a bound. This is the CEDED-FIELD case
            ///   (a CNPG/Flux manager relinquishing `limits.memory` for breathe to
            ///   take over) and the deliberate "cap this noisy neighbour" case. It
            ///   has to be said out loud now, rather than inferred from a zero.
            ///
            /// Inert for a band whose layout's absence does not mean "unconstrained"
            /// (a PVC always has a size; a sysctl always has a value; `0` replicas is
            /// a real count) — see `LimitLayout::absence_is_unconstrained`.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub bound_introduction: Option<BoundIntroductionSpec>,
            /// The golden/ceiling gate (default `restartFreeOnly`). `None` ⇒ resolve
            /// via the 3-tier fold, same as the other 7 behavioral fields.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub disruption_policy: Option<DisruptionPolicy>,
            /// Free-text justification for an EXPLICIT per-CR `disruptionPolicy`
            /// override — carries the "why" WITH the CR when it deliberately
            /// diverges from its posture/compiled default (e.g. "genuinely stateful;
            /// no in-place resize available"). Purely documentary; never read by the
            /// fold.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub disruption_policy_rationale: Option<String>,
            /// FREEZE this band — `true` ⇒ the controller skips observe/plan/act
            /// entirely (phase `Suspended`), the limit is left exactly as-is. Distinct
            /// from `dryRun` (which still observes + reports what it WOULD do): suspend
            /// is "stop deciding". Resume with `suspend:false`. The k8s-native pause.
            #[serde(default, skip_serializing_if = "std::ops::Not::not")]
            pub suspend: bool,
            /// BREAK-GLASS: pin the limit to exactly this value (a quantity string in
            /// the band's unit, e.g. `8Gi` / `2`). breathe skips the band law and
            /// carves to it — but STILL through the gate (DisruptionPolicy + the
            /// single-writer guard + the L2 ceiling all apply; it cannot bypass
            /// safety). Clear it to resume normal homeostasis. Pair with
            /// `forceLimitExpiry` for an auto-releasing pin.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub force_limit: Option<String>,
            /// RFC3339 time after which `forceLimit` is ignored (auto-release the pin).
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub force_limit_expiry: Option<String>,
            /// M0 PREDICTIVE (opt-in, default off → behaviour byte-unchanged): when
            /// `true`, breathe measures the working-set velocity and pre-grows the
            /// limit for the projected burst via the proven `PredictiveGrow<BandLaw>`
            /// — asymmetric (only ever raises a grow), still `safety_clamp`-contained
            /// (the never-OOM oracle covers it). Shadow-first: observe the predictive
            /// grows under `dryRun` before promoting to live.
            #[serde(default, skip_serializing_if = "std::ops::Not::not")]
            pub predictive: bool,
            /// Forecast horizon for predictive carving (seconds). Default 60s
            /// (≈ refresh + cooldown for memory); set higher for slow-filling
            /// resources (storage = resize-cooldown × safety factor).
            #[serde(default = "d_predictive_lookahead")]
            pub predictive_lookahead_seconds: u64,
            /// The operator's declared `requests.<resource>` floor (a quantity
            /// string in the band's unit, e.g. `512Mi` / `250m`). A shrink can NEVER
            /// carve the limit below this — requests is the scheduler's guaranteed
            /// working set, and a limit under the request is both invalid in k8s and
            /// unsafe. Empty (the default) ⇒ no request floor. Typically mirrors the
            /// workload's actual `resources.requests.<resource>`.
            #[serde(default, skip_serializing_if = "String::is_empty")]
            pub request_floor: String,
            /// The trailing-window PEAK decay per tick `∈ [0,1)` — the never-OOM
            /// shrink floor is keyed on the demonstrated PEAK working set (max RSS),
            /// which decays geometrically by this factor each tick so a real spike
            /// raises the floor and HOLDS it for a meaningful window rather than
            /// evaporating on the next low-water sample (the authentik-Celery OOM
            /// fix). Default 0.98 (a spike holds for ~tens of ticks); `0.0` = pure
            /// single-tick max (no window memory).
            #[serde(default = "d_peak_decay")]
            pub peak_decay: f64,
            /// WARMUP HOLD (seconds) — the minimum time a workload must be OBSERVED
            /// since its last (re)start before any SHRINK is permitted. A workload
            /// that restarted less than this ago has not demonstrated a full duty
            /// cycle, so its idle reading is not yet proof the slack is safe to
            /// reclaim: a shrink is HELD (phase `Warmup`) until the window elapses. A
            /// grow is never held. This closes the un-observed-boot-spike OOM (the
            /// authentik worker's blueprint-discovery spike happens at boot, before
            /// the first scrape, so the demonstrated-peak floor only ever saw idle).
            /// Default 600s (10 min); `0` disables the gate.
            #[serde(default = "d_warmup_seconds")]
            pub warmup_seconds: u64,
        }

        impl crate::Band for $kind {
            fn target_ref(&self) -> &TargetRef {
                &self.spec.target_ref
            }
            fn band_config(&self) -> anyhow::Result<BandConfig> {
                let s = &self.spec;
                crate::band_config_of(
                    s.setpoint.unwrap_or_else(d_setpoint),
                    s.grow_above.unwrap_or_else(d_grow_above),
                    s.shrink_below.unwrap_or_else(d_shrink_below),
                    s.grow_factor.unwrap_or_else(d_grow_factor),
                    s.shrink_factor.unwrap_or_else(d_shrink_factor),
                    &s.floor, &s.ceiling, &s.request_floor, s.warmup_seconds, $unit,
                )
            }
            fn peak_decay(&self) -> f64 {
                self.spec.peak_decay
            }
            fn warmup_seconds(&self) -> u64 {
                self.spec.warmup_seconds
            }
            fn max_staleness_seconds(&self) -> u64 {
                self.spec.max_staleness_seconds.unwrap_or_else(d_max_staleness)
            }
            fn cooldown_seconds(&self) -> u64 {
                self.spec.cooldown_seconds.unwrap_or_else(d_cooldown)
            }
            fn dimension_id(&self) -> breathe_provider::DimensionId {
                $dim
            }
            fn dry_run(&self) -> bool {
                self.spec.dry_run
            }
            fn write_intent_spec(&self) -> Option<&WriteIntentSpec> {
                self.spec.write_intent.as_ref()
            }
            fn mode_spec(&self) -> Option<PromotionMode> {
                self.spec.mode
            }
            fn bound_introduction_spec(&self) -> Option<BoundIntroductionSpec> {
                self.spec.bound_introduction
            }
            fn confirm_after_seconds(&self) -> u64 {
                self.spec.confirm_after_seconds
            }
            fn last_change_epoch(&self) -> Option<i64> {
                self.status.as_ref().and_then(|s| s.last_change_epoch)
            }
            fn disruption_policy(&self) -> DisruptionPolicy {
                self.spec.disruption_policy.unwrap_or_default()
            }
            fn suspended(&self) -> bool {
                self.spec.suspend
            }
            fn force_limit_value(&self) -> Option<u64> {
                self.spec.force_limit.as_deref().and_then(|q| $unit.parse(q))
            }
            fn force_limit_expiry(&self) -> Option<&str> {
                self.spec.force_limit_expiry.as_deref()
            }
            fn predictive(&self) -> Option<f64> {
                self.spec
                    .predictive
                    .then_some(self.spec.predictive_lookahead_seconds as f64)
            }
            fn status(&self) -> Option<&BandStatus> {
                self.status.as_ref()
            }

            // ── BreathePosture 3-tier fold — the real implementation. Every
            // other `Band` impl (HostParamBand/KubeParamBand/AppBand/ReplicaBand)
            // keeps the trait's default (posture-blind) methods; these six
            // macro-stamped kinds are the ones that actually carry a
            // `postureRef` + `Option<T>` behavioral fields to fold over.
            fn posture_ref(&self) -> Option<&str> {
                self.spec.posture_ref.as_deref()
            }
            fn band_config_with_posture(&self, posture: Option<&BreathePostureSpec>) -> anyhow::Result<BandConfig> {
                let s = &self.spec;
                crate::band_config_of(
                    s.setpoint.or_else(|| posture.map(|p| p.setpoint)).unwrap_or_else(d_setpoint),
                    s.grow_above.or_else(|| posture.map(|p| p.grow_above)).unwrap_or_else(d_grow_above),
                    s.shrink_below.or_else(|| posture.map(|p| p.shrink_below)).unwrap_or_else(d_shrink_below),
                    s.grow_factor.or_else(|| posture.map(|p| p.grow_factor)).unwrap_or_else(d_grow_factor),
                    s.shrink_factor.or_else(|| posture.map(|p| p.shrink_factor)).unwrap_or_else(d_shrink_factor),
                    &s.floor, &s.ceiling, &s.request_floor, s.warmup_seconds, $unit,
                )
            }
            fn cooldown_seconds_with_posture(&self, posture: Option<&BreathePostureSpec>) -> u64 {
                self.spec
                    .cooldown_seconds
                    .or_else(|| posture.map(|p| u64::from(p.cooldown_seconds)))
                    .unwrap_or_else(d_cooldown)
            }
            fn max_staleness_seconds_with_posture(&self, posture: Option<&BreathePostureSpec>) -> u64 {
                self.spec
                    .max_staleness_seconds
                    .or_else(|| posture.map(|p| u64::from(p.max_staleness_seconds)))
                    .unwrap_or_else(d_max_staleness)
            }
            fn disruption_policy_with_posture(&self, posture: Option<&BreathePostureSpec>) -> DisruptionPolicy {
                self.spec
                    .disruption_policy
                    .or_else(|| posture.map(|p| p.disruption_policy))
                    .unwrap_or_default()
            }
        }
    };
}

band_kind!(MemoryBandSpec, MemoryBand, "MemoryBand", "mband", Unit::Bytes, "d_floor_bytes", "d_ceiling_bytes", breathe_provider::DimensionId::Memory);
band_kind!(CpuBandSpec, CpuBand, "CpuBand", "cband", Unit::Millicores, "d_floor_milli", "d_ceiling_milli", breathe_provider::DimensionId::Cpu);
band_kind!(StorageBandSpec, StorageBand, "StorageBand", "sband", Unit::Bytes, "d_storage_floor_bytes", "d_storage_ceiling_bytes", breathe_provider::DimensionId::Storage);
// HOST bands — the descriptor (breathe-host) encodes the host addressing; the
// CRD shape is identical to the byte-valued k8s bands, so the same macro stamps
// them. targetRef.name carries the systemd unit (CgroupBand) or the node
// (ArcBand); the agent applies via HostCluster within the BreatheNodePool L2 ceiling.
band_kind!(ArcBandSpec, ArcBand, "ArcBand", "aband", Unit::Bytes, "d_floor_bytes", "d_ceiling_bytes", breathe_provider::DimensionId::Arc);
band_kind!(CgroupBandSpec, CgroupBand, "CgroupBand", "gband", Unit::Bytes, "d_floor_bytes", "d_ceiling_bytes", breathe_provider::DimensionId::Cgroup);
// HOST cpu band — the unit's transient CPUQuota cap, millicores (like CpuBand).
band_kind!(CgroupCpuBandSpec, CgroupCpuBand, "CgroupCpuBand", "gcband", Unit::Millicores, "d_floor_milli", "d_ceiling_milli", breathe_provider::DimensionId::CgroupCpu);

// ───────────── HostParamBand — the GENERIC sysctl / ZFS-param band (PR-2) ─────────────
// Hand-rolled (not band_kind!) because it carries EXTRA spec fields — the knob,
// the metric, and the per-instance directionality — that `band_kind!`'s fixed
// shape can't express. Every vm.*/net.*/fs.* sysctl + every ZFS module param is
// a CR INSTANCE of this ONE kind (PR-2's "collapse the family to data"). Value is
// a bare u64 straight through the sysfs/procfs seam; floor/ceiling parse as Bytes
// (which also accepts bare integers for count-valued params like fs.file-max).

/// Which host lever a [`HostParamBand`] carves (the serializable mirror of
/// `breathe_provider::HostKnob`'s generic arms).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum HostKnobSpec {
    /// A dotted sysctl key (`vm.dirty_bytes`) → `/proc/sys/vm/dirty_bytes`.
    Sysctl { key: String },
    /// A ZFS module parameter (`zfs_arc_min`) → `/sys/module/zfs/parameters/zfs_arc_min`.
    ZfsParam { param: String },
    /// A systemd unit's per-device `io.max` cap — Step-4. `field` is one of
    /// `rbps`/`wbps`/`riops`/`wiops`; `device` is `<maj>:<min>`.
    CgroupIoMax { unit: String, device: String, field: IoMaxFieldSpec },
}

/// Which `io.max` sub-knob a [`HostKnobSpec::CgroupIoMax`] carves (serde mirror of
/// `breathe_provider::IoMaxField`). `bps` fields are bytes/s, `iops` are ops/s.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum IoMaxFieldSpec {
    Rbps,
    Wbps,
    Riops,
    Wiops,
}

impl IoMaxFieldSpec {
    #[must_use]
    fn provider(self) -> breathe_provider::IoMaxField {
        match self {
            Self::Rbps => breathe_provider::IoMaxField::Rbps,
            Self::Wbps => breathe_provider::IoMaxField::Wbps,
            Self::Riops => breathe_provider::IoMaxField::Riops,
            Self::Wiops => breathe_provider::IoMaxField::Wiops,
        }
    }
}

/// Where a [`HostParamBand`] reads its `used` signal (mirror of the generic
/// `breathe_provider::HostMetric` arms).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum HostMetricSpec {
    /// A `/proc/meminfo` field in bytes (`Dirty`, `MemFree`, `Writeback`).
    MeminfoField { field: String },
    /// A named `/proc/spl/kstat/zfs/arcstats` row (`size`, `dnode_size`).
    ArcstatsRow { row: String },
    /// A systemd unit's io RATE (the cumulative io-accounting counter differenced
    /// over the window) — Step-4. `field` selects rbps/wbps/riops/wiops.
    CgroupIoStat { unit: String, field: IoMaxFieldSpec },
    /// PRESSURE-STALL avg10 (×100) from `/proc/pressure/<resource>` — Step-3, the
    /// throttle signal for a soft band. `resource` ∈ cpu/memory/io, `kind` ∈ some/full.
    Psi { resource: PsiResourceSpec, kind: PsiKindSpec },
}

/// Mirror of `breathe_provider::PsiResource` for the CRD.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PsiResourceSpec {
    Cpu,
    Memory,
    Io,
}

/// Mirror of `breathe_provider::PsiKind` for the CRD.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PsiKindSpec {
    Some,
    Full,
}

/// A host-param band's carve directionality (serializable mirror of
/// `breathe_provider::Directionality`; `ObserveOnly` is not a carve).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum DirectionalitySpec {
    /// Breathes both ways (`vm.dirty_bytes`, `zfs_arc_dnode_limit`).
    #[default]
    Bidirectional,
    /// Never shrinks — a protection floor (`zfs_arc_min`, `vm.min_free_kbytes`).
    GrowOnly,
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "HostParamBand",
    namespaced,
    status = "BandStatus",
    shortname = "hpband",
    category = "breathe",
    printcolumn = r#"{"name":"Knob","type":"string","jsonPath":".spec.knob"}"#,
    printcolumn = r#"{"name":"Dir","type":"string","jsonPath":".spec.directionality"}"#,
    printcolumn = r#"{"name":"Util","type":"string","jsonPath":".status.lastUtil"}"#,
    printcolumn = r#"{"name":"Limit","type":"string","jsonPath":".status.currentLimit"}"#,
    printcolumn = r#"{"name":"Last","type":"string","jsonPath":".status.lastDecision"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct HostParamBandSpec {
    /// The node this band carves on (`targetRef.name` = node name, `kind: Node`).
    pub target_ref: TargetRef,
    /// The host lever to carve.
    pub knob: HostKnobSpec,
    /// Where to read the `used` signal.
    pub metric: HostMetricSpec,
    /// Carve directionality (default bidirectional; set `growOnly` for protection floors).
    #[serde(default)]
    pub directionality: DirectionalitySpec,
    #[serde(default = "d_setpoint")]
    pub setpoint: f64,
    #[serde(default = "d_grow_above")]
    pub grow_above: f64,
    #[serde(default = "d_shrink_below")]
    pub shrink_below: f64,
    #[serde(default = "d_grow_factor")]
    pub grow_factor: f64,
    #[serde(default = "d_shrink_factor")]
    pub shrink_factor: f64,
    #[serde(default = "d_floor_bytes")]
    pub floor: String,
    #[serde(default = "d_ceiling_bytes")]
    pub ceiling: String,
    #[serde(default = "d_cooldown")]
    pub cooldown_seconds: u64,
    #[serde(default = "d_max_staleness")]
    pub max_staleness_seconds: u64,
    /// SHADOW (two-state). **This kind is one of the only two that actually
    /// read `dryRun`**: `HostParamBand`/`KubeParamBand` override
    /// `promotion_mode()` with a pure `dryRun ? Shadow : Effect` reading and
    /// never auto-promote. (Every k8s / app / replica band kind ignores it —
    /// see their own field docs.) Superseded by `writeIntent`, which wins
    /// whenever it is authored.
    #[serde(default)]
    pub dry_run: bool,
    /// **The authorization intent** — supersedes `dryRun` on this kind. See
    /// `MemoryBandSpec::write_intent` for the four arms. `authorizedBy` is
    /// REQUIRED on `write`: an `{intent: write}` naming no `authorizedBy` never goes live: it is
    /// held in shadow as `intentMalformed` (a runtime mitigation — a k8s
    /// structural schema cannot express a conditional `required`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_intent: Option<WriteIntentSpec>,
    #[serde(default, skip_serializing_if = "breathe_provider::DisruptionPolicy::is_restart_free_only")]
    pub disruption_policy: DisruptionPolicy,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suspend: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_limit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_limit_expiry: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub predictive: bool,
    #[serde(default = "d_predictive_lookahead")]
    pub predictive_lookahead_seconds: u64,
}

impl HostParamBandSpec {
    /// The provider-typed host knob this band carves.
    #[must_use]
    pub fn provider_knob(&self) -> breathe_provider::HostKnob {
        match &self.knob {
            HostKnobSpec::Sysctl { key } => breathe_provider::HostKnob::Sysctl { key: key.clone() },
            HostKnobSpec::ZfsParam { param } => breathe_provider::HostKnob::ZfsParam { param: param.clone() },
            HostKnobSpec::CgroupIoMax { unit, device, field } => breathe_provider::HostKnob::CgroupIoMax {
                unit: unit.clone(),
                device: device.clone(),
                field: field.provider(),
            },
        }
    }
    /// The provider-typed metric source this band reads `used` from.
    #[must_use]
    pub fn provider_metric(&self) -> breathe_provider::HostMetric {
        match &self.metric {
            HostMetricSpec::MeminfoField { field } => breathe_provider::HostMetric::MeminfoField { field: field.clone() },
            HostMetricSpec::ArcstatsRow { row } => breathe_provider::HostMetric::ArcKstat { row: row.clone() },
            HostMetricSpec::CgroupIoStat { unit, field } => breathe_provider::HostMetric::CgroupIoStat {
                unit: unit.clone(),
                field: field.provider(),
            },
            HostMetricSpec::Psi { resource, kind } => breathe_provider::HostMetric::Psi {
                resource: match resource {
                    PsiResourceSpec::Cpu => breathe_provider::PsiResource::Cpu,
                    PsiResourceSpec::Memory => breathe_provider::PsiResource::Memory,
                    PsiResourceSpec::Io => breathe_provider::PsiResource::Io,
                },
                kind: match kind {
                    PsiKindSpec::Some => breathe_provider::PsiKind::Some,
                    PsiKindSpec::Full => breathe_provider::PsiKind::Full,
                },
            },
        }
    }
    /// The provider-typed directionality (the band law's lower-band gate).
    #[must_use]
    pub fn provider_directionality(&self) -> breathe_provider::Directionality {
        match self.directionality {
            DirectionalitySpec::Bidirectional => breathe_provider::Directionality::Bidirectional,
            DirectionalitySpec::GrowOnly => breathe_provider::Directionality::GrowOnly,
        }
    }
}

impl crate::Band for HostParamBand {
    fn dimension_id(&self) -> breathe_provider::DimensionId {
        breathe_provider::DimensionId::HostParam
    }
    fn target_ref(&self) -> &TargetRef {
        &self.spec.target_ref
    }
    fn band_config(&self) -> anyhow::Result<BandConfig> {
        let s = &self.spec;
        crate::band_config_of(
            s.setpoint, s.grow_above, s.shrink_below, s.grow_factor, s.shrink_factor,
            &s.floor, &s.ceiling, "", 0, Unit::Bytes,
        )
    }
    fn max_staleness_seconds(&self) -> u64 {
        self.spec.max_staleness_seconds
    }
    fn cooldown_seconds(&self) -> u64 {
        self.spec.cooldown_seconds
    }
    fn dry_run(&self) -> bool {
        self.spec.dry_run
    }
    fn write_intent_spec(&self) -> Option<&WriteIntentSpec> {
        self.spec.write_intent.as_ref()
    }
    fn mode_spec(&self) -> Option<PromotionMode> {
        None
    }
    fn confirm_after_seconds(&self) -> u64 {
        d_confirm_after()
    }
    /// Host bands keep pure two-state (shadow/effect) semantics until explicitly
    /// migrated to the promotion lifecycle — they never auto-promote.
    fn promotion_mode(&self) -> PromotionMode {
        if self.dry_run() {
            PromotionMode::Shadow
        } else {
            PromotionMode::Effect
        }
    }
    fn last_change_epoch(&self) -> Option<i64> {
        self.status.as_ref().and_then(|s| s.last_change_epoch)
    }
    fn disruption_policy(&self) -> DisruptionPolicy {
        self.spec.disruption_policy
    }
    fn suspended(&self) -> bool {
        self.spec.suspend
    }
    fn force_limit_value(&self) -> Option<u64> {
        self.spec.force_limit.as_deref().and_then(|q| Unit::Bytes.parse(q))
    }
    fn force_limit_expiry(&self) -> Option<&str> {
        self.spec.force_limit_expiry.as_deref()
    }
    fn predictive(&self) -> Option<f64> {
        self.spec.predictive.then_some(self.spec.predictive_lookahead_seconds as f64)
    }
    fn status(&self) -> Option<&BandStatus> {
        self.status.as_ref()
    }
}

// ───────────── KubeParamBand — the GENERIC k8s-CR / app band (Step-6/8/12) ─────────────
// The k8s-plane peer of HostParamBand: one CR carves any k8s-CR field (Istio
// DestinationRule connection pool, ResourceQuota hard limit, CNPG/VM CR field,
// HPA setpoint) via KubeCluster's generic CR-path SSA. The `used` signal is a
// PromQL (the metric plane). Every Step-6/8/12 vector is a CR instance of this.

/// Which k8s-CR field a [`KubeParamBand`] carves (serde mirror of the k8s-plane
/// `breathe_provider::LimitLayout` arms). Maps to a generic SSA path-write.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum KubeLayoutSpec {
    // NOTE: the enum-level `rename_all = "camelCase"` renames only the variant
    // TAGS (CrField -> crField); it does NOT cascade to struct-variant inner
    // fields. Each variant with a multi-word field therefore carries its OWN
    // `rename_all` so its snake_case Rust fields serialize camelCase
    // (`fieldPath`/`apiVersion`/`restartFree`) — camelCase like every other field
    // in the breathe API. Without these, the CRD + the wire CR would be the lone
    // snake_case island (an idiom leak); the round-trip test below locks it.
    /// A field of any operator CR (CNPG/VictoriaMetrics/OpenSearch) at `fieldPath`.
    #[serde(rename_all = "camelCase")]
    CrField { api_version: String, kind: String, name: String, field_path: String, #[serde(default)] restart_free: bool },
    /// An Istio DestinationRule connection-pool field (Envoy live-reload).
    #[serde(rename_all = "camelCase")]
    DestinationRuleField { name: String, field_path: String },
    /// A namespace ResourceQuota / LimitRange envelope field.
    #[serde(rename_all = "camelCase")]
    NamespaceEnvelope { namespace: String, kind: NamespaceEnvelopeKindSpec, field_path: String },
    /// A controller setpoint — HPA target / PDB minAvailable.
    #[serde(rename_all = "camelCase")]
    ControllerSetpoint { api_version: String, kind: String, name: String, field_path: String },
}

/// Mirror of `breathe_provider::NamespaceEnvelopeKind` for the CRD.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum NamespaceEnvelopeKindSpec {
    ResourceQuota,
    LimitRange,
}

/// The `used` metric for a [`KubeParamBand`] — a PromQL whose scalar is the
/// utilization signal (Envoy cx_active, quota status.used, retention disk%).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KubeMetricSpec {
    pub prometheus: String,
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "KubeParamBand",
    namespaced,
    status = "BandStatus",
    shortname = "kpband",
    category = "breathe",
    printcolumn = r#"{"name":"Dir","type":"string","jsonPath":".spec.directionality"}"#,
    printcolumn = r#"{"name":"Util","type":"string","jsonPath":".status.lastUtil"}"#,
    printcolumn = r#"{"name":"Limit","type":"string","jsonPath":".status.currentLimit"}"#,
    printcolumn = r#"{"name":"Last","type":"string","jsonPath":".status.lastDecision"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KubeParamBandSpec {
    /// The CR this band carves (`targetRef.kind`/`name`/`apiVersion` = the object;
    /// the layout's `fieldPath` points into its `/spec`).
    pub target_ref: TargetRef,
    /// The k8s-CR field to carve.
    pub layout: KubeLayoutSpec,
    /// Where to read the `used` signal (a PromQL).
    pub metric: KubeMetricSpec,
    #[serde(default)]
    pub directionality: DirectionalitySpec,
    #[serde(default = "d_setpoint")]
    pub setpoint: f64,
    #[serde(default = "d_grow_above")]
    pub grow_above: f64,
    #[serde(default = "d_shrink_below")]
    pub shrink_below: f64,
    #[serde(default = "d_grow_factor")]
    pub grow_factor: f64,
    #[serde(default = "d_shrink_factor")]
    pub shrink_factor: f64,
    #[serde(default = "d_floor_bytes")]
    pub floor: String,
    #[serde(default = "d_ceiling_bytes")]
    pub ceiling: String,
    #[serde(default = "d_cooldown")]
    pub cooldown_seconds: u64,
    #[serde(default = "d_max_staleness")]
    pub max_staleness_seconds: u64,
    /// SHADOW (two-state). **This kind is one of the only two that actually
    /// read `dryRun`**: `HostParamBand`/`KubeParamBand` override
    /// `promotion_mode()` with a pure `dryRun ? Shadow : Effect` reading and
    /// never auto-promote. (Every k8s / app / replica band kind ignores it —
    /// see their own field docs.) Superseded by `writeIntent`, which wins
    /// whenever it is authored.
    #[serde(default)]
    pub dry_run: bool,
    /// **The authorization intent** — supersedes `dryRun` on this kind. See
    /// `MemoryBandSpec::write_intent` for the four arms. `authorizedBy` is
    /// REQUIRED on `write`: an `{intent: write}` naming no `authorizedBy` never goes live: it is
    /// held in shadow as `intentMalformed` (a runtime mitigation — a k8s
    /// structural schema cannot express a conditional `required`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_intent: Option<WriteIntentSpec>,
    #[serde(default, skip_serializing_if = "breathe_provider::DisruptionPolicy::is_restart_free_only")]
    pub disruption_policy: DisruptionPolicy,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suspend: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_limit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_limit_expiry: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub predictive: bool,
    #[serde(default = "d_predictive_lookahead")]
    pub predictive_lookahead_seconds: u64,
}

impl KubeParamBandSpec {
    /// The provider-typed k8s-plane layout this band carves.
    #[must_use]
    pub fn provider_layout(&self) -> breathe_provider::LimitLayout {
        use breathe_provider::LimitLayout;
        match &self.layout {
            KubeLayoutSpec::CrField { api_version, kind, name, field_path, restart_free } => LimitLayout::CrField {
                api_version: api_version.clone(), kind: kind.clone(), name: name.clone(),
                field_path: field_path.clone(), restart_free: *restart_free,
            },
            KubeLayoutSpec::DestinationRuleField { name, field_path } => {
                LimitLayout::DestinationRuleField { name: name.clone(), field_path: field_path.clone() }
            }
            KubeLayoutSpec::NamespaceEnvelope { namespace, kind, field_path } => LimitLayout::NamespaceEnvelope {
                namespace: namespace.clone(),
                kind: match kind {
                    NamespaceEnvelopeKindSpec::ResourceQuota => breathe_provider::NamespaceEnvelopeKind::ResourceQuota,
                    NamespaceEnvelopeKindSpec::LimitRange => breathe_provider::NamespaceEnvelopeKind::LimitRange,
                },
                field_path: field_path.clone(),
            },
            KubeLayoutSpec::ControllerSetpoint { api_version, kind, name, field_path } => LimitLayout::ControllerSetpoint {
                api_version: api_version.clone(), kind: kind.clone(), name: name.clone(), field_path: field_path.clone(),
            },
        }
    }
    /// The provider-typed metric source (a PromQL).
    #[must_use]
    pub fn provider_metric(&self) -> breathe_provider::MetricSource {
        breathe_provider::MetricSource::Prometheus(self.metric.prometheus.clone())
    }
    /// The provider-typed directionality.
    #[must_use]
    pub fn provider_directionality(&self) -> breathe_provider::Directionality {
        match self.directionality {
            DirectionalitySpec::Bidirectional => breathe_provider::Directionality::Bidirectional,
            DirectionalitySpec::GrowOnly => breathe_provider::Directionality::GrowOnly,
        }
    }
}

impl crate::Band for KubeParamBand {
    fn dimension_id(&self) -> breathe_provider::DimensionId {
        breathe_provider::DimensionId::KubeParam
    }
    fn target_ref(&self) -> &TargetRef {
        &self.spec.target_ref
    }
    fn band_config(&self) -> anyhow::Result<BandConfig> {
        let s = &self.spec;
        // k8s-CR fields are bare integers (maxConnections, retention secs, quota counts).
        crate::band_config_of(s.setpoint, s.grow_above, s.shrink_below, s.grow_factor, s.shrink_factor, &s.floor, &s.ceiling, "", 0, Unit::Count)
    }
    fn max_staleness_seconds(&self) -> u64 {
        self.spec.max_staleness_seconds
    }
    fn cooldown_seconds(&self) -> u64 {
        self.spec.cooldown_seconds
    }
    fn dry_run(&self) -> bool {
        self.spec.dry_run
    }
    fn write_intent_spec(&self) -> Option<&WriteIntentSpec> {
        self.spec.write_intent.as_ref()
    }
    fn mode_spec(&self) -> Option<PromotionMode> {
        None
    }
    fn confirm_after_seconds(&self) -> u64 {
        d_confirm_after()
    }
    /// Host bands keep pure two-state (shadow/effect) semantics until explicitly
    /// migrated to the promotion lifecycle — they never auto-promote.
    fn promotion_mode(&self) -> PromotionMode {
        if self.dry_run() {
            PromotionMode::Shadow
        } else {
            PromotionMode::Effect
        }
    }
    fn last_change_epoch(&self) -> Option<i64> {
        self.status.as_ref().and_then(|s| s.last_change_epoch)
    }
    fn disruption_policy(&self) -> DisruptionPolicy {
        self.spec.disruption_policy
    }
    fn suspended(&self) -> bool {
        self.spec.suspend
    }
    fn force_limit_value(&self) -> Option<u64> {
        self.spec.force_limit.as_deref().and_then(|q| Unit::Count.parse(q))
    }
    fn force_limit_expiry(&self) -> Option<&str> {
        self.spec.force_limit_expiry.as_deref()
    }
    fn predictive(&self) -> Option<f64> {
        self.spec.predictive.then_some(self.spec.predictive_lookahead_seconds as f64)
    }
    fn status(&self) -> Option<&BandStatus> {
        self.status.as_ref()
    }
}

// ───────────── AppBand — the GENERIC app-plane actuator band (Step-9/13) ─────────────
// The app-plane peer of KubeParamBand: one CR carves any application knob via the
// ConfigFile/ApiCall layouts, dispatched by the `ActuatorCluster` sum type to the
// ConfigReload / redis-CLI / JMX-Jolokia / app-admin-RPC actuator. The `used` signal
// is read from the k8s metrics plane (a PromQL) — the actuators have no read path.

/// How a [`AppBand`] config-file value takes effect (serde mirror of
/// `breathe_provider::ConfigReload`).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AppReloadSpec {
    /// `SIGHUP` re-reads the file live (PostgreSQL `work_mem`, nginx) — RestartFree.
    Sighup,
    /// A protocol `RELOAD` command (pgbouncer) — RestartFree.
    Reload,
    /// Requires a process restart (PostgreSQL `shared_buffers`) — RestartRequiring.
    Restart,
}

/// Which app-plane actuator + layout a [`AppBand`] carves. The variant TAG selects
/// the actuator (never sniffed from the command string) — the app-plane peer of
/// `KubeLayoutSpec`. Per-variant `rename_all` keeps inner fields camelCase on the
/// wire (the enum-level attr renames only the tag).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AppLayoutSpec {
    /// A config file `key` at `path`, applied by the ConfigReload actuator + `reload`.
    #[serde(rename_all = "camelCase")]
    ConfigFile { path: String, key: String, reload: AppReloadSpec },
    /// A protocol `CONFIG SET` knob (Redis/Kafka/NATS) via the redis-CLI actuator.
    /// `command` = the protocol param (e.g. `maxmemory`); `endpoint` = the server URL.
    #[serde(rename_all = "camelCase")]
    ApiCall { endpoint: String, command: String },
    /// A JVM MBean over Jolokia. `endpoint` = the Jolokia base URL; `command` =
    /// `ObjectName:attribute`.
    #[serde(rename_all = "camelCase")]
    Jmx { endpoint: String, command: String },
    /// An app admin RPC knob (GOMEMLIMIT/prefetch/max-concurrency). `endpoint` = the
    /// admin base URL; `command` = the knob name.
    #[serde(rename_all = "camelCase")]
    AppRpc { endpoint: String, command: String },
}

/// Which actuator backend services an [`AppBand`] — the controller builds the
/// matching `ActuatorBackend` from this tag. Decoupled from `AppLayoutSpec` so the
/// controller need not depend on the layout's data to pick the backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppActuatorKind {
    ConfigReload,
    ApiCall,
    Jmx,
    AppRpc,
}

/// The `used` metric for an [`AppBand`] — a PromQL whose scalar is the live
/// utilization signal (redis used_memory, pool active connections, working set).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AppMetricSpec {
    pub prometheus: String,
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "AppBand",
    namespaced,
    status = "BandStatus",
    shortname = "apband",
    category = "breathe",
    printcolumn = r#"{"name":"Dir","type":"string","jsonPath":".spec.directionality"}"#,
    printcolumn = r#"{"name":"Util","type":"string","jsonPath":".status.lastUtil"}"#,
    printcolumn = r#"{"name":"Limit","type":"string","jsonPath":".status.currentLimit"}"#,
    printcolumn = r#"{"name":"Last","type":"string","jsonPath":".status.lastDecision"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct AppBandSpec {
    /// The workload this band carves (`targetRef.name`/`namespace` locate the app +
    /// its metric pods; the layout addresses the app's own knob).
    pub target_ref: TargetRef,
    /// The app-plane knob to carve (its variant tag selects the actuator).
    pub layout: AppLayoutSpec,
    /// Where to read the `used` signal (a PromQL on the metrics plane).
    pub metric: AppMetricSpec,
    #[serde(default)]
    pub directionality: DirectionalitySpec,
    #[serde(default = "d_setpoint")]
    pub setpoint: f64,
    #[serde(default = "d_grow_above")]
    pub grow_above: f64,
    #[serde(default = "d_shrink_below")]
    pub shrink_below: f64,
    #[serde(default = "d_grow_factor")]
    pub grow_factor: f64,
    #[serde(default = "d_shrink_factor")]
    pub shrink_factor: f64,
    #[serde(default = "d_floor_bytes")]
    pub floor: String,
    #[serde(default = "d_ceiling_bytes")]
    pub ceiling: String,
    #[serde(default = "d_cooldown")]
    pub cooldown_seconds: u64,
    #[serde(default = "d_max_staleness")]
    pub max_staleness_seconds: u64,
    /// RETIRED 2026-06-19 (breathe@76924b0) — **this field has NO effect on
    /// this band kind.** `AppBand` resolves through the compiled
    /// `shadowConfirmEffect` default and never consults `dryRun`, so a band
    /// authored `dryRun: true` writes for real once its confirm window
    /// elapses. Kept as the record of an authored decision, never read.
    ///
    /// Note `AppBand` carries no `mode` field either — so until `writeIntent`
    /// landed, **shadow was entirely unrepresentable on the app plane** (Redis
    /// maxmemory, connection pools). Authorization is `spec.writeIntent`; the
    /// live verdict is `status.effectiveGate`.
    #[serde(default)]
    pub dry_run: bool,
    /// **The authorization intent** — on this kind, the ONLY way to hold the
    /// band in shadow. See `MemoryBandSpec::write_intent` for the four arms.
    /// `authorizedBy` is REQUIRED on `write`: an `{intent: write}` naming no `authorizedBy` never goes live: it is
    /// held in shadow as `intentMalformed` (a runtime mitigation — a k8s
    /// structural schema cannot express a conditional `required`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_intent: Option<WriteIntentSpec>,
    #[serde(default, skip_serializing_if = "breathe_provider::DisruptionPolicy::is_restart_free_only")]
    pub disruption_policy: DisruptionPolicy,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suspend: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_limit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_limit_expiry: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub predictive: bool,
    #[serde(default = "d_predictive_lookahead")]
    pub predictive_lookahead_seconds: u64,
}

impl AppBandSpec {
    /// The provider-typed layout this band carves (Jmx/AppRpc share the ApiCall layout;
    /// the actuator is disambiguated by [`AppBandSpec::actuator_kind`]).
    #[must_use]
    pub fn provider_layout(&self) -> breathe_provider::LimitLayout {
        use breathe_provider::{ConfigReload, LimitLayout};
        match &self.layout {
            AppLayoutSpec::ConfigFile { path, key, reload } => LimitLayout::ConfigFile {
                path: path.clone(),
                key: key.clone(),
                reload: match reload {
                    AppReloadSpec::Sighup => ConfigReload::Sighup,
                    AppReloadSpec::Reload => ConfigReload::Reload,
                    AppReloadSpec::Restart => ConfigReload::Restart,
                },
            },
            AppLayoutSpec::ApiCall { endpoint, command }
            | AppLayoutSpec::Jmx { endpoint, command }
            | AppLayoutSpec::AppRpc { endpoint, command } => {
                LimitLayout::ApiCall { endpoint: endpoint.clone(), command: command.clone() }
            }
        }
    }
    /// Which actuator backend the controller must build for this band's layout.
    #[must_use]
    pub fn actuator_kind(&self) -> AppActuatorKind {
        match &self.layout {
            AppLayoutSpec::ConfigFile { .. } => AppActuatorKind::ConfigReload,
            AppLayoutSpec::ApiCall { .. } => AppActuatorKind::ApiCall,
            AppLayoutSpec::Jmx { .. } => AppActuatorKind::Jmx,
            AppLayoutSpec::AppRpc { .. } => AppActuatorKind::AppRpc,
        }
    }
    /// The provider-typed metric source (a PromQL).
    #[must_use]
    pub fn provider_metric(&self) -> breathe_provider::MetricSource {
        breathe_provider::MetricSource::Prometheus(self.metric.prometheus.clone())
    }
    /// The provider-typed directionality.
    #[must_use]
    pub fn provider_directionality(&self) -> breathe_provider::Directionality {
        match self.directionality {
            DirectionalitySpec::Bidirectional => breathe_provider::Directionality::Bidirectional,
            DirectionalitySpec::GrowOnly => breathe_provider::Directionality::GrowOnly,
        }
    }
}

impl crate::Band for AppBand {
    fn dimension_id(&self) -> breathe_provider::DimensionId {
        breathe_provider::DimensionId::AppParam
    }
    fn target_ref(&self) -> &TargetRef {
        &self.spec.target_ref
    }
    fn band_config(&self) -> anyhow::Result<BandConfig> {
        let s = &self.spec;
        // app knobs are bare integers (maxmemory bytes, max_connections counts, …).
        crate::band_config_of(s.setpoint, s.grow_above, s.shrink_below, s.grow_factor, s.shrink_factor, &s.floor, &s.ceiling, "", 0, Unit::Count)
    }
    fn max_staleness_seconds(&self) -> u64 {
        self.spec.max_staleness_seconds
    }
    fn cooldown_seconds(&self) -> u64 {
        self.spec.cooldown_seconds
    }
    fn dry_run(&self) -> bool {
        self.spec.dry_run
    }
    fn write_intent_spec(&self) -> Option<&WriteIntentSpec> {
        self.spec.write_intent.as_ref()
    }
    fn last_change_epoch(&self) -> Option<i64> {
        self.status.as_ref().and_then(|s| s.last_change_epoch)
    }
    fn disruption_policy(&self) -> DisruptionPolicy {
        self.spec.disruption_policy
    }
    fn suspended(&self) -> bool {
        self.spec.suspend
    }
    fn force_limit_value(&self) -> Option<u64> {
        self.spec.force_limit.as_deref().and_then(|q| Unit::Count.parse(q))
    }
    fn force_limit_expiry(&self) -> Option<&str> {
        self.spec.force_limit_expiry.as_deref()
    }
    fn predictive(&self) -> Option<f64> {
        self.spec.predictive.then_some(self.spec.predictive_lookahead_seconds as f64)
    }
    fn status(&self) -> Option<&BandStatus> {
        self.status.as_ref()
    }
    fn mode_spec(&self) -> Option<PromotionMode> {
        None
    }
    fn confirm_after_seconds(&self) -> u64 {
        d_confirm_after()
    }
}

// ───────────── ReplicaBand — the HORIZONTAL band (workload replica count) ─────────────
// The horizontal peer of the vertical MemoryBand/CpuBand: those hold a pod's
// LIMIT at a utilization band; a ReplicaBand holds a workload's COUNT at a
// work-rate band. It rides the SAME shadow→confirm→effect gate (the `Band` trait
// default lifecycle) and the SAME SSA actuator (`LimitLayout::Replica` →
// KubeCluster writes `.spec.replicas`), but its DECISION is the horizontal band
// law (`breathe_control::replica::decide_replicas`: HPA ratio + asymmetric
// anti-flap + HA floor + spot-reclaim scale-OUT), NOT the vertical `decide`. The
// `used` signal is a PromQL (request-rate / queue-depth / utilization — never
// memory, which does not shed with replicas). Floor defaults to 2 (HA).

/// Which signal a [`ReplicaBand`] scales on (serde mirror of
/// `breathe_control::replica::ReplicaSignal`). There is deliberately no `memory`
/// arm — adding replicas does not reduce per-pod memory, so a memory-keyed
/// horizontal signal runs away; the illegal signal is unrepresentable.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum ReplicaSignalSpec {
    /// A per-replica utilization RATIO vs its target (CPU% of request, concurrency
    /// fraction). `desired = ceil(current × value/target)`.
    #[default]
    Utilization,
    /// An ABSOLUTE total work rate (requests/sec). `desired = ceil(value/targetPerReplica)`.
    RequestRate,
    /// An ABSOLUTE backlog / queue depth (lag, pending). `desired = ceil(value/targetPerReplica)`.
    QueueDepth,
}

impl ReplicaSignalSpec {
    /// The control-layer signal this maps to.
    #[must_use]
    pub fn control(self) -> ReplicaSignal {
        match self {
            Self::Utilization => ReplicaSignal::Utilization,
            Self::RequestRate => ReplicaSignal::RequestRate,
            Self::QueueDepth => ReplicaSignal::QueueDepth,
        }
    }
}

/// Which topology CLASS a [`ReplicaBand`] scales as — the plain string discriminant
/// (serde mirror of the `breathe_control::replica::Topology` arms, minus their
/// params). A unit enum so the CRD schema is all-`String` (structural-schema-safe,
/// exactly like `ReplicaSignalSpec` / `PromotionMode`).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum TopologyKind {
    /// Stateless: any pod interchangeable — free HPA-style scaling, HA floor only.
    #[default]
    NonPersistent,
    /// Stateful, PVC-per-ordinal — grow adds an ordinal+PVC freely; a scale-in is
    /// HELD for drain/rebalance and never rests below `replicationFactor`.
    Persistent,
    /// Primary + read-replicas — only the read-replicas breathe; the band never
    /// scales the `primaries` away (a primary loss is a failover, not a scale).
    MasterSlave,
    /// Quorum/consensus (Raft/etcd) — odd count ≥ 3, majority-safe one-rung steps.
    FullyDistributed,
}

/// The workload TOPOLOGY of a [`ReplicaBand`] (serde mirror of
/// `breathe_control::replica::Topology`). It picks BOTH the scaling algorithm and the
/// hard invariant the band may never violate (theory/BREATHABILITY.md §II.5). Default
/// `nonPersistent` (stateless) — an omitted `topology` leaves an existing band's
/// behaviour byte-unchanged.
///
/// A FLAT STRUCT (a string `kind` + the per-class params as optionals), NOT a
/// tagged enum: the k8s apiserver's structural-schema conversion rejects both a
/// mixed unit/struct enum (String-vs-Object variants) and an internally-tagged enum
/// (a per-variant `kind` const in a `oneOf` — the property must be identical across
/// subschemas). The flat struct keeps every property a single fixed schema. A
/// `persistent`/`masterSlave` class whose param is omitted becomes a
/// [`breathe_control::replica::ReplicaError::TopologyUnsatisfiable`] at the config
/// gate (parse-time-rejected), surfaced as an error status before any scale.
///
/// Wire forms: `{"kind": "nonPersistent"}` |
/// `{"kind": "persistent", "replicationFactor": 3}` |
/// `{"kind": "masterSlave", "primaries": 1}` | `{"kind": "fullyDistributed"}`.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TopologySpec {
    /// The topology class.
    #[serde(default)]
    pub kind: TopologyKind,
    /// `persistent` only: the data-replication factor — the band never rests below
    /// this many replicas. Ignored by the other classes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication_factor: Option<u32>,
    /// `masterSlave` only: the writable-primary count folded into the floor (never
    /// scaled away). Ignored by the other classes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primaries: Option<u32>,
}

impl TopologySpec {
    /// The control-layer topology this maps to. A `persistent`/`masterSlave` class
    /// with its param omitted maps to a `0` param, which the control-layer
    /// [`ReplicaBandConfig::validate`] then parse-rejects
    /// ([`breathe_control::replica::ReplicaError::TopologyUnsatisfiable`]) — a missing
    /// factor/primary count is never silently a wrong scale.
    #[must_use]
    pub fn control(self) -> Topology {
        match self.kind {
            TopologyKind::NonPersistent => Topology::NonPersistent,
            TopologyKind::Persistent => Topology::Persistent { replication_factor: self.replication_factor.unwrap_or(0) },
            TopologyKind::MasterSlave => Topology::MasterSlave { primaries: self.primaries.unwrap_or(0) },
            TopologyKind::FullyDistributed => Topology::FullyDistributed,
        }
    }
}

/// The `used` signal for a [`ReplicaBand`] — a PromQL whose scalar is the driving
/// work-rate metric (RPS, queue depth, per-replica utilization).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaMetricSpec {
    pub prometheus: String,
    /// An OPTIONAL PromQL whose scalar is the count of this workload's replicas
    /// about to be lost to a pending node/spot reclaim (the `retirada` signal). A
    /// non-zero value drives a pre-emptive scale-OUT before the doomed pods drain.
    /// `None` ⇒ no spot-awareness (the reclaim count is always 0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reclaim_prometheus: Option<String>,
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "ReplicaBand",
    namespaced,
    status = "BandStatus",
    shortname = "rband",
    category = "breathe",
    printcolumn = r#"{"name":"Target","type":"string","jsonPath":".spec.targetRef.kind"}"#,
    printcolumn = r#"{"name":"Name","type":"string","jsonPath":".spec.targetRef.name"}"#,
    printcolumn = r#"{"name":"Signal","type":"string","jsonPath":".spec.signal"}"#,
    printcolumn = r#"{"name":"Replicas","type":"string","jsonPath":".status.currentLimit"}"#,
    printcolumn = r#"{"name":"Last","type":"string","jsonPath":".status.lastDecision"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.conditions[?(@.type=='Ready')].status"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaBandSpec {
    /// The workload whose `.spec.replicas` this band scales (`Deployment` /
    /// `StatefulSet`).
    pub target_ref: TargetRef,
    /// The workload TOPOLOGY — selects the scaling ALGORITHM and the hard invariant
    /// the band may never violate. Default `nonPersistent` (stateless: free HPA
    /// scaling, HA floor). `persistent` (StatefulSet/PVC-per-ordinal: grow freely, a
    /// scale-in is HELD for drain/rebalance, never below `replicationFactor`).
    /// `masterSlave` (breathe the read-replicas only, never scale the primary away).
    /// `fullyDistributed` (quorum/consensus: odd count ≥ 3, majority-safe one-rung
    /// steps).
    #[serde(default)]
    pub topology: TopologySpec,
    /// Which signal drives scaling.
    #[serde(default)]
    pub signal: ReplicaSignalSpec,
    /// Where to read the `used` signal (a PromQL) + the optional reclaim signal.
    pub metric: ReplicaMetricSpec,
    /// The setpoint: target per-replica utilization (`utilization`) or target work
    /// PER replica (`requestRate` / `queueDepth`).
    #[serde(default = "d_replica_target")]
    pub target: f64,
    /// The at-rest HA floor — never scale below this many replicas. Default 2 (a
    /// single replica tolerates no disruption; floor 1 + a PDB blocks node drains).
    #[serde(default = "d_replica_floor")]
    pub floor: u32,
    /// A stronger during-maintenance HA floor (e.g. 3) — survive one disruption
    /// while still serving with 2. Effective floor = `max(floor, haFloor)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ha_floor: Option<u32>,
    /// Never scale above this many replicas (the L2 wall).
    #[serde(default = "d_replica_ceiling")]
    pub ceiling: u32,
    /// SCALE-UP dead-band — scale up only when the metric ratio exceeds `1 + this`.
    /// Small (react fast to spikes). Default 0.10.
    #[serde(default = "d_replica_tol_up")]
    pub tolerance_up: f64,
    /// SCALE-DOWN dead-band — scale down only below `1 - this`. Large (resist
    /// churn). Default 0.20.
    #[serde(default = "d_replica_tol_down")]
    pub tolerance_down: f64,
    /// Velocity cap UP (percent of current per tick). Default 100%.
    #[serde(default = "d_replica_up_pct")]
    pub max_scale_up_pct: u32,
    /// Velocity cap UP (absolute pods per tick). Default 4.
    #[serde(default = "d_replica_up_pods")]
    pub max_scale_up_pods: u32,
    /// Velocity cap DOWN (percent of current per tick). Default 10%.
    #[serde(default = "d_replica_down_pct")]
    pub max_scale_down_pct: u32,
    /// Velocity cap DOWN (absolute pods per tick). Default 1.
    #[serde(default = "d_replica_down_pods")]
    pub max_scale_down_pods: u32,
    #[serde(default = "d_cooldown")]
    pub cooldown_seconds: u64,
    #[serde(default = "d_max_staleness")]
    pub max_staleness_seconds: u64,
    /// RETIRED 2026-06-19 (breathe@76924b0) — **this field has NO effect on
    /// this band kind.** The carve gate resolves `writeIntent` > `mode` > the
    /// compiled `shadowConfirmEffect` default; `dryRun` is not consulted, so a
    /// band authored `dryRun: true` scales replicas for real once its confirm
    /// window elapses. Kept as the record of an authored decision, never read.
    /// Authorization is `spec.writeIntent`; the verdict is
    /// `status.effectiveGate`.
    #[serde(default)]
    pub dry_run: bool,
    /// **The authorization intent** — the first and highest link in the
    /// resolution chain `writeIntent` > `mode` > the compiled
    /// `shadowConfirmEffect`. See `MemoryBandSpec::write_intent` for the four
    /// arms. `authorizedBy` is REQUIRED on `write`: an `{intent: write}` naming no `authorizedBy` never goes live: it is
    /// held in shadow as `intentMalformed` (a runtime mitigation — a k8s
    /// structural schema cannot express a conditional `required`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_intent: Option<WriteIntentSpec>,
    /// The PROMOTION LIFECYCLE (unset ⇒ the fleet default `ShadowConfirmEffect`:
    /// shadow, then auto-promote once the clean-observation window proves it safe).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<PromotionMode>,
    #[serde(default = "d_confirm_after")]
    pub confirm_after_seconds: u64,
    /// The golden/ceiling gate. Because a scale-IN sheds a pod (`RestartRequiring`),
    /// the default `restartFreeOnly` scales OUT freely but GATES scale-in; set
    /// `allowRestart` to let the band shed replicas (the usual autoscaler posture).
    #[serde(default, skip_serializing_if = "breathe_provider::DisruptionPolicy::is_restart_free_only")]
    pub disruption_policy: DisruptionPolicy,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suspend: bool,
    /// BREAK-GLASS: pin the replica count to exactly this value (still through the
    /// gate + single-writer guard).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_limit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_limit_expiry: Option<String>,
}

impl ReplicaBandSpec {
    /// The typed horizontal band config the decision (`decide_replicas`) runs on.
    #[must_use]
    pub fn replica_band_config(&self) -> ReplicaBandConfig {
        ReplicaBandConfig {
            floor: self.floor,
            ha_floor: self.ha_floor,
            ceiling: self.ceiling,
            signal: self.signal.control(),
            target: self.target,
            tolerance_up: self.tolerance_up,
            tolerance_down: self.tolerance_down,
            max_scale_up_pct: self.max_scale_up_pct,
            max_scale_up_pods: self.max_scale_up_pods,
            max_scale_down_pct: self.max_scale_down_pct,
            max_scale_down_pods: self.max_scale_down_pods,
            topology: self.topology.control(),
        }
    }
    /// The typed actuator layout — SSA-write `.spec.replicas` on the owner kind.
    #[must_use]
    pub fn provider_layout(&self) -> LimitLayout {
        LimitLayout::Replica { kind: self.target_ref.kind.clone() }
    }
    /// The provider-typed driving metric source (a PromQL).
    #[must_use]
    pub fn provider_metric(&self) -> MetricSource {
        MetricSource::Prometheus(self.metric.prometheus.clone())
    }
    /// The provider-typed reclaim (spot) metric source, if spot-aware.
    #[must_use]
    pub fn provider_reclaim_metric(&self) -> Option<MetricSource> {
        self.metric.reclaim_prometheus.clone().map(MetricSource::Prometheus)
    }

    /// Parse-time validation of THIS band, including the topology ↔ target-kind
    /// coupling: a stateful topology (`persistent` / `masterSlave` /
    /// `fullyDistributed`) whose `targetRef.kind` is not `StatefulSet` is refused
    /// (ordinal-drain + PVC-per-replica semantics hold only on a StatefulSet). Reuses
    /// the control-layer [`breathe_control::replica::ReplicaBandConfig::validate_for_target`]
    /// with this band's own `targetRef.kind` — the CRD is the layer that owns the
    /// target, so it supplies the kind the numeric config gate cannot see.
    ///
    /// # Errors
    /// Any [`breathe_control::replica::ReplicaError`] the coupled gate raises.
    pub fn validate_for_target(&self) -> Result<(), breathe_control::replica::ReplicaError> {
        self.replica_band_config().validate_for_target(&self.target_ref.kind)
    }
}

impl crate::Band for ReplicaBand {
    fn dimension_id(&self) -> breathe_provider::DimensionId {
        breathe_provider::DimensionId::Replica
    }
    fn target_ref(&self) -> &TargetRef {
        &self.spec.target_ref
    }
    /// The vertical `BandConfig` is provided ONLY so the ReplicaBand rides the same
    /// `Band` gate (shadow/confirm/effect, force-limit, status) uniformly — the
    /// horizontal DECISION uses [`ReplicaBandSpec::replica_band_config`], never this.
    /// Counts live in the unit-blind floor/ceiling fields (`Unit::Count`), exactly
    /// as `BreatheCloudPool` holds node counts. `Trust` metric policy: a replica
    /// count of 0 is a real value, not a broken metric.
    fn band_config(&self) -> anyhow::Result<BandConfig> {
        let rc = self.spec.replica_band_config();
        Ok(BandConfig {
            grow_above: 0.85,
            shrink_below: 0.70,
            setpoint: 0.80,
            grow_factor: 1.25,
            shrink_factor: 0.90,
            floor_bytes: u64::from(rc.topology_floor()),
            ceiling_bytes: u64::from(rc.ceiling.max(rc.topology_floor())),
            request_floor_bytes: 0,
            warmup_seconds: 0,
            metric_missing_policy: MetricMissingPolicy::Trust,
        })
    }
    fn max_staleness_seconds(&self) -> u64 {
        self.spec.max_staleness_seconds
    }
    fn cooldown_seconds(&self) -> u64 {
        self.spec.cooldown_seconds
    }
    fn dry_run(&self) -> bool {
        self.spec.dry_run
    }
    fn write_intent_spec(&self) -> Option<&WriteIntentSpec> {
        self.spec.write_intent.as_ref()
    }
    fn mode_spec(&self) -> Option<PromotionMode> {
        self.spec.mode
    }
    fn confirm_after_seconds(&self) -> u64 {
        self.spec.confirm_after_seconds
    }
    fn last_change_epoch(&self) -> Option<i64> {
        self.status.as_ref().and_then(|s| s.last_change_epoch)
    }
    fn disruption_policy(&self) -> DisruptionPolicy {
        self.spec.disruption_policy
    }
    fn suspended(&self) -> bool {
        self.spec.suspend
    }
    fn force_limit_value(&self) -> Option<u64> {
        self.spec.force_limit.as_deref().and_then(|q| Unit::Count.parse(q))
    }
    fn force_limit_expiry(&self) -> Option<&str> {
        self.spec.force_limit_expiry.as_deref()
    }
    fn predictive(&self) -> Option<f64> {
        // Predictive horizontal pre-scaling is a documented follow-on (shadow-first
        // forecast that only raises the reactive floor); reactive today.
        None
    }
    fn status(&self) -> Option<&BandStatus> {
        self.status.as_ref()
    }
}

// ───────────── RequestBand — the RESERVATION band (requests + QoS) ─────────────
//
// Hand-rolled rather than `band_kind!`-stamped for the same reason
// HostParamBand/KubeParamBand are: it carries EXTRA spec fields the macro's
// fixed shape cannot express (`resource`, `demand`, `workloadClass`,
// `qosTarget`, `durability`), and its unit is a FUNCTION of `resource` rather
// than a constant baked into the macro call.
//
// ── The mirror pattern, and why it is not duplication ──
//
// `QosClass` / `WorkloadClass` live exactly once, in
// `breathe_invariant::isolation`, together with the algebra that matters
// (`requires_seal`, `default_qos`, `try_seal`, `carve_respecting_seal`). That
// crate deliberately depends on serde ALONE — no schemars, no kube — so a CRD
// field cannot name those types directly.
//
// This is the SAME situation `PromotionMode` is in, and it takes the SAME
// documented answer: a local `JsonSchema`-bearing mirror with a total
// conversion back to the canonical type. What makes it a mirror rather than a
// fork is that (a) it holds no logic at all, and (b)
// `qos_class_mirror_covers_every_arm` / `workload_class_mirror_covers_every_arm`
// iterate the upstream's own `ALL` and fail the build if a variant is added
// there and not here. The enums cannot silently drift.

/// CRD mirror of [`breathe_invariant::isolation::QosClass`]. Schema-only; the
/// algebra lives upstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum QosClassSpec {
    /// `requests == limits`. Never throttled below its request; LAST evicted.
    Guaranteed,
    /// `requests < limits`. A reserved floor plus burst headroom.
    Burstable,
    /// No requests. FIRST evicted — **no isolation at all**.
    BestEffort,
}

impl QosClassSpec {
    /// The canonical type. Total by construction.
    #[must_use]
    pub const fn to_invariant(self) -> breathe_invariant::isolation::QosClass {
        use breathe_invariant::isolation::QosClass as Q;
        match self {
            Self::Guaranteed => Q::Guaranteed,
            Self::Burstable => Q::Burstable,
            Self::BestEffort => Q::BestEffort,
        }
    }

    /// The inverse. Total both ways — that totality is what the drift test pins.
    #[must_use]
    pub const fn from_invariant(q: breathe_invariant::isolation::QosClass) -> Self {
        use breathe_invariant::isolation::QosClass as Q;
        match q {
            Q::Guaranteed => Self::Guaranteed,
            Q::Burstable => Self::Burstable,
            Q::BestEffort => Self::BestEffort,
        }
    }
}

#[allow(clippy::doc_markdown)] // "QoS"/"BestEffort" are English here, not code items
/// CRD mirror of [`breathe_invariant::isolation::WorkloadClass`] — the
/// criticality axis that selects a default QoS posture.
///
/// **Name collision, stated so nobody trips on it:** `breathe_catalog::preset`
/// also exports a `WorkloadClass`, and it means something else entirely (the
/// replica TOPOLOGY axis). This mirror is the *criticality* one. The two are
/// unrelated and neither is renamed here — a rename is a separate, deliberate
/// change, not a side effect of adding a dimension.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum WorkloadClassSpec {
    /// Interference-sensitive, must-not-be-disturbed. MUST be sealed — an
    /// unsealed Critical is the invariant violation (`IsolationPosture::try_seal`).
    Critical,
    /// The ordinary workload: a reserved floor plus burst headroom.
    Standard,
    /// Interruptible / re-runnable. The one class for which BestEffort is CORRECT.
    Batch,
    /// A noisy neighbour: hard-capped so it cannot starve others.
    Noisy,
}

#[allow(clippy::doc_markdown)] // "QoS" is an English acronym in these method docs
impl WorkloadClassSpec {
    #[must_use]
    pub const fn to_invariant(self) -> breathe_invariant::isolation::WorkloadClass {
        use breathe_invariant::isolation::WorkloadClass as W;
        match self {
            Self::Critical => W::Critical,
            Self::Standard => W::Standard,
            Self::Batch => W::Batch,
            Self::Noisy => W::Noisy,
        }
    }

    #[must_use]
    pub const fn from_invariant(w: breathe_invariant::isolation::WorkloadClass) -> Self {
        use breathe_invariant::isolation::WorkloadClass as W;
        match w {
            W::Critical => Self::Critical,
            W::Standard => Self::Standard,
            W::Batch => Self::Batch,
            W::Noisy => Self::Noisy,
        }
    }

    /// The best-known QoS posture for this class — delegated upstream, never
    /// re-decided here.
    #[must_use]
    pub const fn default_qos(self) -> QosClassSpec {
        QosClassSpec::from_invariant(self.to_invariant().default_qos())
    }
}

/// Which resource's request this band carves. CRD mirror of
/// [`breathe_provider::RequestResource`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum RequestResourceSpec {
    /// `resources.requests.memory`, in bytes. The OOM-ranking lever.
    Memory,
    /// `resources.requests.cpu`, in millicores.
    Cpu,
}

impl RequestResourceSpec {
    #[must_use]
    pub const fn to_provider(self) -> breathe_provider::RequestResource {
        use breathe_provider::RequestResource as R;
        match self {
            Self::Memory => R::Memory,
            Self::Cpu => R::Cpu,
        }
    }

    /// The band's unit — a FUNCTION of the resource, which is exactly why this
    /// kind could not be `band_kind!`-stamped (that macro takes one constant).
    #[must_use]
    pub const fn unit(self) -> Unit {
        match self {
            Self::Memory => Unit::Bytes,
            Self::Cpu => Unit::Millicores,
        }
    }
}

#[allow(clippy::doc_markdown)] // "QoS" is an English acronym in this prose
/// Where a converged request value must LAND. CRD mirror of
/// [`breathe_provider::Durability`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum DurabilitySpec {
    /// In-place only — **lost on the next rollout**, never visible in git.
    Ephemeral,
    /// The value must reach the committed manifest. The default, and the only
    /// honest setting for anything whose QoS posture matters: an in-place
    /// request change silently reverts on redeploy, i.e. the OOM protection
    /// evaporates at exactly the moment things are already moving.
    #[default]
    Committed,
}

impl DurabilitySpec {
    #[must_use]
    pub const fn to_provider(self) -> breathe_provider::Durability {
        use breathe_provider::Durability as D;
        match self {
            Self::Ephemeral => D::Ephemeral,
            Self::Committed => D::Committed,
        }
    }
}

#[allow(clippy::doc_markdown)] // "PromQL" is a proper noun in this prose
/// **The demand statistic a RESERVATION tracks — deliberately NOT the limit's
/// setpoint chase.**
///
/// A limit bounds blast radius, so its correct value sits *above* the
/// demonstrated peak and the band chases it with a two-sided deadband. A request
/// buys scheduling priority and OOM ranking, so its correct value is what the
/// workload typically needs resident — reserving the peak wastes cluster capacity
/// linearly in replica count and, past node allocatable, makes the workload
/// permanently unschedulable.
///
/// Running the LIMIT's law on a request is not merely suboptimal, it is unsafe in
/// the one direction that matters: `safe_min` is keyed on a geometrically-decaying
/// PEAK, so a single boot spike would ratchet the *reservation* to the spike and
/// hold it for hundreds of ticks.
///
/// The statistic is computed in PromQL, not in breathe: `quantile_over_time` over
/// a multi-day window is already stored by Prometheus, so the controller holds no
/// history and a restart loses nothing — the same reason the storage dimension
/// reads its peak from PromQL rather than accumulating it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DemandSignalSpec {
    /// The quantile of observed usage the reservation tracks, `∈ (0,1)`.
    /// Default 0.95 — a stable high-water, not the peak.
    #[serde(default = "d_demand_quantile")]
    pub quantile: f64,
    /// The trailing window the quantile is taken over, as a PromQL duration
    /// (`7d`). Long by design: a reservation should reflect a duty cycle, not an
    /// afternoon.
    #[serde(default = "d_demand_window")]
    pub window: String,
    /// Fractional headroom above the quantile, `≥ 0`. Default 0.15.
    ///
    /// Note this is a MULTIPLIER near 1.0 (`target = raw × (1 + headroom)`), not
    /// the limit law's `1/setpoint` divisor near 1.25 — the two carve different
    /// quantities toward different goals and must not share a constant.
    #[serde(default = "d_demand_headroom")]
    pub headroom: f64,
}

impl Default for DemandSignalSpec {
    fn default() -> Self {
        Self { quantile: d_demand_quantile(), window: d_demand_window(), headroom: d_demand_headroom() }
    }
}

#[allow(clippy::doc_markdown)] // "QoS"/"BestEffort"/"OOMKills" are English here
/// **The RESERVATION band — `resources.requests.<res>` and the QoS class it
/// derives.**
///
/// # What this kind does that no other kind can
///
/// Every other band carves a LIMIT, which bounds blast radius. This one carves
/// the REQUEST, which decides survival: `oom_score_adj` is computed from the
/// request, QoS class is a pure function of requests-vs-limits, and
/// schedulability keys on requests. A workload can be OOM-killed repeatedly
/// while its limit is never once binding — `sui-cache-pg` took 34 OOMKills at a
/// 202.8Mi high-water under a 1Gi limit with cgroup `failcnt = 0`.
///
/// # What it deliberately does NOT do
///
/// It never performs a QoS **class transition** in place. Kubernetes refuses one
/// unconditionally (`ValidatePodResize`, release-1.33
/// `validation.go:5665` — "Pod QOS Class may not change as a result of
/// resizing"), so a class change is a template edit and therefore a git commit.
/// `spec.qosTarget` declares the desired class; a gap between it and the observed
/// class produces a proposal for the durable door, never an in-place write. The
/// two actuations carry disjoint payload types through disjoint traits
/// (`breathe_provider::request`), so routing one through the other is a compile
/// error rather than a runtime check.
///
/// # Status, and what is honestly missing from it
///
/// This kind reuses [`BandStatus`] verbatim. The request-specific projection an
/// operator will eventually want — `qosObserved`, the typed `qosGap`, a
/// `pendingProposal` — is **not** on the status yet, on purpose: nothing writes
/// it at this stage, and a status field with no writer is precisely the
/// claimed-but-not-real shape this whole dimension exists to end. The types
/// (`QosGap`, `ClassTransitionBlocked`) ship in `breathe-provider` ready for the
/// controller pass that fills them.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "RequestBand",
    namespaced,
    status = "BandStatus",
    shortname = "rqband",
    category = "breathe",
    printcolumn = r#"{"name":"Target","type":"string","jsonPath":".spec.targetRef.name"}"#,
    printcolumn = r#"{"name":"Resource","type":"string","jsonPath":".spec.resource"}"#,
    printcolumn = r#"{"name":"Class","type":"string","jsonPath":".spec.workloadClass"}"#,
    printcolumn = r#"{"name":"QoSTarget","type":"string","jsonPath":".spec.qosTarget"}"#,
    printcolumn = r#"{"name":"Durability","type":"string","jsonPath":".spec.durability"}"#,
    printcolumn = r#"{"name":"Util","type":"string","jsonPath":".status.lastUtil"}"#,
    printcolumn = r#"{"name":"Last","type":"string","jsonPath":".status.lastDecision"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Gate","type":"string","jsonPath":".status.effectiveGate.state"}"#,
    printcolumn = r#"{"name":"Why","type":"string","jsonPath":".status.effectiveGate.reason"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RequestBandSpec {
    pub target_ref: TargetRef,
    /// Which resource's request to carve. REQUIRED — there is no sensible
    /// default, and guessing between the OOM lever and the scheduling lever is
    /// exactly the ambiguity this dimension exists to remove.
    pub resource: RequestResourceSpec,
    /// The named [`BreathePosture`] this band's unset behavioral fields fall back
    /// to — including the request-policy fields (`workloadClass`, `qosTarget`,
    /// `demand`) added for this kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posture_ref: Option<String>,
    /// The demand statistic. Unset ⇒ the posture's, else the compiled default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub demand: Option<DemandSignalSpec>,
    /// The workload's criticality class. Unset ⇒ the posture's, else `standard`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_class: Option<WorkloadClassSpec>,
    /// The DESIRED QoS class. Unset ⇒ the posture's `qosTarget`, else the
    /// resolved `workloadClass`'s default.
    ///
    /// **Note the order, because it is a safety property and it is not the
    /// order people guess.** The fold is PER FIELD: an explicit posture
    /// `qosTarget` outranks a class-DERIVED default, so setting `workloadClass:
    /// batch` on a band does NOT quietly downgrade a workload the posture pinned
    /// to `guaranteed`. To weaken a pinned seal you must say `qosTarget`
    /// explicitly on the band — out loud, where a reviewer sees it. The other
    /// reading would let a band author strip a seal as a side effect of naming a
    /// class, which is the victoria-logs-422 shape arriving by a new route.
    ///
    /// A gap between this and the observed class is reported and, when a durable
    /// writer exists, proposed as a manifest edit. It is **never** actuated in
    /// place — see the type doc.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qos_target: Option<QosClassSpec>,
    /// Where a converged value must land. Defaults to `committed`.
    #[serde(default)]
    pub durability: DurabilitySpec,
    /// **Where in git the converged value comes to rest.**
    ///
    /// `{ path, marker }` — a repo-relative manifest path plus the marker id
    /// that must appear in a `# {"$breathe": "<marker>"}` comment on the value
    /// line. Both are operator-authored; neither is inferred.
    ///
    /// Unset is the honest default and NOT a silent downgrade to ephemeral: a
    /// `durability: committed` band with no `manifestRef` reports
    /// `Blocked(noManifestCoordinate)`, because a band that declares its value
    /// must survive a rollout and cannot say where it lives is misauthored, not
    /// merely unconfigured.
    ///
    /// The marker is also the blast-radius floor. breathe never walks a
    /// document looking for a likely field — the only anchor it has is one a
    /// human put in the file, so **a manifest nobody marked cannot be written
    /// to at all**.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_ref: Option<breathe_provider::ManifestCoordinate>,
    /// Advisory lower bound in the band's unit (`256Mi` / `250m`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub floor: String,
    /// Advisory upper bound. **NOT the binding ceiling** — that is always the
    /// LIVE limit, because k8s rejects `request > limit` at admission and a
    /// band's declared ceiling is capacity policy, not the value the apiserver
    /// measures against. Enforced by `RequestTarget::new`, which has no arm
    /// above the live limit.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ceiling: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setpoint: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grow_above: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shrink_below: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grow_factor: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shrink_factor: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cooldown_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staleness_seconds: Option<u64>,
    /// RETIRED 2026-06-19 (breathe@76924b0) — **this field has NO effect on this
    /// band kind.** It is not read by the carve gate; setting it to `true` does
    /// not hold the band in shadow, and a band with `dryRun: true` and no `mode`
    /// carves for real once its confirm window elapses.
    ///
    /// It is kept, not deleted, because it is the record of a decision an
    /// operator authored — but it decides nothing. Authorization is
    /// `spec.writeIntent`; the live verdict (and its reason or witness) is
    /// `status.effectiveGate`.
    #[serde(default)]
    pub dry_run: bool,
    /// **The authorization intent** — the first and highest link in the
    /// resolution chain `writeIntent` > `mode` > the compiled
    /// `shadowConfirmEffect`.
    ///
    /// * `{intent: observe}` — decide, report, attest; never write.
    /// * `{intent: calibrateThenWrite, confirmAfterSeconds: 1800}` — shadow until
    ///   a clean-observation window proves the band safe, then write.
    /// * `{intent: write, authorizedBy: "…"}` — write now.
    /// * `{intent: frozen}` — never write, but keep observing.
    ///
    /// `authorizedBy` is REQUIRED on `write`: an `{intent: write}` naming no
    /// `authorizedBy` never goes live: it is held in shadow as `intentMalformed`.
    /// NOTE the tier — that is a runtime mitigation, not an apiserver rejection:
    /// a k8s structural schema cannot express "this property is required only
    /// when another property has this value", so the API accepts the object and
    /// the controller refuses to act on it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_intent: Option<WriteIntentSpec>,
    /// The RETIRED promotion lifecycle. Still read (below `writeIntent`), so an
    /// already-authored CR keeps working; new CRs should author `writeIntent`
    /// instead. Unset ⇒ the compiled fleet default `shadowConfirmEffect` — it
    /// does **not** mean "derived from `dryRun`". Values: `shadow` | `effect` |
    /// `shadowConfirmEffect` | `suspended`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<PromotionMode>,
    #[serde(default = "d_confirm_after")]
    pub confirm_after_seconds: u64,
    /// May breathe create a request on a target whose author declared none?
    /// Default `forbidden`.
    ///
    /// This is the BestEffort case, and it is load-bearing: a BestEffort pod is
    /// the one most in danger AND the one a bound-introduction would change most
    /// (it moves the pod's QoS class, which cannot be done in place at all). So
    /// the default refuses, and the honest path for a BestEffort target is a
    /// declared `qosTarget` plus a durable writer — not a silently-seeded request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bound_introduction: Option<BoundIntroductionSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disruption_policy: Option<DisruptionPolicy>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suspend: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_limit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_limit_expiry: Option<String>,
    #[serde(default = "d_peak_decay")]
    pub peak_decay: f64,
    #[serde(default = "d_warmup_seconds")]
    pub warmup_seconds: u64,
}

#[allow(clippy::doc_markdown)] // "QoS" is an English acronym in these method docs
impl RequestBandSpec {
    /// The resolved workload class (spec > posture > `standard`).
    #[must_use]
    pub fn resolved_workload_class(&self, posture: Option<&BreathePostureSpec>) -> WorkloadClassSpec {
        self.workload_class
            .or_else(|| posture.and_then(|p| p.workload_class))
            .unwrap_or(WorkloadClassSpec::Standard)
    }

    /// The resolved QoS TARGET (spec > posture > the workload class's default).
    #[must_use]
    pub fn resolved_qos_target(&self, posture: Option<&BreathePostureSpec>) -> QosClassSpec {
        self.qos_target
            .or_else(|| posture.and_then(|p| p.qos_target))
            .unwrap_or_else(|| self.resolved_workload_class(posture).default_qos())
    }

    /// The resolved demand statistic (spec > posture > compiled default).
    #[must_use]
    pub fn resolved_demand(&self, posture: Option<&BreathePostureSpec>) -> DemandSignalSpec {
        self.demand
            .clone()
            .or_else(|| posture.and_then(|p| p.demand.clone()))
            .unwrap_or_default()
    }

    /// The typed layout this band carves — the REQUEST peer of `PodResize`.
    #[must_use]
    pub fn provider_layout(&self) -> LimitLayout {
        LimitLayout::PodRequestResize { container: self.target_ref.container.clone() }
    }

    /// **Where this band's value must land in git — or why it cannot.**
    ///
    /// The ONE place the `durability × manifestRef` decision is made, so a
    /// reconciler, the status writer and any future consumer cannot disagree
    /// about whether a band has a durable home. The three cases:
    ///
    /// * `Ephemeral` ⇒ `Err(EphemeralCannotTransition)` — this band declared it
    ///   does not need to survive a rollout, so there is nothing to commit and
    ///   asking for a coordinate would be the wrong question.
    /// * `Committed` + no `manifestRef` ⇒ `Err(NoManifestCoordinate)` — the
    ///   misauthored case, reported rather than silently downgraded to
    ///   ephemeral. A band that says its value must survive a rollout and
    ///   cannot say where it lives is a bug in the CR, and the operator has to
    ///   see that.
    /// * `Committed` + a `manifestRef` ⇒ `Ok(coordinate)`.
    ///
    /// # Errors
    ///
    /// The typed reason no durable write is possible, ready to publish straight
    /// into `status.qosGap` as `Blocked(..)`.
    pub fn durable_coordinate(
        &self,
    ) -> Result<&breathe_provider::ManifestCoordinate, breathe_provider::ClassTransitionBlocked> {
        use breathe_provider::ClassTransitionBlocked as B;
        match self.durability {
            DurabilitySpec::Ephemeral => Err(B::EphemeralCannotTransition),
            DurabilitySpec::Committed => self.manifest_ref.as_ref().ok_or(B::NoManifestCoordinate),
        }
    }
}

impl crate::Band for RequestBand {
    fn dimension_id(&self) -> breathe_provider::DimensionId {
        breathe_provider::DimensionId::Request
    }
    fn target_ref(&self) -> &TargetRef {
        &self.spec.target_ref
    }
    fn band_config(&self) -> anyhow::Result<BandConfig> {
        self.band_config_with_posture(None)
    }
    fn peak_decay(&self) -> f64 {
        self.spec.peak_decay
    }
    fn warmup_seconds(&self) -> u64 {
        self.spec.warmup_seconds
    }
    fn max_staleness_seconds(&self) -> u64 {
        self.spec.max_staleness_seconds.unwrap_or_else(d_max_staleness)
    }
    fn cooldown_seconds(&self) -> u64 {
        self.spec.cooldown_seconds.unwrap_or_else(d_cooldown)
    }
    fn dry_run(&self) -> bool {
        self.spec.dry_run
    }
    fn write_intent_spec(&self) -> Option<&WriteIntentSpec> {
        self.spec.write_intent.as_ref()
    }
    fn mode_spec(&self) -> Option<PromotionMode> {
        self.spec.mode
    }
    fn bound_introduction_spec(&self) -> Option<BoundIntroductionSpec> {
        self.spec.bound_introduction
    }
    fn confirm_after_seconds(&self) -> u64 {
        self.spec.confirm_after_seconds
    }
    fn last_change_epoch(&self) -> Option<i64> {
        self.status.as_ref().and_then(|s| s.last_change_epoch)
    }
    fn disruption_policy(&self) -> DisruptionPolicy {
        self.spec.disruption_policy.unwrap_or_default()
    }
    fn suspended(&self) -> bool {
        self.spec.suspend
    }
    fn force_limit_value(&self) -> Option<u64> {
        self.spec.force_limit.as_deref().and_then(|q| self.spec.resource.unit().parse(q))
    }
    fn force_limit_expiry(&self) -> Option<&str> {
        self.spec.force_limit_expiry.as_deref()
    }
    /// **Always `None` — predictive carving is deliberately not offered here.**
    ///
    /// Predictive grow pre-raises a bound for a *projected* burst. On a limit
    /// that is free: an unhit ceiling costs nothing. On a RESERVATION it is not
    /// free — every predicted byte is capacity actually withheld from the
    /// scheduler, times replicas, and a wrong prediction is a workload that
    /// cannot place. The reservation tracks a measured quantile of what the
    /// workload really used; it does not speculate.
    fn predictive(&self) -> Option<f64> {
        None
    }
    fn status(&self) -> Option<&BandStatus> {
        self.status.as_ref()
    }
    fn posture_ref(&self) -> Option<&str> {
        self.spec.posture_ref.as_deref()
    }
    fn band_config_with_posture(&self, posture: Option<&BreathePostureSpec>) -> anyhow::Result<BandConfig> {
        let s = &self.spec;
        let unit = s.resource.unit();
        // A blank floor/ceiling means "unset" and falls back to the unit's
        // compiled default, so an author who cares about only the demand signal
        // does not have to restate capacity bounds. The band's `ceiling` is
        // advisory regardless — the BINDING ceiling is the live limit, enforced
        // by `RequestTarget::new`, which no CRD value can widen.
        let (d_floor, d_ceiling) = match s.resource {
            RequestResourceSpec::Memory => (d_floor_bytes(), d_ceiling_bytes()),
            RequestResourceSpec::Cpu => (d_floor_milli(), d_ceiling_milli()),
        };
        let floor = if s.floor.is_empty() { d_floor } else { s.floor.clone() };
        let ceiling = if s.ceiling.is_empty() { d_ceiling } else { s.ceiling.clone() };
        crate::band_config_of(
            s.setpoint.or_else(|| posture.map(|p| p.setpoint)).unwrap_or_else(d_setpoint),
            s.grow_above.or_else(|| posture.map(|p| p.grow_above)).unwrap_or_else(d_grow_above),
            s.shrink_below.or_else(|| posture.map(|p| p.shrink_below)).unwrap_or_else(d_shrink_below),
            s.grow_factor.or_else(|| posture.map(|p| p.grow_factor)).unwrap_or_else(d_grow_factor),
            s.shrink_factor.or_else(|| posture.map(|p| p.shrink_factor)).unwrap_or_else(d_shrink_factor),
            &floor,
            &ceiling,
            // No `requestFloor`: on THIS kind the carved value IS the request, so
            // a "request floor" separate from the floor would be the same number
            // twice — and a second place to get it wrong.
            "",
            s.warmup_seconds,
            unit,
        )
    }
    fn cooldown_seconds_with_posture(&self, posture: Option<&BreathePostureSpec>) -> u64 {
        self.spec
            .cooldown_seconds
            .or_else(|| posture.map(|p| u64::from(p.cooldown_seconds)))
            .unwrap_or_else(d_cooldown)
    }
    fn max_staleness_seconds_with_posture(&self, posture: Option<&BreathePostureSpec>) -> u64 {
        self.spec
            .max_staleness_seconds
            .or_else(|| posture.map(|p| u64::from(p.max_staleness_seconds)))
            .unwrap_or_else(d_max_staleness)
    }
    fn disruption_policy_with_posture(&self, posture: Option<&BreathePostureSpec>) -> DisruptionPolicy {
        self.spec
            .disruption_policy
            .or_else(|| posture.map(|p| p.disruption_policy))
            .unwrap_or_default()
    }
}

// ─────────────────── BreatheNodePool — host enrollment ──────────────────

/// A GiB quantity bounded to a sane node maximum (1 PiB) so that `value * 2^30`
/// (the bytes conversion the host agent performs) can NEVER overflow `u64`. The
/// bound is an OpenAPI `maximum` enforced at the apiserver parse boundary — an
/// overflowing ceiling is rejected at admission, not merely caught at runtime
/// (★★ UNREPRESENTABILITY: parse-time-rejected). The agent additionally uses
/// `checked_mul` as a truly-unrepresentable backstop for any non-apiserver write.
///
/// `JsonSchema` is hand-written: a `#[serde(transparent)]` newtype drops a
/// field-level `#[schemars(range)]`, so the `maximum` is injected here directly.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(transparent)]
pub struct GiB(pub u64);

/// The largest GiB value whose byte conversion (`* 2^30`) still fits `u64` with
/// vast headroom — 1 PiB. No real node ARC/cgroup ceiling approaches this.
pub const GIB_MAX: u64 = 1_048_576;

impl schemars::JsonSchema for GiB {
    fn schema_name() -> String {
        "GiB".into()
    }
    fn json_schema(_g: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        use schemars::schema::{InstanceType, NumberValidation, SchemaObject};
        SchemaObject {
            instance_type: Some(InstanceType::Integer.into()),
            format: Some("uint64".into()),
            number: Some(Box::new(NumberValidation {
                minimum: Some(0.0),
                maximum: Some(GIB_MAX as f64),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

/// The per-node L2 ceilings, mirrored from `pleme.nixos.nodeBudget` — the host
/// agent refuses any write above these (the second safety wall). Cluster-scoped:
/// one BreatheNodePool enrolls one node.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "BreatheNodePool",
    shortname = "bnp",
    category = "breathe",
    status = "NodePoolStatus",
    printcolumn = r#"{"name":"Node","type":"string","jsonPath":".spec.nodeName"}"#,
    printcolumn = r#"{"name":"Writes","type":"boolean","jsonPath":".spec.writeEnabled"}"#,
    printcolumn = r#"{"name":"ArcMaxGiB","type":"integer","jsonPath":".spec.arcMaxGiB"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BreatheNodePoolSpec {
    /// The node this pool enrolls (matches `kubernetes.io/hostname`). The agent
    /// reconciles only the pool whose `nodeName` equals its own `NODE_NAME`.
    pub node_name: String,
    /// L2 ARC ceiling — `nodeBudget.arcMaxGiB` (the boot modprobe cap), in GiB
    /// (bounded ≤ 1 PiB so the bytes conversion cannot overflow).
    pub arc_max_gi_b: GiB,
    /// L2 cgroup ceiling per systemd unit — the unit's `nodeBudget` `memoryMaxGiB`,
    /// in GiB. A `CgroupBand` whose unit is absent here is refused (never written
    /// blind).
    #[serde(default)]
    pub cgroup_max_gi_b: BTreeMap<String, GiB>,
    /// L2 cpu ceiling per systemd unit — the unit's cpu territory in MILLICORES
    /// (`nodeBudget` cpu budget). A `CgroupCpuBand` whose unit is absent here is
    /// refused (never written blind). Millicores need no overflow bound (they are
    /// compared, never multiplied), so a plain integer — unlike `GiB`.
    #[serde(default)]
    pub cgroup_cpu_max_milli: BTreeMap<String, u64>,
    /// Node-level MASTER write switch. `false` = the whole node is in SHADOW —
    /// every host band decides + reports but never mutates the host, regardless of
    /// per-band `dryRun`. The safe default; flip to `true` only after the shadow
    /// window holds.
    #[serde(default)]
    pub write_enabled: bool,
}

/// BreatheNodePool status — the enrollment receipt.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodePoolStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_units: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_epoch: Option<i64>,
}

// ────────── PodMemoryHigh — the SOFT-k8s-carve controller→host-agent dispatch ──────────
// `docs/OOM-VERIFICATION.md` § Part 1. A `MemoryBand` efficiency carve must write the
// live pod's cgroup-v2 `memory.high` (SOFT/reclaim), NOT the k8s `limits.memory`
// (`memory.max`, HARD/kill). The DECISION (what soft value) is the controller's — it
// reads the pod working set via metrics-server. The WRITE is the host-agent's — it
// owns the node's cgroup files. This CR is the typed hand-off: the controller declares
// the desired pod `memory.high`; the host-agent that owns the node reconciles it via the
// shipped `HostKnob::PodCgroupMemoryHigh` writer (NOT a parallel mechanism — the same
// node-keyed-band reconcile shape ArcBand/CgroupBand/HostParamBand use). A DESIRED-VALUE
// dispatch (a number to write), never a self-deciding band (the agent never re-decides —
// it has no metrics-server access). Cluster-scoped, one per managed pod-container.

/// The kubelet cgroup driver mirror for the CRD (serde mirror of
/// `breathe_provider::CgroupDriver`) — selects the pod cgroup-v2 path layout the
/// host-agent writes (systemd `.slice`/`.scope` vs cgroupfs flat).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum CgroupDriverSpec {
    /// systemd driver (NixOS/containerd default + rio's live driver).
    #[default]
    Systemd,
    /// cgroupfs driver (flat kubepods layout).
    Cgroupfs,
}

impl CgroupDriverSpec {
    /// The provider-typed driver the host-agent path mapper dispatches on.
    #[must_use]
    pub fn provider(self) -> breathe_provider::CgroupDriver {
        match self {
            Self::Systemd => breathe_provider::CgroupDriver::Systemd,
            Self::Cgroupfs => breathe_provider::CgroupDriver::Cgroupfs,
        }
    }
}

/// **PodMemoryHigh** — the controller→host-agent SOFT-carve dispatch (cluster-scoped).
/// The controller resolves the pod's cgroup coordinates (UID + CRI container id + QoS)
/// and writes the desired `memory.high` bytes here; the host-agent on `nodeName`
/// reconciles it onto the pod's cgroup file. The HARD `memory.max` (k8s `limits.memory`)
/// is NEVER carved here — it is governed by the never-OOM peak ceiling on the k8s plane.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "PodMemoryHigh",
    shortname = "pmh",
    category = "breathe",
    status = "PodMemoryHighStatus",
    printcolumn = r#"{"name":"Node","type":"string","jsonPath":".spec.nodeName"}"#,
    printcolumn = r#"{"name":"QoS","type":"string","jsonPath":".spec.qosClass"}"#,
    printcolumn = r#"{"name":"DesiredBytes","type":"integer","jsonPath":".spec.desiredBytes"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PodMemoryHighSpec {
    /// The node hosting the pod — the host-agent reconciles only the PodMemoryHigh
    /// whose `nodeName` equals its own `NODE_NAME` (the BreatheNodePool node-match).
    pub node_name: String,
    /// The pod's `status.qosClass` (`Guaranteed`/`Burstable`/`BestEffort`) — which
    /// kubepods cgroup subtree the pod's `memory.high` lives under.
    pub qos_class: String,
    /// The pod's `metadata.uid` — the per-pod cgroup slice/dir is named for it.
    pub pod_uid: String,
    /// The CRI container-runtime id (`containerd://…`/`cri-o://…`) — the per-container
    /// cgroup scope/dir; the host-agent path mapper scheme-strips it.
    pub container_runtime_id: String,
    /// The kubelet cgroup driver — selects the path layout (default systemd).
    #[serde(default)]
    pub cgroup_driver: CgroupDriverSpec,
    /// The DESIRED `memory.high` value in BYTES the host-agent must write — the
    /// controller's efficiency-carve target (SOFT/reclaim). NEVER a `memory.max` value.
    pub desired_bytes: u64,
    /// The owning `MemoryBand` (namespace/name) — provenance for the audit trail +
    /// the controller's ownership of this dispatch CR.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_band: Option<String>,
    /// SHADOW: the agent observes + reports the desired write but performs no cgroup
    /// mutation. Composes with the node's `BreatheNodePool.writeEnabled` master switch
    /// (either being shadow keeps the agent observe-only).
    #[serde(default)]
    pub dry_run: bool,
}

/// PodMemoryHigh status — the host-agent's reconcile receipt for the dispatch.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PodMemoryHighStatus {
    /// `Applied` (cgroup written) / `ShadowWouldApply` / `Error` / `Pending`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// The `memory.high` value the agent last wrote (or would write in shadow), bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub written_bytes: Option<i64>,
    /// The node that reconciled this dispatch (the host-agent's `NODE_NAME`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_node: Option<String>,
    /// A typed error message when the write failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// **The typed authorization verdict for the last tick** — the same field
    /// `BandStatus` carries, on a Tier-B kind. Added 2026-07-26: this kind wrote
    /// ONLY the legacy `effectiveDryRun` bool, so after a refactor whose whole
    /// point was one legible verdict, two status surfaces still answered "why?"
    /// with nothing at all. `witness: legacyDefault` + `legacyPath:
    /// twoStateDryRun` is the honest reading here — this kind has no
    /// `spec.writeIntent` field yet, so every live write it makes IS migration
    /// debt, and now says so.
    ///
    /// Written from the SAME `EffectiveGate` value as `effectiveDryRun` below,
    /// so the two can never disagree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_gate: Option<EffectiveGateReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_epoch: Option<i64>,
}

impl PodMemoryHighSpec {
    /// The provider-typed host knob the agent writes — maps the dispatch CR fields to
    /// the shipped `HostKnob::PodCgroupMemoryHigh` (the SOFT cgroup-file lever).
    #[must_use]
    pub fn provider_knob(&self) -> breathe_provider::HostKnob {
        breathe_provider::HostKnob::PodCgroupMemoryHigh {
            driver: self.cgroup_driver.provider(),
            qos: self.qos_class.clone(),
            pod_uid: self.pod_uid.clone(),
            container_runtime_id: self.container_runtime_id.clone(),
        }
    }
}

// ─────────────────── BreatheCloudPool — node-count Forma enrollment (BU2) ──────

/// **BreatheCloudPool** — the declarative enrollment of a node-count `Forma`
/// into a breathe band (cluster-scoped). Where a `MemoryBand`/`CpuBand` holds a
/// workload's LIMIT in band, a `BreatheCloudPool` holds a node POOL's COUNT in
/// band — the same shape-blind law (`decide`) converges on a node count exactly
/// as on bytes. It binds a `Forma` (the resource shape, e.g. `node-on-demand`)
/// to a `Densa`-style envelope (floor/ceiling node counts + optional cost SLA)
/// and a relief-latency cadence; the controller's node-Forma reconciler (BU1)
/// watches these and runs `reconcile_forma` per pool.
///
/// SHADOW-first (`dryRun`) + a pool-level master `writeEnabled` switch (peer of
/// `BreatheNodePool`): a pool provisions for real only when BOTH `writeEnabled`
/// AND `!dryRun` AND the actuator (a magma `Plan`, BU10) is wired. Until then it
/// is observe-only — it reports what it WOULD provision. `kubectl get bcp`.
///
/// Which executor realizes the pool. The default `KubeObserve` can NEVER mutate
/// (its provision/deprovision are `DryRun` by construction) — a pool is
/// observe-only unless it EXPLICITLY opts into an actuating provider.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ProviderKind {
    /// Read the live node inventory; provision/deprovision are always `DryRun`.
    /// Observe-only by construction — the safe default.
    #[default]
    KubeObserve,
    /// Create/drain **kwok fake nodes** (the multi-node go-live bed). Actuates
    /// only when the pool is live (`writeEnabled && !dryRun`); a fake node is
    /// tainted + labelled so real pods never land and only breathe's own fakes
    /// are ever deleted. Zero cloud cost.
    Kwok,
}

/// Which REALIZATION mechanism turns a `Grew` decision into an actual new
/// node. Orthogonal to [`ProviderKind`] (the SIGNAL source — real cluster vs.
/// the kwok fake-node test bed): `ProviderKind::KubeObserve` reads real node
/// demand/capacity; `NodeProvisioningBackend` then picks HOW that pool's
/// `Grew` tick gets realized. Consulted ONLY when `provider == KubeObserve` —
/// a `Kwok` test-bed pool ignores it (crossing kwok's fake nodes with a real
/// Karpenter realization is nonsensical and unsafe, so it is structurally
/// excluded rather than merely discouraged).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum NodeProvisioningBackend {
    /// The existing, live path (census/`CamelotAgentNode` precedent): breathe
    /// stays observe-only for the cloud mutation itself — reports
    /// `wouldProvision` in shadow; when live, claims a Ready node a
    /// human/Pangea separately provisioned via
    /// `Pangea::Architectures::CamelotAgentNode` + pangea-operator. The
    /// default — matches Camelot's active "stick to AMI" posture.
    #[default]
    K3sCustomAmi,
    /// Realize against a real upstream Karpenter install: read the
    /// referenced `karpenter.sh/v1 NodePool` (`karpenterNodePoolRef`) and, on
    /// `Grew`, mint `karpenter.sh/v1 NodeClaim` objects copying its
    /// `spec.template.spec` verbatim. Shadow-first via the same
    /// `dryRun`/`writeEnabled` gates as every other backend.
    EksKarpenter,
    /// Realize against a plain EKS-managed nodegroup — an ASG the EKS
    /// service itself owns, NOT a real Karpenter install (Camelot's
    /// `system`/`controllers` pools today: zero Karpenter, plain managed
    /// nodegroups). Reads the referenced nodegroup's live
    /// `scalingConfig`/`status` via `DescribeNodegroup` and, on
    /// `Grew`/`Shrank`, mutates `scalingConfig.desiredSize` via
    /// `UpdateNodegroupConfig` — the ONLY mutable knob a managed nodegroup
    /// exposes (see `breathe_controller::eks_nodegroup_provedor`'s module
    /// doc for why the underlying ASG is never touched directly). Shadow-
    /// first via the same `dryRun`/`writeEnabled` gates as every other
    /// backend. Requires `eksManagedNodegroupRef`.
    EksManagedNodegroup,
}

impl NodeProvisioningBackend {
    fn is_default(&self) -> bool {
        matches!(self, NodeProvisioningBackend::K3sCustomAmi)
    }
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "BreatheCloudPool",
    shortname = "bcp",
    category = "breathe",
    status = "CloudPoolStatus",
    printcolumn = r#"{"name":"Forma","type":"string","jsonPath":".spec.forma"}"#,
    printcolumn = r#"{"name":"Floor","type":"integer","jsonPath":".spec.floor"}"#,
    printcolumn = r#"{"name":"Ceiling","type":"integer","jsonPath":".spec.ceiling"}"#,
    printcolumn = r#"{"name":"Used","type":"integer","jsonPath":".status.observedUsed"}"#,
    printcolumn = r#"{"name":"Capacity","type":"integer","jsonPath":".status.observedCapacity"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"DryRun","type":"boolean","jsonPath":".spec.dryRun"}"#,
    printcolumn = r#"{"name":"Lane","type":"string","jsonPath":".spec.lane"}"#,
    printcolumn = r#"{"name":"Tainted","type":"string","jsonPath":".status.taintedNode"}"#,
    printcolumn = r#"{"name":"Backend","type":"string","jsonPath":".spec.nodeProvisioningBackend"}"#,
    printcolumn = r#"{"name":"Flapping","type":"boolean","jsonPath":".status.flapDetected"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BreatheCloudPoolSpec {
    /// The resource SHAPE this pool provisions — the `Forma` name (e.g.
    /// `node-on-demand`). Must match a `breathe_provider::Forma` variant.
    pub forma: String,
    /// The node-COUNT floor (the never-swap base — always provisioned).
    pub floor: u64,
    /// The node-COUNT ceiling (the L2 wall — the band carves ≤ it).
    pub ceiling: u64,
    /// Cost ceiling (cents per accounting period) the pool must stay within.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_sla_cents: Option<u64>,
    /// The provisioning dead-time — how long one `provision(1)` takes to become
    /// usable capacity. The predictor must forecast ≥ this far ahead or
    /// provisioning is always late (thesis P8). Seconds.
    #[serde(default = "d_relief_latency")]
    pub relief_latency_seconds: u64,
    #[serde(default = "d_setpoint")]
    pub setpoint: f64,
    #[serde(default = "d_grow_above")]
    pub grow_above: f64,
    #[serde(default = "d_shrink_below")]
    pub shrink_below: f64,
    #[serde(default = "d_grow_factor")]
    pub grow_factor: f64,
    #[serde(default = "d_shrink_factor")]
    pub shrink_factor: f64,
    #[serde(default = "d_cooldown")]
    pub cooldown_seconds: u64,
    /// SHADOW: observe + report what it WOULD provision; never actuate.
    #[serde(default)]
    pub dry_run: bool,
    /// Pool-level MASTER write switch (peer of `BreatheNodePool.writeEnabled`):
    /// `false` ⇒ the whole pool is in shadow regardless of `dryRun`. Safe default.
    #[serde(default)]
    pub write_enabled: bool,
    /// How the pool is FILLED — `pack` (bin-pack tight, the efficiency-first
    /// default) or `spread` (distribute across failure domains for HA). breathe
    /// SETS this posture + surfaces the scheduler scoring hint; the scheduler
    /// binds. Omitted on serialize at the `pack` default.
    #[serde(default, skip_serializing_if = "breathe_provider::FillPolicy::is_pack")]
    pub fill_policy: breathe_provider::FillPolicy,
    /// Forecast demand AHEAD of `reliefLatencySeconds` instead of reacting to
    /// current util. Node boot is slow, so a reactive pool is always late; the
    /// `LinearTrendPrevisor` projects the recent slope `reliefLatencySeconds`
    /// ahead so capacity lands in time. MONOTONE-SAFE: it only ever provisions
    /// EARLIER, never shrinks prematurely (a falling trend echoes the reactive
    /// value), so it is strictly safer than reactive. Default off (peer of the
    /// limit-side `predictive`). Omitted on serialize at the `false` default.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub predictive: bool,
    /// Which executor realizes this pool (default `kubeObserve` = observe-only,
    /// can never mutate). Set `kwok` for the fake-node go-live bed.
    #[serde(default, skip_serializing_if = "ProviderKind::is_default")]
    pub provider: ProviderKind,
    /// The AWS lane this pool's nodes join on — one of
    /// `breathe_auction::spread::Lane::as_str()`'s values (e.g.
    /// `"standalone-ec2-instance"`). A plain string BY REFERENCE (never a
    /// dependency on the `breathe-auction` crate from here) — the same
    /// composes-by-reference discipline `breathe-invariant`'s `doctrine_ref`
    /// uses, so the CRD stays decoupled from that crate's churn. `None` ⇒ no
    /// lane-specific behaviour (today: node-claiming stays off — see
    /// `node_forma::claim_unassigned_node_for_pool`, gated on the
    /// `"standalone-ec2-instance"` string matching `Lane::StandaloneEc2Instance`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane: Option<String>,
    /// Which mechanism realizes a `Grew` decision into an actual new node.
    /// Consulted only when `provider == KubeObserve`; a `Kwok` test-bed pool
    /// ignores it. Default `k3sCustomAmi` — today's shadow + correnteza-claim
    /// path, zero behaviour change.
    #[serde(default, skip_serializing_if = "NodeProvisioningBackend::is_default")]
    pub node_provisioning_backend: NodeProvisioningBackend,
    /// The real `karpenter.sh/v1 NodePool`'s `metadata.name` this pool mints
    /// `NodeClaim`s against — REQUIRED when `nodeProvisioningBackend ==
    /// eksKarpenter` (validated at reconcile time: an unset ref under that
    /// backend reconciles to `phase: Error`, mirroring the unknown-`forma`
    /// early-return — never guesses a name). Ignored under `k3sCustomAmi`.
    /// The referenced NodePool is a PRECONDITION breathe only ever READS — it
    /// is authored the existing way, by `pleme-lib`'s `_karpenter.tpl` via
    /// Helm (GitOps-native); breathe never creates or mutates a NodePool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub karpenter_node_pool_ref: Option<String>,
    /// The real EKS-managed nodegroup this pool reflects/scales — REQUIRED
    /// when `nodeProvisioningBackend == eksManagedNodegroup` (validated at
    /// reconcile time exactly like `karpenterNodePoolRef`: an unset ref
    /// under that backend reconciles to `phase: Error`, never guesses).
    /// Ignored under every other backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eks_managed_nodegroup_ref: Option<EksManagedNodegroupRef>,
}

/// The `(clusterName, nodegroupName)` pair scoping an EKS `DescribeNodegroup`/
/// `UpdateNodegroupConfig` call. Two fields, unlike `karpenterNodePoolRef`'s
/// bare `String`: a `karpenter.sh NodePool` name alone is enough (it's a
/// cluster-scoped k8s object breathe reads via the SAME apiserver connection
/// it already has), but the EKS control-plane API is scoped by BOTH the
/// owning EKS cluster's name AND the nodegroup's name — neither is
/// inferable from the in-cluster `kube::Client` breathe otherwise uses.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EksManagedNodegroupRef {
    /// The EKS cluster's name (`DescribeNodegroup`'s own `clusterName`
    /// parameter — the EKS control-plane's name, not necessarily this
    /// kube context's own name).
    pub cluster_name: String,
    /// The EKS managed nodegroup's name (`DescribeNodegroup`'s own
    /// `nodegroupName` parameter).
    pub nodegroup_name: String,
}

impl ProviderKind {
    fn is_default(&self) -> bool {
        matches!(self, ProviderKind::KubeObserve)
    }
}

impl BreatheCloudPoolSpec {
    /// The `BandConfig` this pool carves with — node COUNTS in the unit-blind
    /// `floor_bytes`/`ceiling_bytes` fields (the band law is shape-blind).
    #[must_use]
    pub fn band_config(&self) -> BandConfig {
        BandConfig {
            grow_above: self.grow_above,
            shrink_below: self.shrink_below,
            setpoint: self.setpoint,
            grow_factor: self.grow_factor,
            shrink_factor: self.shrink_factor,
            floor_bytes: self.floor,
            ceiling_bytes: self.ceiling,
            // node-count bands have no k8s requests.<resource> concept.
            request_floor_bytes: 0,
            // node Formas have no restart/boot-spike concept ⇒ warmup disabled.
            warmup_seconds: 0,
            // A node-COUNT of 0 is a REAL value (a pool scaled to zero), not a
            // degraded metric — so the split-brain gate must NOT fire here; run
            // the law on the true count. (Memory/cpu pod bands gate 0 as untrusted.)
            metric_missing_policy: breathe_control::MetricMissingPolicy::Trust,
        }
    }
}

/// `BreatheCloudPool` status — the per-tick node-Forma receipt (observe-only in
/// shadow; what it WOULD provision surfaced via `would_provision`).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CloudPoolStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_used: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_capacity: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_decision: Option<String>,
    /// Signed node delta the pool WOULD provision (+) / deprovision (−) this tick.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub would_provision: Option<i64>,
    /// The kube-scheduler scoringStrategy hint the pool's `fillPolicy` implies
    /// (`MostAllocated` for pack / `LeastAllocated` for spread) — surfaced for the
    /// scheduler profile; breathe never binds a pod.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduler_scoring: Option<String>,
    /// Whether the forecasting (`LinearTrendPrevisor`) path drove this tick's
    /// decision (`spec.predictive` on) vs the reactive echo. Lets an operator
    /// confirm the predictive posture is live from the status alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predictive_active: Option<bool>,
    /// **The typed authorization verdict for the last tick** — the same field
    /// `BandStatus` carries, on a Tier-B kind. Added 2026-07-26: this kind wrote
    /// ONLY the legacy `effectiveDryRun` bool, so after a refactor whose whole
    /// point was one legible verdict, two status surfaces still answered "why?"
    /// with nothing at all. `witness: legacyDefault` + `legacyPath:
    /// twoStateDryRun` is the honest reading here — this kind has no
    /// `spec.writeIntent` field yet, so every live write it makes IS migration
    /// debt, and now says so.
    ///
    /// Written from the SAME `EffectiveGate` value as `effectiveDryRun` below,
    /// so the two can never disagree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_gate: Option<EffectiveGateReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_dry_run: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_epoch: Option<i64>,
    /// SHADOW half of a `StandaloneEc2Instance`-lane claim decision: the name of
    /// the Ready, unclaimed node this pool WOULD taint+label into itself this
    /// tick. Set only in shadow (`effectiveDryRun == true`); mirrors
    /// `wouldProvision`'s shadow-first convention at the per-node claim grain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub would_taint: Option<String>,
    /// LIVE half: the name of the node this pool ACTUALLY tainted+labelled into
    /// itself this tick (`breathe.pleme.io/pool=<this pool>`,
    /// `breathe.pleme.io/lane=<lane>`, `NoSchedule`). Set only when the claim
    /// mutation ran (`effectiveDryRun == false` and a candidate existed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tainted_node: Option<String>,
    /// The underlying `provedor.provision()` call's error, when the actuator
    /// itself failed (a non-ACTIVE nodegroup, an IAM permission error, a
    /// throttled API call) -- distinct from `lastDecision`, which only ever
    /// describes the BAND'S decision, never whether acting on it actually
    /// succeeded. `None` means the last provision attempt (if any this tick)
    /// succeeded; this field is only ever set on a `Growing` phase. Added
    /// 2026-07-18 after a real incident: this Result used to be silently
    /// discarded, so a perpetually-failing actuator produced "would provision
    /// N" forever with no observable evidence anywhere of why capacity never
    /// actually grew.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_provision_error: Option<String>,
    /// FLAP/STUCK DETECTION (task #51). The number of consecutive ticks the
    /// pool has spent in `phase: Growing` WITHOUT `observedCapacity` actually
    /// increasing tick-over-tick — i.e. the band keeps deciding "grow" but
    /// nothing is landing. Reset to `0` the instant the phase leaves
    /// `Growing` OR capacity moves forward again, so a pool that is
    /// genuinely (if slowly) gaining capacity every tick never accumulates
    /// this counter, however many ticks it takes to reach its target.
    /// Mirrors `pangea-operator`'s `FailureEscalation.maxConsecutiveFailures`
    /// shape (N-consecutive-bad-states → escalate) applied to no-progress
    /// growth instead of failed reconciles. See [`node_forma::flap_status`]
    /// in `breathe-controller` for the pure computation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consecutive_stuck_ticks: Option<u32>,
    /// `true` once [`Self::consecutive_stuck_ticks`] crosses the flap
    /// threshold — the pool has been silently failing to grow for long
    /// enough that it needs operator attention rather than another quiet
    /// retry. Distinct from `phase` (which stays `Growing`, still an
    /// accurate description of what the band WANTS to do) — this is the
    /// "and it isn't working" signal layered on top. Never set back to
    /// `false` mid-episode by a single good tick; it clears the same tick
    /// `consecutiveStuckTicks` resets to `0` (capacity moved, or the phase
    /// left `Growing`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flap_detected: Option<bool>,
    /// Human-readable reason, set only alongside `flapDetected: true`
    /// (`None` once the episode clears — never a stale message pointing at
    /// a stuck run that already resolved).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flap_reason: Option<String>,
}

// ─────────────────── IsolationBand — membership-CLOSING node reservation ────────
//
// The node-claim family in `BreatheCloudPool` (above) OPENS membership: a
// `Grew` tick claims one Ready node INTO a pool. `IsolationBand` is the
// membership-CLOSING peer: it PROTECTS a named node (its first use: the
// Camelot origin/control-plane node) by keeping it tainted against every
// workload except an explicit allowlist. theory/CORRENTEZA.md §4/§11.3 names
// this shape as a degenerate N=1 instance of the not-yet-built generic
// `IsolationBand` — this type is exactly that, scoped to what origin-guard
// needs today (`targetNodes` carrying one hostname).
//
// `PlacementIsolationKind` below intentionally does NOT depend on
// `breathe-invariant::isolation::PlacementIsolation`, even though the two are
// semantically identical (CoLocate/AntiAffinity/TopologySpread/Dedicated).
// `breathe-invariant` deliberately declares its own `[workspace]` root (see
// its Cargo.toml header) so it composes the breathe substrate BY REFERENCE
// without coupling to breathe's in-flight band-crate churn — verified
// empirically (`cargo check -p breathe-crd` with a path dep onto
// `crates/breathe-invariant` fails with "multiple workspace roots found in
// the same workspace", the exact error `crates/breathe-spread`'s own header
// already named and had to be folded out of its nested `[workspace]` to
// avoid). Folding `breathe-invariant` into the parent workspace the same way
// would unify these two enums, but is a bigger, separately-scoped move (it
// touches an existing crate's deliberate build isolation) — out of scope for
// this type. Tracked as a follow-up, not silently worked around.
#[allow(clippy::doc_markdown)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum PlacementIsolationKind {
    /// Co-locate freely — bin-pack. Not a meaningful choice for origin-guard
    /// (a co-located posture would be no isolation at all) but kept so the
    /// enum mirrors `breathe_invariant::isolation::PlacementIsolation`'s full
    /// variant set for the future multi-node placement engine.
    CoLocate,
    AntiAffinity,
    TopologySpread,
    /// The origin-guard posture: a dedicated, tainted node the workload runs
    /// alone on (or, for origin-guard, that ONLY the allowlist may run on).
    Dedicated,
}

impl Default for PlacementIsolationKind {
    fn default() -> Self {
        Self::Dedicated
    }
}

/// The taint `IsolationBand` ensures is present on every `targetNodes` entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaintSpec {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// `NoSchedule` / `PreferNoSchedule` / `NoExecute` — the raw k8s taint
    /// effect string (validated by the apiserver against `Taint`'s own enum,
    /// not re-validated here).
    pub effect: String,
}

fn d_origin_taint_key() -> String {
    ORIGIN_TAINT_KEY.to_string()
}

fn d_no_schedule() -> String {
    "NoSchedule".to_string()
}

impl Default for TaintSpec {
    fn default() -> Self {
        Self { key: d_origin_taint_key(), value: None, effect: d_no_schedule() }
    }
}

/// A workload this band's `targetNodes` MAY run — matched against a pod's
/// namespace + (its own name, for a bare/unmanaged pod, OR an owner
/// reference's name, allowing the standard ReplicaSet `<name>-<hash>` prefix
/// so a Deployment's `WorkloadRef` matches its pods without an extra
/// apiserver hop to resolve the ReplicaSet's own owner — see
/// `origin_guard::is_authorized_pod` in breathe-controller).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadRef {
    pub namespace: String,
    pub name: String,
}

/// The taint key `IsolationBand` uses by default — the origin-guard posture.
pub const ORIGIN_TAINT_KEY: &str = "breathe.pleme.io/origin-reserved";

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "IsolationBand",
    shortname = "isob",
    category = "breathe",
    status = "IsolationBandStatus",
    printcolumn = r#"{"name":"Placement","type":"string","jsonPath":".spec.placement"}"#,
    printcolumn = r#"{"name":"TaintKey","type":"string","jsonPath":".spec.taint.key"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Tainted","type":"integer","jsonPath":".status.nodesTainted"}"#,
    printcolumn = r#"{"name":"Unauthorized","type":"integer","jsonPath":".status.unauthorizedCount"}"#,
    printcolumn = r#"{"name":"DryRun","type":"boolean","jsonPath":".spec.dryRun"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct IsolationBandSpec {
    /// The node(s) this band protects, BY HOSTNAME. Origin-guard declares
    /// exactly one hostname — this list IS "declare a node as origin".
    ///
    /// A hostname list can only name nodes that already exist and keep
    /// existing. On an autoscaled pool (Karpenter, an ASG) node names are
    /// ephemeral — a node is `ip-10-0-3-71...` today and gone tomorrow — so
    /// this field alone can protect a PET, never a POOL. `targetNodeSelector`
    /// below is the pool-scoped peer; see its doc for why that gap mattered.
    #[serde(default)]
    pub target_nodes: Vec<String>,
    /// The node(s) this band protects, BY LABEL — resolved fresh every tick.
    /// The pool-scoped peer of `targetNodes`, and the field that lets ONE
    /// band express "this whole pool is reserved for X" on infrastructure
    /// whose node identities churn.
    ///
    /// WHY THIS EXISTS (a real, measured gap — 2026-07-27): the placement
    /// pathology "workload W is running on pool P, which is not for W" had
    /// NO detector at the scope the pathology lives at. `IsolationBand`
    /// shipped and was registered, but its only node-naming surface was a
    /// static hostname list, so on Camelot's Karpenter-managed builder pools
    /// there was no way to *say* "this pool". The pathology was therefore
    /// only ever found by a human reading a cost report — the exact
    /// audit-detected-instead-of-self-detected shape the taxonomy exists to
    /// eliminate. One selector field converts this CRD from a pet-protector
    /// into the pool-scoped tenancy detector the taxonomy already assumed.
    ///
    /// Semantics: an equality-based label selector (the `nodeSelector`
    /// convention — ALL pairs must match), e.g.
    /// `{"karpenter.sh/nodepool": "camelot-builder-amd64"}`. Resolved
    /// against live Nodes each reconcile and UNIONed with `targetNodes`
    /// (see `origin_guard::merge_target_nodes`), so a band may use either
    /// surface or both.
    ///
    /// TIER-HONEST: this is still OBSERVATION + a taint, never admission
    /// enforcement — same C2 ceiling `unauthorizedPods` already names for
    /// itself. A pod carrying a wildcard toleration still lands on a
    /// selector-matched node exactly as it does on a hostname-matched one;
    /// this field widens WHAT CAN BE WATCHED, it does not change what a
    /// taint can enforce.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node_selector: Option<BTreeMap<String, String>>,
    /// The placement-isolation posture these nodes carry. `dedicated` (the
    /// default) is the only posture origin-guard actuates on today; the other
    /// arms are named for the future multi-node placement engine this CRD
    /// kind is designed to also carry (the elasticity fields below).
    #[serde(default)]
    pub placement: PlacementIsolationKind,
    /// The taint every `targetNodes` entry is kept carrying. Defaults to the
    /// origin-guard taint (`breathe.pleme.io/origin-reserved:NoSchedule`).
    #[serde(default)]
    pub taint: TaintSpec,
    /// Workloads allowed to run on `targetNodes` despite the taint (i.e. that
    /// carry a matching toleration by convention). An origin-guard band
    /// enumerates every legitimate daemon + controller explicitly — anything
    /// unnamed is unauthorized by design.
    #[serde(default)]
    pub allowed_workloads: Vec<WorkloadRef>,
    // ── elasticity fields — a future multi-node placement engine's setpoint
    // knobs. All `Option`, all `None` for origin-guard (a single reserved
    // node has no "grow/shrink" concept); present only so that engine reuses
    // THIS SAME CRD kind rather than a second one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setpoint: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grow_above: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shrink_below: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cooldown_seconds: Option<u64>,
    /// Pool-level MASTER write switch (the `BreatheCloudPool.spec.writeEnabled`
    /// convention): `false` ⇒ the whole band is in shadow regardless of
    /// `dryRun`. Safe default — a freshly-applied `IsolationBand` taints
    /// nothing until an operator opts in.
    #[serde(default)]
    pub write_enabled: bool,
    /// SHADOW: observe + report what the band WOULD taint; never actuate.
    #[serde(default)]
    pub dry_run: bool,
}

/// `IsolationBand` status — the per-tick protect-and-observe receipt.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IsolationBandStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// How many of `spec.targetNodes` currently carry the taint (live count;
    /// under `dryRun` this is the WOULD-be count).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nodes_tainted: Option<i64>,
    /// Pods observed running on a `targetNodes` entry that match no
    /// `allowed_workloads` entry — `"<namespace>/<pod-name>"` per finding.
    /// OBSERVATION, not enforcement (only-mitigated / C2 tier, same ceiling
    /// `breathe-lifecycle::OrphanTracker` names for itself): a wildcard
    /// toleration on some unrelated pod still bypasses the taint entirely;
    /// this field reports that, it does not prevent it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unauthorized_pods: Vec<String>,
    /// `unauthorized_pods.len()` mirrored as its own field so `kubectl get
    /// isolationband` (a printcolumn can't index into a list) shows a count
    /// at a glance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unauthorized_count: Option<i64>,
    /// **The typed authorization verdict for the last tick** — the same field
    /// `BandStatus` carries, on a Tier-B kind. Added 2026-07-26: this kind wrote
    /// ONLY the legacy `effectiveDryRun` bool, so after a refactor whose whole
    /// point was one legible verdict, two status surfaces still answered "why?"
    /// with nothing at all. `witness: legacyDefault` + `legacyPath:
    /// twoStateDryRun` is the honest reading here — this kind has no
    /// `spec.writeIntent` field yet, so every live write it makes IS migration
    /// debt, and now says so.
    ///
    /// Written from the SAME `EffectiveGate` value as `effectiveDryRun` below,
    /// so the two can never disagree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_gate: Option<EffectiveGateReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_dry_run: Option<bool>,
    /// How many live Nodes `spec.targetNodeSelector` matched this tick, or
    /// `None` when no selector is declared (a pure `targetNodes` band).
    ///
    /// THIS FIELD EXISTS TO MAKE A SPECIFIC FALSE-GREEN IMPOSSIBLE TO READ AS
    /// HEALTHY. A selector that matches nothing — because the label was
    /// renamed, the pool was deleted, or it was mistyped at authoring time —
    /// produces zero nodes to taint and therefore zero unauthorized pods,
    /// which under the original two-phase logic reported the SAME
    /// `Protecting` as a band genuinely guarding a clean pool. That is the
    /// recurring defect this whole session kept finding in other systems (a
    /// scanner that scans nothing and prints a pass, a join that reuses a
    /// stale ref and succeeds); it is not acceptable to ship it here.
    ///
    /// So a declared-but-empty selector reports `Degraded`, never
    /// `Protecting` — see `origin_guard::isolation_band_status`. Zero nodes
    /// matched is a claim about the WORLD (the pool is absent), not a proof
    /// about the WORKLOAD (nothing unauthorized is running), and the two must
    /// never be collapsed into one green word.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector_resolved: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_epoch: Option<i64>,
}

// ─────────────────── BreatheOverview — the fleet dashboard as a k8s object ──────

/// A FLEET-OVERVIEW object (cluster-scoped). The controller keeps its status
/// current by listing EVERY band, so ONE `kubectl get breatheoverview` (bov) shows
/// the whole fleet's homeostasis at a glance — the dashboard as a single k8s object,
/// no Grafana. Create one (e.g. `metadata.name: rio`); the controller fills the rest.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "BreatheOverview",
    shortname = "bov",
    category = "breathe",
    status = "OverviewStatus",
    printcolumn = r#"{"name":"Bands","type":"integer","jsonPath":".status.total"}"#,
    printcolumn = r#"{"name":"Converged","type":"integer","jsonPath":".status.converged"}"#,
    printcolumn = r#"{"name":"Carving","type":"integer","jsonPath":".status.carving"}"#,
    printcolumn = r#"{"name":"Deferred","type":"integer","jsonPath":".status.deferred"}"#,
    printcolumn = r#"{"name":"Shadow","type":"integer","jsonPath":".status.shadow"}"#,
    printcolumn = r#"{"name":"Updated","type":"string","jsonPath":".status.lastUpdated"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BreatheOverviewSpec {
    /// How often (seconds) the controller re-aggregates the fleet (default 30).
    #[serde(default = "d_overview_refresh")]
    pub refresh_seconds: u64,
}
fn d_overview_refresh() -> u64 {
    30
}

/// One band's line in the fleet overview.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BandSummary {
    pub kind: String,
    pub namespace: String,
    pub name: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub util: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_limit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dry_run: bool,
    /// Has this band EVER observed a live pod? Projection of
    /// `BandStatus::first_observed_epoch.is_some()` (see that field's doc).
    ///
    /// Surfaced on the row, not just in the totals, so `kubectl get bov -o yaml`
    /// names WHICH bands are unproven. A count alone tells an operator that
    /// something governs nothing without telling them what — which is the same
    /// unactionable-signal problem one level up.
    ///
    /// Defaults FALSE and, unlike the sibling booleans here, is serialized even
    /// when false: an absent field would be indistinguishable from a band that
    /// pre-dates this field, and silently reading old-and-unknown as
    /// proven-and-fine is the exact false-green being closed.
    #[serde(default)]
    pub ever_governed: bool,
}

/// The aggregated fleet status — totals + the per-band roll-up.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OverviewStatus {
    #[serde(default)]
    pub total: i64,
    #[serde(default)]
    pub converged: i64,
    #[serde(default)]
    pub carving: i64,
    #[serde(default)]
    pub deferred: i64,
    #[serde(default)]
    pub suspended: i64,
    #[serde(default)]
    pub shadow: i64,
    /// Bands that are `Dormant` and have NEVER observed a pod — i.e. they may be
    /// governing nothing at all, and nobody would know.
    ///
    /// SPLIT OUT OF `converged` DELIBERATELY. `Dormant` was previously folded
    /// into the converged total on the reasoning that an empty pod group is a
    /// benign resting state, which is true for a scale-to-zero workload and
    /// false for a stale selector — and the two were indistinguishable. A fleet
    /// where a third of the bands pointed at renamed labels still reported 100%
    /// converged, because the metric counted declarations rather than
    /// governance.
    ///
    /// A band that has ever seen a pod stays in `converged` when it later rests;
    /// only the never-proven ones land here. So this number is normally 0, and a
    /// non-zero value means precisely "this many bands have never governed
    /// anything in their lifetime" — actionable rather than merely alarming.
    #[serde(default)]
    pub unproven: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_updated: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bands: Vec<BandSummary>,
}

// ─────────────────── BreatheConfig — the env knobs as a k8s object ──────────────

/// Cluster-scoped fleet CONFIG — lifts breathe's last env-only knobs onto the k8s
/// API. Both binaries read it at startup (merging over the env defaults), so an
/// operator tunes the fleet via `kubectl edit breatheconfig <name>` instead of
/// editing a Deployment env + redeploying (dynamic hot-reload is a noted refinement;
/// a config change currently applies on the next controller restart). Create one
/// (e.g. `metadata.name: default`).
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[kube(group = "breathe.pleme.io", version = "v1", kind = "BreatheConfig", shortname = "bcfg", category = "breathe")]
#[serde(rename_all = "camelCase")]
pub struct BreatheConfigSpec {
    /// PromQL endpoint the storage dimension reads `used` from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prometheus_url: Option<String>,
    /// Base requeue interval (seconds) when no per-class cadence applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_requeue_seconds: Option<u64>,
    /// Per-restart-class cooldown windows (seconds) — the real-time cadence knob.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class_cooldowns: Option<CooldownsSpec>,
}

/// Per-restart-class cooldown windows (seconds): golden ≤ conditional ≤ requiring.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CooldownsSpec {
    pub restart_free: u64,
    pub restart_conditional: u64,
    pub restart_requiring: u64,
}

/// **Densa** — the per-namespace capacity + cost ENVELOPE (the breathability
/// thesis L2 / P7; docs/PROVISIONING.md §2.5). The hard wall every breathe band
/// in the namespace carves WITHIN (L1 ⊂ L2): a band may grow its workload's
/// limit, but the sum of the namespace's floors + reserve must always fit
/// `poolCapacity` (the cluster-scale never-swap proof, BREATHABILITY-MATH §4.3),
/// and the namespace's cost must stay inside `costSlaCents`. The typed-value peer
/// is `breathe_catalog::forma::Densa` (Forma-keyed, auction-side); this CRD is the
/// k8s wire border (string-keyed, namespace-scoped). One per ephemeral-env
/// namespace = the Dev-Loop cost-guard. `kubectl get densa`.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "Densa",
    namespaced,
    shortname = "densa",
    category = "breathe",
    status = "DensaStatus",
    printcolumn = r#"{"name":"Fits","type":"boolean","jsonPath":".status.fits"}"#,
    printcolumn = r#"{"name":"SumFloors","type":"integer","jsonPath":".status.sumFloors"}"#,
    printcolumn = r#"{"name":"Capacity","type":"integer","jsonPath":".spec.poolCapacity"}"#,
    printcolumn = r#"{"name":"CostRemaining","type":"integer","jsonPath":".status.costRemainingCents"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct DensaSpec {
    /// The per-resource bounds (one per dimension/forma the namespace caps).
    pub bounds: Vec<DensaBound>,
    /// Units (in the pool's unit) that must stay free — reserve headroom.
    #[serde(default)]
    pub reserve: u64,
    /// The pool's hard capacity (the never-swap denominator), same unit as bounds.
    pub pool_capacity: u64,
    /// The cost ceiling (cents per accounting period) the namespace must stay within.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_sla_cents: Option<u64>,
}

/// One resource bound in a [`Densa`] envelope. `name` is the resource key — a
/// dimension (`memory`/`cpu`) or a forma (`node-on-demand`).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DensaBound {
    pub name: String,
    /// The provisioned-from-peak floor (must always fit — the never-swap base).
    pub floor: u64,
    /// The L2 hard ceiling — bands carve ≤ it.
    pub ceiling: u64,
}

/// Densa status — the fits-check result + the live cost headroom (the Dev-Loop
/// cost-guard surface the controller keeps current).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DensaStatus {
    /// Did the never-swap fits-check pass?
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fits: Option<bool>,
    /// Σ floors (the fits arithmetic surface).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sum_floors: Option<i64>,
    /// A human-legible refusal reason when `fits=false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Cost remaining under the SLA (cents); negative ⇒ over budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_remaining_cents: Option<i64>,
    /// The EnvId this envelope bounds (the ephemeral-env binding, Dev Loop).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_env_id: Option<String>,
}

/// The typed refusal of a [`DensaSpec`] — the never-swap fits-check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DensaError {
    FloorAboveCeiling { name: String, floor: u64, ceiling: u64 },
    DoesNotFit { sum_floors: u64, reserve: u64, capacity: u64 },
}

impl std::fmt::Display for DensaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FloorAboveCeiling { name, floor, ceiling } => {
                write!(f, "{name}: floor {floor} > ceiling {ceiling}")
            }
            Self::DoesNotFit { sum_floors, reserve, capacity } => {
                write!(f, "Σfloors {sum_floors} + reserve {reserve} > capacity {capacity} (never-swap breach)")
            }
        }
    }
}

impl DensaSpec {
    /// The never-swap fits-check (BREATHABILITY-MATH §4.3 / V4): every floor ≤ its
    /// ceiling AND Σ floors + reserve ≤ `poolCapacity`. A `Densa` that fails is
    /// REFUSED (`status.fits=false` + reason), never applied — the cluster-scale
    /// floor-from-peak proof. Same invariant as
    /// `breathe_catalog::forma::Densa::fits` (string-keyed here, for the namespace
    /// wire surface).
    pub fn fits(&self) -> Result<(), DensaError> {
        for b in &self.bounds {
            if b.floor > b.ceiling {
                return Err(DensaError::FloorAboveCeiling { name: b.name.clone(), floor: b.floor, ceiling: b.ceiling });
            }
        }
        let sum: u64 = self.bounds.iter().map(|b| b.floor).sum();
        if sum.saturating_add(self.reserve) <= self.pool_capacity {
            Ok(())
        } else {
            Err(DensaError::DoesNotFit { sum_floors: sum, reserve: self.reserve, capacity: self.pool_capacity })
        }
    }

    /// The L2 ceiling for a resource key (the `BandConfig.ceiling` bands carve within).
    #[must_use]
    pub fn ceiling(&self, name: &str) -> Option<u64> {
        self.bounds.iter().find(|b| b.name == name).map(|b| b.ceiling)
    }

    /// The status this spec should carry — the fits-check folded into the wire
    /// surface (a controller patches this; pure so it's unit-testable).
    #[must_use]
    pub fn status_now(&self, cost_spent_cents: Option<u64>) -> DensaStatus {
        let sum_floors = self.bounds.iter().map(|b| b.floor).sum::<u64>() as i64;
        let (fits, reason) = match self.fits() {
            Ok(()) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };
        let cost_remaining_cents = match (self.cost_sla_cents, cost_spent_cents) {
            (Some(sla), Some(spent)) => Some(sla as i64 - spent as i64),
            _ => None,
        };
        DensaStatus { fits: Some(fits), sum_floors: Some(sum_floors), reason, cost_remaining_cents, observed_env_id: None }
    }
}

// ─────────────────── QuinhaoPool — the hierarchical-vector fair-share allocator ──
//
// The k8s wire border for `breathe_auction::quinhao` (BREATHABILITY-FABRIC §III.0
// — "every part held at the same 80/20 band by the same law, so they all shift
// together"). Where a `StorageBand` holds the POOL at its 80% band, a
// `QuinhaoPool` DIVIDES that band among a forest of weighted claimants (groups →
// users) per dimension, and publishes the computed per-claimant grants in its
// status — the grant ledger gaveta reads. Additive + advisory: it carves NOTHING
// (status only); the pool's own StorageBand still holds the 80%.
//
// gaveta's drive product: `claims[].kind = Group` = shared-folders/families;
// `kind = User` (with `parentId` = its group) = members. `weight: 1` everywhere ⇒
// a strictly even split (4 users → ~20% of the 80% band each). The allocation is
// a PURE function of the claim list, so a member joining/leaving/going idle is a
// re-derivation — the "resident flexibility that shifts accordingly".

/// A claimant's role in the fabric tree — purely descriptive (groups parent
/// users), surfaced for `kubectl` legibility; the allocator keys off `parentId`.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum ClaimantKind {
    /// A top-level claimant that splits the pool band (a gaveta shared-folder).
    #[default]
    Group,
    /// A child claimant that splits its parent's grant (a gaveta member).
    User,
}

/// One claimant's bounded, weighted demand on a single fabric dimension — the
/// wire mirror of `breathe_auction::quinhao::Demand`. Quantities are strings in
/// the dimension's unit (bytes for storage) so an operator writes `10Gi`, not a
/// raw byte count; `breathe_auction::Unit` parses them in the controller.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DimDemand {
    /// The fabric dimension this demand is on (`storage` / `cpu` / `bandwidth` /
    /// `request-rate`). Storage is the live axis; the others are typed-but-dormant.
    pub dim: String,
    /// Relative share weight. `1` (the default) ⇒ an even share; `0` ⇒ idle on
    /// this axis (claims only its floor). A larger weight buys a larger share.
    #[serde(default = "d_weight")]
    pub weight: u32,
    /// The floor always granted (a reserved quota), a quantity string. Default `0`.
    #[serde(default = "d_zero_qty")]
    pub min: String,
    /// The ceiling never exceeded, a quantity string. Omit ⇒ no cap (the whole
    /// pool). Empty string is treated as "no cap".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<String>,
    /// What the claimant would actually use (a generous share is trimmed to this).
    /// Omit ⇒ would use the whole pool (the even default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub demand: Option<String>,
}

/// One claimant in the fabric forest — the wire mirror of
/// `breathe_auction::quinhao::Quinhao`. A group is `kind: Group` with no
/// `parentId`; a user is `kind: User` naming its group as `parentId`.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClaimSpec {
    /// Stable identity (a gaveta group-id or member-id), unique within the pool.
    pub id: String,
    /// `Group` (top-level) or `User` (child).
    #[serde(default)]
    pub kind: ClaimantKind,
    /// The parent claimant's id (a user's group). Omit for a top-level claimant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// The per-dimension demand vector — one entry per participating axis. A
    /// dimension absent from this list is `absent` (granted 0 on that axis). The
    /// common case is a single `storage` entry with `weight: 1` (the even member).
    #[serde(default)]
    pub demands: Vec<DimDemand>,
}

/// A pool-capacity entry — the total quantity of ONE dimension the band holds.
/// The allocatable band per dimension is `capacity * setpoint`.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PoolCapacityEntry {
    /// The fabric dimension (`storage` / `cpu` / `bandwidth` / `request-rate`).
    pub dim: String,
    /// The total capacity quantity for this dimension, a string in the dim's unit
    /// (`3.6Ti` for storage). The band the claimants split is this × `setpoint`.
    pub capacity: String,
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "QuinhaoPool",
    namespaced,
    status = "QuinhaoPoolStatus",
    shortname = "qpool",
    category = "breathe",
    printcolumn = r#"{"name":"Setpoint","type":"string","jsonPath":".spec.setpoint"}"#,
    printcolumn = r#"{"name":"Claims","type":"integer","jsonPath":".status.claimCount"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"DryRun","type":"boolean","jsonPath":".spec.dryRun"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct QuinhaoPoolSpec {
    /// The pool's total capacity per dimension. The allocatable band each
    /// dimension's claimants split is `capacity × setpoint`. A dimension absent
    /// here has a zero band (every claim on it is granted 0).
    pub pool_capacity: Vec<PoolCapacityEntry>,
    /// OPTIONAL: pull the storage capacity from a referenced `StorageBand`'s
    /// `status.observedCapacity` instead of (or in addition to) an explicit
    /// `poolCapacity` storage entry — so the divider tracks the band that holds
    /// the pool. When set AND the band reports a capacity, it OVERRIDES the
    /// explicit storage entry. (The explicit entry is the shippable default; this
    /// is the destination coupling.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_band_ref: Option<StorageBandRef>,
    /// The utilization setpoint — the fraction of capacity the claimants divide
    /// (the 80% band). Default `0.80`. Clamped to `(0, 1]` in the allocator.
    #[serde(default = "d_setpoint")]
    pub setpoint: f64,
    /// The claimant forest (groups + users). Even by default (`weight: 1`).
    #[serde(default)]
    pub claims: Vec<ClaimSpec>,
    /// SHADOW: the controller computes + publishes grants but the consumer
    /// (gaveta) should treat them as advisory. Default true (advisory-first). The
    /// pool NEVER carves a k8s/host limit regardless — it divides; the StorageBand
    /// holds the 80%. `dryRun` here marks the GRANT LEDGER advisory vs enforced.
    #[serde(default = "d_true")]
    pub dry_run: bool,
}

/// A reference to a `StorageBand` whose `status.observedCapacity` sources this
/// pool's storage capacity.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageBandRef {
    /// The `StorageBand` name.
    pub name: String,
    /// The `StorageBand` namespace. Omit ⇒ the pool's own namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// One claimant's computed grant — what gaveta reads. `grants[dim]` is the
/// quota in that dimension's unit (bytes for storage).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClaimGrant {
    /// The claimant id (a gaveta group-id or member-id).
    pub id: String,
    /// `Group` / `User` (echoed from the spec for ledger legibility).
    pub kind: ClaimantKind,
    /// The parent id, if a child (echoed for the consumer's tree walk).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// The granted quota per dimension, keyed by dim string — raw quantities in
    /// the dim's unit (bytes for storage). gaveta reads `grants["storage"]` as the
    /// member's storage quota in bytes.
    pub grants: BTreeMap<String, i64>,
}

/// `QuinhaoPool` status — the computed grant ledger + the band summary.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct QuinhaoPoolStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// How many claimants carry a grant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_count: Option<i64>,
    /// The allocatable band per dimension (`capacity × setpoint`), keyed by dim —
    /// what the claimants divided. Lets an operator see the band from the status.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub band: BTreeMap<String, i64>,
    /// The effective pool capacity per dimension the controller resolved (after a
    /// `storageBandRef` override, if any), keyed by dim.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub observed_capacity: BTreeMap<String, i64>,
    /// The grant ledger — one entry per claimant. THE surface gaveta consumes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<ClaimGrant>,
    /// A typed refusal when the claim forest is malformed (duplicate id / unknown
    /// parent / cycle) — the allocation is refused, never half-published.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Whether the published ledger is advisory (`dryRun`) — echoed for the consumer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_dry_run: Option<bool>,
    /// `metadata.generation` the controller last reconciled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_epoch: Option<i64>,
}

/// Why a [`QuinhaoPoolSpec`] cannot be turned into a typed allocation — the
/// parse-time refusal (a malformed quantity / unknown dimension). Forest-shape
/// errors come from the allocator itself ([`breathe_auction::quinhao::FabricError`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuinhaoPoolError {
    /// A quantity string failed to parse in its dimension's unit.
    BadQuantity { field: String, value: String },
    /// A `dim` string names no known fabric dimension.
    UnknownDim { dim: String },
}

impl std::fmt::Display for QuinhaoPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadQuantity { field, value } => write!(f, "{field}: invalid quantity {value:?}"),
            Self::UnknownDim { dim } => write!(f, "unknown fabric dimension {dim:?}"),
        }
    }
}

impl std::error::Error for QuinhaoPoolError {}

impl QuinhaoPoolSpec {
    /// The unit a fabric dimension's quantities parse in. Storage is bytes; the
    /// dormant compute/rate axes are bare counts (millicores / bytes-per-sec /
    /// req-per-sec are all integer-valued at this border).
    fn unit_for(dim: breathe_auction::quinhao::Dim) -> Unit {
        match dim {
            breathe_auction::quinhao::Dim::Storage => Unit::Bytes,
            _ => Unit::Count,
        }
    }

    /// Parse one [`DimDemand`] into a typed `(Dim, Demand)`. Quantities parse in
    /// the dimension's unit; an omitted `max`/`demand` ⇒ `u64::MAX` (no cap / would
    /// use everything), matching `Demand::even`.
    fn parse_demand(
        d: &DimDemand,
    ) -> Result<(breathe_auction::quinhao::Dim, breathe_auction::quinhao::Demand), QuinhaoPoolError> {
        let dim = breathe_auction::quinhao::Dim::from_str(&d.dim)
            .ok_or_else(|| QuinhaoPoolError::UnknownDim { dim: d.dim.clone() })?;
        let unit = Self::unit_for(dim);
        let parse_q = |field: &str, q: &str| -> Result<u64, QuinhaoPoolError> {
            unit.parse(q).ok_or_else(|| QuinhaoPoolError::BadQuantity { field: field.into(), value: q.into() })
        };
        let parse_opt = |field: &str, q: &Option<String>| -> Result<u64, QuinhaoPoolError> {
            match q.as_deref().filter(|s| !s.is_empty()) {
                Some(s) => parse_q(field, s),
                None => Ok(u64::MAX),
            }
        };
        let min = parse_q("min", &d.min)?;
        let max = parse_opt("max", &d.max)?;
        let demand = parse_opt("demand", &d.demand)?;
        Ok((dim, breathe_auction::quinhao::Demand { weight: d.weight, min, max, demand }))
    }

    /// Build the typed claimant forest from the spec — the allocator input. A
    /// claim with no `demands` is treated as an even storage member (`storage_only(even)`)
    /// so the simplest CR (`{id, kind, parentId}`) is the even default.
    ///
    /// # Errors
    /// A [`QuinhaoPoolError`] for the first malformed quantity / unknown dimension.
    pub fn to_claimants(&self) -> Result<Vec<breathe_auction::quinhao::Quinhao>, QuinhaoPoolError> {
        use breathe_auction::quinhao::{Demand, DemandVector, Quinhao};
        let mut out = Vec::with_capacity(self.claims.len());
        for c in &self.claims {
            let demand = if c.demands.is_empty() {
                DemandVector::storage_only(Demand::even())
            } else {
                // Start every axis absent; fill the ones the claim declares.
                let mut storage = Demand::absent();
                let mut cpu = Demand::absent();
                let mut bandwidth = Demand::absent();
                let mut request_rate = Demand::absent();
                for dd in &c.demands {
                    let (dim, dem) = Self::parse_demand(dd)?;
                    match dim {
                        breathe_auction::quinhao::Dim::Storage => storage = dem,
                        breathe_auction::quinhao::Dim::Cpu => cpu = dem,
                        breathe_auction::quinhao::Dim::Bandwidth => bandwidth = dem,
                        breathe_auction::quinhao::Dim::RequestRate => request_rate = dem,
                    }
                }
                DemandVector::new(storage, cpu, bandwidth, request_rate)
            };
            out.push(Quinhao { id: c.id.clone(), parent: c.parent_id.clone(), demand });
        }
        Ok(out)
    }

    /// Build the typed pool capacity from the spec's `poolCapacity` entries.
    /// `storage_band_observed` (when `Some`) OVERRIDES the explicit storage entry
    /// — the destination coupling where the divider tracks the holding band.
    ///
    /// # Errors
    /// A [`QuinhaoPoolError`] for the first malformed quantity / unknown dimension.
    pub fn to_capacity(
        &self,
        storage_band_observed: Option<u64>,
    ) -> Result<breathe_auction::quinhao::PoolCapacity, QuinhaoPoolError> {
        let mut storage = 0u64;
        let mut cpu = 0u64;
        let mut bandwidth = 0u64;
        let mut request_rate = 0u64;
        for e in &self.pool_capacity {
            let dim = breathe_auction::quinhao::Dim::from_str(&e.dim)
                .ok_or_else(|| QuinhaoPoolError::UnknownDim { dim: e.dim.clone() })?;
            let unit = Self::unit_for(dim);
            let v = unit
                .parse(&e.capacity)
                .ok_or_else(|| QuinhaoPoolError::BadQuantity { field: "capacity".into(), value: e.capacity.clone() })?;
            match dim {
                breathe_auction::quinhao::Dim::Storage => storage = v,
                breathe_auction::quinhao::Dim::Cpu => cpu = v,
                breathe_auction::quinhao::Dim::Bandwidth => bandwidth = v,
                breathe_auction::quinhao::Dim::RequestRate => request_rate = v,
            }
        }
        if let Some(observed) = storage_band_observed {
            storage = observed; // the band's observedCapacity wins (destination coupling)
        }
        Ok(breathe_auction::quinhao::PoolCapacity::new(storage, cpu, bandwidth, request_rate))
    }

    /// The full pure allocation: build the forest + capacity, run the allocator,
    /// fold into the typed [`QuinhaoPoolStatus`] grant ledger. PURE + unit-tested
    /// — so the CR status, gaveta's read, and the logs can never disagree. The
    /// controller calls this and patches the result.
    ///
    /// A malformed spec (bad quantity / unknown dim) or malformed forest
    /// (cycle/dup/unknown-parent) folds into `phase: Refused` + `status.reason` so
    /// the receipt is observable, never a silently-wrong allocation.
    #[must_use]
    pub fn allocate(&self, storage_band_observed: Option<u64>) -> QuinhaoPoolStatus {
        use breathe_auction::quinhao::{allocate_fabric, Dim};
        let dry_run = self.dry_run;
        let refused = |reason: String| QuinhaoPoolStatus {
            phase: Some("Refused".into()),
            reason: Some(reason),
            effective_dry_run: Some(dry_run),
            ..Default::default()
        };
        let claimants = match self.to_claimants() {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        let capacity = match self.to_capacity(storage_band_observed) {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        let grants = match allocate_fabric(capacity, self.setpoint, &claimants) {
            Ok(g) => g,
            Err(e) => return refused(e.to_string()),
        };

        // Per-dim band + observed-capacity summary (for the status surface).
        let setpoint = if self.setpoint > 0.0 && self.setpoint <= 1.0 { self.setpoint } else { 1.0 };
        let mut band = BTreeMap::new();
        let mut observed_capacity = BTreeMap::new();
        for dim in Dim::ALL {
            let cap = capacity.get(dim);
            if cap > 0 {
                observed_capacity.insert(dim.as_str().to_string(), cap as i64);
                #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
                let b = (cap as f64 * setpoint) as u64;
                band.insert(dim.as_str().to_string(), b as i64);
            }
        }

        // The grant ledger — one entry per spec claim (preserves spec order +
        // echoes kind/parent for the consumer's tree walk).
        let ledger: Vec<ClaimGrant> = self
            .claims
            .iter()
            .map(|c| {
                let gv = grants.get(&c.id);
                let mut per_dim = BTreeMap::new();
                for dim in Dim::ALL {
                    let v = gv.get(dim);
                    // Only surface a dimension the pool actually has a band for —
                    // keeps the ledger tight (storage-only pools show only storage).
                    if band.contains_key(dim.as_str()) {
                        per_dim.insert(dim.as_str().to_string(), v as i64);
                    }
                }
                ClaimGrant { id: c.id.clone(), kind: c.kind, parent_id: c.parent_id.clone(), grants: per_dim }
            })
            .collect();

        QuinhaoPoolStatus {
            phase: Some("Allocated".into()),
            claim_count: Some(ledger.len() as i64),
            band,
            observed_capacity,
            grants: ledger,
            reason: None,
            effective_dry_run: Some(dry_run),
            observed_generation: None,
            last_seen_epoch: None,
        }
    }
}

fn d_weight() -> u32 { 1 }
fn d_zero_qty() -> String { "0".into() }
fn d_true() -> bool { true }

fn d_floor_bytes() -> String { "256Mi".into() }
fn d_ceiling_bytes() -> String { "16Gi".into() }
fn d_floor_milli() -> String { "250m".into() }
fn d_ceiling_milli() -> String { "2".into() }
// StorageBand PROVISION-MINIMAL defaults. Storage carves grow-only
// (provision-minimal + grow-on-demand): a fresh volume is born at this small
// floor and expands online toward the setpoint as real data lands, so an
// over-provisioned volume (a fixed `50Gi` holding a few hundred MiB) is a state
// breathe's own carve never constructs (breathe_control::classify_provision). The
// floor is a fresh-PVC minimum, NOT memory's 256Mi (a PVC below ~1Gi is rarely
// useful and CSI minimums bite); the ceiling is a generous grow headroom a data
// tier reaches only with real data.
fn d_storage_floor_bytes() -> String { "2Gi".into() }
fn d_storage_ceiling_bytes() -> String { "200Gi".into() }
// The RESERVATION demand statistic. A stable high-water over a duty cycle, with
// a small multiplicative headroom — NOT the limit law's `1/setpoint` divisor.
fn d_demand_quantile() -> f64 { 0.95 }
fn d_demand_window() -> String { "7d".into() }
fn d_demand_headroom() -> f64 { 0.15 }
fn d_setpoint() -> f64 { 0.80 }
fn d_grow_above() -> f64 { 0.85 }
fn d_shrink_below() -> f64 { 0.70 }
fn d_grow_factor() -> f64 { 1.25 }
fn d_shrink_factor() -> f64 { 0.90 }
fn d_cooldown() -> u64 { 600 }
fn d_max_staleness() -> u64 { 120 }
fn d_predictive_lookahead() -> u64 { 60 }
fn d_peak_decay() -> f64 { 0.98 } // trailing-window peak holds a spike for ~tens of ticks
fn d_warmup_seconds() -> u64 { 600 } // hold a shrink for 10 min after a (re)start (boot-spike window)
fn d_relief_latency() -> u64 { 180 } // ~3min node boot→Ready (the NodeOnDemand dead-time)
fn d_replica_floor() -> u32 { 2 } // HA floor: a single replica tolerates no disruption
fn d_replica_ceiling() -> u32 { 10 }
fn d_replica_target() -> f64 { 0.80 }
fn d_replica_tol_up() -> f64 { 0.10 } // small up → react fast to spikes
fn d_replica_tol_down() -> f64 { 0.20 } // large down → resist churn (asymmetric)
fn d_replica_up_pct() -> u32 { 100 }
fn d_replica_up_pods() -> u32 { 4 }
fn d_replica_down_pct() -> u32 { 10 }
fn d_replica_down_pods() -> u32 { 1 }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn densa_fits_check_and_status() {
        // a valid envelope fits; status reflects it + cost headroom
        let d = DensaSpec {
            bounds: vec![
                DensaBound { name: "memory".into(), floor: 2_000, ceiling: 8_000 },
                DensaBound { name: "node-on-demand".into(), floor: 1, ceiling: 5 },
            ],
            reserve: 500,
            pool_capacity: 10_000,
            cost_sla_cents: Some(5_000),
        };
        assert!(d.fits().is_ok());
        assert_eq!(d.ceiling("memory"), Some(8_000));
        let st = d.status_now(Some(3_000));
        assert_eq!(st.fits, Some(true));
        assert_eq!(st.sum_floors, Some(2_001));
        assert_eq!(st.cost_remaining_cents, Some(2_000)); // 5000 sla − 3000 spent

        // floor above ceiling → refused
        let bad = DensaSpec {
            bounds: vec![DensaBound { name: "memory".into(), floor: 9_000, ceiling: 8_000 }],
            reserve: 0,
            pool_capacity: 100_000,
            cost_sla_cents: None,
        };
        assert!(matches!(bad.fits(), Err(DensaError::FloorAboveCeiling { .. })));
        assert_eq!(bad.status_now(None).fits, Some(false));
        assert!(bad.status_now(None).reason.is_some());

        // over-subscribed floors → never-swap breach
        let over = DensaSpec {
            bounds: vec![DensaBound { name: "memory".into(), floor: 9_000, ceiling: 9_500 }],
            reserve: 2_000,
            pool_capacity: 10_000,
            cost_sla_cents: None,
        };
        assert!(matches!(over.fits(), Err(DensaError::DoesNotFit { sum_floors: 9_000, reserve: 2_000, capacity: 10_000 })));
    }

    #[test]
    fn densa_crd_generates() {
        let crd = <Densa as kube::CustomResourceExt>::crd();
        assert_eq!(crd.spec.names.kind, "Densa");
        assert_eq!(crd.spec.scope, "Namespaced");
    }

    #[test]
    fn three_kinds_share_band_config_parse() {
        // each kind constructs a valid BandConfig from its spec
        let tr = TargetRef { kind: "Cluster".into(), name: "x".into(), api_version: None, container: None, pod_selector: None };
        let mem = MemoryBand::new("m", MemoryBandSpec {
            target_ref: tr.clone(), posture_ref: None, setpoint: Some(0.80), grow_above: Some(0.85), shrink_below: Some(0.70),
            grow_factor: Some(1.25), shrink_factor: Some(0.90), floor: "512Mi".into(), ceiling: "4Gi".into(),
            cooldown_seconds: Some(600), max_staleness_seconds: Some(120), dry_run: true, disruption_policy: Default::default(), disruption_policy_rationale: None, suspend: false, force_limit: None, force_limit_expiry: None, predictive: false, predictive_lookahead_seconds: 60, request_floor: String::new(), peak_decay: 0.98, mode: None, write_intent: None, bound_introduction: None, confirm_after_seconds: 1800, warmup_seconds: 600,
        });
        let cfg = Band::band_config(&mem).unwrap();
        assert_eq!(cfg.floor_bytes, 512 * (1 << 20));
        assert!(mem.dry_run());
    }

    #[test]
    fn cpu_band_parses_floor_ceiling_as_millicores() {
        let tr = TargetRef { kind: "Cluster".into(), name: "db".into(), api_version: None, container: None, pod_selector: None };
        let cpu = CpuBand::new("c", CpuBandSpec {
            target_ref: tr, posture_ref: None, setpoint: Some(0.80), grow_above: Some(0.85), shrink_below: Some(0.70),
            grow_factor: Some(1.25), shrink_factor: Some(0.90), floor: "250m".into(), ceiling: "2".into(),
            cooldown_seconds: Some(600), max_staleness_seconds: Some(120), dry_run: false, disruption_policy: Default::default(), disruption_policy_rationale: None, suspend: false, force_limit: None, force_limit_expiry: None, predictive: false, predictive_lookahead_seconds: 60, request_floor: String::new(), peak_decay: 0.98, mode: None, write_intent: None, bound_introduction: None, confirm_after_seconds: 1800, warmup_seconds: 600,
        });
        let cfg = Band::band_config(&cpu).unwrap();
        // millicores, NOT bytes: "250m" → 250, "2" cores → 2000m.
        assert_eq!(cfg.floor_bytes, 250);
        assert_eq!(cfg.ceiling_bytes, 2000);
    }

    #[test]
    fn cpu_band_default_floor_ceiling_parse_as_millicores() {
        // an omitted floor/ceiling on a CpuBand must default to cpu-valid values
        // (250m / 2), not the byte default 256Mi which can't parse as millicores.
        let cfg = crate::band_config_of(0.80, 0.85, 0.70, 1.25, 0.90,
            &d_floor_milli(), &d_ceiling_milli(), "", 0, Unit::Millicores).unwrap();
        assert_eq!(cfg.floor_bytes, 250);
        assert_eq!(cfg.ceiling_bytes, 2000);
        assert_eq!(cfg.request_floor_bytes, 0, "empty request_floor ⇒ no floor");
    }

    #[test]
    fn storage_band_defaults_to_the_provision_minimal_floor() {
        // PROVISION-MINIMAL: a StorageBand authored with ONLY a targetRef (every
        // other field defaulted) is born at the small 2Gi floor with a generous
        // 200Gi grow ceiling — NOT memory's 256Mi and NOT a fixed large size. A
        // fresh PVC therefore starts minimal and grows on demand; a 50Gi-declared
        // volume is an EXTERNAL over-declaration, never breathe's default.
        let spec: StorageBandSpec = serde_json::from_value(serde_json::json!({
            "targetRef": { "kind": "PersistentVolumeClaim", "name": "data-x" }
        }))
        .expect("a minimal StorageBandSpec must deserialize on defaults");
        assert_eq!(spec.floor, "2Gi", "the provision-minimal floor default");
        assert_eq!(spec.ceiling, "200Gi", "the grow-on-demand ceiling default");
        let band = StorageBand::new("data-x", spec);
        let cfg = Band::band_config(&band).unwrap();
        assert_eq!(cfg.floor_bytes, 2 * (1 << 30), "2Gi provision floor in bytes");
        assert_eq!(cfg.ceiling_bytes, 200 * (1 << 30), "200Gi grow ceiling in bytes");
        // The carve target for a nearly-empty volume is the floor — the grow-on-
        // demand contract: breathe would provision ~2Gi, never a fixed 50Gi.
        assert_eq!(
            breathe_control::provision_target(890 << 20, 890 << 20, &cfg),
            2 * (1 << 30),
            "an 890MiB volume carves to the 2Gi provision floor",
        );
    }

    #[test]
    fn host_bands_share_the_band_shape_and_parse_bytes() {
        // ArcBand: the target is the node; floor/ceiling are byte quantities.
        let tr = TargetRef { kind: "Node".into(), name: "rio".into(), api_version: None, container: None, pod_selector: None };
        let arc = ArcBand::new("rio-arc", ArcBandSpec {
            target_ref: tr, posture_ref: None, setpoint: Some(0.80), grow_above: Some(0.85), shrink_below: Some(0.70),
            grow_factor: Some(1.25), shrink_factor: Some(0.90), floor: "1Gi".into(), ceiling: "6Gi".into(),
            cooldown_seconds: Some(600), max_staleness_seconds: Some(120), dry_run: true, disruption_policy: Default::default(), disruption_policy_rationale: None, suspend: false, force_limit: None, force_limit_expiry: None, predictive: false, predictive_lookahead_seconds: 60, request_floor: String::new(), peak_decay: 0.98, mode: None, write_intent: None, bound_introduction: None, confirm_after_seconds: 1800, warmup_seconds: 600,
        });
        let cfg = Band::band_config(&arc).unwrap();
        assert_eq!(cfg.floor_bytes, 1 << 30);
        assert_eq!(cfg.ceiling_bytes, 6 * (1 << 30));
        assert!(arc.dry_run());

        // CgroupBand: the target NAME is the systemd unit the agent addresses.
        let g = CgroupBand::new("nix-daemon", CgroupBandSpec {
            target_ref: TargetRef { kind: "HostUnit".into(), name: "nix-daemon.service".into(), api_version: None, container: None, pod_selector: None },
            posture_ref: None, setpoint: Some(0.80), grow_above: Some(0.85), shrink_below: Some(0.70), grow_factor: Some(1.25), shrink_factor: Some(0.90),
            floor: "1Gi".into(), ceiling: "12Gi".into(), cooldown_seconds: Some(600), max_staleness_seconds: Some(120), dry_run: true, disruption_policy: Default::default(), disruption_policy_rationale: None, suspend: false, force_limit: None, force_limit_expiry: None, predictive: false, predictive_lookahead_seconds: 60, request_floor: String::new(), peak_decay: 0.98, mode: None, write_intent: None, bound_introduction: None, confirm_after_seconds: 1800, warmup_seconds: 600,
        });
        assert_eq!(g.target_ref().name, "nix-daemon.service");
    }

    #[test]
    fn disruption_policy_defaults_golden_and_parses_per_band() {
        // omitted → RestartFreeOnly (golden-by-default).
        let def: MemoryBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "MemoryBand",
            "metadata": { "name": "m" },
            "spec": { "targetRef": { "kind": "Deployment", "name": "app" } }
        })).unwrap();
        assert_eq!(def.disruption_policy(), DisruptionPolicy::RestartFreeOnly);
        // a CNPG band declares allowRestart (its only resize path is a roll).
        let allow: MemoryBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "MemoryBand",
            "metadata": { "name": "db" },
            "spec": { "targetRef": { "kind": "Cluster", "name": "pangea-database" }, "disruptionPolicy": "allowRestart" }
        })).unwrap();
        assert_eq!(allow.disruption_policy(), DisruptionPolicy::AllowRestart);
    }

    #[test]
    fn nodepool_carries_the_l2_ceilings_and_master_switch() {
        let mut cgroup = BTreeMap::new();
        cgroup.insert("nix-daemon.service".to_string(), GiB(12));
        let mut cgroup_cpu = BTreeMap::new();
        cgroup_cpu.insert("nix-daemon.service".to_string(), 8000u64);
        let pool = BreatheNodePool::new("rio", BreatheNodePoolSpec {
            node_name: "rio".into(),
            arc_max_gi_b: GiB(6),
            cgroup_max_gi_b: cgroup,
            cgroup_cpu_max_milli: cgroup_cpu,
            write_enabled: false, // safe default — whole node in shadow
        });
        assert_eq!(pool.spec.node_name, "rio");
        assert_eq!(pool.spec.arc_max_gi_b, GiB(6));
        assert_eq!(pool.spec.cgroup_max_gi_b.get("nix-daemon.service"), Some(&GiB(12)));
        assert_eq!(pool.spec.cgroup_cpu_max_milli.get("nix-daemon.service"), Some(&8000));
        assert!(!pool.spec.write_enabled, "writeEnabled must default off (shadow-first)");
    }

    #[test]
    fn nodepool_gib_fields_carry_an_openapi_maximum() {
        // the parse-time bound: the apiserver rejects an arcMaxGiB whose *2^30
        // would overflow, so an overflowing ceiling is unrepresentable at admission.
        let crd = <BreatheNodePool as kube::CustomResourceExt>::crd();
        let yaml = serde_yaml::to_string(&crd).unwrap();
        assert!(yaml.contains("maximum"), "BreatheNodePool GiB fields must emit an OpenAPI maximum");
    }

    #[test]
    fn nodepool_is_cluster_scoped() {
        use kube::Resource;
        // a cluster-scoped CRD has no namespace in its dynamic type scope; assert
        // via the generated CRD's scope field.
        let crd = <BreatheNodePool as kube::CustomResourceExt>::crd();
        assert_eq!(crd.spec.scope, "Cluster", "BreatheNodePool must be cluster-scoped");
        let _ = BreatheNodePool::kind(&());
    }

    #[test]
    fn quinhao_pool_crd_generates_namespaced() {
        let crd = <QuinhaoPool as kube::CustomResourceExt>::crd();
        assert_eq!(crd.spec.names.kind, "QuinhaoPool");
        assert_eq!(crd.spec.scope, "Namespaced");
        assert_eq!(crd.spec.names.short_names.as_ref().unwrap(), &["qpool"]);
    }

    #[test]
    fn isolation_band_crd_generates_cluster_scoped() {
        use kube::Resource;
        let crd = <IsolationBand as kube::CustomResourceExt>::crd();
        assert_eq!(crd.spec.names.kind, "IsolationBand");
        assert_eq!(crd.spec.scope, "Cluster", "IsolationBand targets Nodes — cluster-scoped like BreatheCloudPool/BreatheNodePool");
        assert_eq!(crd.spec.names.short_names.as_ref().unwrap(), &["isob"]);
        let _ = IsolationBand::kind(&());
    }

    #[test]
    fn isolation_band_taint_defaults_to_the_origin_reserved_taint() {
        // A minimal spec (just targetNodes) parses to the origin-guard default —
        // an operator does not have to spell out the taint/placement for the
        // common case.
        let band: IsolationBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "IsolationBand",
            "metadata": { "name": "origin" },
            "spec": { "targetNodes": ["camelot-origin"] }
        }))
        .expect("deserializes with defaults");
        assert_eq!(band.spec.target_nodes, vec!["camelot-origin".to_string()]);
        assert_eq!(band.spec.placement, PlacementIsolationKind::Dedicated);
        assert_eq!(band.spec.taint.key, ORIGIN_TAINT_KEY);
        assert_eq!(band.spec.taint.effect, "NoSchedule");
        assert_eq!(band.spec.taint.value, None);
        assert!(band.spec.allowed_workloads.is_empty());
        assert!(!band.spec.write_enabled, "writeEnabled must default off (shadow-first)");
        assert!(!band.spec.dry_run);
        assert_eq!(band.spec.setpoint, None, "elasticity fields are None for a plain origin-guard band");
    }

    #[test]
    fn isolation_band_status_round_trips_and_hides_empty_unauthorized() {
        let empty = IsolationBandStatus::default();
        let js = serde_json::to_value(&empty).unwrap();
        assert!(js.get("unauthorizedPods").is_none(), "an empty unauthorized_pods list must not serialize");

        let found = IsolationBandStatus {
            phase: Some("Protecting".into()),
            nodes_tainted: Some(1),
            unauthorized_pods: vec!["default/stray-pod".into()],
            unauthorized_count: Some(1),
            // A LIVE Tier-B write: `legacyDefault` witness on the `twoStateDryRun`
            // path, since this kind carries no `spec.writeIntent` yet.
            effective_gate: Some(breathe_provider::legacy_two_state_gate(false, false).report()),
            effective_dry_run: Some(false),
            selector_resolved: Some(2),
            last_seen_epoch: Some(1_000),
        };
        let js = serde_json::to_string(&found).unwrap();
        let back: IsolationBandStatus = serde_json::from_str(&js).unwrap();
        assert_eq!(found, back);

        // `selectorResolved` must survive the wire as a DISTINCT value from
        // absent: `Some(0)` is what the `Degraded` phase keys on, so a
        // round-trip that quietly folded it into `None` would silently
        // restore the very false-green this field was added to prevent.
        let zero = IsolationBandStatus { selector_resolved: Some(0), ..Default::default() };
        let js = serde_json::to_string(&zero).unwrap();
        assert!(js.contains("selectorResolved"), "Some(0) must serialize — it is a real finding, not an absence");
        let back: IsolationBandStatus = serde_json::from_str(&js).unwrap();
        assert_eq!(back.selector_resolved, Some(0));
        assert_eq!(
            IsolationBandStatus::default().selector_resolved,
            None,
            "and a band with no selector stays None, never 0"
        );
    }

    #[test]
    fn quinhao_pool_allocates_an_even_storage_split() {
        // 4 even members (no groups), pool 1000 bytes, setpoint 0.80 → band 800 →
        // 200 each. The operator's literal ask, through the CRD fold.
        let spec = QuinhaoPoolSpec {
            pool_capacity: vec![PoolCapacityEntry { dim: "storage".into(), capacity: "1000".into() }],
            storage_band_ref: None,
            setpoint: 0.80,
            claims: (0..4)
                .map(|i| ClaimSpec { id: format!("m{i}"), kind: ClaimantKind::User, parent_id: None, demands: vec![] })
                .collect(),
            dry_run: true,
        };
        let st = spec.allocate(None);
        assert_eq!(st.phase.as_deref(), Some("Allocated"));
        assert_eq!(st.claim_count, Some(4));
        assert_eq!(st.band.get("storage"), Some(&800));
        assert_eq!(st.observed_capacity.get("storage"), Some(&1000));
        for g in &st.grants {
            assert_eq!(g.grants.get("storage"), Some(&200), "{} should get 200", g.id);
        }
        assert_eq!(st.effective_dry_run, Some(true));
    }

    #[test]
    fn quinhao_pool_allocates_a_group_user_hierarchy() {
        // 2 groups split 800 → 400 each; group A's 2 users → 200 each; group B's
        // 1 user → 400. The hierarchy through the CRD.
        let spec = QuinhaoPoolSpec {
            pool_capacity: vec![PoolCapacityEntry { dim: "storage".into(), capacity: "1000".into() }],
            storage_band_ref: None,
            setpoint: 0.80,
            claims: vec![
                ClaimSpec { id: "groupA".into(), kind: ClaimantKind::Group, parent_id: None, demands: vec![] },
                ClaimSpec { id: "groupB".into(), kind: ClaimantKind::Group, parent_id: None, demands: vec![] },
                ClaimSpec { id: "a1".into(), kind: ClaimantKind::User, parent_id: Some("groupA".into()), demands: vec![] },
                ClaimSpec { id: "a2".into(), kind: ClaimantKind::User, parent_id: Some("groupA".into()), demands: vec![] },
                ClaimSpec { id: "b1".into(), kind: ClaimantKind::User, parent_id: Some("groupB".into()), demands: vec![] },
            ],
            dry_run: true,
        };
        let st = spec.allocate(None);
        let grant = |id: &str| st.grants.iter().find(|g| g.id == id).unwrap().grants.get("storage").copied().unwrap();
        assert_eq!(grant("groupA"), 400);
        assert_eq!(grant("groupB"), 400);
        assert_eq!(grant("a1"), 200);
        assert_eq!(grant("a2"), 200);
        assert_eq!(grant("b1"), 400);
    }

    #[test]
    fn quinhao_pool_storage_band_observed_capacity_overrides_the_explicit_entry() {
        // a storageBandRef-sourced 2000-byte capacity overrides the explicit 1000.
        let spec = QuinhaoPoolSpec {
            pool_capacity: vec![PoolCapacityEntry { dim: "storage".into(), capacity: "1000".into() }],
            storage_band_ref: Some(StorageBandRef { name: "garage-data".into(), namespace: Some("drive".into()) }),
            setpoint: 0.80,
            claims: vec![ClaimSpec { id: "m0".into(), kind: ClaimantKind::User, parent_id: None, demands: vec![] }],
            dry_run: true,
        };
        let st = spec.allocate(Some(2000));
        assert_eq!(st.observed_capacity.get("storage"), Some(&2000), "the band's capacity wins");
        assert_eq!(st.band.get("storage"), Some(&1600)); // 2000 * 0.80
        assert_eq!(st.grants[0].grants.get("storage"), Some(&1600));
    }

    #[test]
    fn quinhao_pool_refuses_a_malformed_forest() {
        // a user naming an unknown parent → Refused, no grants published.
        let spec = QuinhaoPoolSpec {
            pool_capacity: vec![PoolCapacityEntry { dim: "storage".into(), capacity: "1000".into() }],
            storage_band_ref: None,
            setpoint: 0.80,
            claims: vec![ClaimSpec { id: "u".into(), kind: ClaimantKind::User, parent_id: Some("ghost".into()), demands: vec![] }],
            dry_run: true,
        };
        let st = spec.allocate(None);
        assert_eq!(st.phase.as_deref(), Some("Refused"));
        assert!(st.reason.as_deref().unwrap().contains("ghost"));
        assert!(st.grants.is_empty());
    }

    #[test]
    fn quinhao_pool_parses_quantity_strings_and_bad_quantity_refuses() {
        // a Gi quantity parses to bytes; a garbage quantity is refused.
        let ok = QuinhaoPoolSpec {
            pool_capacity: vec![PoolCapacityEntry { dim: "storage".into(), capacity: "2Gi".into() }],
            storage_band_ref: None,
            setpoint: 0.80,
            claims: vec![ClaimSpec { id: "m0".into(), kind: ClaimantKind::User, parent_id: None, demands: vec![] }],
            dry_run: true,
        };
        let st = ok.allocate(None);
        assert_eq!(st.observed_capacity.get("storage"), Some(&(2 * (1 << 30))));

        let bad = QuinhaoPoolSpec {
            pool_capacity: vec![PoolCapacityEntry { dim: "storage".into(), capacity: "not-a-quantity".into() }],
            storage_band_ref: None,
            setpoint: 0.80,
            claims: vec![],
            dry_run: true,
        };
        assert_eq!(bad.allocate(None).phase.as_deref(), Some("Refused"));
    }

    #[test]
    fn quinhao_pool_minimal_cr_deserializes_with_even_defaults() {
        // the simplest CR an operator writes: claims carry only id+kind+parentId,
        // weight/min default to even, dryRun defaults true.
        let pool: QuinhaoPool = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "QuinhaoPool",
            "metadata": { "name": "drive", "namespace": "drive" },
            "spec": {
                "poolCapacity": [{ "dim": "storage", "capacity": "3.6Ti" }],
                "claims": [
                    { "id": "fam-smith", "kind": "group" },
                    { "id": "alice", "kind": "user", "parentId": "fam-smith" }
                ]
            }
        })).expect("a minimal QuinhaoPool CR must deserialize");
        assert!(pool.spec.dry_run, "dryRun defaults true (advisory-first)");
        assert_eq!(pool.spec.setpoint, 0.80);
        assert_eq!(pool.spec.claims[1].parent_id.as_deref(), Some("fam-smith"));
        // and it allocates without error.
        let st = pool.spec.allocate(None);
        assert_eq!(st.phase.as_deref(), Some("Allocated"));
    }

    #[test]
    fn kube_layout_inner_fields_are_camelcase_on_the_wire() {
        // Regression lock for the idiom leak the deploy-verify pass caught: the
        // enum-level rename_all does NOT cascade to struct-variant fields, so each
        // KubeLayoutSpec variant carries its own. crField's inner fields MUST
        // serialize camelCase (apiVersion/fieldPath/restartFree) — matching the
        // generated CRD + the rest of the breathe API — or a hand-authored CR is
        // pruned and rejected by the apiserver on the required snake_case names.
        let layout = KubeLayoutSpec::CrField {
            api_version: "postgresql.cnpg.io/v1".into(),
            kind: "Cluster".into(),
            name: "pangea-database".into(),
            field_path: "/spec/postgresql/parameters/max_connections".into(),
            restart_free: false,
        };
        let j = serde_json::to_value(&layout).unwrap();
        let cr = &j["crField"];
        assert!(cr.get("apiVersion").is_some(), "crField must serialize apiVersion (camelCase)");
        assert!(cr.get("fieldPath").is_some(), "crField must serialize fieldPath (camelCase)");
        assert!(cr.get("restartFree").is_some(), "crField must serialize restartFree (camelCase)");
        assert!(
            cr.get("api_version").is_none() && cr.get("field_path").is_none(),
            "crField must NOT emit snake_case keys (the idiom leak)"
        );

        // a camelCase CR spec round-trips into the typed band.
        let band: KubeParamBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "KubeParamBand",
            "metadata": { "name": "k", "namespace": "pangea-system" },
            "spec": {
                "targetRef": { "kind": "Cluster", "name": "pangea-database", "apiVersion": "postgresql.cnpg.io/v1" },
                "layout": { "crField": {
                    "apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "name": "pangea-database",
                    "fieldPath": "/spec/postgresql/parameters/max_connections", "restartFree": false
                } },
                "metric": { "prometheus": "max(cnpg_backends_total)" },
                "dryRun": true
            }
        })).expect("a camelCase crField CR must deserialize");
        match &band.spec.layout {
            KubeLayoutSpec::CrField { field_path, .. } => {
                assert_eq!(field_path, "/spec/postgresql/parameters/max_connections");
            }
            other => panic!("expected CrField, got {other:?}"),
        }

        // the generated CRD schema advertises the camelCase property — what the
        // apiserver validates a hand-authored CR against.
        let crd = <KubeParamBand as kube::CustomResourceExt>::crd();
        let yaml = serde_yaml::to_string(&crd).unwrap();
        assert!(yaml.contains("fieldPath"), "the KubeParamBand CRD must advertise fieldPath (camelCase)");
        assert!(!yaml.contains("field_path"), "the CRD must not carry the snake_case field_path");
    }

    #[test]
    fn pod_memory_high_dispatch_round_trips_and_maps_to_the_soft_knob() {
        // a camelCase PodMemoryHigh dispatch CR round-trips into the typed spec and
        // maps to the SOFT HostKnob::PodCgroupMemoryHigh (never memory.max).
        let pmh: PodMemoryHigh = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "PodMemoryHigh",
            "metadata": { "name": "authentik-worker-xyz-worker" },
            "spec": {
                "nodeName": "rio",
                "qosClass": "Burstable",
                "podUid": "abc12345-6789-def0-1234-56789abcdef0",
                "containerRuntimeId": "containerd://deadbeefcafe",
                "cgroupDriver": "systemd",
                "desiredBytes": 469_762_048u64, // 448Mi reclaim seat
                "ownerBand": "authentik/authentik-worker-memory"
            }
        })).expect("a camelCase PodMemoryHigh CR must deserialize");
        assert_eq!(pmh.spec.node_name, "rio");
        assert_eq!(pmh.spec.desired_bytes, 469_762_048);
        match pmh.spec.provider_knob() {
            breathe_provider::HostKnob::PodCgroupMemoryHigh { driver, qos, pod_uid, container_runtime_id } => {
                assert_eq!(driver, breathe_provider::CgroupDriver::Systemd);
                assert_eq!(qos, "Burstable");
                assert_eq!(pod_uid, "abc12345-6789-def0-1234-56789abcdef0");
                assert_eq!(container_runtime_id, "containerd://deadbeefcafe");
            }
            other => panic!("the dispatch must map to the SOFT pod memory.high knob, got {other:?}"),
        }
        // the generated CRD advertises the camelCase desiredBytes the apiserver validates.
        let crd = <PodMemoryHigh as kube::CustomResourceExt>::crd();
        let yaml = serde_yaml::to_string(&crd).unwrap();
        assert!(yaml.contains("desiredBytes"), "the PodMemoryHigh CRD must advertise desiredBytes (camelCase)");
        assert!(yaml.contains("containerRuntimeId"));
        // cgroupDriver defaults to systemd when omitted.
        let dflt: PodMemoryHigh = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "PodMemoryHigh",
            "metadata": { "name": "p" },
            "spec": { "nodeName": "rio", "qosClass": "Guaranteed", "podUid": "u", "containerRuntimeId": "containerd://c", "desiredBytes": 1024u64 }
        })).unwrap();
        assert_eq!(dflt.spec.cgroup_driver, CgroupDriverSpec::Systemd);
    }

    // ── Promotion lifecycle (ShadowConfirmEffect) gate ────────────────────────

    fn mk_band(
        spec_extra: serde_json::Value,
        status: Option<serde_json::Value>,
        annotations: Option<serde_json::Value>,
    ) -> MemoryBand {
        let mut spec = serde_json::json!({
            "targetRef": { "kind": "Deployment", "name": "d", "apiVersion": "apps/v1" }
        });
        spec.as_object_mut()
            .unwrap()
            .extend(spec_extra.as_object().unwrap().clone());
        let mut meta = serde_json::json!({ "name": "x", "namespace": "y" });
        if let Some(a) = annotations {
            meta.as_object_mut().unwrap().insert("annotations".into(), a);
        }
        let mut obj = serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "MemoryBand",
            "metadata": meta, "spec": spec
        });
        if let Some(s) = status {
            obj.as_object_mut().unwrap().insert("status".into(), s);
        }
        serde_json::from_value(obj).unwrap()
    }

    /// `Ready=True` since `ts`, with `extra` conditions appended.
    fn ready_status(ts: &str, extra: serde_json::Value) -> serde_json::Value {
        let mut conds = vec![serde_json::json!(
            { "type": "Ready", "status": "True", "reason": "R", "message": "m", "lastTransitionTime": ts }
        )];
        conds.extend(extra.as_array().unwrap().clone());
        serde_json::json!({ "conditions": conds })
    }

    const EPOCH_1000: &str = "1970-01-01T00:16:40Z"; // 1000s after the epoch

    // ── S1: the AUTHORIZATION AXIS (writeIntent > mode > default) ────────────
    //
    // `spec.dryRun` is absent from that chain, deliberately. These rows pin the
    // fact `76924b0` changed silently: an authored `dryRun: true` does NOT hold
    // a k8s band, and the verdict now SAYS so instead of reporting "dryRun".

    /// **THE ROOT-DEFECT ROW.** The exact shape of ~70 live camelot-eks bands:
    /// `dryRun: true`, no `mode`, no `writeIntent`, confirm window long past.
    /// It is LIVE — and the typed verdict names the legacy path that promoted
    /// it, so the state is attributable instead of merely surprising.
    #[test]
    fn dry_run_true_with_no_mode_is_live_and_says_which_legacy_path_promoted_it() {
        let b = mk_band(
            serde_json::json!({ "dryRun": true }),
            Some(ready_status(EPOCH_1000, serde_json::json!([]))),
            None,
        );
        let g = b.resolve_gate(1000 + 100_000, false);
        assert!(g.is_live(), "dryRun has not gated a k8s band since 76924b0 — this is the truth, stated");
        let w = g.witness().expect("live");
        assert!(w.is_legacy_default(), "no writeIntent was authored ⇒ migration debt");
        assert_eq!(w.legacy_path(), Some(LegacyPath::ConfirmGate { required_secs: 1800 }));
        // The bool projection is unchanged — S1 is additive, not behavioural.
        assert!(!b.effective_dry_run(1000 + 100_000));
    }

    /// An authored `writeIntent` beats the retired `mode`, in both directions.
    #[test]
    fn write_intent_outranks_mode() {
        let ready = || Some(ready_status(EPOCH_1000, serde_json::json!([])));
        // mode says carve; intent says observe ⇒ shadow.
        let held = mk_band(
            serde_json::json!({ "mode": "effect", "writeIntent": { "intent": "observe" } }),
            ready(),
            None,
        );
        let g = held.resolve_gate(1000 + 100_000, false);
        assert!(g.is_shadow() && g.shadow_reason() == Some(gate::ShadowReason::ModeShadow));
        assert!(g.shadow_reason().unwrap().is_authored(), "an authored hold, not an accident");

        // mode says shadow; intent names an author ⇒ live, attributed.
        let live = mk_band(
            serde_json::json!({ "mode": "shadow", "writeIntent": { "intent": "write", "authorizedBy": "drzzln 2026-07-26" } }),
            ready(),
            None,
        );
        let g = live.resolve_gate(0, false);
        assert_eq!(g.witness().and_then(gate::LiveWitness::authorized_by), Some("drzzln 2026-07-26"));
    }

    /// An unattributed go-live never carves — it shadows with a reason that
    /// names the malformation. (Enforced in Rust, not by the apiserver: the
    /// cross-field rule is not expressible in a structural schema. Deliberately
    /// NOT a deserialize failure, which would break the watch for every band.)
    #[test]
    fn an_unattributed_write_intent_fails_safe() {
        for bad in [serde_json::json!({ "intent": "write" }), serde_json::json!({ "intent": "write", "authorizedBy": "  " })] {
            let b = mk_band(
                serde_json::json!({ "mode": "effect", "writeIntent": bad }),
                Some(ready_status(EPOCH_1000, serde_json::json!([]))),
                None,
            );
            let g = b.resolve_gate(1000 + 100_000, false);
            assert!(g.is_shadow(), "an anonymous go-live must never be granted");
            assert_eq!(g.shadow_reason(), Some(gate::ShadowReason::IntentMalformed));
        }
    }

    /// A calibrating intent runs the SAME outorga confirm gate the legacy
    /// default does — shadow while the window holds, live once it passes, and
    /// the reason carries the numbers rather than a bare "dryRun".
    #[test]
    fn calibrate_then_write_reports_its_window_then_promotes() {
        let b = mk_band(
            serde_json::json!({ "writeIntent": { "intent": "calibrateThenWrite", "confirmAfterSeconds": 1800 } }),
            Some(ready_status(EPOCH_1000, serde_json::json!([]))),
            None,
        );
        assert_eq!(
            b.resolve_gate(1000 + 400, false).shadow_reason(),
            Some(gate::ShadowReason::ConfirmPending { held_secs: 400, need_secs: 1800 })
        );
        let g = b.resolve_gate(1000 + 1801, false);
        assert_eq!(g.witness().map(gate::LiveWitness::kind), Some(gate::WitnessKind::ConfirmGatePassed));
    }

    /// `writeIntent: frozen` holds without stopping observation — the single
    /// word that now covers the retired `mode: suspended`. (`spec.suspend`
    /// remains the distinct "stop reconciling entirely" switch, applied by the
    /// controller before a band ever reaches this gate.)
    #[test]
    fn frozen_intent_holds_and_subsumes_mode_suspended() {
        let by_intent = mk_band(serde_json::json!({ "writeIntent": { "intent": "frozen" } }), None, None);
        let by_mode = mk_band(serde_json::json!({ "mode": "suspended" }), None, None);
        assert_eq!(by_intent.resolve_gate(0, false).shadow_reason(), Some(gate::ShadowReason::Suspended));
        assert_eq!(by_mode.resolve_gate(0, false).shadow_reason(), Some(gate::ShadowReason::Suspended));
    }

    /// The two-key rule survives the new axis: an external freeze outranks even
    /// an explicitly-authored write.
    #[test]
    fn an_external_freeze_still_outranks_an_authored_write() {
        let b = mk_band(
            serde_json::json!({ "writeIntent": { "intent": "write", "authorizedBy": "drzzln" } }),
            None,
            None,
        );
        assert!(b.resolve_gate(0, false).is_live());
        assert_eq!(b.resolve_gate(0, true).shadow_reason(), Some(gate::ShadowReason::Frozen));
    }

    /// ADDITIVITY GUARD — the property that makes S1 safe to deploy. For every
    /// band shape that exists TODAY (i.e. no `writeIntent` authored), the new
    /// typed verdict's bool projection must equal the old
    /// `outorga::PromotionPolicy::decide` answer, exactly.
    #[test]
    fn unauthored_bands_are_byte_identical_to_the_previous_resolution() {
        let shapes = [
            serde_json::json!({}),
            serde_json::json!({ "dryRun": true }),
            serde_json::json!({ "dryRun": false }),
            serde_json::json!({ "mode": "shadow" }),
            serde_json::json!({ "mode": "effect" }),
            serde_json::json!({ "mode": "suspended" }),
            serde_json::json!({ "mode": "shadowConfirmEffect", "dryRun": true }),
        ];
        let statuses = [
            None,
            Some(ready_status(EPOCH_1000, serde_json::json!([]))),
            Some(ready_status(EPOCH_1000, serde_json::json!([{ "type": "Stale", "status": "True", "reason": "R", "message": "m", "lastTransitionTime": EPOCH_1000 }]))),
            Some(ready_status(EPOCH_1000, serde_json::json!([{ "type": "Conflict", "status": "True", "reason": "R", "message": "m", "lastTransitionTime": EPOCH_1000 }]))),
        ];
        for spec in &shapes {
            for st in &statuses {
                for frozen in [false, true] {
                    for now in [0_i64, 1000 + 100, 1000 + 100_000] {
                        let b = mk_band(spec.clone(), st.clone(), None);
                        // The PREVIOUS implementation, verbatim.
                        let old = outorga::PromotionPolicy::new(b.promotion_mode().to_outorga())
                            .confirm_after(b.confirm_after_seconds())
                            .effective_dry_run(&BandObservation(&b), now, frozen);
                        assert_eq!(
                            b.effective_dry_run_frozen(now, frozen),
                            old,
                            "spec={spec} frozen={frozen} now={now}: the typed gate must not change any existing band's behaviour"
                        );
                    }
                }
            }
        }
    }

    /// **The claim every operator surface now makes, proved against real CRs.**
    ///
    /// `DimensionId::dry_run_is_honored()` tells the MCP/REST/GraphQL/gRPC
    /// surfaces whether flipping `spec.dryRun` on a given kind does anything, so
    /// they can refuse the no-op instead of returning success. That flag lives in
    /// `breathe-provider` (no CRD types in scope), which makes it exactly the
    /// kind of free-standing assertion that rots. This builds one CR of every
    /// one of the ten kinds in the root defect's own shape — `dryRun: true`, no
    /// `mode`, no `writeIntent`, confirm window long past — and asserts the
    /// resolved gate agrees with the flag.
    ///
    /// The two `*ParamBand` kinds keep a two-state `dryRun ? Shadow : Effect`
    /// `promotion_mode()`, so they shadow. The other eight fall through to the
    /// compiled `ShadowConfirmEffect`, whose confirm gate has long since passed,
    /// so they are LIVE with `dryRun: true` authored — the inversion this whole
    /// refactor exists to make legible.
    ///
    /// Tier: **CI forcing-function**, not a type. A kind could change its
    /// `promotion_mode()` and the flag would be wrong until this test ran.
    #[test]
    fn dry_run_is_honored_matches_every_band_kind() {
        use breathe_provider::DimensionId;
        // `dryRun: true`, nothing else authored — the ~70-live-band shape.
        let dr = serde_json::json!({ "dryRun": true });
        let st = ready_status(EPOCH_1000, serde_json::json!([]));
        let target = serde_json::json!({ "kind": "Deployment", "name": "d", "apiVersion": "apps/v1" });

        /// Build a CR of `$t` from the shared meta + the kind's own required
        /// fields, then hand back `(dimension_id, is_shadow)`.
        macro_rules! probe {
            ($t:ty, $extra:expr) => {{
                let mut spec = serde_json::json!({ "targetRef": target });
                spec.as_object_mut().unwrap().extend($extra.as_object().unwrap().clone());
                spec.as_object_mut().unwrap().extend(dr.as_object().unwrap().clone());
                let b: $t = serde_json::from_value(serde_json::json!({
                    "apiVersion": "breathe.pleme.io/v1", "kind": stringify!($t),
                    "metadata": { "name": "x", "namespace": "n" },
                    "spec": spec, "status": st,
                }))
                .unwrap_or_else(|e| panic!("{} fixture must parse: {e}", stringify!($t)));
                (b.dimension_id(), b.resolve_gate(1000 + 100_000, false).is_shadow())
            }};
        }

        let none = serde_json::json!({});
        let probes = [
            probe!(MemoryBand, none),
            probe!(CpuBand, none),
            probe!(StorageBand, none),
            probe!(ArcBand, none),
            probe!(CgroupBand, none),
            probe!(CgroupCpuBand, none),
            probe!(
                HostParamBand,
                serde_json::json!({
                    "knob": { "sysctl": { "key": "vm.dirty_bytes" } },
                    "metric": { "meminfoField": { "field": "Dirty" } },
                })
            ),
            probe!(
                KubeParamBand,
                serde_json::json!({
                    "layout": { "crField": {
                        "apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "name": "db",
                        "fieldPath": "/spec/postgresql/parameters/max_connections", "restartFree": false
                    } },
                    "metric": { "prometheus": "max(cnpg_backends_total)" },
                })
            ),
            probe!(
                AppBand,
                serde_json::json!({
                    "layout": { "apiCall": { "endpoint": "redis://redis:6379", "command": "maxmemory" } },
                    "metric": { "prometheus": "redis_memory_used_bytes" },
                })
            ),
            probe!(ReplicaBand, serde_json::json!({ "metric": { "prometheus": "rate(http_requests_total[1m])" } })),
            // The RESERVATION band. Proven here to be one of the EIGHT kinds for
            // which `dryRun` is inert — a `dryRun: true` RequestBand with nothing
            // else authored still resolves LIVE once its confirm window elapses.
            // That is deliberately inherited, not overridden: authorization is
            // `writeIntent`, and a band that can decide whether a workload
            // survives must not be holdable by a field that decides nothing
            // everywhere else.
            probe!(RequestBand, serde_json::json!({ "resource": "memory" })),
        ];

        // Every dimension is probed exactly once — no kind quietly skipped.
        let seen: Vec<_> = probes.iter().map(|(d, _)| *d).collect();
        for d in DimensionId::ALL {
            assert_eq!(seen.iter().filter(|x| **x == d).count(), 1, "{d} must be probed exactly once");
        }

        for (dim, is_shadow) in probes {
            assert_eq!(
                is_shadow,
                dim.dry_run_is_honored(),
                "{dim}: dryRun:true resolved to shadow={is_shadow}, but dry_run_is_honored()={} — \
                 an operator surface would tell the truth about the wrong kind",
                dim.dry_run_is_honored()
            );
        }
    }

    /// Every band kind reaches the new axis — including `AppBand`, for which
    /// shadow was previously UNREPRESENTABLE (it carries no `mode` field at all,
    /// and `dryRun` is inert), so the app plane could not be held at any price.
    #[test]
    fn every_band_kind_can_now_be_held_in_shadow() {
        let intent = serde_json::json!({ "intent": "observe" });
        let app: AppBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "AppBand",
            "metadata": { "name": "a", "namespace": "n" },
            "spec": {
                "targetRef": { "kind": "Deployment", "name": "redis", "apiVersion": "apps/v1" },
                "layout": { "apiCall": { "endpoint": "redis://redis:6379", "command": "maxmemory" } },
                "metric": { "prometheus": "redis_memory_used_bytes" },
                "writeIntent": intent,
            }
        })).expect("AppBand parses");
        assert!(app.resolve_gate(i64::MAX / 2, false).is_shadow(), "the app plane can finally be held");

        let replica: ReplicaBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "ReplicaBand",
            "metadata": { "name": "r", "namespace": "n" },
            "spec": {
                "targetRef": { "kind": "Deployment", "name": "w", "apiVersion": "apps/v1" },
                "metric": { "prometheus": "rate(http_requests_total[1m])" },
                "writeIntent": intent,
            }
        })).expect("ReplicaBand parses");
        assert!(replica.resolve_gate(i64::MAX / 2, false).is_shadow());
    }

    #[test]
    fn shadow_mode_never_carves() {
        let b = mk_band(serde_json::json!({ "mode": "shadow" }), Some(ready_status(EPOCH_1000, serde_json::json!([]))), None);
        assert_eq!(b.promotion_mode(), PromotionMode::Shadow);
        assert!(b.effective_dry_run(i64::MAX / 2), "shadow must never carve, even when the window is long past");
    }

    #[test]
    fn effect_mode_always_carves() {
        let b = mk_band(serde_json::json!({ "mode": "effect" }), None, None);
        assert!(!b.effective_dry_run(0), "effect mode carves immediately");
    }

    /// REGRESSION (the never-goes-live trap): a band set with the legacy
    /// `dryRun:true` boolean and NO explicit `mode` must NOT resolve to permanent
    /// `Shadow`. Before the fix it did, so such a band carved never and had no
    /// exit — it was parked live-forever-never by a bare boolean. The invariant:
    /// permanent shadow is reachable ONLY by the explicit `mode: shadow`; the
    /// legacy boolean now means the bounded `ShadowConfirmEffect` (calibrate,
    /// then auto-promote). A band that "never goes live" without explicit operator
    /// intent is now unrepresentable.
    #[test]
    fn legacy_dry_run_true_calibrates_then_promotes_not_permanent_shadow() {
        // No explicit mode + dryRun:true ⇒ the bounded FSM, NOT permanent Shadow.
        let b = mk_band(serde_json::json!({ "dryRun": true }), None, None);
        assert_eq!(
            b.promotion_mode(),
            PromotionMode::ShadowConfirmEffect,
            "legacy dryRun:true must map to the bounded lifecycle, never permanent Shadow"
        );

        // With a clean-observation window it auto-promotes off shadow — the exit a
        // permanent-Shadow band never had. Ready since epoch 1000, confirm_after 1800.
        let promoted = mk_band(
            serde_json::json!({ "dryRun": true }),
            Some(ready_status(EPOCH_1000, serde_json::json!([]))),
            None,
        );
        assert!(
            promoted.effective_dry_run(1000 + 100),
            "still shadowed while the calibration window is open"
        );
        assert!(
            !promoted.effective_dry_run(1000 + 1801),
            "REGRESSION: legacy dryRun:true band auto-promotes to live after the clean window — never parked forever"
        );
    }

    /// The deliberate hold still works: explicit `mode: shadow` IS permanent (the
    /// one eyes-open way to never carve — critical-path holds rely on it).
    #[test]
    fn explicit_mode_shadow_is_still_permanent() {
        let b = mk_band(
            serde_json::json!({ "mode": "shadow", "dryRun": true }),
            Some(ready_status(EPOCH_1000, serde_json::json!([]))),
            None,
        );
        assert_eq!(b.promotion_mode(), PromotionMode::Shadow);
        assert!(
            b.effective_dry_run(i64::MAX / 2),
            "explicit mode:shadow never carves regardless of dryRun or window"
        );
    }

    /// FORCING-FUNCTION (the invariant, not one example): across the WHOLE
    /// (mode_spec × dryRun) input space, a band that "never carves even with a
    /// long-clean window" is reachable ONLY through an explicit `mode` of
    /// `Shadow`/`Suspended` — NEVER through the legacy `dryRun` boolean alone.
    /// This is the mechanical statement of "it never goes live should require
    /// explicit operator intent": enumerate every authoring combination and prove
    /// no boolean-only path lands in a never-exit state. A future edit that
    /// re-introduces a `dryRun ⇒ permanent-shadow` arm fails HERE, not in prod.
    #[test]
    fn never_carve_requires_explicit_mode_across_the_whole_input_space() {
        let modes = [
            (None, "unset"),
            (Some("shadow"), "shadow"),
            (Some("effect"), "effect"),
            (Some("shadowConfirmEffect"), "shadowConfirmEffect"),
            (Some("suspended"), "suspended"),
        ];
        let long_past = i64::MAX / 2; // a clean-observation window that has surely elapsed
        for (mode, mode_label) in modes {
            for dry_run in [false, true] {
                let mut spec = serde_json::Map::new();
                if let Some(m) = mode {
                    spec.insert("mode".into(), serde_json::json!(m));
                }
                spec.insert("dryRun".into(), serde_json::json!(dry_run));
                // Give every band a long-clean Ready window so the ONLY thing that
                // can keep it shadowed is a deliberate permanent mode.
                let b = mk_band(
                    serde_json::Value::Object(spec),
                    Some(ready_status(EPOCH_1000, serde_json::json!([]))),
                    None,
                );
                let never_carves = b.effective_dry_run(long_past);
                let explicitly_held = matches!(mode, Some("shadow") | Some("suspended"));
                assert_eq!(
                    never_carves, explicitly_held,
                    "INVARIANT VIOLATED for (mode={mode_label}, dryRun={dry_run}): a band may stay \
                     never-live with a long-clean window IFF it carries an explicit permanent mode; \
                     the legacy dryRun boolean must never produce a never-exit state"
                );
            }
        }
    }

    #[test]
    fn unset_defaults_to_shadow_confirm_effect() {
        let b = mk_band(serde_json::json!({}), None, None);
        assert_eq!(b.promotion_mode(), PromotionMode::ShadowConfirmEffect);
        // no status yet ⇒ the gate hasn't passed ⇒ still shadow
        assert!(b.effective_dry_run(0));
    }

    #[test]
    fn shadow_confirm_effect_promotes_after_clean_window() {
        // Ready since epoch 1000, confirm_after default 1800.
        let b = mk_band(serde_json::json!({}), Some(ready_status(EPOCH_1000, serde_json::json!([]))), None);
        assert!(b.effective_dry_run(1000 + 100), "still shadow before the window elapses");
        assert!(!b.effective_dry_run(1000 + 1801), "auto-promotes once the clean window has held");
    }

    #[test]
    fn conflict_or_stale_blocks_promotion() {
        let conflicted = ready_status(
            EPOCH_1000,
            serde_json::json!([{ "type": "Conflict", "status": "True", "reason": "C", "message": "m", "lastTransitionTime": EPOCH_1000 }]),
        );
        let b = mk_band(serde_json::json!({}), Some(conflicted), None);
        assert!(b.effective_dry_run(i64::MAX / 2), "a field-owned/Conflict band must NOT auto-promote");
    }

    #[test]
    fn operator_annotation_promotes_immediately() {
        let b = mk_band(
            serde_json::json!({}),
            None, // no window elapsed, no Ready condition
            Some(serde_json::json!({ "breathe.pleme.io/confirmed": "true" })),
        );
        assert!(!b.effective_dry_run(0), "the operator fast-path confirms immediately");
    }

    // ── outorga migration (2026-07-18): the two-key `effective_dry_run_frozen` +
    //    the Tier-B `legacy_effective_dry_run` helper ─────────────────────────

    /// The `frozen` key overrides EVERY promotion state, exactly like
    /// `outorga`'s `freeze_is_the_master_switch_overriding_every_mode` unit
    /// test proves at the FSM layer — this is that same law observed through
    /// the `Band` trait's own two-key method. Even a band whose OWN gate has
    /// long passed (Effect mode) is shadowed when an external freeze applies.
    #[test]
    fn effective_dry_run_frozen_key_overrides_even_effect_mode() {
        let b = mk_band(serde_json::json!({ "mode": "effect" }), None, None);
        assert!(!b.effective_dry_run_frozen(0, false), "unfrozen + effect mode ⇒ apply");
        assert!(b.effective_dry_run_frozen(0, true), "frozen must shadow even explicit effect mode");
    }

    /// REGRESSION for the `breathe-host-agent/src/main.rs` bug fixed 2026-07-18:
    /// before the fix, the host reconcile composed the RAW `dry_run()` field
    /// (`obj.dry_run() || !write_enabled`) instead of the confirm-gated
    /// `effective_dry_run`. A freshly-created `ArcBand` — the exact CRD kind the
    /// host agent watches — with `dryRun` unset (defaults false) and NO status
    /// yet (never observed, so no `Ready` condition has ever been recorded) is
    /// the case that was SILENTLY WRONG: the old formula said "go live now"
    /// purely because the raw boolean defaulted false, even though the band had
    /// never proven itself Ready for even one tick. Prove the two formulas
    /// actually diverge on this exact input, and that the new one is the safe
    /// (still-shadow) answer.
    #[test]
    fn host_agent_bug_fresh_arc_band_no_longer_treated_as_confirmed_live() {
        let b: ArcBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "ArcBand",
            "metadata": { "name": "rio" },
            "spec": { "targetRef": { "kind": "Node", "name": "rio" } }
        }))
        .expect("a minimal ArcBand must deserialize");
        let write_enabled = true;
        let now = 0i64;

        // The OLD (buggy) formula, reproduced verbatim for the comparison.
        let old_effective_dry_run = b.dry_run() || !write_enabled;
        assert!(!old_effective_dry_run, "the bug: the raw boolean alone said 'go live now'");

        // The NEW (fixed) formula: still shadow — no Ready condition has ever
        // been observed, so the ShadowConfirmEffect confirm-gate correctly
        // blocks on NotReady.
        let new_effective_dry_run = b.effective_dry_run_frozen(now, !write_enabled);
        assert!(new_effective_dry_run, "FIX: a never-observed band must stay shadowed, not go live");
        assert_ne!(
            old_effective_dry_run, new_effective_dry_run,
            "the two formulas must diverge on this exact input — that divergence IS the bug"
        );
    }

    /// The Tier-B legacy-boolean composition (`BreatheCloudPool`/`IsolationBand`/
    /// the `PodMemoryHigh` dispatch): `legacy_effective_dry_run` must reproduce
    /// the exact truth table of the `dry_run || !write_enabled` expression it
    /// replaces at every call site, while also surfacing a typed
    /// [`outorga::ShadowReason`] distinguishing WHY (explicit dryRun vs an
    /// external freeze) — a real improvement no bare bool could carry.
    #[test]
    fn legacy_effective_dry_run_matches_the_old_truth_table_and_types_the_reason() {
        // (dry_run, frozen) -> is_shadow, matching `dry_run || frozen` exactly.
        let cases = [(false, false, false), (false, true, true), (true, false, true), (true, true, true)];
        for (dry_run, frozen, want_shadow) in cases {
            let d = legacy_effective_dry_run(dry_run, frozen);
            assert_eq!(
                d.is_shadow(),
                want_shadow,
                "legacy_effective_dry_run({dry_run}, {frozen}) must match dry_run || frozen"
            );
        }
        // The NEW capability: the reason distinguishes explicit dryRun from an
        // external freeze — previously both collapsed into the same bare bool.
        assert_eq!(
            legacy_effective_dry_run(true, false).shadow_reason(),
            Some(outorga::ShadowReason::ModeShadow),
            "an explicit dryRun:true is reported as ModeShadow"
        );
        assert_eq!(
            legacy_effective_dry_run(false, true).shadow_reason(),
            Some(outorga::ShadowReason::Frozen),
            "a frozen pool/node is reported as Frozen, even with dryRun:false"
        );
        assert_eq!(legacy_effective_dry_run(false, false).shadow_reason(), None, "an applied decision carries no shadow reason");
    }

    // ── ReplicaBand (the HORIZONTAL band) ─────────────────────────────────────

    #[test]
    fn replica_band_defaults_ha_floor_two_and_bridges_to_the_control_config() {
        // A minimal ReplicaBand: only targetRef + metric are required.
        let rb: ReplicaBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "ReplicaBand",
            "metadata": { "name": "web", "namespace": "prod" },
            "spec": {
                "targetRef": { "kind": "Deployment", "name": "web", "apiVersion": "apps/v1" },
                "metric": { "prometheus": "sum(rate(http_requests_total{app='web'}[1m]))" }
            }
        })).expect("a minimal ReplicaBand must deserialize");
        // Floor defaults to 2 (HA) and the signal defaults to utilization.
        assert_eq!(rb.spec.floor, 2, "HA floor default is 2");
        assert_eq!(rb.spec.signal, ReplicaSignalSpec::Utilization);
        // The CRD bridges to the tested control-layer config…
        let rc = rb.spec.replica_band_config();
        assert_eq!(rc.effective_floor(), 2);
        assert_eq!(rc.signal, ReplicaSignal::Utilization);
        // …and its actuator addresses `.spec.replicas` on the owner kind.
        assert_eq!(rb.spec.provider_layout(), LimitLayout::Replica { kind: "Deployment".into() });
        // No reclaim metric ⇒ not spot-aware by default.
        assert!(rb.spec.provider_reclaim_metric().is_none());
    }

    #[test]
    fn replica_band_rides_the_same_shadow_confirm_effect_gate() {
        // Same lifecycle default as MemoryBand: shadow until a clean window holds.
        let rb: ReplicaBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "ReplicaBand",
            "metadata": { "name": "web", "namespace": "prod" },
            "spec": {
                "targetRef": { "kind": "Deployment", "name": "web", "apiVersion": "apps/v1" },
                "metric": { "prometheus": "q" },
                "haFloor": 3, "ceiling": 20, "signal": "queueDepth", "target": 10.0
            }
        })).expect("deserialize");
        // starts shadowed (no status) — the horizontal band is never live-unconfirmed.
        assert!(rb.effective_dry_run(0), "ReplicaBand starts in shadow (ShadowConfirmEffect)");
        // haFloor raises the effective floor to 3.
        assert_eq!(rb.spec.replica_band_config().effective_floor(), 3);
        // and the vertical band_config it exposes for the gate carries the counts.
        let bc = crate::Band::band_config(&rb).unwrap();
        assert_eq!(bc.floor_bytes, 3);
        assert_eq!(bc.ceiling_bytes, 20);
    }

    #[test]
    fn replica_band_crd_advertises_its_camelcase_surface() {
        let crd = <ReplicaBand as kube::CustomResourceExt>::crd();
        let yaml = serde_yaml::to_string(&crd).unwrap();
        assert!(yaml.contains("haFloor"), "the CRD must advertise haFloor (camelCase)");
        assert!(yaml.contains("toleranceUp"));
        assert!(yaml.contains("maxScaleDownPods"));
        assert!(yaml.contains("rband"), "the shortname is registered");
    }

    #[test]
    fn replica_band_topology_defaults_non_persistent_and_bridges_each_arm() {
        // An omitted `topology` ⇒ NonPersistent (back-compat: existing bands unchanged).
        let plain: ReplicaBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "ReplicaBand",
            "metadata": { "name": "web", "namespace": "prod" },
            "spec": {
                "targetRef": { "kind": "Deployment", "name": "web", "apiVersion": "apps/v1" },
                "metric": { "prometheus": "q" }
            }
        })).expect("deserialize");
        assert_eq!(plain.spec.topology, TopologySpec::default());
        assert_eq!(plain.spec.topology.kind, TopologyKind::NonPersistent);
        assert_eq!(plain.spec.replica_band_config().topology, Topology::NonPersistent);

        // Each authored arm bridges to the control-layer topology + raises the floor.
        let quorum: ReplicaBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "ReplicaBand",
            "metadata": { "name": "etcd", "namespace": "kube-system" },
            "spec": {
                "targetRef": { "kind": "StatefulSet", "name": "etcd", "apiVersion": "apps/v1" },
                "metric": { "prometheus": "q" }, "topology": { "kind": "fullyDistributed" }, "ceiling": 9
            }
        })).expect("deserialize");
        assert_eq!(quorum.spec.topology.kind, TopologyKind::FullyDistributed);
        let rc = quorum.spec.replica_band_config();
        assert_eq!(rc.topology, Topology::FullyDistributed);
        assert_eq!(rc.topology_floor(), 3, "a quorum floor is snapped odd, ≥ 3");

        let db: ReplicaBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "ReplicaBand",
            "metadata": { "name": "mysql", "namespace": "camelot" },
            "spec": {
                "targetRef": { "kind": "StatefulSet", "name": "mysql", "apiVersion": "apps/v1" },
                "metric": { "prometheus": "q" },
                "topology": { "kind": "masterSlave", "primaries": 1 }, "ceiling": 8
            }
        })).expect("deserialize");
        assert_eq!(db.spec.topology.kind, TopologyKind::MasterSlave);
        assert_eq!(db.spec.topology.primaries, Some(1));
        assert_eq!(db.spec.replica_band_config().topology, Topology::MasterSlave { primaries: 1 });

        let neo: ReplicaBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "ReplicaBand",
            "metadata": { "name": "neo4j", "namespace": "camelot" },
            "spec": {
                "targetRef": { "kind": "StatefulSet", "name": "neo4j", "apiVersion": "apps/v1" },
                "metric": { "prometheus": "q" },
                "topology": { "kind": "persistent", "replicationFactor": 3 }, "ceiling": 10
            }
        })).expect("deserialize");
        assert_eq!(neo.spec.topology.kind, TopologyKind::Persistent);
        assert_eq!(neo.spec.topology.replication_factor, Some(3));
        assert_eq!(neo.spec.replica_band_config().topology, Topology::Persistent { replication_factor: 3 });
        assert_eq!(neo.spec.replica_band_config().topology_floor(), 3);
    }

    #[test]
    fn replica_band_crd_advertises_the_topology_surface() {
        let crd = <ReplicaBand as kube::CustomResourceExt>::crd();
        let yaml = serde_yaml::to_string(&crd).unwrap();
        assert!(yaml.contains("topology"), "the CRD must advertise the topology field");
        assert!(yaml.contains("fullyDistributed"), "the FullyDistributed arm is in the schema");
        assert!(yaml.contains("replicationFactor"), "the Persistent factor is camelCase in the schema");
    }

    #[test]
    fn topology_kind_mirror_agrees_with_the_control_border() {
        // CATALOG REFLECTION (CRD ↔ Rust border): every TopologyKind arm maps to a
        // distinct breathe_control Topology whose stable label is one of ALL_LABELS,
        // and the four arms cover ALL_LABELS exactly — the CRD mirror can't drift from
        // the control enum without failing here.
        use breathe_control::replica::Topology;
        let arms = [
            TopologyKind::NonPersistent,
            TopologyKind::Persistent,
            TopologyKind::MasterSlave,
            TopologyKind::FullyDistributed,
        ];
        let mut labels: Vec<&'static str> = arms
            .iter()
            .map(|k| {
                let spec = TopologySpec { kind: *k, replication_factor: Some(1), primaries: Some(1) };
                spec.control().as_str()
            })
            .collect();
        labels.sort_unstable();
        let mut expected = Topology::ALL_LABELS.to_vec();
        expected.sort_unstable();
        assert_eq!(labels, expected, "the CRD TopologyKind arms must mirror breathe_control::Topology exactly");

        // and the serde wire tokens are the camelCase mirror (structural-schema-safe).
        for (k, tok) in [
            (TopologyKind::NonPersistent, "nonPersistent"),
            (TopologyKind::Persistent, "persistent"),
            (TopologyKind::MasterSlave, "masterSlave"),
            (TopologyKind::FullyDistributed, "fullyDistributed"),
        ] {
            let j = serde_json::to_value(k).unwrap();
            assert_eq!(j, serde_json::Value::String(tok.to_string()));
        }
    }

    #[test]
    fn stateful_replica_band_on_a_deployment_is_parse_rejected() {
        use breathe_control::replica::ReplicaError;
        // a masterSlave band pointed at a Deployment is refused (topology ↔ kind gate).
        let db_on_deploy: ReplicaBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "ReplicaBand",
            "metadata": { "name": "mysql", "namespace": "camelot" },
            "spec": {
                "targetRef": { "kind": "Deployment", "name": "mysql" },
                "metric": { "prometheus": "q" },
                "topology": { "kind": "masterSlave", "primaries": 1 }, "ceiling": 8
            }
        })).expect("deserializes");
        assert_eq!(
            db_on_deploy.spec.validate_for_target(),
            Err(ReplicaError::TopologyTargetMismatch("master-slave"))
        );

        // the SAME band on a StatefulSet validates.
        let db_on_sts: ReplicaBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "ReplicaBand",
            "metadata": { "name": "mysql", "namespace": "camelot" },
            "spec": {
                "targetRef": { "kind": "StatefulSet", "name": "mysql" },
                "metric": { "prometheus": "q" },
                "topology": { "kind": "masterSlave", "primaries": 1 }, "ceiling": 8
            }
        })).expect("deserializes");
        assert_eq!(db_on_sts.spec.validate_for_target(), Ok(()));

        // a NonPersistent band on a Deployment is fine (stateless pods interchangeable).
        let web: ReplicaBand = serde_json::from_value(serde_json::json!({
            "apiVersion": "breathe.pleme.io/v1", "kind": "ReplicaBand",
            "metadata": { "name": "web", "namespace": "camelot" },
            "spec": {
                "targetRef": { "kind": "Deployment", "name": "web" },
                "metric": { "prometheus": "q" }, "ceiling": 10
            }
        })).expect("deserializes");
        assert_eq!(web.spec.validate_for_target(), Ok(()));
    }

    // ═══════════════ BreathePosture — the 3-tier fold (override > posture > compiled-default) ═══════════════

    /// Every value here is DELIBERATELY far from the compiled default (`d_setpoint() =
    /// 0.80`, `d_grow_above() = 0.85`, etc.) so a test can never accidentally pass by
    /// reading the wrong tier.
    fn posture_fixture() -> BreathePostureSpec {
        BreathePostureSpec {
            description: Some("test fixture — every field deliberately diverges from the compiled default".into()),
            setpoint: 0.5,
            grow_above: 0.99,
            grow_factor: 2.0,
            shrink_below: 0.4,
            shrink_factor: 0.5,
            cooldown_seconds: 999,
            max_staleness_seconds: 555,
            disruption_policy: DisruptionPolicy::AllowConditional,
            // The request-policy axis is `None` here on purpose: this fixture
            // exercises the BAND-LAW 8-tuple fold, and leaving the new axis unset
            // is the shape every one of the five live camelot postures actually
            // has. `request_policy_falls_through_the_same_three_tiers` covers the
            // Some(_) case separately.
            workload_class: None,
            qos_target: None,
            demand: None,
        }
    }

    #[test]
    fn real_live_cr_fixture_deserializes_with_all_eight_fields_explicit() {
        // A real, live CR pulled verbatim from `akeyless-k8s`'s
        // `clusters/camelot-eks/infrastructure/karpenter/breathe-bands.yaml` (the
        // karpenter-cpu CpuBand). Every hand-authored CR on the fleet sets these 7
        // numeric fields + disruptionPolicy explicitly — this proves the Option<T>
        // conversion is wire-compatible: an EXISTING CR deserializes to Some(value)
        // unchanged, never silently regresses to None.
        let cpu: CpuBand = serde_yaml::from_str(
            r#"
apiVersion: breathe.pleme.io/v1
kind: CpuBand
metadata:
  name: karpenter-cpu
  namespace: karpenter
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: karpenter
    container: controller
  setpoint: 0.8
  floor: "100m"
  ceiling: "500m"
  requestFloor: "100m"
  growAbove: 0.85
  growFactor: 1.25
  shrinkBelow: 0.7
  shrinkFactor: 0.9
  cooldownSeconds: 600
  disruptionPolicy: restartFreeOnly
  dryRun: true
  maxStalenessSeconds: 120
"#,
        )
        .expect("a real live CR must deserialize");
        assert_eq!(cpu.spec.setpoint, Some(0.8));
        assert_eq!(cpu.spec.grow_above, Some(0.85));
        assert_eq!(cpu.spec.grow_factor, Some(1.25));
        assert_eq!(cpu.spec.shrink_below, Some(0.7));
        assert_eq!(cpu.spec.shrink_factor, Some(0.9));
        assert_eq!(cpu.spec.cooldown_seconds, Some(600));
        assert_eq!(cpu.spec.max_staleness_seconds, Some(120));
        assert_eq!(cpu.spec.disruption_policy, Some(DisruptionPolicy::RestartFreeOnly));
        assert_eq!(cpu.spec.posture_ref, None, "no postureRef authored ⇒ None, not an error");
        // And the resolved values are BYTE-IDENTICAL to before this change.
        let cfg = Band::band_config(&cpu).unwrap();
        assert_eq!(cfg.setpoint, 0.8);
        assert_eq!(cfg.floor_bytes, 100);
        assert_eq!(cfg.ceiling_bytes, 500);
        assert_eq!(cpu.cooldown_seconds(), 600);
        assert_eq!(cpu.max_staleness_seconds(), 120);
        assert_eq!(cpu.disruption_policy(), DisruptionPolicy::RestartFreeOnly);
    }

    #[test]
    fn explicit_override_always_wins_over_posture() {
        let spec: MemoryBandSpec = serde_json::from_value(serde_json::json!({
            "targetRef": { "kind": "Deployment", "name": "x" },
            "postureRef": "platform-default",
            "setpoint": 0.95,
            "cooldownSeconds": 42,
            "disruptionPolicy": "allowRestart",
        }))
        .unwrap();
        let band = MemoryBand::new("x", spec);
        let p = posture_fixture();
        let cfg = band.band_config_with_posture(Some(&p)).unwrap();
        assert_eq!(cfg.setpoint, 0.95, "the CR's own explicit setpoint wins over the posture's 0.5");
        assert_eq!(
            band.cooldown_seconds_with_posture(Some(&p)),
            42,
            "the CR's own explicit cooldown wins over the posture's 999"
        );
        assert_eq!(
            band.disruption_policy_with_posture(Some(&p)),
            DisruptionPolicy::AllowRestart,
            "the CR's own explicit disruptionPolicy wins over the posture's allowConditional"
        );
        // A field NOT overridden on this CR still falls through to the posture —
        // proving the fold is genuinely PER-FIELD, not all-or-nothing.
        assert_eq!(cfg.grow_above, 0.99, "unset on the CR ⇒ the posture's value, not the compiled default's 0.85");
    }

    #[test]
    fn posture_fills_every_unset_field_when_override_is_absent() {
        let spec: MemoryBandSpec = serde_json::from_value(serde_json::json!({
            "targetRef": { "kind": "Deployment", "name": "x" },
            "postureRef": "platform-default",
        }))
        .unwrap();
        let band = MemoryBand::new("x", spec);
        let p = posture_fixture();
        let cfg = band.band_config_with_posture(Some(&p)).unwrap();
        assert_eq!(cfg.setpoint, 0.5);
        assert_eq!(cfg.grow_above, 0.99);
        assert_eq!(cfg.shrink_below, 0.4);
        assert_eq!(cfg.grow_factor, 2.0);
        assert_eq!(cfg.shrink_factor, 0.5);
        assert_eq!(band.cooldown_seconds_with_posture(Some(&p)), 999);
        assert_eq!(band.max_staleness_seconds_with_posture(Some(&p)), 555);
        assert_eq!(band.disruption_policy_with_posture(Some(&p)), DisruptionPolicy::AllowConditional);
    }

    #[test]
    fn compiled_default_is_the_final_floor_when_no_override_and_no_posture() {
        let spec: MemoryBandSpec = serde_json::from_value(serde_json::json!({
            "targetRef": { "kind": "Deployment", "name": "x" },
        }))
        .unwrap();
        let band = MemoryBand::new("x", spec);
        let cfg = band.band_config_with_posture(None).unwrap();
        assert_eq!(cfg.setpoint, 0.80);
        assert_eq!(cfg.grow_above, 0.85);
        assert_eq!(cfg.shrink_below, 0.70);
        assert_eq!(cfg.grow_factor, 1.25);
        assert_eq!(cfg.shrink_factor, 0.90);
        assert_eq!(band.cooldown_seconds_with_posture(None), 600);
        assert_eq!(band.max_staleness_seconds_with_posture(None), 120);
        assert_eq!(band.disruption_policy_with_posture(None), DisruptionPolicy::RestartFreeOnly);
        // Byte-identical to the plain (posture-blind) accessors — the existing
        // 2-tier fold (override > compiled-default) is completely unaffected.
        assert_eq!(Band::band_config(&band).unwrap().setpoint, 0.80);
        assert_eq!(band.cooldown_seconds(), 600);
        assert_eq!(band.max_staleness_seconds(), 120);
        assert_eq!(band.disruption_policy(), DisruptionPolicy::RestartFreeOnly);
    }

    #[test]
    fn dangling_posture_ref_never_panics_and_falls_through_to_compiled_default() {
        // The CR NAMES a posture (`postureRef` is Some) but the caller (the
        // reconcile loop's `resolve_posture`) could not resolve it — it doesn't
        // exist, or the reflector cache hasn't seen it yet. A dangling reference is
        // represented identically to "no posture referenced at all" at this API:
        // the caller passes `None`. This proves that path never panics/errors.
        let spec: MemoryBandSpec = serde_json::from_value(serde_json::json!({
            "targetRef": { "kind": "Deployment", "name": "x" },
            "postureRef": "does-not-exist",
        }))
        .unwrap();
        let band = MemoryBand::new("x", spec);
        assert_eq!(band.posture_ref(), Some("does-not-exist"));
        let cfg = band.band_config_with_posture(None).unwrap();
        assert_eq!(cfg.setpoint, 0.80, "dangling ref ⇒ compiled default, never a panic/error");
        assert_eq!(band.cooldown_seconds_with_posture(None), 600);
        assert_eq!(band.disruption_policy_with_posture(None), DisruptionPolicy::RestartFreeOnly);
    }

    #[test]
    fn posture_ref_and_rationale_serialize_camel_case_and_round_trip() {
        let spec: MemoryBandSpec = serde_json::from_value(serde_json::json!({
            "targetRef": { "kind": "Deployment", "name": "x" },
            "postureRef": "stateful-workload",
            "disruptionPolicy": "allowConditional",
            "disruptionPolicyRationale": "genuinely stateful; no in-place resize available",
        }))
        .unwrap();
        assert_eq!(spec.posture_ref.as_deref(), Some("stateful-workload"));
        assert_eq!(spec.disruption_policy, Some(DisruptionPolicy::AllowConditional));
        assert_eq!(spec.disruption_policy_rationale.as_deref(), Some("genuinely stateful; no in-place resize available"));
        let v = serde_json::to_value(&spec).unwrap();
        assert_eq!(v["postureRef"], "stateful-workload");
        assert_eq!(v["disruptionPolicyRationale"], "genuinely stateful; no in-place resize available");

        // An UNSET postureRef/rationale/8-tuple field is omitted entirely
        // (skip_serializing_if), keeping an un-migrated CR's serialized shape
        // byte-identical to before this change — no `"setpoint": null` noise.
        let bare: MemoryBandSpec = serde_json::from_value(serde_json::json!({
            "targetRef": { "kind": "Deployment", "name": "y" },
        }))
        .unwrap();
        let bare_v = serde_json::to_value(&bare).unwrap();
        assert!(bare_v.get("postureRef").is_none());
        assert!(bare_v.get("disruptionPolicyRationale").is_none());
        assert!(bare_v.get("setpoint").is_none(), "an unset Option field must not appear in the serialized form");
        assert!(bare_v.get("disruptionPolicy").is_none());
    }

    #[test]
    fn breatheposture_crd_generates_cluster_scoped() {
        let crd = <BreathePosture as kube::CustomResourceExt>::crd();
        assert_eq!(crd.spec.names.kind, "BreathePosture");
        assert_eq!(crd.spec.scope, "Cluster");
    }

    const POSTURE_PLATFORM_DEFAULT_YAML: &str = r#"
apiVersion: breathe.pleme.io/v1
kind: BreathePosture
metadata: { name: platform-default }
spec:
  description: "Fleet-wide default for stateless/restart-tolerant workloads."
  setpoint: 0.8
  growAbove: 0.85
  growFactor: 1.25
  shrinkBelow: 0.7
  shrinkFactor: 0.9
  cooldownSeconds: 600
  maxStalenessSeconds: 120
  disruptionPolicy: restartFreeOnly
"#;

    const POSTURE_STATEFUL_WORKLOAD_YAML: &str = r#"
apiVersion: breathe.pleme.io/v1
kind: BreathePosture
metadata: { name: stateful-workload }
spec:
  description: >
    Genuinely stateful workloads (neo4j/rustfs-class, pangea-operator) where a
    restart-free in-place resize is not always available.
  setpoint: 0.8
  growAbove: 0.85
  growFactor: 1.25
  shrinkBelow: 0.7
  shrinkFactor: 0.9
  cooldownSeconds: 600
  maxStalenessSeconds: 120
  disruptionPolicy: allowConditional
"#;

    const POSTURE_STORAGE_VOLUME_YAML: &str = r#"
apiVersion: breathe.pleme.io/v1
kind: BreathePosture
metadata: { name: storage-volume }
spec:
  description: "PVC-capacity StorageBand growth — slower reaction, longer staleness tolerance."
  setpoint: 0.8
  growAbove: 0.8
  growFactor: 1.5
  shrinkBelow: 0.7
  shrinkFactor: 0.9
  cooldownSeconds: 3600
  maxStalenessSeconds: 300
  disruptionPolicy: restartFreeOnly
"#;

    #[test]
    fn the_three_example_postures_deserialize_and_drive_a_real_band_end_to_end() {
        let platform: BreathePosture =
            serde_yaml::from_str(POSTURE_PLATFORM_DEFAULT_YAML).expect("platform-default deserializes");
        assert_eq!(platform.spec.setpoint, 0.8);
        assert_eq!(platform.spec.disruption_policy, DisruptionPolicy::RestartFreeOnly);

        let stateful: BreathePosture =
            serde_yaml::from_str(POSTURE_STATEFUL_WORKLOAD_YAML).expect("stateful-workload deserializes");
        assert_eq!(stateful.spec.disruption_policy, DisruptionPolicy::AllowConditional);

        let storage: BreathePosture =
            serde_yaml::from_str(POSTURE_STORAGE_VOLUME_YAML).expect("storage-volume deserializes");
        assert_eq!(storage.spec.cooldown_seconds, 3600);
        assert_eq!(storage.spec.max_staleness_seconds, 300);

        // End-to-end: a bare band CR (no explicit tunables) referencing
        // `stateful-workload` resolves to THAT posture's tuple — proving the
        // fixture -> BreathePostureSpec -> Band::band_config_with_posture wiring,
        // not just each half in isolation.
        let bare: CpuBand = serde_yaml::from_str(
            r#"
apiVersion: breathe.pleme.io/v1
kind: CpuBand
metadata: { name: some-cpu-band, namespace: camelot }
spec:
  targetRef: { kind: Deployment, name: some-workload }
  postureRef: stateful-workload
  floor: "250m"
  ceiling: "2"
"#,
        )
        .expect("deserializes");
        let cfg = bare.band_config_with_posture(Some(&stateful.spec)).unwrap();
        assert_eq!(cfg.setpoint, 0.8);
        assert_eq!(cfg.grow_factor, 1.25);
        assert_eq!(
            bare.disruption_policy_with_posture(Some(&stateful.spec)),
            DisruptionPolicy::AllowConditional
        );
    }

    // ══ S2: the bound-introduction axis on the CRD border ═════════════════════

    /// The wire enum and the pure `breathe-control` atom are pinned ONE-TO-ONE by
    /// an exhaustive match in both directions. The two types exist separately only
    /// because a CRD field needs `JsonSchema` and `breathe-control` is deliberately
    /// dependency-free — the same already-blessed split as `PromotionMode` vs
    /// `outorga::PromotionMode`. Adding an arm to either without the other is a
    /// compile error here, so they cannot drift.
    #[test]
    fn bound_introduction_spec_maps_one_to_one_onto_the_control_atom() {
        for (wire, control) in [
            (BoundIntroductionSpec::Forbidden, BoundIntroduction::Forbidden),
            (BoundIntroductionSpec::Allowed, BoundIntroduction::Allowed),
        ] {
            assert_eq!(wire.to_control(), control);
            // the reverse direction, exhaustively — a new control arm fails to compile.
            let back = match control {
                BoundIntroduction::Forbidden => BoundIntroductionSpec::Forbidden,
                BoundIntroduction::Allowed => BoundIntroductionSpec::Allowed,
            };
            assert_eq!(back, wire);
        }
    }

    /// The compiled default is `forbidden`, and it is a DEFAULT — not a reading of
    /// some other field. A band that says nothing does not cap a workload whose
    /// author declared no limit.
    #[test]
    fn an_unauthored_band_may_not_introduce_a_bound() {
        let bare: CpuBand = serde_yaml::from_str(
            r#"
apiVersion: breathe.pleme.io/v1
kind: CpuBand
metadata: { name: coredns, namespace: kube-system }
spec:
  targetRef: { kind: Deployment, name: coredns }
  floor: "50m"
  ceiling: "2"
"#,
        )
        .expect("deserializes");
        assert_eq!(bare.bound_introduction_spec(), None, "nothing authored");
        assert_eq!(
            bare.bound_introduction(),
            BoundIntroduction::Forbidden,
            "and the compiled default refuses to invent a cpu limit for cluster DNS"
        );
    }

    /// The escape hatch is authorable, and reads through the same accessor the
    /// controller uses — so "I declared allowed" and "the tick saw allowed" cannot
    /// diverge.
    #[test]
    fn an_authored_allowance_reaches_the_reconcile_input() {
        let ceded: MemoryBand = serde_yaml::from_str(
            r#"
apiVersion: breathe.pleme.io/v1
kind: MemoryBand
metadata: { name: pg, namespace: pangea-system }
spec:
  targetRef: { kind: Cluster, name: pangea-database, apiVersion: postgresql.cnpg.io/v1 }
  floor: "1Gi"
  ceiling: "8Gi"
  boundIntroduction: allowed
"#,
        )
        .expect("deserializes");
        assert_eq!(ceded.bound_introduction_spec(), Some(BoundIntroductionSpec::Allowed));
        assert_eq!(ceded.bound_introduction(), BoundIntroduction::Allowed);
    }

    // ═════════════════ RequestBand — the RESERVATION dimension ═════════════════

    /// **The mirror-drift gate for `QosClass`.** Iterates the UPSTREAM's own
    /// `ALL` and round-trips each arm, so adding a variant in
    /// `breathe-invariant` without adding it here fails the build (`E0004` in
    /// `from_invariant`) rather than silently producing a CRD that cannot express
    /// a class the algebra knows about.
    ///
    /// This is what makes `QosClassSpec` a MIRROR rather than a FORK — the same
    /// standard `PromotionMode`↔`outorga::PromotionMode` is held to.
    #[test]
    fn qos_class_mirror_covers_every_arm() {
        use breathe_invariant::isolation::QosClass;
        for q in QosClass::ALL {
            assert_eq!(QosClassSpec::from_invariant(q).to_invariant(), q, "round-trip must be identity for {q:?}");
        }
        // …and the serde spelling matches the upstream's, so a CR and a contract
        // value are the same string on the wire.
        for q in QosClass::ALL {
            let mirrored = serde_json::to_value(QosClassSpec::from_invariant(q)).unwrap();
            assert_eq!(mirrored, serde_json::json!(q.as_str()), "wire spelling must match upstream for {q:?}");
        }
    }

    /// The same gate for `WorkloadClass`, plus the delegation check: the mirror
    /// must not re-decide `default_qos`, it must ask upstream.
    #[test]
    fn workload_class_mirror_covers_every_arm_and_delegates_its_default() {
        use breathe_invariant::isolation::WorkloadClass;
        for w in WorkloadClass::ALL {
            let m = WorkloadClassSpec::from_invariant(w);
            assert_eq!(m.to_invariant(), w, "round-trip must be identity for {w:?}");
            assert_eq!(
                serde_json::to_value(m).unwrap(),
                serde_json::json!(w.as_str()),
                "wire spelling must match upstream for {w:?}"
            );
            assert_eq!(
                m.default_qos().to_invariant(),
                w.default_qos(),
                "the mirror must DELEGATE default_qos, never restate it"
            );
        }
        // The concrete postures, pinned so a silent upstream change is visible.
        assert_eq!(WorkloadClassSpec::Critical.default_qos(), QosClassSpec::Guaranteed);
        assert_eq!(WorkloadClassSpec::Standard.default_qos(), QosClassSpec::Burstable);
        assert_eq!(WorkloadClassSpec::Batch.default_qos(), QosClassSpec::BestEffort);
        assert_eq!(WorkloadClassSpec::Noisy.default_qos(), QosClassSpec::Burstable);
    }

    /// A minimal `RequestBand` parses, and every default is the safe one.
    #[test]
    fn a_minimal_request_band_defaults_to_the_safe_posture() {
        let b: RequestBand = serde_yaml::from_str(
            r"
apiVersion: breathe.pleme.io/v1
kind: RequestBand
metadata: { name: sui-request, namespace: camelot-build }
spec:
  targetRef: { kind: Deployment, name: sui, apiVersion: apps/v1 }
  resource: memory
",
        )
        .expect("a minimal RequestBand deserializes");

        assert_eq!(b.spec.resource, RequestResourceSpec::Memory);
        assert_eq!(b.dimension_id(), breathe_provider::DimensionId::Request);
        // COMMITTED by default — an in-place-only request silently reverts on the
        // next rollout, taking the QoS protection with it.
        assert_eq!(b.spec.durability, DurabilitySpec::Committed);
        // No bound introduction: breathe does not seed a request onto a
        // BestEffort target by default, because doing so moves the QoS class and
        // a class move has no in-place path at all.
        assert_eq!(b.spec.bound_introduction, None);
        // Predictive is structurally unavailable on a reservation — every
        // predicted byte is capacity actually withheld from the scheduler.
        assert_eq!(b.predictive(), None);
        // Grow-only in practice: the controller withholds the reclaim.
        assert!(!b.suspended());
    }

    /// The band's UNIT is a function of `resource` — the reason this kind could
    /// not be `band_kind!`-stamped (that macro takes one constant unit).
    #[test]
    fn the_unit_follows_the_resource() {
        let build = |res: &str| -> RequestBand {
            serde_yaml::from_str(&format!(
                r#"
apiVersion: breathe.pleme.io/v1
kind: RequestBand
metadata: {{ name: b, namespace: n }}
spec:
  targetRef: {{ kind: Deployment, name: d, apiVersion: apps/v1 }}
  resource: {res}
  forceLimit: "512Mi"
"#
            ))
            .expect("deserializes")
        };
        // Bytes: "512Mi" parses as 512 MiB.
        assert_eq!(build("memory").force_limit_value(), Some(512 * 1024 * 1024));
        // Millicores: the SAME string parses differently (or not at all) — which
        // is exactly why the unit must not be a constant on this kind.
        assert_ne!(build("cpu").force_limit_value(), Some(512 * 1024 * 1024));

        assert_eq!(RequestResourceSpec::Memory.unit(), Unit::Bytes);
        assert_eq!(RequestResourceSpec::Cpu.unit(), Unit::Millicores);
    }

    /// The request-policy axis folds through the SAME three tiers as the band-law
    /// fields: an explicit per-CR value ALWAYS wins > the referenced posture >
    /// the compiled default.
    #[test]
    fn request_policy_falls_through_the_same_three_tiers() {
        let band = |spec_extra: &str| -> RequestBand {
            serde_yaml::from_str(&format!(
                r"
apiVersion: breathe.pleme.io/v1
kind: RequestBand
metadata: {{ name: b, namespace: n }}
spec:
  targetRef: {{ kind: Deployment, name: d, apiVersion: apps/v1 }}
  resource: memory
{spec_extra}"
            ))
            .expect("deserializes")
        };

        let mut posture = posture_fixture();
        posture.workload_class = Some(WorkloadClassSpec::Critical);
        posture.qos_target = Some(QosClassSpec::Guaranteed);
        posture.demand = Some(DemandSignalSpec { quantile: 0.99, window: "30d".into(), headroom: 0.5 });

        // Tier 3 — nothing authored anywhere: compiled defaults.
        let bare = band("");
        assert_eq!(bare.spec.resolved_workload_class(None), WorkloadClassSpec::Standard);
        assert_eq!(bare.spec.resolved_qos_target(None), QosClassSpec::Burstable);
        assert_eq!(bare.spec.resolved_demand(None), DemandSignalSpec::default());

        // Tier 2 — the posture fills every unset field.
        assert_eq!(bare.spec.resolved_workload_class(Some(&posture)), WorkloadClassSpec::Critical);
        assert_eq!(bare.spec.resolved_qos_target(Some(&posture)), QosClassSpec::Guaranteed);
        assert_eq!(bare.spec.resolved_demand(Some(&posture)).window, "30d");

        // Tier 1 — an explicit per-CR value beats the posture.
        let explicit = band("  workloadClass: batch\n  demand: { quantile: 0.5, window: 1h, headroom: 0.0 }\n");
        assert_eq!(explicit.spec.resolved_workload_class(Some(&posture)), WorkloadClassSpec::Batch);
        assert_eq!(explicit.spec.resolved_demand(Some(&posture)).window, "1h");

        // ── THE SUBTLE ONE, pinned because it is a safety property ──
        //
        // The band says `workloadClass: batch` (whose default QoS is
        // best-effort) while the posture explicitly pins `qosTarget: guaranteed`.
        // The posture WINS, and that is deliberate.
        //
        // The fold is PER FIELD, exactly as it is for the eight band-law fields:
        // `qosTarget` is unset on this band, so the band has expressed no opinion
        // about it, and an explicit posture value outranks a *derived* default.
        // The alternative reading — let `workloadClass` re-derive `qosTarget` and
        // beat the posture — errs in the dangerous direction: it would let a band
        // author silently strip the seal off a workload an operator had pinned to
        // guaranteed, which is the victoria-logs-422 shape arriving by a new
        // route. Over-provisioning is wasteful; under-sealing is fatal.
        assert_eq!(
            explicit.spec.resolved_qos_target(Some(&posture)),
            QosClassSpec::Guaranteed,
            "an explicit posture qosTarget outranks a class-DERIVED default — the safe direction"
        );
        // The band can still say so explicitly; it just has to say it out loud.
        let loud = band("  workloadClass: batch\n  qosTarget: best-effort\n");
        assert_eq!(loud.spec.resolved_qos_target(Some(&posture)), QosClassSpec::BestEffort);
        // And with NO posture pin, the class's own default does apply.
        let mut unpinned = posture.clone();
        unpinned.qos_target = None;
        assert_eq!(explicit.spec.resolved_qos_target(Some(&unpinned)), QosClassSpec::BestEffort);
    }

    /// The reverse of the case above, which is the one that actually bites: a
    /// posture pins `critical`/`guaranteed`, and a band declares a weaker class.
    /// The seal must survive.
    #[test]
    fn a_band_cannot_silently_strip_a_postures_pinned_seal() {
        let mut critical = posture_fixture();
        critical.workload_class = Some(WorkloadClassSpec::Critical);
        critical.qos_target = Some(QosClassSpec::Guaranteed);

        let weaker: RequestBand = serde_yaml::from_str(
            r"
apiVersion: breathe.pleme.io/v1
kind: RequestBand
metadata: { name: b, namespace: n }
spec:
  targetRef: { kind: Deployment, name: d, apiVersion: apps/v1 }
  resource: memory
  workloadClass: standard
",
        )
        .expect("deserializes");

        assert_eq!(weaker.spec.resolved_workload_class(Some(&critical)), WorkloadClassSpec::Standard);
        assert_eq!(
            weaker.spec.resolved_qos_target(Some(&critical)),
            QosClassSpec::Guaranteed,
            "downgrading the class must NOT silently unseal a workload the posture pinned"
        );
    }

    /// **Backward compatibility, asserted rather than assumed.** The five live
    /// `BreathePosture` CRs on camelot-eks carry only the 8 band-law fields. If
    /// the request-policy axis had been added as REQUIRED, every one of them
    /// would stop deserializing and the controller would break on the postures it
    /// is currently serving. This pins that it does not.
    #[test]
    fn a_live_posture_without_the_request_axis_still_deserializes() {
        let p: BreathePosture = serde_yaml::from_str(
            r"
apiVersion: breathe.pleme.io/v1
kind: BreathePosture
metadata: { name: standard }
spec:
  setpoint: 0.80
  growAbove: 0.85
  growFactor: 1.25
  shrinkBelow: 0.70
  shrinkFactor: 0.90
  cooldownSeconds: 600
  maxStalenessSeconds: 120
  disruptionPolicy: restartFreeOnly
",
        )
        .expect("a posture with no request policy must still parse");
        assert_eq!(p.spec.workload_class, None);
        assert_eq!(p.spec.qos_target, None);
        assert_eq!(p.spec.demand, None);
        // …and it round-trips without inventing the absent fields.
        let round = serde_json::to_value(&p.spec).unwrap();
        assert!(round.get("workloadClass").is_none(), "an absent axis must not be serialized back");
        assert!(round.get("qosTarget").is_none());
        assert!(round.get("demand").is_none());
    }

    // ── the durable coordinate (B3) ──────────────────────────────────────────

    fn request_band(spec_extra: &str) -> RequestBand {
        let mut y = String::from(
            r"
apiVersion: breathe.pleme.io/v1
kind: RequestBand
metadata: { name: sui-request, namespace: camelot-build }
spec:
  targetRef: { kind: Deployment, name: sui, container: sui }
  resource: memory
",
        );
        y.push_str(spec_extra);
        serde_yaml::from_str(&y).expect("the RequestBand fixture parses")
    }

    /// A band that must survive a rollout and cannot say where it lives is
    /// **misauthored** — reported, never silently downgraded to ephemeral.
    ///
    /// The silent downgrade is the tempting shape and the dangerous one: it
    /// would leave an operator believing a quality-of-service posture is durable
    /// while it evaporates on the next rollout — illegal state I5 arriving
    /// through the CRD instead of through the actuator.
    #[test]
    fn committed_with_no_manifest_ref_is_blocked_not_downgraded() {
        let b = request_band("  durability: committed\n");
        assert_eq!(
            b.spec.durable_coordinate().unwrap_err(),
            breathe_provider::ClassTransitionBlocked::NoManifestCoordinate
        );
    }

    /// `committed` is the DEFAULT, so an author who says nothing about
    /// durability lands in the reported-gap case rather than the silent-loss
    /// one. Pinned because flipping this default would make every existing band
    /// quietly ephemeral.
    #[test]
    fn durability_defaults_to_committed() {
        let b = request_band("");
        assert_eq!(b.spec.durability, DurabilitySpec::Committed);
        assert!(b.spec.durable_coordinate().is_err(), "and therefore reports its missing coordinate");
    }

    /// An `ephemeral` band reports a DIFFERENT reason than a misauthored
    /// `committed` one. Distinct arms because they send an operator to
    /// different fixes: one is "this band opted out", the other is "this band
    /// is incomplete".
    #[test]
    fn ephemeral_and_missing_ref_are_distinguishable_reasons() {
        use breathe_provider::ClassTransitionBlocked as B;
        assert_eq!(request_band("  durability: ephemeral\n").spec.durable_coordinate().unwrap_err(), B::EphemeralCannotTransition);
        assert_eq!(request_band("  durability: committed\n").spec.durable_coordinate().unwrap_err(), B::NoManifestCoordinate);
    }

    /// The coordinate round-trips into the provider type the writer consumes —
    /// one declaration, no CRD-side mirror to drift.
    #[test]
    fn a_manifest_ref_reaches_the_writer_as_a_real_coordinate() {
        let b = request_band(
            "  durability: committed\n  manifestRef:\n    path: clusters/camelot/apps/sui/release.yaml\n    marker: camelot-build/sui-request\n",
        );
        let c = b.spec.durable_coordinate().expect("a committed band with a ref has a home");
        assert_eq!(c.path, "clusters/camelot/apps/sui/release.yaml");
        assert_eq!(c.marker, "camelot-build/sui-request");

        // …and it composes straight into an addressed proposal, which is the
        // only currency the durable door accepts.
        let p = breathe_provider::AddressedProposal::carve(c, "3Gi", "memory", "sui");
        assert_eq!(p.path(), "clusters/camelot/apps/sui/release.yaml");
        assert_eq!(p.assignments().len(), 1);
    }

    /// Backward compatibility: a `RequestBand` authored before `manifestRef`
    /// existed still parses, and does not gain the field on round-trip.
    #[test]
    fn a_band_without_a_manifest_ref_still_parses_and_stays_absent() {
        let b = request_band("");
        assert!(b.spec.manifest_ref.is_none());
        let round = serde_json::to_value(&b.spec).unwrap();
        assert!(round.get("manifestRef").is_none(), "an absent coordinate must not be serialized back");
    }

    /// **The cross-repo drift gate.** This is the *verbatim* spec that
    /// `helmworks`' `pleme-lib.breatheBand` helper renders for a fully-populated
    /// `RequestBand` — captured from a real `helm template` run, not hand-written
    /// to match.
    ///
    /// Two repos, one contract: the chart is where an operator authors a band,
    /// and this crate is what has to parse it. Nothing else connects them, so
    /// without this test a renamed field or a changed casing on either side is
    /// caught only by a band that silently never reconciles in a live cluster.
    /// Re-capture it whenever the helper's `RequestBand` arm changes.
    #[test]
    fn the_helmworks_rendered_request_band_parses_into_this_type() {
        let b: RequestBand = serde_yaml::from_str(
            r#"
apiVersion: breathe.pleme.io/v1
kind: RequestBand
metadata:
  name: sui-request-memory
  namespace: camelot-build
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: sui
    container: sui
  resource: memory
  workloadClass: critical
  qosTarget: guaranteed
  demand:
    headroom: 0.15
    quantile: 0.95
    windowSeconds: 604800
  durability: committed
  manifestRef:
    path: "clusters/camelot/apps/sui/release.yaml"
    marker: "camelot-build/sui-request"
  floor: "512Mi"
  cooldownSeconds: 600
  disruptionPolicy: restartFreeOnly
  maxStalenessSeconds: 120
  mode: shadow
"#,
        )
        .expect("the chart's rendered RequestBand must parse into RequestBand");

        assert_eq!(b.spec.resource, RequestResourceSpec::Memory);
        assert_eq!(b.spec.workload_class, Some(WorkloadClassSpec::Critical));
        assert_eq!(b.spec.qos_target, Some(QosClassSpec::Guaranteed));
        assert_eq!(b.spec.durability, DurabilitySpec::Committed);
        assert_eq!(b.spec.mode, Some(PromotionMode::Shadow));
        // The whole point of the chart arm: a committed band arrives with a home.
        let c = b.spec.durable_coordinate().expect("the rendered band carries its coordinate");
        assert_eq!(c.marker, "camelot-build/sui-request");
    }

    /// The chart's MINIMAL `RequestBand` — `{enabled: true, resource: cpu}` and
    /// nothing else — is born shadowed and durable-but-unaddressed.
    ///
    /// Pinned because this is the shape an operator gets by flipping one
    /// boolean, and it must be the SAFE one: shadow (so it writes nothing) and
    /// a reported coordinate gap (so the missing durable home is visible rather
    /// than silently ephemeral).
    #[test]
    fn the_chart_minimal_request_band_is_born_shadowed_and_reports_its_gap() {
        let b: RequestBand = serde_yaml::from_str(
            r"
apiVersion: breathe.pleme.io/v1
kind: RequestBand
metadata: { name: sui-request-cpu, namespace: cb }
spec:
  targetRef: { apiVersion: apps/v1, kind: Deployment, name: sui }
  resource: cpu
  durability: committed
  cooldownSeconds: 600
  disruptionPolicy: restartFreeOnly
  maxStalenessSeconds: 120
  mode: shadow
",
        )
        .expect("the chart's minimal RequestBand must parse");
        assert_eq!(b.spec.mode, Some(PromotionMode::Shadow), "one boolean must not produce a live band");
        assert_eq!(
            b.spec.durable_coordinate().unwrap_err(),
            breathe_provider::ClassTransitionBlocked::NoManifestCoordinate
        );
    }

    /// The request-only status projection is OPTIONAL on the shared
    /// `BandStatus`, so every other band kind serializes exactly as before.
    /// Pinned because adding a required field here would change the on-wire
    /// status of all ten existing dimensions at once.
    #[test]
    fn the_request_status_projection_is_absent_on_every_other_kind() {
        let s = BandStatus { phase: Some("Holding".into()), ..Default::default() };
        let v = serde_json::to_value(&s).unwrap();
        for f in ["qosObserved", "qosGap", "pendingProposal"] {
            assert!(v.get(f).is_none(), "{f} must serialize away when unset");
        }
    }

    /// The RESERVATION dimension is a k8s-plane kind, not a host one — pinned
    /// because `is_host` used to be a `matches!`, which answered `false` for any
    /// new variant with no compile error. The answer is right here; the point is
    /// that it is now CHOSEN rather than defaulted.
    #[test]
    fn the_request_dimension_is_k8s_plane() {
        assert!(!breathe_provider::DimensionId::Request.is_host());
        assert_eq!(breathe_provider::DimensionId::Request.as_str(), "request");
    }

    /// The layout a RequestBand carves is the REQUEST peer of `PodResize`, and it
    /// carries the band's container through.
    #[test]
    fn the_request_band_carves_the_request_layout() {
        let b: RequestBand = serde_yaml::from_str(
            r"
apiVersion: breathe.pleme.io/v1
kind: RequestBand
metadata: { name: b, namespace: n }
spec:
  targetRef: { kind: Deployment, name: d, apiVersion: apps/v1, container: app }
  resource: cpu
",
        )
        .expect("deserializes");
        match b.spec.provider_layout() {
            LimitLayout::PodRequestResize { container } => assert_eq!(container.as_deref(), Some("app")),
            other => panic!("a RequestBand must carve PodRequestResize, got {other:?}"),
        }
    }
}

/// Schema for an open JSON value that Kubernetes will actually ACCEPT.
///
/// `Option<serde_json::Value>` makes schemars emit an EMPTY schema — no `type`
/// keyword at all, since the value may be anything. Kubernetes' STRUCTURAL
/// SCHEMA rules reject that outright:
///
/// ```text
/// CustomResourceDefinition "requestbands.breathe.pleme.io" is invalid:
///   status.properties[pendingProposal].type: Required value:
///     must not be empty for specified object fields
/// ```
///
/// Every property must carry a `type` UNLESS it is explicitly marked as
/// preserving unknown fields. This helper emits both, so the field stays open
/// while the CRD stays appliable.
///
/// COST OF NOT HAVING THIS, measured 2026-07-28: chart 0.1.32 published and was
/// completely unappliable — helm-controller never got past the CRD stage, so a
/// bump intended to restore `writeIntent`/`effectiveGate` applied NOTHING, and
/// the failure looked like the 7th in an unrelated upgrade-flakiness streak.
/// ALL ELEVEN band kinds carry these same two fields, so this was never a
/// RequestBand bug — fixing only the kind that surfaced it would have moved the
/// failure, not removed it.
///
/// DESTINATION, not the end state: both fields' own doc comments describe a
/// KNOWN shape (`qosGap` is documented as `held | promotionProposed |
/// blocked(<why>)`). An open value is the honest M0 form while nothing
/// populates them — verified: no assignment to either field exists anywhere in
/// this workspace. Typing them properly is the real fix and is deliberately not
/// done here, because the surrounding comment in this file records that a second
/// status type would ripple through breathe-runtime's status mapping, the facade
/// and the gate matrix. Revisit when something first writes one.
fn open_json_schema(_: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type": "object",
        "x-kubernetes-preserve-unknown-fields": true,
    }))
    .expect("static open-JSON schema is well-formed")
}

#[cfg(test)]
mod structural_schema_tests {
    //! Kubernetes STRUCTURAL SCHEMA validity, checked without a cluster.
    //!
    //! WHY THIS EXISTS. chart 0.1.32 was published and was completely
    //! unappliable: `status.pendingProposal` and `status.qosGap` carried no
    //! `type`, which the apiserver rejects, so helm-controller never got past
    //! the CRD stage and a bump meant to restore `writeIntent`/`effectiveGate`
    //! applied NOTHING.
    //!
    //! The existing `crd-drift` CI job could not catch it, and that is the
    //! important part: it compares crdgen output against the committed chart
    //! YAML, so BOTH SIDES AGREED AND BOTH WERE WRONG. A gate that compares two
    //! renderings of the same defect is not a gate. This asserts a property of
    //! the schema itself instead.
    //!
    //! Deliberately a unit test, not a CI apiserver: it needs no cluster, no
    //! kind/k3d spin-up and no network, so it runs in the ordinary suite on
    //! every push — the cheapest place a seal can live is the place that always
    //! runs.

    use super::*;

    /// Walk a schema and collect every property path lacking a `type`, unless
    /// it is explicitly open (`x-kubernetes-preserve-unknown-fields`) or a
    /// `$ref`. Mirrors the apiserver rule that actually fired.
    fn untyped_properties(node: &serde_json::Value, path: &str, out: &mut Vec<String>) {
        if let Some(props) = node.get("properties").and_then(|p| p.as_object()) {
            for (k, v) in props {
                let p2 = [path, k].join(".");
                let typed = v.get("type").is_some();
                let open = v
                    .get("x-kubernetes-preserve-unknown-fields")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true);
                let is_ref = v.get("$ref").is_some();
                if !typed && !open && !is_ref {
                    out.push(p2.clone());
                }
                untyped_properties(v, &p2, out);
            }
        }
        if let Some(items) = node.get("items") {
            untyped_properties(items, &[path, "[]"].concat(), out);
        }
    }

    #[test]
    fn every_generated_crd_is_structurally_valid() {
        use kube::CustomResourceExt;
        // Every CRD crdgen emits. Kept in step with src/bin/crdgen.rs; a kind
        // added there and forgotten here is a gap, so the count is asserted
        // below rather than left implicit.
        let crds: Vec<(&str, _)> = vec![
            ("MemoryBand", MemoryBand::crd()),
            ("CpuBand", CpuBand::crd()),
            ("StorageBand", StorageBand::crd()),
            ("ReplicaBand", ReplicaBand::crd()),
            ("ArcBand", ArcBand::crd()),
            ("CgroupBand", CgroupBand::crd()),
            ("CgroupCpuBand", CgroupCpuBand::crd()),
            ("HostParamBand", HostParamBand::crd()),
            ("KubeParamBand", KubeParamBand::crd()),
            ("AppBand", AppBand::crd()),
            ("RequestBand", RequestBand::crd()),
        ];
        assert_eq!(
            crds.len(), 11,
            "all 11 band kinds must be covered -- every one of them carries the \
             pendingProposal/qosGap pair, so a kind omitted here is a kind whose \
             schema is unchecked"
        );
        let mut offenders = Vec::new();
        for (kind, crd) in &crds {
            let v = serde_json::to_value(crd).expect("CRD serializes");
            for ver in v["spec"]["versions"].as_array().into_iter().flatten() {
                let name = ver["name"].as_str().unwrap_or("?");
                let mut bad = Vec::new();
                untyped_properties(&ver["schema"]["openAPIV3Schema"], "", &mut bad);
                offenders.extend(bad.into_iter().map(|p| [*kind, " ", name, p.as_str()].concat()));
            }
        }
        assert!(
            offenders.is_empty(),
            "{} propert(ies) have no `type` and are not marked \
             x-kubernetes-preserve-unknown-fields. The apiserver REJECTS the whole \
             CRD for this -- it is what made chart 0.1.32 unappliable. Fix the Rust \
             type (an Option<serde_json::Value> needs #[schemars(schema_with = \
             \"open_json_schema\")]), never the generated YAML, which crdgen \
             overwrites:\n  - {}",
            offenders.len(),
            offenders.join("\n  - ")
        );
    }

    #[test]
    fn the_check_actually_detects_an_untyped_property() {
        // RED RUN, welded in. A gate never observed failing may be checking
        // nothing -- so prove the detector fires on the exact shape that got
        // through: a property with neither `type` nor preserve-unknown-fields.
        let schema = serde_json::json!({
            "properties": {
                "status": { "type": "object", "properties": {
                    "fine":   { "type": "string" },
                    "broken": { "description": "no type, not open -- the 0.1.32 defect" },
                    "open":   { "type": "object", "x-kubernetes-preserve-unknown-fields": true }
                }}
            }
        });
        let mut bad = Vec::new();
        untyped_properties(&schema, "", &mut bad);
        assert_eq!(bad, vec![".status.broken".to_string()],
            "the detector must flag exactly the untyped property, and must NOT \
             flag a typed one or an explicitly-open one");
    }
}

// ---------------------------------------------------------------------------
// BreathePolicy — selector-based band auto-enrollment.
//
// The destination named in `k8s/clusters/rio/.../generate-bands.rb`'s own header
// ("a selector-based auto-enroll ... that materializes a band per matching
// workload and auto-extends to every new one") and never built. The interim
// generator ossified for months, and the measurement on camelot-eks 2026-08-05
// is what it cost: 115 bands across 2 of 11 dimensions, `requestbands: 0` while
// 10.9 vCPU (17% of the cluster) sat reserved-and-unused, and 23% of bands
// pointed at workloads that no longer exist.
//
// The decision logic deliberately does NOT live here. `breathe-discovery` owns
// `WorkloadShape -> {BandDimension}` as a pure total function, so the whole
// decision table is unit-tested without a cluster; this CRD is the operator's
// declaration of WHERE to apply it, and the controller is the I/O between them.
// ---------------------------------------------------------------------------

/// Which workloads a [`BreathePolicy`] enrolls.
///
/// A **selector, never a list.** A list is what the generator emitted, and a list
/// can only ever encode what its author knew on the day they ran it — that is the
/// root cause of every defect above, so it is the one shape this type refuses to
/// offer.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct EnrollmentSelector {
    /// Namespaces to enroll. Empty ⇒ every namespace the controller can read.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub namespaces: Vec<String>,
    /// Namespaces to exclude even when matched above.
    ///
    /// Present because the useful policy is nearly always "everything except the
    /// few places carving is unsafe", and expressing that as an allow-list
    /// reintroduces the hand-list this type exists to eliminate.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude_namespaces: Vec<String>,
    /// Workload label selector (`k=v,k2=v2`). Empty ⇒ all workloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_labels: Option<String>,
}

/// How a policy arms the bands it materializes.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EnrollmentArming {
    /// The `writeIntent` stamped on newly materialized bands.
    ///
    /// Defaults to `calibrateThenWrite`, NOT `observe`, and the difference is the
    /// entire point. Observe never promotes itself: camelot ran 115 bands at
    /// `mode: shadow` with `writeIntent: None` indefinitely, deciding correctly
    /// every tick and applying none of it, while the cluster sat at 30% requested
    /// and 13% used. Safety comes from the calibration gate — a band still
    /// refuses to write until its own observation window is clean — not from
    /// never arming at all.
    #[serde(default = "default_initial_intent")]
    pub initial_intent: String,
    /// Seconds of clean observation before a calibrating band promotes itself.
    #[serde(default = "default_confirm_after")]
    pub confirm_after_seconds: u32,
    /// Who authorized writing. Required when `initialIntent` is `write`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorized_by: Option<String>,
    /// Dimensions this policy refuses to arm, by `BandDimension` name
    /// (`RequestCpu`, `Memory`, …). Materialized frozen, so they observe and
    /// report but never write.
    ///
    /// Data rather than a code path, so a refusal is visible in the policy an
    /// operator reads instead of buried in a controller special case.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub never_arm: Vec<String>,
}

fn default_initial_intent() -> String {
    "calibrateThenWrite".to_owned()
}
const fn default_confirm_after() -> u32 {
    3600
}

impl Default for EnrollmentArming {
    fn default() -> Self {
        Self {
            initial_intent: default_initial_intent(),
            confirm_after_seconds: default_confirm_after(),
            authorized_by: None,
            never_arm: Vec::new(),
        }
    }
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "BreathePolicy",
    status = "BreathePolicyStatus",
    shortname = "bpol",
    category = "breathe",
    printcolumn = r#"{"name":"Matched","type":"integer","jsonPath":".status.workloadsMatched"}"#,
    printcolumn = r#"{"name":"Desired","type":"integer","jsonPath":".status.bandsDesired"}"#,
    printcolumn = r#"{"name":"Created","type":"integer","jsonPath":".status.bandsCreated"}"#,
    printcolumn = r#"{"name":"Adopted","type":"integer","jsonPath":".status.bandsAdopted"}"#,
    printcolumn = r#"{"name":"Retired","type":"integer","jsonPath":".status.bandsRetired"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BreathePolicySpec {
    /// Which workloads to enroll.
    #[serde(default)]
    pub selector: EnrollmentSelector,
    /// How to arm what gets materialized.
    #[serde(default)]
    pub arming: EnrollmentArming,
    /// The `BreathePosture` supplying materialized bands' unset behavioural fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posture_ref: Option<String>,
    /// Plan and report, but materialize nothing.
    ///
    /// The honest first rung for a new policy: an operator sees exactly which
    /// bands WOULD be created, on which dimensions, before any object is written.
    /// Distinct from a band's own shadow mode — this gates *enrollment*, that
    /// gates *carving*.
    #[serde(default)]
    pub plan_only: bool,
    /// Stop reconciling entirely, leaving existing bands untouched.
    ///
    /// Suspension must never cascade into mass retirement: the bands this policy
    /// created stay, keep their observation history, and keep deciding. MODULARIZE,
    /// DON'T DELETE, at the policy level.
    #[serde(default)]
    pub suspend: bool,
}

/// What the last reconcile of a [`BreathePolicy`] did.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct BreathePolicyStatus {
    /// Coarse state: `Reconciled`, `PlanOnly`, `Suspended`, `Error`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Workloads the selector matched.
    #[serde(default)]
    pub workloads_matched: u32,
    /// Bands the derivation says should exist.
    #[serde(default)]
    pub bands_desired: u32,
    /// Bands created this reconcile.
    #[serde(default)]
    pub bands_created: u32,
    /// Pre-existing bands adopted (owner reference attached).
    #[serde(default)]
    pub bands_adopted: u32,
    /// Bands retired because their dimension is no longer warranted.
    #[serde(default)]
    pub bands_retired: u32,
    /// Workloads skipped because no UID could be observed, so no collectable
    /// owner reference could be built.
    ///
    /// Reported rather than silently dropped: a nonzero value here means the
    /// enrolled set is smaller than the selector implies, which is exactly the
    /// kind of quiet shortfall that reads as "working" from the outside.
    #[serde(default)]
    pub workloads_unobservable: u32,
    /// Distinct dimensions in play, as `BandDimension` names.
    ///
    /// CATALOG REFLECTION: the surface self-describes, so an operator counts
    /// dimensions from the status rather than from a directory listing — the
    /// habit that let `requestbands: 0` go unnoticed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dimensions: Vec<String>,
    /// RFC3339 timestamp of the last successful reconcile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reconciled: Option<String>,
    /// Standard conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}
