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
use breathe_control::{BandConfig, MetricMissingPolicy, Unit};
use breathe_provider::{DisruptionPolicy, LimitLayout, MetricSource};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
    /// The DisruptionPolicy in effect for this band (`restartFreeOnly` / …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_policy: Option<String>,
    /// The effective mode: `true` = SHADOW (observe + attest, never carve).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_dry_run: Option<bool>,
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

/// The band's PROMOTION LIFECYCLE — the typed, configurable state controlling
/// whether (and when) breathe moves from observing (SHADOW) to carving (EFFECT).
/// The fleet default is `ShadowConfirmEffect`: no band is parked in permanent
/// shadow, and none goes live unconfirmed — it shadows until a clean-observation
/// window proves it's safe, then auto-begins.
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
/// satisfies a `ShadowConfirmEffect` band's confirm gate immediately.
pub const CONFIRMED_ANNOTATION: &str = "breathe.pleme.io/confirmed";

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
    fn dry_run(&self) -> bool;
    /// The band's explicit `mode`, if authored. `None` ⇒ derive in
    /// [`Band::promotion_mode`] (legacy `dryRun:true` → Shadow, else the default).
    fn mode_spec(&self) -> Option<PromotionMode>;
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
    fn confirm_gate_passed(&self, now_epoch: i64) -> bool {
        if self.operator_confirmed() {
            return true;
        }
        let Some(st) = self.status() else {
            return false;
        };
        let cond = |t: &str| st.conditions.iter().find(|c| c.type_ == t);
        let is_true = |t: &str| cond(t).is_some_and(|c| c.status == "True");
        if is_true("Stale") || is_true("Conflict") {
            return false;
        }
        match cond("Ready") {
            Some(r) if r.status == "True" => chrono::DateTime::parse_from_rfc3339(&r.last_transition_time)
                .map(|t| now_epoch - t.timestamp() >= self.confirm_after_seconds() as i64)
                .unwrap_or(false),
            _ => false,
        }
    }

    /// The EFFECTIVE dry-run for this tick, derived from the promotion lifecycle.
    /// THIS — not the raw `dryRun` field — is what gates the carve.
    fn effective_dry_run(&self, now_epoch: i64) -> bool {
        match self.promotion_mode() {
            PromotionMode::Effect => false,
            PromotionMode::Shadow | PromotionMode::Suspended => true,
            PromotionMode::ShadowConfirmEffect => !self.confirm_gate_passed(now_epoch),
        }
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
    ($spec:ident, $kind:ident, $kindlit:literal, $short:literal, $unit:expr, $dfloor:literal, $dceiling:literal) => {
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
            printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.conditions[?(@.type=='Ready')].status"}"#
        )]
        #[serde(rename_all = "camelCase")]
        pub struct $spec {
            pub target_ref: TargetRef,
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
            #[serde(default = $dfloor)]
            pub floor: String,
            #[serde(default = $dceiling)]
            pub ceiling: String,
            #[serde(default = "d_cooldown")]
            pub cooldown_seconds: u64,
            #[serde(default = "d_max_staleness")]
            pub max_staleness_seconds: u64,
            #[serde(default)]
            pub dry_run: bool,
            /// The PROMOTION LIFECYCLE. Unset ⇒ resolved by `promotion_mode()`
            /// (legacy `dryRun:true` → Shadow, else the fleet default
            /// `ShadowConfirmEffect`). Set explicitly to pin the state:
            /// `shadow` | `effect` | `shadowConfirmEffect` | `suspended`.
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub mode: Option<PromotionMode>,
            /// Clean-observation window (seconds) a `ShadowConfirmEffect` band holds
            /// Ready-and-healthy before it auto-promotes to carving (default 1800).
            #[serde(default = "d_confirm_after")]
            pub confirm_after_seconds: u64,
            /// The golden/ceiling gate (default `restartFreeOnly`). Omitted on
            /// serialize when default so the strict typed-gRPC surface stays safe.
            #[serde(default, skip_serializing_if = "breathe_provider::DisruptionPolicy::is_restart_free_only")]
            pub disruption_policy: DisruptionPolicy,
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
                    s.setpoint, s.grow_above, s.shrink_below, s.grow_factor, s.shrink_factor,
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
                self.spec.max_staleness_seconds
            }
            fn cooldown_seconds(&self) -> u64 {
                self.spec.cooldown_seconds
            }
            fn dry_run(&self) -> bool {
                self.spec.dry_run
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
        }
    };
}

band_kind!(MemoryBandSpec, MemoryBand, "MemoryBand", "mband", Unit::Bytes, "d_floor_bytes", "d_ceiling_bytes");
band_kind!(CpuBandSpec, CpuBand, "CpuBand", "cband", Unit::Millicores, "d_floor_milli", "d_ceiling_milli");
band_kind!(StorageBandSpec, StorageBand, "StorageBand", "sband", Unit::Bytes, "d_storage_floor_bytes", "d_storage_ceiling_bytes");
// HOST bands — the descriptor (breathe-host) encodes the host addressing; the
// CRD shape is identical to the byte-valued k8s bands, so the same macro stamps
// them. targetRef.name carries the systemd unit (CgroupBand) or the node
// (ArcBand); the agent applies via HostCluster within the BreatheNodePool L2 ceiling.
band_kind!(ArcBandSpec, ArcBand, "ArcBand", "aband", Unit::Bytes, "d_floor_bytes", "d_ceiling_bytes");
band_kind!(CgroupBandSpec, CgroupBand, "CgroupBand", "gband", Unit::Bytes, "d_floor_bytes", "d_ceiling_bytes");
// HOST cpu band — the unit's transient CPUQuota cap, millicores (like CpuBand).
band_kind!(CgroupCpuBandSpec, CgroupCpuBand, "CgroupCpuBand", "gcband", Unit::Millicores, "d_floor_milli", "d_ceiling_milli");

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
    #[serde(default)]
    pub dry_run: bool,
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
    #[serde(default)]
    pub dry_run: bool,
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
    #[serde(default)]
    pub dry_run: bool,
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
    #[serde(default)]
    pub dry_run: bool,
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
    printcolumn = r#"{"name":"Backend","type":"string","jsonPath":".spec.nodeProvisioningBackend"}"#
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
    /// The node(s) this band protects. Origin-guard declares exactly one
    /// hostname — this list IS "declare a node as origin".
    pub target_nodes: Vec<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_dry_run: Option<bool>,
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
            target_ref: tr.clone(), setpoint: 0.80, grow_above: 0.85, shrink_below: 0.70,
            grow_factor: 1.25, shrink_factor: 0.90, floor: "512Mi".into(), ceiling: "4Gi".into(),
            cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: true, disruption_policy: Default::default(), suspend: false, force_limit: None, force_limit_expiry: None, predictive: false, predictive_lookahead_seconds: 60, request_floor: String::new(), peak_decay: 0.98, mode: None, confirm_after_seconds: 1800, warmup_seconds: 600,
        });
        let cfg = Band::band_config(&mem).unwrap();
        assert_eq!(cfg.floor_bytes, 512 * (1 << 20));
        assert!(mem.dry_run());
    }

    #[test]
    fn cpu_band_parses_floor_ceiling_as_millicores() {
        let tr = TargetRef { kind: "Cluster".into(), name: "db".into(), api_version: None, container: None, pod_selector: None };
        let cpu = CpuBand::new("c", CpuBandSpec {
            target_ref: tr, setpoint: 0.80, grow_above: 0.85, shrink_below: 0.70,
            grow_factor: 1.25, shrink_factor: 0.90, floor: "250m".into(), ceiling: "2".into(),
            cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: false, disruption_policy: Default::default(), suspend: false, force_limit: None, force_limit_expiry: None, predictive: false, predictive_lookahead_seconds: 60, request_floor: String::new(), peak_decay: 0.98, mode: None, confirm_after_seconds: 1800, warmup_seconds: 600,
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
            target_ref: tr, setpoint: 0.80, grow_above: 0.85, shrink_below: 0.70,
            grow_factor: 1.25, shrink_factor: 0.90, floor: "1Gi".into(), ceiling: "6Gi".into(),
            cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: true, disruption_policy: Default::default(), suspend: false, force_limit: None, force_limit_expiry: None, predictive: false, predictive_lookahead_seconds: 60, request_floor: String::new(), peak_decay: 0.98, mode: None, confirm_after_seconds: 1800, warmup_seconds: 600,
        });
        let cfg = Band::band_config(&arc).unwrap();
        assert_eq!(cfg.floor_bytes, 1 << 30);
        assert_eq!(cfg.ceiling_bytes, 6 * (1 << 30));
        assert!(arc.dry_run());

        // CgroupBand: the target NAME is the systemd unit the agent addresses.
        let g = CgroupBand::new("nix-daemon", CgroupBandSpec {
            target_ref: TargetRef { kind: "HostUnit".into(), name: "nix-daemon.service".into(), api_version: None, container: None, pod_selector: None },
            setpoint: 0.80, grow_above: 0.85, shrink_below: 0.70, grow_factor: 1.25, shrink_factor: 0.90,
            floor: "1Gi".into(), ceiling: "12Gi".into(), cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: true, disruption_policy: Default::default(), suspend: false, force_limit: None, force_limit_expiry: None, predictive: false, predictive_lookahead_seconds: 60, request_floor: String::new(), peak_decay: 0.98, mode: None, confirm_after_seconds: 1800, warmup_seconds: 600,
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
            effective_dry_run: Some(false),
            last_seen_epoch: Some(1_000),
        };
        let js = serde_json::to_string(&found).unwrap();
        let back: IsolationBandStatus = serde_json::from_str(&js).unwrap();
        assert_eq!(found, back);
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
}
