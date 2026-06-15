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

use breathe_control::{BandConfig, Unit};
use breathe_provider::DisruptionPolicy;
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
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct BandStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
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
    /// The observed `capacity` (the current limit the util is measured against).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_capacity: Option<i64>,
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
    /// M0 PREDICTIVE: `Some(lookahead_secs)` when the band opts into preemptive
    /// carving (`predictive: true`) — the controller measures the working-set
    /// velocity and feeds `PredictiveGrow` so the limit pre-grows for the burst
    /// the instantaneous reading misses. `None` (default) ⇒ plain reactive carving.
    fn predictive(&self) -> Option<f64>;
    /// `metadata.generation` — set as `status.observedGeneration` so an operator can
    /// confirm the controller reconciled their latest spec edit.
    fn generation(&self) -> Option<i64> {
        self.meta().generation
    }
}

fn band_config_of(
    setpoint: f64,
    grow_above: f64,
    shrink_below: f64,
    grow_factor: f64,
    shrink_factor: f64,
    floor: &str,
    ceiling: &str,
    unit: Unit,
) -> anyhow::Result<BandConfig> {
    let parse = |q: &str| -> anyhow::Result<u64> {
        unit.parse(q)
            .ok_or_else(|| anyhow::anyhow!("invalid {unit:?} quantity {q:?}"))
    };
    Ok(BandConfig {
        grow_above,
        shrink_below,
        setpoint,
        grow_factor,
        shrink_factor,
        floor_bytes: parse(floor)?,
        ceiling_bytes: parse(ceiling)?,
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
        }

        impl crate::Band for $kind {
            fn target_ref(&self) -> &TargetRef {
                &self.spec.target_ref
            }
            fn band_config(&self) -> anyhow::Result<BandConfig> {
                let s = &self.spec;
                crate::band_config_of(
                    s.setpoint, s.grow_above, s.shrink_below, s.grow_factor, s.shrink_factor,
                    &s.floor, &s.ceiling, $unit,
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
band_kind!(StorageBandSpec, StorageBand, "StorageBand", "sband", Unit::Bytes, "d_floor_bytes", "d_ceiling_bytes");
// HOST bands — the descriptor (breathe-host) encodes the host addressing; the
// CRD shape is identical to the byte-valued k8s bands, so the same macro stamps
// them. targetRef.name carries the systemd unit (CgroupBand) or the node
// (ArcBand); the agent applies via HostCluster within the BreatheNodePool L2 ceiling.
band_kind!(ArcBandSpec, ArcBand, "ArcBand", "aband", Unit::Bytes, "d_floor_bytes", "d_ceiling_bytes");
band_kind!(CgroupBandSpec, CgroupBand, "CgroupBand", "gband", Unit::Bytes, "d_floor_bytes", "d_ceiling_bytes");
// HOST cpu band — the unit's transient CPUQuota cap, millicores (like CpuBand).
band_kind!(CgroupCpuBandSpec, CgroupCpuBand, "CgroupCpuBand", "gcband", Unit::Millicores, "d_floor_milli", "d_ceiling_milli");

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
    printcolumn = r#"{"name":"DryRun","type":"boolean","jsonPath":".spec.dryRun"}"#
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

fn d_floor_bytes() -> String { "256Mi".into() }
fn d_ceiling_bytes() -> String { "16Gi".into() }
fn d_floor_milli() -> String { "250m".into() }
fn d_ceiling_milli() -> String { "2".into() }
fn d_setpoint() -> f64 { 0.80 }
fn d_grow_above() -> f64 { 0.85 }
fn d_shrink_below() -> f64 { 0.70 }
fn d_grow_factor() -> f64 { 1.25 }
fn d_shrink_factor() -> f64 { 0.90 }
fn d_cooldown() -> u64 { 600 }
fn d_max_staleness() -> u64 { 120 }
fn d_predictive_lookahead() -> u64 { 60 }
fn d_relief_latency() -> u64 { 180 } // ~3min node boot→Ready (the NodeOnDemand dead-time)

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
            cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: true, disruption_policy: Default::default(), suspend: false, force_limit: None, force_limit_expiry: None, predictive: false, predictive_lookahead_seconds: 60,
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
            cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: false, disruption_policy: Default::default(), suspend: false, force_limit: None, force_limit_expiry: None, predictive: false, predictive_lookahead_seconds: 60,
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
            &d_floor_milli(), &d_ceiling_milli(), Unit::Millicores).unwrap();
        assert_eq!(cfg.floor_bytes, 250);
        assert_eq!(cfg.ceiling_bytes, 2000);
    }

    #[test]
    fn host_bands_share_the_band_shape_and_parse_bytes() {
        // ArcBand: the target is the node; floor/ceiling are byte quantities.
        let tr = TargetRef { kind: "Node".into(), name: "rio".into(), api_version: None, container: None, pod_selector: None };
        let arc = ArcBand::new("rio-arc", ArcBandSpec {
            target_ref: tr, setpoint: 0.80, grow_above: 0.85, shrink_below: 0.70,
            grow_factor: 1.25, shrink_factor: 0.90, floor: "1Gi".into(), ceiling: "6Gi".into(),
            cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: true, disruption_policy: Default::default(), suspend: false, force_limit: None, force_limit_expiry: None, predictive: false, predictive_lookahead_seconds: 60,
        });
        let cfg = Band::band_config(&arc).unwrap();
        assert_eq!(cfg.floor_bytes, 1 << 30);
        assert_eq!(cfg.ceiling_bytes, 6 * (1 << 30));
        assert!(arc.dry_run());

        // CgroupBand: the target NAME is the systemd unit the agent addresses.
        let g = CgroupBand::new("nix-daemon", CgroupBandSpec {
            target_ref: TargetRef { kind: "HostUnit".into(), name: "nix-daemon.service".into(), api_version: None, container: None, pod_selector: None },
            setpoint: 0.80, grow_above: 0.85, shrink_below: 0.70, grow_factor: 1.25, shrink_factor: 0.90,
            floor: "1Gi".into(), ceiling: "12Gi".into(), cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: true, disruption_policy: Default::default(), suspend: false, force_limit: None, force_limit_expiry: None, predictive: false, predictive_lookahead_seconds: 60,
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
}
