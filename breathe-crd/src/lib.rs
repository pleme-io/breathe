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
    /// The band's CURRENT status (read before reconcile) — the `prior` that
    /// `status_for` carries cumulative counters + the cooldown epoch forward from.
    fn status(&self) -> Option<&BandStatus>;
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
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_kinds_share_band_config_parse() {
        // each kind constructs a valid BandConfig from its spec
        let tr = TargetRef { kind: "Cluster".into(), name: "x".into(), api_version: None, container: None };
        let mem = MemoryBand::new("m", MemoryBandSpec {
            target_ref: tr.clone(), setpoint: 0.80, grow_above: 0.85, shrink_below: 0.70,
            grow_factor: 1.25, shrink_factor: 0.90, floor: "512Mi".into(), ceiling: "4Gi".into(),
            cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: true, disruption_policy: Default::default(),
        });
        let cfg = Band::band_config(&mem).unwrap();
        assert_eq!(cfg.floor_bytes, 512 * (1 << 20));
        assert!(mem.dry_run());
    }

    #[test]
    fn cpu_band_parses_floor_ceiling_as_millicores() {
        let tr = TargetRef { kind: "Cluster".into(), name: "db".into(), api_version: None, container: None };
        let cpu = CpuBand::new("c", CpuBandSpec {
            target_ref: tr, setpoint: 0.80, grow_above: 0.85, shrink_below: 0.70,
            grow_factor: 1.25, shrink_factor: 0.90, floor: "250m".into(), ceiling: "2".into(),
            cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: false, disruption_policy: Default::default(),
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
        let tr = TargetRef { kind: "Node".into(), name: "rio".into(), api_version: None, container: None };
        let arc = ArcBand::new("rio-arc", ArcBandSpec {
            target_ref: tr, setpoint: 0.80, grow_above: 0.85, shrink_below: 0.70,
            grow_factor: 1.25, shrink_factor: 0.90, floor: "1Gi".into(), ceiling: "6Gi".into(),
            cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: true, disruption_policy: Default::default(),
        });
        let cfg = Band::band_config(&arc).unwrap();
        assert_eq!(cfg.floor_bytes, 1 << 30);
        assert_eq!(cfg.ceiling_bytes, 6 * (1 << 30));
        assert!(arc.dry_run());

        // CgroupBand: the target NAME is the systemd unit the agent addresses.
        let g = CgroupBand::new("nix-daemon", CgroupBandSpec {
            target_ref: TargetRef { kind: "HostUnit".into(), name: "nix-daemon.service".into(), api_version: None, container: None },
            setpoint: 0.80, grow_above: 0.85, shrink_below: 0.70, grow_factor: 1.25, shrink_factor: 0.90,
            floor: "1Gi".into(), ceiling: "12Gi".into(), cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: true, disruption_policy: Default::default(),
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
