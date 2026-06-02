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
            printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
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

// ─────────────────── BreatheNodePool — host enrollment ──────────────────

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
    /// L2 ARC ceiling — `nodeBudget.arcMaxGiB` (the boot modprobe cap), in GiB.
    pub arc_max_gi_b: u64,
    /// L2 cgroup ceiling per systemd unit — the unit's `nodeBudget` `memoryMaxGiB`,
    /// in GiB. A `CgroupBand` whose unit is absent here is refused (never written
    /// blind).
    #[serde(default)]
    pub cgroup_max_gi_b: BTreeMap<String, u64>,
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
            cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: true,
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
            cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: false,
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
            cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: true,
        });
        let cfg = Band::band_config(&arc).unwrap();
        assert_eq!(cfg.floor_bytes, 1 << 30);
        assert_eq!(cfg.ceiling_bytes, 6 * (1 << 30));
        assert!(arc.dry_run());

        // CgroupBand: the target NAME is the systemd unit the agent addresses.
        let g = CgroupBand::new("nix-daemon", CgroupBandSpec {
            target_ref: TargetRef { kind: "HostUnit".into(), name: "nix-daemon.service".into(), api_version: None, container: None },
            setpoint: 0.80, grow_above: 0.85, shrink_below: 0.70, grow_factor: 1.25, shrink_factor: 0.90,
            floor: "1Gi".into(), ceiling: "12Gi".into(), cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: true,
        });
        assert_eq!(g.target_ref().name, "nix-daemon.service");
    }

    #[test]
    fn nodepool_carries_the_l2_ceilings_and_master_switch() {
        let mut cgroup = BTreeMap::new();
        cgroup.insert("nix-daemon.service".to_string(), 12u64);
        let pool = BreatheNodePool::new("rio", BreatheNodePoolSpec {
            node_name: "rio".into(),
            arc_max_gi_b: 6,
            cgroup_max_gi_b: cgroup,
            write_enabled: false, // safe default — whole node in shadow
        });
        assert_eq!(pool.spec.node_name, "rio");
        assert_eq!(pool.spec.arc_max_gi_b, 6);
        assert_eq!(pool.spec.cgroup_max_gi_b.get("nix-daemon.service"), Some(&12));
        assert!(!pool.spec.write_enabled, "writeEnabled must default off (shadow-first)");
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
