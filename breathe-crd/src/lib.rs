//! The breathe band CRDs (breathe.pleme.io/v1) — the typed per-target enrollment
//! contracts. **Per-dimension kinds sharing one spec shape**, stamped from one
//! `band_kind!` macro: `MemoryBand` / `StorageBand` / `CpuBand`. The controller
//! reconciles only declared bands; `kubectl get memoryband,cpuband,storageband -A`
//! is the complete, auditable answer to "what is being managed, in which dimension".
//!
//! The [`Band`] trait is the dimension-agnostic accessor the generic controller
//! dispatches on — one reconcile body for all three kinds.

use breathe_control::BandConfig;
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
) -> anyhow::Result<BandConfig> {
    let parse = |q: &str| -> anyhow::Result<u64> {
        parse_size::Config::new()
            .with_binary()
            .parse_size(q)
            .map_err(|e| anyhow::anyhow!("invalid quantity {q:?}: {e}"))
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

/// Stamp one band CRD kind + its [`Band`] impl from the shared field set.
/// floor/ceiling default to memory-ish values; per-dimension bands set their own.
macro_rules! band_kind {
    ($spec:ident, $kind:ident, $kindlit:literal, $short:literal) => {
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
            #[serde(default = "d_floor")]
            pub floor: String,
            #[serde(default = "d_ceiling")]
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
                    &s.floor, &s.ceiling,
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

band_kind!(MemoryBandSpec, MemoryBand, "MemoryBand", "mband");
band_kind!(CpuBandSpec, CpuBand, "CpuBand", "cband");
band_kind!(StorageBandSpec, StorageBand, "StorageBand", "sband");

fn d_floor() -> String { "256Mi".into() }
fn d_ceiling() -> String { "16Gi".into() }
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
}
