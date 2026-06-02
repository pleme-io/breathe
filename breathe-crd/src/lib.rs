//! `MemoryBand` — the typed per-target enrollment contract (breathe.pleme.io/v1).
//!
//! The controller reconciles **only** declared `MemoryBand` objects; nothing
//! implicit. `kubectl get memoryband -A` is the complete, auditable answer to
//! "what is having its memory managed?". The per-target band policy lives in the
//! typed spec, so two targets can carry different floors / ceilings / setpoints.
//! `StorageBand`/`CpuBand` (M2) share the [`BandSpec`] shape; only the kind differs.

use breathe_control::BandConfig;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The workload owner whose container memory limit this band controls. For CNPG
/// the kind is `Cluster` and the patched field lives on the `Cluster` CR (which
/// the CNPG operator propagates to its pods) — see BREATHE.md §15.5.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TargetRef {
    /// `Deployment` | `StatefulSet` | `Cluster` (CNPG). Drives the apiVersion.
    pub kind: String,
    /// The owner's metadata.name (same namespace as the MemoryBand).
    pub name: String,
    /// Optional explicit apiVersion override (e.g. `postgresql.cnpg.io/v1`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    /// Container within the pod template; `None` = the first container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "breathe.pleme.io",
    version = "v1",
    kind = "MemoryBand",
    namespaced,
    status = "MemoryBandStatus",
    shortname = "mband",
    category = "breathe",
    printcolumn = r#"{"name":"Target","type":"string","jsonPath":".spec.targetRef.kind"}"#,
    printcolumn = r#"{"name":"Name","type":"string","jsonPath":".spec.targetRef.name"}"#,
    printcolumn = r#"{"name":"Util","type":"string","jsonPath":".status.lastUtil"}"#,
    printcolumn = r#"{"name":"Limit","type":"string","jsonPath":".status.currentLimit"}"#,
    printcolumn = r#"{"name":"Last","type":"string","jsonPath":".status.lastDecision"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct MemoryBandSpec {
    /// The workload owner whose memory limit this band controls.
    pub target_ref: TargetRef,

    /// Target utilization setpoint (used / limit). Default `0.80` (80/20).
    #[serde(default = "d_setpoint")]
    pub setpoint: f64,
    /// Grow when utilization is strictly above this. Default `0.85`.
    #[serde(default = "d_grow_above")]
    pub grow_above: f64,
    /// Shrink when utilization is strictly below this. Default `0.70`.
    #[serde(default = "d_shrink_below")]
    pub shrink_below: f64,
    /// Limit multiplier on grow. Default `1.25`.
    #[serde(default = "d_grow_factor")]
    pub grow_factor: f64,
    /// Limit multiplier on shrink (gentle). Default `0.90`.
    #[serde(default = "d_shrink_factor")]
    pub shrink_factor: f64,

    /// Never shrink below this. Kubernetes quantity. Default `256Mi`.
    #[serde(default = "d_floor")]
    pub floor: String,
    /// Never grow above this. Kubernetes quantity. Default `16Gi`.
    #[serde(default = "d_ceiling")]
    pub ceiling: String,

    /// Minimum seconds between limit changes for this target. Default `600`.
    #[serde(default = "d_cooldown")]
    pub cooldown_seconds: u64,

    /// Max acceptable metric sample age before a mutation is refused. Default `120`.
    #[serde(default = "d_max_staleness")]
    pub max_staleness_seconds: u64,

    /// Observe-and-attest only; never mutate (the shadow window). Default `false`.
    #[serde(default)]
    pub dry_run: bool,
}

/// Typed status — the per-cycle receipt for this band.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct MemoryBandStatus {
    /// `Holding` | `Growing` | `Shrinking` | `AtCeiling` | `Cooldown` |
    /// `Conflict` | `Stale` | `NoLimit` | `TargetNotFound` | `MetricsMissing`.
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
    /// When phase == Conflict: the competing field-manager we yielded to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_manager: Option<String>,
}

impl MemoryBandSpec {
    /// Build the pure [`BandConfig`] from this CR's typed spec, parsing the
    /// floor/ceiling quantities. Errors surface as a typed result rather than a
    /// panic, so a bad CR is skipped + reported, never crashes the tick.
    pub fn band_config(&self) -> anyhow::Result<BandConfig> {
        let parse = |q: &str| -> anyhow::Result<u64> {
            parse_size::Config::new()
                .with_binary()
                .parse_size(q)
                .map_err(|e| anyhow::anyhow!("invalid quantity {q:?}: {e}"))
        };
        Ok(BandConfig {
            grow_above: self.grow_above,
            shrink_below: self.shrink_below,
            setpoint: self.setpoint,
            grow_factor: self.grow_factor,
            shrink_factor: self.shrink_factor,
            floor_bytes: parse(&self.floor)?,
            ceiling_bytes: parse(&self.ceiling)?,
        })
    }
}

fn d_setpoint() -> f64 { 0.80 }
fn d_grow_above() -> f64 { 0.85 }
fn d_shrink_below() -> f64 { 0.70 }
fn d_grow_factor() -> f64 { 1.25 }
fn d_shrink_factor() -> f64 { 0.90 }
fn d_floor() -> String { "256Mi".into() }
fn d_ceiling() -> String { "16Gi".into() }
fn d_cooldown() -> u64 { 600 }
fn d_max_staleness() -> u64 { 120 }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn band_config_parses_quantities() {
        let spec = MemoryBandSpec {
            target_ref: TargetRef { kind: "Cluster".into(), name: "pangea-database".into(), api_version: Some("postgresql.cnpg.io/v1".into()), container: None },
            setpoint: 0.80, grow_above: 0.85, shrink_below: 0.70, grow_factor: 1.25, shrink_factor: 0.90,
            floor: "512Mi".into(), ceiling: "4Gi".into(), cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: true,
        };
        let cfg = spec.band_config().unwrap();
        assert_eq!(cfg.floor_bytes, 512 * (1 << 20));
        assert_eq!(cfg.ceiling_bytes, 4 * (1 << 30));
        assert_eq!(cfg.setpoint, 0.80);
    }

    #[test]
    fn bad_quantity_errors_not_panics() {
        let mut spec = MemoryBandSpec {
            target_ref: TargetRef { kind: "Deployment".into(), name: "x".into(), api_version: None, container: None },
            setpoint: 0.80, grow_above: 0.85, shrink_below: 0.70, grow_factor: 1.25, shrink_factor: 0.90,
            floor: "not-a-size".into(), ceiling: "4Gi".into(), cooldown_seconds: 600, max_staleness_seconds: 120, dry_run: false,
        };
        assert!(spec.band_config().is_err());
        spec.floor = "256Mi".into();
        assert!(spec.band_config().is_ok());
    }
}
