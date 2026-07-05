//! `render` — the typed renderer that turns a [`BreatheDefaults`] preset into the
//! chart's `global.breathe` band values (the SINGLE SOURCE for the Camelot
//! posture overlay).
//!
//! The Camelot breathe posture used to live in TWO uncoupled places: the typed
//! [`crate::preset::CAMELOT`] preset (Rust) and a hand-authored `global.breathe`
//! block in `helmworks-akeyless/charts/lareira-akeyless-deployment/architectures/camelot.yaml`
//! (YAML). Two copies, no oracle — a RESOLVE-vs-REPLACE duplication where an edit
//! to one silently drifted from the other. This module removes the duplication:
//! the typed preset RENDERS the exact `global.breathe` block, a golden test pins
//! the render, and the chart carries a verbatim copy of that golden (the kata
//! parity oracle — the chart's `global.breathe.preset: camelot` names this source,
//! the explicit bands below it are this render's materialization).
//!
//! ## What this ADDS to the typed source
//!
//! The preset previously resolved only a single `vertical_setpoint`
//! ([`crate::preset::ResolvedBandPosture`]); nothing rendered a concrete band
//! block. This module renders the WHOLE band-set:
//!
//! - the **MemoryBand** (the `Hard` vertical dimension — OOM cliff, held at the
//!   80/20 setpoint),
//! - the **CpuBand** (the `Soft` vertical dimension — throttle, held at the same
//!   setpoint; previously absent from the typed render),
//! - the **ReplicaBand** (the horizontal dimension — HA floor + topology arm;
//!   previously a static `replicaCount`, never a band).
//!
//! ## TYPED EMISSION
//!
//! The YAML is emitted through a [`std::fmt::Display`] impl (the sanctioned typed
//! render surface), never `format!()` / string concat. The `Display` output IS
//! the serialization contract the golden pins.
//!
//! ## Tier-honest
//!
//! This is a rung-3 destination artifact: it renders the chart's band VALUES and
//! proves the collapse-to-one-source is lossless. It is NOT live band rendering —
//! the vendored `pleme-lib 0.16.0` has no MemoryBand/CpuBand/ReplicaBand template,
//! so both the preset and the chart block are inert against the live cluster
//! today. Live band emission (a `pleme-lib` band template that consumes
//! `global.breathe`, the breathe controller resolving `preset: camelot` directly,
//! and reaping the LIVE orphan `camelot-rabbitmq` band) are named LiveTODOs, never
//! rounded up.

use crate::preset::{BreatheDefaults, ResolvedBandPosture, WorkloadClass};
use core::fmt;

/// The fleet-DEFAULT workload class the `global.breathe` block's replica band is
/// rendered from. `global.breathe` is the fleet-wide default a subchart inherits;
/// the stateless-service class (interchangeable pods, `nonPersistent`, HA floor)
/// is that default. A stateful subchart (mysql/neo4j) overrides the topology per
/// class via [`render_workload_breathe`].
pub const DEFAULT_CLASS: WorkloadClass = WorkloadClass::StatelessService;

/// The vertical MemoryBand values — the `Hard` (OOM-cliff) dimension held at the
/// setpoint. Mirrors the chart's `global.breathe.memory` sub-block. Memory omits
/// an `enabled` flag: it is armed by `global.breathe.enabled` itself (memory is
/// the load-bearing vertical dimension — there is no breathe posture without it).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MemoryBand {
    pub setpoint: f64,
    pub dry_run: bool,
    pub mode: &'static str,
}

/// The vertical CpuBand values — the `Soft` (throttle) dimension held at the same
/// setpoint. Mirrors the chart's `global.breathe.cpu` sub-block. Carries its own
/// `enabled` (cpu breathing is separately toggleable from memory).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CpuBand {
    pub enabled: bool,
    pub setpoint: f64,
    pub dry_run: bool,
    pub mode: &'static str,
}

/// The horizontal ReplicaBand values — the HA floor + the topology arm the class
/// breathes under. Mirrors the chart's `global.breathe.replica` sub-block. This
/// replaces a static `replicaCount` with a typed band: a floor the workload never
/// rests below and the topology arm that picks the scale algorithm.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReplicaBand {
    pub enabled: bool,
    /// The at-rest replica floor (`≥ 2` for HA; a quorum class raises to `3`).
    pub floor: u32,
    /// The replica-topology `crd_kind` (`nonPersistent` / `masterSlave` /
    /// `persistent` / `fullyDistributed`).
    pub topology: &'static str,
    pub dry_run: bool,
    pub mode: &'static str,
}

/// The rendered `global.breathe` value block — the typed materialization of a
/// preset for one workload class. Its [`Display`] is the canonical YAML the chart
/// carries under `global.breathe`.
///
/// [`Display`]: std::fmt::Display
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GlobalBreatheValues {
    /// The source-of-truth preset name (`global.breathe.preset`). Names the typed
    /// [`BreatheDefaults`] this block was rendered from — the kata binding.
    pub preset: &'static str,
    /// `global.breathe.enabled` — the master toggle; always `true` for a rendered
    /// posture (a preset that renders nothing would not render this struct).
    pub enabled: bool,
    pub memory: MemoryBand,
    pub cpu: CpuBand,
    pub replica: ReplicaBand,
}

/// Render a preset's fleet-DEFAULT `global.breathe` block (the stateless-service
/// class — the fleet default a subchart inherits). The MemoryBand + CpuBand hold
/// the vertical setpoint; the ReplicaBand carries the HA floor + the default
/// (stateless) topology. Panics only if the preset has no profile for the default
/// class — a preset that cannot arm the fleet default is an authoring error the
/// bijection test already forbids, so this is unreachable in practice.
#[must_use]
pub fn render_global_breathe(preset: &BreatheDefaults) -> GlobalBreatheValues {
    render_workload_breathe(preset, DEFAULT_CLASS)
        .expect("a preset must arm the fleet-default (stateless-service) class")
}

/// Render a preset's `global.breathe` block for a SPECIFIC workload class — the
/// per-class ReplicaBand topology (mysql → `masterSlave`, neo4j → `persistent`, a
/// quorum store → `fullyDistributed`) and floor come from the class profile via
/// [`BreatheDefaults::resolve`]. `None` when the preset has no profile for the
/// class (a typed absence, never a silent wrong posture).
#[must_use]
pub fn render_workload_breathe(
    preset: &BreatheDefaults,
    class: WorkloadClass,
) -> Option<GlobalBreatheValues> {
    let posture: ResolvedBandPosture = preset.resolve(class)?;
    Some(GlobalBreatheValues {
        preset: preset.name,
        enabled: true,
        memory: MemoryBand {
            setpoint: posture.vertical_setpoint,
            dry_run: posture.dry_run,
            mode: posture.mode,
        },
        cpu: CpuBand {
            enabled: true,
            setpoint: posture.vertical_setpoint,
            dry_run: posture.dry_run,
            mode: posture.mode,
        },
        replica: ReplicaBand {
            enabled: true,
            floor: posture.replica_floor,
            topology: posture.replica_topology,
            dry_run: posture.dry_run,
            mode: posture.mode,
        },
    })
}

/// Render a bool as the YAML token (`true` / `false`).
fn yaml_bool(b: bool) -> &'static str {
    if b {
        "true"
    } else {
        "false"
    }
}

impl fmt::Display for GlobalBreatheValues {
    /// The canonical `global.breathe` YAML block (2-space indent). This IS the
    /// serialization contract the golden pins + the chart carries verbatim. The
    /// setpoint is a lossless-round-trip `f64` (`0.8` renders `0.8`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "preset: {}", self.preset)?;
        writeln!(f, "enabled: {}", yaml_bool(self.enabled))?;
        writeln!(f, "memory:")?;
        writeln!(f, "  setpoint: {}", self.memory.setpoint)?;
        writeln!(f, "  dryRun: {}", yaml_bool(self.memory.dry_run))?;
        writeln!(f, "  mode: {}", self.memory.mode)?;
        writeln!(f, "cpu:")?;
        writeln!(f, "  enabled: {}", yaml_bool(self.cpu.enabled))?;
        writeln!(f, "  setpoint: {}", self.cpu.setpoint)?;
        writeln!(f, "  dryRun: {}", yaml_bool(self.cpu.dry_run))?;
        writeln!(f, "  mode: {}", self.cpu.mode)?;
        writeln!(f, "replica:")?;
        writeln!(f, "  enabled: {}", yaml_bool(self.replica.enabled))?;
        writeln!(f, "  floor: {}", self.replica.floor)?;
        writeln!(f, "  topology: {}", self.replica.topology)?;
        writeln!(f, "  dryRun: {}", yaml_bool(self.replica.dry_run))?;
        write!(f, "  mode: {}", self.replica.mode)
    }
}

#[cfg(test)]
mod tests {
    use super::{render_global_breathe, render_workload_breathe, GlobalBreatheValues};
    use crate::preset::{WorkloadClass, CAMELOT};

    /// The rendered golden — the canonical `global.breathe` block the chart carries
    /// verbatim. THE KATA PARITY ORACLE: the typed preset render (the source) must
    /// equal the committed golden (the oracle-copy); an edit to the preset that
    /// changes the block fails here until the golden (and the chart) are re-synced.
    const GOLDEN: &str = include_str!("../goldens/camelot-global-breathe.golden.yaml");

    /// The render of the CAMELOT preset is BYTE-EQUAL to the committed golden. The
    /// golden is the exact `global.breathe` value block the chart's
    /// `architectures/camelot.yaml` carries — so a preset edit that would drift the
    /// chart is caught in CI here, not discovered live.
    #[test]
    fn render_matches_the_committed_golden() {
        let rendered = render_global_breathe(&CAMELOT).to_string();
        assert_eq!(
            rendered.trim_end(),
            GOLDEN.trim_end(),
            "the CAMELOT preset render drifted from the committed golden — \
             re-render and re-sync helmworks-akeyless architectures/camelot.yaml"
        );
    }

    /// The rendered block names the preset it came from — the kata binding
    /// (`global.breathe.preset: camelot`).
    #[test]
    fn render_names_its_source_preset() {
        let v = render_global_breathe(&CAMELOT);
        assert_eq!(v.preset, "camelot");
        assert!(v.to_string().contains("preset: camelot"));
    }

    /// The vertical bands (memory + cpu) both hold the aggressive 80/20 setpoint,
    /// and BOTH are rendered — the CpuBand the typed source previously omitted is
    /// now emitted.
    #[test]
    fn both_vertical_bands_hold_the_setpoint() {
        let v = render_global_breathe(&CAMELOT);
        assert!((v.memory.setpoint - 0.8).abs() < f64::EPSILON);
        assert!((v.cpu.setpoint - 0.8).abs() < f64::EPSILON);
        assert!(v.cpu.enabled, "the cpu band must be enabled");
        let s = v.to_string();
        assert!(s.contains("memory:"), "memory band rendered");
        assert!(s.contains("cpu:"), "cpu band rendered");
    }

    /// The ReplicaBand is rendered with the HA floor and the class's topology arm —
    /// the horizontal dimension the chart previously expressed only as a static
    /// `replicaCount`.
    #[test]
    fn replica_band_carries_the_ha_floor_and_topology() {
        let v = render_global_breathe(&CAMELOT);
        assert!(v.replica.enabled);
        assert_eq!(v.replica.floor, 2, "HA floor");
        assert_eq!(
            v.replica.topology, "nonPersistent",
            "the fleet-default replica band is the stateless (nonPersistent) topology"
        );
        assert!(v.to_string().contains("replica:"), "replica band rendered");
    }

    /// Every band is born SHADOW-FIRST (`dryRun: true`, `mode: shadow`) — the whole
    /// rendered block is observe-only, matching the preset's posture. Guards against
    /// a render that would silently emit a live-carving band.
    #[test]
    fn every_rendered_band_is_shadow_first() {
        let v = render_global_breathe(&CAMELOT);
        for (dry, mode) in [
            (v.memory.dry_run, v.memory.mode),
            (v.cpu.dry_run, v.cpu.mode),
            (v.replica.dry_run, v.replica.mode),
        ] {
            assert!(dry, "band must be born dryRun (shadow-first)");
            assert_eq!(mode, "shadow", "band mode must be shadow");
        }
    }

    /// The PER-CLASS render picks the RIGHT topology arm per workload class — mysql
    /// breathes `masterSlave`, neo4j `persistent`, a quorum store `fullyDistributed`
    /// with an odd quorum floor. Proves the ReplicaBand is topology-correct, not a
    /// one-size band stamped on every tier.
    #[test]
    fn per_class_render_picks_the_right_topology() {
        let cases = [
            (WorkloadClass::StatelessService, "nonPersistent", 2u32),
            (WorkloadClass::RelationalDatabase, "masterSlave", 2),
            (WorkloadClass::PersistentStore, "persistent", 2),
            (WorkloadClass::QuorumStore, "fullyDistributed", 3),
        ];
        for (class, topology, floor) in cases {
            let v = render_workload_breathe(&CAMELOT, class)
                .unwrap_or_else(|| panic!("no render for {}", class.as_str()));
            assert_eq!(v.replica.topology, topology, "{} topology", class.as_str());
            assert_eq!(v.replica.floor, floor, "{} floor", class.as_str());
        }
    }

    /// REAP-THE-RABBIT, offline half: the typed source admits NO message-queue /
    /// rabbit workload class, so a `rabbitmq` breathe band is UNREPRESENTABLE in the
    /// render — the render can never emit one. (The LIVE orphan `camelot-rabbitmq`
    /// band stuck in phase Error is a separate live-cluster reap — a LiveTODO, not
    /// something this offline render can close.)
    #[test]
    fn no_rendered_band_is_a_rabbit_or_message_queue() {
        for class in crate::preset::ALL_WORKLOAD_CLASSES {
            let label = class.as_str();
            assert!(
                !label.contains("rabbit") && !label.contains("mq") && !label.contains("queue"),
                "{label} looks like a message-queue class — the preset must not arm one"
            );
        }
        // And the concrete render carries no rabbit token anywhere.
        for class in crate::preset::ALL_WORKLOAD_CLASSES {
            if let Some(v) = render_workload_breathe(&CAMELOT, class) {
                let s = v.to_string();
                assert!(!s.contains("rabbit"), "a rendered band names rabbit");
                assert!(!s.contains("amqp"), "a rendered band names amqp");
            }
        }
    }

    /// The struct-level fields agree with the serialized YAML (a cheap round-trip
    /// sanity: what the fields say is what the Display emits).
    #[test]
    fn display_reflects_the_struct_fields() {
        let v: GlobalBreatheValues = render_global_breathe(&CAMELOT);
        let s = v.to_string();
        assert!(s.contains(&alloc_line("floor", v.replica.floor)));
        assert!(s.contains("topology: nonPersistent"));
    }

    fn alloc_line(key: &str, floor: u32) -> String {
        // A tiny typed helper for the assertion above — not an emission surface.
        let mut out = String::from("  ");
        out.push_str(key);
        out.push_str(": ");
        out.push_str(&floor.to_string());
        out
    }
}
