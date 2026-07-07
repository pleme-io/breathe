//! `preset` — the `CamelotBreatheDefaults` breathe-posture preset (Pillar 12).
//!
//! A breathe preset is a NAMED BUNDLE that, per workload TOPOLOGY-CLASS, selects
//! the whole band-set (vertical setpoint, replica floor + topology, whether a
//! StorageBand applies) plus the shared spot placement and the flex-window cost
//! envelope. It is the "declare, don't author" move for breathe posture: one
//! preset reference (`global.breathe.preset: camelot`) arms every Camelot
//! workload's full band-set from ONE typed row, instead of a hand-authored
//! per-workload `global.breathe` block. It is the Rust border of the TYPED-SPEC
//! triplet — the authored `(defbreathe-preset :camelot …)` lisp form in
//! `specs/presets.lisp` is the spec, and [`BreatheDefaults::resolve`] is the pure
//! interpreter that renders a class into a concrete per-band posture.
//!
//! Interpreter honesty: rendering a preset into a band posture has **no side
//! effects** — it is a pure `class → posture` fold, so it needs no `Environment`
//! trait (the triplet's mockable-side-effects requirement is vacuous here; the
//! side-effecting interpreter is the chart renderer + the breathe controller
//! downstream, which own their own seams).
//!
//! Tier-honest: the CAMELOT preset is born SHADOW-FIRST (`dryRun: true`,
//! `mode: shadow`, setpoint `0.8`) — every band attests what it WOULD carve but
//! mutates nothing until live-applied. That is correct + honest with no live
//! cluster; the flex-window auction the placement points at is a LiveTODO (see
//! [`crate::cost`]).

use crate::cost::{FlexWindow, CAMELOT_FLEX_WINDOW};
use crate::{TopologyArm, REPLICA_TOPOLOGY_AXIS};

/// A workload TOPOLOGY-CLASS the preset arms. Each class maps to exactly one
/// replica-topology arm; the four classes below cover all four
/// [`REPLICA_TOPOLOGY_AXIS`] arms (enforced by a reflection test).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadClass {
    /// A stateless SaaS service pod (auth / bis / uam / gator / kfm …). Pods are
    /// interchangeable — free HPA-style scaling, HA floor only. `nonPersistent`.
    StatelessService,
    /// A relational primary + read-replicas tier (MySQL). Only the read-replicas
    /// breathe; the primary is never scaled away. `masterSlave`.
    RelationalDatabase,
    /// A single-writer persistent store, PVC-per-ordinal (Neo4j graph). Grow adds
    /// an ordinal+PVC; a scale-in is HELD for drain. `persistent`.
    PersistentStore,
    /// A quorum/consensus tier (a distributed object/metadata store). Odd count
    /// ≥ 3, majority-safe one-rung steps. `fullyDistributed`.
    QuorumStore,
}

impl WorkloadClass {
    /// The kebab-case stable label (used in the authored lisp + as a stable id).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StatelessService => "stateless-service",
            Self::RelationalDatabase => "relational-database",
            Self::PersistentStore => "persistent-store",
            Self::QuorumStore => "quorum-store",
        }
    }
}

/// Every workload class — the domain side of the preset's bijection check.
pub const ALL_WORKLOAD_CLASSES: [WorkloadClass; 4] = [
    WorkloadClass::StatelessService,
    WorkloadClass::RelationalDatabase,
    WorkloadClass::PersistentStore,
    WorkloadClass::QuorumStore,
];

/// The 100%-spot placement the preset stamps on every armed workload — the
/// tainted-node targeting that keeps Camelot on its own isolated capacity and
/// auctions it entirely from the interruptible pool.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpotPlacement {
    /// The `nodeSelector` role value that pins onto the Camelot node group.
    pub node_selector_role: &'static str,
    /// The taint key the workload tolerates to land on the (tainted) Camelot nodes.
    pub toleration_key: &'static str,
    /// The spot fraction of the placement (`1.0` = 100% spot — even the databases).
    pub spot_fraction: f64,
}

/// One workload-class profile — the per-class band selections. The shared posture
/// (setpoint / dryRun / mode / placement / flex-window) lives on
/// [`BreatheDefaults`]; a profile carries only what VARIES by topology class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadProfile {
    pub class: WorkloadClass,
    /// The replica-topology `crd_kind` this class breathes under — MUST be a real
    /// arm of [`REPLICA_TOPOLOGY_AXIS`] (reflection-enforced).
    pub topology_kind: &'static str,
    /// The at-rest replica floor for this class. `≥ ha_replica_floor` always; a
    /// `fullyDistributed` class raises it to an odd quorum `≥ 3`.
    pub replica_floor: u32,
    /// `true` ⇒ this class carries persistent data and gets a StorageBand. Couples
    /// to the topology's StatefulSet requirement (reflection-enforced): a stateful
    /// class has storage, a stateless one does not.
    pub has_storage: bool,
}

/// The Camelot per-class profiles. The four classes cover all four topology arms;
/// the stateful three carry storage, the stateless one does not; `quorum-store`
/// raises its floor to an odd quorum.
pub const CAMELOT_PROFILES: &[WorkloadProfile] = &[
    WorkloadProfile {
        class: WorkloadClass::StatelessService,
        topology_kind: "nonPersistent",
        replica_floor: 2, // HA floor
        has_storage: false,
    },
    WorkloadProfile {
        class: WorkloadClass::RelationalDatabase,
        topology_kind: "masterSlave",
        replica_floor: 2, // primary + ≥1 read-replica
        has_storage: true,
    },
    WorkloadProfile {
        class: WorkloadClass::PersistentStore,
        topology_kind: "persistent",
        replica_floor: 2, // never rest below the replication factor
        has_storage: true,
    },
    WorkloadProfile {
        class: WorkloadClass::QuorumStore,
        topology_kind: "fullyDistributed",
        replica_floor: 3, // odd quorum ≥ 3
        has_storage: true,
    },
];

/// A breathe-posture preset — a named bundle that arms a whole fleet's band-set
/// from one typed value. [`CAMELOT`] is the canonical (and, today, only) instance.
#[derive(Debug, Clone, Copy)]
pub struct BreatheDefaults {
    /// The preset name (`global.breathe.preset: <name>`).
    pub name: &'static str,
    /// The utilization setpoint every VERTICAL band (memory/cpu) holds. The
    /// aggressive posture is `0.8` (80% used / 20% headroom).
    pub setpoint: f64,
    /// Shadow-first: every band is born `dryRun` (attest what it would carve,
    /// mutate nothing) until explicitly live-applied.
    pub dry_run: bool,
    /// The promotion mode every band is born in — `"shadow"` (observe-only) for the
    /// aggressive shadow-first posture (matches `PromotionMode::Shadow`'s serde).
    pub mode: &'static str,
    /// The HA replica floor every armed workload never rests below (`2`). A
    /// per-class profile MAY raise it (a quorum class → `3`), never lower it.
    pub ha_replica_floor: u32,
    /// The PROVISION-MINIMAL storage floor every armed STORAGE band is born at (a
    /// quantity string, e.g. `2Gi`). Storage carves grow-only: a stateful
    /// workload's PVC is provisioned at this small floor and grows online toward
    /// the setpoint as real data lands, so an over-provisioned volume (a fixed
    /// `50Gi` holding a few hundred MiB) is never the default posture — it is only
    /// ever an external over-declaration. Mirrors the `StorageBand` CRD default.
    pub storage_provision_floor: &'static str,
    /// The 100%-spot placement stamped on every armed workload.
    pub placement: SpotPlacement,
    /// The flex-window cost envelope (diversified instance families + `$`/mo budget).
    pub flex_window: FlexWindow,
    /// The per-workload-class profiles.
    pub profiles: &'static [WorkloadProfile],
}

/// The Camelot breathe-defaults preset — the aggressive 80/20 shadow-first,
/// 100%-spot posture. One typed row arms every Camelot workload's whole band-set.
pub const CAMELOT: BreatheDefaults = BreatheDefaults {
    name: "camelot",
    setpoint: 0.8,
    dry_run: true,
    mode: "shadow",
    ha_replica_floor: 2,
    storage_provision_floor: "2Gi",
    placement: SpotPlacement {
        node_selector_role: "camelot",
        toleration_key: "camelot-only",
        spot_fraction: 1.0,
    },
    flex_window: CAMELOT_FLEX_WINDOW,
    profiles: CAMELOT_PROFILES,
};

/// The concrete per-band posture a preset resolves a workload class into — the
/// interpreter's typed output. Pure: no side effects, so no `Environment` seam.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResolvedBandPosture {
    pub class: WorkloadClass,
    /// The vertical (memory/cpu) band setpoint.
    pub vertical_setpoint: f64,
    /// The band is born in shadow (`dryRun`).
    pub dry_run: bool,
    /// The promotion mode label (`"shadow"`).
    pub mode: &'static str,
    /// The ReplicaBand topology `crd_kind`.
    pub replica_topology: &'static str,
    /// The ReplicaBand at-rest floor (`max(profile.replica_floor, ha_replica_floor)`).
    pub replica_floor: u32,
    /// Whether a StorageBand is emitted for this class.
    pub storage_band: bool,
    /// The provision-minimal floor a StorageBand (when emitted) is born at — the
    /// grow-on-demand starting size. Carried on every posture so the render never
    /// hard-codes it; inert for a class with `storage_band == false`.
    pub storage_provision_floor: &'static str,
    /// The 100%-spot placement.
    pub placement: SpotPlacement,
}

impl BreatheDefaults {
    /// The profile for a workload class (`None` if the preset has no row for it —
    /// a reflection-test failure).
    #[must_use]
    pub fn profile(&self, class: WorkloadClass) -> Option<&WorkloadProfile> {
        self.profiles.iter().find(|p| p.class == class)
    }

    /// THE INTERPRETER — render a workload class into its concrete band posture. A
    /// pure `class → posture` fold; the effective replica floor is
    /// `max(profile floor, the preset HA floor)` so a class can raise but never
    /// lower the shared HA floor. `None` if the preset has no profile for the class.
    #[must_use]
    pub fn resolve(&self, class: WorkloadClass) -> Option<ResolvedBandPosture> {
        let p = self.profile(class)?;
        Some(ResolvedBandPosture {
            class,
            vertical_setpoint: self.setpoint,
            dry_run: self.dry_run,
            mode: self.mode,
            replica_topology: p.topology_kind,
            replica_floor: p.replica_floor.max(self.ha_replica_floor),
            storage_band: p.has_storage,
            storage_provision_floor: self.storage_provision_floor,
            placement: self.placement,
        })
    }
}

/// The [`TopologyArm`] for a `crd_kind` (`None` if it is not a real axis arm).
#[must_use]
pub fn topology_arm(crd_kind: &str) -> Option<&'static TopologyArm> {
    REPLICA_TOPOLOGY_AXIS.iter().find(|a| a.crd_kind == crd_kind)
}

#[cfg(test)]
mod tests {
    use super::{
        topology_arm, BreatheDefaults, WorkloadClass, ALL_WORKLOAD_CLASSES, CAMELOT, CAMELOT_PROFILES,
    };
    use crate::RequiresTarget;

    const PRESETS_LISP: &str = include_str!("../../specs/presets.lisp");

    /// Every workload class has exactly one profile, and every profile names a
    /// known class — the bijection that makes the preset the arming inventory.
    #[test]
    fn profiles_are_a_bijection_with_workload_classes() {
        assert_eq!(CAMELOT_PROFILES.len(), ALL_WORKLOAD_CLASSES.len(), "row count == class count");
        for c in ALL_WORKLOAD_CLASSES {
            let n = CAMELOT_PROFILES.iter().filter(|p| p.class == c).count();
            assert_eq!(n, 1, "exactly one profile for {}", c.as_str());
        }
    }

    /// Every profile's topology is a REAL arm of the replica axis — no profile can
    /// name a topology the substrate does not implement.
    #[test]
    fn every_profile_topology_is_a_real_axis_arm() {
        for p in CAMELOT_PROFILES {
            assert!(topology_arm(p.topology_kind).is_some(), "{}'s topology {} is not a real axis arm", p.class.as_str(), p.topology_kind);
        }
    }

    /// The four profiles cover ALL FOUR replica-topology arms — the preset exercises
    /// the whole topology axis, not just the stateless case.
    #[test]
    fn profiles_cover_every_topology_arm() {
        for arm in &crate::REPLICA_TOPOLOGY_AXIS {
            let n = CAMELOT_PROFILES.iter().filter(|p| p.topology_kind == arm.crd_kind).count();
            assert!(n >= 1, "no profile covers the {} topology arm", arm.crd_kind);
        }
    }

    /// THE storage-coupling invariant: a class HAS a StorageBand iff its topology
    /// requires a StatefulSet (has data). A stateless class never carries storage; a
    /// stateful one always does. Ties persistence to the topology, structurally.
    #[test]
    fn storage_couples_to_the_stateful_topology() {
        for p in CAMELOT_PROFILES {
            let arm = topology_arm(p.topology_kind).expect("real arm");
            let stateful = matches!(arm.requires_target, RequiresTarget::Kind("StatefulSet"));
            assert_eq!(p.has_storage, stateful, "{}: has_storage must equal 'topology requires StatefulSet'", p.class.as_str());
        }
    }

    /// Every profile respects the HA floor, and a `fullyDistributed` class raises it
    /// to an odd quorum `≥ 3` (never an even count that can split-brain).
    #[test]
    fn floors_respect_ha_and_quorum() {
        for p in CAMELOT_PROFILES {
            assert!(p.replica_floor >= CAMELOT.ha_replica_floor, "{}: floor below the HA floor", p.class.as_str());
            if p.topology_kind == "fullyDistributed" {
                assert!(p.replica_floor >= 3 && p.replica_floor % 2 == 1, "{}: a quorum floor must be odd and ≥ 3", p.class.as_str());
            }
        }
    }

    /// THE CAMELOT posture: aggressive 80/20, shadow-first, 100%-spot, HA floor 2.
    /// Guards the whole named posture against a future edit rounding it up (e.g.
    /// flipping `dryRun` off, or the spot fraction below 1.0).
    #[test]
    fn camelot_posture_is_aggressive_shadow_first_100pct_spot() {
        assert!((CAMELOT.setpoint - 0.8).abs() < f64::EPSILON, "setpoint must be 0.8 (80/20)");
        assert!(CAMELOT.dry_run, "shadow-first: born dryRun");
        assert_eq!(CAMELOT.mode, "shadow", "shadow-first: mode shadow");
        assert_eq!(CAMELOT.ha_replica_floor, 2, "HA floor 2");
        assert_eq!(CAMELOT.storage_provision_floor, "2Gi", "provision-minimal storage floor");
        assert_eq!(CAMELOT.placement.node_selector_role, "camelot");
        assert_eq!(CAMELOT.placement.toleration_key, "camelot-only");
        assert!((CAMELOT.placement.spot_fraction - 1.0).abs() < f64::EPSILON, "100% spot");
    }

    /// THE INTERPRETER renders every class into a posture, and the posture agrees
    /// with the preset + profile (setpoint, shadow, topology, floor, storage).
    #[test]
    fn resolve_renders_every_class_faithfully() {
        for c in ALL_WORKLOAD_CLASSES {
            let posture = CAMELOT.resolve(c).unwrap_or_else(|| panic!("no posture for {}", c.as_str()));
            let p = CAMELOT.profile(c).expect("profile");
            assert!((posture.vertical_setpoint - CAMELOT.setpoint).abs() < f64::EPSILON);
            assert_eq!(posture.dry_run, CAMELOT.dry_run);
            assert_eq!(posture.mode, CAMELOT.mode);
            assert_eq!(posture.replica_topology, p.topology_kind);
            assert_eq!(posture.replica_floor, p.replica_floor.max(CAMELOT.ha_replica_floor));
            assert_eq!(posture.storage_band, p.has_storage);
            assert_eq!(posture.placement, CAMELOT.placement);
        }
    }

    /// The interpreter never resolves a floor below the HA floor — the `max` guard
    /// holds even if a profile were (illegally) authored below it.
    #[test]
    fn resolve_never_drops_below_the_ha_floor() {
        for c in ALL_WORKLOAD_CLASSES {
            let posture = CAMELOT.resolve(c).expect("posture");
            assert!(posture.replica_floor >= CAMELOT.ha_replica_floor);
        }
    }

    /// A preset with no profile for a class resolves to `None` — a missing row is a
    /// typed absence, never a silent wrong posture.
    #[test]
    fn resolve_is_none_for_an_unarmed_class() {
        // A synthetic preset that only arms the stateless class.
        const PARTIAL: BreatheDefaults = BreatheDefaults {
            profiles: &[super::WorkloadProfile {
                class: WorkloadClass::StatelessService,
                topology_kind: "nonPersistent",
                replica_floor: 2,
                has_storage: false,
            }],
            ..CAMELOT
        };
        assert!(PARTIAL.resolve(WorkloadClass::StatelessService).is_some());
        assert!(PARTIAL.resolve(WorkloadClass::RelationalDatabase).is_none());
    }

    // ── Lisp ↔ Rust reflection (the TYPED-SPEC triplet cross-check) ──────────────

    /// The authored `(defbreathe-preset :camelot …)` names the preset, its
    /// aggressive posture, its spot placement, and every workload class + topology —
    /// so the lisp spec and the Rust border can never drift. The class labels and
    /// topology crd_kinds are mutually non-substring, so bare `contains` is
    /// unambiguous (the same convention the dimensions catalog uses).
    #[test]
    fn camelot_preset_is_declared_in_the_lisp() {
        assert!(PRESETS_LISP.contains(":camelot"), "the lisp must declare the :camelot preset");
        assert!(PRESETS_LISP.contains("0.8"), "the lisp must carry the 0.8 setpoint");
        assert!(PRESETS_LISP.contains("camelot-only"), "the lisp must carry the spot toleration key");
        for p in CAMELOT_PROFILES {
            assert!(PRESETS_LISP.contains(p.class.as_str()), "the lisp is missing the {} class", p.class.as_str());
            assert!(PRESETS_LISP.contains(p.topology_kind), "the lisp is missing the {} topology", p.topology_kind);
        }
    }
}
