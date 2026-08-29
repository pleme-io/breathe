//! `preset` — the `BreatheDefaults` breathe-posture preset (Pillar 12).
//!
//! A breathe preset is a NAMED BUNDLE that, per workload TOPOLOGY-CLASS, selects
//! the whole band-set (vertical setpoint, replica floor + topology, whether a
//! StorageBand applies) plus the shared spot placement and the flex-window cost
//! envelope. It is the "declare, don't author" move for breathe posture: one
//! preset reference (`global.breathe.preset: the private estate`) arms every the private estate
//! workload's full band-set from ONE typed row, instead of a hand-authored
//! per-workload `global.breathe` block. It is the Rust border of the TYPED-SPEC
//! triplet — the authored `(defbreathe-preset :the private estate …)` lisp form in
//! `specs/presets.lisp` is the spec, and [`BreatheDefaults::resolve`] is the pure
//! interpreter that renders a class into a concrete per-band posture.
//!
//! Interpreter honesty: rendering a preset into a band posture has **no side
//! effects** — it is a pure `class → posture` fold, so it needs no `Environment`
//! trait (the triplet's mockable-side-effects requirement is vacuous here; the
//! side-effecting interpreter is the chart renderer + the breathe controller
//! downstream, which own their own seams).
//!
//! Tier-honest: the SPOT_AGGRESSIVE preset is born SHADOW-FIRST (`dryRun: true`,
//! `mode: shadow`, setpoint `0.8`) — every band attests what it WOULD carve but
//! mutates nothing until live-applied. That is correct + honest with no live
//! cluster; the flex-window auction the placement points at is a LiveTODO (see
//! [`crate::cost`]).

use crate::cost::{FlexWindow, SPOT_AGGRESSIVE_FLEX_WINDOW};
use crate::{REPLICA_TOPOLOGY_AXIS, TopologyArm};

/// A workload TOPOLOGY-CLASS the preset arms. Each class maps to exactly one
/// replica-topology arm; the four classes below cover all four
/// [`REPLICA_TOPOLOGY_AXIS`] arms (enforced by a reflection test).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadClass {
    /// A stateless SaaS service pod (a request-serving API/worker tier). Pods are
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
/// tainted-node targeting that keeps the private estate on its own isolated capacity and
/// auctions it entirely from the interruptible pool.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpotPlacement {
    /// The `nodeSelector` role value that pins onto the private estate node group.
    pub node_selector_role: &'static str,
    /// The taint key the workload tolerates to land on the (tainted) the private estate nodes.
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

/// The private estate per-class profiles. The four classes cover all four topology arms;
/// the stateful three carry storage, the stateless one does not; `quorum-store`
/// raises its floor to an odd quorum.
pub const SPOT_AGGRESSIVE_PROFILES: &[WorkloadProfile] = &[
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
/// from one typed value. [`SPOT_AGGRESSIVE`] is the canonical (and, today, only) instance.
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

/// The aggressive 80/20 shadow-first, 100%-spot breathe-defaults preset. One
/// typed row arms a whole workload's band-set.
///
/// Named for the POSTURE it encodes, not for the estate that first ran it: the
/// preset is a spot-aggressive default any cluster can pick, and its placement
/// fields below are the consumer's topology, not this catalog's.
pub const SPOT_AGGRESSIVE: BreatheDefaults = BreatheDefaults {
    name: "spot-aggressive",
    setpoint: 0.8,
    dry_run: true,
    mode: "shadow",
    ha_replica_floor: 2,
    storage_provision_floor: "2Gi",
    placement: SpotPlacement {
        // Generic placement: a nodeSelector role and the taint key that
        // isolates it. A consumer overrides both with its own pool's names —
        // shipping a real pool name here would make this catalog carry one
        // estate's topology.
        node_selector_role: "spot",
        toleration_key: "spot-only",
        spot_fraction: 1.0,
    },
    flex_window: SPOT_AGGRESSIVE_FLEX_WINDOW,
    profiles: SPOT_AGGRESSIVE_PROFILES,
};

/// DEPRECATED alias for [`SPOT_AGGRESSIVE`], kept so an existing consumer keeps
/// compiling across the rename (★★ MODULARIZE, DON'T DELETE). The value is
/// identical; only the name moved off one estate.
#[deprecated(note = "renamed to SPOT_AGGRESSIVE — the preset names a posture, not an estate")]
pub const CAMELOT: BreatheDefaults = SPOT_AGGRESSIVE;

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
    REPLICA_TOPOLOGY_AXIS
        .iter()
        .find(|a| a.crd_kind == crd_kind)
}

#[cfg(test)]
mod tests {
    use super::{
        ALL_WORKLOAD_CLASSES, BreatheDefaults, SPOT_AGGRESSIVE, SPOT_AGGRESSIVE_PROFILES,
        WorkloadClass, topology_arm,
    };
    use crate::RequiresTarget;

    const PRESETS_LISP: &str = include_str!("../../specs/presets.lisp");

    /// Every workload class has exactly one profile, and every profile names a
    /// known class — the bijection that makes the preset the arming inventory.
    #[test]
    fn profiles_are_a_bijection_with_workload_classes() {
        assert_eq!(
            SPOT_AGGRESSIVE_PROFILES.len(),
            ALL_WORKLOAD_CLASSES.len(),
            "row count == class count"
        );
        for c in ALL_WORKLOAD_CLASSES {
            let n = SPOT_AGGRESSIVE_PROFILES
                .iter()
                .filter(|p| p.class == c)
                .count();
            assert_eq!(n, 1, "exactly one profile for {}", c.as_str());
        }
    }

    /// Every profile's topology is a REAL arm of the replica axis — no profile can
    /// name a topology the substrate does not implement.
    #[test]
    fn every_profile_topology_is_a_real_axis_arm() {
        for p in SPOT_AGGRESSIVE_PROFILES {
            assert!(
                topology_arm(p.topology_kind).is_some(),
                "{}'s topology {} is not a real axis arm",
                p.class.as_str(),
                p.topology_kind
            );
        }
    }

    /// The four profiles cover ALL FOUR replica-topology arms — the preset exercises
    /// the whole topology axis, not just the stateless case.
    #[test]
    fn profiles_cover_every_topology_arm() {
        for arm in &crate::REPLICA_TOPOLOGY_AXIS {
            let n = SPOT_AGGRESSIVE_PROFILES
                .iter()
                .filter(|p| p.topology_kind == arm.crd_kind)
                .count();
            assert!(
                n >= 1,
                "no profile covers the {} topology arm",
                arm.crd_kind
            );
        }
    }

    /// THE storage-coupling invariant: a class HAS a StorageBand iff its topology
    /// requires a StatefulSet (has data). A stateless class never carries storage; a
    /// stateful one always does. Ties persistence to the topology, structurally.
    #[test]
    fn storage_couples_to_the_stateful_topology() {
        for p in SPOT_AGGRESSIVE_PROFILES {
            let arm = topology_arm(p.topology_kind).expect("real arm");
            let stateful = matches!(arm.requires_target, RequiresTarget::Kind("StatefulSet"));
            assert_eq!(
                p.has_storage,
                stateful,
                "{}: has_storage must equal 'topology requires StatefulSet'",
                p.class.as_str()
            );
        }
    }

    /// Every profile respects the HA floor, and a `fullyDistributed` class raises it
    /// to an odd quorum `≥ 3` (never an even count that can split-brain).
    #[test]
    fn floors_respect_ha_and_quorum() {
        for p in SPOT_AGGRESSIVE_PROFILES {
            assert!(
                p.replica_floor >= SPOT_AGGRESSIVE.ha_replica_floor,
                "{}: floor below the HA floor",
                p.class.as_str()
            );
            if p.topology_kind == "fullyDistributed" {
                assert!(
                    p.replica_floor >= 3 && p.replica_floor % 2 == 1,
                    "{}: a quorum floor must be odd and ≥ 3",
                    p.class.as_str()
                );
            }
        }
    }

    /// THE SPOT_AGGRESSIVE posture: aggressive 80/20, shadow-first, 100%-spot, HA floor 2.
    /// Guards the whole named posture against a future edit rounding it up (e.g.
    /// flipping `dryRun` off, or the spot fraction below 1.0).
    #[test]
    fn isolated_posture_is_aggressive_shadow_first_100pct_spot() {
        assert!(
            (SPOT_AGGRESSIVE.setpoint - 0.8).abs() < f64::EPSILON,
            "setpoint must be 0.8 (80/20)"
        );
        assert!(SPOT_AGGRESSIVE.dry_run, "shadow-first: born dryRun");
        assert_eq!(SPOT_AGGRESSIVE.mode, "shadow", "shadow-first: mode shadow");
        assert_eq!(SPOT_AGGRESSIVE.ha_replica_floor, 2, "HA floor 2");
        assert_eq!(
            SPOT_AGGRESSIVE.storage_provision_floor, "2Gi",
            "provision-minimal storage floor"
        );
        assert_eq!(SPOT_AGGRESSIVE.placement.node_selector_role, "spot");
        assert_eq!(SPOT_AGGRESSIVE.placement.toleration_key, "spot-only");
        assert!(
            (SPOT_AGGRESSIVE.placement.spot_fraction - 1.0).abs() < f64::EPSILON,
            "100% spot"
        );
    }

    /// THE INTERPRETER renders every class into a posture, and the posture agrees
    /// with the preset + profile (setpoint, shadow, topology, floor, storage).
    #[test]
    fn resolve_renders_every_class_faithfully() {
        for c in ALL_WORKLOAD_CLASSES {
            let posture = SPOT_AGGRESSIVE
                .resolve(c)
                .unwrap_or_else(|| panic!("no posture for {}", c.as_str()));
            let p = SPOT_AGGRESSIVE.profile(c).expect("profile");
            assert!((posture.vertical_setpoint - SPOT_AGGRESSIVE.setpoint).abs() < f64::EPSILON);
            assert_eq!(posture.dry_run, SPOT_AGGRESSIVE.dry_run);
            assert_eq!(posture.mode, SPOT_AGGRESSIVE.mode);
            assert_eq!(posture.replica_topology, p.topology_kind);
            assert_eq!(
                posture.replica_floor,
                p.replica_floor.max(SPOT_AGGRESSIVE.ha_replica_floor)
            );
            assert_eq!(posture.storage_band, p.has_storage);
            assert_eq!(posture.placement, SPOT_AGGRESSIVE.placement);
        }
    }

    /// The interpreter never resolves a floor below the HA floor — the `max` guard
    /// holds even if a profile were (illegally) authored below it.
    #[test]
    fn resolve_never_drops_below_the_ha_floor() {
        for c in ALL_WORKLOAD_CLASSES {
            let posture = SPOT_AGGRESSIVE.resolve(c).expect("posture");
            assert!(posture.replica_floor >= SPOT_AGGRESSIVE.ha_replica_floor);
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
            ..SPOT_AGGRESSIVE
        };
        assert!(PARTIAL.resolve(WorkloadClass::StatelessService).is_some());
        assert!(PARTIAL.resolve(WorkloadClass::RelationalDatabase).is_none());
    }

    // ── Lisp ↔ Rust reflection (the TYPED-SPEC triplet cross-check) ──────────────

    /// The authored `(defbreathe-preset :the private estate …)` names the preset, its
    /// aggressive posture, its spot placement, and every workload class + topology —
    /// so the lisp spec and the Rust border can never drift. The class labels and
    /// topology crd_kinds are mutually non-substring, so bare `contains` is
    /// unambiguous (the same convention the dimensions catalog uses).
    #[test]
    fn spot_aggressive_preset_is_declared_in_the_lisp() {
        assert!(
            PRESETS_LISP.contains(":spot-aggressive"),
            "the lisp must declare the :spot-aggressive preset"
        );
        assert!(
            PRESETS_LISP.contains("0.8"),
            "the lisp must carry the 0.8 setpoint"
        );
        // DERIVED from the const, never restated: a literal here is free to
        // disagree with the preset it is meant to pin, and that is exactly
        // how it drifted through the rename.
        assert!(
            PRESETS_LISP.contains(SPOT_AGGRESSIVE.placement.toleration_key),
            "the lisp must carry the {} toleration key",
            SPOT_AGGRESSIVE.placement.toleration_key
        );
        assert!(
            PRESETS_LISP.contains(SPOT_AGGRESSIVE.placement.node_selector_role),
            "the lisp must carry the {} nodeSelector role",
            SPOT_AGGRESSIVE.placement.node_selector_role
        );
        assert!(
            PRESETS_LISP.contains(SPOT_AGGRESSIVE.name),
            "the lisp must carry the preset name {}",
            SPOT_AGGRESSIVE.name
        );
        for p in SPOT_AGGRESSIVE_PROFILES {
            assert!(
                PRESETS_LISP.contains(p.class.as_str()),
                "the lisp is missing the {} class",
                p.class.as_str()
            );
            assert!(
                PRESETS_LISP.contains(p.topology_kind),
                "the lisp is missing the {} topology",
                p.topology_kind
            );
        }
    }
}
