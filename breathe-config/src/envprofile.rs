//! Environment-discovered band defaults — the "best defaults from the running
//! environment" tier for breathe.
//!
//! A typed [`EnvironmentProfile`] (*what kind of cluster are we reconciling
//! in?*) resolves, via the pure [`resolve`] policy, to a [`BandDefaults`]
//! override set layered **above** breathe-crd's static serde defaults and
//! **below** a band's explicit CR fields:
//!
//! ```text
//! crd serde defaults  ->  ENVIRONMENT-DISCOVERED (this)  ->  explicit CR spec
//!   (the floor)            (best fit for the cluster)        (operator wins)
//! ```
//!
//! This mirrors the fleet-generic shikumi discovered-defaults tier
//! (`shikumi::discovered`) at breathe's CRD layer, and shares
//! [`kanchi`]'s axis vocabulary so "what cluster am I in?" is answered the same
//! way everywhere. The profile is *detected* once by the controller (kanchi
//! host probes + cluster-API facts); [`resolve`] is **pure + total**, so the
//! whole best-default policy is unit-tested without a cluster.
//!
//! The load-bearing axis is [`kanchi::axes::Tenancy`]: a workload reconciling a
//! cluster it does **not** own (a customer's, a multi-tenant staging EKS) gets
//! the least-disruptive, shadow-first, never-touch-the-nodes posture by
//! *default* — least-privilege by construction, not by remembering to set it.

pub use kanchi::axes::{CapacityType, Cloud, Orchestrator, Tenancy};

/// The promotion posture a band runs under. Mirrors breathe-crd's
/// `PromotionMode` as a config-layer value (kept plain here so breathe-config
/// need not depend on breathe-crd; the controller maps it onto the CRD enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BandMode {
    /// Decide + record, never carve (pure observation).
    Shadow,
    /// Shadow until a clean window proves the decision, then carve.
    ShadowConfirmEffect,
    /// Sample only — don't even decide.
    Observe,
    /// Carve immediately.
    Live,
}

/// What kind of environment breathe is reconciling in. Every axis defaults, via
/// [`Self::conservative`], to its least-known / safest value so an *undetected*
/// axis lands on the fail-safe posture rather than the permissive one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvironmentProfile {
    /// Kubernetes vs bare host.
    pub orchestrator: Orchestrator,
    /// The cloud, if recognized.
    pub cloud: Cloud,
    /// **The load-bearing axis.** Own cluster vs a foreign/multi-tenant one.
    pub tenancy: Tenancy,
    /// The node market posture (spot churns; on-demand is stable).
    pub capacity: CapacityType,
    /// k8s ≥ 1.33 — `pods/resize` is GA, so carve in place vs roll.
    pub resize_capable: bool,
}

impl EnvironmentProfile {
    /// The fail-safe profile: assume the most constrained environment until
    /// detection proves otherwise — a **foreign** cluster, unknown cloud,
    /// unknown capacity, no in-place resize. Resolving this yields the
    /// least-disruptive posture, so a controller that cannot probe its
    /// environment still does no harm.
    #[must_use]
    pub const fn conservative() -> Self {
        Self {
            orchestrator: Orchestrator::Kubernetes,
            cloud: Cloud::None,
            tenancy: Tenancy::Unknown,
            capacity: CapacityType::Unknown,
            resize_capable: false,
        }
    }

    /// True when this cluster should be treated as foreign — explicitly
    /// `Foreign`, or `Unknown` (fail-safe: an unprobed tenancy is treated as
    /// not-ours, so we never freely mutate a cluster we might not own).
    #[must_use]
    pub const fn is_foreign(&self) -> bool {
        matches!(self.tenancy, Tenancy::Foreign | Tenancy::Unknown)
    }
}

/// The band-default overrides an environment recommends. A `None` field means
/// "no opinion — keep the breathe-crd static default"; only the fields the
/// environment genuinely wants to shape are `Some`, exactly like a discovered
/// *partial* config dict (an undetected/neutral axis contributes nothing).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BandDefaults {
    /// Target utilization band centre.
    pub setpoint: Option<f64>,
    /// Multiplicative grow step (gentler in a foreign cluster).
    pub grow_factor: Option<f64>,
    /// Multiplicative shrink step (gentler in a foreign cluster).
    pub shrink_factor: Option<f64>,
    /// Seconds between carves (longer in a foreign cluster).
    pub cooldown_seconds: Option<u64>,
    /// Post-(re)start shrink hold (longer on spot / boot-churny nodes).
    pub warmup_seconds: Option<u64>,
    /// Promotion posture (shadow-only in a foreign cluster).
    pub mode: Option<BandMode>,
    /// Whether to plan only and never carve.
    pub dry_run: Option<bool>,
    /// Whether node-pool / node-provisioning band actions are permitted at all
    /// (never, in a cluster we don't own).
    pub allow_node_provisioning: Option<bool>,
}

/// Resolve the best-default band overrides for `profile`. **Pure + total.**
///
/// Policy (Operating Principle #0 — the *destination* default, not the easy
/// one):
///
/// - **Foreign / unknown tenancy** → least-disruptive, shadow-first,
///   conservative bands, and node provisioning **off**. We are a guest: more
///   headroom (lower setpoint), gentle grow/shrink, long cooldown, plan-only,
///   and we never create/delete nodes.
/// - **Own cluster** → trust the broad breathe-crd statics (no band overrides);
///   node provisioning permitted.
/// - **Spot capacity (any tenancy)** → a longer warmup to ride out boot-spike +
///   interruption churn.
#[must_use]
pub fn resolve(profile: &EnvironmentProfile) -> BandDefaults {
    let mut defaults = BandDefaults::default();

    if profile.is_foreign() {
        defaults.setpoint = Some(0.70);
        defaults.grow_factor = Some(1.10);
        defaults.shrink_factor = Some(0.95);
        defaults.cooldown_seconds = Some(1200);
        defaults.warmup_seconds = Some(900);
        defaults.mode = Some(BandMode::Shadow);
        defaults.dry_run = Some(true);
        defaults.allow_node_provisioning = Some(false);
    } else {
        // Our own cluster: the crd statics are the right posture already
        // (setpoint 0.80, grow 1.25, ShadowConfirmEffect). Only assert the one
        // thing that differs from the fail-safe: node provisioning is allowed.
        defaults.allow_node_provisioning = Some(true);
    }

    // Spot churns harder than the band's tenancy posture accounts for: hold a
    // shrink longer after a (re)start regardless of who owns the cluster.
    if matches!(profile.capacity, CapacityType::Spot) {
        let base = defaults.warmup_seconds.unwrap_or(600);
        defaults.warmup_seconds = Some(base.max(900));
    }

    defaults
}

#[cfg(test)]
mod tests {
    use super::*;

    fn own() -> EnvironmentProfile {
        EnvironmentProfile {
            orchestrator: Orchestrator::Kubernetes,
            cloud: Cloud::None,
            tenancy: Tenancy::Own,
            capacity: CapacityType::OnDemand,
            resize_capable: true,
        }
    }

    #[test]
    fn foreign_cluster_gets_least_disruptive_shadow_posture() {
        let p = EnvironmentProfile { tenancy: Tenancy::Foreign, ..own() };
        let d = resolve(&p);
        assert_eq!(d.setpoint, Some(0.70), "more headroom");
        assert_eq!(d.mode, Some(BandMode::Shadow), "shadow-only in a guest cluster");
        assert_eq!(d.dry_run, Some(true), "plan-only");
        assert_eq!(d.allow_node_provisioning, Some(false), "never touch a foreign cluster's nodes");
        assert!(d.grow_factor.unwrap() < 1.25, "gentler than the rio static grow");
        assert!(d.cooldown_seconds.unwrap() > 600, "longer cooldown than the rio static");
    }

    #[test]
    fn unknown_tenancy_is_treated_as_foreign_fail_safe() {
        let p = EnvironmentProfile { tenancy: Tenancy::Unknown, ..own() };
        // An unprobed cluster gets the SAME safe posture as an explicitly foreign one.
        assert_eq!(resolve(&p), resolve(&EnvironmentProfile { tenancy: Tenancy::Foreign, ..own() }));
    }

    #[test]
    fn own_cluster_keeps_crd_statics_and_allows_node_provisioning() {
        let d = resolve(&own());
        assert_eq!(d.setpoint, None, "no override — keep the crd static (0.80)");
        assert_eq!(d.mode, None, "keep the crd default (ShadowConfirmEffect)");
        assert_eq!(d.dry_run, None);
        assert_eq!(d.allow_node_provisioning, Some(true));
    }

    #[test]
    fn spot_capacity_lengthens_warmup_in_any_tenancy() {
        // own + spot: own has no warmup override, so spot installs the 900s floor.
        let own_spot = resolve(&EnvironmentProfile { capacity: CapacityType::Spot, ..own() });
        assert_eq!(own_spot.warmup_seconds, Some(900));
        // foreign already sets 900; spot keeps it ≥ 900 (no regression).
        let foreign_spot = resolve(&EnvironmentProfile {
            tenancy: Tenancy::Foreign,
            capacity: CapacityType::Spot,
            ..own()
        });
        assert!(foreign_spot.warmup_seconds.unwrap() >= 900);
    }

    #[test]
    fn conservative_profile_resolves_to_the_safe_posture() {
        // The fail-safe profile (used when detection fails) must be foreign-safe.
        let d = resolve(&EnvironmentProfile::conservative());
        assert_eq!(d.mode, Some(BandMode::Shadow));
        assert_eq!(d.allow_node_provisioning, Some(false));
        assert!(EnvironmentProfile::conservative().is_foreign());
    }
}
