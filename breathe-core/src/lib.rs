//! `breathe-core` — the composed reconcile loop.
//!
//! breathe-core **owns** the loop; it is not inherited (the real
//! `promessa-types::TargetController` is pure `diff`/`classify`/`decide` with no
//! `tick()` — BREATHE.md §0). One [`reconcile_one`] tick, per
//! `(target × dimension)`, binds the proven [`breathe_control::plan_tick`] to a
//! provider's atomic I/O: observe via the provider → plan purely (single-writer
//! → band → directionality → freshness → cooldown) → and, *only on Act*, perform
//! the one atomic mutation via the provider. The decision math lives entirely in
//! `breathe-control`; this crate adds only the I/O orchestration + the typed receipt.

use breathe_control::{plan_tick, BandConfig, Decision, TickPlan};
use breathe_provider::{DisruptionClass, DisruptionPolicy, EdgeTier, ProviderError, ResourceProvider, Target};

/// Everything one tick needs that isn't carried by the provider. The provider
/// supplies its own `directionality()` and `owned_field()`; this carries the
/// per-target band policy + the operational gates.
pub struct ReconcileInput<'a> {
    pub target: &'a Target,
    pub cfg: &'a BandConfig,
    /// Max acceptable metric sample age before a mutation is refused.
    pub max_staleness_secs: u64,
    /// Whether this target is inside its post-change cooldown window.
    pub in_cooldown: bool,
    /// Observe-and-attest only; never mutate (the shadow window).
    pub dry_run: bool,
    /// The band's restart policy — the golden/ceiling gate. A carve whose
    /// per-direction [`DisruptionClass`] this policy does not permit is DEFERRED
    /// (surfaced), never silently rolled. Default [`DisruptionPolicy::RestartFreeOnly`]
    /// = golden-by-default (hold every workload that can be held with zero restart).
    pub policy: DisruptionPolicy,
}

/// The typed per-tick receipt — every branch observable, none silent.
#[derive(Debug, PartialEq, Eq)]
pub enum TickReceipt {
    /// Another manager owns the field — yielded, never fought.
    Conflict { manager: String },
    /// The driving sample was too old to carve — held + surfaced.
    Stale { staleness_secs: u64 },
    /// A mutation was warranted but the target is cooling down.
    Cooldown,
    /// The one atomic mutation was applied via the provider, with the restart
    /// cost of the carve (`class`) — the attestation evidence. A `RestartFree`
    /// Applied is a golden carve; a `RestartRequiring` Applied is a witnessed
    /// ceiling crossing (only reachable under an `AllowRestart` policy).
    Applied { from: u64, to: u64, class: DisruptionClass },
    /// dry-run: a mutation would have been applied (shadow attestation).
    DryRunWouldApply { from: u64, to: u64 },
    /// The band law warranted a carve, but its restart cost (`class`) is a
    /// ceiling crossing the band's [`DisruptionPolicy`] does not permit — DEFERRED
    /// rather than rolled. The comfortable berth: the workload stays golden
    /// (undisturbed), un-converged, until the operator widens the policy.
    DeferredWouldRestart { from: u64, to: u64, class: DisruptionClass },
    /// An observable, non-mutating outcome (Hold / AtCeiling / NoSafeShrink / NoLimit).
    Observed { decision: Decision },
    /// A typed provider error (transient → fast requeue; permanent → escalate).
    Error { error: ProviderError },
}

impl TickReceipt {
    /// Where this tick sits on the golden/ceiling line — the per-tick attestation
    /// evidence. A carve that PASSED the gate carries its own class; a refused
    /// crossing (`DeferredWouldRestart`) is GoldenPreserving (it REFUSED to leave
    /// golden); every non-mutating outcome is golden. So "this band stayed on
    /// golden rails (zero ceiling crossings) over window W" is a provable
    /// property of the receipt stream, not a hope (the K4 continuity theorem).
    #[must_use]
    pub fn edge_tier(&self) -> EdgeTier {
        match self {
            Self::Applied { class, .. } => class.edge_tier(),
            // refusing the crossing KEEPS the workload golden.
            Self::DeferredWouldRestart { .. }
            | Self::DryRunWouldApply { .. }
            | Self::Observed { .. }
            | Self::Cooldown
            | Self::Stale { .. }
            | Self::Conflict { .. }
            | Self::Error { .. } => EdgeTier::GoldenPreserving,
        }
    }
}

/// One reconcile tick for one `(target × dimension)`.
pub async fn reconcile_one(
    input: &ReconcileInput<'_>,
    provider: &dyn ResourceProvider,
) -> TickReceipt {
    // OBSERVE — read-only projection via the provider.
    let obs = match provider.observe(input.target).await {
        Ok(o) => o,
        Err(error) => return TickReceipt::Error { error },
    };
    // PLAN — pure: single-writer guard → band law → directionality → freshness → cooldown.
    let of = provider.owned_field();
    let plan = plan_tick(
        &obs,
        input.cfg,
        provider.directionality(),
        input.in_cooldown,
        &of.manager,
        &of.path,
        input.max_staleness_secs,
    );
    match plan {
        TickPlan::Conflict { manager } => TickReceipt::Conflict { manager },
        TickPlan::Stale { staleness_secs, .. } => TickReceipt::Stale { staleness_secs },
        TickPlan::Cooldown { .. } => TickReceipt::Cooldown,
        TickPlan::Observe { decision } => TickReceipt::Observed { decision },
        // ACT — the ONLY mutation, delegated atomically to the provider.
        TickPlan::Act { decision } => {
            let (from, to) = match decision {
                Decision::Grow { from, to } | Decision::Shrink { from, to } => (from, to),
                // unreachable: plan_tick only emits Act for Grow/Shrink.
                other => return TickReceipt::Observed { decision: other },
            };
            // THE GOLDEN-EDGE GATE: name the precise restart cost of THIS carve
            // (per-direction, per-resource) and refuse a crossing the policy does
            // not permit — never silently roll. A grow is golden (RestartFree); a
            // memory shrink is RestartConditional; a CNPG/template carve is
            // RestartRequiring.
            let growing = matches!(decision, Decision::Grow { .. });
            let class = provider.action_class(input.target, growing);
            if !input.policy.permits(class) {
                return TickReceipt::DeferredWouldRestart { from, to, class };
            }
            if input.dry_run {
                return TickReceipt::DryRunWouldApply { from, to };
            }
            match provider.assign(input.target, to).await {
                Ok(r) => TickReceipt::Applied { from: r.from, to: r.to, class },
                Err(error) => TickReceipt::Error { error },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use breathe_control::FieldOwner;
    use breathe_provider::{mock::MockCluster, BandProvider, Target};
    use breathe_dimensions::MemoryDescriptor;

    const MEMORY_FIELD: &str = "resources.limits.memory";
    const MEMORY_MANAGER: &str = "breathe/memory";
    fn provider(cluster: MockCluster) -> BandProvider<MockCluster, MemoryDescriptor> {
        BandProvider::new(cluster, MemoryDescriptor::default())
    }

    const MI: u64 = 1 << 20;
    const GI: u64 = 1 << 30;

    fn target() -> Target {
        Target {
            namespace: "pangea-system".into(),
            name: "pangea-database".into(),
            kind: "Cluster".into(),
            api_version: "postgresql.cnpg.io/v1".into(),
            container: None,
        }
    }
    fn we_own() -> Vec<FieldOwner> {
        vec![FieldOwner { manager: MEMORY_MANAGER.into(), field: MEMORY_FIELD.into() }]
    }

    // The headline proof: observe → plan_tick → assign flows end-to-end through
    // the trait against a mock, and exactly one true-SSA patch is carved on the
    // memory field by the memory field-manager. No real cluster.
    #[tokio::test]
    async fn reconcile_grows_and_carves_one_ssa_patch() {
        // util ~0.93 @ 1Gi, fresh sample, we own the field → Act(Grow).
        let cluster = MockCluster::new(950 * MI, 0, GI, we_own());
        let prov = provider(cluster);
        let cfg = BandConfig::default();
        let t = target();
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowRestart };

        match reconcile_one(&input, &prov).await {
            TickReceipt::Applied { from, to, .. } => {
                assert_eq!(from, GI);
                assert!(to > from, "must grow");
            }
            other => panic!("expected Applied, got {other:?}"),
        }
        let patches = prov.cluster().applied();
        assert_eq!(patches.len(), 1, "exactly one atomic carve");
        assert_eq!(patches[0].field_manager, MEMORY_MANAGER);
        assert_eq!(patches[0].resource, "memory");
        let _ = MEMORY_FIELD; // owned_field().path label (asserted via the guard tests)
    }

    #[tokio::test]
    async fn reconcile_yields_to_a_competing_owner_without_carving() {
        // VPA owns the memory field → Conflict, and NOTHING is applied.
        let owners = vec![FieldOwner { manager: "vpa".into(), field: MEMORY_FIELD.into() }];
        let prov = provider(MockCluster::new(950 * MI, 0, GI, owners));
        let cfg = BandConfig::default();
        let t = target();
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowRestart };
        assert_eq!(reconcile_one(&input, &prov).await, TickReceipt::Conflict { manager: "vpa".into() });
        assert!(prov.cluster().applied().is_empty(), "must not carve under conflict");
    }

    #[tokio::test]
    async fn reconcile_refuses_stale_and_dry_run_never_carves() {
        let cfg = BandConfig::default();
        let t = target();
        // stale sample (120s > 60s bound) → Stale, no carve.
        let stale = provider(MockCluster::new(950 * MI, 120, GI, we_own()));
        let in_stale = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowRestart };
        assert_eq!(reconcile_one(&in_stale, &stale).await, TickReceipt::Stale { staleness_secs: 120 });
        assert!(stale.cluster().applied().is_empty());

        // dry-run with a real grow signal → DryRunWouldApply, no carve.
        let dry = provider(MockCluster::new(950 * MI, 0, GI, we_own()));
        let in_dry = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: true, policy: DisruptionPolicy::AllowRestart };
        match reconcile_one(&in_dry, &dry).await {
            TickReceipt::DryRunWouldApply { .. } => {}
            other => panic!("expected DryRunWouldApply, got {other:?}"),
        }
        assert!(dry.cluster().applied().is_empty(), "shadow never mutates");
    }

    /// The golden-edge gate: a CNPG `Cluster` target carves at `ClusterTopLevel`
    /// (RestartRequiring), so under the DEFAULT `RestartFreeOnly` policy breathe
    /// DEFERS the carve (never silently rolls) — the workload stays golden,
    /// un-converged, and nothing is applied.
    #[tokio::test]
    async fn reconcile_defers_a_restart_requiring_carve_under_restart_free_only() {
        let prov = provider(MockCluster::new(950 * MI, 0, GI, we_own())); // real Grow signal
        let cfg = BandConfig::default();
        let t = target(); // kind = Cluster ⇒ ClusterTopLevel ⇒ RestartRequiring
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly };
        match reconcile_one(&input, &prov).await {
            TickReceipt::DeferredWouldRestart { from, to, class } => {
                assert_eq!(from, GI);
                assert!(to > from);
                assert_eq!(class, DisruptionClass::RestartRequiring);
            }
            other => panic!("expected DeferredWouldRestart, got {other:?}"),
        }
        assert!(prov.cluster().applied().is_empty(), "a refused crossing carves NOTHING");
    }

    /// The K4 continuity property: under `RestartFreeOnly` EVERY receipt is
    /// GoldenPreserving (a permitted golden carve, or a refusal that KEPT the
    /// workload golden) — zero ceiling crossings. The SAME carve under
    /// `AllowRestart` is a witnessed crossing — the rare, auditable event.
    #[tokio::test]
    async fn golden_continuity_restart_free_only_emits_zero_crossings() {
        let cfg = BandConfig::default();
        let t = target(); // CNPG ClusterTopLevel ⇒ RestartRequiring
        // RestartFreeOnly: the carve is refused → the tick stays golden.
        let prov = provider(MockCluster::new(950 * MI, 0, GI, we_own()));
        let golden = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly };
        assert!(reconcile_one(&golden, &prov).await.edge_tier().is_golden(), "RestartFreeOnly is golden end-to-end");
        // AllowRestart: the same carve is APPLIED as a witnessed ceiling crossing.
        let prov2 = provider(MockCluster::new(950 * MI, 0, GI, we_own()));
        let allow = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowRestart };
        let r = reconcile_one(&allow, &prov2).await;
        assert!(matches!(r, TickReceipt::Applied { class: DisruptionClass::RestartRequiring, .. }));
        assert!(!r.edge_tier().is_golden(), "an AllowRestart roll is a witnessed crossing");
    }
}
