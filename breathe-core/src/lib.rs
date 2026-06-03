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
use breathe_provider::{ProviderError, ResourceProvider, Target};

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
    /// The one atomic mutation was applied via the provider.
    Applied { from: u64, to: u64 },
    /// dry-run: a mutation would have been applied (shadow attestation).
    DryRunWouldApply { from: u64, to: u64 },
    /// An observable, non-mutating outcome (Hold / AtCeiling / NoSafeShrink / NoLimit).
    Observed { decision: Decision },
    /// A typed provider error (transient → fast requeue; permanent → escalate).
    Error { error: ProviderError },
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
            if input.dry_run {
                return TickReceipt::DryRunWouldApply { from, to };
            }
            match provider.assign(input.target, to).await {
                Ok(r) => TickReceipt::Applied { from: r.from, to: r.to },
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
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false };

        match reconcile_one(&input, &prov).await {
            TickReceipt::Applied { from, to } => {
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
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false };
        assert_eq!(reconcile_one(&input, &prov).await, TickReceipt::Conflict { manager: "vpa".into() });
        assert!(prov.cluster().applied().is_empty(), "must not carve under conflict");
    }

    #[tokio::test]
    async fn reconcile_refuses_stale_and_dry_run_never_carves() {
        let cfg = BandConfig::default();
        let t = target();
        // stale sample (120s > 60s bound) → Stale, no carve.
        let stale = provider(MockCluster::new(950 * MI, 120, GI, we_own()));
        let in_stale = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false };
        assert_eq!(reconcile_one(&in_stale, &stale).await, TickReceipt::Stale { staleness_secs: 120 });
        assert!(stale.cluster().applied().is_empty());

        // dry-run with a real grow signal → DryRunWouldApply, no carve.
        let dry = provider(MockCluster::new(950 * MI, 0, GI, we_own()));
        let in_dry = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: true };
        match reconcile_one(&in_dry, &dry).await {
            TickReceipt::DryRunWouldApply { .. } => {}
            other => panic!("expected DryRunWouldApply, got {other:?}"),
        }
        assert!(dry.cluster().applied().is_empty(), "shadow never mutates");
    }
}
