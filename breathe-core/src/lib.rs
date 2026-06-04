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

use breathe_control::{plan_tick, BandConfig, Decision, Observation, TickPlan};
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
    /// BREAK-GLASS: when `Some`, skip the band law and carve to exactly this value
    /// (already in the band's base unit) — still THROUGH the gate (policy + the L2
    /// clamp in `assign` apply; it cannot bypass safety). `None` = normal homeostasis.
    pub force: Option<u64>,
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

/// The full result of one tick — the [`TickReceipt`] (what happened) PLUS the
/// `Observation` that drove it (`used`/`capacity`/`staleness`) and the effective
/// `policy`/`dry_run`. This is the KEYSTONE for observability: `status_for`,
/// `event_for`, and `metrics_for` all `match` on this ONE value, so the status,
/// the k8s Events, and the Prometheus series can never disagree about what a tick
/// meant — and the observed utilization (`used/capacity`) finally reaches the
/// surface instead of being discarded after `plan_tick`. `observed` is `None`
/// ONLY on the pre-observe Error arm (there is no reading yet).
#[derive(Debug)]
pub struct TickOutcome {
    pub receipt: TickReceipt,
    pub observed: Option<Observation>,
    pub policy: DisruptionPolicy,
    pub dry_run: bool,
}

/// One reconcile tick for one `(target × dimension)`.
pub async fn reconcile_one(
    input: &ReconcileInput<'_>,
    provider: &dyn ResourceProvider,
) -> TickOutcome {
    let outcome = |receipt, observed| TickOutcome {
        receipt,
        observed,
        policy: input.policy,
        dry_run: input.dry_run,
    };
    // OBSERVE — read-only projection via the provider.
    let obs = match provider.observe(input.target).await {
        Ok(o) => o,
        Err(error) => return outcome(TickReceipt::Error { error }, None),
    };
    // BREAK-GLASS forceLimit: skip the band law and carve to the pinned value — but
    // STILL through the gate (the DisruptionPolicy check + the L2 clamp inside
    // `assign` both apply; a pin can never bypass safety or the node ceiling).
    if let Some(forced) = input.force {
        let to = forced.clamp(input.cfg.floor_bytes, input.cfg.ceiling_bytes);
        let from = obs.capacity;
        let receipt = if to == from {
            TickReceipt::Observed { decision: Decision::Hold } // already pinned
        } else {
            let class = provider
                .action_class(input.target, to > from)
                .refined_by_resize_policy(obs.memory_shrink_restart_free);
            if !input.policy.permits(class) {
                TickReceipt::DeferredWouldRestart { from, to, class }
            } else if input.dry_run {
                TickReceipt::DryRunWouldApply { from, to }
            } else {
                match provider.assign(input.target, to).await {
                    Ok(r) => TickReceipt::Applied { from: r.from, to: r.to, class },
                    Err(error) => TickReceipt::Error { error },
                }
            }
        };
        return outcome(receipt, Some(obs));
    }
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
    let receipt = match plan {
        TickPlan::Conflict { manager } => TickReceipt::Conflict { manager },
        TickPlan::Stale { staleness_secs, .. } => TickReceipt::Stale { staleness_secs },
        TickPlan::Cooldown { .. } => TickReceipt::Cooldown,
        TickPlan::Observe { decision } => TickReceipt::Observed { decision },
        // ACT — the ONLY mutation, delegated atomically to the provider.
        TickPlan::Act { decision } => match decision {
            Decision::Grow { from, to } | Decision::Shrink { from, to } => {
                // THE GOLDEN-EDGE GATE: name the precise restart cost of THIS carve
                // (per-direction, per-resource) and refuse a crossing the policy
                // does not permit — never silently roll. A grow is golden
                // (RestartFree); a memory shrink is RestartConditional UNLESS the
                // observed resizePolicy is NotRequired (then in-place → golden); a
                // CNPG/template carve is RestartRequiring.
                let growing = matches!(decision, Decision::Grow { .. });
                let class = provider
                    .action_class(input.target, growing)
                    .refined_by_resize_policy(obs.memory_shrink_restart_free);
                if !input.policy.permits(class) {
                    TickReceipt::DeferredWouldRestart { from, to, class }
                } else if input.dry_run {
                    TickReceipt::DryRunWouldApply { from, to }
                } else {
                    match provider.assign(input.target, to).await {
                        Ok(r) => TickReceipt::Applied { from: r.from, to: r.to, class },
                        Err(error) => TickReceipt::Error { error },
                    }
                }
            }
            // unreachable: plan_tick only emits Act for Grow/Shrink.
            other => TickReceipt::Observed { decision: other },
        },
    };
    outcome(receipt, Some(obs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use breathe_control::FieldOwner;
    use breathe_provider::{mock::MockCluster, BandProvider, DimensionDescriptor, Target};
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
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowRestart, force: None };

        match reconcile_one(&input, &prov).await.receipt {
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
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowRestart, force: None };
        assert_eq!(reconcile_one(&input, &prov).await.receipt, TickReceipt::Conflict { manager: "vpa".into() });
        assert!(prov.cluster().applied().is_empty(), "must not carve under conflict");
    }

    #[tokio::test]
    async fn reconcile_refuses_stale_and_dry_run_never_carves() {
        let cfg = BandConfig::default();
        let t = target();
        // stale sample (120s > 60s bound) → Stale, no carve.
        let stale = provider(MockCluster::new(950 * MI, 120, GI, we_own()));
        let in_stale = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowRestart, force: None };
        assert_eq!(reconcile_one(&in_stale, &stale).await.receipt, TickReceipt::Stale { staleness_secs: 120 });
        assert!(stale.cluster().applied().is_empty());

        // dry-run with a real grow signal → DryRunWouldApply, no carve.
        let dry = provider(MockCluster::new(950 * MI, 0, GI, we_own()));
        let in_dry = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: true, policy: DisruptionPolicy::AllowRestart, force: None };
        match reconcile_one(&in_dry, &dry).await.receipt {
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
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None };
        match reconcile_one(&input, &prov).await.receipt {
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
        let golden = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None };
        assert!(reconcile_one(&golden, &prov).await.receipt.edge_tier().is_golden(), "RestartFreeOnly is golden end-to-end");
        // AllowRestart: the same carve is APPLIED as a witnessed ceiling crossing.
        let prov2 = provider(MockCluster::new(950 * MI, 0, GI, we_own()));
        let allow = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowRestart, force: None };
        let r = reconcile_one(&allow, &prov2).await.receipt;
        assert!(matches!(r, TickReceipt::Applied { class: DisruptionClass::RestartRequiring, .. }));
        assert!(!r.edge_tier().is_golden(), "an AllowRestart roll is a witnessed crossing");
    }

    // ── Phase 2: resizePolicy-aware shrink — memory breathes DOWN on golden rails ──

    fn deploy_target() -> Target {
        Target {
            namespace: "pangea-system".into(),
            name: "api".into(),
            kind: "Deployment".into(),
            api_version: "apps/v1".into(),
            container: Some("app".into()),
        }
    }
    /// A resize-capable memory provider (k8s ≥1.33) over a Deployment ⇒ the carve
    /// layout is `PodResize`, so a memory shrink is `RestartConditional` UNLESS the
    /// observed `resizePolicy[memory]` is `NotRequired`.
    fn resize_provider(cluster: MockCluster) -> BandProvider<MockCluster, MemoryDescriptor> {
        BandProvider::new(cluster, MemoryDescriptor::with_resize_capability(true))
    }

    /// A `resizePolicy[memory] = NotRequired` pod's memory SHRINK is `RestartFree`,
    /// so breathe ACTS on it even under the strict `RestartFreeOnly` default —
    /// memory now breathes bidirectionally on golden rails (the Phase 2 win).
    #[tokio::test]
    async fn reconcile_acts_on_a_not_required_memory_shrink_under_restart_free_only() {
        // util 0.20 @ 2Gi (well below shrink_below) ⇒ Act(Shrink); we own the field;
        // the pod's resizePolicy is NotRequired ⇒ the in-place shrink is golden.
        let cluster = MockCluster::new(400 * MI, 0, 2 * GI, we_own()).with_resize_restart_free(true);
        let prov = resize_provider(cluster);
        let cfg = BandConfig::default();
        let t = deploy_target();
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None };
        match reconcile_one(&input, &prov).await.receipt {
            TickReceipt::Applied { from, to, class } => {
                assert_eq!(from, 2 * GI);
                assert!(to < from, "a shrink lowers the limit");
                assert_eq!(class, DisruptionClass::RestartFree, "NotRequired ⇒ the shrink is golden");
            }
            other => panic!("expected Applied(RestartFree shrink), got {other:?}"),
        }
        assert_eq!(prov.cluster().applied().len(), 1, "exactly one in-place shrink carved");
    }

    /// The SAME shrink with the default `resizePolicy` (`RestartContainer` ⇒ flag
    /// false) is `RestartConditional`, so under `RestartFreeOnly` it DEFERS — never
    /// a silent container restart. Under `AllowConditional` the operator opts in and
    /// the same carve Acts.
    #[tokio::test]
    async fn reconcile_defers_a_restart_container_memory_shrink_under_restart_free_only() {
        let cfg = BandConfig::default();
        let t = deploy_target();
        // RestartFreeOnly + RestartContainer (flag false) ⇒ DEFER, carve nothing.
        let prov = resize_provider(MockCluster::new(400 * MI, 0, 2 * GI, we_own()));
        let strict = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None };
        match reconcile_one(&strict, &prov).await.receipt {
            TickReceipt::DeferredWouldRestart { class, .. } => {
                assert_eq!(class, DisruptionClass::RestartConditional, "a may-restart shrink under the strict policy");
            }
            other => panic!("expected DeferredWouldRestart(RestartConditional), got {other:?}"),
        }
        assert!(prov.cluster().applied().is_empty(), "a deferred conditional shrink carves nothing");
        // AllowConditional opts the same RestartConditional shrink in ⇒ Applied.
        let prov2 = resize_provider(MockCluster::new(400 * MI, 0, 2 * GI, we_own()));
        let allow = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowConditional, force: None };
        assert!(matches!(
            reconcile_one(&allow, &prov2).await.receipt,
            TickReceipt::Applied { class: DisruptionClass::RestartConditional, .. }
        ));
    }
}
