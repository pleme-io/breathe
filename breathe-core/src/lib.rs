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

use breathe_control::{plan_tick, BandConfig, Decision, Directionality, Observation, TickPlan};
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
    /// M0 predictive input — the prior fresh sample + the band's lookahead. The
    /// controller builds this from the band's last history sample when the band
    /// opts into prediction (`predictive: true`); `reconcile_one` measures the
    /// working-set velocity and feeds `PredictiveGrow`. `None` ⇒ plain reactive
    /// carving (the default; behaviour byte-identical to before).
    pub predictive: Option<PredictiveInput>,
    /// The trailing-window PEAK working set (max RSS) the controller has carried
    /// across ticks from the band status — the never-OOM shrink floor is keyed on
    /// THIS, not the instantaneous sample (the authentik-Celery-worker OOM fix). The
    /// controller computes it with `breathe_control::update_peak(prior_peak, used,
    /// decay)`; `reconcile_one` folds it into the observation (`max(used, hint)`) so
    /// no carve can drop the limit below a recently-demonstrated spike. `None` ⇒ no
    /// history (first tick) ⇒ instantaneous-equivalent (peak == used).
    pub peak_used: Option<u64>,
    /// WARMUP: how many seconds the workload has been observed since its last
    /// (re)start — the warmup-gate input (a shrink is held while this is below the
    /// band's `warmup_seconds`; see `breathe_control::clamp_to_warmup`). The
    /// controller computes it from the band's `warmup_start_epoch` status (reset on a
    /// detected restart). `None` ⇒ "warmup not applicable" (`u64::MAX` — host/node
    /// dimensions with no restart concept, and the byte-identical-to-before path for
    /// any band whose `warmup_seconds` is 0).
    pub observed_for_secs: Option<u64>,
    /// **Part 1 (SOFT k8s carve):** force the HARD-plane (the k8s `limits.memory` /
    /// `memory.max`, kill ceiling) to be GROW-ONLY for this tick — an efficiency
    /// shrink of the limit becomes `NoSafeShrink` (never lowered), so the kill ceiling
    /// only ever rises. The shrink pressure is routed to the SOFT `memory.high` plane
    /// by the controller's per-pod `PodMemoryHigh` dispatch (`reconcile_memory`),
    /// NEVER by lowering `memory.max`. `false` (the default) keeps the dimension's own
    /// directionality byte-identical to before (host/cgroup/cpu/storage paths). Set
    /// `true` ONLY for a k8s `MemoryBand` whose soft carve is dispatched separately —
    /// this is the structural pin that makes the k8s plane OOM-impossible.
    pub hard_plane_grow_only: bool,
}

/// The prior fresh sample + lookahead used to compute the working-set rate fed to
/// `PredictiveGrow`. Built by the controller from the band's last `TrendSample`.
pub struct PredictiveInput {
    pub prior_used: u64,
    pub dt_secs: f64,
    pub lookahead_secs: f64,
}

/// The typed per-tick receipt — every branch observable, none silent.
#[derive(Debug, PartialEq, Eq)]
pub enum TickReceipt {
    /// Another manager owns the field — yielded, never fought.
    Conflict { manager: String },
    /// The driving metric reports `used > capacity` — impossible for a true
    /// per-entity gauge, so it is not measuring this entity (local-path PVC →
    /// whole-node-fs bytes). Held + surfaced; NEVER carved on the lie.
    MetricUnrepresentable { used: u64, capacity: u64 },
    /// Mirrors `TickPlan::CapabilityMissing` — a `GrowOnly` dimension's
    /// backing StorageClass cannot support what breathe needs to safely
    /// carve it (no online expansion and/or no per-volume metrics). Reported
    /// as CRD `phase = "Unsupported"` — checked FIRST (before the
    /// single-writer guard), so this is what the operator sees instead of a
    /// `Conflict`/`MetricUnrepresentable` red herring produced by the exact
    /// same underlying gap. NEVER carved.
    CapabilityMissing { volume_expansion: bool, per_volume_metrics: bool, provisioner: String },
    /// A SHRINK was warranted but the workload is still in its WARMUP window (it
    /// (re)started less than `warmup_seconds` ago and hasn't demonstrated a full duty
    /// cycle) — held + surfaced. The idle reading is not yet proof the slack is safe
    /// to reclaim (the un-observed-boot-spike OOM); the band waits until a boot spike
    /// would have been observed before any carve. A grow during warmup still acts.
    Warmup { observed_for: u64, warmup: u64 },
    /// A SHRINK was warranted by the (CFS-capped) usage metric, but the workload's
    /// SUPPRESSED DEMAND is non-blind — it is being actively THROTTLED, or recently
    /// (re)started / crash-looping — so the low usage is a symptom, not proof of safe
    /// slack. HELD + surfaced (the no-starve invariant). This closes the CPU-blindness
    /// ratchet that starved pangea-operator (2026-06): a CFS-throttled workload's
    /// usage can never exceed its limit, so a usage-keyed shrink would ratchet it to
    /// the floor. A grow is never held. `restarting` distinguishes a crash-loop hold
    /// from a live-throttle hold.
    Throttled { restarting: bool },
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
    /// The band's label-selected pod group is currently EMPTY — the target is
    /// DORMANT (an ephemeral runner / Job / scale-to-zero workload with no pod right
    /// now). A benign resting state, NOT an error: nothing to observe, nothing to
    /// carve, the band waits for a pod to appear. Reported as `Dormant`, counted as
    /// at-rest (converged), never as a failure.
    Dormant,
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
            // refusing the crossing KEEPS the workload golden — as does HOLDING a
            // shrink through warmup (no carve, the workload is undisturbed).
            Self::DeferredWouldRestart { .. }
            | Self::DryRunWouldApply { .. }
            | Self::Observed { .. }
            | Self::Dormant
            | Self::Warmup { .. }
            | Self::Throttled { .. }
            | Self::Cooldown
            | Self::Stale { .. }
            | Self::Conflict { .. }
            | Self::MetricUnrepresentable { .. }
            | Self::CapabilityMissing { .. }
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
    let mut obs = match provider.observe(input.target).await {
        Ok(o) => o,
        // A label-selected pod group with zero pods is DORMANT (scaled to zero), a
        // benign resting state — not an error to escalate.
        Err(ProviderError::NoTargetPods) => return outcome(TickReceipt::Dormant, None),
        Err(error) => return outcome(TickReceipt::Error { error }, None),
    };
    // NEVER-OOM-FROM-CARVE: fold the controller's trailing-window peak (carried
    // across ticks from the band status) into the observation. The provider is
    // history-free (`peak_used == used`); here we raise it to the demonstrated peak
    // so `plan_tick`'s shrink-safety floor can never carve under a recent spike
    // (the authentik-Celery-worker OOM). Always ≥ the live `used`, so this can only
    // ever RAISE the floor — non-spiky bands are byte-unchanged.
    obs.peak_used = obs.used.max(input.peak_used.unwrap_or(obs.used));
    // WARMUP: raise the provider's history-free `u64::MAX` to the real observed-
    // since-restart age (the controller carries it from the band's warmup-start
    // epoch). A workload still inside its warmup window has a shrink HELD by
    // `plan_tick` (the un-observed-boot-spike OOM); a grow is never held. `None`
    // keeps `u64::MAX` ⇒ the gate never fires (host/node dimensions, warmup off).
    obs.observed_for_secs = input.observed_for_secs.unwrap_or(u64::MAX);
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
    // M0: measure the working-set velocity from the prior fresh sample to this one,
    // and (only when the band opted into prediction) feed it to `PredictiveGrow`.
    // `rate` is signed bytes/sec; `None` ⇒ plain reactive carving.
    let predictive = input.predictive.as_ref().filter(|p| p.dt_secs > 0.0).map(|p| {
        let rate = (((obs.used as i64) - (p.prior_used as i64)) as f64 / p.dt_secs) as i64;
        (rate, p.lookahead_secs)
    });
    // REQUEST FLOOR: fold the target's LIVE declared `requests.<resource>` into the
    // effective config — the inviolable shrink floor (a limit below the request is
    // invalid in k8s + unsafe). `max(spec.requestFloor, live request)` so the
    // declared request is honored even when the band CR omitted `requestFloor`. The
    // shared `safety_clamp` already enforces it; this just SOURCES it from the live
    // pod. `0` request (host/node, or none declared) leaves the config unchanged.
    let cfg = if obs.request_floor > input.cfg.request_floor_bytes {
        &BandConfig { request_floor_bytes: obs.request_floor, ..input.cfg.clone() }
    } else {
        input.cfg
    };
    // PLAN — pure: single-writer guard → band law → directionality → freshness → cooldown.
    let of = provider.owned_field();
    // HARD-PLANE PIN (Part 1): when the caller routes the efficiency carve to the SOFT
    // memory.high plane, force the HARD limit (memory.max) GROW-ONLY so an efficiency
    // shrink can never lower the kill ceiling. A Bidirectional dimension becomes
    // grow-only for THIS tick; ObserveOnly/GrowOnly are unchanged (already ≤ grow).
    let directionality = if input.hard_plane_grow_only && provider.directionality() == Directionality::Bidirectional {
        Directionality::GrowOnly
    } else {
        provider.directionality()
    };
    let plan = plan_tick(
        &obs,
        cfg,
        directionality,
        input.in_cooldown,
        &of.manager,
        &of.path,
        input.max_staleness_secs,
        predictive,
    );
    let receipt = match plan {
        TickPlan::Conflict { manager } => TickReceipt::Conflict { manager },
        TickPlan::Unrepresentable { used, capacity } => {
            TickReceipt::MetricUnrepresentable { used, capacity }
        }
        TickPlan::CapabilityMissing { volume_expansion, per_volume_metrics, provisioner } => {
            TickReceipt::CapabilityMissing { volume_expansion, per_volume_metrics, provisioner }
        }
        TickPlan::Warmup { observed_for, warmup, .. } => TickReceipt::Warmup { observed_for, warmup },
        TickPlan::Throttled { restarting, .. } => TickReceipt::Throttled { restarting },
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
    use breathe_provider::{mock::MockCluster, BandProvider, DimensionDescriptor, StorageCapability, Target};
    use breathe_dimensions::{MemoryDescriptor, StorageDescriptor};

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
            pod_selector: None,
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
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowRestart, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };

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
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowRestart, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
        assert_eq!(reconcile_one(&input, &prov).await.receipt, TickReceipt::Conflict { manager: "vpa".into() });
        assert!(prov.cluster().applied().is_empty(), "must not carve under conflict");
    }

    #[tokio::test]
    async fn reconcile_refuses_stale_and_dry_run_never_carves() {
        let cfg = BandConfig::default();
        let t = target();
        // stale sample (120s > 60s bound) → Stale, no carve.
        let stale = provider(MockCluster::new(950 * MI, 120, GI, we_own()));
        let in_stale = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowRestart, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
        assert_eq!(reconcile_one(&in_stale, &stale).await.receipt, TickReceipt::Stale { staleness_secs: 120 });
        assert!(stale.cluster().applied().is_empty());

        // dry-run with a real grow signal → DryRunWouldApply, no carve.
        let dry = provider(MockCluster::new(950 * MI, 0, GI, we_own()));
        let in_dry = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: true, policy: DisruptionPolicy::AllowRestart, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
        match reconcile_one(&in_dry, &dry).await.receipt {
            TickReceipt::DryRunWouldApply { .. } => {}
            other => panic!("expected DryRunWouldApply, got {other:?}"),
        }
        assert!(dry.cluster().applied().is_empty(), "shadow never mutates");
    }

    /// A label-selected pod group with zero pods (an ARC runner between builds, a
    /// scaled-to-zero workload) surfaces as `NoTargetPods` from the metric read —
    /// the loop maps it to the benign `Dormant` receipt (not `Error`), carves
    /// nothing, and stays on golden rails.
    #[tokio::test]
    async fn reconcile_maps_empty_pod_group_to_dormant_not_error() {
        use breathe_provider::EdgeTier;
        let cluster = MockCluster::new(0, 0, 0, we_own())
            .with_read_used_error(ProviderError::NoTargetPods);
        let prov = provider(cluster);
        let cfg = BandConfig::default();
        let t = target();
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowRestart, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
        let out = reconcile_one(&input, &prov).await;
        assert_eq!(out.receipt, TickReceipt::Dormant);
        assert_eq!(out.receipt.edge_tier(), EdgeTier::GoldenPreserving);
        assert!(out.observed.is_none(), "dormant has no observation");
        assert!(prov.cluster().applied().is_empty(), "dormant never carves");
        // a genuine metric outage (pods exist, usage unreadable) is STILL an Error —
        // only an empty selector group is dormant.
        let broken = provider(MockCluster::new(0, 0, 0, we_own()).with_read_used_error(ProviderError::MetricsMissing));
        assert_eq!(
            reconcile_one(&input, &broken).await.receipt,
            TickReceipt::Error { error: ProviderError::MetricsMissing }
        );
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
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
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
        let golden = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
        assert!(reconcile_one(&golden, &prov).await.receipt.edge_tier().is_golden(), "RestartFreeOnly is golden end-to-end");
        // AllowRestart: the same carve is APPLIED as a witnessed ceiling crossing.
        let prov2 = provider(MockCluster::new(950 * MI, 0, GI, we_own()));
        let allow = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowRestart, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
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
            pod_selector: None,
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
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
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
        let strict = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
        match reconcile_one(&strict, &prov).await.receipt {
            TickReceipt::DeferredWouldRestart { class, .. } => {
                assert_eq!(class, DisruptionClass::RestartConditional, "a may-restart shrink under the strict policy");
            }
            other => panic!("expected DeferredWouldRestart(RestartConditional), got {other:?}"),
        }
        assert!(prov.cluster().applied().is_empty(), "a deferred conditional shrink carves nothing");
        // AllowConditional opts the same RestartConditional shrink in ⇒ Applied.
        let prov2 = resize_provider(MockCluster::new(400 * MI, 0, 2 * GI, we_own()));
        let allow = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::AllowConditional, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
        assert!(matches!(
            reconcile_one(&allow, &prov2).await.receipt,
            TickReceipt::Applied { class: DisruptionClass::RestartConditional, .. }
        ));
    }

    // ── PART 3: the LIVE requests.<resource> floor is honored end-to-end ───────

    /// A shrink can NEVER carve the limit below the target's LIVE declared
    /// `requests.memory` — even when the band CR omits `requestFloor`. The provider
    /// reads the live request (`MockCluster::with_request_floor`); `reconcile_one`
    /// folds it into the effective config; `safety_clamp` enforces it. This is the
    /// Part-3 end-to-end wiring proof (the floor was previously honored only when
    /// the operator hand-declared it in the band spec).
    #[tokio::test]
    async fn reconcile_honors_the_live_request_floor_even_when_the_cr_omits_it() {
        // band CR declares NO requestFloor (cfg.request_floor_bytes == 0), but the
        // live pod declares requests.memory = 1Gi. util 0.05 @ 2Gi ⇒ a hard shrink
        // that, unclamped, would carve well below 1Gi — the live floor must bind.
        let cfg = BandConfig { request_floor_bytes: 0, ..BandConfig::default() };
        assert_eq!(cfg.request_floor_bytes, 0, "the band CR declares no request floor");
        let t = deploy_target();
        let cluster = MockCluster::new(100 * MI, 0, 2 * GI, we_own())
            .with_resize_restart_free(true) // golden in-place shrink so it Acts
            .with_request_floor(GI); // the LIVE pod request the controller must read
        let prov = resize_provider(cluster);
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
        match reconcile_one(&input, &prov).await.receipt {
            TickReceipt::Applied { to, .. } => {
                assert!(to >= GI, "the live 1Gi request floor must bind: shrank to {to} < 1Gi");
            }
            TickReceipt::Observed { decision: Decision::NoSafeShrink { .. } } => {} // also acceptable
            other => panic!("expected a request-floor-bound shrink, got {other:?}"),
        }
    }

    // ── PART 2: warmup-hold through the full reconcile (the authentik fix) ─────

    /// A workload restarted < warmup ago is NEVER carved, no matter how idle —
    /// surfaced as the typed `Warmup` receipt, golden-preserving (nothing applied).
    #[tokio::test]
    async fn reconcile_holds_a_warming_up_workload_and_carves_nothing() {
        use breathe_provider::EdgeTier;
        let cfg = BandConfig { warmup_seconds: 600, ..BandConfig::default() };
        let t = deploy_target();
        // util 0.05 @ 2Gi ⇒ a strong shrink signal, but observed for only 60s.
        let prov = resize_provider(MockCluster::new(100 * MI, 0, 2 * GI, we_own()).with_resize_restart_free(true));
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None, observed_for_secs: Some(60), hard_plane_grow_only: false };
        let out = reconcile_one(&input, &prov).await;
        assert!(matches!(out.receipt, TickReceipt::Warmup { observed_for: 60, warmup: 600 }), "warming up ⇒ held, got {:?}", out.receipt);
        assert_eq!(out.receipt.edge_tier(), EdgeTier::GoldenPreserving, "a warmup hold keeps the workload golden");
        assert!(prov.cluster().applied().is_empty(), "warmup never carves");
        // past warmup (observed_for 601 > 600), the SAME idle workload carves.
        let prov2 = resize_provider(MockCluster::new(100 * MI, 0, 2 * GI, we_own()).with_resize_restart_free(true));
        let warm = ReconcileInput { observed_for_secs: Some(601), ..ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false } };
        assert!(matches!(reconcile_one(&warm, &prov2).await.receipt, TickReceipt::Applied { .. }), "past warmup the idle workload carves");
    }

    // ── PART 1 (k8s plane): the HARD-plane pin — memory.max is never lowered ───

    /// THE PART-1 INTEGRATION PROOF: with `hard_plane_grow_only` set (the MemoryBand
    /// soft-carve routing), an IDLE in-place memory band NEVER carves the HARD
    /// `limits.memory` (`memory.max`) down for efficiency — the tick is `NoSafeShrink`
    /// (the kill ceiling is held), and NOTHING is applied to `limits.memory`. The
    /// reclaim is routed to the SOFT `memory.high` plane by the controller's per-pod
    /// dispatch (proven separately in breathe-controller's `pod_memory_high` tests).
    #[tokio::test]
    async fn reconcile_hard_plane_pin_never_shrinks_memory_max_for_efficiency() {
        let cfg = BandConfig::default();
        let t = deploy_target();
        // util 0.05 @ 2Gi ⇒ a STRONG idle shrink signal; resizePolicy NotRequired so
        // it WOULD be a golden in-place shrink — but the hard-plane pin holds it.
        let prov = resize_provider(MockCluster::new(100 * MI, 0, 2 * GI, we_own()).with_resize_restart_free(true));
        let pinned = ReconcileInput {
            target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false,
            policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None,
            observed_for_secs: None, hard_plane_grow_only: true,
        };
        match reconcile_one(&pinned, &prov).await.receipt {
            // the HARD memory.max efficiency shrink is suppressed to NoSafeShrink.
            TickReceipt::Observed { decision: Decision::NoSafeShrink { current } } => assert_eq!(current, 2 * GI),
            other => panic!("the hard plane must NOT shrink memory.max for efficiency, got {other:?}"),
        }
        assert!(prov.cluster().applied().is_empty(), "memory.max is never carved down for efficiency");

        // CONTRAST: the SAME idle band WITHOUT the pin (default) DOES carve memory.max
        // down (the legacy single-limit behaviour) — proving the pin is what holds it.
        let prov2 = resize_provider(MockCluster::new(100 * MI, 0, 2 * GI, we_own()).with_resize_restart_free(true));
        let unpinned = ReconcileInput { hard_plane_grow_only: false, ..ReconcileInput {
            target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false,
            policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None,
            observed_for_secs: None, hard_plane_grow_only: false,
        } };
        assert!(
            matches!(reconcile_one(&unpinned, &prov2).await.receipt, TickReceipt::Applied { .. }),
            "without the pin the legacy path DOES shrink the limit — the pin is load-bearing"
        );
    }

    /// The HARD-plane pin NEVER blocks a GROW — a memory band under pressure still
    /// raises `memory.max` (buying kill-ceiling headroom is always the safe direction).
    #[tokio::test]
    async fn reconcile_hard_plane_pin_still_grows_memory_max_under_pressure() {
        let cfg = BandConfig::default();
        let t = deploy_target();
        // util 0.93 @ 1Gi ⇒ grow; the pin only suppresses shrinks, never grows.
        let prov = resize_provider(MockCluster::new(950 * MI, 0, GI, we_own()).with_resize_restart_free(true));
        let input = ReconcileInput {
            target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false,
            policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None,
            observed_for_secs: None, hard_plane_grow_only: true,
        };
        assert!(
            matches!(reconcile_one(&input, &prov).await.receipt, TickReceipt::Applied { from: GI, .. }),
            "the hard-plane pin must still let memory.max GROW under pressure"
        );
    }

    /// A GROW during warmup STILL acts — refusing headroom at boot would itself OOM.
    #[tokio::test]
    async fn reconcile_warmup_never_blocks_a_grow() {
        let cfg = BandConfig { warmup_seconds: 600, ..BandConfig::default() };
        let t = deploy_target();
        // util 0.93 @ 1Gi, restarted 5s ago ⇒ STILL grows (in-place, golden).
        let prov = resize_provider(MockCluster::new(950 * MI, 0, GI, we_own()).with_resize_restart_free(true));
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None, observed_for_secs: Some(5), hard_plane_grow_only: false };
        assert!(matches!(reconcile_one(&input, &prov).await.receipt, TickReceipt::Applied { from: GI, .. }), "a grow at boot must still act");
    }

    // ── CPU-BLINDNESS / NO-STARVE end-to-end (the pangea-operator 2026-06 fix) ────

    /// THE OPERATOR CASE end-to-end through the provider + cluster: a CPU band with
    /// an IDLE observed usage but ACTIVE CFS throttling (the descriptor's throttle
    /// source read via `MockCluster::with_throttle_source`) must NEVER carve the
    /// limit down — it grows out of the cap instead. The reconcile-path proof that
    /// breathe will not starve a throttled CPU workload the way it starved
    /// pangea-operator (1000m→283m). Uses the real `CpuDescriptor` so the throttle
    /// read fires exactly as it would on-cluster.
    #[tokio::test]
    async fn reconcile_never_starves_an_idle_but_throttled_cpu_workload() {
        use breathe_dimensions::CpuDescriptor;
        const CPU_FIELD: &str = "resources.limits.cpu";
        let cfg = BandConfig { floor_bytes: 100, ceiling_bytes: 4000, ..BandConfig::default() };
        let t = deploy_target();
        let cpu = CpuDescriptor::with_resize_capability(true);
        // the exact throttle source the descriptor will ask the cluster to read.
        let throttle_src = cpu.throttle_source(&t).expect("cpu has a throttle source");
        let cpu_owns = vec![FieldOwner { manager: "breathe/cpu".into(), field: CPU_FIELD.into() }];
        // idle 50m @ 1000m (a STRONG idle shrink signal) BUT actively throttled.
        let cluster = MockCluster::new(50, 0, 1000, cpu_owns.clone())
            .with_resize_restart_free(true)
            .with_throttle_source(throttle_src)
            .with_throttle(5);
        let prov = BandProvider::new(cluster, cpu);
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
        let out = reconcile_one(&input, &prov).await;
        // never a shrink — it grows out of the cap (or holds); both honor no-starve.
        match out.receipt {
            TickReceipt::Applied { from, to, .. } => {
                assert_eq!(from, 1000);
                assert!(to > from, "a throttled idle cpu workload must GROW out of the cap, not shrink");
            }
            TickReceipt::Throttled { .. } => {}
            other => panic!("throttled idle cpu workload must grow or hold (never starve), got {other:?}"),
        }
        // CRUCIAL: nothing was carved DOWN.
        for p in &prov.cluster().applied() {
            assert!(p.value >= 1000, "a throttled cpu carve must never lower the limit, wrote {}", p.value);
        }

        // CONTRAST: the SAME idle reading WITHOUT a throttle signal DOES shrink
        // (legacy path), proving the throttle read is exactly what holds the invariant.
        let cpu2 = CpuDescriptor::with_resize_capability(true);
        let src2 = cpu2.throttle_source(&t).unwrap();
        let cluster2 = MockCluster::new(50, 0, 1000, cpu_owns)
            .with_resize_restart_free(true)
            .with_throttle_source(src2); // registered but throttle_signal stays 0
        let prov2 = BandProvider::new(cluster2, cpu2);
        let input2 = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
        assert!(
            matches!(reconcile_one(&input2, &prov2).await.receipt, TickReceipt::Applied { from, to, .. } if to < from),
            "without a throttle signal the idle cpu workload shrinks (the signal is load-bearing)"
        );
    }

    /// A crash-looping (restarting) workload is never starved end-to-end either —
    /// `read_restarting` flows from the cluster through the provider into the no-starve
    /// path. `restarting` is descriptor-independent (read for every dimension), so a
    /// memory band proves it too. The outcome is grow-or-hold — NEVER a shrink (the
    /// demand lift grows out of the idle reading; the gate holds it if it doesn't).
    #[tokio::test]
    async fn reconcile_never_starves_a_crash_looping_workload() {
        let cfg = BandConfig::default();
        let t = deploy_target();
        // idle + restarting, no live throttle ⇒ the no-starve path holds/grows, never shrinks.
        let cluster = MockCluster::new(100 * MI, 0, 2 * GI, we_own())
            .with_resize_restart_free(true)
            .with_restarting(true);
        let prov = resize_provider(cluster);
        let input = ReconcileInput { target: &t, cfg: &cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false };
        let out = reconcile_one(&input, &prov).await;
        // never a shrink: a crash-looping idle workload grows out (demand lift) or holds.
        assert!(
            !matches!(out.receipt, TickReceipt::Applied { from, to, .. } if to < from),
            "a crash-looping workload must NEVER be carved down, got {:?}", out.receipt
        );
        for p in &prov.cluster().applied() {
            assert!(p.value >= 2 * GI, "a crash-loop carve must never lower the limit, wrote {}", p.value);
        }
    }

    // ── STORAGE CAPABILITY GATE end-to-end (the fail-fast fix) ───────────────

    fn pvc_target(name: &str) -> Target {
        Target {
            namespace: "akeyless".into(),
            name: name.into(),
            kind: "PersistentVolumeClaim".into(),
            api_version: String::new(),
            container: None,
            pod_selector: None,
        }
    }
    fn unsupported_cap() -> StorageCapability {
        StorageCapability { volume_expansion: false, per_volume_metrics: false, provisioner: "rancher.io/local-path".into() }
    }
    fn storage_input<'a>(t: &'a Target, cfg: &'a BandConfig) -> ReconcileInput<'a> {
        ReconcileInput { target: t, cfg, max_staleness_secs: 60, in_cooldown: false, dry_run: false, policy: DisruptionPolicy::RestartFreeOnly, force: None, predictive: None, peak_used: None, observed_for_secs: None, hard_plane_grow_only: false }
    }

    /// The `rustfs-data-storage` case: no competing field manager, but `local-path`
    /// can neither online-expand nor report a trustworthy per-volume metric. The
    /// fix catches this at capability-discovery time — never carved.
    #[tokio::test]
    async fn reconcile_flags_an_unsupported_storage_class_as_capability_missing_never_carving() {
        let cluster = MockCluster::new(GI, 0, GI, Vec::new()).with_storage_capability(Some(unsupported_cap()));
        let prov = BandProvider::new(cluster, StorageDescriptor);
        let cfg = BandConfig::default();
        let t = pvc_target("rustfs-data-storage");
        let out = reconcile_one(&storage_input(&t, &cfg), &prov).await;
        match out.receipt {
            TickReceipt::CapabilityMissing { volume_expansion, per_volume_metrics, provisioner } => {
                assert!(!volume_expansion);
                assert!(!per_volume_metrics);
                assert_eq!(provisioner, "rancher.io/local-path");
            }
            other => panic!("expected CapabilityMissing, got {other:?}"),
        }
        assert!(prov.cluster().applied().is_empty(), "an unsupported StorageClass must never be carved");
    }

    /// THE collapse this fix exists for — the `data-mysql-0-storage` case: k3s's own
    /// controller-manager owns the field (the real Camelot shape). WITHOUT the
    /// capability gate this would reconcile to `Conflict`; WITH it, the IDENTICAL
    /// StorageClass gap reports the SAME `CapabilityMissing` terminal as the
    /// no-competing-owner case above — never a field-ownership red herring.
    #[tokio::test]
    async fn reconcile_capability_missing_beats_a_competing_field_manager() {
        let competing = vec![FieldOwner { manager: "k3s".into(), field: "spec.resources.requests.storage".into() }];
        let cluster = MockCluster::new(GI, 0, GI, competing).with_storage_capability(Some(unsupported_cap()));
        let prov = BandProvider::new(cluster, StorageDescriptor);
        let cfg = BandConfig::default();
        let t = pvc_target("data-mysql-0-storage");
        match reconcile_one(&storage_input(&t, &cfg), &prov).await.receipt {
            TickReceipt::CapabilityMissing { .. } => {}
            other => panic!(
                "expected CapabilityMissing (never Conflict) for a competing-owner + unsupported StorageClass, got {other:?}"
            ),
        }
    }

    /// The regression guard: a real elastic StorageClass must still carve normally —
    /// the gate is additive, never a tax on the golden path.
    #[tokio::test]
    async fn reconcile_never_gates_a_supported_storage_class() {
        let supported = StorageCapability { volume_expansion: true, per_volume_metrics: true, provisioner: "ebs.csi.aws.com".into() };
        let cluster = MockCluster::new(950 * MI, 0, GI, Vec::new()).with_storage_capability(Some(supported));
        let prov = BandProvider::new(cluster, StorageDescriptor);
        let cfg = BandConfig::default();
        let t = pvc_target("elastic-data");
        match reconcile_one(&storage_input(&t, &cfg), &prov).await.receipt {
            TickReceipt::CapabilityMissing { .. } => panic!("a supported StorageClass must never be gated"),
            _ => {} // Applied / Observed / etc. — whatever the band law decides, just not gated.
        }
    }
}
