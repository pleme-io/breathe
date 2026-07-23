//! `breathe-controller` — the multi-dimension homeostasis controller.
//!
//! Runs one kube `Controller` per band kind (MemoryBand / CpuBand / StorageBand),
//! all driving a single generic `reconcile<B: Band, D: DimensionDescriptor>`:
//! observe → plan → assign via `breathe_core::reconcile_one` over the dimension's
//! `BandProvider`. Adding a dimension is one more `Controller` line + the matching
//! descriptor — the reconcile body never changes.
//!
//! Config via env:
//!   BREATHE_PROMETHEUS_URL  — PromQL endpoint for the storage dimension
//!   BREATHE_REQUEUE_SECONDS — refresh interval (default 60)

use std::{sync::Arc, time::Duration};

mod app_band;
mod eks_nodegroup_provedor;
mod karpenter_provedor;
mod nats_trigger;
mod node_forma;
mod origin_guard;
mod kube_param;
mod quinhao;
mod pod_memory_high;
mod replica_band;

use breathe_core::{reconcile_one, PredictiveInput, ReconcileInput};
use breathe_crd::{
    AppBand, ArcBand, Band, BandSummary, BreatheCloudPool, BreatheConfig, BreatheConfigSpec, BreatheOverview,
    CgroupBand, CgroupCpuBand, CpuBand, Densa, IsolationBand, KubeParamBand, MemoryBand, OverviewStatus, QuinhaoPool,
    ReplicaBand, StorageBand,
};
use breathe_dimensions::{CpuDescriptor, MemoryDescriptor, StorageDescriptor};
use breathe_kube::KubeCluster;
use breathe_provider::{BandProvider, ClassCooldowns, DimensionDescriptor, ResourceProvider, Target};
use breathe_runtime::{
    apply_env_context, counters_from_status, entry_for, error_status, event_for, health_verdict, metrics_for,
    next_requeue, now_rfc3339, now_secs, patch_status, patch_status_if_changed, rfc3339_in_future, should_emit_event,
    should_emit_health_event, health_event_for, status_for, suspended_status, STUCK_AFTER_SECS,
    BandLabels, CumulativeCounters, EnvContext, EventKind,
};
use breathe_store::{BandRef, DecisionLog, InMemDecisionLog, InMemSampleCache, Sample, SampleCache};
use breathe_config::{CacheConfig, CoordinationConfig, ScaleConfig, StoreConfig};
use k8s_openapi::api::core::v1::Namespace;
use futures::StreamExt;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::Action,
        events::{Event, EventType, Recorder, Reporter},
        predicates, reflector, watcher, Controller, WatchStreamExt,
    },
    Client, ResourceExt,
};
use serde_json::json;
use tracing::{error, info, warn};

/// Build a `Controller` whose PRIMARY watch ignores its own status self-patches.
/// `predicates::generation` triggers only on spec/generation changes, so a status
/// write never re-fires the watch — the structural fix for the whole self-trigger
/// hot-loop class. The reflector store (fed by `reflect` BEFORE the predicate) keeps
/// the reconcile's view of the object fresh; `Action::requeue` still drives the
/// periodic refresh; spec edits still reconcile immediately.
macro_rules! gen_controller {
    ($api:expr) => {{
        let (reader, writer) = reflector::store();
        let stream = watcher($api, watcher::Config::default())
            .reflect(writer)
            .applied_objects()
            .predicate_filter(predicates::generation);
        Controller::for_stream(stream, reader)
    }};
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("kube: {0}")]
    Kube(#[from] kube::Error),
}

struct Ctx {
    client: Client,
    prometheus_url: String,
    requeue: Duration,
    /// The cluster exposes `pods/resize` (k8s ≥1.33) → memory/cpu carve a
    /// pod-backed workload IN PLACE (zero restart) instead of rolling it. The K1
    /// "breathe never rolls" default — detected once at startup.
    resize_capable: bool,
    /// Per-restart-class requeue cadence — golden carves re-tick near the scrape
    /// interval; refused crossings back off.
    cooldowns: ClassCooldowns,
    /// The k8s Event reporter identity (controller name + pod instance) — every
    /// carve/defer/conflict is published as an Event on the band object.
    reporter: Reporter,
    /// Per-`BreatheCloudPool` demand forecasters, keyed by pool name. A
    /// `LinearTrendPrevisor` is stateful (it accumulates a sample window across
    /// ticks), so it must outlive a single reconcile — it lives here, fetched +
    /// fed once per reconcile when the pool sets `spec.predictive`.
    forecasters: std::sync::Mutex<std::collections::HashMap<String, Arc<breathe_auction::LinearTrendPrevisor>>>,
    /// The durable-store seam (M0; docs/BREATHE-MICROSERVICE.md). `decisions` is
    /// the single counter-accumulation point + the append-only decision feed;
    /// `samples` is the predictive prior-sample cache. Held behind `&dyn` so the
    /// VERY-SMALL `InMem*` tier swaps for the Postgres/Redis tier (M2/M3) by
    /// config with no reconcile-loop change.
    decisions: Arc<dyn DecisionLog>,
    samples: Arc<dyn SampleCache>,
    /// The `EksManagedNodegroup` backend's AWS boundary (task #205) —
    /// `DescribeNodegroup`/`UpdateNodegroupConfig` against the real EKS
    /// control plane. Built ONCE at startup via the standard SDK
    /// credential+region chain (`AWS_PROFILE` locally / IRSA in-cluster —
    /// the same chain `crates/breathe-lifecycle`'s AWS integration test
    /// documents) and cloned per-reconcile like `client` — cheap: an
    /// `aws_sdk_eks::Client` is a thin `Arc`-backed handle, and
    /// `aws_config::ConfigLoader::load()` resolves no credentials at load
    /// time (lazy — the first real API call is where auth is actually
    /// checked), so building it costs nothing on clusters that never
    /// declare an `eksManagedNodegroup` pool.
    eks_client: aws_sdk_eks::Client,
    /// The `EksManagedNodegroup` backend's SECOND AWS boundary (added
    /// 2026-07-23, the Camelot runner-instability incident) —
    /// `SetInstanceProtection`/`DescribeAutoScalingInstances` against the
    /// nodegroup's underlying ASG, orthogonal to `eks_client` above (see
    /// eks_nodegroup_provedor.rs's module doc's "Instance scale-in
    /// protection" section for why this doesn't race `eks_client`'s own
    /// `UpdateNodegroupConfig` calls). Same construction/cloning discipline
    /// as `eks_client`.
    autoscaling_client: aws_sdk_autoscaling::Client,
}

impl Ctx {
    /// Fetch (or lazily create) the forecaster for a pool. `horizon_ticks` is the
    /// lookahead in reconcile intervals (`reliefLatency / requeue`); a pool's
    /// forecaster is created once with the horizon current at first sight — a
    /// horizon change takes effect on the next controller restart (documented).
    fn forecaster_for(&self, pool: &str, horizon_ticks: u64) -> Arc<breathe_auction::LinearTrendPrevisor> {
        let mut map = self.forecasters.lock().expect("forecasters poisoned");
        map.entry(pool.to_string())
            .or_insert_with(|| Arc::new(breathe_auction::LinearTrendPrevisor::new(6, horizon_ticks)))
            .clone()
    }
}

/// Accumulate this tick's decision into the band's cumulative counters via the
/// `DecisionLog` (the single fold) and return the new count for `status_for`.
/// Shared by every controller reconcile (generic mem/cpu/storage + app/kube
/// param) so the counter math lives in exactly one place. On the (in-memory:
/// impossible) store error, falls back to the status-backed fold so the count is
/// never worse than the pre-store behavior.
pub(crate) async fn fold_counters(
    ctx: &Ctx,
    band_ref: &BandRef,
    prior: Option<&breathe_crd::BandStatus>,
    outcome: &breathe_core::TickOutcome,
) -> CumulativeCounters {
    let prior_counters = counters_from_status(prior);
    let entry = entry_for(outcome);
    match ctx.decisions.append(band_ref, prior_counters, entry).await {
        Ok(counters) => counters,
        // On a durable-store error (e.g. a transient Postgres outage) HOLD the
        // count un-advanced rather than advancing it: the carve already happened
        // (the band law ran — safety preserved), but the decision did NOT durably
        // record, so the status projection must not claim a count the durable
        // authority lacks (that was the M2 count-divergence the adversarial pass
        // found). status stays == the un-advanced durable count; the store resumes
        // on recovery. The InMem tier never errors, so this path is Postgres-only.
        Err(e) => {
            warn!(band = %band_ref, error = %e, "decision-log append failed — holding counters (durable store will resume)");
            prior_counters
        }
    }
}

/// Publish a k8s Event for this tick onto `obj`, transition-gated so a resting
/// band emits ~0 events. Non-fatal: a failed publish is logged, never propagated
/// (an event is observability, not the reconcile's job).
async fn emit_event<B: Band>(ctx: &Ctx, obj: &B, receipt: &breathe_core::TickReceipt, new_phase: Option<&str>, prior_phase: Option<&str>) {
    let Some((kind, reason, note)) = event_for(receipt) else { return };
    if !should_emit_event(receipt, new_phase, prior_phase) {
        return;
    }
    let type_ = match kind {
        EventKind::Normal => EventType::Normal,
        EventKind::Warning => EventType::Warning,
    };
    let recorder = Recorder::new(ctx.client.clone(), ctx.reporter.clone(), obj.object_ref(&()));
    let ev = Event { type_, reason: reason.to_string(), note: Some(note), action: "Reconcile".to_string(), secondary: None };
    if let Err(e) = recorder.publish(ev).await {
        warn!(error = %e, "event publish failed (non-fatal)");
    }
}

/// Publish a k8s Event on a health-label transition (Healthy -> Stuck/Unsupported
/// or vice versa), transition-gated the same way `emit_event` gates on phase — a
/// band parked in `Stuck` emits exactly once, not every tick. This is the piece
/// that makes "ensure all bands are always healthy" a reactive property rather
/// than something an operator/agent has to notice by polling: the existing
/// NATS/escuta nervous-system path (nats_trigger.rs) already reacts to k8s
/// Events, so a band going Stuck now nudges its OWN next reconcile sooner with no
/// new wiring on that side.
async fn emit_health_event<B: Band>(
    ctx: &Ctx,
    obj: &B,
    conditions: &[breathe_crd::Condition],
    prior_health: Option<&str>,
    effective_dry_run: bool,
) {
    let verdict = health_verdict(conditions, &now_rfc3339(), STUCK_AFTER_SECS, effective_dry_run);
    if !should_emit_health_event(&verdict, prior_health) {
        return;
    }
    let Some((kind, reason, note)) = health_event_for(&verdict) else { return };
    let type_ = match kind {
        EventKind::Normal => EventType::Normal,
        EventKind::Warning => EventType::Warning,
    };
    let recorder = Recorder::new(ctx.client.clone(), ctx.reporter.clone(), obj.object_ref(&()));
    let ev = Event { type_, reason: reason.to_string(), note: Some(note), action: "Reconcile".to_string(), secondary: None };
    if let Err(e) = recorder.publish(ev).await {
        warn!(error = %e, "health event publish failed (non-fatal)");
    }
}

/// True when the apiserver is k8s ≥1.33 (the `pods/resize` subresource is GA).
async fn detect_resize_capable(client: &Client) -> bool {
    match client.apiserver_version().await {
        Ok(info) => {
            let digits = |s: &str| s.trim_matches(|c: char| !c.is_ascii_digit()).parse::<u32>().unwrap_or(0);
            let (major, minor) = (digits(&info.major), digits(&info.minor));
            major > 1 || (major == 1 && minor >= 33)
        }
        Err(e) => {
            error!(error = %e, "could not read apiserver version; assuming no in-place resize (will roll)");
            false
        }
    }
}

// ── Environment discovery: the inputs to the env-discovered band-default tier
// (breathe_config::envprofile). Read-only + best-effort — every probe miss
// lands on the fail-safe value, so an unprobeable cluster is treated as FOREIGN
// (least-privilege by default). The detected profile + its resolved best-fit
// band defaults are logged + exported once at startup so operators see the
// posture without it silently mutating anything.
use breathe_config::envprofile::{
    self, CapacityType, Cloud, EnvironmentProfile, Orchestrator, Tenancy,
};
use k8s_openapi::api::authorization::v1::{
    ResourceAttributes, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
};
use k8s_openapi::api::core::v1::Node;
use kube::api::{ListParams, PostParams};

/// Classify a node's cloud + market posture from its `providerID` prefix and
/// capacity-type label. **Pure** — the unit-testable core of
/// [`detect_environment`].
fn classify_node(provider_id: &str, capacity_label: Option<&str>) -> (Cloud, CapacityType) {
    let cloud = if provider_id.starts_with("aws://") {
        Cloud::Aws
    } else if provider_id.starts_with("gce://") || provider_id.starts_with("gcp://") {
        Cloud::Gcp
    } else if provider_id.starts_with("azure://") {
        Cloud::Azure
    } else {
        Cloud::None
    };
    let capacity = match capacity_label.map(str::to_ascii_lowercase).as_deref() {
        Some("spot") => CapacityType::Spot,
        Some("on-demand" | "on_demand" | "ondemand") => CapacityType::OnDemand,
        _ => CapacityType::Unknown,
    };
    (cloud, capacity)
}

/// Can this controller create nodes cluster-wide? A `SelfSubjectAccessReview`;
/// `Some(false)` (denied) is the strongest "we don't own this cluster" signal.
/// `None` on probe failure → the caller treats tenancy as Unknown (fail-safe).
async fn can_create_nodes(client: &Client) -> Option<bool> {
    let ssar = SelfSubjectAccessReview {
        spec: SelfSubjectAccessReviewSpec {
            resource_attributes: Some(ResourceAttributes {
                verb: Some("create".into()),
                resource: Some("nodes".into()),
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    };
    match Api::<SelfSubjectAccessReview>::all(client.clone())
        .create(&PostParams::default(), &ssar)
        .await
    {
        Ok(r) => r.status.map(|s| s.allowed),
        Err(e) => {
            warn!(error = %e, "SelfSubjectAccessReview(create nodes) failed; tenancy unknown");
            None
        }
    }
}

/// Detect the [`EnvironmentProfile`] breathe is reconciling in. Read-only +
/// best-effort; every miss fails safe (foreign / unknown).
async fn detect_environment(client: &Client, resize_capable: bool) -> EnvironmentProfile {
    let (cloud, capacity) = match Api::<Node>::all(client.clone())
        .list(&ListParams::default().limit(1))
        .await
    {
        Ok(list) => list.items.first().map_or((Cloud::None, CapacityType::Unknown), |node| {
            let provider = node.spec.as_ref().and_then(|s| s.provider_id.as_deref()).unwrap_or_default();
            let cap = node.metadata.labels.as_ref().and_then(|l| {
                l.get("karpenter.sh/capacity-type")
                    .or_else(|| l.get("eks.amazonaws.com/capacityType"))
            });
            classify_node(provider, cap.map(String::as_str))
        }),
        Err(e) => {
            warn!(error = %e, "node list failed during environment detection; assuming conservative");
            (Cloud::None, CapacityType::Unknown)
        }
    };
    let tenancy = match can_create_nodes(client).await {
        Some(true) => Tenancy::Own,
        Some(false) => Tenancy::Foreign,
        None => Tenancy::Unknown,
    };
    EnvironmentProfile { orchestrator: Orchestrator::Kubernetes, cloud, tenancy, capacity, resize_capable }
}

/// The namespace label carrying a band's `EphemeralEnvId` (the ephemeral-env binding).
const ENV_ID_LABEL: &str = "breathe.pleme.io/env-id";

/// Read the ephemeral-env cost-guard context for a band's namespace (Dev Loop M3):
/// the `EphemeralEnvId` from the namespace label + the cost-remaining from the
/// namespace's `Densa` envelope status. **Read-only + best-effort** — any miss (no
/// label, no Densa, API error) yields an empty context (the band keeps `None`, the
/// rio default). breathe NEVER writes the Densa or the namespace from here.
async fn read_env_context(client: &Client, namespace: &str) -> EnvContext {
    if namespace.is_empty() {
        return EnvContext::default();
    }
    let env_id = Api::<Namespace>::all(client.clone())
        .get_opt(namespace)
        .await
        .ok()
        .flatten()
        .and_then(|ns| ns.metadata.labels)
        .and_then(|l| l.get(ENV_ID_LABEL).cloned());
    let cost_remaining_cents = Api::<Densa>::namespaced(client.clone(), namespace)
        .list(&Default::default())
        .await
        .ok()
        .and_then(|l| l.into_iter().next())
        .and_then(|d| d.status)
        .and_then(|s| s.cost_remaining_cents);
    EnvContext { env_id, cost_remaining_cents }
}

/// The one reconcile body for every dimension. `B` is the band kind, `D` its descriptor.
async fn reconcile<B: Band, D: DimensionDescriptor + Default>(
    obj: Arc<B>,
    ctx: Arc<Ctx>,
) -> Result<Action, Error> {
    let ns = obj.namespace().unwrap_or_default();
    let name = obj.name_any();

    // SUSPEND (M5): a frozen band skips observe/plan/act entirely — the limit is left
    // as-is. A spec edit (suspend:false) fires the watcher to resume.
    if obj.suspended() {
        patch_status::<B>(&ctx.client, &ns, &name, &suspended_status(obj.status())).await?;
        return Ok(Action::requeue(ctx.requeue));
    }

    let tr = obj.target_ref();
    let target = Target {
        namespace: ns.clone(),
        name: tr.name.clone(),
        kind: tr.kind.clone(),
        api_version: tr.api_version.clone().unwrap_or_default(),
        container: tr.container.clone(),
        // label-selected pod group (ARC ephemeral runners) when set; else the
        // owner-resolved path. carve + metric both read this selector.
        pod_selector: tr.pod_selector.clone(),
    };

    let cfg = match obj.band_config() {
        Ok(c) => c,
        Err(e) => {
            patch_status::<B>(&ctx.client, &ns, &name, &error_status(e.to_string())).await?;
            return Ok(Action::requeue(ctx.requeue));
        }
    };

    let in_cooldown = obj
        .last_change_epoch()
        .is_some_and(|last| now_secs().saturating_sub(last) < obj.cooldown_seconds() as i64);

    // Bind the descriptor to THIS CR's own identity (namespace/name of the Band
    // CR itself, NOT `target` — the k8s object it carves) so its SSA field
    // manager scopes per-CR (task #200): two CRs of the same dimension
    // uncoordinatedly targeting the SAME object (confirmed live for CpuBand --
    // `pangea-operator` + `pangea-operator-cpu` both targeting one Deployment via
    // the identical static "breathe/cpu" manager) become two DIFFERENT k8s field
    // managers, so SSA's own conflict detection can actually see them collide.
    let mut descriptor = D::with_resize_capability(ctx.resize_capable);
    descriptor.set_cr_identity(ns.clone(), name.clone());
    let provider = BandProvider::new(
        KubeCluster::new(ctx.client.clone(), ctx.prometheus_url.clone()),
        descriptor,
    );
    // BREAK-GLASS forceLimit: active iff set AND (no expiry OR expiry in the future).
    let force = obj.force_limit_value().filter(|_| obj.force_limit_expiry().map_or(true, rfc3339_in_future));
    // M0 PREDICTIVE: when the band opts in, feed the prior observed `used` + the
    // reconcile cadence so `reconcile_one` can measure the working-set velocity and
    // pre-grow via PredictiveGrow. Skipped (None) on the first tick (no prior used)
    // and for non-predictive bands — both keep the plain reactive path.
    let predictive = obj.predictive().and_then(|lookahead_secs| {
        let prior_used = u64::try_from(obj.status()?.observed_used?).ok()?;
        let dt_secs = ctx.requeue.as_secs_f64();
        (dt_secs > 0.0).then_some(PredictiveInput { prior_used, dt_secs, lookahead_secs })
    });
    // NEVER-OOM-FROM-CARVE: carry the trailing-window PEAK working set across ticks.
    // Pass the DECAYED prior peak as the hint; `reconcile_one` folds in the current
    // `used` (`max(used, hint)`), so the pair is exactly
    // `update_peak(prior_peak, used, decay)` — the shrink-safety floor is keyed on
    // the demonstrated peak, never the instantaneous low-water sample (the
    // authentik-Celery-worker OOM). `None` on the first tick (no prior peak).
    let peak_used = obj
        .status()
        .and_then(|s| s.observed_peak_used.or(s.observed_used))
        .and_then(|p| u64::try_from(p).ok())
        .map(|prior_peak| {
            let decay = obj.peak_decay().clamp(0.0, 0.999);
            ((prior_peak as f64) * decay) as u64
        });
    // WARMUP: how long this target has been observed since its last (re)start, and
    // the (possibly restart-reset) warmup-start epoch to carry forward. A shrink is
    // HELD while observed_for < warmup_seconds (the un-observed-boot-spike OOM fix).
    let prior_capacity = obj.status().and_then(|s| s.observed_capacity).and_then(|c| u64::try_from(c).ok());
    let (observed_for_secs, warmup_start_epoch) =
        breathe_runtime::warmup_state(obj.status(), prior_capacity, obj.warmup_seconds(), now_secs());
    let input = ReconcileInput {
        target: &target,
        cfg: &cfg,
        max_staleness_secs: obj.max_staleness_seconds(),
        in_cooldown,
        // The PROMOTION LIFECYCLE gates the carve — not the raw `dryRun` field.
        // ShadowConfirmEffect (the default) stays shadow until its confirm window
        // of clean observation passes, then auto-begins. (See Band::effective_dry_run.)
        dry_run: obj.effective_dry_run(now_secs()),
        // per-band golden/ceiling gate (default RestartFreeOnly) — the band
        // declares its own policy; the fleet env is only a fallback default.
        policy: obj.disruption_policy(),
        force,
        predictive,
        peak_used,
        observed_for_secs: Some(observed_for_secs),
        // PART 1 (SOFT k8s carve): for the MEMORY dimension ONLY, pin the HARD plane
        // (k8s limits.memory / memory.max) GROW-ONLY so an efficiency shrink can never
        // lower the kill ceiling — the reclaim is routed to the SOFT memory.high plane
        // by `reconcile_memory`'s per-pod PodMemoryHigh dispatch. cpu/storage have no
        // breathe-carved soft cgroup plane, so they keep their own directionality.
        hard_plane_grow_only: provider.id() == breathe_provider::DimensionId::Memory,
    };

    let outcome = reconcile_one(&input, &provider).await;
    let prior_phase = obj.status().and_then(|s| s.phase.as_deref()).map(String::from);
    let prior_health = obj.status().and_then(|s| s.health.as_deref()).map(String::from);
    let kind = <B as kube::Resource>::kind(&());
    let band_ref = BandRef::new(&kind, &ns, &name);
    let counters = fold_counters(&ctx, &band_ref, obj.status(), &outcome).await;
    // Record this tick's observed sample for the next predictive prior (the
    // SampleCache write seam; the authoritative read flips to cache-first at M2,
    // when Postgres — not the CRD status — is the durable home).
    if let Some(obs) = outcome.observed.as_ref() {
        let _ = ctx
            .samples
            .record(&band_ref, Sample { used: obs.used, at_epoch: now_secs() })
            .await;
    }
    let mut status = status_for(&outcome, obj.status(), obj.cooldown_seconds(), obj.generation(), counters);
    // carry the warmup-start epoch forward (reset on a detected restart) so the next
    // tick measures observed-since-restart correctly — the warmup gate's persistence.
    status.warmup_start_epoch = Some(warmup_start_epoch);
    // M3 (Dev Loop): surface the namespace's ephemeral-env cost-guard (EnvId +
    // Densa cost-remaining) on the band status. Read-only; empty (no label / no
    // Densa) ⇒ no change (the rio default).
    let env_ctx = read_env_context(&ctx.client, &ns).await;
    apply_env_context(&mut status, &env_ctx);
    info!(dim = %provider.id(), band = %name, target = %target.name, phase = ?status.phase, health = ?status.health, "reconciled");
    emit_event(&ctx, obj.as_ref(), &outcome.receipt, status.phase.as_deref(), prior_phase.as_deref()).await;
    emit_health_event(&ctx, obj.as_ref(), &status.conditions, prior_health.as_deref(), outcome.dry_run).await;
    metrics_for(
        &BandLabels { dim: provider.id().to_string(), namespace: ns.clone(), name: name.clone() },
        &outcome,
        &cfg,
        status.cooldown_remaining_seconds.unwrap_or(0),
    );
    // DIFF-GATE (task #220): skip the write entirely when this tick's status is
    // byte-identical to the CR's current live status — a resting band (the
    // common case at the default 60s requeue, and especially so under
    // NATS-reactive triggering) otherwise wrote to etcd via the apiserver on
    // EVERY tick for no observable change. `obj.status()` is the prior status
    // this same reconcile already read at the top (used for `prior_phase`/
    // `prior_health`/`fold_counters` above) — reused here, not re-fetched.
    patch_status_if_changed::<B>(&ctx.client, &ns, &name, obj.status(), &status).await?;
    // requeue keyed on the action class just taken — golden carves re-tick fast.
    Ok(Action::requeue(next_requeue(&outcome.receipt, &ctx.cooldowns)))
}

fn error_policy<B: Band>(_obj: Arc<B>, err: &Error, ctx: Arc<Ctx>) -> Action {
    error!(error = %err, "reconcile error — backing off");
    Action::requeue(ctx.requeue)
}

/// **Part 1 (SOFT k8s carve):** the MemoryBand reconcile — runs the generic
/// `reconcile<MemoryBand, MemoryDescriptor>` (which governs the HARD `memory.max` /
/// k8s `limits.memory` ceiling: it only ever GROWS or HOLDS it, never lowers it for
/// efficiency), then ROUTES the efficiency carve to the SOFT plane by emitting one
/// `PodMemoryHigh` dispatch per managed pod (`docs/OOM-VERIFICATION.md` § Part 1).
/// The dispatch's host-agent write of `memory.high` (reclaim) is what reclaims slack
/// without ever lowering the kill ceiling — the k8s-plane never-OOM guarantee.
///
/// The soft routing reads the band's freshly-written status (`observed_used`/
/// `observed_peak_used`/`observed_capacity`) — the same observation the generic tick
/// produced — and computes the soft target via `pod_memory_high::soft_target_for`
/// (which reuses `breathe_control::plan_k8s_memory_carve`, so the HARD plane is never
/// touched). Shadow-first: the dispatch is `dryRun` whenever the band is in shadow.
///
/// `tier-honest`: the routing DECISION + the dispatch PAYLOAD are pure + library-
/// tested; the live convergence (apiserver-listing pods, applying the dispatch CR,
/// the host-agent cgroup write) is `pending-deploy`. A dispatch failure is logged,
/// never propagated — the HARD-plane never-OOM guarantee holds regardless.
async fn reconcile_memory(obj: Arc<MemoryBand>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    // HARD plane + status: the unchanged generic tick (governs limits.memory).
    let action = reconcile::<MemoryBand, MemoryDescriptor>(obj.clone(), ctx.clone()).await?;

    // SOFT plane: route the efficiency carve to memory.high via a per-pod dispatch.
    // Only for a NON-suspended band with a fresh observation; a suspended/observation-
    // less band has nothing to route (the HARD plane already held the kill ceiling).
    if obj.suspended() {
        return Ok(action);
    }
    let band = match Api::<MemoryBand>::all(ctx.client.clone()).get_opt(&obj.name_any()).await {
        Ok(Some(b)) => b,
        _ => return Ok(action), // can't re-read ⇒ skip the soft dispatch this tick
    };
    let (Ok(cfg), Some(st)) = (band.band_config(), band.status()) else {
        return Ok(action);
    };
    let (Some(used), Some(cap)) = (st.observed_used, st.observed_capacity) else {
        return Ok(action); // no fresh observation ⇒ nothing to route
    };
    let used = used.max(0) as u64;
    let hard_current = cap.max(0) as u64;
    let peak = st.observed_peak_used.unwrap_or(used as i64).max(0) as u64;
    // The live pod memory.high is unknown to the controller (it has no node access);
    // pass `u64::MAX` (unset) so the planner snaps it down to the routed soft target —
    // the host-agent's read of the live cgroup file is the authoritative current value.
    let Some(soft_bytes) = pod_memory_high::soft_target_for(used, peak, hard_current, u64::MAX, &cfg) else {
        return Ok(action); // in-band hold / refused shrink ⇒ no soft dispatch
    };

    let tr = band.target_ref();
    let target = Target {
        namespace: band.namespace().unwrap_or_default(),
        name: tr.name.clone(),
        kind: tr.kind.clone(),
        api_version: tr.api_version.clone().unwrap_or_default(),
        container: tr.container.clone(),
        pod_selector: tr.pod_selector.clone(),
    };
    let cluster = KubeCluster::new(ctx.client.clone(), ctx.prometheus_url.clone());
    let dry_run = band.effective_dry_run(now_secs());
    // rio runs the systemd cgroup driver (the default); a CgroupDriver config knob is
    // the named follow-on for cgroupfs clusters.
    match pod_memory_high::ensure_soft_carve_dispatch(
        &ctx.client,
        &cluster,
        &band,
        &target,
        breathe_crd::CgroupDriverSpec::Systemd,
        soft_bytes,
        dry_run,
    )
    .await
    {
        Ok(n) if n > 0 => info!(band = %band.name_any(), dispatches = n, soft_bytes, dry_run, "routed soft memory.high carve to the host-agent"),
        Ok(_) => {}
        Err(e) => warn!(band = %band.name_any(), error = %e, "soft memory.high dispatch failed (non-fatal — HARD plane still held the kill ceiling)"),
    }
    Ok(action)
}

/// Summarize every band of kind `B` (across all namespaces) into the fleet overview.
async fn summarize<B: Band>(client: &Client, kind: &str, out: &mut Vec<BandSummary>) {
    let api: Api<B> = Api::all(client.clone());
    match api.list(&Default::default()).await {
        Ok(list) => {
            for b in list {
                let st = b.status();
                out.push(BandSummary {
                    kind: kind.to_string(),
                    namespace: b.namespace().unwrap_or_default(),
                    name: b.name_any(),
                    target: b.target_ref().name.clone(),
                    util: st.and_then(|s| s.last_util.clone()),
                    phase: st.and_then(|s| s.phase.clone()),
                    current_limit: st.and_then(|s| s.current_limit.clone()),
                    policy: st.and_then(|s| s.effective_policy.clone()),
                    // the EFFECTIVE (lifecycle-gated) dry-run — what's actually happening
                    dry_run: b.effective_dry_run(now_secs()),
                });
            }
        }
        Err(e) => warn!(kind, error = %e, "overview: failed to list a band kind"),
    }
}

/// Keep the fleet-OVERVIEW object current: list EVERY band across every namespace,
/// roll up the totals, patch the status. One `kubectl get bov` = the whole fleet's
/// homeostasis at a glance — the dashboard as a single k8s object (no Grafana).
async fn reconcile_overview(obj: Arc<BreatheOverview>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let mut bands = Vec::new();
    summarize::<MemoryBand>(&ctx.client, "MemoryBand", &mut bands).await;
    summarize::<CpuBand>(&ctx.client, "CpuBand", &mut bands).await;
    summarize::<StorageBand>(&ctx.client, "StorageBand", &mut bands).await;
    summarize::<ReplicaBand>(&ctx.client, "ReplicaBand", &mut bands).await;
    summarize::<ArcBand>(&ctx.client, "ArcBand", &mut bands).await;
    summarize::<CgroupBand>(&ctx.client, "CgroupBand", &mut bands).await;
    summarize::<CgroupCpuBand>(&ctx.client, "CgroupCpuBand", &mut bands).await;
    bands.sort_by(|a, b| (&a.kind, &a.namespace, &a.name).cmp(&(&b.kind, &b.namespace, &b.name)));

    let count = |ps: &[&str]| bands.iter().filter(|b| b.phase.as_deref().is_some_and(|x| ps.contains(&x))).count() as i64;
    let total = bands.len() as i64;
    let converged = count(&["Holding", "AtFloor", "AtCeiling", "Dormant"]);
    let carving = count(&["Growing", "Shrinking"]);
    let deferred = count(&["DeferredWouldRestart"]);
    let suspended = count(&["Suspended"]);
    let shadow = bands.iter().filter(|b| b.dry_run).count() as i64;
    let refresh = Duration::from_secs(obj.spec.refresh_seconds.max(5));

    // DIFF-GATE: only patch when the SUBSTANTIVE fleet state changed. `last_updated`
    // then marks the last CHANGE (not the last tick), and a stable fleet produces
    // ZERO writes — so the heartbeat timestamp can never re-fire the watch (belt to
    // the generation-predicate's suspenders).
    let unchanged = obj.status.as_ref().is_some_and(|s| {
        s.total == total
            && s.converged == converged
            && s.carving == carving
            && s.deferred == deferred
            && s.suspended == suspended
            && s.shadow == shadow
            && s.bands == bands
    });
    if unchanged {
        return Ok(Action::requeue(refresh));
    }

    let status = OverviewStatus {
        total,
        converged,
        carving,
        deferred,
        suspended,
        shadow,
        last_updated: Some(now_rfc3339()),
        bands,
    };
    let api: Api<BreatheOverview> = Api::all(ctx.client.clone());
    api.patch_status(&obj.name_any(), &PatchParams::default(), &Patch::Merge(&json!({ "status": status })))
        .await?;
    info!(overview = %obj.name_any(), total, "fleet overview changed");
    Ok(Action::requeue(refresh))
}

fn overview_error_policy(_obj: Arc<BreatheOverview>, err: &Error, _ctx: Arc<Ctx>) -> Action {
    error!(error = %err, "overview reconcile error — backing off");
    Action::requeue(Duration::from_secs(30))
}

/// Load the cluster `BreatheConfig` (the first one found) — its set fields override
/// the env defaults. Empty/absent ⇒ all defaults. (Read once at startup; a config
/// change applies on the next controller restart — dynamic reload is a refinement.)
async fn load_breathe_config(client: &Client) -> BreatheConfigSpec {
    Api::<BreatheConfig>::all(client.clone())
        .list(&Default::default())
        .await
        .ok()
        .and_then(|l| l.into_iter().next())
        .map_or_else(BreatheConfigSpec::default, |c| c.spec)
}

/// A fatal startup misconfiguration: a scale tier selected in config that this
/// build does not yet implement. Fail-fast — no silent downgrade (★★ MAGMA-NATIVE
/// "config decides") — naming the milestone that ships the tier.
#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error("unimplemented scale tier: {0}")]
    UnsupportedScale(String),
}

/// Build the durable-store seam from the typed scale config — the config-driven
/// backend selection. The very-small arms (in-memory store, no cache, single
/// replica) are byte-identical to today; `store=Postgres` (M2) connects the
/// durable `PgDecisionLog`. Every still-unimplemented arm is a typed fail-fast
/// naming the milestone that ships it — never a silent downgrade to in-memory.
async fn build_stores(
    scale: &ScaleConfig,
) -> Result<(Arc<dyn DecisionLog>, Arc<dyn SampleCache>), Box<dyn std::error::Error>> {
    let decisions: Arc<dyn DecisionLog> = match &scale.store {
        StoreConfig::InMemory => Arc::new(InMemDecisionLog::new()),
        StoreConfig::Postgres(pg) => {
            // The DSN is read here (the one place the secret is exposed) to
            // connect; PgDecisionLog applies its migrations on connect.
            Arc::new(
                breathe_store::PgDecisionLog::connect(pg.dsn.expose(), pg.pool_max, pg.pool_min)
                    .await?,
            )
        }
    };
    let samples: Arc<dyn SampleCache> = match &scale.cache {
        CacheConfig::None => Arc::new(InMemSampleCache::new()),
        CacheConfig::Redis(_) => {
            return Err(StartupError::UnsupportedScale(
                "scale.cache=redis — the cache tier ships at M3; set `cache: none`".into(),
            )
            .into())
        }
    };
    match &scale.coordination {
        CoordinationConfig::SingleReplica => {}
        CoordinationConfig::LeaderElection(_) => {
            return Err(StartupError::UnsupportedScale(
                "scale.coordination=leaderElection — ships at M3; set `coordination: singleReplica`".into(),
            )
            .into())
        }
        CoordinationConfig::Sharded(_) => {
            return Err(StartupError::UnsupportedScale(
                "scale.coordination=sharded — ships at M4; set `coordination: singleReplica`".into(),
            )
            .into())
        }
    }
    Ok((decisions, samples))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Explicit CryptoProvider install -- must run before any TLS use
    // (the very first is `Client::try_default()` below). The workspace
    // resolves both rustls 0.21 (ring-only) and 0.23 (aws-lc-rs + ring)
    // across different transitive consumers, so rustls 0.23's own
    // single-feature auto-detection is ambiguous and panics without
    // this. Confirmed live 2026-07-18: this exact panic crash-looped the
    // deployed controller and caused a Flux rollback.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install_default() should only be called once, at startup");

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,breathe_controller=info".into()),
        )
        .init();

    let client = Client::try_default().await?;
    let resize_capable = detect_resize_capable(&client).await;

    // The EksManagedNodegroup backend's AWS boundary (task #205) — built
    // once, region+credentials resolved via the standard SDK chain. Never
    // fails here: `load()` only assembles a lazy config, it performs no
    // network I/O and resolves no credentials until the first real EKS call
    // — so this line is safe to run unconditionally, even on clusters (or
    // this dev machine) with no AWS credentials present at all.
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest()).load().await;
    let eks_client = aws_sdk_eks::Client::new(&aws_config);
    let autoscaling_client = aws_sdk_autoscaling::Client::new(&aws_config);

    // ── Environment-discovered band-default posture (best fit for THIS cluster).
    // Detected once, read-only; the resolved defaults are the recommendation a
    // foreign/multi-tenant cluster gets least-disruptive shadow-first values,
    // our own cluster keeps the broad statics. Surfaced (log + metric); auto-
    // apply onto unset band fields is the staged next step (needs the band-spec
    // Option<T> migration so it never clobbers an explicit operator value).
    let environment = detect_environment(&client, resize_capable).await;
    let env_defaults = envprofile::resolve(&environment);
    info!(
        tenancy = %environment.tenancy,
        cloud = %environment.cloud,
        capacity = %environment.capacity,
        foreign = environment.is_foreign(),
        rec_mode = ?env_defaults.mode,
        rec_setpoint = ?env_defaults.setpoint,
        rec_node_provisioning = ?env_defaults.allow_node_provisioning,
        "environment-discovered band-default posture resolved"
    );
    metrics::gauge!("breathe_environment_foreign", "tenancy" => environment.tenancy.as_str())
        .set(if environment.is_foreign() { 1.0 } else { 0.0 });

    // BreatheConfig (a cluster k8s object) overrides the env defaults for the last
    // env-only knobs — prometheusUrl / requeue / class cooldowns.
    let bcfg = load_breathe_config(&client).await;
    let prometheus_url = bcfg
        .prometheus_url
        .unwrap_or_else(|| std::env::var("BREATHE_PROMETHEUS_URL").unwrap_or_default());
    let requeue = Duration::from_secs(bcfg.base_requeue_seconds.unwrap_or_else(|| {
        std::env::var("BREATHE_REQUEUE_SECONDS").ok().and_then(|s| s.parse().ok()).unwrap_or(60)
    }));
    // `.max(1)`: a user-supplied cooldown of 0 would make a golden carve return
    // `Action::requeue(Duration::ZERO)` → a CPU-spinning reconcile with no backoff.
    // Clamp every class to ≥1s so a misconfigured BreatheConfig can't busy-loop.
    let cooldowns = bcfg.class_cooldowns.map_or_else(ClassCooldowns::default, |c| ClassCooldowns {
        restart_free: c.restart_free.max(1),
        restart_conditional: c.restart_conditional.max(1),
        restart_requiring: c.restart_requiring.max(1),
    });

    // ── M1: the shikumi service config selects the elasticity tier. ─────────
    // prescribed_default = VERY-SMALL (in-memory / no cache / single replica) =
    // byte-identical to today, so a controller with no config file runs exactly
    // as before. A present-but-malformed file fails loud (no silent default).
    // Read ONCE at startup via the one-shot `load` (no watcher): scale is
    // restart-required, so there is nothing to hot-reload, AND an absent
    // ConfigMap can never crash startup (the notify-watch-on-missing-path
    // os-error-2 class stays gone). A committed scale change takes effect on the
    // next pod restart, where `build_stores` re-validates it.
    let config_path = breathe_config::default_config_path();
    let scale = breathe_config::load(&config_path)?.get().scale.clone();
    info!(
        store = ?scale.store, cache = ?scale.cache, coordination = ?scale.coordination,
        window = scale.window, "breathe scale tier resolved"
    );
    let (decisions, samples) = build_stores(&scale).await?;

    // Prometheus /metrics on :9100 — scraped by VictoriaMetrics for the over-time
    // "watch it breathe" view. Non-fatal: a failed install logs + continues.
    if let Err(e) = metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], 9100))
        .install()
    {
        error!(error = %e, "failed to install /metrics exporter — continuing without metrics");
    }
    metrics::gauge!("breathe_build_info", "binary" => "breathe-controller", "version" => env!("CARGO_PKG_VERSION")).set(1.0);

    let reporter = Reporter { controller: "breathe-controller".into(), instance: std::env::var("POD_NAME").ok() };
    let ctx = Arc::new(Ctx {
        client: client.clone(),
        prometheus_url,
        requeue,
        resize_capable,
        cooldowns,
        reporter,
        forecasters: std::sync::Mutex::new(std::collections::HashMap::new()),
        // The durable-store seam, selected by `scale.{store,cache}` (M1). At the
        // very-small default these are the in-memory impls (byte-identical to
        // today); a Postgres/Redis selection is built here at M2/M3 behind the
        // same `Arc<dyn …>` fields, with no reconcile-loop change.
        decisions,
        samples,
        eks_client,
        autoscaling_client,
    });

    info!(
        resize_capable,
        carve = if resize_capable { "in-place (pods/resize, zero-restart)" } else { "rolling (template)" },
        "breathe-controller starting — golden-edge gate active, per-band DisruptionPolicy (default RestartFreeOnly)"
    );

    // ── NATS reactive-reconcile trigger (task #90, nervous-system integration).
    // ADDITIVE, default-OFF, zero regression on the proven-live correnteza path:
    // `kube_runtime::Controller::reconcile_all_on` is additive by construction
    // (see nats_trigger.rs's module docs, verified against the exact pinned
    // kube-runtime 0.96.0 source) — it is called ONLY when
    // `nats_trigger::resolve_trigger` (the tested two-gate decision point)
    // returns `Some`. Both gates unmet (the default: no `BREATHE_NATS_URL`, no
    // `BREATHE_NATS_RECONCILE_ENABLED`) ⇒ `nats_client` stays `None` ⇒ every
    // `.reconcile_all_on()` call below is skipped entirely ⇒ each `Controller`
    // value is the LITERAL SAME object `gen_controller!` alone already produces
    // — structurally, not just behaviorally, byte-identical to before this
    // change. escuta-breathe-bridge (Piece 2) is what would publish onto these
    // subjects; until it's deployed (see its own named blocker) this trigger has
    // nothing to subscribe to even when enabled — the primary kube watch is the
    // standing safety net regardless, this only ever makes a reconcile happen
    // SOONER, never instead-of.
    let nats_url = std::env::var("BREATHE_NATS_URL").ok();
    let nats_enabled = std::env::var("BREATHE_NATS_RECONCILE_ENABLED").as_deref() == Ok("true");
    let nats_client: Option<nats_trigger::LiveNats> = if let (Some(url), true) = (&nats_url, nats_enabled) {
        match async_nats::connect(url).await {
            Ok(c) => Some(nats_trigger::LiveNats(c)),
            Err(e) => {
                warn!(error = %e, url, "NATS connect failed at startup — every band controller stays watch-only");
                None
            }
        }
    } else {
        None
    };

    let mem_ctrl = gen_controller!(Api::<MemoryBand>::all(client.clone()));
    let mem_ctrl = match &nats_client {
        Some(nc) => match nats_trigger::resolve_trigger(nats_url.clone(), nats_enabled, nc, "escuta.*.memoryband.>").await {
            Some(trigger) => mem_ctrl.reconcile_all_on(trigger),
            None => mem_ctrl,
        },
        None => mem_ctrl,
    };
    let mem = mem_ctrl
        .run(reconcile_memory, error_policy::<MemoryBand>, ctx.clone())
        .for_each(|_| async {});

    let cpu_ctrl = gen_controller!(Api::<CpuBand>::all(client.clone()));
    let cpu_ctrl = match &nats_client {
        Some(nc) => match nats_trigger::resolve_trigger(nats_url.clone(), nats_enabled, nc, "escuta.*.cpuband.>").await {
            Some(trigger) => cpu_ctrl.reconcile_all_on(trigger),
            None => cpu_ctrl,
        },
        None => cpu_ctrl,
    };
    let cpu = cpu_ctrl
        .run(reconcile::<CpuBand, CpuDescriptor>, error_policy::<CpuBand>, ctx.clone())
        .for_each(|_| async {});

    // StorageBand is NOT wired to the NATS trigger — a named follow-up (a
    // one-line addition identical in shape to mem/cpu above), not scoped here.
    let sto = gen_controller!(Api::<StorageBand>::all(client.clone()))
        .run(reconcile::<StorageBand, StorageDescriptor>, error_policy::<StorageBand>, ctx.clone())
        .for_each(|_| async {});
    // The fleet-overview reconciler — keeps every BreatheOverview's status current.
    let overview = gen_controller!(Api::<BreatheOverview>::all(client.clone()))
        .run(reconcile_overview, overview_error_policy, ctx.clone())
        .for_each(|_| async {});

    // BU1+BU2 — the node tier: reconcile_forma runs in the live controller,
    // watching BreatheCloudPool CRs (one node-count Forma band per pool),
    // observe-only (provision = DryRun). Each pool binds a Forma to a Densa-style
    // envelope; the shape-blind band law holds the node COUNT at the 80/20 band
    // exactly as it holds a pod's bytes.
    //
    // BreatheCloudPool is CLUSTER-SCOPED (no `namespaced` in its `#[kube(...)]`
    // attrs, breathe-crd/src/lib.rs — confirmed by reading the source) — the
    // subject wildcard `escuta.*.breathecloudpool.>` already matches escuta's
    // own cluster-scoped sentinel schema (`escuta._cluster.breathecloudpool.*`,
    // `_cluster` is one NATS token, matched by the single-token `*`), no
    // special-casing needed here vs the two namespaced kinds above.
    let cloud_pools_ctrl = gen_controller!(Api::<BreatheCloudPool>::all(client.clone()));
    let cloud_pools_ctrl = match &nats_client {
        Some(nc) => match nats_trigger::resolve_trigger(nats_url.clone(), nats_enabled, nc, "escuta.*.breathecloudpool.>").await {
            Some(trigger) => cloud_pools_ctrl.reconcile_all_on(trigger),
            None => cloud_pools_ctrl,
        },
        None => cloud_pools_ctrl,
    };
    // A SECOND, additive trigger on the SAME controller (`reconcile_all_on` is
    // additive-by-construction — kube-runtime 0.96.0 `Controller::reconcile_all_on`
    // just pushes another stream into the same `SelectAll`, per that fn's own
    // source, cited above) — fires an immediate reconcile the moment
    // escuta-breathe-bridge publishes a core `Event(reason=FailedScheduling)`
    // (2026-07-22), instead of waiting for the next periodic tick. This closes
    // a real, live gap: a 42m51s arm64 runner Pending incident this session sat
    // unwatched between ticks with nothing reacting sooner. `reconcile_forma`'s
    // own `sample.used` (breathe-controller/src/eks_nodegroup_provedor.rs
    // `observe_pod_demand_milli`) already sums BOTH Running and Pending pod
    // demand — the DECISION logic was already correct; what was missing was
    // REACTION SPEED, not a new predictor. This subject re-triggers the exact
    // same, already-correct reconcile loop sooner, nothing more.
    let cloud_pools_ctrl = match &nats_client {
        Some(nc) => match nats_trigger::resolve_trigger(nats_url.clone(), nats_enabled, nc, "escuta.*.event.>").await {
            Some(trigger) => cloud_pools_ctrl.reconcile_all_on(trigger),
            None => cloud_pools_ctrl,
        },
        None => cloud_pools_ctrl,
    };
    let cloud_pools = cloud_pools_ctrl
        .run(node_forma::reconcile_cloud_pool, node_forma::error_policy_cloud_pool, ctx.clone())
        .for_each(|_| async {});
    // The membership-CLOSING peer of the correnteza claim path above: watches
    // IsolationBand CRs, keeping every declared target node tainted against
    // everything but its allowlist (Camelot's origin node is the first
    // consumer) and observing unauthorized occupants. Reconciles every tick,
    // unconditionally — a standing PROTECT posture, not a Grew-gated reaction.
    let isolation_bands = gen_controller!(Api::<IsolationBand>::all(client.clone()))
        .run(origin_guard::reconcile_isolation_band, origin_guard::error_policy_isolation_band, ctx.clone())
        .for_each(|_| async {});
    // Step-6/8/12: the generic k8s-CR / app band — reconciled via KubeCluster's
    // generic CR-path SSA. Additive; the mem/cpu/storage reconcile is untouched.
    let kube_params = gen_controller!(Api::<KubeParamBand>::all(client.clone()))
        .run(kube_param::reconcile_kube_param, kube_param::error_policy_kube_param, ctx.clone())
        .for_each(|_| async {});
    // The hierarchical-vector fair-share allocator — watches QuinhaoPool CRs,
    // divides the band among the claimant forest (groups → users) per dimension,
    // publishes the grant ledger to status. ADVISORY: status-only, carves nothing
    // (the pool's StorageBand still holds the 80%). The grant ledger gaveta reads.
    let quinhao_pools = gen_controller!(Api::<QuinhaoPool>::all(client.clone()))
        .run(quinhao::reconcile_quinhao_pool, quinhao::error_policy_quinhao_pool, ctx.clone())
        .for_each(|_| async {});

    // Step-9/13: the generic app-plane actuator band — reconciled via the
    // ActuatorCluster sum type (ConfigFile/ApiCall → ConfigReload/redis/JMX/app-RPC),
    // `used` from the metric KubeCluster. Additive; every other reconcile untouched.
    let app_bands = gen_controller!(Api::<AppBand>::all(client.clone()))
        .run(app_band::reconcile_app_band, app_band::error_policy_app_band, ctx.clone())
        .for_each(|_| async {});

    // The HORIZONTAL band — ReplicaBand holds a workload's `.spec.replicas` at a
    // work-rate band via the horizontal band law (`plan_replica_tick`), NOT the
    // vertical decide loop, but rides the SAME shadow→confirm→effect gate + the SAME
    // `.spec.replicas` SSA actuator + the SAME status machinery. Additive; every other
    // reconcile is untouched.
    let replica_bands = gen_controller!(Api::<ReplicaBand>::all(client.clone()))
        .run(replica_band::reconcile_replica_band, replica_band::error_policy_replica_band, ctx.clone())
        .for_each(|_| async {});

    tokio::join!(mem, cpu, sto, overview, cloud_pools, isolation_bands, kube_params, quinhao_pools, app_bands, replica_bands);
    Ok(())
}

#[cfg(test)]
mod env_detect_tests {
    use super::{classify_node, CapacityType, Cloud};

    #[test]
    fn classify_node_maps_provider_prefix_and_capacity_label() {
        assert_eq!(
            classify_node("aws:///us-east-2a/i-0abc", Some("spot")),
            (Cloud::Aws, CapacityType::Spot)
        );
        assert_eq!(
            classify_node("gce://proj/zone/inst", Some("on-demand")),
            (Cloud::Gcp, CapacityType::OnDemand)
        );
        assert_eq!(classify_node("azure:///subs/x", Some("SPOT")), (Cloud::Azure, CapacityType::Spot));
        // kind / bare / no label → no cloud, unknown capacity (fail-safe).
        assert_eq!(
            classify_node("kind://podman/kind/control-plane", None),
            (Cloud::None, CapacityType::Unknown)
        );
        assert_eq!(classify_node("", None), (Cloud::None, CapacityType::Unknown));
    }
}
