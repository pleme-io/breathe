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

mod node_forma;
mod kube_param;
mod quinhao;

use breathe_core::{reconcile_one, PredictiveInput, ReconcileInput};
use breathe_crd::{
    ArcBand, Band, BandSummary, BreatheCloudPool, BreatheConfig, BreatheConfigSpec, BreatheOverview, CgroupBand,
    CgroupCpuBand, CpuBand, Densa, KubeParamBand, MemoryBand, OverviewStatus, QuinhaoPool, StorageBand,
};
use breathe_dimensions::{CpuDescriptor, MemoryDescriptor, StorageDescriptor};
use breathe_kube::KubeCluster;
use breathe_provider::{BandProvider, ClassCooldowns, DimensionDescriptor, ResourceProvider, Target};
use breathe_runtime::{
    apply_env_context, error_status, event_for, metrics_for, next_requeue, now_rfc3339, now_secs, patch_status,
    rfc3339_in_future, should_emit_event, status_for, suspended_status, BandLabels, EnvContext, EventKind,
};
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

    let provider = BandProvider::new(
        KubeCluster::new(ctx.client.clone(), ctx.prometheus_url.clone()),
        D::with_resize_capability(ctx.resize_capable),
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
    };

    let outcome = reconcile_one(&input, &provider).await;
    let prior_phase = obj.status().and_then(|s| s.phase.as_deref()).map(String::from);
    let mut status = status_for(&outcome, obj.status(), obj.cooldown_seconds(), obj.generation());
    // M3 (Dev Loop): surface the namespace's ephemeral-env cost-guard (EnvId +
    // Densa cost-remaining) on the band status. Read-only; empty (no label / no
    // Densa) ⇒ no change (the rio default).
    let env_ctx = read_env_context(&ctx.client, &ns).await;
    apply_env_context(&mut status, &env_ctx);
    info!(dim = %provider.id(), band = %name, target = %target.name, phase = ?status.phase, "reconciled");
    emit_event(&ctx, obj.as_ref(), &outcome.receipt, status.phase.as_deref(), prior_phase.as_deref()).await;
    metrics_for(
        &BandLabels { dim: provider.id().to_string(), namespace: ns.clone(), name: name.clone() },
        &outcome,
        &cfg,
        status.cooldown_remaining_seconds.unwrap_or(0),
    );
    patch_status::<B>(&ctx.client, &ns, &name, &status).await?;
    // requeue keyed on the action class just taken — golden carves re-tick fast.
    Ok(Action::requeue(next_requeue(&outcome.receipt, &ctx.cooldowns)))
}

fn error_policy<B: Band>(_obj: Arc<B>, err: &Error, ctx: Arc<Ctx>) -> Action {
    error!(error = %err, "reconcile error — backing off");
    Action::requeue(ctx.requeue)
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,breathe_controller=info".into()),
        )
        .init();

    let client = Client::try_default().await?;
    let resize_capable = detect_resize_capable(&client).await;
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
    });

    info!(
        resize_capable,
        carve = if resize_capable { "in-place (pods/resize, zero-restart)" } else { "rolling (template)" },
        "breathe-controller starting — golden-edge gate active, per-band DisruptionPolicy (default RestartFreeOnly)"
    );

    let mem = gen_controller!(Api::<MemoryBand>::all(client.clone()))
        .run(reconcile::<MemoryBand, MemoryDescriptor>, error_policy::<MemoryBand>, ctx.clone())
        .for_each(|_| async {});
    let cpu = gen_controller!(Api::<CpuBand>::all(client.clone()))
        .run(reconcile::<CpuBand, CpuDescriptor>, error_policy::<CpuBand>, ctx.clone())
        .for_each(|_| async {});
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
    let cloud_pools = gen_controller!(Api::<BreatheCloudPool>::all(client.clone()))
        .run(node_forma::reconcile_cloud_pool, node_forma::error_policy_cloud_pool, ctx.clone())
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

    tokio::join!(mem, cpu, sto, overview, cloud_pools, kube_params, quinhao_pools);
    Ok(())
}
