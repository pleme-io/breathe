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

use breathe_core::{reconcile_one, ReconcileInput};
use breathe_crd::{Band, CpuBand, MemoryBand, StorageBand};
use breathe_dimensions::{CpuDescriptor, MemoryDescriptor, StorageDescriptor};
use breathe_kube::KubeCluster;
use breathe_provider::{BandProvider, ClassCooldowns, DimensionDescriptor, ResourceProvider, Target};
use breathe_runtime::{
    error_status, event_for, metrics_for, next_requeue, now_secs, patch_status, should_emit_event, status_for,
    suspended_status, BandLabels, EventKind,
};
use futures::StreamExt;
use kube::{
    api::Api,
    runtime::{
        controller::Action,
        events::{Event, EventType, Recorder, Reporter},
        watcher, Controller,
    },
    Client, ResourceExt,
};
use tracing::{error, info, warn};

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
        patch_status::<B>(&ctx.client, &ns, &name, &suspended_status()).await?;
        return Ok(Action::requeue(ctx.requeue));
    }

    let tr = obj.target_ref();
    let target = Target {
        namespace: ns.clone(),
        name: tr.name.clone(),
        kind: tr.kind.clone(),
        api_version: tr.api_version.clone().unwrap_or_default(),
        container: tr.container.clone(),
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
    let input = ReconcileInput {
        target: &target,
        cfg: &cfg,
        max_staleness_secs: obj.max_staleness_seconds(),
        in_cooldown,
        dry_run: obj.dry_run(),
        // per-band golden/ceiling gate (default RestartFreeOnly) — the band
        // declares its own policy; the fleet env is only a fallback default.
        policy: obj.disruption_policy(),
    };

    let outcome = reconcile_one(&input, &provider).await;
    let prior_phase = obj.status().and_then(|s| s.phase.as_deref()).map(String::from);
    let status = status_for(&outcome, obj.status(), obj.cooldown_seconds(), obj.generation());
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,breathe_controller=info".into()),
        )
        .init();

    let prometheus_url = std::env::var("BREATHE_PROMETHEUS_URL").unwrap_or_default();
    let requeue = Duration::from_secs(
        std::env::var("BREATHE_REQUEUE_SECONDS").ok().and_then(|s| s.parse().ok()).unwrap_or(60),
    );
    let client = Client::try_default().await?;
    let resize_capable = detect_resize_capable(&client).await;
    let cooldowns = ClassCooldowns::default();
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
    let ctx = Arc::new(Ctx { client: client.clone(), prometheus_url, requeue, resize_capable, cooldowns, reporter });

    info!(
        resize_capable,
        carve = if resize_capable { "in-place (pods/resize, zero-restart)" } else { "rolling (template)" },
        "breathe-controller starting — golden-edge gate active, per-band DisruptionPolicy (default RestartFreeOnly)"
    );

    let mem = Controller::new(Api::<MemoryBand>::all(client.clone()), watcher::Config::default())
        .run(reconcile::<MemoryBand, MemoryDescriptor>, error_policy::<MemoryBand>, ctx.clone())
        .for_each(|_| async {});
    let cpu = Controller::new(Api::<CpuBand>::all(client.clone()), watcher::Config::default())
        .run(reconcile::<CpuBand, CpuDescriptor>, error_policy::<CpuBand>, ctx.clone())
        .for_each(|_| async {});
    let sto = Controller::new(Api::<StorageBand>::all(client.clone()), watcher::Config::default())
        .run(reconcile::<StorageBand, StorageDescriptor>, error_policy::<StorageBand>, ctx.clone())
        .for_each(|_| async {});

    tokio::join!(mem, cpu, sto);
    Ok(())
}
