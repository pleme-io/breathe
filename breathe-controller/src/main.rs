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
use breathe_provider::{BandProvider, ClassCooldowns, DimensionDescriptor, DisruptionPolicy, ResourceProvider, Target};
use breathe_runtime::{error_status, next_requeue, now_secs, patch_status, status_for};
use futures::StreamExt;
use kube::{
    api::Api,
    runtime::{controller::Action, watcher, Controller},
    Client, ResourceExt,
};
use tracing::{error, info};

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
    /// The golden/ceiling gate: a carve whose restart class this policy does not
    /// permit is DEFERRED, never silently rolled. Default `RestartFreeOnly`.
    policy: DisruptionPolicy,
    /// Per-restart-class requeue cadence — golden carves re-tick near the scrape
    /// interval; refused crossings back off.
    cooldowns: ClassCooldowns,
}

/// Parse the fleet `BREATHE_DISRUPTION_POLICY` env (default golden).
fn parse_policy(s: &str) -> DisruptionPolicy {
    match s.to_ascii_lowercase().replace('_', "-").as_str() {
        "allow-restart" => DisruptionPolicy::AllowRestart,
        "allow-conditional" => DisruptionPolicy::AllowConditional,
        _ => DisruptionPolicy::RestartFreeOnly,
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
        policy: ctx.policy,
    };

    let receipt = reconcile_one(&input, &provider).await;
    let status = status_for(&receipt);
    info!(dim = %provider.id(), band = %name, target = %target.name, phase = ?status.phase, "reconciled");
    patch_status::<B>(&ctx.client, &ns, &name, &status).await?;
    // requeue keyed on the action class just taken — golden carves re-tick fast.
    Ok(Action::requeue(next_requeue(&receipt, &ctx.cooldowns)))
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
    let policy = parse_policy(&std::env::var("BREATHE_DISRUPTION_POLICY").unwrap_or_default());
    let cooldowns = ClassCooldowns::default();
    let ctx = Arc::new(Ctx { client: client.clone(), prometheus_url, requeue, resize_capable, policy, cooldowns });

    info!(
        resize_capable,
        policy = ?policy,
        carve = if resize_capable { "in-place (pods/resize, zero-restart)" } else { "rolling (template)" },
        "breathe-controller starting — memory + cpu + storage dimensions (golden-edge gate active)"
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
