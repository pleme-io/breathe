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

use std::{sync::Arc, time::Duration, time::{SystemTime, UNIX_EPOCH}};

use breathe_core::{reconcile_one, ReconcileInput, TickReceipt};
use breathe_crd::{Band, BandStatus, CpuBand, MemoryBand, StorageBand};
use breathe_dimensions::{CpuDescriptor, MemoryDescriptor, StorageDescriptor};
use breathe_kube::KubeCluster;
use breathe_provider::{BandProvider, DimensionDescriptor, ResourceProvider, Target};
use futures::StreamExt;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{controller::Action, watcher, Controller},
    Client, ResourceExt,
};
use serde_json::json;
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
}

fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

fn status_for(receipt: &TickReceipt) -> BandStatus {
    use breathe_control::Decision;
    let mut s = BandStatus::default();
    match receipt {
        TickReceipt::Conflict { manager } => {
            s.phase = Some("Conflict".into());
            s.conflict_manager = Some(manager.clone());
            s.last_decision = Some(format!("yielded to {manager}"));
        }
        TickReceipt::Stale { staleness_secs } => {
            s.phase = Some("Stale".into());
            s.last_decision = Some(format!("metric {staleness_secs}s stale — held"));
        }
        TickReceipt::Cooldown => s.phase = Some("Cooldown".into()),
        TickReceipt::Applied { from, to } => {
            s.phase = Some(if to > from { "Growing" } else { "Shrinking" }.into());
            s.current_limit = Some(to.to_string());
            s.last_decision = Some(format!("{from} -> {to}"));
            s.last_change_epoch = Some(now_secs());
        }
        TickReceipt::DryRunWouldApply { from, to } => {
            s.phase = Some("ShadowWouldApply".into());
            s.current_limit = Some(from.to_string());
            s.last_decision = Some(format!("dry-run: {from} -> {to}"));
        }
        TickReceipt::Observed { decision } => {
            s.phase = Some(match decision {
                Decision::Hold => "Holding",
                Decision::AtCeiling { .. } => "AtCeiling",
                Decision::NoSafeShrink { .. } => "AtFloor",
                Decision::NoLimit => "NoLimit",
                Decision::Grow { .. } | Decision::Shrink { .. } => "Observed",
            }.into());
        }
        TickReceipt::Error { error } => {
            s.phase = Some("Error".into());
            s.last_decision = Some(error.to_string());
        }
    }
    s
}

async fn patch_status<B: Band>(ctx: &Ctx, ns: &str, name: &str, status: &BandStatus) -> Result<(), Error> {
    let api: Api<B> = Api::namespaced(ctx.client.clone(), ns);
    let patch = json!({ "status": status });
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch)).await?;
    Ok(())
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
            let mut s = BandStatus::default();
            s.phase = Some("Error".into());
            s.last_decision = Some(e.to_string());
            patch_status::<B>(&ctx, &ns, &name, &s).await?;
            return Ok(Action::requeue(ctx.requeue));
        }
    };

    let in_cooldown = obj
        .last_change_epoch()
        .is_some_and(|last| now_secs().saturating_sub(last) < obj.cooldown_seconds() as i64);

    let provider = BandProvider::new(KubeCluster::new(ctx.client.clone(), ctx.prometheus_url.clone()), D::default());
    let input = ReconcileInput {
        target: &target,
        cfg: &cfg,
        max_staleness_secs: obj.max_staleness_seconds(),
        in_cooldown,
        dry_run: obj.dry_run(),
    };

    let receipt = reconcile_one(&input, &provider).await;
    let status = status_for(&receipt);
    info!(dim = %provider.id(), band = %name, target = %target.name, phase = ?status.phase, "reconciled");
    patch_status::<B>(&ctx, &ns, &name, &status).await?;
    Ok(Action::requeue(ctx.requeue))
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
    let ctx = Arc::new(Ctx { client: client.clone(), prometheus_url, requeue });

    info!("breathe-controller starting — memory + cpu + storage dimensions");

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
