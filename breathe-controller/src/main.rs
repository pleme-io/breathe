//! `breathe-controller` — the memory-dimension controller binary.
//!
//! Watches `MemoryBand` CRs and, per band, runs the composed reconcile loop
//! (`breathe_core::reconcile_one`: observe → plan → assign) against the real
//! `KubeCluster`, then patches the band's typed status. breathe-core owns the
//! loop; this binary supplies the kube runtime, the provider wiring, and the
//! level-triggered requeue. Config via env:
//!
//!   BREATHE_PROMETHEUS_URL  — Prometheus/VictoriaMetrics base (required)
//!   BREATHE_REQUEUE_SECONDS — refresh interval (default 60)

use std::{sync::Arc, time::Duration, time::{SystemTime, UNIX_EPOCH}};

use breathe_core::{reconcile_one, ReconcileInput, TickReceipt};
use breathe_crd::{MemoryBand, MemoryBandStatus};
use breathe_kube::KubeCluster;
use breathe_provider::Target;
use dimension_memory::MemoryProvider;
use futures::StreamExt;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{controller::Action, watcher, Controller},
    Client, ResourceExt,
};
use serde_json::json;
use tracing::{error, info, warn};

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("kube: {0}")]
    Kube(#[from] kube::Error),
}

struct Ctx {
    client: Client,
    provider: MemoryProvider<KubeCluster>,
    requeue: Duration,
}

fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

/// Map a tick receipt to the typed CR status (the per-cycle receipt).
fn status_for(receipt: &TickReceipt) -> MemoryBandStatus {
    use breathe_control::Decision;
    let mut s = MemoryBandStatus::default();
    match receipt {
        TickReceipt::Conflict { manager } => {
            s.phase = Some("Conflict".into());
            s.conflict_manager = Some(manager.clone());
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

async fn reconcile(mb: Arc<MemoryBand>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let ns = mb.namespace().unwrap_or_default();
    let name = mb.name_any();
    let tr = &mb.spec.target_ref;
    let target = Target {
        namespace: ns.clone(),
        name: tr.name.clone(),
        kind: tr.kind.clone(),
        api_version: tr.api_version.clone().unwrap_or_default(),
        container: tr.container.clone(),
    };

    let cfg = match mb.spec.band_config() {
        Ok(c) => c,
        Err(e) => {
            warn!(band = %name, error = %e, "invalid band spec — holding");
            patch_status(&ctx, &ns, &name, &{
                let mut s = MemoryBandStatus::default();
                s.phase = Some("Error".into());
                s.last_decision = Some(e.to_string());
                s
            }).await?;
            return Ok(Action::requeue(ctx.requeue));
        }
    };

    let in_cooldown = mb
        .status
        .as_ref()
        .and_then(|s| s.last_change_epoch)
        .is_some_and(|last| now_secs().saturating_sub(last) < mb.spec.cooldown_seconds as i64);

    let input = ReconcileInput {
        target: &target,
        cfg: &cfg,
        max_staleness_secs: mb.spec.max_staleness_seconds,
        in_cooldown,
        dry_run: mb.spec.dry_run,
    };

    let receipt = reconcile_one(&input, &ctx.provider).await;
    let status = status_for(&receipt);
    info!(band = %name, target = %target.name, phase = ?status.phase, "reconciled");
    patch_status(&ctx, &ns, &name, &status).await?;
    Ok(Action::requeue(ctx.requeue))
}

async fn patch_status(
    ctx: &Ctx,
    ns: &str,
    name: &str,
    status: &MemoryBandStatus,
) -> Result<(), Error> {
    let api: Api<MemoryBand> = Api::namespaced(ctx.client.clone(), ns);
    let patch = json!({ "status": status });
    // diff-gated by the apiserver: a no-op status merge is a cheap no-write.
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch)).await?;
    Ok(())
}

fn error_policy(_mb: Arc<MemoryBand>, err: &Error, ctx: Arc<Ctx>) -> Action {
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

    let prometheus_url = std::env::var("BREATHE_PROMETHEUS_URL")
        .unwrap_or_else(|_| "http://vmsingle-victoria-metrics-k8s-stack.monitoring.svc.cluster.local:8429".into());
    let requeue = Duration::from_secs(
        std::env::var("BREATHE_REQUEUE_SECONDS").ok().and_then(|s| s.parse().ok()).unwrap_or(60),
    );

    let client = Client::try_default().await?;
    let provider = MemoryProvider::new(KubeCluster::new(client.clone(), prometheus_url));
    let ctx = Arc::new(Ctx { client: client.clone(), provider, requeue });

    let bands: Api<MemoryBand> = Api::all(client);
    info!("breathe-controller starting — watching MemoryBand (memory dimension)");
    Controller::new(bands, watcher::Config::default())
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _action)) => info!(band = %obj.name, "tick ok"),
                Err(e) => warn!(error = %e, "controller stream error"),
            }
        })
        .await;
    Ok(())
}
