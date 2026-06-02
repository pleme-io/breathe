//! `breathe-host-agent` — the HANDS.
//!
//! A privileged DaemonSet pod that runs the FULL host reconcile loop locally on
//! its node. `HostCluster` does host read+write, so the agent is self-contained
//! per host dimension: no cross-process target handoff. It watches `ArcBand` /
//! `CgroupBand`, reads its node's [`BreatheNodePool`] for the L2 ceilings and the
//! master write switch, builds a [`HostCluster`], and drives the *same* generic
//! `reconcile_one` the brain uses — only the `Cluster` impl differs.
//!
//! ### Shadow-first by construction
//! `effective_dry_run = band.dryRun || !pool.writeEnabled`. The node-level master
//! switch forces shadow regardless of any band's setting, and `HostCluster` is
//! built with `write_enabled = pool.writeEnabled` as the second wall — so when the
//! node is in shadow the agent reports `ShadowWouldApply` and `apply` is a no-op.
//! A host write happens only when BOTH the pool master switch is on AND the band's
//! `dryRun` is off — and even then never above the L2 ceiling.
//!
//! Config via env:
//!   NODE_NAME               — the node this agent runs on (downward API spec.nodeName)
//!   BREATHE_REQUEUE_SECONDS — refresh interval (default 30; host metrics are live)

use std::{sync::Arc, time::Duration};

use breathe_core::{reconcile_one, ReconcileInput};
use breathe_crd::{
    ArcBand, Band, BreatheNodePool, CgroupBand, NodePoolStatus,
};
use breathe_host::{ArcDescriptor, CgroupMemoryDescriptor, HostCluster, NodeEnvelopes, SystemdSysfsEnv};
use breathe_provider::{BandProvider, DimensionDescriptor, ResourceProvider, Target};
use breathe_runtime::{error_status, now_secs, patch_status, status_for};
use futures::StreamExt;
use kube::{
    api::{Api, ListParams, Patch, PatchParams},
    runtime::{controller::Action, watcher, Controller},
    Client, ResourceExt,
};
use serde_json::json;
use tracing::{error, info, warn};

const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("kube: {0}")]
    Kube(#[from] kube::Error),
}

struct Ctx {
    client: Client,
    requeue: Duration,
    node_name: String,
}

/// Find this node's enrollment charter (cluster-scoped; matched by `nodeName`).
async fn node_pool(ctx: &Ctx) -> Result<Option<BreatheNodePool>, Error> {
    let api: Api<BreatheNodePool> = Api::all(ctx.client.clone());
    let list = api.list(&ListParams::default()).await?;
    Ok(list.into_iter().find(|p| p.spec.node_name == ctx.node_name))
}

/// The L2 ceilings (GiB in the CR → bytes for the provider).
fn envelopes_from(pool: &BreatheNodePool) -> NodeEnvelopes {
    NodeEnvelopes {
        arc_max_bytes: pool.spec.arc_max_gi_b * GIB,
        cgroup_max_bytes: pool
            .spec
            .cgroup_max_gi_b
            .iter()
            .map(|(unit, gib)| (unit.clone(), gib * GIB))
            .collect(),
    }
}

/// The one host reconcile body for every host dimension. `B` is the band kind,
/// `D` its descriptor. Mirrors the brain's reconcile, but over `HostCluster` and
/// gated by the node's `BreatheNodePool`.
async fn reconcile_host<B: Band, D: DimensionDescriptor + Default>(
    obj: Arc<B>,
    ctx: Arc<Ctx>,
) -> Result<Action, Error> {
    let ns = obj.namespace().unwrap_or_default();
    let name = obj.name_any();

    // The enrollment charter carries the L2 ceilings + the master write switch.
    // No charter for this node ⇒ refuse to manage anything (never write blind).
    let Some(pool) = node_pool(&ctx).await? else {
        let s = error_status(format!("no BreatheNodePool enrolls node {}", ctx.node_name));
        patch_status::<B>(&ctx.client, &ns, &name, &s).await?;
        warn!(node = %ctx.node_name, band = %name, "unenrolled node — holding");
        return Ok(Action::requeue(ctx.requeue));
    };
    let envelopes = envelopes_from(&pool);
    let write_enabled = pool.spec.write_enabled;

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

    // The node master switch forces shadow: never report Applied when the agent
    // is not permitted to write the host.
    let effective_dry_run = obj.dry_run() || !write_enabled;

    let provider = BandProvider::new(
        HostCluster::new(SystemdSysfsEnv, envelopes, write_enabled),
        D::default(),
    );
    let input = ReconcileInput {
        target: &target,
        cfg: &cfg,
        max_staleness_secs: obj.max_staleness_seconds(),
        in_cooldown,
        dry_run: effective_dry_run,
    };

    let receipt = reconcile_one(&input, &provider).await;
    let status = status_for(&receipt);
    info!(
        dim = %provider.id(), band = %name, unit = %target.name,
        write_enabled, phase = ?status.phase, "host reconciled"
    );
    patch_status::<B>(&ctx.client, &ns, &name, &status).await?;
    Ok(Action::requeue(ctx.requeue))
}

/// Reconcile the node's own enrollment charter — surface it as Active so
/// `kubectl get bnp` shows the agent has adopted it.
async fn reconcile_pool(obj: Arc<BreatheNodePool>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let name = obj.name_any();
    if obj.spec.node_name != ctx.node_name {
        // not ours — another node's agent owns it.
        return Ok(Action::requeue(ctx.requeue));
    }
    let status = NodePoolStatus {
        phase: Some(if obj.spec.write_enabled { "Active".into() } else { "Shadow".into() }),
        observed_node: Some(ctx.node_name.clone()),
        managed_units: Some(obj.spec.cgroup_max_gi_b.len() as i64),
        last_seen_epoch: Some(now_secs()),
    };
    let api: Api<BreatheNodePool> = Api::all(ctx.client.clone());
    api.patch_status(&name, &PatchParams::default(), &Patch::Merge(&json!({ "status": status })))
        .await?;
    Ok(Action::requeue(ctx.requeue))
}

fn error_policy<B: Band>(_obj: Arc<B>, err: &Error, ctx: Arc<Ctx>) -> Action {
    error!(error = %err, "host reconcile error — backing off");
    Action::requeue(ctx.requeue)
}

fn pool_error_policy(_obj: Arc<BreatheNodePool>, err: &Error, ctx: Arc<Ctx>) -> Action {
    error!(error = %err, "nodepool reconcile error — backing off");
    Action::requeue(ctx.requeue)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,breathe_host_agent=info".into()),
        )
        .init();

    let node_name = std::env::var("NODE_NAME").unwrap_or_default();
    if node_name.is_empty() {
        warn!("NODE_NAME is empty — set it via the downward API (spec.nodeName); the agent will not match any BreatheNodePool");
    }
    let requeue = Duration::from_secs(
        std::env::var("BREATHE_REQUEUE_SECONDS").ok().and_then(|s| s.parse().ok()).unwrap_or(30),
    );
    let client = Client::try_default().await?;
    let ctx = Arc::new(Ctx { client: client.clone(), requeue, node_name: node_name.clone() });

    info!(node = %node_name, "breathe-host-agent starting — arc + cgroup host dimensions");

    let arc = Controller::new(Api::<ArcBand>::all(client.clone()), watcher::Config::default())
        .run(reconcile_host::<ArcBand, ArcDescriptor>, error_policy::<ArcBand>, ctx.clone())
        .for_each(|_| async {});
    let cgroup = Controller::new(Api::<CgroupBand>::all(client.clone()), watcher::Config::default())
        .run(
            reconcile_host::<CgroupBand, CgroupMemoryDescriptor>,
            error_policy::<CgroupBand>,
            ctx.clone(),
        )
        .for_each(|_| async {});
    let pool = Controller::new(Api::<BreatheNodePool>::all(client.clone()), watcher::Config::default())
        .run(reconcile_pool, pool_error_policy, ctx.clone())
        .for_each(|_| async {});

    tokio::join!(arc, cgroup, pool);
    Ok(())
}
