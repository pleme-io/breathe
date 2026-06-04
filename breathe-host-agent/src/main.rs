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
    ArcBand, Band, BreatheNodePool, CgroupBand, CgroupCpuBand, GiB, NodePoolStatus,
};
use breathe_host::{
    ArcDescriptor, CgroupCpuDescriptor, CgroupMemoryDescriptor, CpuSampleCache, HostCluster,
    NodeEnvelopes, SystemdSysfsEnv, new_cpu_sample_cache,
};
use breathe_provider::{BandProvider, ClassCooldowns, DimensionDescriptor, ResourceProvider, Target};
use breathe_runtime::{
    error_status, event_for, metrics_for, next_requeue, now_secs, patch_status, should_emit_event, status_for,
    BandLabels, EventKind,
};
use futures::StreamExt;
use kube::{
    api::{Api, ListParams, Patch, PatchParams},
    runtime::{
        controller::Action,
        events::{Event, EventType, Recorder, Reporter},
        watcher, Controller,
    },
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
    /// Long-lived cross-tick cpu-usage samples — shared into every per-tick
    /// `HostCluster` so the cgroup-cpu RATE is differenced across ticks.
    cpu_samples: CpuSampleCache,
    /// The k8s Event reporter identity (agent name + node instance).
    reporter: Reporter,
}

/// Publish a k8s Event for this host tick onto `obj`, transition-gated. Non-fatal.
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

/// Find this node's enrollment charter (cluster-scoped; matched by `nodeName`).
async fn node_pool(ctx: &Ctx) -> Result<Option<BreatheNodePool>, Error> {
    let api: Api<BreatheNodePool> = Api::all(ctx.client.clone());
    let list = api.list(&ListParams::default()).await?;
    Ok(list.into_iter().find(|p| p.spec.node_name == ctx.node_name))
}

/// The L2 ceilings (GiB in the CR → bytes for the provider). Total by
/// construction: `checked_mul` is the truly-unrepresentable backstop to the CRD's
/// parse-time `GiB` bound, so an overflowing ceiling can never silently wrap into
/// SAFETY WALL 2 — it is refused (and the node held), never written blind.
fn envelopes_from(pool: &BreatheNodePool) -> Result<NodeEnvelopes, String> {
    let to_bytes = |g: GiB, what: &str| -> Result<u64, String> {
        g.0.checked_mul(GIB).ok_or_else(|| format!("{what} = {} GiB overflows u64 bytes", g.0))
    };
    let arc_max_bytes = to_bytes(pool.spec.arc_max_gi_b, "arcMaxGiB")?;
    let mut cgroup_max_bytes = std::collections::BTreeMap::new();
    for (unit, gib) in &pool.spec.cgroup_max_gi_b {
        cgroup_max_bytes.insert(unit.clone(), to_bytes(*gib, unit)?);
    }
    // cpu ceilings are already millicores (no byte conversion / overflow risk).
    let cgroup_cpu_max_millicores = pool.spec.cgroup_cpu_max_milli.clone();
    Ok(NodeEnvelopes { arc_max_bytes, cgroup_max_bytes, cgroup_cpu_max_millicores })
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
    // A ceiling that can't convert to bytes (overflow) ⇒ refuse the node, never
    // manage with a corrupt SAFETY WALL 2.
    let envelopes = match envelopes_from(&pool) {
        Ok(e) => e,
        Err(reason) => {
            let s = error_status(format!("invalid BreatheNodePool envelopes: {reason}"));
            patch_status::<B>(&ctx.client, &ns, &name, &s).await?;
            warn!(node = %ctx.node_name, band = %name, %reason, "bad envelopes — holding");
            return Ok(Action::requeue(ctx.requeue));
        }
    };
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
        HostCluster::new(SystemdSysfsEnv::from_env(), envelopes, write_enabled)
            .with_cpu_samples(ctx.cpu_samples.clone()),
        D::default(),
    );
    let input = ReconcileInput {
        target: &target,
        cfg: &cfg,
        max_staleness_secs: obj.max_staleness_seconds(),
        in_cooldown,
        dry_run: effective_dry_run,
        // host carves (ARC/cgroup) are ALWAYS RestartFree → any policy permits
        // them; honor the band's declared policy for consistency anyway.
        policy: obj.disruption_policy(),
    };

    let outcome = reconcile_one(&input, &provider).await;
    let prior_phase = obj.status().and_then(|s| s.phase.as_deref()).map(String::from);
    let status = status_for(&outcome, obj.status(), obj.cooldown_seconds(), obj.generation());
    info!(
        dim = %provider.id(), band = %name, unit = %target.name,
        write_enabled, phase = ?status.phase, "host reconciled"
    );
    emit_event(&ctx, obj.as_ref(), &outcome.receipt, status.phase.as_deref(), prior_phase.as_deref()).await;
    metrics_for(
        &BandLabels { dim: provider.id().to_string(), namespace: ns.clone(), name: name.clone() },
        &outcome,
        &cfg,
        status.cooldown_remaining_seconds.unwrap_or(0),
    );
    patch_status::<B>(&ctx.client, &ns, &name, &status).await?;
    // host carves are all RestartFree → re-tick at the fast golden cadence.
    Ok(Action::requeue(next_requeue(&outcome.receipt, &ClassCooldowns::default())))
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
        // every host lever this node manages: cgroup-memory units + cgroup-cpu units + ARC (1).
        managed_units: Some((obj.spec.cgroup_max_gi_b.len() + obj.spec.cgroup_cpu_max_milli.len() + 1) as i64),
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
    // Prometheus /metrics on :9101 (9100 is host node-exporter) — scraped via a
    // VMPodScrape on the DaemonSet. Non-fatal install.
    if let Err(e) = metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], 9101))
        .install()
    {
        error!(error = %e, "failed to install /metrics exporter — continuing without metrics");
    }
    metrics::gauge!("breathe_build_info", "binary" => "breathe-host-agent", "version" => env!("CARGO_PKG_VERSION")).set(1.0);

    let reporter = Reporter {
        controller: "breathe-host-agent".into(),
        instance: std::env::var("POD_NAME").ok().or_else(|| (!node_name.is_empty()).then(|| node_name.clone())),
    };
    let ctx = Arc::new(Ctx {
        client: client.clone(),
        requeue,
        node_name: node_name.clone(),
        cpu_samples: new_cpu_sample_cache(),
        reporter,
    });

    info!(node = %node_name, "breathe-host-agent starting — arc + cgroup-memory + cgroup-cpu host dimensions");

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
    let cgroup_cpu = Controller::new(Api::<CgroupCpuBand>::all(client.clone()), watcher::Config::default())
        .run(
            reconcile_host::<CgroupCpuBand, CgroupCpuDescriptor>,
            error_policy::<CgroupCpuBand>,
            ctx.clone(),
        )
        .for_each(|_| async {});
    let pool = Controller::new(Api::<BreatheNodePool>::all(client.clone()), watcher::Config::default())
        .run(reconcile_pool, pool_error_policy, ctx.clone())
        .for_each(|_| async {});

    tokio::join!(arc, cgroup, cgroup_cpu, pool);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use breathe_crd::BreatheNodePoolSpec;
    use std::collections::BTreeMap;

    fn pool(arc_gib: u64, units: &[(&str, u64)]) -> BreatheNodePool {
        let mut m = BTreeMap::new();
        for (u, g) in units {
            m.insert((*u).to_string(), GiB(*g));
        }
        BreatheNodePool::new(
            "rio",
            BreatheNodePoolSpec {
                node_name: "rio".into(),
                arc_max_gi_b: GiB(arc_gib),
                cgroup_max_gi_b: m,
                cgroup_cpu_max_milli: BTreeMap::new(),
                write_enabled: false,
            },
        )
    }

    #[test]
    fn envelopes_convert_gib_to_bytes() {
        let e = envelopes_from(&pool(6, &[("nix-daemon.service", 12)])).unwrap();
        assert_eq!(e.arc_max_bytes, 6 * GIB);
        assert_eq!(e.cgroup_max_bytes.get("nix-daemon.service"), Some(&(12 * GIB)));
    }

    #[test]
    fn an_overflowing_ceiling_is_refused_never_wrapped() {
        // the safety-review PoC: a ceiling whose *2^30 wraps u64 must REFUSE
        // (so SAFETY WALL 2 is never corrupted), never silently wrap to a small
        // value that would let a live write exceed the L2 partition.
        assert!(envelopes_from(&pool(u64::MAX, &[])).is_err(), "overflowing arc ceiling must refuse");
        assert!(
            envelopes_from(&pool(6, &[("x.service", u64::MAX)])).is_err(),
            "overflowing cgroup ceiling must refuse"
        );
    }
}
