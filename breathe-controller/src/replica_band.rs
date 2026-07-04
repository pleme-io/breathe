//! The HORIZONTAL band reconciler — the `ReplicaBand` peer of the vertical
//! MemoryBand/CpuBand reconcile (`crate::reconcile`).
//!
//! It deliberately does NOT route through `breathe_core::reconcile_one` / the vertical
//! `decide` loop: that loop holds a `(used, capacity)` LIMIT at a utilization band,
//! and a replica COUNT is a different law (a mis-applied vertical band mis-scales the
//! count). Instead it runs the HORIZONTAL band law
//! (`breathe_control::replica::plan_replica_tick` over the KubeCluster-backed
//! `KubeReplicaEnv`), but RIDES the same substrate as every other band — Operating
//! Principle #1, reuse don't double up:
//!   * the SAME shadow→confirm→effect gate (`Band::effective_dry_run`);
//!   * the SAME SSA actuator (`KubeCluster::apply` over `LimitLayout::Replica` → a
//!     no-`.force()` cooperative-yield write of `.spec.replicas`);
//!   * the SAME status machinery (`breathe_runtime::replica_status_for` — same phases,
//!     conditions, counters, cooldown, history as `status_for`);
//!   * the SAME durable counter fold (`ctx.decisions` DecisionLog);
//!   * the SAME per-restart-class requeue cadence (`replica_next_requeue`).
//!
//! Flow: OBSERVE (async, one pass → a sync env snapshot) → never-scale-on-a-stale gate
//! → PLAN (pure `plan_replica_tick`: interpret + shadow/cooldown/scale-in-policy/force)
//! → ACTUATE (async SSA, only when the plan says to) → typed `ReplicaReceipt` → STATUS.

use std::sync::Arc;

use breathe_control::replica::{plan_replica_tick, ReplicaGate};
use breathe_crd::{Band, ReplicaBand};
use breathe_kube::KubeCluster;
use breathe_provider::{Cluster, DisruptionClass, ProviderError, SsaPatch, Target};
use breathe_runtime::{
    counters_from_status, error_status, now_secs, patch_status, replica_entry_for, replica_next_requeue,
    replica_status_for, rfc3339_in_future, suspended_status, ReplicaReceipt,
};
use breathe_store::BandRef;
use kube::runtime::controller::Action;
use kube::ResourceExt;
use tracing::{error, info, warn};

use crate::{Ctx, Error};

/// The SSA field manager for the horizontal `.spec.replicas` write — distinct from
/// the vertical managers so a `kubectl get <obj> -o yaml` managedFields read shows
/// exactly which breathe band owns the count.
const FIELD_MANAGER: &str = "breathe/replica";

/// Reconcile one `ReplicaBand`: observe → plan (pure horizontal law + gate) → actuate
/// (SSA) → status. The horizontal peer of `crate::reconcile`; shares the gate, the
/// actuator, and the status machinery, never the vertical decide loop.
pub async fn reconcile_replica_band(obj: Arc<ReplicaBand>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let ns = obj.namespace().unwrap_or_default();
    let name = obj.name_any();

    // SUSPEND: a frozen band skips observe/plan/act entirely — the count is left as-is.
    if obj.suspended() {
        patch_status::<ReplicaBand>(&ctx.client, &ns, &name, &suspended_status(obj.status())).await?;
        return Ok(Action::requeue(ctx.requeue));
    }

    let tr = obj.target_ref();
    let target = Target {
        namespace: ns.clone(),
        name: tr.name.clone(),
        kind: tr.kind.clone(),
        api_version: tr.api_version.clone().unwrap_or_default(),
        container: tr.container.clone(),
        pod_selector: tr.pod_selector.clone(),
    };

    // parse-time gate: a malformed band is a typed error status, never a silent scale.
    let cfg = obj.spec.replica_band_config();
    if let Err(e) = cfg.validate() {
        patch_status::<ReplicaBand>(&ctx.client, &ns, &name, &error_status(e.to_string())).await?;
        return Ok(Action::requeue(ctx.requeue));
    }

    let layout = obj.spec.provider_layout();
    let signal = obj.spec.provider_metric();
    let reclaim = obj.spec.provider_reclaim_metric();
    let cluster = KubeCluster::new(ctx.client.clone(), ctx.prometheus_url.clone());

    // OBSERVE (async): current `.spec.replicas` + the driving signal (raw f64) + the
    // optional spot-reclaim signal, frozen into one sync `ReplicaEnvironment` snapshot.
    let env = match cluster.observe_replica_env(&target, &layout, &signal, reclaim.as_ref()).await {
        Ok(e) => e,
        Err(e) => {
            let msg = match &e {
                ProviderError::TargetNotFound => format!("target {}/{} not found", target.kind, target.name),
                other => other.to_string(),
            };
            patch_status::<ReplicaBand>(&ctx.client, &ns, &name, &error_status(msg)).await?;
            return Ok(Action::requeue(ctx.requeue));
        }
    };

    let now = now_secs();
    let metric_ratio = cfg.signal.metric_ratio(env.current(), env.signal(), cfg.target);
    let staleness = env.staleness_secs();
    let prior = obj.status();
    // The effective (lifecycle-gated) dry-run for this tick — the SAME confirm gate the
    // vertical bands use. Computed once; the plan's gate and the status share it.
    let dry_run = obj.effective_dry_run(now);

    // NEVER SCALE ON A STALE SAMPLE → held, reported Stale (no plan, no write).
    let receipt = if staleness > obj.max_staleness_seconds() {
        ReplicaReceipt::Stale { staleness_secs: staleness, current: env.current() }
    } else {
        let in_cooldown = obj
            .last_change_epoch()
            .is_some_and(|last| now.saturating_sub(last) < obj.cooldown_seconds() as i64);
        // a scale-IN sheds a pod (RestartRequiring); the default `restartFreeOnly`
        // scales OUT freely but gates the scale-in — set `allowRestart` to shed.
        let scale_in_permitted = obj.disruption_policy().permits(DisruptionClass::RestartRequiring);
        // BREAK-GLASS forceLimit: active iff set AND (no expiry OR expiry in the future).
        let force = obj
            .force_limit_value()
            .filter(|_| obj.force_limit_expiry().map_or(true, rfc3339_in_future))
            .and_then(|v| u32::try_from(v).ok());
        let gate = ReplicaGate { dry_run, in_cooldown, scale_in_permitted, force };

        // PLAN (pure): interpret the band law (or the forced count) + apply the gate.
        let plan = match plan_replica_tick(&cfg, &env, gate) {
            Ok(p) => p,
            Err(e) => {
                patch_status::<ReplicaBand>(&ctx.client, &ns, &name, &error_status(e.to_string())).await?;
                return Ok(Action::requeue(ctx.requeue));
            }
        };

        // ACTUATE (async SSA) only when the plan says to — the SAME no-`.force()`
        // cooperative-yield write every generic-path layout uses (a 409 = yield).
        let (applied, conflict) = match plan.actuate {
            Some(to) => {
                let patch = SsaPatch {
                    target: target.clone(),
                    field_manager: FIELD_MANAGER.into(),
                    layout: layout.clone(),
                    resource: "count".into(),
                    value: u64::from(to),
                };
                match cluster.apply(&patch).await {
                    Ok(_) => (true, false),
                    // KubeCluster maps a 409 field-conflict (a competing KEDA/HPA owns
                    // `.spec.replicas`) to ApiTransient → yield + re-observe next tick.
                    Err(ProviderError::ApiTransient(m)) => {
                        warn!(band = %name, error = %m, "replica SSA yielded `.spec.replicas` to a competing writer");
                        (false, true)
                    }
                    Err(e) => {
                        patch_status::<ReplicaBand>(&ctx.client, &ns, &name, &error_status(e.to_string())).await?;
                        return Ok(Action::requeue(ctx.requeue));
                    }
                }
            }
            None => (false, false),
        };
        ReplicaReceipt::resolve(&plan, applied, conflict, dry_run, in_cooldown)
    };

    // COUNTERS — the SAME durable DecisionLog fold the vertical bands use.
    let band_ref = BandRef::new(&<ReplicaBand as kube::Resource>::kind(&()), &ns, &name);
    let entry = replica_entry_for(&receipt, dry_run);
    let counters = match ctx.decisions.append(&band_ref, counters_from_status(prior), entry).await {
        Ok(c) => c,
        Err(e) => {
            warn!(band = %name, error = %e, "replica decision-log append failed — holding counters");
            counters_from_status(prior)
        }
    };

    let status = replica_status_for(
        &receipt,
        metric_ratio,
        staleness,
        dry_run,
        obj.disruption_policy(),
        prior,
        obj.cooldown_seconds(),
        obj.generation(),
        counters,
    );
    metrics::gauge!("breathe_replica_current_replicas", "namespace" => ns.clone(), "name" => name.clone())
        .set(f64::from(env.current()));
    info!(dim = "replica", band = %name, target = %target.name, phase = ?status.phase, ratio = metric_ratio, "replica reconciled");
    patch_status::<ReplicaBand>(&ctx.client, &ns, &name, &status).await?;
    Ok(Action::requeue(replica_next_requeue(&receipt, &ctx.cooldowns)))
}

pub fn error_policy_replica_band(_obj: Arc<ReplicaBand>, err: &Error, ctx: Arc<Ctx>) -> Action {
    error!(error = %err, "replica reconcile error — backing off");
    Action::requeue(ctx.requeue)
}
