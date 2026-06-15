//! The node-tier reconciler: drives `breathe_provision::reconcile_forma` in the
//! live controller, watching `BreatheCloudPool` CRs (BU2). Each pool binds a
//! `Forma` to a `Densa`-style envelope; the same shape-blind band law that holds
//! a pod's memory at 80% holds a node POOL's count at 80%.
//!
//! OBSERVE-ONLY by construction: the [`KubeNodeProvedor`] reads real node
//! demand/capacity from the apiserver but its `provision`/`deprovision` return
//! [`ProvisionReceipt::DryRun`] — it mutates NOTHING. It proves the
//! observe→predict→decide→(would-provision)→admit pipeline runs on live signal
//! and reports what it WOULD provision via the CR status + metrics. The real
//! actuator (a magma `Plan`) is BU10, gated on magma's node path.
#![allow(clippy::doc_markdown, clippy::integer_division)]

use std::sync::Arc;

use breathe_admission::{Allocatable, CapacidadeProof, Portao, Viveiro};
use breathe_auction::{BandLeiloeiro, LinearTrendPrevisor, Previsao, Previsor, ReactivePrevisor};
use breathe_crd::{BreatheCloudPool, CloudPoolStatus};
use breathe_provider::{Forma, FormaSample, ProviderError, ProvisionReceipt, Provedor};
use breathe_provision::{reconcile_forma, FormaTick};
use breathe_runtime::now_secs;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    api::{Api, ListParams, Patch, PatchParams},
    runtime::controller::Action,
    Client, ResourceExt,
};
use metrics::{counter, gauge};
use tracing::{info, warn};

use crate::{Ctx, Error};

/// Parse a Kubernetes CPU quantity into millicores. `"500m"` → 500, `"2"` →
/// 2000, `"1.5"` → 1500, `"250000000n"` (nanocores) → 250. Unparseable ⇒ 0.
fn parse_cpu_milli(q: &str) -> u64 {
    let q = q.trim();
    if let Some(m) = q.strip_suffix('m') {
        m.trim().parse::<u64>().unwrap_or(0)
    } else if let Some(n) = q.strip_suffix('n') {
        n.trim().parse::<u64>().unwrap_or(0) / 1_000_000
    } else if let Some(u) = q.strip_suffix('u') {
        u.trim().parse::<u64>().unwrap_or(0) / 1_000
    } else {
        (q.parse::<f64>().unwrap_or(0.0) * 1000.0) as u64
    }
}

/// Map the CR's `forma` string onto a typed [`Forma`]. `None` ⇒ unknown shape
/// (the reconcile reports it + skips, never guesses).
fn forma_from_str(s: &str) -> Option<Forma> {
    match s {
        "node-on-demand" => Some(Forma::NodeOnDemand),
        _ => None,
    }
}

/// A minted shadow node — the admission `T`. `allocatable` is the mean per-node
/// CPU (millicores) observed at provision time; `CapacidadeProof` checks it.
#[derive(Debug, Clone)]
pub struct NodeRef {
    allocatable: u64,
}
impl Allocatable for NodeRef {
    fn allocatable(&self) -> u64 {
        self.allocatable
    }
}

fn node_ready(n: &Node) -> bool {
    n.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        .unwrap_or(false)
}

/// Observes the node-count `Forma`'s `(used, capacity)` from the live apiserver:
/// `capacity` = count of Ready nodes; `used` = node-EQUIVALENTS of current
/// demand = ⌈Σ (Running+Pending) pod CPU requests / mean-per-node allocatable⌉.
/// Provision/deprovision are DryRun — **observe-only, mutates nothing** (the M0
/// shadow posture; the magma actuator is BU10).
pub struct KubeNodeProvedor {
    client: Client,
}
impl KubeNodeProvedor {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
    /// The mean per-node allocatable (millicores) — sizes a minted `NodeRef`.
    async fn per_node_alloc_milli(&self) -> u64 {
        match Api::<Node>::all(self.client.clone()).list(&ListParams::default()).await {
            Ok(nodes) => {
                let mut count = 0u64;
                let mut total = 0u64;
                for n in &nodes.items {
                    if !node_ready(n) {
                        continue;
                    }
                    count += 1;
                    if let Some(cpu) =
                        n.status.as_ref().and_then(|s| s.allocatable.as_ref()).and_then(|a| a.get("cpu"))
                    {
                        total += parse_cpu_milli(&cpu.0);
                    }
                }
                if count > 0 { (total / count).max(1) } else { 1 }
            }
            Err(_) => 1,
        }
    }
}

#[async_trait::async_trait]
impl Provedor for KubeNodeProvedor {
    async fn observe(&self) -> Result<FormaSample, ProviderError> {
        let nodes = Api::<Node>::all(self.client.clone())
            .list(&ListParams::default())
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        let mut node_count = 0u64;
        let mut total_alloc_milli = 0u64;
        for n in &nodes.items {
            if !node_ready(n) {
                continue;
            }
            node_count += 1;
            if let Some(cpu) =
                n.status.as_ref().and_then(|s| s.allocatable.as_ref()).and_then(|a| a.get("cpu"))
            {
                total_alloc_milli += parse_cpu_milli(&cpu.0);
            }
        }

        let pods = Api::<Pod>::all(self.client.clone())
            .list(&ListParams::default())
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        let mut demand_milli = 0u64;
        for p in &pods.items {
            let phase = p.status.as_ref().and_then(|s| s.phase.as_deref()).unwrap_or("");
            if phase != "Running" && phase != "Pending" {
                continue;
            }
            if let Some(spec) = &p.spec {
                for c in &spec.containers {
                    if let Some(cpu) =
                        c.resources.as_ref().and_then(|r| r.requests.as_ref()).and_then(|m| m.get("cpu"))
                    {
                        demand_milli += parse_cpu_milli(&cpu.0);
                    }
                }
            }
        }

        let per_node = if node_count > 0 { (total_alloc_milli / node_count).max(1) } else { 1 };
        let used = demand_milli.div_ceil(per_node).max(1);
        let capacity = node_count.max(1);
        Ok(FormaSample { used, capacity })
    }

    async fn provision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        Ok(ProvisionReceipt::DryRun { would: n as i64 })
    }
    async fn deprovision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        Ok(ProvisionReceipt::DryRun { would: -(n as i64) })
    }
}

/// PURE: map a `FormaTick` (+ the observed sample + effective mode) onto the
/// typed `CloudPoolStatus`. The node-tier peer of `breathe_runtime::status_for`;
/// unit-tested below, no I/O — so the CR status, the metrics, and the logs can
/// never disagree about what a tick meant.
#[must_use]
pub fn cloud_pool_status(
    tick: &FormaTick,
    used: Option<u64>,
    capacity: Option<u64>,
    dry_run: bool,
) -> CloudPoolStatus {
    let mut s = CloudPoolStatus {
        observed_used: used.map(|u| u as i64),
        observed_capacity: capacity.map(|c| c as i64),
        effective_dry_run: Some(dry_run),
        last_seen_epoch: Some(now_secs()),
        ..Default::default()
    };
    match tick {
        FormaTick::Held => {
            s.phase = Some("Held".into());
            s.last_decision = Some("in band — held".into());
        }
        FormaTick::Grew { requested, admitted, rejected, .. } => {
            s.phase = Some("Growing".into());
            s.would_provision = Some(*requested as i64);
            s.last_decision =
                Some(format!("would provision {requested} (admitted {admitted}, rejected {rejected})"));
        }
        FormaTick::Shrank { released, .. } => {
            s.phase = Some("Shrinking".into());
            s.would_provision = Some(-(*released as i64));
            s.last_decision = Some(format!("would deprovision {released}"));
        }
        FormaTick::EnvelopeExhausted { shortfall, .. } => {
            s.phase = Some("EnvelopeExhausted".into());
            s.last_decision = Some(format!("demand beyond the envelope — short {shortfall} nodes"));
        }
        FormaTick::ObserveError(e) => {
            s.phase = Some("Error".into());
            s.last_decision = Some(format!("observe failed: {e}"));
        }
    }
    s
}

/// The outcome label for `breathe_forma_ticks_total`.
fn outcome_of(tick: &FormaTick) -> &'static str {
    match tick {
        FormaTick::Held => "held",
        FormaTick::Grew { .. } => "grew",
        FormaTick::Shrank { .. } => "shrank",
        FormaTick::EnvelopeExhausted { .. } => "envelope_exhausted",
        FormaTick::ObserveError(_) => "observe_error",
    }
}

/// Reconcile ONE `BreatheCloudPool` — observe→decide→(would-provision)→admit via
/// The per-pool demand previsor selected by `spec.predictive`: typed dispatch
/// (a sum type, no `dyn`) between the reactive echo and the stateful forecaster,
/// so `reconcile_forma` is driven from ONE call site regardless of posture.
enum PoolPrevisor {
    Reactive(ReactivePrevisor),
    Forecast(Arc<LinearTrendPrevisor>),
}

impl Previsor for PoolPrevisor {
    fn predict(&self, used: u64, capacity: u64) -> Previsao {
        match self {
            Self::Reactive(p) => p.predict(used, capacity),
            Self::Forecast(p) => p.predict(used, capacity),
        }
    }
}

/// `reconcile_forma`, map the tick to the CR status, emit metrics, requeue.
/// Observe-only/DryRun: mutates no infrastructure.
pub async fn reconcile_cloud_pool(cr: Arc<BreatheCloudPool>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let name = cr.name_any();
    let provedor = KubeNodeProvedor::new(ctx.client.clone());

    let Some(forma) = forma_from_str(&cr.spec.forma) else {
        warn!(pool = %name, forma = %cr.spec.forma, "BreatheCloudPool: unknown Forma — skipping");
        let st = CloudPoolStatus {
            phase: Some("Error".into()),
            last_decision: Some(format!("unknown forma {:?}", cr.spec.forma)),
            last_seen_epoch: Some(now_secs()),
            ..Default::default()
        };
        patch_status(&ctx.client, &name, &st).await;
        return Ok(Action::requeue(ctx.requeue));
    };
    // Effective shadow = per-pool dryRun OR pool master writeEnabled off. (The
    // actuator is DryRun regardless until BU10, so this is observe-only either way.)
    let dry_run = cr.spec.dry_run || !cr.spec.write_enabled;

    // Observe once for the gauges + status (reconcile_forma observes again).
    let sample = provedor.observe().await.ok();
    if let Some(s) = &sample {
        let labels = [("forma", "node-on-demand".to_string()), ("pool", name.clone())];
        gauge!("breathe_forma_used", &labels).set(s.used as f64);
        gauge!("breathe_forma_capacity", &labels).set(s.capacity as f64);
        if s.capacity > 0 {
            gauge!("breathe_forma_util_ratio", &labels).set(s.used as f64 / s.capacity as f64);
        }
    }

    let alloc = provedor.per_node_alloc_milli().await;
    let gates: Vec<Box<dyn Portao<NodeRef>>> = vec![Box::new(CapacidadeProof { required_floor: 1 })];
    let mut viveiro: Viveiro<NodeRef> = Viveiro::new();
    // Select the demand previsor: monotone-safe forecaster (provisions ahead of
    // the boot dead-time) when `spec.predictive`, else the reactive echo. The
    // forecaster is stateful + per-pool, so it lives in the controller Ctx and
    // is fed once per reconcile here. Horizon = reliefLatency / requeue, in ticks.
    let previsor = if cr.spec.predictive {
        let horizon_ticks = (cr.spec.relief_latency_seconds / ctx.requeue.as_secs().max(1)).max(1);
        PoolPrevisor::Forecast(ctx.forecaster_for(&name, horizon_ticks))
    } else {
        PoolPrevisor::Reactive(ReactivePrevisor)
    };
    let tick = reconcile_forma(
        forma,
        &provedor,
        &previsor,
        &BandLeiloeiro,
        &cr.spec.band_config(),
        &gates,
        3,
        &mut viveiro,
        |_id| NodeRef { allocatable: alloc },
    )
    .await;

    counter!("breathe_forma_ticks_total", "forma" => "node-on-demand", "pool" => name.clone(), "outcome" => outcome_of(&tick)).increment(1);
    let mut status = cloud_pool_status(&tick, sample.as_ref().map(|s| s.used), sample.as_ref().map(|s| s.capacity), dry_run);
    // BU(fillPolicy): surface the scheduler scoring hint the pool's fillPolicy
    // implies — breathe SETS the posture; the scheduler (profile-configured) binds.
    status.scheduler_scoring = Some(cr.spec.fill_policy.scheduler_scoring().to_string());
    status.predictive_active = Some(cr.spec.predictive);
    if let Some(w) = status.would_provision {
        gauge!("breathe_forma_would_provision", "forma" => "node-on-demand", "pool" => name.clone()).set(w as f64);
    }
    info!(pool = %name, forma = ?forma, fill = %cr.spec.fill_policy, phase = ?status.phase, decision = ?status.last_decision, dry_run, "node-Forma reconciled");
    patch_status(&ctx.client, &name, &status).await;

    Ok(Action::requeue(ctx.requeue))
}

/// Patch a `BreatheCloudPool`'s `.status` (cluster-scoped, status subresource).
/// Non-fatal — a failed patch logs + continues (status is observability).
async fn patch_status(client: &Client, name: &str, status: &CloudPoolStatus) {
    let api: Api<BreatheCloudPool> = Api::all(client.clone());
    let patch = serde_json::json!({ "status": status });
    if let Err(e) = api
        .patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
    {
        warn!(pool = %name, error = %e, "BreatheCloudPool status patch failed (non-fatal)");
    }
}

/// Error policy for the cloud-pool controller — back off + requeue.
pub fn error_policy_cloud_pool(_cr: Arc<BreatheCloudPool>, err: &Error, ctx: Arc<Ctx>) -> Action {
    warn!(error = %err, "BreatheCloudPool reconcile error — backing off");
    Action::requeue(ctx.requeue)
}

#[cfg(test)]
mod tests {
    use super::{cloud_pool_status, forma_from_str, outcome_of, parse_cpu_milli};
    use breathe_provider::Forma;
    use breathe_provision::FormaTick;

    #[test]
    fn parses_cpu_quantities_to_millicores() {
        assert_eq!(parse_cpu_milli("500m"), 500);
        assert_eq!(parse_cpu_milli("2"), 2000);
        assert_eq!(parse_cpu_milli("1.5"), 1500);
        assert_eq!(parse_cpu_milli("250000000n"), 250);
        assert_eq!(parse_cpu_milli("garbage"), 0);
    }

    #[test]
    fn forma_string_maps_to_typed_or_none() {
        assert_eq!(forma_from_str("node-on-demand"), Some(Forma::NodeOnDemand));
        assert_eq!(forma_from_str("nonsense"), None);
    }

    #[test]
    fn status_maps_grow_to_would_provision_and_records_observed() {
        let s = cloud_pool_status(
            &FormaTick::Grew { forma: Forma::NodeOnDemand, requested: 2, admitted: 2, rejected: 0 },
            Some(5),
            Some(4),
            true,
        );
        assert_eq!(s.phase.as_deref(), Some("Growing"));
        assert_eq!(s.would_provision, Some(2));
        assert_eq!(s.observed_used, Some(5));
        assert_eq!(s.observed_capacity, Some(4));
        assert_eq!(s.effective_dry_run, Some(true));
        assert!(s.last_decision.as_deref().unwrap().contains("would provision 2"));
    }

    #[test]
    fn status_maps_held_envelope_and_error() {
        assert_eq!(cloud_pool_status(&FormaTick::Held, Some(1), Some(2), false).phase.as_deref(), Some("Held"));
        let env = cloud_pool_status(
            &FormaTick::EnvelopeExhausted { forma: Forma::NodeOnDemand, shortfall: 3 },
            None,
            None,
            true,
        );
        assert_eq!(env.phase.as_deref(), Some("EnvelopeExhausted"));
        assert!(env.last_decision.as_deref().unwrap().contains("short 3"));
        let err = cloud_pool_status(&FormaTick::ObserveError("boom".into()), None, None, true);
        assert_eq!(err.phase.as_deref(), Some("Error"));
        assert_eq!(outcome_of(&FormaTick::ObserveError("x".into())), "observe_error");
    }
}
