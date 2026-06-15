//! BU1 — the node-tier shadow loop: wire `breathe_provision::reconcile_forma`
//! into the running controller so the node-count band actually RECONCILES live
//! (observe-only). Before this, the whole provisioning tier was library +
//! self-tests, never instantiated — nothing in the fabric's node rung ran.
//!
//! This is OBSERVE-ONLY by construction: the [`KubeNodeProvedor`] reads real
//! node demand/capacity from the apiserver but its `provision`/`deprovision`
//! return [`ProvisionReceipt::DryRun`] — it mutates NOTHING. It proves the
//! observe→predict→decide→(would-provision)→admit pipeline runs on live signal,
//! and emits the decision as metrics + logs so the shadow is watchable. The
//! real actuator (a magma `Plan`) is BU10, gated on magma's node path.
#![allow(clippy::doc_markdown, clippy::integer_division)]

use std::time::Duration;

use breathe_admission::{Allocatable, CapacidadeProof, Portao, Viveiro};
use breathe_auction::{BandLeiloeiro, ReactivePrevisor};
use breathe_control::BandConfig;
use breathe_provider::{Forma, FormaSample, ProviderError, ProvisionReceipt, Provedor};
use breathe_provision::{reconcile_forma, FormaTick};
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{api::ListParams, Api, Client};
use metrics::{counter, gauge};
use tracing::{info, warn};

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

/// A minted shadow node — the admission `T`. Its `allocatable` is the mean
/// per-node CPU (millicores) observed at provision time; the `CapacidadeProof`
/// gate checks it against the band floor.
#[derive(Debug, Clone)]
pub struct NodeRef {
    allocatable: u64,
}
impl Allocatable for NodeRef {
    fn allocatable(&self) -> u64 {
        self.allocatable
    }
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
    /// The mean per-node allocatable (millicores) — exposed so the shadow loop
    /// can mint a `NodeRef` whose size matches a real node.
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

fn node_ready(n: &Node) -> bool {
    n.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        .unwrap_or(false)
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
        // node-equivalents of demand; never below 1 once any node exists.
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

/// The shadow node-Forma reconcile loop — runs `reconcile_forma` every
/// `interval` against the live `KubeNodeProvedor`, observe-only, emitting the
/// decision as metrics + logs. Perpetual (joined into the controller's main
/// `tokio::join!`). Mutates nothing: a `Grew` decision only DryRuns the
/// provision and admits minted shadow `NodeRef`s into a per-tick `Viveiro`.
pub async fn run_node_forma_shadow(client: Client, cfg: BandConfig, interval: Duration) {
    let provedor = KubeNodeProvedor::new(client);
    // CapacidadeProof is the one real admission gate; a shadow node must clear a
    // 1-millicore floor (everything does) — the gate's bite lands live at BU11.
    let gates: Vec<Box<dyn Portao<NodeRef>>> = vec![Box::new(CapacidadeProof { required_floor: 1 })];
    let mut ticker = tokio::time::interval(interval);
    info!(
        forma = "node-on-demand",
        floor = cfg.floor_bytes,
        ceiling = cfg.ceiling_bytes,
        "node-Forma SHADOW loop starting (observe-only; provision = DryRun)"
    );
    loop {
        ticker.tick().await;

        // Metric gauges from a standalone observe (cheap) so the shadow is
        // watchable on the breathe dashboard even on a Held tick.
        if let Ok(s) = provedor.observe().await {
            gauge!("breathe_forma_used", "forma" => "node-on-demand").set(s.used as f64);
            gauge!("breathe_forma_capacity", "forma" => "node-on-demand").set(s.capacity as f64);
            if s.capacity > 0 {
                gauge!("breathe_forma_util_ratio", "forma" => "node-on-demand")
                    .set(s.used as f64 / s.capacity as f64);
            }
        }

        let alloc = provedor.per_node_alloc_milli().await;
        let mut viveiro: Viveiro<NodeRef> = Viveiro::new(); // shadow: fresh per tick
        let tick = reconcile_forma(
            Forma::NodeOnDemand,
            &provedor,
            &ReactivePrevisor,
            &BandLeiloeiro,
            &cfg,
            &gates,
            3,
            &mut viveiro,
            |_id| NodeRef { allocatable: alloc },
        )
        .await;

        record_tick(&tick);
    }
}

/// Surface a `FormaTick` as a typed metric + an info/warn log line.
fn record_tick(tick: &FormaTick) {
    let outcome = match tick {
        FormaTick::Held => "held",
        FormaTick::Grew { forma, requested, admitted, rejected } => {
            gauge!("breathe_forma_would_provision", "forma" => "node-on-demand").set(*requested as f64);
            info!(?forma, requested, admitted, rejected, "node-Forma SHADOW: would provision (DryRun)");
            "grew"
        }
        FormaTick::Shrank { forma, released } => {
            gauge!("breathe_forma_would_provision", "forma" => "node-on-demand").set(-(*released as f64));
            info!(?forma, released, "node-Forma SHADOW: would deprovision (DryRun)");
            "shrank"
        }
        FormaTick::EnvelopeExhausted { forma, shortfall } => {
            warn!(?forma, shortfall, "node-Forma SHADOW: demand beyond the envelope — would need more nodes than the ceiling allows");
            "envelope_exhausted"
        }
        FormaTick::ObserveError(e) => {
            warn!(error = %e, "node-Forma SHADOW: observe failed");
            "observe_error"
        }
    };
    counter!("breathe_forma_ticks_total", "forma" => "node-on-demand", "outcome" => outcome).increment(1);
}
