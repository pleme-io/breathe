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
// correnteza M0 — the compute/auction permutation-space lock (`Lane` etc,
// used below via fully-qualified `breathe_spread::Lane` paths). Lives in the
// sibling `breathe-spread` crate (renamed from `crates/breathe-auction`,
// theory/CORRENTEZA.md §1.2 — it originally shared the literal package name
// `breathe-auction` with the elasticity engine imported above; the rename
// resolved the collision, no import-time aliasing needed anymore).
use breathe_crd::{BreatheCloudPool, CloudPoolStatus};
use breathe_provider::{Forma, FormaSample, ProviderError, ProvisionReceipt, Provedor};
use breathe_provision::{reconcile_forma, FormaTick};
use breathe_runtime::now_secs;
use k8s_openapi::api::core::v1::{Node, NodeSpec, NodeStatus, Pod, Taint};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::{
    api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams},
    runtime::{
        controller::Action,
        events::{Event, EventType, Recorder},
    },
    Client, Resource, ResourceExt,
};
use metrics::{counter, gauge};
use tracing::{debug, error, info, warn};

use crate::eks_nodegroup_provedor::{EksNodegroupProvedor, KubeEksNodegroupEnvironment};
use crate::karpenter_provedor::{KarpenterProvedor, KubeKarpenterEnvironment};
use crate::{Ctx, Error};

/// Parse a Kubernetes CPU quantity into millicores. `"500m"` → 500, `"2"` →
/// 2000, `"1.5"` → 1500, `"250000000n"` (nanocores) → 250. Unparseable ⇒ 0.
pub(crate) fn parse_cpu_milli(q: &str) -> u64 {
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
///
/// Delegates to `Forma`'s own `#[serde(rename_all = "kebab-case")]` derive
/// (`breathe-provider::Forma`) rather than a hand-maintained match — the
/// prior version only recognized `"node-on-demand"`, silently rejecting
/// every other real `Forma` variant (`"node-spot"` included) as `unknown
/// forma` even though `Forma::as_str()` already names it correctly. A
/// second hand-written string table can drift from the enum the moment a
/// new `Forma` variant lands; this can't.
fn forma_from_str(s: &str) -> Option<Forma> {
    serde_json::from_value(serde_json::Value::String(s.to_string())).ok()
}

/// Per-node utilisation SKEW across the Ready nodes — the rebalance SIGNAL.
/// breathe OBSERVES this (and would emit a rebalance *hint*); the
/// descheduler/scheduler binds the actual pod moves (owns-vs-yields — breathe
/// decides node COUNT, never placement). On a single node `spread` is 0 (a lone
/// node cannot be imbalanced), so it is inert on rio and lights up multi-node.
#[derive(Debug, Clone, PartialEq)]
struct NodeImbalance {
    /// The hottest Ready node's `requested / allocatable`.
    max_util: f64,
    /// The coldest Ready node's `requested / allocatable`.
    min_util: f64,
    /// `max_util - min_util`. 0 ⇒ perfectly balanced (or a single node); a large
    /// spread is the cue a rebalance would relieve a hot node onto a cold one.
    spread: f64,
    /// The hottest node's name (the rebalance source candidate), if any.
    hottest: Option<String>,
}

/// PURE: given each Ready node's `(name, allocatable_milli, requested_milli)`,
/// compute the utilisation skew. A node with zero allocatable is skipped (it
/// can't host); an empty input or all-zero-alloc ⇒ a perfectly-balanced zero.
fn node_imbalance(nodes: &[(String, u64, u64)]) -> NodeImbalance {
    let mut max_util = 0.0f64;
    let mut min_util = f64::INFINITY;
    let mut hottest: Option<String> = None;
    let mut seen = 0u64;
    for (name, alloc, req) in nodes {
        if *alloc == 0 {
            continue;
        }
        seen += 1;
        let util = *req as f64 / *alloc as f64;
        if util > max_util {
            max_util = util;
            hottest = Some(name.clone());
        }
        if util < min_util {
            min_util = util;
        }
    }
    if seen == 0 {
        return NodeImbalance { max_util: 0.0, min_util: 0.0, spread: 0.0, hottest: None };
    }
    NodeImbalance { max_util, min_util, spread: max_util - min_util, hottest }
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

pub(crate) fn node_ready(n: &Node) -> bool {
    n.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
        .unwrap_or(false)
}

/// A kwok FAKE node (carries the kwok adoption annotation). The real-node
/// observer ([`KubeNodeProvedor`]) skips these so a `KwokProvedor` bed's fakes
/// never inflate the REAL node-count capacity signal — the two pools observe
/// disjoint fleets even though both list `nodes`.
fn is_kwok_fake(n: &Node) -> bool {
    n.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("kwok.x-k8s.io/node"))
        .is_some_and(|v| v == "fake")
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
                    if !node_ready(n) || is_kwok_fake(n) {
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
        // Per-Ready-node allocatable, keyed by name — feeds both the aggregate
        // sample and the per-node imbalance projection in one pass.
        let mut node_alloc: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        for n in &nodes.items {
            // Skip kwok FAKE nodes — the real-node pool observes the real fleet
            // only; a KwokProvedor bed's fakes belong to its own pool.
            if !node_ready(n) || is_kwok_fake(n) {
                continue;
            }
            let alloc = n
                .status
                .as_ref()
                .and_then(|s| s.allocatable.as_ref())
                .and_then(|a| a.get("cpu"))
                .map_or(0, |cpu| parse_cpu_milli(&cpu.0));
            node_alloc.insert(n.name_any(), alloc);
        }
        let node_count = node_alloc.len() as u64;
        let total_alloc_milli: u64 = node_alloc.values().sum();

        let pods = Api::<Pod>::all(self.client.clone())
            .list(&ListParams::default())
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        let mut demand_milli = 0u64;
        // Requested millicores PLACED on each Ready node — the skew numerator.
        let mut node_req: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        for p in &pods.items {
            let phase = p.status.as_ref().and_then(|s| s.phase.as_deref()).unwrap_or("");
            if phase != "Running" && phase != "Pending" {
                continue;
            }
            let mut pod_req = 0u64;
            if let Some(spec) = &p.spec {
                for c in &spec.containers {
                    if let Some(cpu) =
                        c.resources.as_ref().and_then(|r| r.requests.as_ref()).and_then(|m| m.get("cpu"))
                    {
                        pod_req += parse_cpu_milli(&cpu.0);
                    }
                }
                demand_milli += pod_req;
                // A placed pod (has nodeName, on a Ready node) loads that node.
                // Unplaced (Pending) pods count as demand but no node's skew.
                if let Some(node) = spec.node_name.as_ref() {
                    if node_alloc.contains_key(node) {
                        *node_req.entry(node.clone()).or_insert(0) += pod_req;
                    }
                }
            }
        }

        // Per-node imbalance — the rebalance signal (observe-only; the scheduler
        // binds). Emitted as cluster-level gauges, not a band input.
        let per_node_vec: Vec<(String, u64, u64)> = node_alloc
            .iter()
            .map(|(name, alloc)| (name.clone(), *alloc, node_req.get(name).copied().unwrap_or(0)))
            .collect();
        let imb = node_imbalance(&per_node_vec);
        gauge!("breathe_node_util_ratio_max").set(imb.max_util);
        gauge!("breathe_node_util_ratio_min").set(imb.min_util);
        gauge!("breathe_node_imbalance_spread").set(imb.spread);

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

// ============================================================================
// KwokProvedor — a LIVE node ACTUATOR against kwok FAKE nodes (the multi-node
// go-live bed). The in-cluster peer of breathe-provision::sim::SimProvedor:
// SimProvedor proves the loop in memory; KwokProvedor exercises it against the
// real apiserver, creating/observing/draining Node objects that kwok
// ("Kubernetes WithOut Kubelet") fakes the kubelet for — zero cloud cost,
// instant boot — so provision→ready→use→evict runs end-to-end on single-node rio.
// ============================================================================

/// The label every breathe-created fake node carries — value = the pool name.
/// It is BOTH the safety boundary (only a node bearing it for THIS pool is a
/// deletion target — breathe can never delete a real node) AND the observe scope.
const KWOK_MANAGED_LABEL: &str = "breathe.pleme.io/kwok-managed";
/// The scheduling label workload pods select on (`nodeSelector`) to land on a
/// pool's fake fleet — stamped on every fake node so the pool's demand is real.
const KWOK_POOL_LABEL: &str = "breathe.pleme.io/kwok-pool";
/// Each fake node's allocatable CPU (millicores). The node-equivalent unit.
const KWOK_NODE_CPU_MILLI: u64 = 4000;

/// SAFETY PREDICATE (load-bearing, pure, tested): is `node` a fake this pool
/// owns? True iff it carries `KWOK_MANAGED_LABEL == pool`. The single delete
/// path filters on this — a real node (no label) or another pool's fake is not
/// a deletion target, so breathe deleting a real node is unrepresentable here.
fn is_kwok_managed(node: &Node, pool: &str) -> bool {
    node.metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(KWOK_MANAGED_LABEL))
        .is_some_and(|v| v == pool)
}

/// PURE (tested): build a kwok fake `Node` for `pool` named `name` with
/// `cpu_milli` allocatable. Carries the kwok adoption annotation + NoSchedule
/// taint (so real pods never land), the managed + pool labels, and an explicit
/// capacity/allocatable so the scheduler sees room. kwok marks it `Ready`.
fn fake_node_object(pool: &str, name: &str, cpu_milli: u64) -> Node {
    let mut labels = std::collections::BTreeMap::new();
    labels.insert("type".to_string(), "kwok".to_string());
    labels.insert(KWOK_MANAGED_LABEL.to_string(), pool.to_string());
    labels.insert(KWOK_POOL_LABEL.to_string(), pool.to_string());
    labels.insert("kubernetes.io/hostname".to_string(), name.to_string());
    labels.insert("kubernetes.io/os".to_string(), "linux".to_string());

    let mut annotations = std::collections::BTreeMap::new();
    annotations.insert("kwok.x-k8s.io/node".to_string(), "fake".to_string());
    annotations.insert("node.alpha.kubernetes.io/ttl".to_string(), "0".to_string());

    let mut quantities = std::collections::BTreeMap::new();
    quantities.insert("cpu".to_string(), Quantity(format!("{cpu_milli}m")));
    quantities.insert("memory".to_string(), Quantity("16Gi".to_string()));
    quantities.insert("pods".to_string(), Quantity("110".to_string()));

    Node {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            labels: Some(labels),
            annotations: Some(annotations),
            ..Default::default()
        },
        spec: Some(NodeSpec {
            taints: Some(vec![Taint {
                key: "kwok.x-k8s.io/node".to_string(),
                value: Some("fake".to_string()),
                effect: "NoSchedule".to_string(),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: Some(NodeStatus {
            capacity: Some(quantities.clone()),
            allocatable: Some(quantities),
            ..Default::default()
        }),
    }
}

/// A LIVE actuator: creates/drains kwok fake nodes for one pool. `observe`
/// scopes to this pool's fake fleet (capacity = Ready fakes; demand = Σ cpu of
/// workload pods that `nodeSelector` this pool). All mutation is gated by the
/// safety predicate [`is_kwok_managed`].
pub struct KwokProvedor {
    client: Client,
    pool: String,
    /// Shadow gate: when true, `provision`/`deprovision` report what they WOULD
    /// do (`DryRun`) and mutate NOTHING — `observe` still reads (read-only is
    /// safe in shadow). So a shadow kwok pool never creates a Node.
    dry_run: bool,
}

impl KwokProvedor {
    pub fn new(client: Client, pool: String, dry_run: bool) -> Self {
        Self { client, pool, dry_run }
    }

    /// This pool's fake fleet (nodes carrying `KWOK_MANAGED_LABEL=<pool>`).
    async fn managed_nodes(&self) -> Result<Vec<Node>, ProviderError> {
        let lp = ListParams::default().labels(&format!("{KWOK_MANAGED_LABEL}={}", self.pool));
        Ok(Api::<Node>::all(self.client.clone())
            .list(&lp)
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?
            .items)
    }
}

#[async_trait::async_trait]
impl Provedor for KwokProvedor {
    async fn observe(&self) -> Result<FormaSample, ProviderError> {
        let nodes = self.managed_nodes().await?;
        let capacity = nodes.iter().filter(|n| node_ready(n)).count() as u64;

        // Demand = Σ cpu requests of Running/Pending pods that target THIS pool
        // via nodeSelector — the workload the fake fleet must absorb.
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
            let Some(spec) = &p.spec else { continue };
            let targets_pool = spec
                .node_selector
                .as_ref()
                .and_then(|sel| sel.get(KWOK_POOL_LABEL))
                .is_some_and(|v| v == &self.pool);
            if !targets_pool {
                continue;
            }
            for c in &spec.containers {
                if let Some(cpu) =
                    c.resources.as_ref().and_then(|r| r.requests.as_ref()).and_then(|m| m.get("cpu"))
                {
                    demand_milli += parse_cpu_milli(&cpu.0);
                }
            }
        }
        let used = demand_milli.div_ceil(KWOK_NODE_CPU_MILLI);
        Ok(FormaSample { used, capacity })
    }

    async fn provision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        if n == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        if self.dry_run {
            return Ok(ProvisionReceipt::DryRun { would: n as i64 });
        }
        let api = Api::<Node>::all(self.client.clone());
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let mut created = 0i64;
        for i in 0..n {
            let name = format!("breathe-kwok-{}-{stamp}-{i}", self.pool);
            let node = fake_node_object(&self.pool, &name, KWOK_NODE_CPU_MILLI);
            match api.create(&PostParams::default(), &node).await {
                Ok(_) => created += 1,
                Err(e) => warn!(pool = %self.pool, node = %name, error = %e, "kwok node create failed (non-fatal; retried next tick)"),
            }
        }
        if created == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        Ok(ProvisionReceipt::Applied { delta: created, plan_id: format!("kwok:provision:{stamp}") })
    }

    async fn deprovision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        if n == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        if self.dry_run {
            return Ok(ProvisionReceipt::DryRun { would: -(n as i64) });
        }
        let api = Api::<Node>::all(self.client.clone());
        let mut nodes = self.managed_nodes().await?;
        // Prefer draining the EMPTIEST nodes first would need per-node load; for
        // the fake fleet (no real workload migration) deletion order is by name.
        nodes.sort_by_key(kube::ResourceExt::name_any);
        let mut released = 0i64;
        for node in nodes.iter().take(n as usize) {
            // Defense-in-depth: re-verify the safety predicate before EVERY delete.
            // A node that isn't this pool's fake is unreachable as a target.
            if !is_kwok_managed(node, &self.pool) {
                continue;
            }
            let name = node.name_any();
            match api.delete(&name, &DeleteParams::default()).await {
                Ok(_) => released += 1,
                Err(e) => warn!(pool = %self.pool, node = %name, error = %e, "kwok node delete failed (non-fatal)"),
            }
        }
        if released == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        Ok(ProvisionReceipt::Applied { delta: -released, plan_id: format!("kwok:deprovision:{}", self.pool) })
    }
}

/// The per-pool executor selected by `spec.provider` + (when `KubeObserve`)
/// `spec.nodeProvisioningBackend`: typed dispatch (a sum type, no `dyn`) over
/// the observe-only [`KubeNodeProvedor`], the actuating [`KwokProvedor`], the
/// actuating [`KarpenterProvedor`], and the actuating [`EksNodegroupProvedor`],
/// so `reconcile_forma` is driven from ONE call site regardless of which
/// executor realizes the pool.
enum PoolProvedor {
    KubeObserve(KubeNodeProvedor),
    Kwok(KwokProvedor),
    Karpenter(KarpenterProvedor<KubeKarpenterEnvironment>),
    EksNodegroup(EksNodegroupProvedor<KubeEksNodegroupEnvironment>),
}

impl PoolProvedor {
    /// The per-unit allocatable (millicores) used to size a minted `NodeRef` for
    /// the admission gate. KubeObserve uses the live cluster mean; Kwok uses its
    /// fixed fake-node size; Karpenter/EksNodegroup use the mean over their
    /// OWNED Ready nodes.
    async fn per_node_alloc_milli(&self) -> u64 {
        match self {
            Self::KubeObserve(p) => p.per_node_alloc_milli().await,
            Self::Kwok(_) => KWOK_NODE_CPU_MILLI,
            Self::Karpenter(p) => p.per_node_alloc_milli().await,
            Self::EksNodegroup(p) => p.per_node_alloc_milli().await,
        }
    }
}

#[async_trait::async_trait]
impl Provedor for PoolProvedor {
    async fn observe(&self) -> Result<FormaSample, ProviderError> {
        match self {
            Self::KubeObserve(p) => p.observe().await,
            Self::Kwok(p) => p.observe().await,
            Self::Karpenter(p) => p.observe().await,
            Self::EksNodegroup(p) => p.observe().await,
        }
    }
    async fn provision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        match self {
            Self::KubeObserve(p) => p.provision(n).await,
            Self::Kwok(p) => p.provision(n).await,
            Self::Karpenter(p) => p.provision(n).await,
            Self::EksNodegroup(p) => p.provision(n).await,
        }
    }
    async fn deprovision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        match self {
            Self::KubeObserve(p) => p.deprovision(n).await,
            Self::Kwok(p) => p.deprovision(n).await,
            Self::Karpenter(p) => p.deprovision(n).await,
            Self::EksNodegroup(p) => p.deprovision(n).await,
        }
    }
}

// ============================================================================
// Node claiming — correnteza M0 (theory/CORRENTEZA.md): on a
// `Lane::StandaloneEc2Instance` pool's `Grew` tick, claim ONE Ready, unclaimed,
// non-kwok-fake node into the pool by tainting + labelling it. This is "a
// workload cannot be scheduled due to resource pressure" (the SAME `Grew`
// signal `reconcile_forma` already computes) turned into a NODE-level
// decision. Shadow-first: the caller's EFFECTIVE `dry_run`
// (`cr.spec.dry_run || !cr.spec.write_enabled`, computed once in
// `reconcile_cloud_pool`) is the ONLY gate — the exact same switch that keeps
// `KwokProvedor` from mutating in shadow, threaded one level deeper to the
// per-node claim. In shadow the candidate is picked and reported
// (`would_taint`) WITHOUT mutating; live, the SAME selection is patched for
// real (`tainted_node`) via the identical `Api<Node>` write pathway
// `KwokProvedor::provision` already uses.
// ============================================================================

/// The label a claimed node carries — value = the owning pool's name. It is
/// BOTH the scheduling boundary (paired with a `NoSchedule` taint of the same
/// key, so workloads stay off until they tolerate it) AND the claim-eligibility
/// predicate: a node already carrying this label — for ANY pool — is never
/// claimed a second time.
pub(crate) const CLAIM_POOL_LABEL: &str = "breathe.pleme.io/pool";
/// The lane the claimed node joined on (`Lane::as_str()`), stamped alongside
/// the pool label purely for observability (`kubectl get nodes -L`).
const CLAIM_LANE_LABEL: &str = "breathe.pleme.io/lane";

/// The outcome of one claim attempt this tick.
#[derive(Debug, Clone, PartialEq)]
enum ClaimOutcome {
    /// No Ready, unclaimed, non-kwok-fake node exists to claim.
    NoCandidate,
    /// Shadow: this node WOULD be tainted + labelled into the pool. Mutates nothing.
    WouldTaint { node: String },
    /// Live: this node WAS tainted + labelled into the pool.
    Tainted { node: String },
    /// Live: a candidate existed but the patch call failed — non-fatal, retried
    /// next tick (the same per-node error handling `KwokProvedor::provision`
    /// already uses; logged, never silently promoted to a status field).
    ClaimFailed { node: String },
}

/// PURE (tested): is `node` eligible for a FRESH claim by any pool — Ready,
/// not a kwok fake, and not already carrying [`CLAIM_POOL_LABEL`] (any pool's;
/// a node claimed by pool A is never re-claimed by pool B).
fn is_claim_candidate(node: &Node) -> bool {
    node_ready(node)
        && !is_kwok_fake(node)
        && !node.metadata.labels.as_ref().is_some_and(|l| l.contains_key(CLAIM_POOL_LABEL))
}

/// PURE (tested): pick the claim candidate from a node list — the first, by
/// name, of the eligible set ([`is_claim_candidate`]). Deterministic ordering
/// so two reconciles observing the same cluster state pick the SAME node
/// (no time-of-check/time-of-use race between the shadow report and the live
/// apply of the same tick).
fn pick_claim_candidate(nodes: &[Node]) -> Option<String> {
    let mut names: Vec<&str> = nodes
        .iter()
        .filter(|n| is_claim_candidate(n))
        .filter_map(|n| n.metadata.name.as_deref())
        .collect();
    names.sort_unstable();
    names.first().map(|s| (*s).to_string())
}

/// PURE (tested): merge ONE taint entry (`key`/`value`/`effect`) into an
/// existing taint list — replacing any prior entry for the SAME key (so a
/// re-apply is idempotent, never a duplicate entry) while preserving every
/// OTHER key's taint untouched — and return the result as typed JSON values
/// ready to embed in a k8s JSON merge patch's `spec.taints`. A k8s JSON merge
/// patch REPLACES the whole `spec.taints` list (it is not a per-element
/// merge), so a caller must ALWAYS pass every pre-existing taint through this
/// function — dropping one here would silently un-taint a node for anything
/// else it already carries. Shared by BOTH the membership-OPENING claim path
/// ([`claim_patch`], below) and the membership-CLOSING [`crate::origin_guard`]
/// reconcile, so the two mechanisms structurally cannot diverge on how a
/// merge-patch treats a node's existing taints.
pub(crate) fn upsert_taint(existing: &[Taint], key: &str, value: Option<&str>, effect: &str) -> Vec<serde_json::Value> {
    let mut taints: Vec<serde_json::Value> = existing
        .iter()
        .filter(|t| t.key != key)
        .map(|t| serde_json::json!({ "key": t.key, "value": t.value, "effect": t.effect }))
        .collect();
    taints.push(serde_json::json!({ "key": key, "value": value, "effect": effect }));
    taints
}

/// PURE (tested): the merge-patch body that claims a node into `pool` on
/// `lane` — labels [`CLAIM_POOL_LABEL`]/[`CLAIM_LANE_LABEL`], and the claim
/// taint upserted via [`upsert_taint`] alongside every taint the node already
/// carried. Idempotent: re-claiming a node already tainted for `pool` does not
/// duplicate the entry.
fn claim_patch(pool: &str, lane: &str, existing_taints: &[Taint]) -> serde_json::Value {
    let taints = upsert_taint(existing_taints, CLAIM_POOL_LABEL, Some(pool), "NoSchedule");
    serde_json::json!({
        "metadata": { "labels": { CLAIM_POOL_LABEL: pool, CLAIM_LANE_LABEL: lane } },
        "spec": { "taints": taints },
    })
}

/// Claim one node into `pool` on `lane` this tick. Lists Ready nodes, picks the
/// deterministic candidate ([`pick_claim_candidate`]), and — ONLY when
/// `!dry_run` — patches it for real. `dry_run` is the caller's EFFECTIVE gate;
/// this function adds no second switch.
async fn claim_unassigned_node_for_pool(
    client: &Client,
    pool: &str,
    lane: &str,
    dry_run: bool,
) -> ClaimOutcome {
    let nodes = match Api::<Node>::all(client.clone()).list(&ListParams::default()).await {
        Ok(l) => l.items,
        Err(e) => {
            warn!(pool, error = %e, "claim_unassigned_node_for_pool: node list failed (non-fatal; retried next tick)");
            return ClaimOutcome::NoCandidate;
        }
    };
    let Some(name) = pick_claim_candidate(&nodes) else {
        return ClaimOutcome::NoCandidate;
    };
    if dry_run {
        return ClaimOutcome::WouldTaint { node: name };
    }
    let existing_taints = nodes
        .iter()
        .find(|n| n.metadata.name.as_deref() == Some(name.as_str()))
        .and_then(|n| n.spec.as_ref())
        .and_then(|s| s.taints.clone())
        .unwrap_or_default();
    let patch = claim_patch(pool, lane, &existing_taints);
    let api = Api::<Node>::all(client.clone());
    match api.patch(&name, &PatchParams::default(), &Patch::Merge(&patch)).await {
        Ok(_) => {
            info!(pool, lane, node = %name, "claimed node into pool (tainted + labelled)");
            ClaimOutcome::Tainted { node: name }
        }
        Err(e) => {
            warn!(pool, node = %name, error = %e, "claim patch failed (non-fatal; retried next tick)");
            ClaimOutcome::ClaimFailed { node: name }
        }
    }
}

/// The metrics-label outcome for `breathe_node_claim_total` — the claim-tier
/// peer of `outcome_of`.
fn claim_outcome_label(c: &ClaimOutcome) -> &'static str {
    match c {
        ClaimOutcome::NoCandidate => "no_candidate",
        ClaimOutcome::WouldTaint { .. } => "would_taint",
        ClaimOutcome::Tainted { .. } => "tainted",
        ClaimOutcome::ClaimFailed { .. } => "claim_failed",
    }
}

/// PURE (tested): apply a claim outcome onto an already-built `CloudPoolStatus`
/// — `WouldTaint` → `would_taint`; `Tainted` → `tainted_node`; `NoCandidate`/
/// `ClaimFailed` leave both `None` (nothing happened worth reporting on the CR
/// — a failed claim is logged + metriced, never silently promoted to a status
/// field it doesn't own). Kept OUT of `cloud_pool_status` itself — the same
/// post-hoc-field-set convention `reconcile_cloud_pool` already uses for
/// `scheduler_scoring`/`predictive_active` — so that function's signature (and
/// its existing tests) stay stable.
fn apply_claim_to_status(status: &mut CloudPoolStatus, claim: &ClaimOutcome) {
    match claim {
        ClaimOutcome::WouldTaint { node } => status.would_taint = Some(node.clone()),
        ClaimOutcome::Tainted { node } => status.tainted_node = Some(node.clone()),
        ClaimOutcome::NoCandidate | ClaimOutcome::ClaimFailed { .. } => {}
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
        FormaTick::Grew { requested, admitted, rejected, provision_error, .. } => {
            s.phase = Some("Growing".into());
            s.would_provision = Some(*requested as i64);
            s.last_decision =
                Some(format!("would provision {requested} (admitted {admitted}, rejected {rejected})"));
            s.last_provision_error = provision_error.clone();
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

// ============================================================================
// Flap/stuck detection (task #51) — a pool can decide `Growing` tick after
// tick while `observedCapacity` never actually moves (the live Camelot-EKS
// bug this closes: `phase: Growing`, `lastDecision: "would provision 1
// (admitted 1, rejected 0)"` held for an extended period with nothing ever
// landing). Nothing previously distinguished "still climbing toward target"
// from "wedged — the decide loop keeps repeating the same no-op forever".
//
// Shape mirrors `pangea-operator`'s `ReactivePolicy::failure_escalation`
// (`FailureEscalation.maxConsecutiveFailures` → `onExhaustion`): count
// consecutive BAD ticks, escalate once a threshold is crossed, self-clear the
// instant a good tick lands. Here "bad" = phase stayed `Growing` AND
// `observedCapacity` did not increase since the prior tick.
// ============================================================================

/// After this many consecutive no-progress `Growing` ticks, flag the pool as
/// flapping/stuck. Mirrors `pangea-operator`'s `default_max_consecutive_failures`
/// (5) — same shape, same default magnitude, applied to a different signal.
pub const MAX_CONSECUTIVE_STUCK_TICKS: u32 = 5;

/// PURE (tested): compute this tick's `(consecutiveStuckTicks, flapDetected,
/// flapReason)` from the tick's own `(phase, observedCapacity)` plus the
/// PRIOR status's `(phase, observedCapacity, consecutiveStuckTicks)`.
///
/// Rules, in order:
///   1. `phase != "Growing"` this tick → fully reset (`0`, `false`, `None`).
///      Any non-Growing phase (Held/Shrinking/EnvelopeExhausted/Error) means
///      the band isn't mid-grow-attempt, so there is nothing to flap-detect.
///   2. `phase == "Growing"` but the PRIOR tick wasn't (a fresh entry into
///      Growing) → this tick has no comparable baseline yet; counter starts
///      at `0`, never flagged from tick one.
///   3. `phase == "Growing"` and the prior tick was too → compare
///      `observedCapacity`. Strictly increased ⇒ real progress, counter
///      resets to `0` (this tick becomes the new baseline). Flat or
///      decreased (or either sample is missing, which can't prove progress)
///      ⇒ counter = `prior + 1`.
///   4. `flapDetected = counter >= MAX_CONSECUTIVE_STUCK_TICKS`; `flapReason`
///      is `Some` iff `flapDetected`.
///
/// A pool making SLOW-BUT-REAL progress (capacity strictly increases every
/// tick, however far from target) always resets to `0` at step 3 and can
/// never be flagged — the false-positive case task #51 explicitly guards
/// against.
#[must_use]
pub fn flap_status(
    phase: Option<&str>,
    observed_capacity: Option<i64>,
    prior_phase: Option<&str>,
    prior_observed_capacity: Option<i64>,
    prior_consecutive_stuck_ticks: u32,
) -> (u32, bool, Option<String>) {
    if phase != Some("Growing") {
        return (0, false, None);
    }
    let was_growing_before = prior_phase == Some("Growing");
    let made_progress = match (observed_capacity, prior_observed_capacity) {
        (Some(cur), Some(prior)) => cur > prior,
        // Missing either sample can't PROVE progress happened — treat as no
        // progress rather than silently trusting an absent metric.
        _ => false,
    };
    let consecutive = if !was_growing_before || made_progress {
        0
    } else {
        prior_consecutive_stuck_ticks.saturating_add(1)
    };
    let flap_detected = consecutive >= MAX_CONSECUTIVE_STUCK_TICKS;
    let reason = flap_detected.then(|| {
        format!(
            "phase stuck at Growing for {consecutive} consecutive ticks with no observedCapacity increase (last observed {})",
            observed_capacity.map_or_else(|| "none".to_string(), |c| c.to_string())
        )
    });
    (consecutive, flap_detected, reason)
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
    // Effective shadow = per-pool dryRun OR pool master writeEnabled off — BOTH
    // gates, threaded through the SAME two-key `outorga::PromotionPolicy::decide`
    // every `Band` uses (`breathe_crd::legacy_effective_dry_run` — this CRD kind
    // has no `mode` field or Ready/Stale/Conflict status yet, so it rides the
    // pure two-state Shadow/Effect arm; see that function's doc for the full
    // migration note). This single value is the actuation switch: it is handed
    // to the provider so an actuating provider (Kwok) mutates NOTHING unless the
    // pool is live on both gates. The observe-only KubeObserve provider ignores
    // it (it is DryRun by construction — it can never mutate).
    let promotion = breathe_crd::legacy_effective_dry_run(cr.spec.dry_run, !cr.spec.write_enabled);
    let dry_run = promotion.is_shadow();
    if let Some(reason) = promotion.shadow_reason() {
        debug!(pool = %name, reason = ?reason, "BreatheCloudPool: held in shadow");
    }

    // Select the executor. Default KubeObserve can never actuate on its own —
    // it further dispatches on `nodeProvisioningBackend` (the REALIZATION
    // axis, orthogonal to `provider`'s SIGNAL-source axis): `K3sCustomAmi`
    // (default) stays the existing shadow + correnteza-claim path;
    // `EksKarpenter` actuates via real `karpenter.sh` NodeClaims once live
    // (`!dry_run`). Kwok (the fake-node signal source) ignores the backend
    // entirely and actuates only when `!dry_run`.
    let provedor = match cr.spec.provider {
        breathe_crd::ProviderKind::KubeObserve => match cr.spec.node_provisioning_backend {
            breathe_crd::NodeProvisioningBackend::K3sCustomAmi => {
                PoolProvedor::KubeObserve(KubeNodeProvedor::new(ctx.client.clone()))
            }
            breathe_crd::NodeProvisioningBackend::EksKarpenter => {
                let Some(node_pool_ref) = cr.spec.karpenter_node_pool_ref.clone() else {
                    warn!(pool = %name, "BreatheCloudPool: eksKarpenter backend requires karpenterNodePoolRef — skipping");
                    let st = CloudPoolStatus {
                        phase: Some("Error".into()),
                        last_decision: Some("eksKarpenter backend requires spec.karpenterNodePoolRef".into()),
                        last_seen_epoch: Some(now_secs()),
                        ..Default::default()
                    };
                    patch_status(&ctx.client, &name, &st).await;
                    return Ok(Action::requeue(ctx.requeue));
                };
                PoolProvedor::Karpenter(KarpenterProvedor::new(
                    KubeKarpenterEnvironment::new(ctx.client.clone()),
                    name.clone(),
                    node_pool_ref,
                    dry_run,
                ))
            }
            breathe_crd::NodeProvisioningBackend::EksManagedNodegroup => {
                let Some(ng_ref) = cr.spec.eks_managed_nodegroup_ref.clone() else {
                    warn!(pool = %name, "BreatheCloudPool: eksManagedNodegroup backend requires eksManagedNodegroupRef — skipping");
                    let st = CloudPoolStatus {
                        phase: Some("Error".into()),
                        last_decision: Some("eksManagedNodegroup backend requires spec.eksManagedNodegroupRef".into()),
                        last_seen_epoch: Some(now_secs()),
                        ..Default::default()
                    };
                    patch_status(&ctx.client, &name, &st).await;
                    return Ok(Action::requeue(ctx.requeue));
                };
                PoolProvedor::EksNodegroup(EksNodegroupProvedor::new(
                    KubeEksNodegroupEnvironment::new(ctx.client.clone(), ctx.eks_client.clone(), ctx.autoscaling_client.clone()),
                    name.clone(),
                    ng_ref.cluster_name,
                    ng_ref.nodegroup_name,
                    dry_run,
                    // The pool's OWN declared ceiling/floor (BreatheCloudPoolSpec) —
                    // the one static, human-declared boundary; AWS's real
                    // scalingConfig.maxSize/minSize now breathe toward these
                    // algorithmically instead of staying a second, independently
                    // static Terraform-authored value (see eks_nodegroup_provedor's
                    // module doc, "minSize/maxSize are ALGORITHMIC" section).
                    u32::try_from(cr.spec.ceiling).unwrap_or(u32::MAX),
                    u32::try_from(cr.spec.floor).unwrap_or(0),
                    cr.spec.grow_factor,
                    cr.spec.shrink_factor,
                ))
            }
        },
        breathe_crd::ProviderKind::Kwok => {
            PoolProvedor::Kwok(KwokProvedor::new(ctx.client.clone(), name.clone(), dry_run))
        }
    };

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
    // Instance scale-in protection (task #205 follow-up, the Camelot
    // runner-instability incident) — mark every currently-busy owned instance
    // protected, release any that's gone idle, BEFORE this tick's own
    // provision/deprovision decision runs, so a shrink `reconcile_forma`
    // decides on below can never select a currently-busy instance for
    // termination. EksNodegroup-only: the other backends either don't scale a
    // real ASG (KubeObserve/Kwok) or already delegate instance selection to
    // Karpenter's own PDB-aware NodeClaim drain (Karpenter). Non-fatal: a
    // failed sync degrades to "next tick's protection state is stale," never
    // blocks the reconcile.
    if let PoolProvedor::EksNodegroup(p) = &provedor {
        if let Err(e) = p.sync_instance_protection().await {
            warn!(pool = %name, error = %e, "BreatheCloudPool: sync_instance_protection failed (non-fatal; retried next tick)");
        }
    }

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
    // Real actuator failures (a non-ACTIVE nodegroup, an IAM error, a
    // throttled AWS call) used to be silently discarded here -- see
    // FormaTick::Grew::provision_error's doc comment for the full incident.
    // Log loudly at ERROR (not the routine INFO reconcile line below) so
    // `kubectl logs` surfaces it independently of anyone reading the CR
    // status.
    if let FormaTick::Grew { provision_error: Some(e), .. } = &tick {
        error!(pool = %name, error = %e, "BreatheCloudPool: provision() failed -- capacity will NOT grow this tick");
    }
    let mut status = cloud_pool_status(&tick, sample.as_ref().map(|s| s.used), sample.as_ref().map(|s| s.capacity), dry_run);
    // BU(fillPolicy): surface the scheduler scoring hint the pool's fillPolicy
    // implies — breathe SETS the posture; the scheduler (profile-configured) binds.
    status.scheduler_scoring = Some(cr.spec.fill_policy.scheduler_scoring().to_string());
    status.predictive_active = Some(cr.spec.predictive);

    // correnteza M0 — StandaloneEc2Instance lane: on a Grew tick (this pool
    // cannot cover current demand with its present node count — the trigger
    // grounding names as "a workload cannot be scheduled due to resource
    // pressure"), claim ONE unclaimed Ready node into this pool. Gated on the
    // SAME effective `dry_run` `reconcile_forma` was just run under — no new
    // safety switch, threaded one level deeper (see the module doc above).
    // Gated on `K3sCustomAmi` (the ONLY backend the correnteza taint-claim
    // mechanism belongs to) so a misconfigured `EksKarpenter` pool with a
    // stray `lane` set can never double-realize a `Grew` tick — once via a
    // real NodeClaim (above, through `provedor.provision`/`reconcile_forma`)
    // AND via a taint claim on an unrelated node.
    if cr.spec.node_provisioning_backend == breathe_crd::NodeProvisioningBackend::K3sCustomAmi {
        if let (FormaTick::Grew { .. }, Some(lane)) = (&tick, cr.spec.lane.as_deref()) {
            if lane == breathe_spread::Lane::StandaloneEc2Instance.as_str() {
                let claim = claim_unassigned_node_for_pool(&ctx.client, &name, lane, dry_run).await;
                counter!("breathe_node_claim_total", "pool" => name.clone(), "lane" => lane.to_string(), "outcome" => claim_outcome_label(&claim)).increment(1);
                apply_claim_to_status(&mut status, &claim);
            }
        }
    }

    if let Some(w) = status.would_provision {
        gauge!("breathe_forma_would_provision", "forma" => "node-on-demand", "pool" => name.clone()).set(w as f64);
    }

    // task #51 — flap/stuck detection: compare THIS tick's (phase, capacity)
    // against the PRIOR status (the last thing patch_status wrote for this
    // pool) via the pure `flap_status`. Read prior BEFORE `status` overwrites
    // anything on the CR — `cr.status` is the reflector's view from the last
    // successful patch, exactly the durable state a flap counter needs.
    let prior_status = cr.status.as_ref();
    let prior_flap_detected = prior_status.and_then(|s| s.flap_detected).unwrap_or(false);
    let (consecutive_stuck_ticks, flap_detected, flap_reason) = flap_status(
        status.phase.as_deref(),
        status.observed_capacity,
        prior_status.and_then(|s| s.phase.as_deref()),
        prior_status.and_then(|s| s.observed_capacity),
        prior_status.and_then(|s| s.consecutive_stuck_ticks).unwrap_or(0),
    );
    status.consecutive_stuck_ticks = Some(consecutive_stuck_ticks);
    status.flap_detected = Some(flap_detected);
    status.flap_reason = flap_reason.clone();
    counter!("breathe_forma_flap_detected_total", "pool" => name.clone()).increment(u64::from(flap_detected && !prior_flap_detected));
    gauge!("breathe_forma_consecutive_stuck_ticks", "pool" => name.clone()).set(f64::from(consecutive_stuck_ticks));

    // Transition-gated k8s Event — the SAME dedup shape `breathe_runtime`'s
    // `should_emit_event` already uses for band ticks (emit on CHANGE, not on
    // every tick a resting/stuck state holds): fire exactly once per stuck
    // EPISODE, the tick `flapDetected` flips false -> true. A pool wedged in
    // Growing for hours must not flood `kubectl get events` with one entry
    // per 60s requeue; it clears (and can re-fire) the tick capacity moves or
    // the phase leaves Growing, since `flap_status` resets the counter then.
    if flap_detected && !prior_flap_detected {
        let reason_text = flap_reason.clone().unwrap_or_default();
        warn!(pool = %name, reason = %reason_text, "BreatheCloudPool: flap/stuck detected — Growing with no observedCapacity progress");
        let recorder = Recorder::new(ctx.client.clone(), ctx.reporter.clone(), cr.object_ref(&()));
        let ev = Event {
            type_: EventType::Warning,
            reason: "StuckGrowing".to_string(),
            note: Some(reason_text),
            action: "Reconcile".to_string(),
            secondary: None,
        };
        if let Err(e) = recorder.publish(ev).await {
            warn!(pool = %name, error = %e, "flap-detection event publish failed (non-fatal)");
        }
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
    use super::{
        apply_claim_to_status, claim_outcome_label, claim_patch, cloud_pool_status,
        fake_node_object, flap_status, forma_from_str, is_claim_candidate, is_kwok_managed, node_imbalance,
        outcome_of, parse_cpu_milli, pick_claim_candidate, upsert_taint, ClaimOutcome, CLAIM_POOL_LABEL,
        KWOK_MANAGED_LABEL, MAX_CONSECUTIVE_STUCK_TICKS,
    };
    use breathe_crd::CloudPoolStatus;
    use breathe_provider::Forma;
    use breathe_provision::FormaTick;
    use k8s_openapi::api::core::v1::{Node, NodeCondition, NodeSpec, NodeStatus, Taint};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn n(name: &str, alloc: u64, req: u64) -> (String, u64, u64) {
        (name.to_string(), alloc, req)
    }

    /// A Ready node (optionally already claimed by `claimed_by`, optionally
    /// carrying `extra_taints`) — the fixture the claim-candidate tests share.
    fn ready_node(name: &str, claimed_by: Option<&str>, extra_taints: Vec<Taint>) -> Node {
        let mut labels = std::collections::BTreeMap::new();
        if let Some(pool) = claimed_by {
            labels.insert(CLAIM_POOL_LABEL.to_string(), pool.to_string());
        }
        Node {
            metadata: ObjectMeta { name: Some(name.to_string()), labels: Some(labels), ..Default::default() },
            spec: Some(NodeSpec { taints: (!extra_taints.is_empty()).then_some(extra_taints), ..Default::default() }),
            status: Some(NodeStatus {
                conditions: Some(vec![NodeCondition {
                    type_: "Ready".to_string(),
                    status: "True".to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        }
    }

    fn not_ready_node(name: &str) -> Node {
        Node {
            metadata: ObjectMeta { name: Some(name.to_string()), ..Default::default() },
            status: Some(NodeStatus {
                conditions: Some(vec![NodeCondition {
                    type_: "Ready".to_string(),
                    status: "False".to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn node_with_managed(label: Option<&str>) -> Node {
        let labels = label.map(|v| {
            let mut m = std::collections::BTreeMap::new();
            m.insert(KWOK_MANAGED_LABEL.to_string(), v.to_string());
            m
        });
        Node { metadata: ObjectMeta { labels, ..Default::default() }, ..Default::default() }
    }

    #[test]
    fn kwok_safety_predicate_matches_only_this_pools_fakes() {
        // The load-bearing safety boundary: breathe deletes ONLY its own fakes.
        assert!(!is_kwok_managed(&node_with_managed(None), "rio-kwok"), "a real node (no label) is never a target");
        assert!(!is_kwok_managed(&node_with_managed(Some("other-pool")), "rio-kwok"), "another pool's fake is not a target");
        assert!(is_kwok_managed(&node_with_managed(Some("rio-kwok")), "rio-kwok"), "this pool's own fake matches");
    }

    #[test]
    fn fake_node_is_tainted_labelled_and_sized() {
        let node = fake_node_object("rio-kwok", "breathe-kwok-rio-kwok-1", 4000);
        // managed + pool labels present and equal to the pool.
        assert!(is_kwok_managed(&node, "rio-kwok"));
        let labels = node.metadata.labels.as_ref().unwrap();
        assert_eq!(labels.get("breathe.pleme.io/kwok-pool").map(String::as_str), Some("rio-kwok"));
        // kwok adoption annotation so the fake kubelet takes it over.
        let ann = node.metadata.annotations.as_ref().unwrap();
        assert_eq!(ann.get("kwok.x-k8s.io/node").map(String::as_str), Some("fake"));
        // NoSchedule kwok taint so REAL pods never land on a fake node.
        let taint = &node.spec.as_ref().unwrap().taints.as_ref().unwrap()[0];
        assert_eq!(taint.key, "kwok.x-k8s.io/node");
        assert_eq!(taint.effect, "NoSchedule");
        // Explicit allocatable so the scheduler sees room.
        let alloc = node.status.as_ref().unwrap().allocatable.as_ref().unwrap();
        assert_eq!(alloc.get("cpu").map(|q| q.0.as_str()), Some("4000m"));
        // The real-node observer treats it as a fake (so it never counts it).
        assert!(super::is_kwok_fake(&node), "a fake node is detectable by the kwok annotation");
        assert!(!super::is_kwok_fake(&node_with_managed(Some("rio-kwok"))), "a plain node (no kwok annotation) is not fake");
    }

    #[test]
    fn imbalance_of_a_single_node_is_zero() {
        // A lone node cannot be imbalanced, however loaded it is.
        let i = node_imbalance(&[n("a", 4000, 3800)]);
        assert_eq!(i.spread, 0.0);
        assert_eq!(i.hottest.as_deref(), Some("a"));
    }

    #[test]
    fn imbalance_spreads_hot_vs_cold_and_names_the_hottest() {
        // a: 90% loaded, b: 10% — a 0.80 spread, a is the rebalance source.
        let i = node_imbalance(&[n("a", 1000, 900), n("b", 1000, 100)]);
        assert!((i.max_util - 0.90).abs() < 1e-9);
        assert!((i.min_util - 0.10).abs() < 1e-9);
        assert!((i.spread - 0.80).abs() < 1e-9);
        assert_eq!(i.hottest.as_deref(), Some("a"));
    }

    #[test]
    fn imbalance_skips_zero_allocatable_and_handles_empty() {
        // zero-alloc nodes can't host → skipped; empty input ⇒ balanced zero.
        assert_eq!(node_imbalance(&[]).spread, 0.0);
        let i = node_imbalance(&[n("ghost", 0, 0), n("real", 2000, 1000)]);
        assert!((i.max_util - 0.5).abs() < 1e-9);
        assert!((i.min_util - 0.5).abs() < 1e-9, "the ghost node must not pull min to 0");
        assert_eq!(i.spread, 0.0);
        assert_eq!(i.hottest.as_deref(), Some("real"));
    }

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

    /// Regression for the real bug this session's live Camelot-EKS trial
    /// caught: a `BreatheCloudPool` with `spec.forma: node-spot` reconciled
    /// to `phase: Error, lastDecision: "unknown forma \"node-spot\""` even
    /// though `Forma::NodeSpot` (and `Forma::as_str()`'s `"node-spot"`
    /// mapping) already existed — the old hand-written match in
    /// `forma_from_str` only ever recognized `"node-on-demand"`. Covers
    /// every `Forma` variant's `as_str()` round-trip so a future variant
    /// can't silently repeat the same gap.
    #[test]
    fn every_forma_as_str_round_trips_through_forma_from_str() {
        let all = [
            Forma::NodeOnDemand,
            Forma::NodeSpot,
            Forma::ProvisionedIops,
            Forma::ProvisionedThroughput,
            Forma::DynamoCapacity,
            Forma::Commitment,
            Forma::Accelerator,
            Forma::ServerlessSlot,
            Forma::ZoneCapacity,
            Forma::EdgePlacement,
            Forma::LbCapacity,
            Forma::EgressBandwidth,
            Forma::JitBuilder,
            Forma::LogIngestion,
        ];
        for forma in all {
            assert_eq!(
                forma_from_str(forma.as_str()),
                Some(forma),
                "forma_from_str({:?}) should round-trip",
                forma.as_str()
            );
        }
    }

    #[test]
    fn status_maps_grow_to_would_provision_and_records_observed() {
        let s = cloud_pool_status(
            &FormaTick::Grew {
                forma: Forma::NodeOnDemand,
                requested: 2,
                admitted: 2,
                rejected: 0,
                provision_error: None,
            },
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

    // ── correnteza M0: node-claim tests ─────────────────────────────────────

    #[test]
    fn claim_candidate_predicate_excludes_not_ready_kwok_fake_and_already_claimed() {
        assert!(is_claim_candidate(&ready_node("fresh", None, vec![])), "a bare Ready node is claimable");
        assert!(!is_claim_candidate(&not_ready_node("booting")), "NotReady is never a candidate");
        assert!(!is_claim_candidate(&fake_node_object("some-kwok-pool", "kwok-1", 4000)), "a kwok fake is never a real claim candidate");
        assert!(!is_claim_candidate(&ready_node("owned", Some("other-pool"), vec![])), "already labelled for ANY pool is never re-claimed");
    }

    #[test]
    fn pick_claim_candidate_is_deterministic_and_skips_ineligible() {
        let nodes = vec![
            ready_node("zzz-node", None, vec![]),
            not_ready_node("aaa-not-ready"),
            ready_node("bbb-claimed", Some("pool-a"), vec![]),
            ready_node("ccc-node", None, vec![]),
        ];
        // Two eligible: "zzz-node" and "ccc-node" — deterministic pick = lowest name.
        assert_eq!(pick_claim_candidate(&nodes).as_deref(), Some("ccc-node"));
    }

    #[test]
    fn pick_claim_candidate_none_when_nothing_eligible() {
        let nodes = vec![not_ready_node("a"), ready_node("b", Some("pool-a"), vec![])];
        assert_eq!(pick_claim_candidate(&nodes), None);
        assert_eq!(pick_claim_candidate(&[]), None);
    }

    // ── upsert_taint (the shared primitive claim_patch AND origin_guard use) ──

    #[test]
    fn upsert_taint_preserves_other_keys_and_upserts_its_own() {
        let existing = vec![Taint {
            key: "dedicated".to_string(),
            value: Some("gpu".to_string()),
            effect: "NoSchedule".to_string(),
            ..Default::default()
        }];
        let out = upsert_taint(&existing, "breathe.pleme.io/origin-reserved", None, "NoSchedule");
        assert_eq!(out.len(), 2, "the pre-existing OTHER-key taint is preserved");
        assert_eq!(out[0]["key"], "dedicated");
        assert_eq!(out[1]["key"], "breathe.pleme.io/origin-reserved");
        assert_eq!(out[1]["value"], serde_json::Value::Null);
        assert_eq!(out[1]["effect"], "NoSchedule");
    }

    #[test]
    fn upsert_taint_replaces_rather_than_duplicates_its_own_key() {
        let existing = vec![Taint {
            key: "breathe.pleme.io/origin-reserved".to_string(),
            value: Some("stale".to_string()),
            effect: "PreferNoSchedule".to_string(),
            ..Default::default()
        }];
        let out = upsert_taint(&existing, "breathe.pleme.io/origin-reserved", None, "NoSchedule");
        assert_eq!(out.len(), 1, "re-upserting the SAME key replaces, never duplicates");
        assert_eq!(out[0]["value"], serde_json::Value::Null, "the stale value is replaced");
        assert_eq!(out[0]["effect"], "NoSchedule", "the stale effect is replaced");
    }

    #[test]
    fn claim_patch_labels_and_taints_the_node_preserving_existing_taints() {
        let existing = vec![Taint {
            key: "dedicated".to_string(),
            value: Some("gpu".to_string()),
            effect: "NoSchedule".to_string(),
            ..Default::default()
        }];
        let patch = claim_patch("camelot-agents", "standalone-ec2-instance", &existing);
        let labels = &patch["metadata"]["labels"];
        assert_eq!(labels[CLAIM_POOL_LABEL], "camelot-agents");
        assert_eq!(labels["breathe.pleme.io/lane"], "standalone-ec2-instance");
        let taints = patch["spec"]["taints"].as_array().unwrap();
        assert_eq!(taints.len(), 2, "the pre-existing taint is PRESERVED, not dropped");
        assert_eq!(taints[0]["key"], "dedicated");
        assert_eq!(taints[1]["key"], CLAIM_POOL_LABEL);
        assert_eq!(taints[1]["value"], "camelot-agents");
        assert_eq!(taints[1]["effect"], "NoSchedule");
    }

    #[test]
    fn claim_patch_is_idempotent_on_a_re_claim_of_the_same_pool() {
        // A node already carrying THIS pool's taint (e.g. a retried tick after a
        // status-patch failure) must not accumulate a duplicate taint entry.
        let existing = vec![Taint {
            key: CLAIM_POOL_LABEL.to_string(),
            value: Some("camelot-agents".to_string()),
            effect: "NoSchedule".to_string(),
            ..Default::default()
        }];
        let patch = claim_patch("camelot-agents", "standalone-ec2-instance", &existing);
        let taints = patch["spec"]["taints"].as_array().unwrap();
        assert_eq!(taints.len(), 1, "re-claiming never duplicates the pool taint");
    }

    #[test]
    fn claim_outcome_maps_to_the_right_status_field_or_neither() {
        let mut s = CloudPoolStatus::default();
        apply_claim_to_status(&mut s, &ClaimOutcome::WouldTaint { node: "n1".into() });
        assert_eq!(s.would_taint.as_deref(), Some("n1"));
        assert_eq!(s.tainted_node, None);

        let mut s = CloudPoolStatus::default();
        apply_claim_to_status(&mut s, &ClaimOutcome::Tainted { node: "n2".into() });
        assert_eq!(s.tainted_node.as_deref(), Some("n2"));
        assert_eq!(s.would_taint, None);

        // NoCandidate / ClaimFailed report NOTHING on the CR — a non-event and a
        // logged-but-non-fatal failure are never silently promoted to a status
        // field that would misreport "this WOULD/DID happen".
        for c in [ClaimOutcome::NoCandidate, ClaimOutcome::ClaimFailed { node: "n3".into() }] {
            let mut s = CloudPoolStatus::default();
            apply_claim_to_status(&mut s, &c);
            assert_eq!(s.would_taint, None);
            assert_eq!(s.tainted_node, None);
        }
    }

    #[test]
    fn claim_outcome_labels_are_distinct() {
        let labels: std::collections::HashSet<&str> = [
            claim_outcome_label(&ClaimOutcome::NoCandidate),
            claim_outcome_label(&ClaimOutcome::WouldTaint { node: "x".into() }),
            claim_outcome_label(&ClaimOutcome::Tainted { node: "x".into() }),
            claim_outcome_label(&ClaimOutcome::ClaimFailed { node: "x".into() }),
        ]
        .into_iter()
        .collect();
        assert_eq!(labels.len(), 4, "every claim outcome gets a distinct metric label");
    }

    // ── task #51: flap/stuck detection ──────────────────────────────────────

    /// Drive `flap_status` across a scripted sequence of `(phase, capacity)`
    /// ticks, threading each tick's output as the next tick's "prior" — the
    /// same fold `reconcile_cloud_pool` performs one tick at a time against
    /// the CR's durable status. Returns the `(consecutive, flap_detected)`
    /// after EVERY tick, in order, so a test can assert on the whole episode.
    fn run_ticks(ticks: &[(&str, Option<i64>)]) -> Vec<(u32, bool)> {
        let mut prior_phase: Option<String> = None;
        let mut prior_capacity: Option<i64> = None;
        let mut prior_consecutive = 0u32;
        let mut out = Vec::with_capacity(ticks.len());
        for (phase, capacity) in ticks {
            let (consecutive, flap, _reason) = flap_status(
                Some(*phase),
                *capacity,
                prior_phase.as_deref(),
                prior_capacity,
                prior_consecutive,
            );
            out.push((consecutive, flap));
            prior_phase = Some((*phase).to_string());
            prior_capacity = *capacity;
            prior_consecutive = consecutive;
        }
        out
    }

    #[test]
    fn flap_never_fires_on_a_pool_that_resolves_within_the_threshold() {
        // Growing for 2 ticks (capacity flat — normal boot latency, well under
        // MAX_CONSECUTIVE_STUCK_TICKS) then resolves to Held. Must never flag.
        assert!(MAX_CONSECUTIVE_STUCK_TICKS > 2, "test assumes the default threshold leaves headroom");
        let outcomes = run_ticks(&[
            ("Growing", Some(3)), // fresh entry — no baseline yet
            ("Growing", Some(3)), // capacity hasn't landed yet, 1 no-progress tick
            ("Held", Some(4)),    // capacity landed, band settled
        ]);
        assert!(outcomes.iter().all(|(_, flap)| !flap), "a pool that resolves within the threshold is never flagged: {outcomes:?}");
        assert_eq!(outcomes.last().unwrap().0, 0, "leaving Growing resets the counter to 0");
    }

    #[test]
    fn flap_fires_when_growing_is_genuinely_wedged() {
        // Capacity pinned at 4 for MAX_CONSECUTIVE_STUCK_TICKS+1 consecutive
        // Growing ticks (the live camelot-eks-pool bug: "would provision 1"
        // forever, nothing ever lands) — must flag before the episode ends.
        let mut ticks: Vec<(&str, Option<i64>)> = vec![("Growing", Some(4))]; // baseline tick
        for _ in 0..(MAX_CONSECUTIVE_STUCK_TICKS as usize + 1) {
            ticks.push(("Growing", Some(4))); // capacity never moves
        }
        let outcomes = run_ticks(&ticks);
        assert!(outcomes.iter().any(|(_, flap)| *flap), "a genuinely wedged pool must be flagged: {outcomes:?}");
        // The FIRST tick to cross the threshold is exactly the
        // MAX_CONSECUTIVE_STUCK_TICKS-th no-progress tick, not earlier.
        let first_flap_index = outcomes.iter().position(|(_, flap)| *flap).unwrap();
        assert_eq!(outcomes[first_flap_index].0, MAX_CONSECUTIVE_STUCK_TICKS, "flags at exactly the threshold, not before");
        for (consecutive, flap) in &outcomes[..first_flap_index] {
            assert!(!flap, "no tick before the threshold crossing may be flagged (consecutive={consecutive})");
        }
    }

    #[test]
    fn flap_never_fires_on_slow_but_real_progress() {
        // Capacity increases EVERY tick (1 -> 2 -> 3 -> ... ) for well past
        // MAX_CONSECUTIVE_STUCK_TICKS ticks, never reaching some larger
        // target — real, if slow, progress must never be misread as stuck.
        let ticks: Vec<(&str, Option<i64>)> = (1..=(MAX_CONSECUTIVE_STUCK_TICKS as i64 * 3))
            .map(|c| ("Growing", Some(c)))
            .collect();
        let outcomes = run_ticks(&ticks);
        assert!(outcomes.iter().all(|(_, flap)| !flap), "monotonically increasing capacity must never be flagged: {outcomes:?}");
        assert!(outcomes.iter().all(|(consecutive, _)| *consecutive == 0), "every tick resets the counter when progress is real: {outcomes:?}");
    }

    #[test]
    fn flap_resets_the_instant_progress_resumes_mid_episode() {
        // Stuck for a few ticks, then ONE tick makes progress, then stuck
        // again — the counter must reset on the progress tick, not just stop
        // climbing, and the SECOND stuck run must climb from 0 again (not
        // append onto the count from before the reset).
        let outcomes = run_ticks(&[
            ("Growing", Some(2)), // baseline
            ("Growing", Some(2)), // stuck, consecutive=1
            ("Growing", Some(2)), // stuck, consecutive=2
            ("Growing", Some(3)), // progress! resets to 0
            ("Growing", Some(3)), // stuck again, consecutive=1 (NOT 3)
        ]);
        assert_eq!(outcomes[1].0, 1);
        assert_eq!(outcomes[2].0, 2);
        assert_eq!(outcomes[3].0, 0, "a progress tick resets the counter to 0");
        assert_eq!(outcomes[4].0, 1, "the next stuck run starts counting from 0, not from the pre-reset value");
    }

    #[test]
    fn flap_status_ignores_non_growing_phases_entirely() {
        // Held/Shrinking/EnvelopeExhausted/Error are never flap-detected —
        // the signal is specifically "decided Growing but nothing landed".
        for phase in ["Held", "Shrinking", "EnvelopeExhausted", "Error"] {
            let (consecutive, flap, reason) = flap_status(Some(phase), Some(5), Some(phase), Some(5), 99);
            assert_eq!(consecutive, 0, "non-Growing phase {phase} always resets to 0");
            assert!(!flap, "non-Growing phase {phase} is never flagged");
            assert_eq!(reason, None);
        }
    }

    #[test]
    fn flap_status_gives_a_fresh_entry_into_growing_one_free_baseline_tick() {
        // Coming FROM a non-Growing phase into Growing (or from no prior
        // status at all — a brand-new pool) must not immediately compare
        // capacity against an unrelated prior phase's value.
        let (consecutive, flap, _) = flap_status(Some("Growing"), Some(1), Some("Held"), Some(999), 3);
        assert_eq!(consecutive, 0, "a fresh entry into Growing always starts the counter at 0");
        assert!(!flap);

        let (consecutive, flap, _) = flap_status(Some("Growing"), Some(1), None, None, 0);
        assert_eq!(consecutive, 0, "a brand-new pool's first tick is never flagged");
        assert!(!flap);
    }

    #[test]
    fn flap_status_treats_a_missing_capacity_sample_as_no_progress_not_as_proof_of_progress() {
        // An unrepresentable/missing sample must never be silently read as
        // "capacity increased" — that would let a broken observer mask a
        // genuinely wedged pool forever.
        let (consecutive, _, _) = flap_status(Some("Growing"), None, Some("Growing"), Some(5), 2);
        assert_eq!(consecutive, 3, "missing current capacity counts as no-progress (prior_consecutive + 1)");

        let (consecutive, _, _) = flap_status(Some("Growing"), Some(5), Some("Growing"), None, 2);
        assert_eq!(consecutive, 3, "missing prior capacity counts as no-progress (prior_consecutive + 1)");
    }

    #[test]
    fn flap_reason_is_set_iff_flap_detected() {
        let (_, flap_no, reason_no) = flap_status(Some("Growing"), Some(1), Some("Growing"), Some(1), MAX_CONSECUTIVE_STUCK_TICKS - 2);
        assert!(!flap_no);
        assert_eq!(reason_no, None);

        let (_, flap_yes, reason_yes) = flap_status(Some("Growing"), Some(1), Some("Growing"), Some(1), MAX_CONSECUTIVE_STUCK_TICKS);
        assert!(flap_yes);
        let reason = reason_yes.expect("flapDetected=true must carry a reason");
        assert!(reason.contains("Growing"));
        assert!(reason.contains("observedCapacity"));
    }
}
