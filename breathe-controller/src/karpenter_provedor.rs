//! `KarpenterProvedor` — the `EksKarpenter`-backend realization
//! ([`breathe_crd::NodeProvisioningBackend::EksKarpenter`]): reads a real
//! `karpenter.sh/v1 NodePool` and mints/deletes real `karpenter.sh/v1
//! NodeClaim` objects. Real Karpenter's own `nodeclaim/lifecycle` controller
//! (out of this process, already running in the cluster) does the actual
//! cloud launch/drain — breathe never implements `cloudprovider.CloudProvider`,
//! never ships a `NodeClass`, never runs Karpenter's own controller image.
//!
//! # The side-effecting boundary — [`KarpenterEnvironment`]
//!
//! Every I/O this backend performs (list/read Nodes+Pods+NodePool, create/
//! list/delete NodeClaim) is abstracted behind [`KarpenterEnvironment`] — the
//! same `Environment`-trait discipline `breathe-lifecycle`'s
//! [`DriftEnvironment`](../../../crates/breathe-lifecycle/src/drift.rs) uses
//! (the org's TYPED-SPEC + INTERPRETER TRIPLET convention applied to a
//! provisioning backend): [`KubeKarpenterEnvironment`] is the real impl over
//! `kube::Client`; unit tests below drive [`KarpenterProvedor`] against an
//! in-memory mock — no live EKS/Karpenter cluster required to prove the
//! translation logic (Grew-shaped `provision(n)` → N typed `NodeClaim`
//! objects, or a `DryRun` report of the same) is correct.
//!
//! # What is deliberately NOT built here (the ceiling)
//!
//! No `cloudprovider.CloudProvider` Go-interface equivalent, no
//! `InstanceType` catalog, no drift detection, no repair policies, no
//! `NodeClass` CRD (`nodeClassRef` is opaque JSON copied verbatim from the
//! referenced `NodePool`, never validated or re-modeled), no `NodePool`
//! writes ever (it stays a Helm/GitOps-authored precondition — breathe only
//! READS it), no vendored Karpenter CRD schema (raw `serde_json::Value`
//! passthrough for the ~2 object shapes actually touched), no per-`NodeClaim`
//! status-condition tracking (`observe()` reads real `Node` objects —
//! already-registered capacity — not `NodeClaim.status`, the same shape
//! [`crate::node_forma::KubeNodeProvedor`] already uses).

use std::collections::{BTreeMap, HashSet};

use async_trait::async_trait;
use breathe_provider::{FormaSample, Provedor, ProviderError, ProvisionReceipt};
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    api::{Api, DeleteParams, ListParams, PostParams},
    core::{ApiResource, DynamicObject, GroupVersionKind},
    Client, ResourceExt,
};
use tracing::warn;

use crate::node_forma::{is_busy_runner_pod, node_ready, parse_cpu_milli, CLAIM_POOL_LABEL};

const KARPENTER_GROUP: &str = "karpenter.sh";
const KARPENTER_VERSION: &str = "v1";
/// The label real Karpenter stamps on every node it provisions from a given
/// NodePool — the read-side scoping key for [`KarpenterEnvironment::observe_owned_nodes`].
/// Already a KNOWN label in this codebase (`main.rs` reads
/// `karpenter.sh/capacity-type` off real nodes today for environment
/// detection) — this is its `nodepool` sibling.
const KARPENTER_NODEPOOL_LABEL: &str = "karpenter.sh/nodepool";
/// The label a minted NodeClaim carries recording which NodePool it was built
/// from — observability only (`kubectl get nodeclaims -L`); ownership is
/// [`CLAIM_POOL_LABEL`], same as every other breathe-claimed object.
const KARPENTER_NODE_POOL_REF_LABEL: &str = "breathe.pleme.io/karpenter-node-pool";

fn nodepool_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(&GroupVersionKind::gvk(KARPENTER_GROUP, KARPENTER_VERSION, "NodePool"), "nodepools")
}
fn nodeclaim_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(&GroupVersionKind::gvk(KARPENTER_GROUP, KARPENTER_VERSION, "NodeClaim"), "nodeclaims")
}

/// A real Node this backend's referenced NodePool owns, as observed this
/// tick — just enough to feed the band law (count + mean allocatable).
#[derive(Debug, Clone, PartialEq)]
pub struct ObservedNode {
    pub name: String,
    pub allocatable_cpu_milli: u64,
}

/// A NodeClaim this pool may own, as observed this tick — just enough to
/// drive the deprovision defense-in-depth re-check + a deterministic delete
/// order, without forcing every environment (including the mock) to build a
/// full [`DynamicObject`] for a read-only listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeClaimRef {
    pub name: String,
    /// The [`CLAIM_POOL_LABEL`] value this claim carries, if any.
    pub pool_label: Option<String>,
    /// The Kubernetes Node name this claim's `.status.nodeName` reports it
    /// has registered as, if any (2026-07-25, task #75 — the busy-node
    /// protection join key). `None` for a claim whose node hasn't
    /// registered yet (freshly minted, still launching) — such a claim
    /// carries no running pods by definition, so it is never treated as
    /// busy regardless of this field's absence.
    pub node_name: Option<String>,
}

/// The referenced NodePool's `.spec.template`, split into the opaque
/// NodeClaimSpec (`spec`, copied verbatim into a minted NodeClaim's `.spec`
/// — `NodeClaimSpec`'s field set is structurally a subset of
/// `NodePool.spec.template.spec`, so no field-by-field re-modeling is
/// needed) and the template's own `.metadata` (`labels`/`annotations` an
/// operator authored on the NodePool for every node/claim it launches —
/// copied onto the minted NodeClaim alongside breathe's own ownership
/// labels, mirroring what real Karpenter's own NodePool→NodeClaim
/// controller does when IT constructs a NodeClaim). Both label/annotation
/// maps default empty — most NodePool templates author zero extra
/// metadata, and a nil `.metadata` block is the common case, not an error.
#[derive(Debug, Clone, Default)]
pub struct NodePoolTemplate {
    pub spec: serde_json::Value,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

/// The side-effecting boundary this backend performs ALL its I/O through —
/// the mockable seam. A real implementation ([`KubeKarpenterEnvironment`])
/// wraps a `kube::Client`; tests wrap an in-memory fixture. Every method maps
/// 1:1 onto one real API call, so a test proves the TRANSLATION logic in
/// [`KarpenterProvedor`] (Grew-shaped `n` → N typed NodeClaims; dry-run →
/// nothing mutated) without touching a live cluster.
#[async_trait]
pub trait KarpenterEnvironment: Send + Sync {
    /// Ready nodes carrying `karpenter.sh/nodepool == node_pool_ref`, each
    /// with its allocatable CPU (millicores) — the capacity signal.
    async fn observe_owned_nodes(&self, node_pool_ref: &str) -> Result<Vec<ObservedNode>, ProviderError>;
    /// Requested millicores of Running+Pending pods, cluster-wide.
    ///
    /// v1 simplification, flagged not silently claimed: this is the SAME
    /// cluster-wide aggregate [`crate::node_forma::KubeNodeProvedor`]
    /// computes — UNSCOPED to this specific NodePool. Scoping to pods that
    /// tolerate this pool's taint (the
    /// [`crate::node_forma::KwokProvedor::observe`] pattern) is a named
    /// follow-up once a real multi-NodePool `EksKarpenter` fleet exists.
    async fn observe_pod_demand_milli(&self) -> Result<u64, ProviderError>;
    /// The referenced NodePool's `.spec.template` — see [`NodePoolTemplate`].
    async fn get_nodepool_template(&self, node_pool_ref: &str) -> Result<NodePoolTemplate, ProviderError>;
    /// This pool's own NodeClaims (labeled `CLAIM_POOL_LABEL=pool`).
    async fn list_managed_nodeclaims(&self, pool: &str) -> Result<Vec<NodeClaimRef>, ProviderError>;
    /// Create one NodeClaim object (already built by [`build_nodeclaim`]).
    async fn create_nodeclaim(&self, obj: DynamicObject) -> Result<(), ProviderError>;
    /// Delete one NodeClaim by name.
    async fn delete_nodeclaim(&self, name: &str) -> Result<(), ProviderError>;
    /// Node names currently running >=1 busy ARC ephemeral-runner Pod,
    /// cluster-wide (2026-07-25, task #75). Mirrors
    /// [`crate::eks_nodegroup_provedor::EksNodegroupEnvironment::observe_busy_nodes`]'s
    /// signal — same predicate ([`is_busy_runner_pod`]), same "cluster-wide,
    /// then intersect against MY claims' node names" shape — but this
    /// backend has no ASG-level `SetInstanceProtection` equivalent to lean
    /// on, so [`KarpenterProvedor::deprovision`] uses this directly to
    /// refuse to SELECT a busy claim in the first place, rather than
    /// protecting an instance out-of-band after selection.
    async fn observe_busy_node_names(&self) -> Result<std::collections::HashSet<String>, ProviderError>;
}

/// PURE: is `node` owned by the referenced NodePool (carries
/// `karpenter.sh/nodepool == node_pool_ref`)? The scoping predicate
/// [`KubeKarpenterEnvironment::observe_owned_nodes`] filters on.
fn owned_by_nodepool(node: &Node, node_pool_ref: &str) -> bool {
    node.metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(KARPENTER_NODEPOOL_LABEL))
        .is_some_and(|v| v == node_pool_ref)
}

/// SAFETY PREDICATE (load-bearing, pure, tested): is `claim` a NodeClaim THIS
/// pool owns? The same defense-in-depth shape
/// [`crate::node_forma::is_kwok_managed`] uses — re-checked on EVERY delete,
/// never trusted from the (already label-scoped) list call alone.
fn is_karpenter_managed_ref(claim: &NodeClaimRef, pool: &str) -> bool {
    claim.pool_label.as_deref() == Some(pool)
}

/// SAFETY PREDICATE (load-bearing, pure, tested; 2026-07-25, task #75): is
/// `claim` currently backing a node with >=1 busy ARC runner pod on it? A
/// claim with `node_name: None` (not yet registered as a real Node) is
/// never busy -- it carries no pods by definition. [`KarpenterProvedor::deprovision`]
/// filters this OUT before selecting which `n` claims to delete, the only
/// correct place to enforce it since a direct NodeClaim delete has no
/// downstream do-not-disrupt/PDB check the way a Karpenter-initiated
/// voluntary disruption would.
fn claim_is_busy(claim: &NodeClaimRef, busy_node_names: &HashSet<String>) -> bool {
    claim.node_name.as_deref().is_some_and(|n| busy_node_names.contains(n))
}

/// PURE (tested): build ONE NodeClaim [`DynamicObject`] for `pool`, copying
/// `template.spec` (a NodePool's `.spec.template.spec`, opaque JSON) verbatim
/// into `.spec`, plus `template`'s own authored `labels`/`annotations`.
/// `generateName`, never a fixed name — Karpenter-shaped objects are minted
/// many at a time; letting the apiserver assign the suffix means two
/// concurrent `provision` calls can never collide on a name.
///
/// # The `karpenter.sh/nodepool` ownership label — synthesized, not copied
///
/// Real Karpenter's own NodePool→NodeClaim controller stamps
/// `karpenter.sh/nodepool: <NodePool.Name>` onto every NodeClaim it
/// constructs (`lo.Assign(nodePool.Spec.Template.Labels, map[string]string{
/// v1.NodePoolLabelKey: nodePool.Name})` — template labels first, the
/// injected ownership label layered on top so it always wins a key
/// collision), and that label then propagates onto the launched `Node`.
/// Here breathe IS its own minting authority — it never delegates to that
/// upstream controller — so this function must synthesize the same label
/// itself, from `node_pool_ref` (the NodePool's own object name), applying
/// it AFTER the template's own labels for the identical never-shadowed
/// guarantee. This is the exact key+value
/// [`owned_by_nodepool`]/[`KubeKarpenterEnvironment::observe_owned_nodes`]
/// filter real `Node` objects on — omitting it makes breathe's own
/// `observe()` see zero owned nodes forever, even after a successful mint.
fn build_nodeclaim(pool: &str, node_pool_ref: &str, template: &NodePoolTemplate) -> DynamicObject {
    let mut labels = template.labels.clone();
    labels.insert(CLAIM_POOL_LABEL.to_string(), pool.to_string());
    labels.insert(KARPENTER_NODE_POOL_REF_LABEL.to_string(), node_pool_ref.to_string());
    labels.insert(KARPENTER_NODEPOOL_LABEL.to_string(), node_pool_ref.to_string());

    let mut obj = DynamicObject::new("", &nodeclaim_resource()).data(serde_json::json!({ "spec": template.spec }));
    obj.metadata.name = None;
    obj.metadata.generate_name = Some(format!("breathe-{pool}-"));
    obj.metadata.labels = Some(labels);
    if !template.annotations.is_empty() {
        obj.metadata.annotations = Some(template.annotations.clone());
    }
    obj
}

/// PURE (tested): parse a [`NodePoolTemplate`] out of a NodePool's raw
/// `.data` JSON — isolated from [`KubeKarpenterEnvironment::get_nodepool_template`]'s
/// `kube::Api::get` call so the nil/missing-`.metadata` case (the common
/// case: most NodePool templates author zero extra labels/annotations) is
/// provably non-panicking with zero client mocking required.
fn parse_nodepool_template(node_pool_ref: &str, node_pool_data: &serde_json::Value) -> Result<NodePoolTemplate, ProviderError> {
    let template = node_pool_data.get("spec").and_then(|s| s.get("template"));
    let spec = template
        .and_then(|t| t.get("spec"))
        .cloned()
        .ok_or_else(|| ProviderError::ApiPermanent(format!("NodePool {node_pool_ref} has no spec.template.spec")))?;
    let metadata = template.and_then(|t| t.get("metadata"));
    let labels = metadata
        .and_then(|m| m.get("labels"))
        .and_then(|l| serde_json::from_value::<BTreeMap<String, String>>(l.clone()).ok())
        .unwrap_or_default();
    let annotations = metadata
        .and_then(|m| m.get("annotations"))
        .and_then(|a| serde_json::from_value::<BTreeMap<String, String>>(a.clone()).ok())
        .unwrap_or_default();
    Ok(NodePoolTemplate { spec, labels, annotations })
}

/// A LIVE actuator against a REAL Karpenter install, generic over its
/// [`KarpenterEnvironment`] (production: [`KubeKarpenterEnvironment`]; tests:
/// an in-memory mock). `observe` scopes to Ready nodes carrying
/// `karpenter.sh/nodepool=<node_pool_ref>`; `provision`/`deprovision`
/// mint/delete this pool's own NodeClaims. `dry_run` gates mutation only —
/// `observe` always reads real state (same convention as
/// [`crate::node_forma::KwokProvedor`]).
pub struct KarpenterProvedor<E: KarpenterEnvironment> {
    env: E,
    pool: String,
    node_pool_ref: String,
    /// **The authorization verdict this provedor actuates under** — the typed
    /// value `legacy_two_state_gate` resolved once for the pool, not a hand-set
    /// bool. `Shadow` ⇒ `provision`/`deprovision` compute and report the
    /// `would` and mutate NOTHING; `Live` ⇒ they act.
    ///
    /// **Why a verdict and not a `bool` (changed 2026-07-26).** The bool was
    /// forgeable: any caller could hand-type `false` and authorize real cloud
    /// node provisioning, which is `76924b0`'s defect class on the
    /// highest-blast-radius path in the codebase. `EffectiveGate::Live` carries
    /// a `LiveWitness`, whose only producer is `gate::resolve_gate`, so a live
    /// verdict can no longer be spelled by hand.
    ///
    /// **Tier, not rounded up: only-mitigated.** This raises the bar on
    /// *obtaining* an authorization to actuate (truly-unrepresentable to forge
    /// one) but NOT on *honouring* it: the branches below are still runtime
    /// `if`s, and an implementation that ignored this field would still write.
    /// It is deliberately weaker than `Cluster::apply`, which takes the witness
    /// itself and so cannot be reached at all from a shadow verdict — and it is
    /// weaker for a real structural reason: `provision` must ALSO run in shadow
    /// (it reads live cloud state to compute the clamped `would`), so it cannot
    /// simply demand a witness the way a write-only door can. The destination
    /// that closes the gap is a split — a read-only `plan_provision(n) -> would`
    /// plus a `provision(&LiveWitness, n)` that only ever writes — which is a
    /// real refactor of five impls and is NOT done here.
    gate: breathe_provider::EffectiveGate,
}

impl<E: KarpenterEnvironment> KarpenterProvedor<E> {
    pub fn new(env: E, pool: String, node_pool_ref: String, gate: breathe_provider::EffectiveGate) -> Self {
        Self { env, pool, node_pool_ref, gate }
    }

    /// The per-unit allocatable (millicores) used to size a minted `NodeRef`
    /// for the admission gate — the mean over this NodePool's OWNED Ready
    /// nodes. Mirrors [`crate::node_forma::KubeNodeProvedor::per_node_alloc_milli`]'s
    /// shape, scoped to this backend's node set.
    pub(crate) async fn per_node_alloc_milli(&self) -> u64 {
        match self.env.observe_owned_nodes(&self.node_pool_ref).await {
            Ok(nodes) if !nodes.is_empty() => {
                let count = nodes.len() as u64;
                let total: u64 = nodes.iter().map(|n| n.allocatable_cpu_milli).sum();
                (total / count).max(1)
            }
            _ => 1,
        }
    }
}

#[async_trait]
impl<E: KarpenterEnvironment> Provedor for KarpenterProvedor<E> {
    async fn observe(&self) -> Result<FormaSample, ProviderError> {
        let nodes = self.env.observe_owned_nodes(&self.node_pool_ref).await?;
        let capacity = nodes.len() as u64;
        let total_alloc: u64 = nodes.iter().map(|n| n.allocatable_cpu_milli).sum();
        let demand_milli = self.env.observe_pod_demand_milli().await?;
        // ── ZERO IS A REAL READING HERE. Do not restore a `.max(1)`. ────────
        //
        // `used` and `capacity` both carried a `.max(1)` floor, which made an
        // idle empty pool report used=1/capacity=1 — utilisation 1.0, i.e.
        // permanently `Growing`. Scale-to-zero was not merely unreached, it was
        // arithmetically unreachable, and the shrink edge IS the packing
        // mechanism. Measured on camelot: both pools pegged Growing with
        // flapDetected=true.
        //
        // Nothing downstream needs the lie. `BreatheCloudPool::band_config()`
        // already sets `MetricMissingPolicy::Trust`, whose own doc names this
        // case — "the dimension treats 0 as a real value (node-count
        // scaled-to-zero, etc.)" — so a 0 here runs the band law rather than
        // tripping the split-brain gate that protects pod-memory dimensions.
        // `KwokProvedor::observe` has always omitted the floor, so the
        // zero-capacity path is already exercised in-tree.
        let used = if capacity > 0 {
            let per_node = (total_alloc / capacity).max(1);
            demand_milli.div_ceil(per_node)
        } else {
            // No owned nodes, so no measured per-node size to divide by. The
            // old code substituted per_node = 1, which turned millicores into a
            // node count and asked for thousands of nodes. Demand with no
            // capacity means "at least one node"; no demand means zero.
            u64::from(demand_milli > 0)
        };
        Ok(FormaSample { used, capacity })
    }

    async fn provision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        if n == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        if self.gate.is_shadow() {
            return Ok(ProvisionReceipt::DryRun { would: n as i64 });
        }
        let template = self.env.get_nodepool_template(&self.node_pool_ref).await?;
        let mut created = 0i64;
        for _ in 0..n {
            let obj = build_nodeclaim(&self.pool, &self.node_pool_ref, &template);
            match self.env.create_nodeclaim(obj).await {
                Ok(()) => created += 1,
                Err(e) => warn!(pool = %self.pool, error = %e, "NodeClaim create failed (non-fatal; retried next tick)"),
            }
        }
        if created == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        Ok(ProvisionReceipt::Applied { delta: created, plan_id: format!("karpenter:provision:{}", self.pool) })
    }

    async fn deprovision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        if n == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        if self.gate.is_shadow() {
            return Ok(ProvisionReceipt::DryRun { would: -(n as i64) });
        }
        let mut claims = self.env.list_managed_nodeclaims(&self.pool).await?;
        claims.sort_by(|a, b| a.name.cmp(&b.name));
        // Busy-node protection (2026-07-25, task #75): a direct NodeClaim
        // delete bypasses Karpenter's own Disruption Controller (the
        // component that would otherwise consult `do-not-disrupt`/PDBs)
        // entirely, going straight to the Termination Controller -- so
        // nothing upstream of this call protects a node mid-CI-job unless
        // THIS selection loop refuses to pick it. Mirrors
        // `EksNodegroupProvedor::sync_instance_protection`'s already-proven
        // signal (same `is_busy_runner_pod` predicate, promoted to
        // `node_forma` so both backends share one implementation) -- that
        // backend protects the instance out-of-band via
        // `SetInstanceProtection`; Karpenter has no equivalent API, so the
        // only correct mechanism here is "never select a busy claim in the
        // first place." Best-effort: a transient list failure degrades to
        // an empty busy set (fail toward "protect nothing," matching
        // `create_nodeclaim`'s own "log + continue" tolerance elsewhere in
        // this file) rather than blocking every shrink tick on it.
        let busy_nodes = self.env.observe_busy_node_names().await.unwrap_or_else(|e| {
            warn!(pool = %self.pool, error = %e, "observe_busy_node_names failed; treating as no busy nodes this tick (non-fatal)");
            HashSet::new()
        });
        let mut released = 0i64;
        for claim in claims.iter().filter(|c| !claim_is_busy(c, &busy_nodes)).take(n as usize) {
            // Defense-in-depth: re-verify the safety predicate before EVERY
            // delete. A claim that isn't this pool's is unreachable as a target.
            if !is_karpenter_managed_ref(claim, &self.pool) {
                continue;
            }
            match self.env.delete_nodeclaim(&claim.name).await {
                Ok(()) => released += 1,
                Err(e) => warn!(pool = %self.pool, claim = %claim.name, error = %e, "NodeClaim delete failed (non-fatal)"),
            }
        }
        if released == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        Ok(ProvisionReceipt::Applied { delta: -released, plan_id: format!("karpenter:deprovision:{}", self.pool) })
    }
}

/// The real [`KarpenterEnvironment`] — every method is exactly one
/// `kube::Api` call against the live apiserver.
pub struct KubeKarpenterEnvironment {
    client: Client,
}

impl KubeKarpenterEnvironment {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl KarpenterEnvironment for KubeKarpenterEnvironment {
    async fn observe_owned_nodes(&self, node_pool_ref: &str) -> Result<Vec<ObservedNode>, ProviderError> {
        let nodes = Api::<Node>::all(self.client.clone())
            .list(&ListParams::default())
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        Ok(nodes
            .items
            .iter()
            .filter(|n| node_ready(n) && owned_by_nodepool(n, node_pool_ref))
            .map(|n| ObservedNode {
                name: n.name_any(),
                allocatable_cpu_milli: n
                    .status
                    .as_ref()
                    .and_then(|s| s.allocatable.as_ref())
                    .and_then(|a| a.get("cpu"))
                    .map_or(0, |cpu| parse_cpu_milli(&cpu.0)),
            })
            .collect())
    }

    async fn observe_pod_demand_milli(&self) -> Result<u64, ProviderError> {
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
                    if let Some(cpu) = c.resources.as_ref().and_then(|r| r.requests.as_ref()).and_then(|m| m.get("cpu")) {
                        demand_milli += parse_cpu_milli(&cpu.0);
                    }
                }
            }
        }
        Ok(demand_milli)
    }

    async fn get_nodepool_template(&self, node_pool_ref: &str) -> Result<NodePoolTemplate, ProviderError> {
        let np_api: Api<DynamicObject> = Api::all_with(self.client.clone(), &nodepool_resource());
        let node_pool = np_api.get(node_pool_ref).await.map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        parse_nodepool_template(node_pool_ref, &node_pool.data)
    }

    async fn list_managed_nodeclaims(&self, pool: &str) -> Result<Vec<NodeClaimRef>, ProviderError> {
        let api: Api<DynamicObject> = Api::all_with(self.client.clone(), &nodeclaim_resource());
        let lp = ListParams::default().labels(&format!("{CLAIM_POOL_LABEL}={pool}"));
        let list = api.list(&lp).await.map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        Ok(list
            .items
            .iter()
            .map(|o| NodeClaimRef {
                name: o.name_any(),
                pool_label: o.metadata.labels.as_ref().and_then(|l| l.get(CLAIM_POOL_LABEL)).cloned(),
                // Real Karpenter's own NodeClaim controller writes
                // `.status.nodeName` once the launched instance has
                // registered as a real k8s Node. `o.data` is the raw
                // `serde_json::Value` this DynamicObject carries (this
                // backend deliberately never vendors Karpenter's CRD
                // schema — see this module's own top-level doc comment).
                node_name: o.data.pointer("/status/nodeName").and_then(|v| v.as_str()).map(str::to_string),
            })
            .collect())
    }

    async fn create_nodeclaim(&self, obj: DynamicObject) -> Result<(), ProviderError> {
        let api: Api<DynamicObject> = Api::all_with(self.client.clone(), &nodeclaim_resource());
        api.create(&PostParams::default(), &obj).await.map(|_| ()).map_err(|e| ProviderError::ApiTransient(e.to_string()))
    }

    async fn delete_nodeclaim(&self, name: &str) -> Result<(), ProviderError> {
        let api: Api<DynamicObject> = Api::all_with(self.client.clone(), &nodeclaim_resource());
        api.delete(name, &DeleteParams::default()).await.map(|_| ()).map_err(|e| ProviderError::ApiTransient(e.to_string()))
    }

    async fn observe_busy_node_names(&self) -> Result<HashSet<String>, ProviderError> {
        let pods = Api::<Pod>::all(self.client.clone()).list(&ListParams::default()).await.map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        Ok(pods
            .items
            .iter()
            .filter(|p| is_busy_runner_pod(p))
            .filter_map(|p| p.spec.as_ref()?.node_name.clone())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_gate::{live_gate, shadow_gate};
    use super::{
        build_nodeclaim, claim_is_busy, is_karpenter_managed_ref, nodeclaim_resource, nodepool_resource, owned_by_nodepool,
        parse_nodepool_template, KarpenterEnvironment, KarpenterProvedor, NodeClaimRef, NodePoolTemplate, ObservedNode,
        CLAIM_POOL_LABEL, KARPENTER_NODE_POOL_REF_LABEL, KARPENTER_NODEPOOL_LABEL,
    };
    use async_trait::async_trait;
    use breathe_provider::{FormaSample, Provedor, ProviderError, ProvisionReceipt};
    use k8s_openapi::api::core::v1::Node;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::core::DynamicObject;
    use std::collections::HashSet;
    use std::sync::Mutex;

    fn node_with_nodepool_label(label: Option<&str>) -> Node {
        let labels = label.map(|v| {
            let mut m = std::collections::BTreeMap::new();
            m.insert(KARPENTER_NODEPOOL_LABEL.to_string(), v.to_string());
            m
        });
        Node { metadata: ObjectMeta { labels, ..Default::default() }, ..Default::default() }
    }

    #[test]
    fn resource_gvks_are_the_expected_karpenter_kinds_and_plurals() {
        let np = nodepool_resource();
        assert_eq!(np.group, "karpenter.sh");
        assert_eq!(np.version, "v1");
        assert_eq!(np.kind, "NodePool");
        assert_eq!(np.plural, "nodepools");

        let nc = nodeclaim_resource();
        assert_eq!(nc.group, "karpenter.sh");
        assert_eq!(nc.version, "v1");
        assert_eq!(nc.kind, "NodeClaim");
        assert_eq!(nc.plural, "nodeclaims");
    }

    #[test]
    fn owned_by_nodepool_matches_only_the_referenced_nodepool() {
        assert!(!owned_by_nodepool(&node_with_nodepool_label(None), "camelot-agents"), "an unlabelled node is never owned");
        assert!(
            !owned_by_nodepool(&node_with_nodepool_label(Some("other-pool")), "camelot-agents"),
            "another NodePool's node is not a match"
        );
        assert!(
            owned_by_nodepool(&node_with_nodepool_label(Some("camelot-agents")), "camelot-agents"),
            "the referenced NodePool's own node matches"
        );
    }

    #[test]
    fn is_karpenter_managed_ref_matches_only_this_pool() {
        // The load-bearing safety boundary the deprovision defense-in-depth
        // re-check relies on — breathe deletes ONLY its own claims.
        let none = NodeClaimRef { name: "a".into(), pool_label: None, node_name: None };
        let other = NodeClaimRef { name: "b".into(), pool_label: Some("other-pool".into()), node_name: None };
        let mine = NodeClaimRef { name: "c".into(), pool_label: Some("camelot-agents".into()), node_name: None };
        assert!(!is_karpenter_managed_ref(&none, "camelot-agents"));
        assert!(!is_karpenter_managed_ref(&other, "camelot-agents"));
        assert!(is_karpenter_managed_ref(&mine, "camelot-agents"));
    }

    #[test]
    fn build_nodeclaim_has_no_fixed_name_generates_from_pool_and_copies_spec_verbatim() {
        let spec = serde_json::json!({
            "requirements": [{"key": "karpenter.sh/capacity-type", "operator": "In", "values": ["on-demand"]}],
            "nodeClassRef": {"group": "karpenter.k8s.aws", "kind": "EC2NodeClass", "name": "default"},
        });
        let template = NodePoolTemplate { spec: spec.clone(), ..NodePoolTemplate::default() };
        let obj = build_nodeclaim("camelot-agents", "camelot-nodepool", &template);

        // No fixed name — the apiserver assigns the suffix (concurrent
        // provisions never collide).
        assert_eq!(obj.metadata.name, None, "a minted NodeClaim must never carry a fixed name");
        assert_eq!(obj.metadata.generate_name.as_deref(), Some("breathe-camelot-agents-"));

        let labels = obj.metadata.labels.as_ref().expect("labels set");
        assert_eq!(labels.get(CLAIM_POOL_LABEL).map(String::as_str), Some("camelot-agents"));
        assert_eq!(labels.get(KARPENTER_NODE_POOL_REF_LABEL).map(String::as_str), Some("camelot-nodepool"));

        // The template spec is copied VERBATIM — no field-by-field re-modeling.
        assert_eq!(obj.data["spec"], spec, "spec.template.spec is copied verbatim into the NodeClaim spec");
    }

    #[test]
    fn build_nodeclaim_stamps_the_ownership_label_observe_owned_nodes_actually_filters_on() {
        // Real Karpenter's own NodePool->NodeClaim controller stamps
        // `karpenter.sh/nodepool` on every NodeClaim it constructs, and that
        // label then propagates onto the launched Node. breathe is its OWN
        // minting authority here (never delegating to that controller), so
        // build_nodeclaim must synthesize the same label itself. Without
        // this, breathe's own observe_owned_nodes()/owned_by_nodepool()
        // filter would never match a node breathe itself caused to be
        // launched — self-referentially zero owned capacity forever.
        let template = NodePoolTemplate::default();
        let obj = build_nodeclaim("camelot-agents", "camelot-nodepool", &template);
        let labels = obj.metadata.labels.as_ref().expect("labels set");
        assert_eq!(
            labels.get(KARPENTER_NODEPOOL_LABEL).map(String::as_str),
            Some("camelot-nodepool"),
            "the minted NodeClaim must carry karpenter.sh/nodepool=<NodePool name> — \
             the exact key+value owned_by_nodepool() filters real Nodes on"
        );

        // Round-trip through the real filter predicate: a Node carrying this
        // exact label+value (as real Karpenter's launch flow would produce)
        // is recognized as owned by this NodePool.
        let node = node_with_nodepool_label(labels.get(KARPENTER_NODEPOOL_LABEL).map(String::as_str));
        assert!(owned_by_nodepool(&node, "camelot-nodepool"));
    }

    #[test]
    fn build_nodeclaim_copies_template_metadata_labels_and_annotations_without_letting_them_shadow_ownership() {
        let mut template_labels = std::collections::BTreeMap::new();
        template_labels.insert("team".to_string(), "platform".to_string());
        // A conflicting template-authored label using breathe's own
        // ownership key — must never win. Mirrors real Karpenter's
        // `lo.Assign(template.Labels, {NodePoolLabelKey: name})` ordering,
        // where the injected key always wins a collision.
        template_labels.insert(KARPENTER_NODEPOOL_LABEL.to_string(), "should-never-survive".to_string());
        let mut template_annotations = std::collections::BTreeMap::new();
        template_annotations.insert("example.com/owner".to_string(), "sre".to_string());

        let template = NodePoolTemplate {
            spec: serde_json::json!({"requirements": []}),
            labels: template_labels,
            annotations: template_annotations.clone(),
        };
        let obj = build_nodeclaim("camelot-agents", "camelot-nodepool", &template);

        let labels = obj.metadata.labels.as_ref().expect("labels set");
        assert_eq!(labels.get("team").map(String::as_str), Some("platform"), "template-authored labels propagate");
        assert_eq!(
            labels.get(KARPENTER_NODEPOOL_LABEL).map(String::as_str),
            Some("camelot-nodepool"),
            "breathe's synthesized ownership label always wins over a conflicting template-authored one"
        );
        assert_eq!(
            obj.metadata.annotations.as_ref(),
            Some(&template_annotations),
            "template-authored annotations propagate verbatim"
        );
    }

    #[test]
    fn build_nodeclaim_with_no_template_metadata_does_not_panic_and_still_stamps_ownership() {
        let template = NodePoolTemplate { spec: serde_json::json!({}), ..NodePoolTemplate::default() };
        let obj = build_nodeclaim("pool", "nodepool", &template);
        let labels = obj.metadata.labels.as_ref().expect("labels set");
        assert_eq!(labels.get(KARPENTER_NODEPOOL_LABEL).map(String::as_str), Some("nodepool"));
        assert_eq!(labels.get(CLAIM_POOL_LABEL).map(String::as_str), Some("pool"));
        assert!(obj.metadata.annotations.is_none(), "no annotations block is set on the NodeClaim when the template carries none");
    }

    #[test]
    fn parse_nodepool_template_with_no_metadata_block_defaults_to_empty_maps_and_does_not_panic() {
        let node_pool_data = serde_json::json!({
            "spec": { "template": { "spec": { "requirements": [] } } }
        });
        let template = parse_nodepool_template("camelot-nodepool", &node_pool_data).expect("spec present, must parse");
        assert_eq!(template.spec, serde_json::json!({"requirements": []}));
        assert!(template.labels.is_empty(), "a nil metadata block yields empty labels, not a panic");
        assert!(template.annotations.is_empty(), "a nil metadata block yields empty annotations, not a panic");
    }

    #[test]
    fn parse_nodepool_template_with_metadata_extracts_labels_and_annotations() {
        let node_pool_data = serde_json::json!({
            "spec": {
                "template": {
                    "metadata": {
                        "labels": {"team": "platform"},
                        "annotations": {"example.com/owner": "sre"},
                    },
                    "spec": { "requirements": [] },
                }
            }
        });
        let template = parse_nodepool_template("camelot-nodepool", &node_pool_data).expect("spec present, must parse");
        assert_eq!(template.labels.get("team").map(String::as_str), Some("platform"));
        assert_eq!(template.annotations.get("example.com/owner").map(String::as_str), Some("sre"));
    }

    #[test]
    fn parse_nodepool_template_missing_spec_still_errors_the_same_as_before() {
        let node_pool_data = serde_json::json!({ "spec": { "template": {} } });
        let err = parse_nodepool_template("camelot-nodepool", &node_pool_data).expect_err("missing spec must surface, never be silently defaulted");
        assert!(matches!(err, ProviderError::ApiPermanent(_)));
    }

    /// The mockable [`KarpenterEnvironment`] fixture — proves
    /// [`KarpenterProvedor`]'s translation logic without any live cluster,
    /// the same shape `breathe-lifecycle`'s `DriftEnvironment` tests use.
    struct MockEnv {
        nodes: Vec<ObservedNode>,
        pod_demand_milli: u64,
        template: Result<NodePoolTemplate, ProviderError>,
        managed_claims: Vec<NodeClaimRef>,
        /// The first N `create_nodeclaim` calls fail; the rest succeed —
        /// exercises the SAME non-fatal partial-failure semantics
        /// `KwokProvedor::provision` already has.
        fail_first_n_creates: usize,
        create_attempts: Mutex<usize>,
        created: Mutex<Vec<DynamicObject>>,
        /// Names that fail to delete (the rest succeed).
        fail_deletes: std::collections::BTreeSet<String>,
        deleted: Mutex<Vec<String>>,
        /// Node names `observe_busy_node_names` reports as busy (2026-07-25,
        /// task #75). Empty by default (`empty()`) -- tests that care set it
        /// explicitly, matching every other MockEnv field's convention.
        busy_nodes: HashSet<String>,
    }

    impl MockEnv {
        fn empty() -> Self {
            Self {
                nodes: vec![],
                pod_demand_milli: 0,
                template: Ok(NodePoolTemplate::default()),
                managed_claims: vec![],
                fail_first_n_creates: 0,
                create_attempts: Mutex::new(0),
                created: Mutex::new(vec![]),
                fail_deletes: std::collections::BTreeSet::new(),
                deleted: Mutex::new(vec![]),
                busy_nodes: HashSet::new(),
            }
        }
    }

    #[async_trait]
    impl KarpenterEnvironment for MockEnv {
        async fn observe_owned_nodes(&self, _node_pool_ref: &str) -> Result<Vec<ObservedNode>, ProviderError> {
            Ok(self.nodes.clone())
        }
        async fn observe_pod_demand_milli(&self) -> Result<u64, ProviderError> {
            Ok(self.pod_demand_milli)
        }
        async fn get_nodepool_template(&self, _node_pool_ref: &str) -> Result<NodePoolTemplate, ProviderError> {
            self.template.clone()
        }
        async fn list_managed_nodeclaims(&self, _pool: &str) -> Result<Vec<NodeClaimRef>, ProviderError> {
            Ok(self.managed_claims.clone())
        }
        async fn create_nodeclaim(&self, obj: DynamicObject) -> Result<(), ProviderError> {
            let mut attempts = self.create_attempts.lock().unwrap();
            *attempts += 1;
            if *attempts <= self.fail_first_n_creates {
                return Err(ProviderError::ApiTransient("mock create failure".into()));
            }
            self.created.lock().unwrap().push(obj);
            Ok(())
        }
        async fn delete_nodeclaim(&self, name: &str) -> Result<(), ProviderError> {
            if self.fail_deletes.contains(name) {
                return Err(ProviderError::ApiTransient("mock delete failure".into()));
            }
            self.deleted.lock().unwrap().push(name.to_string());
            Ok(())
        }
        async fn observe_busy_node_names(&self) -> Result<HashSet<String>, ProviderError> {
            Ok(self.busy_nodes.clone())
        }
    }

    #[tokio::test]
    async fn observe_computes_used_and_capacity_from_mocked_nodes_and_pods() {
        let env = MockEnv {
            nodes: vec![
                ObservedNode { name: "n1".into(), allocatable_cpu_milli: 4000 },
                ObservedNode { name: "n2".into(), allocatable_cpu_milli: 4000 },
            ],
            pod_demand_milli: 6000,
            ..MockEnv::empty()
        };
        let p = KarpenterProvedor::new(env, "camelot-agents".into(), "camelot-nodepool".into(), live_gate());
        let sample = p.observe().await.expect("observe succeeds");
        assert_eq!(sample.capacity, 2, "capacity = count of owned Ready nodes");
        // per_node = 8000/2 = 4000; used = ceil(6000/4000) = 2
        assert_eq!(sample, FormaSample { used: 2, capacity: 2 });
    }

    /// **This test used to pin the defect.** It asserted `capacity == 1` and
    /// `used >= 1` for an empty pool — the `.max(1)` floors — which made an
    /// idle pool report utilisation 1.0 and grow forever. Scale-to-zero was
    /// arithmetically unreachable, and the shrink edge is the packing
    /// mechanism. `MetricMissingPolicy::Trust` on this band already treats 0 as
    /// a real reading, so nothing downstream ever needed the lie.
    #[tokio::test]
    async fn an_empty_pool_with_no_demand_reports_zero_not_a_floor_of_one() {
        let env = MockEnv { nodes: vec![], pod_demand_milli: 0, ..MockEnv::empty() };
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        let sample = p.observe().await.expect("observe succeeds");
        assert_eq!(
            sample,
            FormaSample { used: 0, capacity: 0 },
            "an idle empty pool is 0/0 — anything else pins utilisation at 1.0"
        );
    }

    /// Demand with no nodes means "at least one node", never "one node per
    /// millicore". The old `per_node = 1` fallback turned 500 millicores into a
    /// request for 500 nodes, capped only by the pool ceiling.
    #[tokio::test]
    async fn demand_with_no_owned_nodes_asks_for_one_node_not_one_per_millicore() {
        let env = MockEnv { nodes: vec![], pod_demand_milli: 500, ..MockEnv::empty() };
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        let sample = p.observe().await.expect("observe succeeds");
        assert_eq!(sample, FormaSample { used: 1, capacity: 0 });
    }

    #[tokio::test]
    async fn per_node_alloc_milli_is_the_mean_over_owned_nodes_and_floors_to_one_when_empty() {
        let env = MockEnv {
            nodes: vec![
                ObservedNode { name: "n1".into(), allocatable_cpu_milli: 2000 },
                ObservedNode { name: "n2".into(), allocatable_cpu_milli: 6000 },
            ],
            ..MockEnv::empty()
        };
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        assert_eq!(p.per_node_alloc_milli().await, 4000);

        let p_empty = KarpenterProvedor::new(MockEnv::empty(), "pool".into(), "nodepool".into(), live_gate());
        assert_eq!(p_empty.per_node_alloc_milli().await, 1, "an empty owned-node set floors to 1, never 0");
    }

    #[tokio::test]
    async fn provision_zero_is_noop_and_creates_nothing() {
        let env = MockEnv::empty();
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        assert_eq!(p.provision(0).await.unwrap(), ProvisionReceipt::NoOp);
    }

    #[tokio::test]
    async fn provision_dry_run_reports_would_and_creates_nothing() {
        let env = MockEnv {
            template: Ok(NodePoolTemplate { spec: serde_json::json!({"x": 1}), ..NodePoolTemplate::default() }),
            ..MockEnv::empty()
        };
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), shadow_gate());
        let receipt = p.provision(3).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::DryRun { would: 3 });
        assert!(p.env.created.lock().unwrap().is_empty(), "dry-run must create zero NodeClaims");
    }

    #[tokio::test]
    async fn provision_live_creates_n_nodeclaims_copying_the_template_spec_verbatim() {
        let spec = serde_json::json!({"requirements": [{"key": "k", "operator": "In", "values": ["v"]}]});
        let env = MockEnv { template: Ok(NodePoolTemplate { spec: spec.clone(), ..NodePoolTemplate::default() }), ..MockEnv::empty() };
        let p = KarpenterProvedor::new(env, "camelot-agents".into(), "camelot-nodepool".into(), live_gate());
        let receipt = p.provision(3).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::Applied { delta: 3, plan_id: "karpenter:provision:camelot-agents".into() });

        let created = p.env.created.lock().unwrap();
        assert_eq!(created.len(), 3);
        for obj in created.iter() {
            assert_eq!(obj.metadata.name, None);
            assert_eq!(obj.metadata.generate_name.as_deref(), Some("breathe-camelot-agents-"));
            assert_eq!(obj.data["spec"], spec);
            let labels = obj.metadata.labels.as_ref().unwrap();
            assert_eq!(labels.get(CLAIM_POOL_LABEL).map(String::as_str), Some("camelot-agents"));
            assert_eq!(
                labels.get(KARPENTER_NODEPOOL_LABEL).map(String::as_str),
                Some("camelot-nodepool"),
                "every live-created NodeClaim carries the ownership label observe_owned_nodes filters on"
            );
        }
    }

    #[tokio::test]
    async fn provision_live_missing_template_spec_propagates_the_error_and_creates_nothing() {
        let env = MockEnv {
            template: Err(ProviderError::ApiPermanent("NodePool camelot-nodepool has no spec.template.spec".into())),
            ..MockEnv::empty()
        };
        let p = KarpenterProvedor::new(env, "pool".into(), "camelot-nodepool".into(), live_gate());
        let err = p.provision(2).await.expect_err("a missing template spec must surface, never be silently skipped");
        assert!(matches!(err, ProviderError::ApiPermanent(_)));
        assert!(p.env.created.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn provision_live_partial_create_failure_reports_the_actual_applied_count() {
        // The first attempt fails (transient), the remaining two succeed —
        // the same non-fatal-retry semantics KwokProvedor::provision has.
        let env = MockEnv { fail_first_n_creates: 1, ..MockEnv::empty() };
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        let receipt = p.provision(3).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::Applied { delta: 2, plan_id: "karpenter:provision:pool".into() });
        assert_eq!(p.env.created.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn provision_live_all_creates_failing_reports_noop() {
        let env = MockEnv { fail_first_n_creates: 5, ..MockEnv::empty() };
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        assert_eq!(p.provision(2).await.unwrap(), ProvisionReceipt::NoOp);
    }

    #[tokio::test]
    async fn deprovision_zero_is_noop_and_deletes_nothing() {
        let env = MockEnv::empty();
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        assert_eq!(p.deprovision(0).await.unwrap(), ProvisionReceipt::NoOp);
    }

    #[tokio::test]
    async fn deprovision_dry_run_reports_would_and_deletes_nothing() {
        let env = MockEnv {
            managed_claims: vec![NodeClaimRef { name: "c1".into(), pool_label: Some("pool".into()), node_name: None }],
            ..MockEnv::empty()
        };
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), shadow_gate());
        let receipt = p.deprovision(1).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::DryRun { would: -1 });
        assert!(p.env.deleted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn deprovision_live_deletes_n_claims_in_deterministic_sorted_order() {
        let env = MockEnv {
            managed_claims: vec![
                NodeClaimRef { name: "zzz".into(), pool_label: Some("pool".into()), node_name: None },
                NodeClaimRef { name: "aaa".into(), pool_label: Some("pool".into()), node_name: None },
                NodeClaimRef { name: "mmm".into(), pool_label: Some("pool".into()), node_name: None },
            ],
            ..MockEnv::empty()
        };
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        let receipt = p.deprovision(2).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::Applied { delta: -2, plan_id: "karpenter:deprovision:pool".into() });
        assert_eq!(*p.env.deleted.lock().unwrap(), vec!["aaa".to_string(), "mmm".to_string()]);
    }

    #[tokio::test]
    async fn deprovision_defense_in_depth_skips_a_claim_whose_pool_label_does_not_match() {
        // Simulates a scoping bug in the environment's list call: a foreign
        // claim slips into the returned set. The defense-in-depth re-check
        // must refuse to delete it regardless.
        let env = MockEnv {
            managed_claims: vec![
                NodeClaimRef { name: "mine".into(), pool_label: Some("pool".into()), node_name: None },
                NodeClaimRef { name: "foreign".into(), pool_label: Some("other-pool".into()), node_name: None },
            ],
            ..MockEnv::empty()
        };
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        let receipt = p.deprovision(2).await.unwrap();
        // Only "mine" is deleted — "foreign" is skipped, so delta is -1 not -2.
        assert_eq!(receipt, ProvisionReceipt::Applied { delta: -1, plan_id: "karpenter:deprovision:pool".into() });
        assert_eq!(*p.env.deleted.lock().unwrap(), vec!["mine".to_string()]);
    }

    #[tokio::test]
    async fn deprovision_all_matching_only_foreign_claims_reports_noop() {
        let env = MockEnv {
            managed_claims: vec![NodeClaimRef { name: "foreign".into(), pool_label: Some("other-pool".into()), node_name: None }],
            ..MockEnv::empty()
        };
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        assert_eq!(p.deprovision(1).await.unwrap(), ProvisionReceipt::NoOp);
    }

    // ── task #75: busy-node protection ──────────────────────────────────

    #[test]
    fn claim_is_busy_true_only_when_node_name_is_in_the_busy_set() {
        let busy: HashSet<String> = ["node-a".to_string()].into_iter().collect();
        let on_busy_node = NodeClaimRef { name: "c".into(), pool_label: Some("pool".into()), node_name: Some("node-a".into()) };
        let on_idle_node = NodeClaimRef { name: "c".into(), pool_label: Some("pool".into()), node_name: Some("node-b".into()) };
        let unregistered = NodeClaimRef { name: "c".into(), pool_label: Some("pool".into()), node_name: None };
        assert!(claim_is_busy(&on_busy_node, &busy));
        assert!(!claim_is_busy(&on_idle_node, &busy));
        // A freshly-minted claim with no registered node yet carries no
        // running pods by definition — never treated as busy regardless of
        // the busy set's contents (see NodeClaimRef::node_name's own doc).
        assert!(!claim_is_busy(&unregistered, &busy));
    }

    #[tokio::test]
    async fn deprovision_skips_a_busy_claim_even_when_it_sorts_first() {
        // "aaa" is alphabetically first (the plain sort-order tiebreak the
        // no-busy-protection tests above rely on) but sits on a node
        // reported busy — it must never be selected, even though it would
        // win a bare alphabetical pick.
        let env = MockEnv {
            managed_claims: vec![
                NodeClaimRef { name: "aaa".into(), pool_label: Some("pool".into()), node_name: Some("node-busy".into()) },
                NodeClaimRef { name: "mmm".into(), pool_label: Some("pool".into()), node_name: Some("node-idle".into()) },
            ],
            busy_nodes: ["node-busy".to_string()].into_iter().collect(),
            ..MockEnv::empty()
        };
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        let receipt = p.deprovision(1).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::Applied { delta: -1, plan_id: "karpenter:deprovision:pool".into() });
        assert_eq!(*p.env.deleted.lock().unwrap(), vec!["mmm".to_string()]);
    }

    #[tokio::test]
    async fn deprovision_falls_short_of_n_when_not_enough_idle_claims_exist() {
        // Both managed claims sit on busy nodes — asking for 2 must delete
        // 0, not fall back to deleting a busy one to hit the requested count.
        let env = MockEnv {
            managed_claims: vec![
                NodeClaimRef { name: "aaa".into(), pool_label: Some("pool".into()), node_name: Some("node-busy-1".into()) },
                NodeClaimRef { name: "bbb".into(), pool_label: Some("pool".into()), node_name: Some("node-busy-2".into()) },
            ],
            busy_nodes: ["node-busy-1".to_string(), "node-busy-2".to_string()].into_iter().collect(),
            ..MockEnv::empty()
        };
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        assert_eq!(p.deprovision(2).await.unwrap(), ProvisionReceipt::NoOp);
        assert!(p.env.deleted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn deprovision_treats_a_claim_with_no_registered_node_as_idle() {
        // A freshly-minted claim (node not yet registered) must remain a
        // valid deprovision target — it cannot be busy before it has a node.
        let env = MockEnv {
            managed_claims: vec![NodeClaimRef { name: "fresh".into(), pool_label: Some("pool".into()), node_name: None }],
            busy_nodes: ["some-other-node".to_string()].into_iter().collect(),
            ..MockEnv::empty()
        };
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        let receipt = p.deprovision(1).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::Applied { delta: -1, plan_id: "karpenter:deprovision:pool".into() });
        assert_eq!(*p.env.deleted.lock().unwrap(), vec!["fresh".to_string()]);
    }

    #[tokio::test]
    async fn deprovision_degrades_to_treating_all_claims_idle_when_observe_busy_fails() {
        // observe_busy_node_names is best-effort (see deprovision's own doc
        // comment) — a transient failure must not block the shrink tick, it
        // degrades to "protect nothing this tick" rather than erroring out.
        struct FailingBusyEnv(MockEnv);
        #[async_trait]
        impl KarpenterEnvironment for FailingBusyEnv {
            async fn observe_owned_nodes(&self, r: &str) -> Result<Vec<ObservedNode>, ProviderError> {
                self.0.observe_owned_nodes(r).await
            }
            async fn observe_pod_demand_milli(&self) -> Result<u64, ProviderError> {
                self.0.observe_pod_demand_milli().await
            }
            async fn get_nodepool_template(&self, r: &str) -> Result<NodePoolTemplate, ProviderError> {
                self.0.get_nodepool_template(r).await
            }
            async fn list_managed_nodeclaims(&self, p: &str) -> Result<Vec<NodeClaimRef>, ProviderError> {
                self.0.list_managed_nodeclaims(p).await
            }
            async fn create_nodeclaim(&self, obj: DynamicObject) -> Result<(), ProviderError> {
                self.0.create_nodeclaim(obj).await
            }
            async fn delete_nodeclaim(&self, name: &str) -> Result<(), ProviderError> {
                self.0.delete_nodeclaim(name).await
            }
            async fn observe_busy_node_names(&self) -> Result<HashSet<String>, ProviderError> {
                Err(ProviderError::ApiTransient("mock observe_busy_node_names failure".into()))
            }
        }
        let inner = MockEnv {
            managed_claims: vec![NodeClaimRef { name: "c1".into(), pool_label: Some("pool".into()), node_name: Some("node-a".into()) }],
            ..MockEnv::empty()
        };
        let env = FailingBusyEnv(inner);
        let p = KarpenterProvedor::new(env, "pool".into(), "nodepool".into(), live_gate());
        let receipt = p.deprovision(1).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::Applied { delta: -1, plan_id: "karpenter:deprovision:pool".into() });
        assert_eq!(*p.env.0.deleted.lock().unwrap(), vec!["c1".to_string()]);
    }
}
