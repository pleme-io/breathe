//! `EksNodegroupProvedor` — the `EksManagedNodegroup`-backend realization
//! ([`breathe_crd::NodeProvisioningBackend::EksManagedNodegroup`]): reads a
//! real EKS-managed nodegroup's live `scalingConfig`/`status` via
//! `DescribeNodegroup` and, on a `Grew`/`Shrank` tick, mutates
//! `scalingConfig.desiredSize` via `UpdateNodegroupConfig`.
//!
//! # Why this backend exists — the gap `EksKarpenter` doesn't cover
//!
//! Camelot's `system`/`controllers` node pools are plain EKS-managed
//! nodegroups (an ASG the EKS control plane owns) — there is no Karpenter
//! install on that cluster, so [`crate::karpenter_provedor::KarpenterProvedor`]
//! (which mints `karpenter.sh/v1 NodeClaim` objects for a real Karpenter
//! `nodeclaim/lifecycle` controller to consume) has nothing to drive. This
//! backend fills that gap with the ONLY realization mechanism a plain managed
//! nodegroup actually exposes: the EKS `UpdateNodegroupConfig` API's
//! `scalingConfig` field.
//!
//! # The side-effecting boundary — [`EksNodegroupEnvironment`]
//!
//! Every I/O this backend performs (list/read Nodes+Pods, `DescribeNodegroup`,
//! `UpdateNodegroupConfig`) is abstracted behind [`EksNodegroupEnvironment`] —
//! the same `Environment`-trait discipline [`crate::karpenter_provedor::KarpenterEnvironment`]
//! uses (the org's TYPED-SPEC + INTERPRETER TRIPLET convention applied to a
//! provisioning backend): [`KubeEksNodegroupEnvironment`] is the real impl
//! over `kube::Client` + `aws_sdk_eks::Client`; unit tests below drive
//! [`EksNodegroupProvedor`] against an in-memory mock — no live EKS cluster
//! required to prove the clamp-and-decide logic is correct.
//!
//! # Why `UpdateNodegroupConfig`, never `autoscaling:SetDesiredCapacity`
//!
//! An EKS-managed nodegroup's underlying Auto Scaling group is EKS's own —
//! EKS creates it, EKS continuously reconciles it, and AWS's own docs are
//! explicit that you don't interact with it directly (the guidance is to use
//! the EKS API, not the ASG API, for a managed nodegroup). A direct
//! `SetDesiredCapacity` call would race EKS's own control loop: EKS treats
//! its managed ASG's `scalingConfig` as owned state and will reconcile a
//! foreign write back to whatever `UpdateNodegroupConfig` last set, so a
//! bypass either gets silently reverted or fights the control plane on every
//! tick. `UpdateNodegroupConfig`'s `scalingConfig.desiredSize` is therefore
//! not merely the preferred path — it is the only one that is actually
//! effective and supported for a managed nodegroup. `aws-sdk-autoscaling` is
//! deliberately NOT a dependency here for this reason (see the Cargo.toml
//! comment).
//!
//! # What is deliberately NOT built here (the ceiling)
//!
//! No per-instance node selection on scale-down: `UpdateNodegroupConfig`
//! only accepts a new `desiredSize` — WHICH instance the underlying ASG
//! terminates is entirely EKS/ASG's own choice (typically oldest-launch, not
//! PDB-aware), unlike [`crate::karpenter_provedor::KarpenterProvedor`]'s
//! per-`NodeClaim` delete or `KwokProvedor`'s per-`Node` delete. This is a
//! real, honestly-named ceiling of the managed-nodegroup API itself — no
//! amount of client-side cleverness closes it (EKS does not expose a "drain
//! this specific node" scale-down primitive). No launch-template/AMI
//! management, no min/max mutation (only `desiredSize` is ever written —
//! `minSize`/`maxSize` stay whatever was authored on the nodegroup, the
//! existing GitOps-authored precondition, exactly as `karpenterNodePoolRef`
//! is a read-only precondition for the Karpenter backend), no
//! `NodegroupUpdateConfig`/`labels`/`taints` mutation (`UpdateNodegroupConfig`
//! supports those too; this backend only ever sets `scalingConfig`).

use async_trait::async_trait;
use aws_sdk_eks::types::NodegroupScalingConfig;
use breathe_provider::{FormaSample, Provedor, ProviderError, ProvisionReceipt};
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    api::{Api, ListParams},
    Client, ResourceExt,
};
use tracing::warn;

use crate::karpenter_provedor::ObservedNode;
use crate::node_forma::{node_ready, parse_cpu_milli};

/// The label EKS itself stamps on every node a managed nodegroup launches —
/// the read-side scoping key for [`KubeEksNodegroupEnvironment::observe_owned_nodes`].
/// A real, documented EKS label (`aws eks describe-nodegroup` / the AWS docs
/// both name it), not a breathe-authored one — this backend never mints its
/// own ownership label the way [`crate::karpenter_provedor::build_nodeclaim`]
/// synthesizes `karpenter.sh/nodepool`, because EKS already stamps this one.
const EKS_NODEGROUP_LABEL: &str = "eks.amazonaws.com/nodegroup";

/// PURE: is `node` owned by the referenced nodegroup (carries
/// `eks.amazonaws.com/nodegroup == nodegroup_name`)? The scoping predicate
/// [`KubeEksNodegroupEnvironment::observe_owned_nodes`] filters real `Node`
/// objects on.
fn owned_by_nodegroup(node: &Node, nodegroup_name: &str) -> bool {
    node.metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(EKS_NODEGROUP_LABEL))
        .is_some_and(|v| v == nodegroup_name)
}

/// The referenced nodegroup's live `scalingConfig` + `status`, as observed
/// via `DescribeNodegroup` this tick — enough to compute a clamped
/// `desiredSize` delta (SHADOW-safe, always computed) and, on the LIVE path
/// only, to refuse mutating a nodegroup that isn't `ACTIVE` (an
/// `UpdateNodegroupConfig` call against a `CREATING`/`UPDATING`/`DELETING`/
/// `DEGRADED` nodegroup is rejected by the EKS API). `status` is carried as
/// the raw EKS string (`NodegroupStatus::as_str()`) rather than re-modeling
/// the enum — the ONE predicate this backend cares about is
/// [`nodegroup_is_active`], and a raw string keeps the mockable
/// [`EksNodegroupEnvironment`] boundary free of an `aws_sdk_eks` type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodegroupState {
    pub desired_size: u32,
    pub min_size: u32,
    pub max_size: u32,
    pub status: String,
}

/// PURE (tested): is `status` a state `UpdateNodegroupConfig` will actually
/// accept? Only `"ACTIVE"` — every other EKS nodegroup status (`CREATING`,
/// `UPDATING`, `DELETING`, `DEGRADED`, `CREATE_FAILED`, `DELETE_FAILED`, an
/// SDK-unknown future status, …) means a scaling mutation would be rejected
/// by the API, so this refuses ANY status string it doesn't explicitly know
/// is safe rather than deny-listing the ones it happens to have seen.
fn nodegroup_is_active(status: &str) -> bool {
    status == "ACTIVE"
}

/// PURE (tested): the clamped node-count delta `provision(n)` would apply —
/// `current_desired + n`, bounded by the nodegroup's own `maxSize` ceiling.
/// Returns 0 when already at (or past) the ceiling — the caller reports that
/// as [`ProvisionReceipt::NoOp`], never a fabricated `Applied`/`DryRun`.
fn clamp_grow(current_desired: u32, requested_n: u64, max_size: u32) -> u64 {
    let requested = u64::from(current_desired).saturating_add(requested_n);
    let clamped = requested.min(u64::from(max_size));
    clamped.saturating_sub(u64::from(current_desired))
}

/// PURE (tested): the clamped node-count delta `deprovision(n)` would apply —
/// `current_desired - n`, floored by the nodegroup's own `minSize`. Returns 0
/// when already at (or below) the floor.
fn clamp_shrink(current_desired: u32, requested_n: u64, min_size: u32) -> u64 {
    let requested = u64::from(current_desired).saturating_sub(requested_n);
    let clamped = requested.max(u64::from(min_size));
    u64::from(current_desired).saturating_sub(clamped)
}

/// The side-effecting boundary this backend performs ALL its I/O through —
/// the mockable seam. A real implementation ([`KubeEksNodegroupEnvironment`])
/// wraps a `kube::Client` (for node/pod observation) + an `aws_sdk_eks::Client`
/// (for `DescribeNodegroup`/`UpdateNodegroupConfig`); tests wrap an in-memory
/// fixture. Every method maps 1:1 onto one real API call, so a test proves the
/// TRANSLATION logic in [`EksNodegroupProvedor`] (a `Grew`-shaped `n` → a
/// clamped `desiredSize` write, or a `DryRun` report of the same) without
/// touching a live cluster or AWS account.
#[async_trait]
pub trait EksNodegroupEnvironment: Send + Sync {
    /// Ready nodes carrying `eks.amazonaws.com/nodegroup == nodegroup_name`,
    /// each with its allocatable CPU (millicores) — the capacity signal.
    async fn observe_owned_nodes(&self, nodegroup_name: &str) -> Result<Vec<ObservedNode>, ProviderError>;
    /// Requested millicores of Running+Pending pods, cluster-wide.
    ///
    /// v1 simplification, flagged not silently claimed — the SAME
    /// cluster-wide-unscoped shape [`crate::karpenter_provedor::KarpenterEnvironment::observe_pod_demand_milli`]
    /// already carries, for the identical reason: scoping to pods that
    /// tolerate this specific nodegroup's taints is a named follow-up once a
    /// real multi-nodegroup `EksManagedNodegroup` fleet exists.
    async fn observe_pod_demand_milli(&self) -> Result<u64, ProviderError>;
    /// The referenced nodegroup's live `scalingConfig` + `status` — see
    /// [`NodegroupState`]. One real `DescribeNodegroup` call.
    async fn describe_nodegroup(&self, cluster_name: &str, nodegroup_name: &str) -> Result<NodegroupState, ProviderError>;
    /// Write a new `scalingConfig.desiredSize` — one real
    /// `UpdateNodegroupConfig` call. `minSize`/`maxSize` are never touched
    /// (they stay whatever was authored on the nodegroup — a read-only
    /// precondition, same discipline `karpenterNodePoolRef` uses).
    async fn update_desired_size(&self, cluster_name: &str, nodegroup_name: &str, desired_size: u32) -> Result<(), ProviderError>;
}

/// A LIVE actuator against a REAL EKS-managed nodegroup, generic over its
/// [`EksNodegroupEnvironment`] (production: [`KubeEksNodegroupEnvironment`];
/// tests: an in-memory mock). `observe` scopes to Ready nodes carrying
/// `eks.amazonaws.com/nodegroup=<nodegroup_name>` and NEVER calls the EKS API
/// (pure k8s observation — always real, always safe, mirroring
/// [`crate::karpenter_provedor::KarpenterProvedor::observe`]'s convention).
/// `provision`/`deprovision` call `DescribeNodegroup` UNCONDITIONALLY — even
/// in shadow — so the reported `DryRun { would }` reflects the nodegroup's
/// real `minSize`/`maxSize`/`status` ceiling rather than an unclamped echo of
/// `n`. This is a deliberate refinement over `KarpenterProvedor::provision`'s
/// dry-run path (which returns `DryRun { would: n }` without reading the
/// NodePool template at all): a `karpenter.sh NodeClaim` mint has no
/// analogous hard ceiling to clamp against at this grain, an EKS managed
/// nodegroup's `desiredSize` always does.
pub struct EksNodegroupProvedor<E: EksNodegroupEnvironment> {
    env: E,
    pool: String,
    cluster_name: String,
    nodegroup_name: String,
    dry_run: bool,
}

impl<E: EksNodegroupEnvironment> EksNodegroupProvedor<E> {
    pub fn new(env: E, pool: String, cluster_name: String, nodegroup_name: String, dry_run: bool) -> Self {
        Self { env, pool, cluster_name, nodegroup_name, dry_run }
    }

    /// The per-unit allocatable (millicores) used to size a minted `NodeRef`
    /// for the admission gate — the mean over this nodegroup's OWNED Ready
    /// nodes. Mirrors [`crate::karpenter_provedor::KarpenterProvedor::per_node_alloc_milli`]'s
    /// shape, scoped to this backend's node set.
    pub(crate) async fn per_node_alloc_milli(&self) -> u64 {
        match self.env.observe_owned_nodes(&self.nodegroup_name).await {
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
impl<E: EksNodegroupEnvironment> Provedor for EksNodegroupProvedor<E> {
    async fn observe(&self) -> Result<FormaSample, ProviderError> {
        let nodes = self.env.observe_owned_nodes(&self.nodegroup_name).await?;
        let capacity = nodes.len() as u64;
        let total_alloc: u64 = nodes.iter().map(|n| n.allocatable_cpu_milli).sum();
        let demand_milli = self.env.observe_pod_demand_milli().await?;
        let per_node = if capacity > 0 { (total_alloc / capacity).max(1) } else { 1 };
        let used = demand_milli.div_ceil(per_node).max(1);
        Ok(FormaSample { used, capacity: capacity.max(1) })
    }

    async fn provision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        if n == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        // SHADOW-OBSERVE: read the real nodegroup state unconditionally (see
        // the struct doc) so the clamp below is real even in dry-run.
        let state = self.env.describe_nodegroup(&self.cluster_name, &self.nodegroup_name).await?;
        let delta = clamp_grow(state.desired_size, n, state.max_size);
        if delta == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        if self.dry_run {
            return Ok(ProvisionReceipt::DryRun { would: delta as i64 });
        }
        // The live-only status gate: `UpdateNodegroupConfig` rejects a
        // mutation against a nodegroup that isn't `ACTIVE`. Checked here
        // (never above) so a SHADOW pool still reports its real, clamped
        // `would` even while the nodegroup is mid-`UPDATING` — only the
        // REAL mutation is refused on a non-`ACTIVE` status.
        if !nodegroup_is_active(&state.status) {
            return Err(ProviderError::ApiTransient(format!(
                "nodegroup {} status={} (not ACTIVE) — scaling deferred to next tick",
                self.nodegroup_name, state.status
            )));
        }
        let new_desired = state.desired_size.saturating_add(u32::try_from(delta).unwrap_or(u32::MAX));
        self.env.update_desired_size(&self.cluster_name, &self.nodegroup_name, new_desired).await?;
        Ok(ProvisionReceipt::Applied { delta: delta as i64, plan_id: format!("eks-nodegroup:provision:{}", self.pool) })
    }

    async fn deprovision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError> {
        if n == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        let state = self.env.describe_nodegroup(&self.cluster_name, &self.nodegroup_name).await?;
        let delta = clamp_shrink(state.desired_size, n, state.min_size);
        if delta == 0 {
            return Ok(ProvisionReceipt::NoOp);
        }
        if self.dry_run {
            return Ok(ProvisionReceipt::DryRun { would: -(delta as i64) });
        }
        if !nodegroup_is_active(&state.status) {
            return Err(ProviderError::ApiTransient(format!(
                "nodegroup {} status={} (not ACTIVE) — scaling deferred to next tick",
                self.nodegroup_name, state.status
            )));
        }
        let new_desired = state.desired_size.saturating_sub(u32::try_from(delta).unwrap_or(u32::MAX));
        self.env.update_desired_size(&self.cluster_name, &self.nodegroup_name, new_desired).await?;
        Ok(ProvisionReceipt::Applied { delta: -(delta as i64), plan_id: format!("eks-nodegroup:deprovision:{}", self.pool) })
    }
}

/// The real [`EksNodegroupEnvironment`] — node/pod reads are `kube::Api`
/// calls against the live apiserver; `describe_nodegroup`/`update_desired_size`
/// are `aws_sdk_eks::Client` calls against the live EKS control plane.
pub struct KubeEksNodegroupEnvironment {
    kube_client: Client,
    eks_client: aws_sdk_eks::Client,
}

impl KubeEksNodegroupEnvironment {
    pub fn new(kube_client: Client, eks_client: aws_sdk_eks::Client) -> Self {
        Self { kube_client, eks_client }
    }
}

#[async_trait]
impl EksNodegroupEnvironment for KubeEksNodegroupEnvironment {
    async fn observe_owned_nodes(&self, nodegroup_name: &str) -> Result<Vec<ObservedNode>, ProviderError> {
        let nodes = Api::<Node>::all(self.kube_client.clone())
            .list(&ListParams::default())
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        Ok(nodes
            .items
            .iter()
            .filter(|n| node_ready(n) && owned_by_nodegroup(n, nodegroup_name))
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
        let pods = Api::<Pod>::all(self.kube_client.clone())
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

    async fn describe_nodegroup(&self, cluster_name: &str, nodegroup_name: &str) -> Result<NodegroupState, ProviderError> {
        let resp = self
            .eks_client
            .describe_nodegroup()
            .cluster_name(cluster_name)
            .nodegroup_name(nodegroup_name)
            .send()
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        let ng = resp
            .nodegroup()
            .ok_or_else(|| ProviderError::ApiPermanent(format!("DescribeNodegroup returned no nodegroup for {cluster_name}/{nodegroup_name}")))?;
        let scaling = ng
            .scaling_config()
            .ok_or_else(|| ProviderError::ApiPermanent(format!("nodegroup {nodegroup_name} has no scalingConfig")))?;
        Ok(NodegroupState {
            desired_size: u32::try_from(scaling.desired_size().unwrap_or(0)).unwrap_or(0),
            min_size: u32::try_from(scaling.min_size().unwrap_or(0)).unwrap_or(0),
            max_size: u32::try_from(scaling.max_size().unwrap_or(0)).unwrap_or(0),
            status: ng.status().map_or_else(String::new, |s| s.as_str().to_string()),
        })
    }

    async fn update_desired_size(&self, cluster_name: &str, nodegroup_name: &str, desired_size: u32) -> Result<(), ProviderError> {
        let scaling_config = NodegroupScalingConfig::builder().desired_size(i32::try_from(desired_size).unwrap_or(i32::MAX)).build();
        self.eks_client
            .update_nodegroup_config()
            .cluster_name(cluster_name)
            .nodegroup_name(nodegroup_name)
            .scaling_config(scaling_config)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                warn!(cluster_name, nodegroup_name, desired_size, error = %e, "UpdateNodegroupConfig failed (non-fatal; retried next tick)");
                ProviderError::ApiTransient(e.to_string())
            })
    }
}
