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
//! # `minSize`/`maxSize` are ALGORITHMIC, not a static GitOps precondition —
//! corrected 2026-07-19
//!
//! An earlier version of this backend treated `minSize`/`maxSize` as a
//! read-only precondition (mirroring `karpenterNodePoolRef`'s discipline) —
//! only `desiredSize` was ever written, `minSize`/`maxSize` stayed "whatever
//! was authored on the nodegroup" via Terraform/Pangea. That was the wrong
//! model: `BreatheCloudPoolSpec.ceiling`/`.floor` already exist as the ONE
//! governance boundary a human declares (rarely changed, reviewed) — having
//! AWS's OWN `scalingConfig.maxSize`/`minSize` as a SECOND, independently
//! static ceiling meant the pool could get stuck well below its declared
//! `ceiling` with no path to close the gap except a human re-approving a
//! Terraform plan for a value breathe was already supposed to own (caught
//! live, 2026-07-19: `ceiling:5` in spec, AWS `maxSize:2`, a genuinely stuck
//! pool a fresh Terraform-plan-approval was the ONLY way to unblock).
//!
//! Now: `provision`/`deprovision` grow/shrink `maxSize`/`minSize` themselves
//! — by the SAME `growFactor`/`shrinkFactor` tick-by-tick discipline already
//! governing `desiredSize` — whenever the AWS-side bound is what's actually
//! constraining the requested change, clamped at `pool_ceiling`/`pool_floor`
//! (never past them; this is exactly why those fields exist). `ceiling`/
//! `floor` are therefore the only static values left in this whole path —
//! everything AWS actually enforces (`desiredSize`, `maxSize`, `minSize`) is
//! now tick-by-tick derived from them, never hand-set.
//!
//! # What is still deliberately NOT built here
//!
//! No per-instance node selection on scale-down: `UpdateNodegroupConfig`
//! only accepts a new `desiredSize` — WHICH instance the underlying ASG
//! terminates is entirely EKS/ASG's own choice (typically oldest-launch, not
//! PDB-aware), unlike [`crate::karpenter_provedor::KarpenterProvedor`]'s
//! per-`NodeClaim` delete or `KwokProvedor`'s per-`Node` delete. This is a
//! real, honestly-named ceiling of the managed-nodegroup API itself — no
//! amount of client-side cleverness closes it (EKS does not expose a "drain
//! this specific node" scale-down primitive). No launch-template/AMI/
//! `instanceTypes` management (right-sizing NODE SHAPE, as opposed to node
//! COUNT, is a named, separate follow-up — see the module-level tracking
//! note), no `labels`/`taints` mutation (`UpdateNodegroupConfig` supports
//! those too; this backend only ever sets `scalingConfig`).

use async_trait::async_trait;
use aws_sdk_eks::types::NodegroupScalingConfig;
use aws_smithy_types::error::display::DisplayErrorContext;
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

/// PURE (tested): does THIS grow request need `maxSize` itself raised before
/// `desiredSize` can move? Only when the request would exceed the CURRENT
/// AWS-side `max_size` AND there is still room under the pool's own
/// `pool_ceiling` — the one governance boundary that stays human-declared.
/// When both hold, computes the new `maxSize` to write: grown by
/// `grow_factor` (the SAME tick-by-tick pace `desiredSize` already uses),
/// always at least +1 (so a `grow_factor` applied to a small current value
/// like 2 can't round down to a no-op), never past `pool_ceiling`. Returns
/// `None` when `max_size` doesn't need to change (either the request already
/// fits, or `max_size` already equals `pool_ceiling` — genuinely at the
/// governance wall, not a bug).
fn grown_max_size(current_desired: u32, current_max: u32, pool_ceiling: u32, requested_n: u64, grow_factor: f64) -> Option<u32> {
    let requested = u64::from(current_desired).saturating_add(requested_n);
    if requested <= u64::from(current_max) || current_max >= pool_ceiling {
        return None;
    }
    let grown = (f64::from(current_max) * grow_factor).ceil() as u64;
    let grown = grown.max(u64::from(current_max) + 1); // always real forward progress
    Some(grown.min(u64::from(pool_ceiling)) as u32)
}

/// PURE (tested): the mirror of [`grown_max_size`] for shrink — does a
/// SUSTAINED deprovision request justify lowering `maxSize` itself back down
/// toward `pool_floor`? Only when, after this shrink, the resulting
/// `desiredSize` would sit comfortably below a shrunk `maxSize` (never
/// shrinks `maxSize` below the post-shrink `desiredSize` — EKS itself
/// requires `maxSize >= desiredSize`, and this backend never even attempts a
/// write the API would reject) and `current_max` is still above
/// `pool_floor`. Shrinks by `shrink_factor`, floored at `pool_floor`. This is
/// the deliberately slower-moving half — `desiredSize` already reacts fast
/// (`shrinkBelow`/`cooldownSeconds`); ratcheting the AWS ceiling itself back
/// down is a further, separate confirmation that the LOWER bound is truly no
/// longer needed, not just this one tick's dip.
fn shrunk_max_size(post_shrink_desired: u32, current_max: u32, pool_floor: u32, shrink_factor: f64) -> Option<u32> {
    if current_max <= pool_floor {
        return None;
    }
    let shrunk = (f64::from(current_max) * shrink_factor).floor() as u64;
    let shrunk = shrunk.max(u64::from(pool_floor)).max(u64::from(post_shrink_desired));
    if shrunk >= u64::from(current_max) {
        return None; // no real downward movement — don't write a no-op
    }
    Some(shrunk as u32)
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
    /// Write a new `scalingConfig` — one real `UpdateNodegroupConfig` call.
    /// `desired_size` is always set; `new_max_size`/`new_min_size` are set
    /// only on the tick [`grown_max_size`]/[`shrunk_max_size`] decided the
    /// AWS-side bound itself needs to move (most ticks: `None`, so the call
    /// only touches `desiredSize`, exactly as before this backend could also
    /// carve the ceiling itself).
    async fn update_scaling_config(
        &self,
        cluster_name: &str,
        nodegroup_name: &str,
        desired_size: u32,
        new_max_size: Option<u32>,
        new_min_size: Option<u32>,
    ) -> Result<(), ProviderError>;
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
    /// The ONE static, human-declared governance boundary left in this path
    /// (`BreatheCloudPoolSpec.ceiling`) — AWS's own `scalingConfig.maxSize`
    /// is grown toward this algorithmically, never independently hand-set.
    pool_ceiling: u32,
    /// The mirror boundary (`BreatheCloudPoolSpec.floor`) — AWS's own
    /// `scalingConfig.minSize` never shrinks below this.
    pool_floor: u32,
    /// Same pace `desiredSize` already grows/shrinks at
    /// (`BreatheCloudPoolSpec.growFactor`/`.shrinkFactor`) — reused here so
    /// the AWS-side ceiling breathes at the identical tick-by-tick rate, not
    /// a second, independently-tuned velocity.
    grow_factor: f64,
    shrink_factor: f64,
}

impl<E: EksNodegroupEnvironment> EksNodegroupProvedor<E> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        env: E,
        pool: String,
        cluster_name: String,
        nodegroup_name: String,
        dry_run: bool,
        pool_ceiling: u32,
        pool_floor: u32,
        grow_factor: f64,
        shrink_factor: f64,
    ) -> Self {
        Self { env, pool, cluster_name, nodegroup_name, dry_run, pool_ceiling, pool_floor, grow_factor, shrink_factor }
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
        // Does maxSize itself need to grow first? Computed BEFORE clamp_grow
        // so a request the CURRENT max_size would otherwise clamp away gets
        // real headroom when pool_ceiling allows it — this is the whole
        // point: the AWS ceiling breathes toward pool_ceiling instead of
        // silently swallowing demand at a stale static value.
        let new_max = grown_max_size(state.desired_size, state.max_size, self.pool_ceiling, n, self.grow_factor);
        let effective_max = new_max.unwrap_or(state.max_size);
        let delta = clamp_grow(state.desired_size, n, effective_max);
        if delta == 0 && new_max.is_none() {
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
        self.env
            .update_scaling_config(&self.cluster_name, &self.nodegroup_name, new_desired, new_max, None)
            .await?;
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
        let post_shrink_desired = state.desired_size.saturating_sub(u32::try_from(delta).unwrap_or(u32::MAX));
        // Mirror of the grow path: does sustained low demand justify ratcheting
        // maxSize itself back down toward pool_floor? See shrunk_max_size's
        // doc for why this is deliberately more conservative than the
        // desiredSize shrink it rides alongside.
        let new_max = shrunk_max_size(post_shrink_desired, state.max_size, self.pool_floor, self.shrink_factor);
        if self.dry_run {
            return Ok(ProvisionReceipt::DryRun { would: -(delta as i64) });
        }
        if !nodegroup_is_active(&state.status) {
            return Err(ProviderError::ApiTransient(format!(
                "nodegroup {} status={} (not ACTIVE) — scaling deferred to next tick",
                self.nodegroup_name, state.status
            )));
        }
        self.env
            .update_scaling_config(&self.cluster_name, &self.nodegroup_name, post_shrink_desired, new_max, None)
            .await?;
        Ok(ProvisionReceipt::Applied { delta: -(delta as i64), plan_id: format!("eks-nodegroup:deprovision:{}", self.pool) })
    }
}

/// The real [`EksNodegroupEnvironment`] — node/pod reads are `kube::Api`
/// calls against the live apiserver; `describe_nodegroup`/`update_scaling_config`
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
            .map_err(|e| ProviderError::ApiTransient(DisplayErrorContext(e).to_string()))?;
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

    async fn update_scaling_config(
        &self,
        cluster_name: &str,
        nodegroup_name: &str,
        desired_size: u32,
        new_max_size: Option<u32>,
        new_min_size: Option<u32>,
    ) -> Result<(), ProviderError> {
        let mut builder = NodegroupScalingConfig::builder().desired_size(i32::try_from(desired_size).unwrap_or(i32::MAX));
        if let Some(max_size) = new_max_size {
            builder = builder.max_size(i32::try_from(max_size).unwrap_or(i32::MAX));
        }
        if let Some(min_size) = new_min_size {
            builder = builder.min_size(i32::try_from(min_size).unwrap_or(i32::MAX));
        }
        let scaling_config = builder.build();
        self.eks_client
            .update_nodegroup_config()
            .cluster_name(cluster_name)
            .nodegroup_name(nodegroup_name)
            .scaling_config(scaling_config)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                let detail = DisplayErrorContext(e).to_string();
                warn!(
                    cluster_name, nodegroup_name, desired_size, ?new_max_size, ?new_min_size, error = %detail,
                    "UpdateNodegroupConfig failed (non-fatal; retried next tick)"
                );
                ProviderError::ApiTransient(detail)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        clamp_grow, clamp_shrink, grown_max_size, nodegroup_is_active, owned_by_nodegroup, shrunk_max_size, EksNodegroupEnvironment,
        EksNodegroupProvedor, NodegroupState,
    };
    use crate::karpenter_provedor::ObservedNode;
    use async_trait::async_trait;
    use breathe_provider::{FormaSample, Provedor, ProviderError, ProvisionReceipt};
    use k8s_openapi::api::core::v1::Node;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::sync::Mutex;

    fn node_with_nodegroup_label(label: Option<&str>) -> Node {
        let labels = label.map(|v| {
            let mut m = std::collections::BTreeMap::new();
            m.insert(super::EKS_NODEGROUP_LABEL.to_string(), v.to_string());
            m
        });
        Node { metadata: ObjectMeta { labels, ..Default::default() }, ..Default::default() }
    }

    #[test]
    fn owned_by_nodegroup_matches_only_the_referenced_nodegroup() {
        assert!(!owned_by_nodegroup(&node_with_nodegroup_label(None), "system"), "an unlabelled node is never owned");
        assert!(
            !owned_by_nodegroup(&node_with_nodegroup_label(Some("controllers")), "system"),
            "another nodegroup's node is not a match"
        );
        assert!(owned_by_nodegroup(&node_with_nodegroup_label(Some("system")), "system"), "the referenced nodegroup's own node matches");
    }

    #[test]
    fn nodegroup_is_active_accepts_only_the_literal_active_status() {
        assert!(nodegroup_is_active("ACTIVE"));
        for status in ["CREATING", "UPDATING", "DELETING", "DEGRADED", "CREATE_FAILED", "DELETE_FAILED", "", "active"] {
            assert!(!nodegroup_is_active(status), "status {status:?} must not be treated as ACTIVE");
        }
    }

    #[test]
    fn clamp_grow_returns_the_raw_delta_when_well_under_the_ceiling() {
        assert_eq!(clamp_grow(3, 2, 10), 2, "3 -> 5 is within the ceiling of 10");
    }

    #[test]
    fn clamp_grow_clamps_to_the_ceiling_when_the_raw_delta_would_overshoot() {
        assert_eq!(clamp_grow(8, 5, 10), 2, "8 -> 13 requested, clamped to 8 -> 10 => delta 2");
    }

    #[test]
    fn clamp_grow_returns_zero_when_already_at_the_ceiling() {
        assert_eq!(clamp_grow(10, 5, 10), 0, "already at the ceiling — nothing more to grow");
    }

    #[test]
    fn clamp_grow_returns_zero_when_past_the_ceiling() {
        // Defensive: a nodegroup observed ABOVE its own configured max
        // (e.g. an operator manually bumped it out-of-band) must never
        // report a negative-turned-huge delta via saturating arithmetic.
        assert_eq!(clamp_grow(12, 5, 10), 0);
    }

    #[test]
    fn clamp_shrink_returns_the_raw_delta_when_well_above_the_floor() {
        assert_eq!(clamp_shrink(8, 2, 2), 2, "8 -> 6 is within the floor of 2");
    }

    #[test]
    fn clamp_shrink_clamps_to_the_floor_when_the_raw_delta_would_undershoot() {
        assert_eq!(clamp_shrink(4, 5, 2), 2, "4 -> -1 requested, clamped to 4 -> 2 => delta 2");
    }

    #[test]
    fn clamp_shrink_returns_zero_when_already_at_the_floor() {
        assert_eq!(clamp_shrink(2, 5, 2), 0, "already at the floor — nothing more to shrink");
    }

    #[test]
    fn grown_max_size_is_none_when_the_request_already_fits_under_current_max() {
        // desired=3, max=10, requesting 2 -> 5, well under max=10: no ceiling move needed.
        assert_eq!(grown_max_size(3, 10, 20, 2, 1.25), None);
    }

    #[test]
    fn grown_max_size_is_none_when_max_already_equals_pool_ceiling() {
        // max=10 == pool_ceiling=10: genuinely at the governance wall, not a bug.
        assert_eq!(grown_max_size(8, 10, 10, 5, 1.25), None);
    }

    #[test]
    fn grown_max_size_grows_by_grow_factor_when_the_request_overshoots_current_max() {
        // desired=1, max=2, requesting 2 more -> 3, which exceeds max=2.
        // ceil(2*1.25)=3, and max+1=3 too -- both agree on 3 here (a distinct
        // case with a bigger current_max proves the .max(current_max+1) floor
        // actually matters, see grown_max_size_always_makes_real_forward_progress
        // below for the case where they'd otherwise disagree).
        assert_eq!(grown_max_size(1, 2, 5, 2, 1.25), Some(3));
    }

    #[test]
    fn grown_max_size_reproduces_the_live_camelot_stall_and_closes_it() {
        // The exact live shape this fix closes: desiredSize=1, AWS maxSize=2,
        // BreatheCloudPool ceiling=5, a pool wanting +1 more capacity. Old
        // behavior: clamp_grow(1, 1, 2) = 1 (1 -> 2, already reachable, so
        // this specific request didn't even need a max bump) -- but the NEXT
        // tick wanting further growth (request 2 -> 3) is where max=2 used to
        // permanently wall it off with no path forward except a human
        // Terraform-approving a new max. Proven here directly: requesting a
        // 2nd unit of growth once desired is already AT max=2.
        assert_eq!(
            grown_max_size(2, 2, 5, 1, 1.25),
            Some(3),
            "2 -> 3 requested, exceeds max=2, ceiling=5 has room: grow max by grow_factor (ceil(2*1.25)=3), never jump straight to 5"
        );
    }

    #[test]
    fn grown_max_size_never_exceeds_pool_ceiling_even_with_a_large_request() {
        // desired=2, max=2, pool_ceiling=3, requesting 10 more -> grow_factor
        // would suggest ceil(2*1.25)=3, which happens to equal the ceiling
        // here -- but the real invariant under test is the .min(pool_ceiling)
        // clamp, proven distinctly by the next case at a lower ceiling.
        assert_eq!(grown_max_size(2, 2, 3, 10, 1.25), Some(3));
    }

    #[test]
    fn grown_max_size_clamps_to_pool_ceiling_when_grow_factor_would_overshoot_it() {
        // desired=8, max=8, pool_ceiling=9 -- ceil(8*1.25)=10, which would
        // overshoot pool_ceiling=9; must clamp to 9, never write past the
        // governance wall.
        assert_eq!(grown_max_size(8, 8, 9, 5, 1.25), Some(9));
    }

    #[test]
    fn grown_max_size_always_makes_real_forward_progress_even_at_a_tiny_current_max() {
        // current_max=1, grow_factor=1.25 -> ceil(1*1.25)=2, which is already
        // current_max+1, so this doesn't even need the .max(current_max+1)
        // floor to prove progress -- but confirms the small-value case still
        // moves, never silently stalls at 1 forever due to rounding.
        assert_eq!(grown_max_size(1, 1, 10, 1, 1.25), Some(2));
    }

    #[test]
    fn shrunk_max_size_is_none_when_max_already_equals_pool_floor() {
        assert_eq!(shrunk_max_size(2, 2, 2, 0.9), None, "max=floor=2: nothing left to shrink");
    }

    #[test]
    fn shrunk_max_size_is_none_when_the_shrunk_value_would_not_move() {
        // max=3, shrink_factor=0.9 -> floor(3*0.9)=2, floored again at
        // post_shrink_desired=2 (nothing to gain) and pool_floor=1 -- max(2,1,2)=2,
        // which is still < current_max=3, so it DOES move here (Some(2)); this
        // case instead proves the specific "no movement" guard using a max
        // that's already tight against its own floor via post_shrink_desired.
        assert_eq!(shrunk_max_size(3, 3, 1, 0.9), None, "post_shrink_desired=3 == current_max=3 -- nothing to shrink into");
    }

    #[test]
    fn shrunk_max_size_shrinks_by_shrink_factor_toward_pool_floor() {
        // max=10, shrink_factor=0.9 -> floor(10*0.9)=9; post_shrink_desired=2,
        // pool_floor=2 -- max(9,2,2)=9, a real, single-tick step down, not a
        // jump straight to the floor.
        assert_eq!(shrunk_max_size(2, 10, 2, 0.9), Some(9));
    }

    #[test]
    fn shrunk_max_size_never_shrinks_below_the_post_shrink_desired_size() {
        // EKS itself requires maxSize >= desiredSize -- max=10, shrink_factor
        // would suggest floor(10*0.5)=5, but post_shrink_desired=7 means 5
        // would be an invalid (and API-rejected) scalingConfig. Must clamp up
        // to 7, never write a value the API would refuse.
        assert_eq!(shrunk_max_size(7, 10, 1, 0.5), Some(7));
    }

    #[test]
    fn shrunk_max_size_never_shrinks_below_pool_floor() {
        assert_eq!(shrunk_max_size(1, 10, 4, 0.1), Some(4), "floor(10*0.1)=1, but pool_floor=4 wins");
    }

    /// The mockable [`EksNodegroupEnvironment`] fixture — proves
    /// [`EksNodegroupProvedor`]'s translation logic without any live cluster
    /// or AWS account, the same shape
    /// [`crate::karpenter_provedor::tests::MockEnv`] uses.
    struct MockEnv {
        nodes: Vec<ObservedNode>,
        pod_demand_milli: u64,
        state: Result<NodegroupState, ProviderError>,
        /// One entry per `update_scaling_config` call: `(desired_size,
        /// new_max_size, new_min_size)` — the last two `None` on most ticks
        /// (only `desiredSize` moved), `Some` exactly when a test proves the
        /// ceiling/floor itself was carved this tick.
        update_calls: Mutex<Vec<(u32, Option<u32>, Option<u32>)>>,
        fail_update: bool,
    }

    impl MockEnv {
        fn empty() -> Self {
            Self {
                nodes: vec![],
                pod_demand_milli: 0,
                state: Ok(NodegroupState { desired_size: 3, min_size: 1, max_size: 10, status: "ACTIVE".into() }),
                update_calls: Mutex::new(vec![]),
                fail_update: false,
            }
        }
    }

    #[async_trait]
    impl EksNodegroupEnvironment for MockEnv {
        async fn observe_owned_nodes(&self, _nodegroup_name: &str) -> Result<Vec<ObservedNode>, ProviderError> {
            Ok(self.nodes.clone())
        }
        async fn observe_pod_demand_milli(&self) -> Result<u64, ProviderError> {
            Ok(self.pod_demand_milli)
        }
        async fn describe_nodegroup(&self, _cluster_name: &str, _nodegroup_name: &str) -> Result<NodegroupState, ProviderError> {
            self.state.clone()
        }
        async fn update_scaling_config(
            &self,
            _cluster_name: &str,
            _nodegroup_name: &str,
            desired_size: u32,
            new_max_size: Option<u32>,
            new_min_size: Option<u32>,
        ) -> Result<(), ProviderError> {
            if self.fail_update {
                return Err(ProviderError::ApiTransient("mock UpdateNodegroupConfig failure".into()));
            }
            self.update_calls.lock().unwrap().push((desired_size, new_max_size, new_min_size));
            Ok(())
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
        let p = EksNodegroupProvedor::new(env, "system".into(), "camelot-eks".into(), "system".into(), false, 10, 10, 1.25, 0.9);
        let sample = p.observe().await.expect("observe succeeds");
        assert_eq!(sample.capacity, 2, "capacity = count of owned Ready nodes");
        // per_node = 8000/2 = 4000; used = ceil(6000/4000) = 2
        assert_eq!(sample, FormaSample { used: 2, capacity: 2 });
    }

    #[tokio::test]
    async fn observe_with_zero_owned_nodes_reports_zero_capacity_floored_to_one_and_used_at_least_one() {
        let env = MockEnv { nodes: vec![], pod_demand_milli: 500, ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), false, 10, 10, 1.25, 0.9);
        let sample = p.observe().await.expect("observe succeeds");
        assert_eq!(sample.capacity, 1, "capacity floors to 1 even with zero owned nodes (never a div-by-zero)");
        assert!(sample.used >= 1);
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
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), false, 10, 10, 1.25, 0.9);
        assert_eq!(p.per_node_alloc_milli().await, 4000);

        let p_empty = EksNodegroupProvedor::new(MockEnv::empty(), "pool".into(), "cluster".into(), "nodegroup".into(), false, 10, 10, 1.25, 0.9);
        assert_eq!(p_empty.per_node_alloc_milli().await, 1, "an empty owned-node set floors to 1, never 0");
    }

    #[tokio::test]
    async fn provision_zero_is_noop_and_never_calls_describe_nodegroup() {
        let env = MockEnv::empty();
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), false, 10, 10, 1.25, 0.9);
        assert_eq!(p.provision(0).await.unwrap(), ProvisionReceipt::NoOp);
        assert!(p.env.update_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn provision_dry_run_reads_real_state_and_reports_the_clamped_would_mutating_nothing() {
        // desired=3, max=10 -> requesting 5 is well under ceiling, would=5.
        let env = MockEnv::empty();
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), true, 10, 10, 1.25, 0.9);
        let receipt = p.provision(5).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::DryRun { would: 5 });
        assert!(p.env.update_calls.lock().unwrap().is_empty(), "dry-run must call update_scaling_config zero times");
    }

    #[tokio::test]
    async fn provision_dry_run_clamps_the_would_value_to_the_real_ceiling() {
        // desired=3, max=10, requesting 20 -> clamps to would=7 (3 -> 10), NOT
        // a raw unclamped echo of 20. This is the exact behavior task #205
        // asks for: shadow reads real EKS state and reports the REAL would.
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 3, min_size: 1, max_size: 10, status: "ACTIVE".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), true, 10, 10, 1.25, 0.9);
        let receipt = p.provision(20).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::DryRun { would: 7 });
    }

    #[tokio::test]
    async fn provision_dry_run_at_ceiling_reports_noop_not_a_zero_dry_run() {
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 10, min_size: 1, max_size: 10, status: "ACTIVE".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), true, 10, 10, 1.25, 0.9);
        assert_eq!(p.provision(5).await.unwrap(), ProvisionReceipt::NoOp);
    }

    #[tokio::test]
    async fn provision_dry_run_reports_the_real_would_even_while_the_nodegroup_is_not_active() {
        // SHADOW must still compute + report a real, clamped `would` while
        // the nodegroup is mid-UPDATING — only the LIVE mutation is refused
        // on a non-ACTIVE status (see the status gate placement in
        // EksNodegroupProvedor::provision's doc comment).
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 3, min_size: 1, max_size: 10, status: "UPDATING".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), true, 10, 10, 1.25, 0.9);
        let receipt = p.provision(4).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::DryRun { would: 4 });
    }

    #[tokio::test]
    async fn provision_live_writes_the_clamped_desired_size_via_update_desired_size() {
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 3, min_size: 1, max_size: 10, status: "ACTIVE".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "camelot-system".into(), "camelot-eks".into(), "system".into(), false, 10, 10, 1.25, 0.9);
        let receipt = p.provision(4).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::Applied { delta: 4, plan_id: "eks-nodegroup:provision:camelot-system".into() });
        assert_eq!(*p.env.update_calls.lock().unwrap(), vec![(7u32, None, None)], "3 + 4 = 7 written as the new desiredSize, maxSize untouched (well under pool_ceiling=10)");
    }

    #[tokio::test]
    async fn provision_live_clamps_before_writing_so_the_api_never_sees_an_out_of_bounds_desired_size() {
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 8, min_size: 1, max_size: 10, status: "ACTIVE".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), false, 10, 10, 1.25, 0.9);
        let receipt = p.provision(50).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::Applied { delta: 2, plan_id: "eks-nodegroup:provision:pool".into() });
        assert_eq!(
            *p.env.update_calls.lock().unwrap(),
            vec![(10u32, None, None)],
            "clamped to the max_size ceiling, never 58 -- maxSize itself untouched since pool_ceiling (10) already equals the current max_size, no room to grow"
        );
    }

    #[tokio::test]
    async fn provision_live_non_active_nodegroup_refuses_to_mutate_and_surfaces_the_error() {
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 3, min_size: 1, max_size: 10, status: "UPDATING".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), false, 10, 10, 1.25, 0.9);
        let err = p.provision(2).await.expect_err("a non-ACTIVE nodegroup must surface, never be silently skipped");
        assert!(matches!(err, ProviderError::ApiTransient(_)));
        assert!(p.env.update_calls.lock().unwrap().is_empty(), "UpdateNodegroupConfig must never be called against a non-ACTIVE nodegroup");
    }

    #[tokio::test]
    async fn provision_live_describe_nodegroup_failure_propagates_and_writes_nothing() {
        let env = MockEnv { state: Err(ProviderError::ApiTransient("mock DescribeNodegroup failure".into())), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), false, 10, 10, 1.25, 0.9);
        let err = p.provision(2).await.expect_err("a DescribeNodegroup failure must surface, never be silently skipped");
        assert!(matches!(err, ProviderError::ApiTransient(_)));
        assert!(p.env.update_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn provision_live_update_failure_propagates_the_error() {
        let env = MockEnv { fail_update: true, ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), false, 10, 10, 1.25, 0.9);
        let err = p.provision(2).await.expect_err("an UpdateNodegroupConfig failure must surface, retried next tick");
        assert!(matches!(err, ProviderError::ApiTransient(_)));
    }

    #[tokio::test]
    async fn deprovision_zero_is_noop_and_never_calls_describe_nodegroup() {
        let env = MockEnv::empty();
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), false, 10, 10, 1.25, 0.9);
        assert_eq!(p.deprovision(0).await.unwrap(), ProvisionReceipt::NoOp);
        assert!(p.env.update_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn deprovision_dry_run_reports_the_clamped_would_mutating_nothing() {
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 5, min_size: 2, max_size: 10, status: "ACTIVE".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), true, 10, 10, 1.25, 0.9);
        let receipt = p.deprovision(2).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::DryRun { would: -2 });
        assert!(p.env.update_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn deprovision_dry_run_clamps_the_would_value_to_the_real_floor() {
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 3, min_size: 2, max_size: 10, status: "ACTIVE".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), true, 10, 10, 1.25, 0.9);
        let receipt = p.deprovision(10).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::DryRun { would: -1 }, "3 -> -7 requested, clamped to 3 -> 2 => would -1");
    }

    #[tokio::test]
    async fn deprovision_live_writes_the_clamped_desired_size() {
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 5, min_size: 2, max_size: 10, status: "ACTIVE".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "camelot-controllers".into(), "camelot-eks".into(), "controllers".into(), false, 10, 10, 1.25, 0.9);
        let receipt = p.deprovision(2).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::Applied { delta: -2, plan_id: "eks-nodegroup:deprovision:camelot-controllers".into() });
        assert_eq!(*p.env.update_calls.lock().unwrap(), vec![(3u32, None, None)], "5 - 2 = 3 written as the new desiredSize, maxSize untouched");
    }

    #[tokio::test]
    async fn deprovision_live_at_floor_reports_noop_and_writes_nothing() {
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 2, min_size: 2, max_size: 10, status: "ACTIVE".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), false, 10, 10, 1.25, 0.9);
        assert_eq!(p.deprovision(3).await.unwrap(), ProvisionReceipt::NoOp);
        assert!(p.env.update_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn deprovision_live_non_active_nodegroup_refuses_to_mutate_and_surfaces_the_error() {
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 5, min_size: 2, max_size: 10, status: "DEGRADED".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), false, 10, 10, 1.25, 0.9);
        let err = p.deprovision(2).await.expect_err("a non-ACTIVE nodegroup must surface, never be silently skipped");
        assert!(matches!(err, ProviderError::ApiTransient(_)));
        assert!(p.env.update_calls.lock().unwrap().is_empty());
    }

    // ── The ceiling/floor itself is now algorithmic — end-to-end through
    //    provision()/deprovision(), not just the pure grown_max_size/
    //    shrunk_max_size functions above. ──────────────────────────────────

    #[tokio::test]
    async fn provision_live_reproduces_and_closes_the_2026_07_19_camelot_stall() {
        // The exact live shape: desiredSize=1, AWS maxSize=2, pool_ceiling=5
        // (BreatheCloudPool's own spec.ceiling) — a pool that used to be
        // PERMANENTLY stuck at 2 nodes no matter how much demand pressed,
        // because the old backend only ever wrote desiredSize and left
        // maxSize as a static Terraform-authored value. Requesting growth
        // past the current max must now carve maxSize itself toward the
        // pool's own declared ceiling, live, no human Terraform-plan-approval
        // needed.
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 2, min_size: 1, max_size: 2, status: "ACTIVE".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "camelot-eks-pool".into(), "camelot-eks".into(), "camelot-eks-controllers".into(), false, 5, 2, 1.25, 0.9);
        let receipt = p.provision(1).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::Applied { delta: 1, plan_id: "eks-nodegroup:provision:camelot-eks-pool".into() });
        assert_eq!(
            *p.env.update_calls.lock().unwrap(),
            vec![(3u32, Some(3), None)],
            "desiredSize 2->3 AND maxSize 2->3 (ceil(2*1.25)=3, within pool_ceiling=5) written in the SAME call"
        );
    }

    #[tokio::test]
    async fn provision_live_grows_max_size_across_several_ticks_toward_ceiling_never_jumping_straight_there() {
        // Sustained demand over 3 ticks: max grows 2 -> 3 -> 4 -> 5, one
        // grow_factor step at a time, exactly mirroring how desiredSize
        // itself already ramps gradually rather than leaping to the ceiling
        // on the first sign of pressure.
        let mut max = 2u32;
        let mut desired = 2u32;
        let ceiling = 5u32;
        for _ in 0..3 {
            let env = MockEnv { state: Ok(NodegroupState { desired_size: desired, min_size: 1, max_size: max, status: "ACTIVE".into() }), ..MockEnv::empty() };
            let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), false, ceiling, 2, 1.25, 0.9);
            let receipt = p.provision(1).await.unwrap();
            let calls = p.env.update_calls.lock().unwrap();
            let (new_desired, new_max, _) = calls[0];
            desired = new_desired;
            if let Some(m) = new_max {
                max = m;
            }
            assert!(receipt != ProvisionReceipt::NoOp, "sustained demand must keep making progress, never silently stall");
        }
        assert_eq!(max, 5, "3 ticks of ceil(*1.25) from 2 reaches exactly the ceiling: 2->3->4->5");
        assert!(desired <= max, "desiredSize must never be written above the (possibly just-grown) maxSize");
    }

    #[tokio::test]
    async fn provision_live_at_pool_ceiling_with_max_already_matching_reports_noop_not_a_ceiling_bump() {
        // max_size already equals pool_ceiling: this IS the genuine governance
        // wall (a human declared 5 as the absolute limit) — must behave
        // exactly like the pre-existing "at ceiling" test, never attempt a
        // ceiling bump past what was explicitly declared.
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 5, min_size: 2, max_size: 5, status: "ACTIVE".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), false, 5, 2, 1.25, 0.9);
        assert_eq!(p.provision(3).await.unwrap(), ProvisionReceipt::NoOp);
        assert!(p.env.update_calls.lock().unwrap().is_empty(), "genuinely at pool_ceiling -- no write, not even a no-op ceiling bump");
    }

    #[tokio::test]
    async fn deprovision_live_shrinks_max_size_back_toward_pool_floor_on_sustained_low_demand() {
        // A pool that grew to max=9 during a burst, now sustained-idle:
        // deprovision must ratchet maxSize back down toward pool_floor too,
        // not just desiredSize -- the "slowly comes back down" half of the
        // breathing behavior, symmetric with the grow side.
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 3, min_size: 2, max_size: 9, status: "ACTIVE".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), false, 9, 2, 1.25, 0.9);
        let receipt = p.deprovision(1).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::Applied { delta: -1, plan_id: "eks-nodegroup:deprovision:pool".into() });
        assert_eq!(
            *p.env.update_calls.lock().unwrap(),
            vec![(2u32, Some(8), None)],
            "desiredSize 3->2 (floored at min_size=2) AND maxSize 9->8 (floor(9*0.9)=8, within pool_floor=2) in the same call"
        );
    }

    #[tokio::test]
    async fn deprovision_live_dry_run_never_writes_even_when_max_size_would_also_shrink() {
        let env = MockEnv { state: Ok(NodegroupState { desired_size: 3, min_size: 2, max_size: 9, status: "ACTIVE".into() }), ..MockEnv::empty() };
        let p = EksNodegroupProvedor::new(env, "pool".into(), "cluster".into(), "nodegroup".into(), true, 9, 2, 1.25, 0.9);
        let receipt = p.deprovision(1).await.unwrap();
        assert_eq!(receipt, ProvisionReceipt::DryRun { would: -1 });
        assert!(p.env.update_calls.lock().unwrap().is_empty(), "shadow must never write, even the maxSize half");
    }
}
