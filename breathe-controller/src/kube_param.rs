//! The k8s-plane generic band reconciler (Step-6/8/12): drives `reconcile_one`
//! over `KubeCluster` for a `KubeParamBand`, whose layout (CrField /
//! DestinationRuleField / NamespaceEnvelope / ControllerSetpoint) is carried as
//! DATA on the CR and written via KubeCluster's generic CR-path SSA. The k8s
//! peer of `node_forma`/`HostParamBand`: one descriptor, data-driven, every
//! Istio/ResourceQuota/CR-field vector is a CR instance.
#![allow(clippy::doc_markdown)]

use std::sync::Arc;

use breathe_core::{reconcile_one, ReconcileInput};
use breathe_crd::{Band, KubeParamBand};
use breathe_provider::{
    ApplySemantics, BandProvider, DimensionDescriptor, DimensionId, Directionality, LimitLayout, MetricSource,
    ResourceProvider, Target,
};
use breathe_kube::KubeCluster;
use breathe_runtime::{
    error_status, metrics_for, next_requeue, now_secs, patch_status, rfc3339_in_future, status_for,
    suspended_status, BandLabels,
};
use kube::runtime::controller::Action;
use kube::ResourceExt;
use tracing::{error, info};

use crate::{Ctx, Error};

/// The GENERIC, data-driven k8s-plane descriptor — the layout + metric + carve
/// directionality are built from the CR spec (the abstracting peer of
/// `HostParamDescriptor`). A new k8s-CR / app vector is a CR, not new code.
struct KubeParamDescriptor {
    layout: LimitLayout,
    metric: MetricSource,
    dir: Directionality,
}

impl DimensionDescriptor for KubeParamDescriptor {
    fn id(&self) -> DimensionId {
        DimensionId::KubeParam
    }
    fn directionality(&self) -> Directionality {
        self.dir
    }
    fn field_manager(&self) -> &'static str {
        "breathe/kube-param"
    }
    fn logical_field(&self) -> &'static str {
        "kube.param"
    }
    fn resource(&self) -> &'static str {
        // k8s-CR fields are bare counts; the value goes through KubeCluster's
        // generic CR-path (json number), not the byte/millicore unit codec.
        "count"
    }
    fn semantics(&self) -> ApplySemantics {
        ApplySemantics::ContinuousReconciliation
    }
    fn layout(&self, _target: &Target) -> LimitLayout {
        self.layout.clone()
    }
    fn metric_source(&self, _target: &Target) -> MetricSource {
        self.metric.clone()
    }
}

/// Reconcile a `KubeParamBand`: build the data-driven descriptor from its spec,
/// then drive the SAME `reconcile_one` the memory/cpu bands use, over the SAME
/// `KubeCluster` (which writes the layout via generic CR-path SSA).
pub async fn reconcile_kube_param(obj: Arc<KubeParamBand>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let ns = obj.namespace().unwrap_or_default();
    let name = obj.name_any();

    if obj.suspended() {
        patch_status::<KubeParamBand>(&ctx.client, &ns, &name, &suspended_status(obj.status())).await?;
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

    let cfg = match obj.band_config() {
        Ok(c) => c,
        Err(e) => {
            patch_status::<KubeParamBand>(&ctx.client, &ns, &name, &error_status(e.to_string())).await?;
            return Ok(Action::requeue(ctx.requeue));
        }
    };

    let in_cooldown = obj
        .last_change_epoch()
        .is_some_and(|last| now_secs().saturating_sub(last) < obj.cooldown_seconds() as i64);

    let descriptor = KubeParamDescriptor {
        layout: obj.spec.provider_layout(),
        metric: obj.spec.provider_metric(),
        dir: obj.spec.provider_directionality(),
    };
    let provider = BandProvider::new(KubeCluster::new(ctx.client.clone(), ctx.prometheus_url.clone()), descriptor);

    let force = obj.force_limit_value().filter(|_| obj.force_limit_expiry().map_or(true, rfc3339_in_future));
    // NEVER-OOM-FROM-CARVE: carry the decayed trailing-window peak forward (see
    // the main controller); `reconcile_one` folds in the current `used`.
    let peak_used = obj
        .status()
        .and_then(|s| s.observed_peak_used.or(s.observed_used))
        .and_then(|p| u64::try_from(p).ok())
        .map(|prior_peak| ((prior_peak as f64) * obj.peak_decay().clamp(0.0, 0.999)) as u64);
    let input = ReconcileInput {
        target: &target,
        cfg: &cfg,
        max_staleness_secs: obj.max_staleness_seconds(),
        in_cooldown,
        dry_run: obj.dry_run(),
        policy: obj.disruption_policy(),
        force,
        predictive: None,
        peak_used,
    };

    let outcome = reconcile_one(&input, &provider).await;
    let status = status_for(&outcome, obj.status(), obj.cooldown_seconds(), obj.generation());
    info!(dim = %provider.id(), band = %name, target = %target.name, phase = ?status.phase, "kube-param reconciled");
    metrics_for(
        &BandLabels { dim: provider.id().to_string(), namespace: ns.clone(), name: name.clone() },
        &outcome,
        &cfg,
        status.cooldown_remaining_seconds.unwrap_or(0),
    );
    patch_status::<KubeParamBand>(&ctx.client, &ns, &name, &status).await?;
    Ok(Action::requeue(next_requeue(&outcome.receipt, &ctx.cooldowns)))
}

pub fn error_policy_kube_param(_obj: Arc<KubeParamBand>, err: &Error, ctx: Arc<Ctx>) -> Action {
    error!(error = %err, "kube-param reconcile error — backing off");
    Action::requeue(ctx.requeue)
}
