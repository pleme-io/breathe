//! The app-plane generic band reconciler (Step-9/13): drives `reconcile_one` over
//! an [`ActuatorCluster`] for an `AppBand`, whose layout (ConfigFile / ApiCall /
//! Jmx / AppRpc) is carried as DATA on the CR. The actuator backend is selected by
//! the layout's variant tag (`spec.actuator_kind()`) and dispatched by the typed
//! `ActuatorCluster` sum type; `used` is read from the metrics plane via the
//! wrapped `KubeCluster`. The app-plane peer of `kube_param`: one descriptor,
//! data-driven, every config-file / protocol / JMX / admin-RPC vector is a CR.
#![allow(clippy::doc_markdown)]

use std::sync::Arc;

use breathe_actuator::{ActuatorBackend, ActuatorCluster};
use breathe_core::{reconcile_one, ReconcileInput};
use breathe_crd::{AppActuatorKind, AppBand, Band};
use breathe_kube::KubeCluster;
use breathe_provider::{
    ApplySemantics, BandProvider, DimensionDescriptor, DimensionId, Directionality, LimitLayout, MetricSource,
    ResourceProvider, Target,
};
use breathe_runtime::{
    error_status, metrics_for, next_requeue, now_secs, patch_status, rfc3339_in_future, status_for,
    suspended_status, BandLabels,
};
use kube::runtime::controller::Action;
use kube::ResourceExt;
use tracing::{error, info};

use crate::{Ctx, Error};

/// The GENERIC, data-driven app-plane descriptor — layout + metric + carve
/// directionality built from the CR spec (the app-plane peer of
/// `KubeParamDescriptor`). A new app-actuator vector is a CR, not new code.
struct AppParamDescriptor {
    layout: LimitLayout,
    metric: MetricSource,
    dir: Directionality,
}

impl DimensionDescriptor for AppParamDescriptor {
    fn id(&self) -> DimensionId {
        DimensionId::AppParam
    }
    fn directionality(&self) -> Directionality {
        self.dir
    }
    fn field_manager(&self) -> &'static str {
        "breathe/app-param"
    }
    fn logical_field(&self) -> &'static str {
        "app.param"
    }
    fn resource(&self) -> &'static str {
        // app knobs are bare counts (maxmemory bytes, max_connections, …); the value
        // goes through the actuator, not the byte/millicore unit codec.
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

/// Reconcile an `AppBand`: build the data-driven descriptor from its spec, select
/// the actuator backend by the layout tag, wrap it with a metric `KubeCluster`, then
/// drive the SAME `reconcile_one` every band uses.
pub async fn reconcile_app_band(obj: Arc<AppBand>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let ns = obj.namespace().unwrap_or_default();
    let name = obj.name_any();

    if obj.suspended() {
        patch_status::<AppBand>(&ctx.client, &ns, &name, &suspended_status(obj.status())).await?;
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
            patch_status::<AppBand>(&ctx.client, &ns, &name, &error_status(e.to_string())).await?;
            return Ok(Action::requeue(ctx.requeue));
        }
    };

    let in_cooldown = obj
        .last_change_epoch()
        .is_some_and(|last| now_secs().saturating_sub(last) < obj.cooldown_seconds() as i64);

    let descriptor = AppParamDescriptor {
        layout: obj.spec.provider_layout(),
        metric: obj.spec.provider_metric(),
        dir: obj.spec.provider_directionality(),
    };
    // Select the actuator backend by the layout's variant tag (never the command
    // string). write_enabled = !effective_dry_run; apply is ALSO gated by the same
    // value upstream, so a shadowed band never writes regardless. We gate on the
    // FSM-derived `effective_dry_run` (the promotion lifecycle), NOT the raw
    // `dry_run` boolean — so a `dryRun:true` app-band calibrates then auto-promotes
    // like every other plane (permanent shadow needs explicit `mode: shadow`).
    let write_enabled = !obj.effective_dry_run(now_secs());
    let backend = match obj.spec.actuator_kind() {
        AppActuatorKind::ConfigReload => ActuatorBackend::config_reload_default(write_enabled),
        AppActuatorKind::ApiCall => ActuatorBackend::api_call(write_enabled),
        AppActuatorKind::Jmx => ActuatorBackend::jmx(write_enabled),
        AppActuatorKind::AppRpc => ActuatorBackend::app_rpc(write_enabled),
    };
    let metric_cluster = KubeCluster::new(ctx.client.clone(), ctx.prometheus_url.clone());
    let provider = BandProvider::new(ActuatorCluster::new(backend, metric_cluster), descriptor);

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
        dry_run: obj.effective_dry_run(now_secs()),
        policy: obj.disruption_policy(),
        force,
        predictive: None,
        peak_used,
        // app-plane bands carve a bare integer knob — no pod restart ⇒ warmup N/A.
        observed_for_secs: None,
        // app-plane bands carve their own knob directly — no soft/hard-plane split.
        hard_plane_grow_only: false,
    };

    let outcome = reconcile_one(&input, &provider).await;
    let band_ref = breathe_store::BandRef::new(&<AppBand as kube::Resource>::kind(&()), &ns, &name);
    let counters = crate::fold_counters(&ctx, &band_ref, obj.status(), &outcome).await;
    let status = status_for(&outcome, obj.status(), obj.cooldown_seconds(), obj.generation(), counters);
    info!(dim = %provider.id(), band = %name, target = %target.name, phase = ?status.phase, "app-band reconciled");
    metrics_for(
        &BandLabels { dim: provider.id().to_string(), namespace: ns.clone(), name: name.clone() },
        &outcome,
        &cfg,
        status.cooldown_remaining_seconds.unwrap_or(0),
    );
    patch_status::<AppBand>(&ctx.client, &ns, &name, &status).await?;
    Ok(Action::requeue(next_requeue(&outcome.receipt, &ctx.cooldowns)))
}

pub fn error_policy_app_band(_obj: Arc<AppBand>, err: &Error, ctx: Arc<Ctx>) -> Action {
    error!(error = %err, "app-band reconcile error — backing off");
    Action::requeue(ctx.requeue)
}
