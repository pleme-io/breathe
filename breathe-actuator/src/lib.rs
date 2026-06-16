//! `breathe-actuator` — the app-plane `Cluster` that routes an `AppBand` to the
//! actuator its layout names.
//!
//! The four app-plane actuator crates (`breathe-configreload`, `breathe-apicall`,
//! `breathe-jmx`, `breathe-apprpc`) each impl `Cluster` for ONE backend, and three
//! of them (`apicall`/`jmx`/`apprpc`) overload the SAME `LimitLayout::ApiCall` arm —
//! so the layout alone cannot disambiguate them. [`ActuatorCluster`] is the typed
//! sum-type that does the routing, selected by the `AppBand`'s `layout` variant TAG
//! (never sniffed from the command string). It is the app-plane peer of
//! `node_forma`'s `PoolProvedor { KubeObserve | Kwok }` one tier up — the blessed
//! "typed dispatch, no `dyn`, one call site" pattern.
//!
//! The one non-obvious wiring fact: **every actuator's `read_used` is a deliberate
//! typed gap** (used lives on the k8s metrics plane, the limit on the app's own
//! knob). So `ActuatorCluster` holds a metric [`KubeCluster`] and delegates
//! `read_used` to it, while `read_limit`/`apply`/`field_owners` delegate to the
//! selected actuator. A `BandProvider` built over an actuator ALONE is
//! non-functional for observation; this wrapper makes the band whole.

use async_trait::async_trait;
use breathe_apicall::{ApiCallCluster, ProtocolClientEnv};
use breathe_apprpc::{AppRpcCluster, HttpAdminEnv};
use breathe_configreload::{ConfigReloadCluster, FileSignalEnv};
use breathe_jmx::{JmxCluster, JolokiaHttpEnv};
use breathe_kube::KubeCluster;
use breathe_provider::{
    AppliedReceipt, Cluster, FieldOwner, LimitLayout, MetricSource, ProviderError, Sample, SsaPatch, Target,
};

/// Which app-plane actuator services the band — picked by the `AppBand` layout's
/// variant tag. Typed dispatch (a sum type, no `dyn`), mirroring `PoolProvedor`.
pub enum ActuatorBackend {
    /// A config file + reload mechanism (pgbouncer / nginx / PostgreSQL).
    ConfigReload(ConfigReloadCluster<FileSignalEnv>),
    /// A protocol `CONFIG SET` (Redis / Kafka / NATS) — the first-shipped arm,
    /// via the typed `redis` client (no shell).
    ApiCall(ApiCallCluster<ProtocolClientEnv>),
    /// A JVM MBean over Jolokia (HikariCP / Tomcat / Caffeine).
    Jmx(JmxCluster<JolokiaHttpEnv>),
    /// An application admin RPC knob (GOMEMLIMIT / prefetch / max-concurrency).
    AppRpc(AppRpcCluster<HttpAdminEnv>),
}

impl ActuatorBackend {
    /// The Redis (and other `CONFIG SET` protocol) actuator from ambient env, at the
    /// given write posture. `write_enabled = false` ⇒ shadow (apply is a no-op
    /// zero-hash receipt); the band's `dryRun` ALSO gates apply upstream, so a shadow
    /// band never writes regardless.
    #[must_use]
    pub fn api_call(write_enabled: bool) -> Self {
        Self::ApiCall(ApiCallCluster::new(ProtocolClientEnv::new(), write_enabled))
    }
    /// The JMX/Jolokia actuator (defaults `linked=false` → typed `NotLinked` gap
    /// until reachability is verified via `BREATHE_JMX_LINKED=1`).
    #[must_use]
    pub fn jmx(write_enabled: bool) -> Self {
        Self::Jmx(JmxCluster::new(JolokiaHttpEnv::from_env(), write_enabled))
    }
    /// The app-admin-RPC actuator (defaults `linked=false` → typed `NotLinked` gap
    /// until `BREATHE_APPRPC_LINKED=1`).
    #[must_use]
    pub fn app_rpc(write_enabled: bool) -> Self {
        Self::AppRpc(AppRpcCluster::new(HttpAdminEnv::from_env(), write_enabled))
    }
    /// The config-file/reload actuator. `env` carries the pidfile / reload-argv the
    /// `ConfigFile` layout does not (default env ⇒ typed gap until configured).
    #[must_use]
    pub fn config_reload(env: FileSignalEnv, write_enabled: bool) -> Self {
        Self::ConfigReload(ConfigReloadCluster::new(env, write_enabled))
    }
    /// The config-file/reload actuator with the default (unconfigured) env — the
    /// `ConfigFile` layout carries no pidfile/reload-argv yet, so SIGHUP/Reload
    /// return their typed gap until the spec is extended. Lets a controller build
    /// the arm without depending on `breathe-configreload` directly.
    #[must_use]
    pub fn config_reload_default(write_enabled: bool) -> Self {
        Self::ConfigReload(ConfigReloadCluster::new(FileSignalEnv::default(), write_enabled))
    }
}

/// The app-plane `Cluster`. `read_limit`/`apply`/`field_owners` delegate to the
/// selected actuator backend; `read_used` delegates to the metric [`KubeCluster`]
/// (the actuators have no read path). One descriptor + this wrapper makes any
/// app-plane band whole, driven by the SAME `reconcile_one` as every other band.
pub struct ActuatorCluster {
    backend: ActuatorBackend,
    metric: KubeCluster,
}

impl ActuatorCluster {
    /// Pair a selected actuator backend with the metric cluster that answers `used`.
    #[must_use]
    pub fn new(backend: ActuatorBackend, metric: KubeCluster) -> Self {
        Self { backend, metric }
    }
}

#[async_trait]
impl Cluster for ActuatorCluster {
    async fn read_used(&self, source: &MetricSource) -> Result<Sample, ProviderError> {
        // The metrics plane, NOT the actuator — every actuator's read_used is a typed gap.
        self.metric.read_used(source).await
    }

    async fn read_limit(&self, target: &Target, layout: &LimitLayout, resource: &str) -> Result<u64, ProviderError> {
        match &self.backend {
            ActuatorBackend::ConfigReload(c) => c.read_limit(target, layout, resource).await,
            ActuatorBackend::ApiCall(c) => c.read_limit(target, layout, resource).await,
            ActuatorBackend::Jmx(c) => c.read_limit(target, layout, resource).await,
            ActuatorBackend::AppRpc(c) => c.read_limit(target, layout, resource).await,
        }
    }

    async fn field_owners(
        &self,
        target: &Target,
        layout: &LimitLayout,
        resource: &str,
        logical_field: &str,
    ) -> Result<Vec<FieldOwner>, ProviderError> {
        // Every actuator returns an empty owner set (no managedFields on this plane);
        // the single-writer guard always proceeds (only-mitigated, as documented).
        match &self.backend {
            ActuatorBackend::ConfigReload(c) => c.field_owners(target, layout, resource, logical_field).await,
            ActuatorBackend::ApiCall(c) => c.field_owners(target, layout, resource, logical_field).await,
            ActuatorBackend::Jmx(c) => c.field_owners(target, layout, resource, logical_field).await,
            ActuatorBackend::AppRpc(c) => c.field_owners(target, layout, resource, logical_field).await,
        }
    }

    async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError> {
        match &self.backend {
            ActuatorBackend::ConfigReload(c) => c.apply(patch).await,
            ActuatorBackend::ApiCall(c) => c.apply(patch).await,
            ActuatorBackend::Jmx(c) => c.apply(patch).await,
            ActuatorBackend::AppRpc(c) => c.apply(patch).await,
        }
    }
    // read_resize_restart_free: the conservative default (false) — app-plane carves
    // are never PodResize, so a shrink is never claimed restart-free.
}
