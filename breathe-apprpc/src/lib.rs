//! `breathe-apprpc` — the **APP-ADMIN-ENDPOINT** [`Cluster`] implementation (the
//! *hands* for an application's own runtime knobs).
//!
//! Sibling of `breathe-host` (systemd/sysfs) and `breathe-kube` (the k8s API):
//! this actuator carves the levers a *running application* exposes on its admin
//! HTTP endpoint — a Go process's `GOMEMLIMIT` (the soft heap ceiling), a
//! Kafka/RabbitMQ consumer's `prefetch` / `max.poll.records`, an
//! adaptive-concurrency limiter's upper bound. The census of this boundary:
//!
//! | knob | `used` (live signal) | `limit` (the carve) |
//! |---|---|---|
//! | Go `GOMEMLIMIT` | live heap-in-use bytes | the soft heap ceiling bytes |
//! | Kafka/RMQ prefetch | in-flight unacked count | the prefetch / poll bound |
//! | adaptive-concurrency | current in-flight requests | the concurrency upper bound |
//!
//! These are **`RestartFree` by construction** — every knob applies *live* to
//! the running process (the whole point of an admin endpoint), so the band drives
//! them at the fast golden cadence exactly as `HostCluster` drives a live cgroup.
//! That is already encoded in [`LimitLayout::ApiCall`]'s `disruption_class()`
//! (`RestartFree`); this crate adds no new control logic — [`AppRpcCluster`] is
//! just another `Cluster`, so the one generic `breathe_provider::BandProvider`
//! plus the proven `breathe_control::safety_clamp` gate drive app-RPC dimensions
//! exactly as they drive k8s / host ones.
//!
//! ### The one genuinely new thing is the I/O — and it lives behind a trait
//! Every admin-protocol side effect is abstracted behind the [`AppRpcEnv`] trait
//! (the typed-spec-triplet testability seam), so the full decision path is
//! exercised against a mock with zero real network. The real impl is
//! dependency-light — a thin `curl` shell over `std::process::Command` (argv,
//! never a shell string) for the common JSON-admin-endpoint shape; an app whose
//! admin protocol cannot be expressed that way returns a TYPED
//! [`ProviderError::ApiPermanent`] ("apprpc: live admin client not yet linked"),
//! never a panic / `unimplemented!` / `todo!`.
//!
//! ### Addressing — `LimitLayout::ApiCall { endpoint, command }`
//! `endpoint` is the app's admin HTTP base URL (e.g. `http://127.0.0.1:6060`);
//! `command` is the knob name (`gomemlimit`, `prefetch`, `max_concurrency`). The
//! actuator GETs `<endpoint>/<command>` to read the live value and POSTs
//! `<endpoint>/<command>` with the new integer to carve it. A k8s / host layout
//! reaching this actuator is a typed error — it can never legitimately receive one.
//!
//! ### `read_used` is a typed gap, on purpose
//! The app's *live* signal (heap-in-use, unacked count, in-flight requests) is a
//! per-app telemetry shape this actuator does not assume — breathe reads `used`
//! for app-RPC bands from the metrics plane (Prometheus / metrics-server) via the
//! k8s actuator, not from the admin endpoint. So [`AppRpcCluster::read_used`]
//! returns a typed [`ProviderError::ApiPermanent`] pointing the caller at the
//! metric source, never a silent wrong answer.

use async_trait::async_trait;
use breathe_provider::{
    AppliedReceipt, Cluster, FieldOwner, LimitLayout, MetricSource, ProviderError, Sample, SsaPatch,
    Target,
};

// ───────────────────────────── errors ──────────────────────────────

/// Typed app-RPC-I/O error — never a silent wrong answer (TYPED-SPEC discipline).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppRpcError {
    /// The admin endpoint could not be reached / the transport failed.
    Transport(String),
    /// The admin endpoint's response could not be parsed into the expected shape.
    Parse(String),
    /// The admin `curl` invocation exited non-zero.
    Command { argv: String, code: Option<i32>, stderr: String },
    /// This app's admin protocol cannot be driven by the dependency-light client
    /// — a typed gap the caller surfaces, never a panic.
    NotLinked(String),
}

impl std::fmt::Display for AppRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(m) => write!(f, "apprpc transport error: {m}"),
            Self::Parse(m) => write!(f, "apprpc parse error: {m}"),
            Self::Command { argv, code, stderr } => {
                write!(f, "`{argv}` exited {code:?}: {stderr}")
            }
            Self::NotLinked(m) => write!(f, "apprpc: {m}"),
        }
    }
}

impl std::error::Error for AppRpcError {}

impl From<AppRpcError> for ProviderError {
    fn from(e: AppRpcError) -> Self {
        match e {
            // A garbled admin response is "metrics missing" (transient-shaped at
            // the read leg); a transport blip / command failure is transient; a
            // genuinely-unlinked protocol is permanent (it will not fix on retry).
            AppRpcError::Parse(_) => ProviderError::MetricsMissing,
            AppRpcError::NotLinked(m) => ProviderError::ApiPermanent(m),
            AppRpcError::Transport(m) => ProviderError::ApiTransient(m),
            cmd @ AppRpcError::Command { .. } => ProviderError::ApiTransient(cmd.to_string()),
        }
    }
}

// ─────────────────────── the side-effect seam ──────────────────────

/// The app-admin I/O boundary — every real admin-endpoint side effect, behind a
/// trait so the [`AppRpcCluster`] decision path is fully exercised against a mock.
/// `endpoint` is the app's admin HTTP base URL; `knob` is the command/knob name.
pub trait AppRpcEnv: Send + Sync {
    /// Read the live integer value of `knob` from the app at `endpoint`
    /// (`GOMEMLIMIT` bytes, the prefetch bound, the concurrency upper bound).
    fn get_knob(&self, endpoint: &str, knob: &str) -> Result<u64, AppRpcError>;
    /// Set `knob` on the app at `endpoint` to `value` — applies LIVE (RestartFree).
    fn set_knob(&self, endpoint: &str, knob: &str, value: u64) -> Result<(), AppRpcError>;
}

/// The real implementation over a thin `curl` shell (argv, never a shell string).
///
/// The common JSON-admin-endpoint shape: `GET <endpoint>/<knob>` returns the
/// integer value (the body trimmed + parsed); `POST <endpoint>/<knob>` with the
/// integer body sets it. An app whose admin protocol is not this shape is served
/// by a future typed client — until then [`get_knob`](Self::get_knob) /
/// [`set_knob`](Self::set_knob) return a TYPED [`AppRpcError::NotLinked`], never a
/// panic. `curl_bin` defaults to `curl` and is overridable for a pinned path.
#[derive(Debug, Clone)]
pub struct CurlAdminEnv {
    curl_bin: String,
    /// When true, the admin protocol is assumed to be the dependency-light
    /// JSON-over-`curl` shape and reads/writes go over the wire. When false (the
    /// safe default for an unknown app), every method returns the typed
    /// `NotLinked` gap so a mis-wired band can never silently no-op a real carve.
    linked: bool,
}

impl Default for CurlAdminEnv {
    fn default() -> Self {
        Self { curl_bin: "curl".into(), linked: false }
    }
}

impl CurlAdminEnv {
    /// Read the admin-client config from the environment (the DaemonSet / pod sets
    /// these): `BREATHE_CURL_BIN` (the curl path), `BREATHE_APPRPC_LINKED=1` (opt
    /// the app into the dependency-light JSON-over-curl client).
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            curl_bin: std::env::var("BREATHE_CURL_BIN").unwrap_or_else(|_| "curl".into()),
            linked: std::env::var("BREATHE_APPRPC_LINKED").ok().as_deref() == Some("1"),
        }
    }
    /// Opt this env into the dependency-light JSON-over-`curl` admin client (the
    /// app speaks the `GET/POST <endpoint>/<knob>` integer shape).
    #[must_use]
    pub fn linked(mut self) -> Self {
        self.linked = true;
        self
    }

    /// The admin URL for a `(endpoint, knob)` — `<endpoint>/<knob>` with at most
    /// one joining slash. Pure + testable (no `format!` of protocol syntax beyond
    /// a path join; argv is a typed vector at the call site).
    #[must_use]
    pub fn knob_url(endpoint: &str, knob: &str) -> String {
        let base = endpoint.trim_end_matches('/');
        let mut u = String::with_capacity(base.len() + 1 + knob.len());
        u.push_str(base);
        u.push('/');
        u.push_str(knob.trim_start_matches('/'));
        u
    }

    /// Run `curl <args…>` and return trimmed stdout. argv is a typed `&[&str]` —
    /// never a shell string (`std::process::Command` exec, no shell).
    fn curl(&self, args: &[&str]) -> Result<String, AppRpcError> {
        let out = std::process::Command::new(&self.curl_bin)
            .args(args)
            .output()
            .map_err(|e| AppRpcError::Transport(e.to_string()))?;
        if !out.status.success() {
            return Err(AppRpcError::Command {
                argv: format!("{} {}", self.curl_bin, args.join(" ")),
                code: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
            });
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }
}

impl AppRpcEnv for CurlAdminEnv {
    fn get_knob(&self, endpoint: &str, knob: &str) -> Result<u64, AppRpcError> {
        if !self.linked {
            return Err(AppRpcError::NotLinked(
                "live admin client not yet linked".into(),
            ));
        }
        let url = Self::knob_url(endpoint, knob);
        // `curl -fsS <url>` — fail on HTTP error, silent, show errors. The body is
        // the integer value of the knob.
        let body = self.curl(&["-fsS", &url])?;
        body.parse::<u64>()
            .map_err(|e| AppRpcError::Parse(format!("{knob}={body:?}: {e}")))
    }

    fn set_knob(&self, endpoint: &str, knob: &str, value: u64) -> Result<(), AppRpcError> {
        if !self.linked {
            return Err(AppRpcError::NotLinked(
                "live admin client not yet linked".into(),
            ));
        }
        let url = Self::knob_url(endpoint, knob);
        // `curl -fsS -X POST --data <value> <url>` — the integer body sets the
        // knob live. `--data` value + the URL are typed argv tokens.
        let data = value.to_string();
        self.curl(&["-fsS", "-X", "POST", "--data", &data, &url]).map(|_| ())
    }
}

// ─────────────────────────── AppRpcCluster ──────────────────────────

/// The app-admin-endpoint `Cluster`. `write_enabled = false` is the SHADOW mode:
/// it reads + decides + reports `appliedValue` but performs no admin-endpoint
/// mutation, so the full loop can be observed on a live app before a single knob
/// is carved — the same safety gate `HostCluster` ships.
pub struct AppRpcCluster<E: AppRpcEnv> {
    env: E,
    write_enabled: bool,
}

impl<E: AppRpcEnv> AppRpcCluster<E> {
    pub fn new(env: E, write_enabled: bool) -> Self {
        Self { env, write_enabled }
    }
    /// SHADOW constructor — reads + decides, never carves the live app.
    pub fn shadow(env: E) -> Self {
        Self::new(env, false)
    }
    pub fn env(&self) -> &E {
        &self.env
    }
    pub fn writes_enabled(&self) -> bool {
        self.write_enabled
    }
}

#[async_trait]
impl<E: AppRpcEnv> Cluster for AppRpcCluster<E> {
    async fn read_used(&self, _source: &MetricSource) -> Result<Sample, ProviderError> {
        // The app's live `used` signal (Go heap-in-use, consumer unacked count,
        // in-flight request count) is per-app telemetry breathe reads from the
        // METRICS plane (Prometheus / metrics-server) via the k8s actuator — the
        // admin endpoint is a control surface, not a metrics surface. So this
        // actuator never invents a `used`: it returns a typed permanent error
        // routing the caller to the metric source, never a silent wrong answer.
        Err(ProviderError::ApiPermanent(
            "app-RPC `used` is read from the metrics plane (route the metric source to the k8s/metrics actuator, not the admin endpoint)".into(),
        ))
    }

    async fn read_limit(
        &self,
        _target: &Target,
        layout: &LimitLayout,
        _resource: &str,
    ) -> Result<u64, ProviderError> {
        match layout {
            LimitLayout::ApiCall { endpoint, command } => {
                Ok(self.env.get_knob(endpoint, command)?)
            }
            // a k8s / host / config-file layout can never legitimately reach the
            // app-RPC boundary — typed, never silent.
            _ => Err(ProviderError::ApiPermanent(
                "non-ApiCall layout on AppRpcCluster (route k8s/host dimensions to their actuator)".into(),
            )),
        }
    }

    async fn field_owners(
        &self,
        _target: &Target,
        _layout: &LimitLayout,
        _resource: &str,
        _logical_field: &str,
    ) -> Result<Vec<FieldOwner>, ProviderError> {
        // App-RPC knobs have no Kubernetes managedFields and no competing writer:
        // the live `GOMEMLIMIT` / prefetch / concurrency bound is breathe-only on
        // the admin endpoint. An empty owner set ⇒ the single-writer guard always
        // proceeds, never a phantom Conflict.
        Ok(Vec::new())
    }

    async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError> {
        let LimitLayout::ApiCall { endpoint, command } = &patch.layout else {
            return Err(ProviderError::ApiPermanent(
                "non-ApiCall layout on AppRpcCluster apply (route k8s/host dimensions to their actuator)".into(),
            ));
        };
        // SHADOW: decide + report, never carve the live app.
        if !self.write_enabled {
            return Ok(AppliedReceipt { source_hash: [0u8; 16] });
        }
        self.env.set_knob(endpoint, command, patch.value)?;
        Ok(AppliedReceipt { source_hash: [0u8; 16] })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    const MI: u64 = 1024 * 1024;

    /// A programmable in-memory [`AppRpcEnv`] — the testability seam. Holds canned
    /// knob values keyed by `(endpoint, knob)` and records every write so a test
    /// can assert shadow mode carved nothing / live mode carved exactly the value.
    #[derive(Default)]
    struct MockAppRpcEnv {
        knobs: Mutex<BTreeMap<(String, String), u64>>,
        writes: Mutex<Vec<(String, String, u64)>>,
    }

    impl MockAppRpcEnv {
        fn with_knob(self, endpoint: &str, knob: &str, value: u64) -> Self {
            self.knobs
                .lock()
                .unwrap()
                .insert((endpoint.to_string(), knob.to_string()), value);
            self
        }
        fn writes(&self) -> Vec<(String, String, u64)> {
            self.writes.lock().unwrap().clone()
        }
    }

    impl AppRpcEnv for MockAppRpcEnv {
        fn get_knob(&self, endpoint: &str, knob: &str) -> Result<u64, AppRpcError> {
            self.knobs
                .lock()
                .unwrap()
                .get(&(endpoint.to_string(), knob.to_string()))
                .copied()
                .ok_or_else(|| AppRpcError::Parse("no such knob".into()))
        }
        fn set_knob(&self, endpoint: &str, knob: &str, value: u64) -> Result<(), AppRpcError> {
            self.knobs
                .lock()
                .unwrap()
                .insert((endpoint.to_string(), knob.to_string()), value);
            self.writes
                .lock()
                .unwrap()
                .push((endpoint.to_string(), knob.to_string(), value));
            Ok(())
        }
    }

    fn app_target() -> Target {
        Target {
            namespace: "default".into(),
            name: "go-consumer".into(),
            kind: "Deployment".into(),
            api_version: "apps/v1".into(),
            container: None,
            pod_selector: None,
        }
    }

    const ENDPOINT: &str = "http://127.0.0.1:6060";

    fn gomemlimit_layout() -> LimitLayout {
        LimitLayout::ApiCall { endpoint: ENDPOINT.into(), command: "gomemlimit".into() }
    }

    /// READ: read_limit pulls the live knob through `get_knob`.
    #[tokio::test]
    async fn read_limit_reads_the_live_gomemlimit_through_get_knob() {
        let env = MockAppRpcEnv::default().with_knob(ENDPOINT, "gomemlimit", 512 * MI);
        let cluster = AppRpcCluster::shadow(env);
        let v = cluster
            .read_limit(&app_target(), &gomemlimit_layout(), "memory")
            .await
            .unwrap();
        assert_eq!(v, 512 * MI, "read_limit returns the app's live GOMEMLIMIT");
    }

    /// APPLY round-trip: a write-enabled apply carves the knob via `set_knob`,
    /// then read_limit observes the new value (the prefetch census case).
    #[tokio::test]
    async fn apply_round_trips_a_prefetch_carve_through_set_knob() {
        let env = MockAppRpcEnv::default().with_knob(ENDPOINT, "prefetch", 100);
        let layout = LimitLayout::ApiCall { endpoint: ENDPOINT.into(), command: "prefetch".into() };
        let cluster = AppRpcCluster::new(env, true); // write-enabled
        let patch = SsaPatch {
            target: app_target(),
            field_manager: "breathe/apprpc".into(),
            layout: layout.clone(),
            resource: "memory".into(),
            value: 250,
        };
        cluster.apply(&patch).await.unwrap();
        // the carve was recorded …
        assert_eq!(
            cluster.env().writes(),
            vec![(ENDPOINT.to_string(), "prefetch".to_string(), 250)],
            "live apply carves exactly the patch value via set_knob"
        );
        // … and a subsequent read observes the new live value (round-trip).
        let v = cluster.read_limit(&app_target(), &layout, "memory").await.unwrap();
        assert_eq!(v, 250, "read after carve sees the new prefetch bound");
    }

    /// SHADOW: apply decides but carves nothing on the live app.
    #[tokio::test]
    async fn shadow_mode_decides_but_carves_nothing() {
        let env = MockAppRpcEnv::default().with_knob(ENDPOINT, "max_concurrency", 64);
        let layout =
            LimitLayout::ApiCall { endpoint: ENDPOINT.into(), command: "max_concurrency".into() };
        let cluster = AppRpcCluster::shadow(env);
        let patch = SsaPatch {
            target: app_target(),
            field_manager: "breathe/apprpc".into(),
            layout,
            resource: "cpu".into(),
            value: 128,
        };
        cluster.apply(&patch).await.unwrap();
        assert!(cluster.env().writes().is_empty(), "shadow mode must not carve the app");
    }

    /// A wrong (non-ApiCall) layout is a TYPED error at both read_limit and apply,
    /// and carves nothing — never a silent mis-route.
    #[tokio::test]
    async fn a_wrong_layout_is_a_typed_error_and_carves_nothing() {
        let env = MockAppRpcEnv::default();
        let cluster = AppRpcCluster::new(env, true);
        // read_limit with a k8s layout → typed permanent error.
        let read_err = cluster
            .read_limit(&app_target(), &LimitLayout::PodResize { container: None }, "memory")
            .await
            .unwrap_err();
        assert!(matches!(read_err, ProviderError::ApiPermanent(_)));
        // apply with a k8s layout → typed permanent error, nothing written.
        let patch = SsaPatch {
            target: app_target(),
            field_manager: "breathe/apprpc".into(),
            layout: LimitLayout::PvcRequest,
            resource: "storage".into(),
            value: 1,
        };
        let apply_err = cluster.apply(&patch).await.unwrap_err();
        assert!(matches!(apply_err, ProviderError::ApiPermanent(_)));
        assert!(cluster.env().writes().is_empty(), "a mis-routed apply touches nothing");
    }

    /// `read_used` on the app-RPC boundary is a typed gap (the live signal comes
    /// from the metrics plane), never a silent wrong answer.
    #[tokio::test]
    async fn read_used_is_a_typed_gap_routing_to_the_metrics_plane() {
        let cluster = AppRpcCluster::shadow(MockAppRpcEnv::default());
        let err = cluster
            .read_used(&MetricSource::Prometheus("go_memstats_heap_inuse_bytes".into()))
            .await
            .unwrap_err();
        assert!(matches!(err, ProviderError::ApiPermanent(_)));
    }

    /// field_owners is empty (app-RPC knobs have no k8s managedFields), so the
    /// single-writer guard always proceeds.
    #[tokio::test]
    async fn field_owners_is_empty_for_app_rpc_knobs() {
        let cluster = AppRpcCluster::shadow(MockAppRpcEnv::default());
        let owners = cluster
            .field_owners(&app_target(), &gomemlimit_layout(), "memory", "app.rpc.gomemlimit")
            .await
            .unwrap();
        assert!(owners.is_empty(), "app-RPC levers have no competing managedFields owner");
    }

    /// The dependency-light real env is honest about its gap: an UNLINKED
    /// `CurlAdminEnv` returns the typed `NotLinked` → `ApiPermanent`, never a panic.
    #[test]
    fn unlinked_curl_env_returns_the_typed_not_linked_gap() {
        let env = CurlAdminEnv::default(); // not linked
        let err = env.get_knob(ENDPOINT, "gomemlimit").unwrap_err();
        assert!(matches!(err, AppRpcError::NotLinked(_)));
        // and it maps to the agreed typed ProviderError surface.
        assert!(matches!(ProviderError::from(err), ProviderError::ApiPermanent(_)));
    }

    /// The admin URL builder joins endpoint + knob with exactly one slash.
    #[test]
    fn knob_url_joins_with_one_slash() {
        assert_eq!(CurlAdminEnv::knob_url("http://h:6060", "gomemlimit"), "http://h:6060/gomemlimit");
        assert_eq!(CurlAdminEnv::knob_url("http://h:6060/", "/prefetch"), "http://h:6060/prefetch");
    }
}
