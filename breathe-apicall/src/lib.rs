//! `breathe-apicall` — the **PROTOCOL-API** [`Cluster`] implementation (the
//! *hands* for a live data-system parameter).
//!
//! breathe's k8s boundary is `breathe-kube::KubeCluster`; its host peer is
//! `breathe-host::HostCluster`; this is the third actuator — the one that carves
//! a parameter exposed only over a service's own protocol API: **Redis `CONFIG
//! SET maxmemory`** (RestartFree — the value applies live), a **Kafka** config
//! alter, a **NATS JetStream** edit. It owns the [`LimitLayout::ApiCall`] arm.
//!
//! The compounding claim holds unchanged: there is no new control logic here.
//! [`ApiCallCluster`] is just another `Cluster`, so the *one* generic
//! `breathe_provider::BandProvider` + the proven `breathe_control::safety_clamp`
//! gate drive an api-call dimension exactly as they drive k8s + host ones. The
//! only genuinely new thing is the protocol *I/O*, abstracted behind the async
//! [`ApiCallEnv`] trait — the typed-spec-triplet testability seam, so every
//! decision is exercised against a mock with zero real I/O.
//!
//! ### Typed clients, never a shell
//! [`ProtocolClientEnv`] talks the protocol through a **typed Rust client** (the
//! `redis` crate's async connection), NOT by shelling out to `redis-cli` — the
//! stack-only / no-shell law: the actuator stays a self-contained Rust binary, no
//! CLI in the image. Kafka + NATS return a typed [`ApiCallError::NoClient`] gap
//! until their typed clients are linked — a mechanically-visible TODO, never a
//! panic, never a silent wrong answer.

use async_trait::async_trait;
use breathe_provider::{
    ApplySemantics, AppliedReceipt, Cluster, DimensionDescriptor, DimensionId, Directionality,
    FieldOwner, LimitLayout, MetricSource, ProviderError, Sample, SsaPatch, Target,
};

// ───────────────────────────── errors ──────────────────────────────

/// Typed protocol-I/O error — never a silent wrong answer (TYPED-SPEC discipline).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiCallError {
    /// A connection / command round-trip to the protocol endpoint failed (transient).
    Io(String),
    /// The endpoint returned a value that could not be parsed into the expected scalar.
    Parse(String),
    /// No typed client is linked for the addressed protocol — a typed gap (the
    /// surface is named so a consumer sees it mechanically), never silent.
    NoClient(String),
    /// The `command` field of the `ApiCall` layout could not be understood (e.g.
    /// it names no recognized protocol verb) — permanent; retry won't fix it.
    BadCommand(String),
}

impl std::fmt::Display for ApiCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(m) => write!(f, "apicall io error: {m}"),
            Self::Parse(m) => write!(f, "apicall parse error: {m}"),
            Self::NoClient(p) => write!(f, "apicall: live {p} client not yet linked"),
            Self::BadCommand(c) => write!(f, "apicall: unrecognized command {c:?}"),
        }
    }
}

impl std::error::Error for ApiCallError {}

impl From<ApiCallError> for ProviderError {
    fn from(e: ApiCallError) -> Self {
        match e {
            // A garbled value is "metrics missing"; a connection/command failure is
            // transient (the endpoint may recover); a missing client or a malformed
            // command is permanent (it will not fix itself on retry).
            ApiCallError::Parse(_) => ProviderError::MetricsMissing,
            ApiCallError::Io(m) => ProviderError::ApiTransient(m),
            ApiCallError::NoClient(_) | ApiCallError::BadCommand(_) => {
                ProviderError::ApiPermanent(e.to_string())
            }
        }
    }
}

// ─────────────────────── the side-effect seam ──────────────────────

/// The protocol-API I/O boundary — every real protocol side effect, behind an
/// async trait so the [`ApiCallCluster`] decision path is fully exercised against
/// a mock with zero real I/O.
///
/// `endpoint` is the connection coordinate (e.g. `redis://cache.svc:6379`);
/// `command` is the layout's verb string (e.g. `maxmemory`, `CONFIG SET
/// maxmemory`). The impl parses `command` to pick the protocol + parameter and
/// issues the typed client call. The two verbs mirror a band's two needs: read the
/// current value, write a new one. A protocol with no linked typed client returns
/// [`ApiCallError::NoClient`] — a typed gap, never a panic.
#[async_trait]
pub trait ApiCallEnv: Send + Sync {
    /// Read the current value of the parameter named by `command` at `endpoint`,
    /// as a bare scalar (bytes for `maxmemory`, count for a stream limit, …).
    async fn get_config(&self, endpoint: &str, command: &str) -> Result<u64, ApiCallError>;
    /// Write `value` to the parameter named by `command` at `endpoint`.
    async fn set_config(&self, endpoint: &str, command: &str, value: u64) -> Result<(), ApiCallError>;
}

/// Which protocol a `command` string addresses — the typed dispatch the real
/// [`ProtocolClientEnv`] keys its client call off. Parsed once from the layout's
/// `command`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// Redis — the typed `redis` crate (`CONFIG {GET,SET} <param>`).
    Redis,
    /// Kafka — typed client not yet linked.
    Kafka,
    /// NATS JetStream — typed client not yet linked.
    Nats,
}

impl Protocol {
    /// The protocol name used in a [`ApiCallError::NoClient`] gap message.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Redis => "redis",
            Self::Kafka => "kafka",
            Self::Nats => "nats",
        }
    }
}

/// Classify a `command` string into `(protocol, parameter)`. Pure + testable — no
/// I/O. Recognizes a leading protocol hint (`CONFIG SET maxmemory`, `nats …`,
/// `kafka …`) and falls back to bare-Redis-parameter (`maxmemory`), the canonical
/// census case (Redis `CONFIG SET maxmemory`, RestartFree).
///
/// # Errors
/// [`ApiCallError::BadCommand`] when `command` is empty or names no parameter.
pub fn classify_command(command: &str) -> Result<(Protocol, String), ApiCallError> {
    let trimmed = command.trim();
    if trimmed.is_empty() {
        return Err(ApiCallError::BadCommand(command.to_string()));
    }
    let mut tokens = trimmed.split_whitespace();
    let head = tokens.next().unwrap_or_default();
    let lower = head.to_ascii_lowercase();
    let (protocol, param) = match lower.as_str() {
        // `CONFIG SET maxmemory` / `CONFIG GET maxmemory` → Redis, param = last token.
        "config" => {
            let param = trimmed
                .split_whitespace()
                .last()
                .filter(|p| !p.eq_ignore_ascii_case("get") && !p.eq_ignore_ascii_case("set"))
                .ok_or_else(|| ApiCallError::BadCommand(command.to_string()))?;
            (Protocol::Redis, param.to_string())
        }
        "kafka" | "kafka-configs" => {
            let param = tokens.last().unwrap_or(head);
            (Protocol::Kafka, param.to_string())
        }
        "nats" => {
            let param = tokens.last().unwrap_or(head);
            (Protocol::Nats, param.to_string())
        }
        // Bare parameter — the canonical Redis case (`maxmemory`).
        _ => (Protocol::Redis, head.to_string()),
    };
    Ok((protocol, param))
}

/// The real implementation over TYPED Rust protocol clients — no shell, no CLI.
/// Redis is wired through the `redis` crate's async connection; Kafka + NATS return
/// a typed [`ApiCallError::NoClient`] gap until their typed clients are linked.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProtocolClientEnv;

impl ProtocolClientEnv {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Open a multiplexed async Redis connection from a `redis://[:pass@]host:port`
    /// URL (auth, if any, is carried by the URL — the `redis` crate parses it).
    async fn redis_conn(endpoint: &str) -> Result<redis::aio::MultiplexedConnection, ApiCallError> {
        let client = redis::Client::open(endpoint).map_err(|e| ApiCallError::Io(e.to_string()))?;
        client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| ApiCallError::Io(e.to_string()))
    }
}

#[async_trait]
impl ApiCallEnv for ProtocolClientEnv {
    async fn get_config(&self, endpoint: &str, command: &str) -> Result<u64, ApiCallError> {
        let (protocol, param) = classify_command(command)?;
        match protocol {
            Protocol::Redis => {
                let mut con = Self::redis_conn(endpoint).await?;
                // CONFIG GET <param> → ["<param>", "<value>"] (a bulk-string array).
                let reply: Vec<String> = redis::cmd("CONFIG")
                    .arg("GET")
                    .arg(&param)
                    .query_async(&mut con)
                    .await
                    .map_err(|e| ApiCallError::Io(e.to_string()))?;
                let raw = reply
                    .get(1)
                    .ok_or_else(|| ApiCallError::Parse(format!("empty CONFIG GET {param} reply")))?;
                raw.parse::<u64>().map_err(|e| ApiCallError::Parse(format!("{raw:?}: {e}")))
            }
            Protocol::Kafka => Err(ApiCallError::NoClient(Protocol::Kafka.as_str().into())),
            Protocol::Nats => Err(ApiCallError::NoClient(Protocol::Nats.as_str().into())),
        }
    }

    async fn set_config(&self, endpoint: &str, command: &str, value: u64) -> Result<(), ApiCallError> {
        let (protocol, param) = classify_command(command)?;
        match protocol {
            Protocol::Redis => {
                let mut con = Self::redis_conn(endpoint).await?;
                redis::cmd("CONFIG")
                    .arg("SET")
                    .arg(&param)
                    .arg(value)
                    .query_async::<()>(&mut con)
                    .await
                    .map_err(|e| ApiCallError::Io(e.to_string()))?;
                Ok(())
            }
            Protocol::Kafka => Err(ApiCallError::NoClient(Protocol::Kafka.as_str().into())),
            Protocol::Nats => Err(ApiCallError::NoClient(Protocol::Nats.as_str().into())),
        }
    }
}

// ─────────────────────────── ApiCallCluster ────────────────────────

/// The protocol-API `Cluster`. `write_enabled = false` is the SHADOW mode: it
/// reads + decides + reports `appliedValue` but performs no protocol mutation, so
/// the full loop can be observed against a live data system before a single
/// `CONFIG SET`. Mirrors `HostCluster`'s shadow gate exactly.
pub struct ApiCallCluster<E: ApiCallEnv> {
    env: E,
    write_enabled: bool,
}

impl<E: ApiCallEnv> ApiCallCluster<E> {
    pub fn new(env: E, write_enabled: bool) -> Self {
        Self { env, write_enabled }
    }
    /// SHADOW constructor — reads + decides, never writes.
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
impl<E: ApiCallEnv> Cluster for ApiCallCluster<E> {
    async fn read_used(&self, _source: &MetricSource) -> Result<Sample, ProviderError> {
        // The protocol API exposes the *limit*, not the working set: an `ApiCall`
        // band reads its `used` from the k8s metrics plane (a `PodMetricsMax`
        // source routed to `KubeCluster`). Reaching the api-call boundary for a
        // `used` read is therefore a band-wiring error — typed, never silent.
        Err(ProviderError::ApiPermanent(
            "ApiCallCluster has no `used` source — route the band's metric_source to the k8s plane (PodMetricsMax)".into(),
        ))
    }

    async fn read_limit(
        &self,
        _target: &Target,
        layout: &LimitLayout,
        _resource: &str,
    ) -> Result<u64, ProviderError> {
        let LimitLayout::ApiCall { endpoint, command } = layout else {
            return Err(ProviderError::ApiPermanent(
                "non-ApiCall layout on ApiCallCluster (route k8s/host dimensions to their own Cluster)".into(),
            ));
        };
        Ok(self.env.get_config(endpoint, command).await?)
    }

    async fn field_owners(
        &self,
        _target: &Target,
        _layout: &LimitLayout,
        _resource: &str,
        _logical_field: &str,
    ) -> Result<Vec<FieldOwner>, ProviderError> {
        // A protocol-API parameter has no Kubernetes managedFields and no competing
        // SSA writer: breathe is the only writer of the data system's `maxmemory`.
        // An empty owner set ⇒ the single-writer guard always proceeds, never a
        // phantom Conflict (mirrors HostCluster::field_owners).
        Ok(Vec::new())
    }

    async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError> {
        let LimitLayout::ApiCall { endpoint, command } = &patch.layout else {
            return Err(ProviderError::ApiPermanent(
                "non-ApiCall layout on ApiCallCluster apply (route k8s/host dimensions to their own Cluster)".into(),
            ));
        };
        // SHADOW: decide + report, never mutate the data system.
        if !self.write_enabled {
            return Ok(AppliedReceipt { source_hash: [0u8; 16] });
        }
        self.env.set_config(endpoint, command, patch.value).await?;
        Ok(AppliedReceipt { source_hash: [0u8; 16] })
    }
}

// ─────────────────────── the api-call descriptor ───────────────────

/// **ApiCallParam** — the GENERIC, data-driven protocol-parameter descriptor: ONE
/// descriptor carries its `endpoint` + `command` as DATA, so every data-system
/// parameter band (Redis `maxmemory`, a NATS stream `max-bytes`, a Kafka topic
/// retention) is an *instance*, not new code. The `used` is read from the k8s
/// metrics plane (`metric_source` here is a placeholder the controller overrides
/// with the band's real `PodMetricsMax` source), so the descriptor's job is the
/// *limit* layout. RestartFree — a `CONFIG SET maxmemory` applies live with no
/// process restart, so the band ticks at the fast golden cadence.
pub struct ApiCallParamDescriptor {
    pub endpoint: String,
    pub command: String,
    pub dir: Directionality,
}

impl ApiCallParamDescriptor {
    /// A bidirectional Redis `maxmemory` band by endpoint (the census keystone).
    #[must_use]
    pub fn redis_maxmemory(endpoint: impl Into<String>, dir: Directionality) -> Self {
        Self { endpoint: endpoint.into(), command: "maxmemory".into(), dir }
    }
    /// A generic api-call band by explicit endpoint + command verb.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, command: impl Into<String>, dir: Directionality) -> Self {
        Self { endpoint: endpoint.into(), command: command.into(), dir }
    }
}

impl DimensionDescriptor for ApiCallParamDescriptor {
    fn id(&self) -> DimensionId {
        // The api-call boundary carves a memory-shaped quantity (maxmemory) — it
        // reuses the Memory id; the LAYOUT (ApiCall) is what routes it here.
        DimensionId::Memory
    }
    fn directionality(&self) -> Directionality {
        self.dir
    }
    fn field_manager(&self) -> &'static str {
        // No k8s managedFields on this boundary + each band writes a DISTINCT
        // protocol parameter, so a shared manager never creates contention
        // (field_owners is empty for the api-call boundary). Disjoint by parameter.
        "breathe/apicall"
    }
    fn logical_field(&self) -> &'static str {
        "apicall.param"
    }
    fn resource(&self) -> &'static str {
        "memory"
    }
    fn semantics(&self) -> ApplySemantics {
        ApplySemantics::ContinuousReconciliation
    }
    fn layout(&self, _target: &Target) -> LimitLayout {
        LimitLayout::ApiCall { endpoint: self.endpoint.clone(), command: self.command.clone() }
    }
    fn metric_source(&self, target: &Target) -> MetricSource {
        // The `used` lives on the k8s metrics plane; the controller substitutes the
        // band's real PodMetricsMax source. The descriptor names the shape.
        MetricSource::PodMetricsMax {
            resource: "memory".into(),
            pod_prefix: target.name.clone(),
            selector: target.pod_selector.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    const GI: u64 = 1024 * 1024 * 1024;

    /// A programmable in-memory [`ApiCallEnv`] — the testability seam. Seeded with
    /// canned `(endpoint, command) → value` reads; records every write so a test
    /// can assert shadow mode wrote nothing / live mode wrote exactly the value.
    #[derive(Default)]
    struct MockApiCallEnv {
        values: Mutex<BTreeMap<(String, String), u64>>,
        writes: Mutex<Vec<(String, String, u64)>>,
    }

    impl MockApiCallEnv {
        fn with(endpoint: &str, command: &str, value: u64) -> Self {
            let m = Self::default();
            m.values.lock().unwrap().insert((endpoint.into(), command.into()), value);
            m
        }
        fn writes(&self) -> Vec<(String, String, u64)> {
            self.writes.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ApiCallEnv for MockApiCallEnv {
        async fn get_config(&self, endpoint: &str, command: &str) -> Result<u64, ApiCallError> {
            self.values
                .lock()
                .unwrap()
                .get(&(endpoint.to_string(), command.to_string()))
                .copied()
                .ok_or_else(|| ApiCallError::Parse("no canned value for this param".into()))
        }
        async fn set_config(&self, endpoint: &str, command: &str, value: u64) -> Result<(), ApiCallError> {
            self.values
                .lock()
                .unwrap()
                .insert((endpoint.to_string(), command.to_string()), value);
            self.writes.lock().unwrap().push((endpoint.to_string(), command.to_string(), value));
            Ok(())
        }
    }

    fn redis_target() -> Target {
        Target {
            namespace: "cache".into(),
            name: "redis".into(),
            kind: "ApiCall".into(),
            api_version: String::new(),
            container: None,
            pod_selector: None,
        }
    }
    fn redis_layout() -> LimitLayout {
        LimitLayout::ApiCall { endpoint: "redis://cache.svc:6379".into(), command: "maxmemory".into() }
    }

    #[test]
    fn classify_recognizes_bare_redis_param_config_verb_and_other_protocols() {
        assert_eq!(classify_command("maxmemory").unwrap(), (Protocol::Redis, "maxmemory".into()));
        assert_eq!(
            classify_command("CONFIG SET maxmemory").unwrap(),
            (Protocol::Redis, "maxmemory".into())
        );
        assert_eq!(classify_command("nats stream edit ORDERS --max-bytes").unwrap().0, Protocol::Nats);
        assert_eq!(classify_command("kafka-configs --alter retention.ms").unwrap().0, Protocol::Kafka);
        assert!(matches!(classify_command("   "), Err(ApiCallError::BadCommand(_))));
    }

    #[tokio::test]
    async fn read_limit_returns_the_current_redis_maxmemory() {
        let env = MockApiCallEnv::with("redis://cache.svc:6379", "maxmemory", 2 * GI);
        let cluster = ApiCallCluster::shadow(env);
        let v = cluster.read_limit(&redis_target(), &redis_layout(), "memory").await.unwrap();
        assert_eq!(v, 2 * GI);
    }

    #[tokio::test]
    async fn apply_round_trips_in_live_mode_and_writes_nothing_in_shadow() {
        // SHADOW: apply decides + reports, writes nothing.
        let shadow = ApiCallCluster::shadow(MockApiCallEnv::with("redis://cache.svc:6379", "maxmemory", GI));
        let patch = SsaPatch {
            target: redis_target(),
            field_manager: "breathe/apicall".into(),
            layout: redis_layout(),
            resource: "memory".into(),
            value: 3 * GI,
        };
        let receipt = shadow.apply(&patch).await.unwrap();
        assert_eq!(receipt.source_hash, [0u8; 16]);
        assert!(shadow.env().writes().is_empty(), "shadow must not write");
        // the underlying value is unchanged in shadow.
        assert_eq!(shadow.read_limit(&redis_target(), &redis_layout(), "memory").await.unwrap(), GI);

        // LIVE: apply writes through, and a subsequent read sees the new value.
        let live = ApiCallCluster::new(MockApiCallEnv::with("redis://cache.svc:6379", "maxmemory", GI), true);
        live.apply(&patch).await.unwrap();
        assert_eq!(
            live.env().writes(),
            vec![("redis://cache.svc:6379".into(), "maxmemory".into(), 3 * GI)]
        );
        assert_eq!(live.read_limit(&redis_target(), &redis_layout(), "memory").await.unwrap(), 3 * GI);
    }

    #[tokio::test]
    async fn wrong_layout_is_a_typed_permanent_error_not_a_panic() {
        let cluster = ApiCallCluster::new(MockApiCallEnv::default(), true);
        // a host knob can never legitimately reach the api-call actuator.
        let host_layout = LimitLayout::Host(breathe_provider::HostKnob::ZfsArcMax);
        let read_err = cluster.read_limit(&redis_target(), &host_layout, "memory").await.unwrap_err();
        assert!(matches!(read_err, ProviderError::ApiPermanent(_)));

        let patch = SsaPatch {
            target: redis_target(),
            field_manager: "breathe/apicall".into(),
            layout: host_layout,
            resource: "memory".into(),
            value: GI,
        };
        let apply_err = cluster.apply(&patch).await.unwrap_err();
        assert!(matches!(apply_err, ProviderError::ApiPermanent(_)));
    }

    #[tokio::test]
    async fn read_used_is_a_typed_gap_routing_metrics_to_the_k8s_plane() {
        let cluster = ApiCallCluster::shadow(MockApiCallEnv::default());
        let src = MetricSource::PodMetricsMax {
            resource: "memory".into(),
            pod_prefix: "redis".into(),
            selector: None,
        };
        let err = cluster.read_used(&src).await.unwrap_err();
        assert!(matches!(err, ProviderError::ApiPermanent(_)));
    }

    #[tokio::test]
    async fn field_owners_is_empty_so_the_single_writer_guard_proceeds() {
        let cluster = ApiCallCluster::shadow(MockApiCallEnv::default());
        let owners = cluster
            .field_owners(&redis_target(), &redis_layout(), "memory", "apicall.param")
            .await
            .unwrap();
        assert!(owners.is_empty());
    }
}
