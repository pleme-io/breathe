//! `breathe-jmx` — the **JVM app-runtime** [`Cluster`] implementation (a third
//! pair of *hands*, beside `breathe-kube`'s k8s SSA and `breathe-host`'s
//! systemd/sysfs).
//!
//! breathe holds a workload in a utilization band by carving its *limit*; for a
//! JVM application that limit is an MBean attribute — `HikariCP`'s
//! `maximumPoolSize`, Tomcat/Jetty's `maxThreads`, a Caffeine cache's
//! `maximumSize`, the JVM's `SoftMaxHeapSize`. Each is a live, restart-free knob
//! the runtime applies the instant it is written, so the *same* generic
//! `breathe_provider::BandProvider` + the proven `breathe_control::safety_clamp`
//! gate drive it exactly as they drive a pod's `limits.memory` or a host's
//! `zfs_arc_max`. There is no new control logic here.
//!
//! ### The layout arm
//! There is no dedicated [`LimitLayout`] arm for JMX, so a JVM band addresses its
//! knob through the existing app-plane [`LimitLayout::ApiCall`]`{ endpoint,
//! command }`, where:
//! - `endpoint` = the JMX/Jolokia base URL (`http://app:8778/jolokia`), and
//! - `command`  = the MBean attribute coordinate
//!   (`HikariPool-1:maximumPoolSize`, `Catalina:type=ThreadPool,name="http"
//!   :maxThreads`, …) — the `ObjectName` and attribute joined by the last colon.
//!
//! `ApiCall` is already `RestartFree` (the value applies live), so a JVM band
//! ticks at the golden cadence with zero workload disturbance — the whole point.
//!
//! ### The I/O seam
//! Every real Jolokia round-trip lives behind the [`JmxEnv`] trait — the
//! typed-spec-triplet testability seam — so the [`JmxCluster`] decision path is
//! fully exercised against a [mock](#tests) with zero network. The real impl
//! ([`JolokiaCurlEnv`]) drives Jolokia over `curl` argv (a typed `Vec`, never a
//! shell) and is dependency-light by construction; a deployment that needs a
//! richer protocol than the curl bridge can offer surfaces a *typed*
//! [`ProviderError::ApiPermanent`] gap, never a panic.

use async_trait::async_trait;
use breathe_provider::{
    AppliedReceipt, Cluster, FieldOwner, LimitLayout, MetricSource, ProviderError, Sample, SsaPatch,
    Target,
};

// ───────────────────────────── errors ──────────────────────────────

/// Typed JMX-I/O error — never a silent wrong answer (TYPED-SPEC discipline).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JmxError {
    /// The `command` (`ObjectName:attribute`) could not be split into an MBean
    /// `ObjectName` + attribute — a mis-authored band, surfaced typed.
    BadCommand(String),
    /// A Jolokia round-trip failed at the transport (process spawn, connection).
    Io(String),
    /// Jolokia returned a body that could not be parsed into the expected `u64`.
    Parse(String),
    /// A `curl` invocation exited non-zero (Jolokia HTTP error / unreachable).
    Command { argv: String, code: Option<i32>, stderr: String },
    /// No live Jolokia client is linked for the addressed protocol — a TYPED gap,
    /// reported (never a panic) so a consumer sees the missing capability
    /// mechanically. Carries the endpoint/attribute for diagnosis.
    NotLinked(String),
}

impl std::fmt::Display for JmxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadCommand(c) => write!(f, "jmx: malformed MBean command `{c}` (want `ObjectName:attribute`)"),
            Self::Io(m) => write!(f, "jmx io error: {m}"),
            Self::Parse(m) => write!(f, "jmx parse error: {m}"),
            Self::Command { argv, code, stderr } => write!(f, "`{argv}` exited {code:?}: {stderr}"),
            Self::NotLinked(m) => write!(f, "jmx: live Jolokia client not yet linked ({m})"),
        }
    }
}

impl std::error::Error for JmxError {}

impl From<JmxError> for ProviderError {
    fn from(e: JmxError) -> Self {
        match e {
            // A garbled body is "metrics missing"; a malformed command or an
            // unlinked client is permanent (it will not fix itself on retry); an
            // IO/HTTP blip is transient.
            JmxError::Parse(_) => ProviderError::MetricsMissing,
            JmxError::BadCommand(m) => ProviderError::ApiPermanent(m),
            JmxError::NotLinked(m) => ProviderError::ApiPermanent(m),
            JmxError::Io(m) => ProviderError::ApiTransient(m),
            cmd @ JmxError::Command { .. } => ProviderError::ApiTransient(cmd.to_string()),
        }
    }
}

/// Split an `ApiCall` `command` into its MBean `ObjectName` + attribute on the
/// LAST colon — an `ObjectName` itself contains colons (`Catalina:type=…`), so
/// the attribute is everything after the final one. Pure + testable.
///
/// # Errors
/// [`JmxError::BadCommand`] when the command has no colon (no attribute) or an
/// empty `ObjectName`/attribute.
pub fn split_mbean_command(command: &str) -> Result<(&str, &str), JmxError> {
    match command.rsplit_once(':') {
        Some((object_name, attribute)) if !object_name.is_empty() && !attribute.is_empty() => {
            Ok((object_name, attribute))
        }
        _ => Err(JmxError::BadCommand(command.to_string())),
    }
}

// ─────────────────────── the side-effect seam ──────────────────────

/// The JVM app-runtime I/O boundary — every real Jolokia MBean read/write,
/// behind a trait so the [`JmxCluster`] decision path is fully exercised against
/// a mock. Symmetric with `breathe_host::HostEnvironment`.
pub trait JmxEnv: Send + Sync {
    /// Read an MBean attribute as a `u64` (the live `limit` — e.g.
    /// `HikariPool-1` / `maximumPoolSize`). `endpoint` is the Jolokia base URL;
    /// `attribute` is the `ObjectName:attribute` coordinate.
    fn read_mbean(&self, endpoint: &str, attribute: &str) -> Result<u64, JmxError>;
    /// Write a `u64` to an MBean attribute (the carve — the new `limit`).
    fn write_mbean(&self, endpoint: &str, attribute: &str, value: u64) -> Result<(), JmxError>;
}

/// The real implementation over Jolokia's HTTP bridge, driven by `curl` argv
/// (a typed `Vec`, never a shell). Dependency-light by construction: it shells
/// out to `curl` exactly like `breathe_host::SystemdSysfsEnv` shells out to
/// `systemctl`.
///
/// Jolokia speaks JSON-over-HTTP: a READ is `GET
/// <endpoint>/read/<ObjectName>/<attribute>` and a WRITE is `GET
/// <endpoint>/write/<ObjectName>/<attribute>/<value>`, each returning a JSON
/// envelope `{"value": …, "status": 200, …}`. The bridge parses the `value`
/// field out of the body.
///
/// `linked = false` (the default) is the SAFE bring-up posture: until a
/// deployment has verified its Jolokia reachability + agent surface, every
/// method returns a *typed* [`JmxError::NotLinked`] instead of attempting a
/// round-trip — a typed gap, never a panic. Flip it on with
/// [`linked`](Self::linked) once the bridge is wired.
#[derive(Debug, Clone)]
pub struct JolokiaCurlEnv {
    curl_bin: String,
    linked: bool,
}

impl Default for JolokiaCurlEnv {
    fn default() -> Self {
        Self { curl_bin: "curl".into(), linked: false }
    }
}

impl JolokiaCurlEnv {
    /// Read the bridge config from the environment (a DaemonSet/sidecar sets
    /// these): `BREATHE_JMX_CURL_BIN` (the curl path), `BREATHE_JMX_LINKED=1`
    /// (enable the live bridge once Jolokia reachability is verified).
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            curl_bin: std::env::var("BREATHE_JMX_CURL_BIN").unwrap_or_else(|_| "curl".into()),
            linked: std::env::var("BREATHE_JMX_LINKED").is_ok_and(|v| v == "1" || v == "true"),
        }
    }
    /// Enable the live Jolokia bridge (once reachability is verified).
    #[must_use]
    pub fn linked(mut self, linked: bool) -> Self {
        self.linked = linked;
        self
    }

    /// Build the typed `curl` argv for a Jolokia request. Pure + testable — no
    /// shell. `-sf` makes curl silent + fail-on-HTTP-error (so a Jolokia 5xx is
    /// a non-zero exit we map to a typed error), `-m 5` bounds the round-trip.
    fn curl_argv(method: &str, endpoint: &str, object_name: &str, attribute: &str, value: Option<u64>) -> Vec<String> {
        // Jolokia GET path: <endpoint>/<read|write>/<ObjectName>/<attribute>[/<value>].
        let base = endpoint.trim_end_matches('/');
        let mut url = format!("{base}/{method}/{object_name}/{attribute}");
        if let Some(v) = value {
            url.push('/');
            url.push_str(&v.to_string());
        }
        vec!["-sf".into(), "-m".into(), "5".into(), url]
    }

    /// Run `curl <argv…>` and return trimmed stdout (the Jolokia JSON body).
    fn curl(&self, argv: &[String]) -> Result<String, JmxError> {
        let out = std::process::Command::new(&self.curl_bin)
            .args(argv)
            .output()
            .map_err(|e| JmxError::Io(e.to_string()))?;
        if !out.status.success() {
            return Err(JmxError::Command {
                argv: format!("{} {}", self.curl_bin, argv.join(" ")),
                code: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
            });
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }
}

/// Extract the integer `"value"` field from a Jolokia JSON response body. Pure +
/// testable. Jolokia bodies are `{"request":…,"value":42,"status":200,…}`; we
/// pull the digits following the `"value":` key. A dependency-light parse that
/// avoids pulling in a JSON crate for one scalar field.
///
/// # Errors
/// [`JmxError::Parse`] when no numeric `"value"` field is present.
pub fn parse_jolokia_value(body: &str) -> Result<u64, JmxError> {
    let after = body
        .split_once("\"value\"")
        .map(|(_, rest)| rest)
        .ok_or_else(|| JmxError::Parse("no \"value\" field in Jolokia body".into()))?;
    // skip the colon + whitespace, then take the leading digit run.
    let digits: String = after
        .trim_start_matches([':', ' ', '\t'])
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    if digits.is_empty() {
        return Err(JmxError::Parse("Jolokia \"value\" is not an integer".into()));
    }
    digits.parse::<u64>().map_err(|e| JmxError::Parse(e.to_string()))
}

impl JmxEnv for JolokiaCurlEnv {
    fn read_mbean(&self, endpoint: &str, attribute: &str) -> Result<u64, JmxError> {
        if !self.linked {
            return Err(JmxError::NotLinked(format!("read {attribute} @ {endpoint}")));
        }
        let (object_name, attr) = split_mbean_command(attribute)?;
        let argv = Self::curl_argv("read", endpoint, object_name, attr, None);
        let body = self.curl(&argv)?;
        parse_jolokia_value(&body)
    }

    fn write_mbean(&self, endpoint: &str, attribute: &str, value: u64) -> Result<(), JmxError> {
        if !self.linked {
            return Err(JmxError::NotLinked(format!("write {attribute}={value} @ {endpoint}")));
        }
        let (object_name, attr) = split_mbean_command(attribute)?;
        let argv = Self::curl_argv("write", endpoint, object_name, attr, Some(value));
        self.curl(&argv).map(|_| ())
    }
}

// ─────────────────────────── JmxCluster ────────────────────────────

/// The JVM app-runtime `Cluster`. `write_enabled = false` is SHADOW mode: it
/// reads + decides + reports `appliedValue` but performs no MBean mutation, so
/// the full loop can be observed against a live JVM before a single attribute is
/// written. Mirrors `breathe_host::HostCluster`'s shadow posture.
pub struct JmxCluster<E: JmxEnv> {
    env: E,
    write_enabled: bool,
}

impl<E: JmxEnv> JmxCluster<E> {
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
impl<E: JmxEnv> Cluster for JmxCluster<E> {
    async fn read_used(&self, _source: &MetricSource) -> Result<Sample, ProviderError> {
        // A JMX actuator carves a JVM knob but does NOT read the workload's `used`
        // utilization — that comes from the band's metric plane (metrics-server /
        // PromQL via KubeCluster). Routing a `used` read here is a wiring error,
        // surfaced TYPED (never a silent zero). The band law's `used` and this
        // actuator's `limit` are read on different boundaries by construction.
        Err(ProviderError::ApiPermanent(
            "JmxCluster has no `used` plane (read the band's metric via the metric Cluster; JMX owns the limit only)".into(),
        ))
    }

    async fn read_limit(
        &self,
        _target: &Target,
        layout: &LimitLayout,
        _resource: &str,
    ) -> Result<u64, ProviderError> {
        match layout {
            // The JVM knob lives behind the app-plane ApiCall arm: endpoint = the
            // Jolokia URL, command = the MBean attribute coordinate.
            LimitLayout::ApiCall { endpoint, command } => {
                Ok(self.env.read_mbean(endpoint, command)?)
            }
            // Any other layout can never legitimately reach the JMX boundary —
            // typed, never silent (mirrors HostCluster's k8s-layout rejection).
            _ => Err(ProviderError::ApiPermanent(
                "non-ApiCall layout on JmxCluster (route k8s/host dimensions to their own Cluster)".into(),
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
        // A JVM MBean attribute has no Kubernetes managedFields and no competing
        // SSA writer — the live JMX knob is breathe-only. An empty owner set ⇒ the
        // single-writer guard always proceeds, never a phantom Conflict (mirrors
        // the host boundary).
        Ok(Vec::new())
    }

    async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError> {
        let LimitLayout::ApiCall { endpoint, command } = &patch.layout else {
            return Err(ProviderError::ApiPermanent(
                "non-ApiCall layout on JmxCluster apply (route k8s/host dimensions to their own Cluster)".into(),
            ));
        };
        // SHADOW: decide + report, never mutate the JVM.
        if !self.write_enabled {
            return Ok(AppliedReceipt { source_hash: [0u8; 16] });
        }
        self.env.write_mbean(endpoint, command, patch.value)?;
        Ok(AppliedReceipt { source_hash: [0u8; 16] })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// A programmable in-memory [`JmxEnv`] — the testability seam. Holds canned
    /// MBean values keyed by `(endpoint, attribute)`; records every write so a
    /// test can assert shadow mode wrote nothing / live mode wrote exactly the
    /// clamped value. Mirrors `breathe_host::tests::MockHostEnv`.
    #[derive(Default)]
    struct MockJmxEnv {
        mbeans: Mutex<BTreeMap<(String, String), u64>>,
        writes: Mutex<Vec<(String, String, u64)>>,
    }

    impl MockJmxEnv {
        fn with(endpoint: &str, attribute: &str, value: u64) -> Self {
            let env = Self::default();
            env.mbeans.lock().unwrap().insert((endpoint.into(), attribute.into()), value);
            env
        }
        fn writes(&self) -> Vec<(String, String, u64)> {
            self.writes.lock().unwrap().clone()
        }
    }

    impl JmxEnv for MockJmxEnv {
        fn read_mbean(&self, endpoint: &str, attribute: &str) -> Result<u64, JmxError> {
            self.mbeans
                .lock()
                .unwrap()
                .get(&(endpoint.to_string(), attribute.to_string()))
                .copied()
                .ok_or_else(|| JmxError::Parse("no such MBean attribute".into()))
        }
        fn write_mbean(&self, endpoint: &str, attribute: &str, value: u64) -> Result<(), JmxError> {
            self.mbeans.lock().unwrap().insert((endpoint.to_string(), attribute.to_string()), value);
            self.writes.lock().unwrap().push((endpoint.to_string(), attribute.to_string(), value));
            Ok(())
        }
    }

    const JOLOKIA: &str = "http://app:8778/jolokia";

    fn jvm_target() -> Target {
        Target {
            namespace: "default".into(),
            name: "payments".into(),
            kind: "Deployment".into(),
            api_version: "apps/v1".into(),
            container: None,
            pod_selector: None,
        }
    }
    fn hikari_layout() -> LimitLayout {
        LimitLayout::ApiCall { endpoint: JOLOKIA.into(), command: "HikariPool-1:maximumPoolSize".into() }
    }

    #[test]
    fn splits_mbean_command_on_the_last_colon() {
        // a plain HikariCP pool attribute.
        assert_eq!(split_mbean_command("HikariPool-1:maximumPoolSize").unwrap(), ("HikariPool-1", "maximumPoolSize"));
        // a Tomcat ObjectName itself contains colons — split on the LAST one.
        assert_eq!(
            split_mbean_command("Catalina:type=ThreadPool,name=http:maxThreads").unwrap(),
            ("Catalina:type=ThreadPool,name=http", "maxThreads")
        );
        // no colon / empty halves ⇒ a typed BadCommand, never a silent guess.
        assert!(matches!(split_mbean_command("maximumPoolSize"), Err(JmxError::BadCommand(_))));
        assert!(matches!(split_mbean_command(":maxThreads"), Err(JmxError::BadCommand(_))));
        assert!(matches!(split_mbean_command("HikariPool-1:"), Err(JmxError::BadCommand(_))));
    }

    #[test]
    fn parses_the_integer_value_out_of_a_jolokia_body() {
        assert_eq!(parse_jolokia_value(r#"{"request":{},"value":42,"status":200}"#).unwrap(), 42);
        assert_eq!(parse_jolokia_value(r#"{"value": 1024 ,"status":200}"#).unwrap(), 1024);
        // missing / non-integer value ⇒ a typed Parse error, never a silent 0.
        assert!(matches!(parse_jolokia_value(r#"{"status":200}"#), Err(JmxError::Parse(_))));
        assert!(matches!(parse_jolokia_value(r#"{"value":"big"}"#), Err(JmxError::Parse(_))));
    }

    #[test]
    fn builds_jolokia_curl_argv_without_a_shell() {
        // READ: GET <endpoint>/read/<ObjectName>/<attribute> — argv, no value.
        let read = JolokiaCurlEnv::curl_argv("read", JOLOKIA, "HikariPool-1", "maximumPoolSize", None);
        assert_eq!(read, vec!["-sf", "-m", "5", "http://app:8778/jolokia/read/HikariPool-1/maximumPoolSize"]);
        // WRITE: GET <endpoint>/write/<ObjectName>/<attribute>/<value> (trailing
        // slash on the endpoint is trimmed, not doubled).
        let write = JolokiaCurlEnv::curl_argv("write", "http://app:8778/jolokia/", "HikariPool-1", "maximumPoolSize", Some(20));
        assert_eq!(write, vec!["-sf", "-m", "5", "http://app:8778/jolokia/write/HikariPool-1/maximumPoolSize/20"]);
    }

    #[tokio::test]
    async fn read_limit_reads_the_mbean_attribute_through_the_apicall_arm() {
        // Census knob: HikariCP maxPoolSize currently 10.
        let env = MockJmxEnv::with(JOLOKIA, "HikariPool-1:maximumPoolSize", 10);
        let cluster = JmxCluster::shadow(env);
        let v = cluster.read_limit(&jvm_target(), &hikari_layout(), "cpu").await.unwrap();
        assert_eq!(v, 10, "read_limit returns the live MBean value via read_mbean");
        // field_owners is always empty — no managedFields on a JVM knob.
        assert!(cluster.field_owners(&jvm_target(), &hikari_layout(), "cpu", "jmx.hikari.maxPoolSize").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn apply_round_trips_the_carve_live_then_reads_it_back() {
        // Caffeine maximumSize census knob: carve 5000 → 8000 live, then confirm.
        let layout = LimitLayout::ApiCall {
            endpoint: JOLOKIA.into(),
            command: "com.example.cache:type=Caffeine,name=sessions:maximumSize".into(),
        };
        let env = MockJmxEnv::with(JOLOKIA, "com.example.cache:type=Caffeine,name=sessions:maximumSize", 5000);
        let cluster = JmxCluster::new(env, true); // write-enabled
        let patch = SsaPatch {
            target: jvm_target(),
            field_manager: "breathe/jmx".into(),
            layout: layout.clone(),
            resource: "cpu".into(),
            value: 8000,
        };
        cluster.apply(&patch).await.unwrap();
        // the write was recorded AND it round-trips back through read_limit.
        assert_eq!(
            cluster.env().writes(),
            vec![(JOLOKIA.to_string(), "com.example.cache:type=Caffeine,name=sessions:maximumSize".to_string(), 8000)]
        );
        assert_eq!(cluster.read_limit(&jvm_target(), &layout, "cpu").await.unwrap(), 8000);
    }

    #[tokio::test]
    async fn shadow_mode_decides_but_writes_no_mbean() {
        // Tomcat maxThreads census knob — shadow must observe + report, never carve.
        let layout = LimitLayout::ApiCall {
            endpoint: JOLOKIA.into(),
            command: "Catalina:type=ThreadPool,name=http:maxThreads".into(),
        };
        let env = MockJmxEnv::with(JOLOKIA, "Catalina:type=ThreadPool,name=http:maxThreads", 200);
        let cluster = JmxCluster::shadow(env);
        let patch = SsaPatch {
            target: jvm_target(),
            field_manager: "breathe/jmx".into(),
            layout,
            resource: "cpu".into(),
            value: 300,
        };
        cluster.apply(&patch).await.unwrap();
        assert!(cluster.env().writes().is_empty(), "shadow mode must not write any MBean");
    }

    #[tokio::test]
    async fn a_wrong_layout_is_a_typed_permanent_error_on_every_verb() {
        // The JMX actuator owns ONLY the ApiCall arm — a host/k8s layout reaching
        // it is a wiring error, surfaced typed (never silent), on read AND apply.
        let cluster = JmxCluster::new(MockJmxEnv::default(), true);
        let wrong = LimitLayout::PodTemplate { container: None };
        let read_err = cluster.read_limit(&jvm_target(), &wrong, "cpu").await.unwrap_err();
        assert!(matches!(read_err, ProviderError::ApiPermanent(_)), "wrong layout on read_limit is permanent");
        let patch = SsaPatch {
            target: jvm_target(),
            field_manager: "breathe/jmx".into(),
            layout: wrong,
            resource: "cpu".into(),
            value: 1,
        };
        let apply_err = cluster.apply(&patch).await.unwrap_err();
        assert!(matches!(apply_err, ProviderError::ApiPermanent(_)), "wrong layout on apply is permanent");
        assert!(cluster.env().writes().is_empty(), "a refused apply touches no MBean");
    }

    #[tokio::test]
    async fn read_used_on_the_jmx_boundary_is_a_typed_error() {
        // JMX owns the limit, not the `used` plane — a `used` read here is a wiring
        // error, surfaced typed (the band reads `used` on its metric Cluster).
        let cluster = JmxCluster::shadow(MockJmxEnv::default());
        let err = cluster
            .read_used(&MetricSource::PodMetricsMax { resource: "cpu".into(), pod_prefix: "payments".into(), selector: None })
            .await
            .unwrap_err();
        assert!(matches!(err, ProviderError::ApiPermanent(_)));
    }

    #[test]
    fn the_unlinked_real_env_reports_a_typed_gap_never_a_panic() {
        // The dependency-light real impl defaults to UNLINKED — every method is a
        // typed ApiPermanent (NotLinked), never a panic/unimplemented/todo, until a
        // deployment verifies Jolokia reachability and flips `linked(true)`.
        let env = JolokiaCurlEnv::default();
        let read = env.read_mbean(JOLOKIA, "HikariPool-1:maximumPoolSize").unwrap_err();
        assert!(matches!(read, JmxError::NotLinked(_)));
        assert!(matches!(ProviderError::from(read), ProviderError::ApiPermanent(_)));
        let write = env.write_mbean(JOLOKIA, "HikariPool-1:maximumPoolSize", 20).unwrap_err();
        assert!(matches!(write, JmxError::NotLinked(_)));
    }
}
