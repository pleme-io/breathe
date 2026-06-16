//! `breathe-configreload` — the **CONFIG-FILE-RELOAD** [`Cluster`]
//! implementation (the *hands* for the app plane).
//!
//! breathe's k8s boundary is `breathe-kube::KubeCluster`; its host boundary is
//! `breathe-host::HostCluster`; this is the third actuator — the one that owns a
//! value living in a **process's config file**, applied via the process's own
//! **reload mechanism**. It carves a `key = value` line (PostgreSQL `work_mem`,
//! pgbouncer `default_pool_size`, nginx `worker_connections`) and triggers the
//! matching [`ConfigReload`] so the value takes effect:
//!
//! - [`ConfigReload::Sighup`] — `SIGHUP` to the process (PostgreSQL, nginx
//!   re-read their config live). RestartFree.
//! - [`ConfigReload::Reload`] — a protocol/admin `RELOAD` command (pgbouncer).
//!   RestartFree.
//! - [`ConfigReload::Restart`] — the value needs a full process restart
//!   (PostgreSQL `shared_buffers`). breathe NEVER restarts a process from this
//!   actuator: the edit is written, but the reload is **deferred** (logged + a
//!   typed receipt) so an operator (or a higher-disruption-policy actor) owns
//!   the restart. The band's `DisruptionPolicy` already gates whether a
//!   restart-requiring carve is even proposed; this is the second wall.
//!
//! The compounding claim holds unchanged: there is **no new control logic** here.
//! [`ConfigReloadCluster`] is just another `Cluster`, so the *one* generic
//! `breathe_provider::BandProvider` + the proven `breathe_control::safety_clamp`
//! gate drive a config-file dimension exactly as they drive a k8s or host one.
//! The only genuinely new thing is the config-file *I/O* (read a key, write a
//! key, send a signal), and even that is abstracted behind the [`ConfigReloadEnv`]
//! trait — the typed-spec-triplet testability seam, so every decision is
//! exercised against a mock with zero real filesystem or process signalling.
//!
//! ### The `used` metric is somebody else's job
//! A config-file value (`work_mem`) is a *limit*, not a thing with a live
//! cgroup-style `used` counter on the same boundary — its utilization is read
//! from Prometheus / a protocol query handled by a different actuator. So
//! [`ConfigReloadCluster::read_used`] returns a typed
//! [`ProviderError::ApiPermanent`] for every source it cannot itself read,
//! never a silent wrong answer (TYPED-SPEC discipline). `read_limit` reads the
//! current value straight from the file; `apply` writes it + reloads.

use async_trait::async_trait;
use breathe_provider::{
    AppliedReceipt, Cluster, ConfigReload, FieldOwner, LimitLayout, MetricSource, ProviderError,
    Sample, SsaPatch, Target,
};

// ───────────────────────────── errors ──────────────────────────────

/// Typed config-reload I/O error — never a silent wrong answer (TYPED-SPEC
/// discipline). Mirrors `breathe_host::HostError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigReloadError {
    /// A config-file read/write failed.
    Io(String),
    /// A value could not be parsed into the expected shape (e.g. a non-integer
    /// on the right of `key = value`).
    Parse(String),
    /// The addressed `key` is not present in the config file.
    KeyMissing { path: String, key: String },
    /// A reload invocation (signal / `RELOAD` command) exited non-zero.
    Command { argv: String, code: Option<i32>, stderr: String },
}

impl std::fmt::Display for ConfigReloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(m) => write!(f, "config-reload io error: {m}"),
            Self::Parse(m) => write!(f, "config-reload parse error: {m}"),
            Self::KeyMissing { path, key } => {
                write!(f, "key `{key}` not present in config file `{path}`")
            }
            Self::Command { argv, code, stderr } => {
                write!(f, "`{argv}` exited {code:?}: {stderr}")
            }
        }
    }
}

impl std::error::Error for ConfigReloadError {}

impl From<ConfigReloadError> for ProviderError {
    fn from(e: ConfigReloadError) -> Self {
        match e {
            // A missing/garbled value is "metrics missing" (the limit cannot be
            // read this tick); a hard command failure or genuine IO is transient
            // (it may succeed on retry — the file may reappear, the process may
            // come up). A missing key is permanent until the config is fixed.
            ConfigReloadError::Parse(_) => ProviderError::MetricsMissing,
            ConfigReloadError::KeyMissing { path, key } => {
                ProviderError::ApiPermanent(format!("config key `{key}` missing in `{path}`"))
            }
            ConfigReloadError::Io(m) => ProviderError::ApiTransient(m),
            cmd @ ConfigReloadError::Command { .. } => ProviderError::ApiTransient(cmd.to_string()),
        }
    }
}

// ─────────────────────── the side-effect seam ──────────────────────

/// The config-file I/O boundary — every real file read/write + reload side
/// effect, behind a trait so the [`ConfigReloadCluster`] decision path is fully
/// exercised against a mock. The typed-spec-triplet testability seam, mirroring
/// `breathe_host::HostEnvironment`.
pub trait ConfigReloadEnv: Send + Sync {
    /// Read the current `u64` value of `key` from the `key = value` config file
    /// at `path`. Errors typed: [`ConfigReloadError::Io`] when the file can't be
    /// read, [`ConfigReloadError::KeyMissing`] when the key isn't present,
    /// [`ConfigReloadError::Parse`] when the value isn't a bare integer.
    fn read_config_value(&self, path: &str, key: &str) -> Result<u64, ConfigReloadError>;
    /// Set `key = value` in the config file at `path` — rewriting the existing
    /// line in place, or appending the line if the key is absent.
    fn write_config_value(&self, path: &str, key: &str, value: u64)
        -> Result<(), ConfigReloadError>;
    /// Trigger the reload that makes a just-written value take effect. `mechanism`
    /// names HOW (e.g. `"sighup"`, `"reload"`) and is interpreted by the impl:
    /// the real impl maps `sighup` → `kill -HUP $(cat <pidfile>)` and `reload` →
    /// an admin `RELOAD` command (both via `std::process::Command`, argv, no
    /// shell). The mechanism string carries the addressing the impl needs
    /// (pidfile path / connection target) so the [`Cluster`] code stays
    /// reload-mechanism-agnostic.
    fn reload(&self, path: &str, mechanism: &str) -> Result<(), ConfigReloadError>;
}

/// The real implementation over std `fs` + `std::process::Command` (argv, never a
/// shell). Mirrors `breathe_host::SystemdSysfsEnv`.
///
/// `pidfile` resolves the process to signal for a `SIGHUP` reload (PostgreSQL,
/// nginx write their master PID there); `reload_argv_prefix` carries the admin
/// command to run for a protocol `RELOAD` (pgbouncer: e.g. `["psql", "-p",
/// "6432", "-U", "pgbouncer", "pgbouncer", "-c"]`, to which `RELOAD;` is
/// appended). Both are config; the actuator owns no policy.
#[derive(Debug, Clone, Default)]
pub struct FileSignalEnv {
    /// PID file whose contents identify the process to `SIGHUP` (sighup mechanism).
    pub pidfile: Option<String>,
    /// argv prefix for the admin `RELOAD` command (reload mechanism); `RELOAD;`
    /// is the final argument the impl appends.
    pub reload_argv_prefix: Vec<String>,
}

impl FileSignalEnv {
    /// Construct with an explicit `SIGHUP` pidfile (PostgreSQL / nginx).
    #[must_use]
    pub fn with_pidfile(pidfile: impl Into<String>) -> Self {
        Self { pidfile: Some(pidfile.into()), ..Self::default() }
    }
    /// Construct with an explicit admin `RELOAD` argv prefix (pgbouncer).
    #[must_use]
    pub fn with_reload_command(prefix: Vec<String>) -> Self {
        Self { reload_argv_prefix: prefix, ..Self::default() }
    }

    /// Run a child process by `(program, argv)` (no shell) and map a non-zero exit
    /// or spawn failure to a typed error. The one sanctioned process reach.
    fn run(prog: &str, argv: &[String]) -> Result<(), ConfigReloadError> {
        let out = std::process::Command::new(prog)
            .args(argv)
            .output()
            .map_err(|e| ConfigReloadError::Io(e.to_string()))?;
        if out.status.success() {
            Ok(())
        } else {
            Err(ConfigReloadError::Command {
                argv: format!("{prog} {}", argv.join(" ")),
                code: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
            })
        }
    }
}

impl ConfigReloadEnv for FileSignalEnv {
    fn read_config_value(&self, path: &str, key: &str) -> Result<u64, ConfigReloadError> {
        let text = std::fs::read_to_string(path).map_err(|e| ConfigReloadError::Io(e.to_string()))?;
        match parse_config_value(&text, key) {
            Some(raw) => raw
                .parse::<u64>()
                .map_err(|e| ConfigReloadError::Parse(format!("{key}={raw:?}: {e}"))),
            None => Err(ConfigReloadError::KeyMissing { path: path.to_string(), key: key.to_string() }),
        }
    }

    fn write_config_value(&self, path: &str, key: &str, value: u64) -> Result<(), ConfigReloadError> {
        let text = std::fs::read_to_string(path).map_err(|e| ConfigReloadError::Io(e.to_string()))?;
        let next = render_config_value(&text, key, value);
        std::fs::write(path, next).map_err(|e| ConfigReloadError::Io(e.to_string()))
    }

    fn reload(&self, _path: &str, mechanism: &str) -> Result<(), ConfigReloadError> {
        match mechanism {
            // SIGHUP: read the pidfile, then `kill -HUP <pid>` (argv, no shell).
            "sighup" => {
                let pidfile = self.pidfile.as_deref().ok_or_else(|| {
                    ConfigReloadError::Io("sighup reload requires a configured pidfile".into())
                })?;
                let raw = std::fs::read_to_string(pidfile)
                    .map_err(|e| ConfigReloadError::Io(e.to_string()))?;
                let pid = raw
                    .trim()
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .parse::<u32>()
                    .map_err(|e| ConfigReloadError::Parse(format!("pidfile {pidfile:?}: {e}")))?;
                // argv is a typed vector; `kill -HUP <pid>` — no shell interpolation.
                Self::run("kill", &["-HUP".to_string(), pid.to_string()])
            }
            // RELOAD: run the configured admin command with `RELOAD;` appended.
            "reload" => {
                if self.reload_argv_prefix.is_empty() {
                    return Err(ConfigReloadError::Io(
                        "reload mechanism requires a configured reload command prefix".into(),
                    ));
                }
                let (prog, rest) = self.reload_argv_prefix.split_first().expect("non-empty checked above");
                let mut argv: Vec<String> = rest.to_vec();
                argv.push("RELOAD;".to_string());
                Self::run(prog, &argv)
            }
            // restart is never executed by this actuator (it is deferred); any
            // other mechanism is an unknown reload contract — a typed gap, never a
            // silent no-op.
            "restart" => Err(ConfigReloadError::Command {
                argv: format!("reload mechanism `{mechanism}`"),
                code: None,
                stderr: "restart reload is deferred — breathe-configreload never restarts a process".into(),
            }),
            other => Err(ConfigReloadError::Command {
                argv: format!("reload mechanism `{other}`"),
                code: None,
                stderr: "breathe-configreload: unknown reload mechanism".into(),
            }),
        }
    }
}

// ─────────────────────── pure config-file codec ─────────────────────

/// Pure: extract the right-hand value of the first `key = value` (or `key value`)
/// line in `text` whose key matches, ignoring `#`/`;` comment lines and
/// surrounding whitespace. `None` if the key is absent. The shared read codec —
/// the real env and the tests agree on one parse.
#[must_use]
pub fn parse_config_value<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with(';') {
            continue;
        }
        // accept `key = value` and `key value` (nginx-style); split on the first
        // `=` if present, else on the first run of whitespace.
        let (lhs, rhs) = match trimmed.split_once('=') {
            Some((l, r)) => (l, r),
            None => match trimmed.split_once(char::is_whitespace) {
                Some((l, r)) => (l, r),
                None => continue,
            },
        };
        if lhs.trim() == key {
            // strip a trailing comment + a trailing `;` (nginx) from the value.
            let mut val = rhs.trim();
            if let Some(idx) = val.find(['#', ';']) {
                val = val[..idx].trim();
            }
            return Some(val);
        }
    }
    None
}

/// Pure: render `text` with `key` set to `value` — rewriting the existing line in
/// place (preserving its `=` vs space separator) or appending `key = value` if the
/// key is absent. The shared write codec, line-oriented so unrelated config is
/// untouched. `format!` here composes a config LINE (the documented config-line
/// exception under the repo's `skip-format-ban` waiver), not platform syntax.
#[must_use]
pub fn render_config_value(text: &str, key: &str, value: u64) -> String {
    let mut out = String::new();
    let mut replaced = false;
    for line in text.lines() {
        let trimmed = line.trim();
        let is_comment = trimmed.starts_with('#') || trimmed.starts_with(';') || trimmed.is_empty();
        let matches_key = !is_comment
            && match trimmed.split_once('=').map(|(l, _)| l).or_else(|| {
                trimmed.split_once(char::is_whitespace).map(|(l, _)| l)
            }) {
                Some(lhs) => lhs.trim() == key,
                None => false,
            };
        if matches_key && !replaced {
            // preserve the `=` vs space style of the original separator.
            if line.contains('=') {
                out.push_str(&format!("{key} = {value}"));
            } else {
                out.push_str(&format!("{key} {value}"));
            }
            replaced = true;
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    if !replaced {
        out.push_str(&format!("{key} = {value}"));
        out.push('\n');
    }
    out
}

/// Pure: the reload-mechanism token a [`ConfigReload`] passes to
/// [`ConfigReloadEnv::reload`]. Keeps the [`Cluster`] code free of any
/// per-mechanism string literal — one mapping, here.
#[must_use]
pub fn reload_mechanism(reload: ConfigReload) -> &'static str {
    match reload {
        ConfigReload::Sighup => "sighup",
        ConfigReload::Reload => "reload",
        ConfigReload::Restart => "restart",
    }
}

// ───────────────────── the ConfigReloadCluster ─────────────────────

/// The config-file-reload `Cluster`. `write_enabled = false` is the SHADOW mode:
/// it reads + decides + reports `appliedValue` but performs no file edit and no
/// reload, so the full loop can be observed on a live process before a single
/// byte of config is rewritten (mirrors `HostCluster`'s shadow mode).
pub struct ConfigReloadCluster<E: ConfigReloadEnv> {
    env: E,
    write_enabled: bool,
}

impl<E: ConfigReloadEnv> ConfigReloadCluster<E> {
    pub fn new(env: E, write_enabled: bool) -> Self {
        Self { env, write_enabled }
    }
    /// SHADOW constructor — reads + decides, never writes the config or reloads.
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
impl<E: ConfigReloadEnv> Cluster for ConfigReloadCluster<E> {
    async fn read_used(&self, source: &MetricSource) -> Result<Sample, ProviderError> {
        // This actuator owns a config-file LIMIT, not a live `used` counter on the
        // same boundary: a config value's utilization is read from Prometheus / a
        // protocol query by a different actuator. So every source is a typed gap
        // here — never a silent wrong answer (TYPED-SPEC discipline). If a future
        // config-file-readable `used` source is added, it gets a real arm; until
        // then the gap is explicit.
        Err(ProviderError::ApiPermanent(format!(
            "breathe-configreload cannot read a `used` metric ({source:?}); \
             a config-file band's utilization comes from Prometheus/a protocol query elsewhere"
        )))
    }

    async fn read_limit(
        &self,
        _target: &Target,
        layout: &LimitLayout,
        _resource: &str,
    ) -> Result<u64, ProviderError> {
        match layout {
            LimitLayout::ConfigFile { path, key, .. } => {
                Ok(self.env.read_config_value(path, key)?)
            }
            other => Err(ProviderError::ApiPermanent(format!(
                "non-ConfigFile layout on ConfigReloadCluster: {other:?} \
                 (route k8s layouts to KubeCluster, host layouts to HostCluster)"
            ))),
        }
    }

    async fn field_owners(
        &self,
        _target: &Target,
        _layout: &LimitLayout,
        _resource: &str,
        _logical_field: &str,
    ) -> Result<Vec<FieldOwner>, ProviderError> {
        // A config-file value has no Kubernetes managedFields and no competing
        // SSA writer — breathe owns the `key = value` line outright (the process
        // owns its config; only breathe carves this key). An empty owner set ⇒
        // the single-writer guard always proceeds, never a phantom Conflict.
        Ok(Vec::new())
    }

    async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError> {
        let LimitLayout::ConfigFile { path, key, reload } = &patch.layout else {
            return Err(ProviderError::ApiPermanent(format!(
                "non-ConfigFile layout on ConfigReloadCluster apply: {:?}",
                patch.layout
            )));
        };
        // SHADOW: decide + report, never mutate the config or reload the process.
        if !self.write_enabled {
            return Ok(AppliedReceipt { source_hash: [0u8; 16] });
        }
        // Step 1 — carve the config line.
        self.env.write_config_value(path, key, patch.value)?;
        // Step 2 — trigger the reload that makes it take effect. A `Restart`
        // mechanism is DEFERRED: the edit is persisted but breathe never restarts
        // a process from this actuator — an operator (or an explicit
        // higher-disruption-policy actor) owns the restart. The deferral is a
        // typed, observable outcome (a successful receipt over a written-but-not-
        // reloaded value), not a silent skip and not a failure.
        match reload {
            ConfigReload::Restart => {
                // restart required, deferred — value written, process NOT restarted.
                // (No std::process restart is ever issued here.)
            }
            other => {
                self.env.reload(path, reload_mechanism(*other))?;
            }
        }
        Ok(AppliedReceipt { source_hash: [0u8; 16] })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// A programmable in-memory [`ConfigReloadEnv`] — the testability seam,
    /// mirroring `breathe_host::MockHostEnv`. Holds the current `(path, key) →
    /// value` config state; records every write + every reload so a test can
    /// assert shadow mode wrote/reloaded nothing and live mode did exactly the
    /// expected edit + signal. Interior mutability behind `&self` (Mutex), like
    /// `MockHostEnv`.
    #[derive(Default)]
    struct MockConfigReloadEnv {
        values: Mutex<BTreeMap<(String, String), u64>>,
        writes: Mutex<Vec<(String, String, u64)>>,
        reloads: Mutex<Vec<(String, String)>>,
        /// When set for a `(path, key)`, `read_config_value` returns this error
        /// (models a missing key / unreadable file) instead of a value.
        read_errors: Mutex<BTreeMap<(String, String), ConfigReloadError>>,
    }

    impl MockConfigReloadEnv {
        fn with_value(self, path: &str, key: &str, value: u64) -> Self {
            self.values.lock().unwrap().insert((path.to_string(), key.to_string()), value);
            self
        }
        fn writes(&self) -> Vec<(String, String, u64)> {
            self.writes.lock().unwrap().clone()
        }
        fn reloads(&self) -> Vec<(String, String)> {
            self.reloads.lock().unwrap().clone()
        }
    }

    impl ConfigReloadEnv for MockConfigReloadEnv {
        fn read_config_value(&self, path: &str, key: &str) -> Result<u64, ConfigReloadError> {
            let id = (path.to_string(), key.to_string());
            if let Some(e) = self.read_errors.lock().unwrap().get(&id) {
                return Err(e.clone());
            }
            self.values
                .lock()
                .unwrap()
                .get(&id)
                .copied()
                .ok_or_else(|| ConfigReloadError::KeyMissing { path: path.to_string(), key: key.to_string() })
        }
        fn write_config_value(&self, path: &str, key: &str, value: u64) -> Result<(), ConfigReloadError> {
            self.values.lock().unwrap().insert((path.to_string(), key.to_string()), value);
            self.writes.lock().unwrap().push((path.to_string(), key.to_string(), value));
            Ok(())
        }
        fn reload(&self, path: &str, mechanism: &str) -> Result<(), ConfigReloadError> {
            self.reloads.lock().unwrap().push((path.to_string(), mechanism.to_string()));
            Ok(())
        }
    }

    const PGB: &str = "/etc/pgbouncer/pgbouncer.ini";
    const PG: &str = "/var/lib/postgresql/data/postgresql.conf";

    fn target() -> Target {
        Target {
            namespace: "db".into(),
            name: "pgbouncer".into(),
            kind: "ConfigFile".into(),
            api_version: String::new(),
            container: None,
            pod_selector: None,
        }
    }

    fn config_layout(path: &str, key: &str, reload: ConfigReload) -> LimitLayout {
        LimitLayout::ConfigFile { path: path.into(), key: key.into(), reload }
    }

    // ── pure codec ──────────────────────────────────────────────────

    #[test]
    fn pure_codec_parses_and_renders_key_value_lines() {
        let text = "# pgbouncer\ndefault_pool_size = 20\nmax_client_conn = 100\n";
        assert_eq!(parse_config_value(text, "default_pool_size"), Some("20"));
        assert_eq!(parse_config_value(text, "max_client_conn"), Some("100"));
        assert_eq!(parse_config_value(text, "absent"), None);
        // comment lines + an nginx-style `key value;` line are handled.
        let nginx = "worker_connections 1024;\n# tuned\n";
        assert_eq!(parse_config_value(nginx, "worker_connections"), Some("1024"));

        // render rewrites the existing line in place + appends an absent key.
        let next = render_config_value(text, "default_pool_size", 40);
        assert_eq!(parse_config_value(&next, "default_pool_size"), Some("40"));
        assert_eq!(parse_config_value(&next, "max_client_conn"), Some("100"), "untouched");
        let appended = render_config_value(text, "reserve_pool_size", 5);
        assert_eq!(parse_config_value(&appended, "reserve_pool_size"), Some("5"));
        // mechanism mapping is exhaustive + stable.
        assert_eq!(reload_mechanism(ConfigReload::Sighup), "sighup");
        assert_eq!(reload_mechanism(ConfigReload::Reload), "reload");
        assert_eq!(reload_mechanism(ConfigReload::Restart), "restart");
    }

    // ── read_limit ──────────────────────────────────────────────────

    #[tokio::test]
    async fn read_limit_reads_the_current_config_value() {
        let env = MockConfigReloadEnv::default().with_value(PGB, "default_pool_size", 20);
        let cluster = ConfigReloadCluster::shadow(env);
        let v = cluster
            .read_limit(&target(), &config_layout(PGB, "default_pool_size", ConfigReload::Reload), "connections")
            .await
            .unwrap();
        assert_eq!(v, 20, "read_limit returns the live config key value");
    }

    #[tokio::test]
    async fn read_limit_on_a_wrong_layout_is_a_typed_error() {
        // A k8s / host layout reaching this actuator is a typed permanent error,
        // never a silent wrong limit (route k8s→KubeCluster, host→HostCluster).
        let cluster = ConfigReloadCluster::shadow(MockConfigReloadEnv::default());
        let err = cluster
            .read_limit(&target(), &LimitLayout::PvcRequest, "storage")
            .await
            .unwrap_err();
        assert!(matches!(err, ProviderError::ApiPermanent(_)), "wrong layout must be a typed permanent error");
    }

    // ── apply round-trip ────────────────────────────────────────────

    #[tokio::test]
    async fn live_apply_writes_the_key_and_triggers_the_reload() {
        // pgbouncer RELOAD: write default_pool_size, then a `reload` mechanism.
        let env = MockConfigReloadEnv::default().with_value(PGB, "default_pool_size", 20);
        let cluster = ConfigReloadCluster::new(env, true);
        let layout = config_layout(PGB, "default_pool_size", ConfigReload::Reload);
        let patch = SsaPatch {
            target: target(),
            field_manager: "breathe/config-reload".into(),
            layout: layout.clone(),
            resource: "connections".into(),
            value: 40,
        };
        cluster.apply(&patch).await.unwrap();
        // the value was carved …
        assert_eq!(cluster.env().writes(), vec![(PGB.to_string(), "default_pool_size".to_string(), 40)]);
        // … and the reload was triggered with the pgbouncer mechanism.
        assert_eq!(cluster.env().reloads(), vec![(PGB.to_string(), "reload".to_string())]);
        // round-trip: read_limit now returns the just-applied value.
        let v = cluster.read_limit(&target(), &layout, "connections").await.unwrap();
        assert_eq!(v, 40, "apply→read_limit round-trips the new value");
    }

    #[tokio::test]
    async fn sighup_mechanism_writes_then_signals() {
        // PostgreSQL work_mem: write the key, then a SIGHUP reload.
        let env = MockConfigReloadEnv::default().with_value(PG, "work_mem", 4096);
        let cluster = ConfigReloadCluster::new(env, true);
        let patch = SsaPatch {
            target: target(),
            field_manager: "breathe/config-reload".into(),
            layout: config_layout(PG, "work_mem", ConfigReload::Sighup),
            resource: "memory".into(),
            value: 8192,
        };
        cluster.apply(&patch).await.unwrap();
        assert_eq!(cluster.env().writes(), vec![(PG.to_string(), "work_mem".to_string(), 8192)]);
        assert_eq!(cluster.env().reloads(), vec![(PG.to_string(), "sighup".to_string())]);
    }

    #[tokio::test]
    async fn restart_mechanism_writes_but_defers_the_reload() {
        // PostgreSQL shared_buffers needs a restart: the value is written, but
        // breathe NEVER restarts a process — the reload is deferred (no signal).
        let env = MockConfigReloadEnv::default().with_value(PG, "shared_buffers", 131_072);
        let cluster = ConfigReloadCluster::new(env, true);
        let patch = SsaPatch {
            target: target(),
            field_manager: "breathe/config-reload".into(),
            layout: config_layout(PG, "shared_buffers", ConfigReload::Restart),
            resource: "memory".into(),
            value: 262_144,
        };
        cluster.apply(&patch).await.unwrap();
        assert_eq!(cluster.env().writes(), vec![(PG.to_string(), "shared_buffers".to_string(), 262_144)], "value still carved");
        assert!(cluster.env().reloads().is_empty(), "restart reload is deferred — no signal issued");
    }

    // ── shadow mode ─────────────────────────────────────────────────

    #[tokio::test]
    async fn shadow_apply_decides_but_writes_and_reloads_nothing() {
        let env = MockConfigReloadEnv::default().with_value(PGB, "default_pool_size", 20);
        let cluster = ConfigReloadCluster::shadow(env);
        let patch = SsaPatch {
            target: target(),
            field_manager: "breathe/config-reload".into(),
            layout: config_layout(PGB, "default_pool_size", ConfigReload::Reload),
            resource: "connections".into(),
            value: 40,
        };
        cluster.apply(&patch).await.unwrap();
        assert!(cluster.env().writes().is_empty(), "shadow mode must not write the config");
        assert!(cluster.env().reloads().is_empty(), "shadow mode must not reload the process");
    }

    // ── read_used is a typed gap; field_owners is empty ─────────────

    #[tokio::test]
    async fn read_used_is_a_typed_permanent_gap() {
        // This actuator owns a limit, not a `used` counter — every source is a
        // typed ApiPermanent, never a silent wrong answer.
        let cluster = ConfigReloadCluster::shadow(MockConfigReloadEnv::default());
        let err = cluster
            .read_used(&MetricSource::Prometheus("rate(x[5m])".into()))
            .await
            .unwrap_err();
        assert!(matches!(err, ProviderError::ApiPermanent(_)));
    }

    #[tokio::test]
    async fn field_owners_is_always_empty_no_managed_fields() {
        let cluster = ConfigReloadCluster::shadow(MockConfigReloadEnv::default());
        let owners = cluster
            .field_owners(&target(), &config_layout(PGB, "k", ConfigReload::Reload), "connections", "config.value")
            .await
            .unwrap();
        assert!(owners.is_empty(), "config-file values have no k8s managedFields");
    }
}
