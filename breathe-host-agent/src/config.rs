//! `breathe-host-agent` configuration — the typed surface.
//!
//! ## Why this exists
//!
//! Before this module the agent's entire configuration was **three bare
//! `std::env::var` calls** (`NODE_NAME`, `BREATHE_REQUEUE_SECONDS`,
//! `POD_NAME`) plus `EnvFilter::try_from_default_env()`. Everything else an
//! operator might reasonably want to change was a literal in `main`: the log
//! encoder (`.json()`), the metrics listener (`([0, 0, 0, 0], 9101)`), the
//! controller identity string, and *which host dimensions run at all* — the
//! agent unconditionally started ArcBand, CgroupBand and CgroupCpuBand
//! controllers.
//!
//! That is the shape ★★ CONFIGURATION MANAGEMENT exists to remove: an
//! operator-facing tool whose knobs are a mix of undocumented env vars and
//! recompile-to-change constants. Every operator-facing pleme-io tool
//! configures through shikumi's tiered surface plus the HM/NixOS/Darwin module
//! trio, and this is that surface for the agent.
//!
//! ## The tiers
//!
//! [`shikumi::TieredConfig`] gives four materializations, and the distinction
//! is load-bearing rather than ceremony:
//!
//! * [`TieredConfig::bare`] — the zero-opinion floor. Every field empty/zero,
//!   nothing inferred. This is what a test or a differ wants.
//! * [`TieredConfig::discovered`] — `bare()` overlaid with what the *runtime*
//!   can detect for itself. For this agent that is the Kubernetes downward
//!   API: `NODE_NAME` and `POD_NAME` are injected by the pod spec, not chosen
//!   by an operator, so they belong to discovery rather than to defaults.
//! * [`TieredConfig::prescribed_default`] — the curated first-launch
//!   experience: discovery plus the values that were previously hardcoded.
//!   **A node that sets nothing at all must behave exactly as it did before
//!   this module existed** — that is the migration contract, and
//!   `prescribed_default_matches_legacy_hardcoded_values` pins it.
//! * `Custom(path)` — a YAML overlay on `prescribed_default()`. This is what
//!   the module trio renders and points `BREATHE_HOST_AGENT_CONFIG` at.
//!
//! ## ★ WHAT IS DELIBERATELY *NOT* CONFIGURABLE HERE
//!
//! There is **no local `write_enabled` / `dry_run` knob**, and its absence is a
//! safety property rather than an omission.
//!
//! The agent's shadow-first guarantee is
//! `effective_dry_run = band.dryRun || !pool.writeEnabled`, where
//! `pool.writeEnabled` is the node-level master switch read from the
//! `BreatheNodePool` CRD — i.e. from the CLUSTER, which is auditable, RBAC'd
//! and reconciled. A local file that could force `write_enabled = true` would
//! let a host opt itself out of the cluster's master switch, turning a
//! cluster-wide safety invariant into a per-host suggestion. The whole point
//! of the master switch is that it cannot be overridden from the thing it
//! governs.
//!
//! So this surface configures *how the agent runs* (cadence, telemetry,
//! identity, which dimensions it watches) and never *whether it may write*.
//! Disabling a dimension here stops the agent watching it at all, which is a
//! strictly-safer direction and cannot manufacture a write.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde::{Deserialize, Serialize};
use shikumi::TieredConfig;

/// The env var naming the tier or the YAML overlay path.
///
/// Must stay in lockstep with `mkModuleTrio`'s `shikumiEnvVar`, which derives
/// exactly this spelling from the package name
/// (`breathe-host-agent` → `BREATHE_HOST_AGENT_CONFIG`). Written out rather
/// than derived so a grep for the variable finds it.
pub const CONFIG_ENV_VAR: &str = "BREATHE_HOST_AGENT_CONFIG";

/// How the agent encodes its log lines.
///
/// A closed enum, not a string: an unknown encoder is a deserialize error at
/// startup instead of a silent fallback that leaves an operator staring at the
/// wrong format in a log aggregator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LogFormat {
    /// One JSON object per line — what the agent has always emitted, and what
    /// the fleet's log pipeline parses.
    Json,
    /// Human-readable multi-line. For an operator tailing a unit by hand.
    Pretty,
    /// Human-readable single-line.
    Compact,
}

/// Log emission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct LoggingConfig {
    /// A `tracing_subscriber::EnvFilter` directive string.
    ///
    /// `RUST_LOG` still wins when set — the agent tries
    /// `EnvFilter::try_from_default_env()` first and falls back to this. That
    /// ordering is deliberate: an operator debugging a live node reaches for
    /// `RUST_LOG`, and a config file that silently beat it would be a trap.
    pub filter: String,
    pub format: LogFormat,
}

/// Prometheus exposition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct MetricsConfig {
    /// When false the exporter is never installed.
    ///
    /// Note the agent treats a *failed* install as non-fatal and continues —
    /// so `enabled = true` means "try", never "guaranteed". Keeping the two
    /// distinct is why this is a separate field from the bind address.
    pub enabled: bool,
    /// Bind address. Defaults to `0.0.0.0` because the scrape arrives from
    /// another pod.
    pub address: IpAddr,
    /// Bind port. **9101, not 9100** — 9100 is the host node-exporter, and
    /// colliding with it is the reason this is pinned rather than left to a
    /// convention.
    pub port: u16,
}

impl MetricsConfig {
    /// The listener as a single value, so callers don't re-assemble it.
    #[must_use]
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.address, self.port)
    }
}

/// Which host dimensions this agent watches.
///
/// All three default to `true`, which is exactly the previous unconditional
/// behaviour. Turning one off makes the agent not start that controller at
/// all — useful on a node where, say, ZFS is absent so `ArcBand` can never
/// apply, and where an always-erroring controller is pure noise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct DimensionsConfig {
    /// ZFS ARC (`ArcBand`).
    pub arc: bool,
    /// cgroup memory (`CgroupBand`).
    pub cgroup_memory: bool,
    /// cgroup CPU (`CgroupCpuBand`).
    pub cgroup_cpu: bool,
}

impl DimensionsConfig {
    /// True when every dimension is off — the agent would watch nothing.
    ///
    /// Not an error: a node may legitimately run the agent purely for its
    /// `/metrics` and build-info. But it IS worth a startup warning, because
    /// the far likelier cause is a typo'd YAML key.
    #[must_use]
    pub fn none_enabled(&self) -> bool {
        !self.arc && !self.cgroup_memory && !self.cgroup_cpu
    }
}

/// Node identity — supplied by the Kubernetes downward API, not by an operator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct NodeConfig {
    /// `spec.nodeName`. Empty means the agent matches no `BreatheNodePool`
    /// and therefore reconciles nothing — `main` warns loudly about it.
    pub name: String,
    /// `metadata.name` of the pod, used as the event-recorder instance.
    /// Empty falls back to `name`.
    pub pod_name: String,
}

/// Reconcile cadence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct ReconcileConfig {
    /// Seconds between refreshes. Host metrics are live, so this is a real
    /// polling interval and not just a watch-resync safety net.
    pub requeue_seconds: u64,
    /// The `controller` field on emitted Kubernetes Events. Configurable so
    /// two agents in one cluster (a canary alongside the fleet build) are
    /// distinguishable in `kubectl get events`.
    pub controller_name: String,
}

/// The whole agent configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct HostAgentConfig {
    pub node: NodeConfig,
    pub reconcile: ReconcileConfig,
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
    pub dimensions: DimensionsConfig,
}

// `#[serde(default)]` on every struct above means a YAML overlay may name ONLY
// the keys it wants to change; absent keys fall back to these `Default` impls,
// which delegate to the prescribed tier. That is what makes a two-line
// `metrics: { port: 9201 }` overlay legal instead of demanding the operator
// restate the entire document.
impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            filter: "info,breathe_host_agent=info".to_owned(),
            format: LogFormat::Json,
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 9101,
        }
    }
}

impl Default for DimensionsConfig {
    fn default() -> Self {
        Self {
            arc: true,
            cgroup_memory: true,
            cgroup_cpu: true,
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            pod_name: String::new(),
        }
    }
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        Self {
            requeue_seconds: 30,
            controller_name: "breathe-host-agent".to_owned(),
        }
    }
}

impl Default for HostAgentConfig {
    fn default() -> Self {
        Self::prescribed_default()
    }
}

impl TieredConfig for HostAgentConfig {
    /// Tier 0 — zero opinion. Note `requeue_seconds = 0` and
    /// `metrics.port = 0`: these are genuinely "unset", not "sensible". Nothing
    /// should run from `bare()`; it exists as a diff baseline and a test floor.
    fn bare() -> Self {
        Self {
            node: NodeConfig {
                name: String::new(),
                pod_name: String::new(),
            },
            reconcile: ReconcileConfig {
                requeue_seconds: 0,
                controller_name: String::new(),
            },
            metrics: MetricsConfig {
                enabled: false,
                address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                port: 0,
            },
            logging: LoggingConfig {
                filter: String::new(),
                format: LogFormat::Json,
            },
            dimensions: DimensionsConfig {
                arc: false,
                cgroup_memory: false,
                cgroup_cpu: false,
            },
        }
    }

    /// Tier 1 — `bare()` plus what the runtime knows about itself.
    ///
    /// Only the downward-API identity is discoverable: these two values are
    /// injected by the pod spec and an operator has no business typing them
    /// into a config file. Everything else is a choice, so it lives in tier 2.
    fn discovered() -> Self {
        let mut cfg = Self::bare();
        cfg.node.name = std::env::var("NODE_NAME").unwrap_or_default();
        cfg.node.pod_name = std::env::var("POD_NAME").unwrap_or_default();
        cfg
    }

    /// Tier 2 — discovery plus the curated defaults.
    ///
    /// ★ These values are the ones that used to be literals in `main`. A node
    /// that configures nothing gets byte-identical behaviour to the pre-shikumi
    /// agent; see `prescribed_default_matches_legacy_hardcoded_values`.
    fn prescribed_default() -> Self {
        let discovered = Self::discovered();
        Self {
            node: discovered.node,
            reconcile: ReconcileConfig::default(),
            metrics: MetricsConfig::default(),
            logging: LoggingConfig::default(),
            dimensions: DimensionsConfig::default(),
        }
    }
}

impl HostAgentConfig {
    /// The single startup entry point.
    ///
    /// Resolves the tier from [`CONFIG_ENV_VAR`] — a tier name (`bare` /
    /// `discovered` / `default`) or a path to a YAML overlay, which is what
    /// the module trio writes.
    ///
    /// ★ Discovery is re-applied AFTER the overlay. `resolve_tier(Custom)`
    /// layers YAML onto `prescribed_default()`, and a YAML file that omits
    /// `node:` therefore reinstates whatever `prescribed_default` captured —
    /// which is correct — but a file that *does* carry a stale `node.name`
    /// (copied between hosts, or rendered once by a module and shipped to
    /// several nodes) would pin every node to one identity. The downward API
    /// is the authority on which node this is, so it wins over the file.
    #[must_use]
    pub fn load() -> Self {
        let mut cfg = <Self as TieredConfig>::resolve_from_env(CONFIG_ENV_VAR);
        let discovered = <Self as TieredConfig>::discovered();
        if !discovered.node.name.is_empty() {
            cfg.node.name = discovered.node.name;
        }
        if !discovered.node.pod_name.is_empty() {
            cfg.node.pod_name = discovered.node.pod_name;
        }
        cfg
    }

    /// The event-recorder instance: the pod name, falling back to the node
    /// name, falling back to none. Mirrors the pre-config expression exactly.
    #[must_use]
    pub fn reporter_instance(&self) -> Option<String> {
        if !self.node.pod_name.is_empty() {
            Some(self.node.pod_name.clone())
        } else if !self.node.name.is_empty() {
            Some(self.node.name.clone())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ★ THE MIGRATION CONTRACT.
    ///
    /// Every value here was a literal in `main` before this module existed. If
    /// one of these assertions has to be edited, a node that configures
    /// nothing just changed behaviour on upgrade — which may be fine, but it
    /// is never incidental and must be a deliberate edit.
    #[test]
    fn prescribed_default_matches_legacy_hardcoded_values() {
        let c = HostAgentConfig::prescribed_default();
        assert_eq!(c.reconcile.requeue_seconds, 30, "BREATHE_REQUEUE_SECONDS default");
        assert_eq!(c.reconcile.controller_name, "breathe-host-agent");
        assert!(c.metrics.enabled);
        assert_eq!(c.metrics.port, 9101, "9100 is the host node-exporter");
        assert_eq!(c.metrics.address, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(c.logging.filter, "info,breathe_host_agent=info");
        assert_eq!(c.logging.format, LogFormat::Json);
        assert!(c.dimensions.arc && c.dimensions.cgroup_memory && c.dimensions.cgroup_cpu);
    }

    /// `bare()` must be genuinely inert, or it is just a second set of
    /// defaults wearing a different name.
    #[test]
    fn bare_is_zero_opinion() {
        let c = HostAgentConfig::bare();
        assert_eq!(c.reconcile.requeue_seconds, 0);
        assert!(c.reconcile.controller_name.is_empty());
        assert!(!c.metrics.enabled);
        assert_eq!(c.metrics.port, 0);
        assert!(c.logging.filter.is_empty());
        assert!(c.dimensions.none_enabled());
    }

    /// `Default::default()` is the standard Rust idiom and must not be a third
    ///, subtly-different tier.
    #[test]
    fn default_delegates_to_prescribed() {
        assert_eq!(HostAgentConfig::default(), HostAgentConfig::prescribed_default());
    }

    /// A partial overlay must leave untouched keys at their prescribed values.
    /// This is the property `#[serde(default)]` buys, and it is the difference
    /// between a 2-line operator override and a 40-line restatement.
    #[test]
    fn partial_yaml_overlay_keeps_other_fields_prescribed() {
        let yaml = "metrics:\n  port: 9201\n";
        let overlaid: HostAgentConfig =
            serde_yaml::from_str(yaml).expect("partial overlay must deserialize");
        assert_eq!(overlaid.metrics.port, 9201, "the named key changes");
        assert_eq!(
            overlaid.reconcile.requeue_seconds, 30,
            "an unnamed key keeps its prescribed value"
        );
        assert!(overlaid.dimensions.arc, "an unnamed section stays prescribed");
    }

    /// An unknown log format is a startup error, not a silent fallback to
    /// JSON — the closed-enum property.
    #[test]
    fn unknown_log_format_is_rejected() {
        let yaml = "logging:\n  format: yaml-lol\n";
        assert!(
            serde_yaml::from_str::<HostAgentConfig>(yaml).is_err(),
            "an unknown encoder must fail to parse, never default silently"
        );
    }

    /// Round-trip: what we serialize must deserialize back identically, or the
    /// rendered module-trio YAML and the running config can disagree.
    #[test]
    fn round_trips_through_yaml() {
        let c = HostAgentConfig::prescribed_default();
        let s = serde_yaml::to_string(&c).expect("serialize");
        let back: HostAgentConfig = serde_yaml::from_str(&s).expect("deserialize");
        assert_eq!(c, back);
    }

    #[test]
    fn socket_addr_composes_address_and_port() {
        let m = MetricsConfig::default();
        assert_eq!(m.socket_addr().to_string(), "0.0.0.0:9101");
    }

    #[test]
    fn reporter_instance_prefers_pod_then_node_then_none() {
        let mut c = HostAgentConfig::bare();
        assert_eq!(c.reporter_instance(), None);
        c.node.name = "rio".into();
        assert_eq!(c.reporter_instance().as_deref(), Some("rio"));
        c.node.pod_name = "breathe-host-agent-abcde".into();
        assert_eq!(
            c.reporter_instance().as_deref(),
            Some("breathe-host-agent-abcde")
        );
    }
}
