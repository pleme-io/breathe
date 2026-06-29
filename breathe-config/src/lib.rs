//! breathe-config — the shikumi-backed SERVICE configuration for breathe
//! (M1 of the Urdume-microservice refactor; destination doc:
//! `docs/BREATHE-MICROSERVICE.md`).
//!
//! One typed root — [`BreatheServiceConfig`] — whose [`ScaleConfig`] section
//! selects the **elasticity tier by config, never a hidden fallback** (the
//! ★★ MAGMA-NATIVE "config decides" rule): WHICH executor realizes the durable
//! store ([`StoreConfig`] / [`CacheConfig`]) and HOW reconcile is coordinated
//! across replicas ([`CoordinationConfig`]).
//!
//! [`TieredConfig::prescribed_default`] is the **VERY-SMALL** tier — in-memory
//! store, no cache, single replica — byte-identical to today's behavior with
//! zero external infra, so a controller with no config file runs exactly as
//! before. The **EXTREME** tier (Postgres + Redis + sharding) is the *same*
//! root with different enum arms; M2/M3/M4 wire those arms behind the same
//! `breathe-store` seam. At M1 only the very-small arms are live; every other
//! arm is a typed not-yet-implemented error at startup (the controller's
//! `build_stores`), never a silent downgrade.
//!
//! ## Relationship to the `BreatheConfig` CRD
//!
//! This is **not** the `breathe-crd::BreatheConfig` CRD. That CRD carries
//! fleet-overview RUNTIME knobs visible cluster-wide (the `bcfg` object); this
//! is the per-process SERVICE/scale/infra config. They own disjoint concerns
//! and coexist. Precedence for service config (via shikumi's `ConfigStore`):
//! **config-file > `BREATHE_SVC_*` env > `prescribed_default`** — the committed
//! `ConfigMap` is authoritative, env carries pod overrides, and an absent file
//! falls back to the very-small default. The `BREATHE_SVC_` prefix is
//! deliberately distinct from the controller's legacy direct-read env knobs
//! (`BREATHE_PROMETHEUS_URL`, `BREATHE_REQUEUE_SECONDS`, `BREATHE_CONFIG`): a
//! shared `BREATHE_` prefix would map those to unknown keys and a
//! `deny_unknown_fields` load would reject them — crash-looping the controller.

pub mod envprofile;

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use shikumi::{ConfigStore, TieredConfig};
use thiserror::Error;

/// The env-overlay prefix for the service config. Distinct from the controller's
/// legacy `BREATHE_PROMETHEUS_URL` / `BREATHE_REQUEUE_SECONDS` / `BREATHE_CONFIG`
/// direct-read knobs so they never collide with this `deny_unknown_fields` root
/// (a shared `BREATHE_` prefix mapped them to unknown keys → load rejected them →
/// the controller crash-looped). Nested fields use `__`, e.g.
/// `BREATHE_SVC_SCALE__WINDOW=8`.
const ENV_PREFIX: &str = "BREATHE_SVC_";

/// The breathe service-config root. See the module docs for the precedence rule
/// and the relationship to the `BreatheConfig` CRD.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BreatheServiceConfig {
    /// The elasticity spectrum — store/cache/coordination + bounded knobs.
    #[serde(default)]
    pub scale: ScaleConfig,
}

impl Default for BreatheServiceConfig {
    fn default() -> Self {
        Self::prescribed_default()
    }
}

impl TieredConfig for BreatheServiceConfig {
    /// Tier 0 — the zero-opinion floor. For breathe the floor IS the
    /// very-small tier (there is no opinion below "smallest possible").
    fn bare() -> Self {
        Self {
            scale: ScaleConfig::bare(),
        }
    }

    /// Tier 2 — the prescribed first-launch value: the VERY-SMALL tier,
    /// byte-identical to today.
    fn prescribed_default() -> Self {
        Self {
            scale: ScaleConfig::default(),
        }
    }
}

/// The elasticity spectrum. Every field defaults, so an empty config file is a
/// valid very-small setup; operators override only what they need.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ScaleConfig {
    /// Where durable decision/attestation state lives.
    #[serde(default)]
    pub store: StoreConfig,
    /// The hot-cache + shared rate-bucket tier.
    #[serde(default)]
    pub cache: CacheConfig,
    /// How reconcile is coordinated across replicas.
    #[serde(default)]
    pub coordination: CoordinationConfig,
    /// Predictive sample-window depth (the `LinearTrendPrevisor` window).
    #[serde(default = "default_window")]
    pub window: u32,
    /// Reconcile-concurrency self-protection bound; `0` = the kube-runtime
    /// default. Typed at M1, enforced at M4.
    #[serde(default)]
    pub reconcile_workers: u32,
    /// Which dimensions this instance owns. **Empty = all known dimensions**
    /// (today's single-replica behavior — every controller runs). Typed at M1;
    /// a non-empty list shards controllers across replicas at M4.
    #[serde(default)]
    pub dimensions: Vec<String>,
}

fn default_window() -> u32 {
    6
}

impl ScaleConfig {
    /// The zero-opinion floor — identical to [`Default`] for breathe.
    fn bare() -> Self {
        Self::default()
    }
}

impl Default for ScaleConfig {
    fn default() -> Self {
        Self {
            store: StoreConfig::InMemory,
            cache: CacheConfig::None,
            coordination: CoordinationConfig::SingleReplica,
            window: default_window(),
            reconcile_workers: 0,
            dimensions: Vec::new(),
        }
    }
}

/// Where durable decision/attestation state lives. Adjacently tagged
/// (`type`/`spec`, k8s-idiomatic + serde_yaml-compatible), so YAML is
/// `store: { type: inMemory }` or `store: { type: postgres, spec: { dsn: …, poolMax: 10 } }`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "type", content = "spec", rename_all = "camelCase")]
pub enum StoreConfig {
    /// VERY-SMALL: in-process, byte-identical to today, zero external infra.
    #[default]
    InMemory,
    /// EXTREME: durable decision/attestation state in Postgres (wired at M2).
    Postgres(PostgresConfig),
}

/// Postgres durable-store settings (M2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PostgresConfig {
    /// The connection DSN (redacted in `Debug`/logs). See [`Secret`].
    pub dsn: Secret,
    /// Connection-pool ceiling.
    #[serde(default = "default_pg_pool_max")]
    pub pool_max: u32,
    /// Connection-pool floor.
    #[serde(default = "default_pg_pool_min")]
    pub pool_min: u32,
}

fn default_pg_pool_max() -> u32 {
    16
}
fn default_pg_pool_min() -> u32 {
    1
}

/// The hot-cache + shared rate-bucket tier. YAML: `cache: { type: none }` or
/// `cache: { type: redis, spec: { url: …, ttlSecs: 30 } }`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "type", content = "spec", rename_all = "camelCase")]
pub enum CacheConfig {
    /// VERY-SMALL: no shared cache.
    #[default]
    None,
    /// EXTREME: Redis hot-cache + shared rate-bucket (wired at M3).
    Redis(RedisConfig),
}

/// Redis cache settings (M3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RedisConfig {
    /// The Redis URL (redacted in `Debug`/logs). See [`Secret`].
    pub url: Secret,
    /// Sample-cache key TTL.
    #[serde(default = "default_redis_ttl")]
    pub ttl_secs: u64,
}

fn default_redis_ttl() -> u64 {
    30
}

/// How reconcile is coordinated across replicas. YAML: `coordination: { type:
/// singleReplica }` / `{ type: leaderElection, spec: { leaseMs: 15000 } }` /
/// `{ type: sharded, spec: { replicas: 3, hash: rendezvous } }`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "type", content = "spec", rename_all = "camelCase")]
pub enum CoordinationConfig {
    /// VERY-SMALL: one replica owns every band (the chart's `replicaCount:1`).
    #[default]
    SingleReplica,
    /// INTERMEDIATE: warm-standby HA — only the leader runs the streams (M3).
    LeaderElection(LeaderElectionConfig),
    /// EXTREME: N replicas, each owning a band hash-range (M4).
    Sharded(ShardedConfig),
}

/// Leader-election settings (M3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LeaderElectionConfig {
    /// Lease duration in milliseconds.
    #[serde(default = "default_lease_ms")]
    pub lease_ms: u64,
}

fn default_lease_ms() -> u64 {
    15_000
}

/// Sharding settings (M4).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ShardedConfig {
    /// The replica count the band hash-range is partitioned across.
    pub replicas: u32,
    /// The hash strategy that maps a band to its owning replica.
    #[serde(default)]
    pub hash: HashStrategy,
}

/// The band → replica hashing strategy (M4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum HashStrategy {
    /// Rendezvous (HRW) hashing — minimal reshuffle on replica-count change.
    #[default]
    Rendezvous,
    /// Modulo hashing — simplest, reshuffles most keys on a count change.
    Modulo,
}

/// A secret config value (DSN / URL / password).
///
/// **M1 placeholder:** a redacting string so a secret never leaks via `Debug`
/// or a logged config snapshot. M2 swaps this for `shikumi::secret::SecretSource`
/// (literal / akeyless / sops / vault / env) once the Postgres/Redis backends
/// are wired and the value is resolved at connect time. Unused at M1 (the
/// Postgres/Redis arms fail-fast before any connection).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    /// Wrap a raw secret value.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the raw value. Call sites that touch this must not log it.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// Errors loading the service config.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A shikumi load/parse error (a present-but-malformed config file, an
    /// unreadable path, etc.). A *missing* file is not an error — it yields
    /// [`BreatheServiceConfig::prescribed_default`].
    #[error("shikumi config error: {0}")]
    Shikumi(#[from] shikumi::ShikumiError),
}

/// The in-pod default config path (a mounted `ConfigMap`). Override with the
/// `BREATHE_CONFIG` env var.
#[must_use]
pub fn default_config_path() -> PathBuf {
    std::env::var_os("BREATHE_CONFIG")
        .map_or_else(|| PathBuf::from("/etc/breathe/config.yaml"), PathBuf::from)
}

/// Load + watch the service config: the `BREATHE_SVC_*` env overlay + the config
/// file + hot-reload (`ArcSwap`). A missing file yields
/// [`BreatheServiceConfig::prescribed_default`] (today's behavior); a
/// present-but-malformed file is an `Err` (fail loud, not a silent default).
///
/// **Missing-path resilience:** the underlying notify watcher errors on a
/// non-existent path (e.g. a `ConfigMap` not yet mounted). Rather than crash —
/// which would reintroduce the os-error-2 restart-loop class — this degrades to
/// a one-shot [`load`] (the very-small default) with NO watcher when the path is
/// absent; hot-reload then begins only after a restart finds the file. When the
/// path exists, the returned [`ConfigStore`] owns the watcher + the live
/// snapshot (keep it alive, read via `.get()`); `on_reload` fires on every
/// successful re-parse. Scale changes (store/coordination) are restart-required.
///
/// # Errors
/// Returns [`ConfigError::Shikumi`] if the config file exists but cannot be read
/// or parsed.
pub fn load_and_watch<F>(
    path: &Path,
    on_reload: F,
) -> Result<ConfigStore<BreatheServiceConfig>, ConfigError>
where
    F: Fn(&BreatheServiceConfig) + Send + Sync + 'static,
{
    if path.exists() {
        Ok(ConfigStore::<BreatheServiceConfig>::load_and_watch(
            path, ENV_PREFIX, on_reload,
        )?)
    } else {
        // Absent path ⇒ no watcher (notify would error); one-shot default load.
        Ok(ConfigStore::<BreatheServiceConfig>::load(path, ENV_PREFIX)?)
    }
}

/// Load the service config once, without a hot-reload watcher. Tolerates a
/// missing file (⇒ [`BreatheServiceConfig::prescribed_default`]); a
/// present-but-malformed file is an `Err`. Same precedence as [`load_and_watch`].
/// This is the controller's startup path — scale is restart-required, so no
/// watcher is needed and an absent `ConfigMap` can never crash startup.
///
/// # Errors
/// Returns [`ConfigError::Shikumi`] if the config file exists but cannot be read
/// or parsed.
pub fn load(path: &Path) -> Result<ConfigStore<BreatheServiceConfig>, ConfigError> {
    Ok(ConfigStore::<BreatheServiceConfig>::load(path, ENV_PREFIX)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── defaults + the very-small tier ────────────────────────────────────

    #[test]
    fn prescribed_default_is_the_very_small_tier() {
        let cfg = BreatheServiceConfig::prescribed_default();
        assert_eq!(cfg.scale.store, StoreConfig::InMemory);
        assert_eq!(cfg.scale.cache, CacheConfig::None);
        assert_eq!(cfg.scale.coordination, CoordinationConfig::SingleReplica);
        assert_eq!(cfg.scale.window, 6, "window matches today's hardcoded LinearTrendPrevisor(6)");
        assert_eq!(cfg.scale.reconcile_workers, 0, "0 = kube-runtime default");
        assert!(cfg.scale.dimensions.is_empty(), "empty = all dimensions (today)");
    }

    #[test]
    fn default_delegates_to_prescribed_default_and_bare_equals_it() {
        assert_eq!(BreatheServiceConfig::default(), BreatheServiceConfig::prescribed_default());
        assert_eq!(BreatheServiceConfig::bare(), BreatheServiceConfig::prescribed_default());
    }

    // ── round-trip + empty-yaml ───────────────────────────────────────────

    #[test]
    fn empty_yaml_yields_the_very_small_default() {
        // A pre-existing controller with no config file (empty doc) gets the
        // very-small tier — the backwards-compat guarantee.
        let cfg: BreatheServiceConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg, BreatheServiceConfig::prescribed_default());
    }

    #[test]
    fn full_extreme_config_round_trips() {
        let cfg = BreatheServiceConfig {
            scale: ScaleConfig {
                store: StoreConfig::Postgres(PostgresConfig {
                    dsn: Secret::new("postgres://breathe@db/breathe"),
                    pool_max: 32,
                    pool_min: 2,
                }),
                cache: CacheConfig::Redis(RedisConfig {
                    url: Secret::new("redis://cache:6379"),
                    ttl_secs: 45,
                }),
                coordination: CoordinationConfig::Sharded(ShardedConfig {
                    replicas: 3,
                    hash: HashStrategy::Rendezvous,
                }),
                window: 12,
                reconcile_workers: 8,
                dimensions: vec!["memory".into(), "cpu".into()],
            },
        };
        let y = serde_yaml::to_string(&cfg).unwrap();
        let back: BreatheServiceConfig = serde_yaml::from_str(&y).unwrap();
        assert_eq!(cfg, back);
    }

    // ── enum YAML shapes (operator-facing grammar) ────────────────────────

    #[test]
    fn unit_arms_are_type_only_and_struct_arms_carry_a_spec() {
        let small: BreatheServiceConfig = serde_yaml::from_str(
            "scale:\n  store: { type: inMemory }\n  cache: { type: none }\n  coordination: { type: singleReplica }\n",
        )
        .unwrap();
        assert_eq!(small.scale.store, StoreConfig::InMemory);

        let pg: ScaleConfig =
            serde_yaml::from_str("store:\n  type: postgres\n  spec:\n    dsn: 'postgres://x'\n    poolMax: 20\n").unwrap();
        match pg.store {
            StoreConfig::Postgres(p) => {
                assert_eq!(p.pool_max, 20);
                assert_eq!(p.pool_min, default_pg_pool_min(), "unspecified field takes its serde default");
                assert_eq!(p.dsn.expose(), "postgres://x");
            }
            StoreConfig::InMemory => panic!("expected Postgres"),
        }
    }

    // ── deny_unknown_fields (typo rejection) ──────────────────────────────

    #[test]
    fn unknown_top_level_field_is_rejected() {
        let err = serde_yaml::from_str::<BreatheServiceConfig>("scal: {}\n").unwrap_err();
        assert!(err.to_string().contains("scal"), "msg: {err}");
    }

    #[test]
    fn unknown_scale_field_is_rejected() {
        let err = serde_yaml::from_str::<BreatheServiceConfig>("scale:\n  windwo: 9\n").unwrap_err();
        assert!(err.to_string().contains("windwo"), "msg: {err}");
    }

    #[test]
    fn unknown_postgres_field_is_rejected() {
        let err = serde_yaml::from_str::<ScaleConfig>(
            "store:\n  type: postgres\n  spec:\n    dsn: x\n    poolMaxx: 9\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("poolMaxx"), "msg: {err}");
    }

    // ── secret redaction ──────────────────────────────────────────────────

    #[test]
    fn secret_redacts_in_debug_but_serializes_transparently() {
        let s = Secret::new("hunter2");
        assert_eq!(format!("{s:?}"), "Secret(<redacted>)");
        assert_eq!(serde_yaml::to_string(&s).unwrap().trim(), "hunter2");
        assert_eq!(s.expose(), "hunter2");
    }

    // ── load resilience (the M1 adversarial-verification regressions) ──────

    #[test]
    fn load_missing_file_yields_prescribed_default() {
        // A controller with no mounted ConfigMap must start with the very-small
        // default, NOT crash. (Guards the os-error-2 restart-loop class.)
        let store = load(Path::new("/nonexistent-breathe-cfg-test/config.yaml")).unwrap();
        assert_eq!(**store.get(), BreatheServiceConfig::prescribed_default());
    }

    #[test]
    fn load_and_watch_tolerates_a_missing_path() {
        // The notify watcher errors on an absent path; load_and_watch must degrade
        // to a one-shot default load rather than crash the controller.
        let store = load_and_watch(Path::new("/nonexistent-breathe-cfg-test/config.yaml"), |_| {}).unwrap();
        assert_eq!(**store.get(), BreatheServiceConfig::prescribed_default());
    }

    #[test]
    fn legacy_breathe_env_var_does_not_collide_with_the_config_prefix() {
        // The controller's legacy `BREATHE_PROMETHEUS_URL` shares the `BREATHE_`
        // namespace but NOT the `BREATHE_SVC_` config prefix — so the config load
        // must IGNORE it. A shared prefix + deny_unknown_fields would map it to an
        // unknown key and reject the load → crash-loop (the M1 bug this guards).
        // Safe under parallel tests: the var is ignored by every BREATHE_SVC_ load.
        unsafe { std::env::set_var("BREATHE_PROMETHEUS_URL", "http://collide:9090") };
        let loaded = load(Path::new("/nonexistent-breathe-cfg-test/config.yaml"));
        unsafe { std::env::remove_var("BREATHE_PROMETHEUS_URL") };
        let store = loaded.expect("legacy BREATHE_ env must not collide with the BREATHE_SVC_ config");
        assert_eq!(**store.get(), BreatheServiceConfig::prescribed_default());
    }
}
