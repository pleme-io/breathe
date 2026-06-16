//! `breathe-host` — the **HOST** [`Cluster`] implementation (the *hands*).
//!
//! breathe's k8s boundary is `breathe-kube::KubeCluster`; this is its host peer.
//! It applies host-dimension decisions — ZFS ARC max, a systemd unit's transient
//! cgroup `MemoryHigh` — via sysfs/procfs/`systemctl`, **strictly within the
//! static `pleme.nixos.nodeBudget` L2 envelopes** (the L1-within-L2 contract).
//!
//! The compounding claim holds unchanged: there is no new control logic here.
//! [`HostCluster`] is just another `Cluster`, so the *one* generic
//! `breathe_provider::BandProvider` + the proven `breathe_control::safety_clamp`
//! gate drive host dimensions exactly as they drive k8s ones. The only genuinely
//! new thing is the host *I/O*, and even that is abstracted behind the
//! [`HostEnvironment`] trait — the typed-spec-triplet testability seam, so every
//! decision is exercised against a mock with zero real sysfs/systemd.
//!
//! ### Two safety walls, not one
//! 1. the brain's `safety_clamp` already clamps every proposal to `[floor,
//!    ceiling]` before it is written to a CR; and
//! 2. [`HostCluster::apply`] independently refuses any value above the L2
//!    ceiling it reads from [`NodeEnvelopes`] — so even a mis-authored CR or a
//!    skipped clamp can never push a host lever past the static partition.
//!
//! ### Disjoint from nodeBudget
//! breathe writes only the *runtime* `zfs_arc_max` parameter and *transient*
//! (`--runtime`) cgroup properties; `nodeBudget` owns the boot modprobe ceiling,
//! the static unit `MemoryMax`, and the cpuset pin. The two layers never write
//! the same field, so they compose without contention. On reboot, Nix restores
//! L2 and breathe re-derives its L1 decisions from live metrics.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use breathe_provider::{
    AppliedReceipt, ApplySemantics, Cluster, DimensionDescriptor, DimensionId, Directionality,
    HostKnob, HostMetric, IoMaxField, LimitLayout, MetricSource, ProviderError, Sample, SsaPatch, Target,
};
#[cfg(test)]
use breathe_provider::{PsiKind, PsiResource};

/// `/sys/module/zfs/parameters/zfs_arc_max` — the live ARC ceiling (bytes).
pub const ZFS_ARC_MAX_PATH: &str = "/sys/module/zfs/parameters/zfs_arc_max";
/// `/proc/spl/kstat/zfs/arcstats` — ARC kstats (the `size` row is current bytes).
pub const ZFS_ARCSTATS_PATH: &str = "/proc/spl/kstat/zfs/arcstats";
/// `/proc/meminfo` — kernel memory fields (kB), the `used` source for sysctl bands.
pub const MEMINFO_PATH: &str = "/proc/meminfo";
/// `/sys/module/zfs/parameters/` — the ZFS module-parameter directory (PR-2 ZfsParam).
pub const ZFS_PARAM_DIR: &str = "/sys/module/zfs/parameters/";

/// Map a dotted sysctl `key` (`vm.dirty_bytes`) to its procfs path
/// (`/proc/sys/vm/dirty_bytes`). Pure — the PR-2 `Sysctl` keystone's addressing.
#[must_use]
pub fn sysctl_path(key: &str) -> String {
    let mut p = String::from("/proc/sys/");
    p.push_str(&key.replace('.', "/"));
    p
}

/// Map a ZFS `param` name to its sysfs path. Pure — the PR-2 `ZfsParam` keystone.
#[must_use]
pub fn zfs_param_path(param: &str) -> String {
    let mut p = String::from(ZFS_PARAM_DIR);
    p.push_str(param);
    p
}

/// Map a PSI `resource` (`cpu`/`memory`/`io`) to its pressure file. PR-3.
#[must_use]
pub fn psi_path(resource: &str) -> String {
    let mut p = String::from("/proc/pressure/");
    p.push_str(resource);
    p
}

// ───────────────────────────── errors ──────────────────────────────

/// Typed host-I/O error — never a silent wrong answer (TYPED-SPEC discipline).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostError {
    /// A filesystem read/write failed.
    Io(String),
    /// A value could not be parsed into the expected shape.
    Parse(String),
    /// A `systemctl` invocation exited non-zero.
    Command { argv: String, code: Option<i32>, stderr: String },
    /// No L2 envelope (ceiling) is declared for the addressed host lever.
    NoEnvelope(String),
}

impl std::fmt::Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(m) => write!(f, "host io error: {m}"),
            Self::Parse(m) => write!(f, "host parse error: {m}"),
            Self::Command { argv, code, stderr } => {
                write!(f, "`{argv}` exited {code:?}: {stderr}")
            }
            Self::NoEnvelope(u) => write!(f, "no L2 envelope (ceiling) declared for `{u}`"),
        }
    }
}

impl std::error::Error for HostError {}

impl From<HostError> for ProviderError {
    fn from(e: HostError) -> Self {
        match e {
            // A missing/garbled metric is "metrics missing"; a missing ceiling or
            // a hard command failure is permanent (it will not fix itself on retry).
            HostError::Parse(_) => ProviderError::MetricsMissing,
            HostError::NoEnvelope(m) => ProviderError::ApiPermanent(m),
            HostError::Io(m) => ProviderError::ApiTransient(m),
            cmd @ HostError::Command { .. } => ProviderError::ApiTransient(cmd.to_string()),
        }
    }
}

// ─────────────────────── the side-effect seam ──────────────────────

/// The host I/O boundary — every real sysfs/procfs/systemctl side effect, behind
/// a trait so the [`HostCluster`] decision path is fully exercised against a mock.
/// A `MemoryHigh` of "infinity"/unset is reported as `Ok(None)` (unbounded), which
/// [`HostCluster::read_limit`] maps to the L2 ceiling.
pub trait HostEnvironment: Send + Sync {
    /// Current ZFS ARC size in bytes (the `size` row of arcstats) — the `used`.
    fn read_arcstats_size(&self) -> Result<u64, HostError>;
    /// Read a single-`u64` sysfs file (e.g. `zfs_arc_max`). `0` means "auto".
    fn read_sysfs_u64(&self, path: &str) -> Result<u64, HostError>;
    /// Write a single-`u64` sysfs file.
    fn write_sysfs_u64(&self, path: &str, value: u64) -> Result<(), HostError>;
    /// A systemd unit's cgroup `memory.current` (bytes) — the `used`.
    fn read_cgroup_memory_current(&self, unit: &str) -> Result<u64, HostError>;
    /// A systemd unit's numeric property (e.g. `MemoryHigh`). `None` = unbounded.
    fn read_unit_property_u64(&self, unit: &str, property: &str)
        -> Result<Option<u64>, HostError>;
    /// Set a transient (`--runtime`) systemd property on a unit (e.g. `MemoryHigh`).
    fn set_unit_property_u64(
        &self,
        unit: &str,
        property: &str,
        value: u64,
    ) -> Result<(), HostError>;
    /// A systemd unit's cumulative CPU time in NANOSECONDS (`CPUUsageNSec`) — the
    /// raw counter `HostCluster` differences over a window to derive a cpu RATE.
    fn read_cpu_usage_nsec(&self, unit: &str) -> Result<u64, HostError>;
    /// A systemd unit property as its RAW string (`systemctl show --value`). `None`
    /// = unset/empty. Unlike [`read_unit_property_u64`], the caller parses — needed
    /// for `CPUQuotaPerSecUSec`, which systemd prints as a timespan (`12s`), not an
    /// integer.
    fn read_unit_property_str(&self, unit: &str, property: &str)
        -> Result<Option<String>, HostError>;
    /// Set a transient systemd property to a STRING value (e.g.
    /// `CPUQuota=150%`) — `CPUQuota` is a percentage, not a bare integer, so it
    /// cannot go through [`set_unit_property_u64`].
    fn set_unit_property_str(
        &self,
        unit: &str,
        property: &str,
        value: &str,
    ) -> Result<(), HostError>;
    /// **PR-2:** a named `arcstats` row's data column in bytes (`size`,
    /// `dnode_size`, …). The generic peer of [`read_arcstats_size`](Self::read_arcstats_size).
    fn read_arcstats_row(&self, row: &str) -> Result<u64, HostError>;
    /// **PR-2:** a `/proc/meminfo` field in BYTES (the file prints kB; the impl
    /// multiplies by 1024). `field` is the label without the colon (`Dirty`).
    fn read_meminfo_field(&self, field: &str) -> Result<u64, HostError>;
    /// **PR-3:** the `avg10` PSI stall percentage ×100 (so `12.34%` → `1234`) from
    /// `/proc/pressure/<resource>`'s `<kind>` line. The throttle signal for soft bands.
    fn read_psi_avg10(&self, resource: &str, kind: &str) -> Result<u64, HostError>;
}

/// The real implementation over std `fs` + `systemctl` (argv, never a shell).
///
/// `root` prefixes every sysfs/procfs path so the agent, running in a pod, reads
/// the HOST's `/sys` + `/proc` mounted at `HOST_ROOT` (e.g. `/host`) rather than
/// the container's own. Empty `root` (the default) addresses the real `/` — the
/// shape unit tests and a bare-metal binary use.
///
/// `nsenter_pid` + `systemctl_bin` carry the host-systemd reach: a pod's own
/// `systemctl` cannot talk to the host's systemd, so when `nsenter_pid` is set
/// (the DaemonSet sets `BREATHE_NSENTER_PID=1`) every `systemctl` call is wrapped
/// in `nsenter -t <pid> -m -u -i -n -p -- <systemctl_bin> …`, entering the host's
/// namespaces and running the host's `systemctl` (its absolute path on nixos).
#[derive(Debug, Clone)]
pub struct SystemdSysfsEnv {
    root: String,
    nsenter_pid: Option<u32>,
    systemctl_bin: String,
}

impl Default for SystemdSysfsEnv {
    fn default() -> Self {
        Self { root: String::new(), nsenter_pid: None, systemctl_bin: "systemctl".into() }
    }
}

impl SystemdSysfsEnv {
    /// Read the host-access config from the environment (the DaemonSet sets these):
    /// `HOST_ROOT` (sysfs/procfs prefix), `BREATHE_NSENTER_PID` (host PID to enter,
    /// e.g. `1`), `BREATHE_SYSTEMCTL_BIN` (the host's systemctl path).
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            root: std::env::var("HOST_ROOT").unwrap_or_default(),
            nsenter_pid: std::env::var("BREATHE_NSENTER_PID").ok().and_then(|s| s.parse().ok()),
            systemctl_bin: std::env::var("BREATHE_SYSTEMCTL_BIN").unwrap_or_else(|_| "systemctl".into()),
        }
    }
    /// Construct with an explicit host-root prefix (bare-metal / tests).
    #[must_use]
    pub fn with_root(root: impl Into<String>) -> Self {
        Self { root: root.into(), ..Self::default() }
    }
    /// Prefix an absolute host path with the configured root.
    fn at(&self, abs: &str) -> String {
        format!("{}{}", self.root, abs)
    }

    /// Build the `(program, argv)` to run host `systemctl`, optionally wrapped in
    /// `nsenter` to enter the host's namespaces from a pod. Pure + testable — no
    /// shell, no `format!` of a command line (argv is a typed vector).
    fn systemctl_invocation(
        nsenter_pid: Option<u32>,
        systemctl_bin: &str,
        args: &[&str],
    ) -> (String, Vec<String>) {
        match nsenter_pid {
            Some(pid) => {
                let mut v = vec![
                    "-t".to_string(), pid.to_string(),
                    "-m".into(), "-u".into(), "-i".into(), "-n".into(), "-p".into(),
                    "--".into(), systemctl_bin.to_string(),
                ];
                v.extend(args.iter().map(|s| (*s).to_string()));
                ("nsenter".to_string(), v)
            }
            None => (systemctl_bin.to_string(), args.iter().map(|s| (*s).to_string()).collect()),
        }
    }

    /// Run `systemctl <args…>` (via nsenter when configured) and return trimmed
    /// stdout.
    fn systemctl(&self, args: &[&str]) -> Result<String, HostError> {
        let (prog, argv) = Self::systemctl_invocation(self.nsenter_pid, &self.systemctl_bin, args);
        let out = std::process::Command::new(&prog)
            .args(&argv)
            .output()
            .map_err(|e| HostError::Io(e.to_string()))?;
        if !out.status.success() {
            return Err(HostError::Command {
                argv: format!("{prog} {}", argv.join(" ")),
                code: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
            });
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }
}

impl HostEnvironment for SystemdSysfsEnv {
    fn read_arcstats_size(&self) -> Result<u64, HostError> {
        self.read_arcstats_row("size")
    }

    fn read_arcstats_row(&self, row: &str) -> Result<u64, HostError> {
        let text = std::fs::read_to_string(self.at(ZFS_ARCSTATS_PATH)).map_err(|e| HostError::Io(e.to_string()))?;
        // arcstats rows are `name  type  data`; return the named row's data column.
        for line in text.lines() {
            let mut it = line.split_whitespace();
            if it.next() == Some(row) {
                let raw = it.last().ok_or_else(|| HostError::Parse("arcstats row has no data column".into()))?;
                return raw.parse::<u64>().map_err(|e| HostError::Parse(e.to_string()));
            }
        }
        Err(HostError::Parse("arcstats has no such row".into()))
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
    fn read_psi_avg10(&self, resource: &str, kind: &str) -> Result<u64, HostError> {
        let path = self.at(&psi_path(resource));
        let text = std::fs::read_to_string(&path).map_err(|e| HostError::Io(e.to_string()))?;
        // lines: `some avg10=0.12 avg60=… avg300=… total=…` / `full …`.
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix(kind).map(str::trim_start) {
                for tok in rest.split_whitespace() {
                    if let Some(v) = tok.strip_prefix("avg10=") {
                        let pct: f64 = v.parse().map_err(|_| HostError::Parse("bad PSI avg10".into()))?;
                        return Ok((pct * 100.0).round() as u64); // ×100 → integer per-mille-ish
                    }
                }
            }
        }
        Err(HostError::Parse("PSI line/avg10 not found".into()))
    }

    fn read_meminfo_field(&self, field: &str) -> Result<u64, HostError> {
        let text = std::fs::read_to_string(self.at(MEMINFO_PATH)).map_err(|e| HostError::Io(e.to_string()))?;
        // meminfo rows are `Field:  <kB> kB`; return bytes (kB × 1024). A bare
        // count field (HugePages_Total) has no `kB` suffix and is returned as-is.
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix(field).and_then(|r| r.strip_prefix(':')) {
                let mut it = rest.split_whitespace();
                let raw = it.next().ok_or_else(|| HostError::Parse("meminfo field has no value".into()))?;
                let n = raw.parse::<u64>().map_err(|e| HostError::Parse(e.to_string()))?;
                let is_kb = it.next() == Some("kB");
                return Ok(if is_kb { n.saturating_mul(1024) } else { n });
            }
        }
        Err(HostError::Parse("meminfo has no such field".into()))
    }

    fn read_sysfs_u64(&self, path: &str) -> Result<u64, HostError> {
        let raw = std::fs::read_to_string(self.at(path)).map_err(|e| HostError::Io(e.to_string()))?;
        raw.trim().parse::<u64>().map_err(|e| HostError::Parse(e.to_string()))
    }

    fn write_sysfs_u64(&self, path: &str, value: u64) -> Result<(), HostError> {
        std::fs::write(self.at(path), value.to_string()).map_err(|e| HostError::Io(e.to_string()))
    }

    fn read_cgroup_memory_current(&self, unit: &str) -> Result<u64, HostError> {
        // `systemctl show <unit> -p MemoryCurrent --value` → bytes (or "[not set]").
        let v = self.systemctl(&["show", unit, "-p", "MemoryCurrent", "--value"])?;
        v.trim().parse::<u64>().map_err(|e| HostError::Parse(format!("MemoryCurrent={v:?}: {e}")))
    }

    fn read_unit_property_u64(&self, unit: &str, property: &str) -> Result<Option<u64>, HostError> {
        let v = self.systemctl(&["show", unit, "-p", property, "--value"])?;
        let t = v.trim();
        // systemd reports an unbounded limit as "infinity"; an unset numeric as "".
        if t.is_empty() || t == "infinity" || t == "[not set]" {
            return Ok(None);
        }
        t.parse::<u64>().map(Some).map_err(|e| HostError::Parse(format!("{property}={t:?}: {e}")))
    }

    fn set_unit_property_u64(&self, unit: &str, property: &str, value: u64) -> Result<(), HostError> {
        // argv token `Property=value` is the allowed typed surface (Command::arg).
        let assignment = format!("{property}={value}");
        self.systemctl(&["set-property", "--runtime", unit, &assignment]).map(|_| ())
    }

    fn read_cpu_usage_nsec(&self, unit: &str) -> Result<u64, HostError> {
        // `systemctl show <unit> -p CPUUsageNSec --value` → cumulative ns (or "[not set]").
        let v = self.systemctl(&["show", unit, "-p", "CPUUsageNSec", "--value"])?;
        let t = v.trim();
        if t.is_empty() || t == "[not set]" || t == "infinity" {
            return Err(HostError::Parse(format!("CPUUsageNSec unavailable: {t:?}")));
        }
        t.parse::<u64>().map_err(|e| HostError::Parse(format!("CPUUsageNSec={t:?}: {e}")))
    }

    fn set_unit_property_str(&self, unit: &str, property: &str, value: &str) -> Result<(), HostError> {
        let assignment = format!("{property}={value}");
        self.systemctl(&["set-property", "--runtime", unit, &assignment]).map(|_| ())
    }

    fn read_unit_property_str(&self, unit: &str, property: &str) -> Result<Option<String>, HostError> {
        let v = self.systemctl(&["show", unit, "-p", property, "--value"])?;
        let t = v.trim();
        if t.is_empty() || t == "[not set]" {
            return Ok(None);
        }
        Ok(Some(t.to_string()))
    }
}

// ───────────────────── the L2 ceilings (mirrored) ──────────────────

/// The static L2 ceilings, mirrored from `pleme.nixos.nodeBudget` into the
/// cluster (via `BreatheNodePool` at M3+). Every host write is refused above the
/// ceiling for its lever — the second of breathe's two safety walls.
#[derive(Debug, Clone, Default)]
pub struct NodeEnvelopes {
    /// `nodeBudget.arcMaxGiB` as bytes — the ARC ceiling.
    pub arc_max_bytes: u64,
    /// per-unit `memoryMaxGiB` as bytes — the cgroup `MemoryHigh` ceiling per unit.
    pub cgroup_max_bytes: BTreeMap<String, u64>,
    /// per-unit cpu ceiling in MILLICORES — the cgroup `CPUQuota` ceiling, the cpu
    /// territory the unit may breathe within (`nodeBudget`'s per-unit cpu budget).
    pub cgroup_cpu_max_millicores: BTreeMap<String, u64>,
}

impl NodeEnvelopes {
    /// The L2 ceiling for a host lever (the value a write may never exceed). Bytes
    /// for the memory levers, MILLICORES for the cpu lever — same unit as the
    /// knob's band value, so the [`HostCluster`] ceiling comparison is unit-correct.
    pub fn ceiling_for(&self, knob: &HostKnob) -> Result<u64, HostError> {
        match knob {
            HostKnob::ZfsArcMax => Ok(self.arc_max_bytes),
            HostKnob::CgroupProperty { unit, .. } => self
                .cgroup_max_bytes
                .get(unit)
                .copied()
                .ok_or_else(|| HostError::NoEnvelope(unit.clone())),
            HostKnob::CgroupCpuQuota { unit } => self
                .cgroup_cpu_max_millicores
                .get(unit)
                .copied()
                .ok_or_else(|| HostError::NoEnvelope(unit.clone())),
            // PR-2/PR-4: host-GLOBAL sysctls / ZFS params / io.max rate caps are not
            // nodepool-enveloped — the band's own CRD ceiling is the cap, no L2 wall.
            HostKnob::Sysctl { .. } | HostKnob::ZfsParam { .. } | HostKnob::CgroupIoMax { .. } => Ok(u64::MAX),
        }
    }
}

// ─────────────────────────── HostCluster ───────────────────────────

/// The host `Cluster`. `write_enabled = false` is the SHADOW mode (M3/M4): it
/// reads + decides + reports `appliedValue` but performs no host mutation, so the
/// full loop can be observed on a live node before a single byte is written.
/// A per-unit cpu-usage sample cache: the last `(CPUUsageNSec, Instant)` read for
/// each systemd unit. `CPUUsageNSec` is cumulative, so a RATE is a difference
/// between two reads; rather than sleep inside the read (the library has no async
/// runtime), breathe differences the CURRENT read against the PREVIOUS tick's —
/// the rate then spans the real host tick (≈30s), more accurate than any short
/// in-read window, with no artificial latency. Shared (the long-lived agent owns
/// it; each per-tick `HostCluster` borrows the same handle).
pub type CpuSampleCache = Arc<Mutex<BTreeMap<String, (u64, Instant)>>>;

/// A fresh, empty cpu-sample cache.
#[must_use]
pub fn new_cpu_sample_cache() -> CpuSampleCache {
    Arc::new(Mutex::new(BTreeMap::new()))
}

pub struct HostCluster<H: HostEnvironment> {
    env: H,
    envelopes: NodeEnvelopes,
    write_enabled: bool,
    /// Cross-tick cpu-usage samples (see [`CpuSampleCache`]). `new` gives each
    /// cluster its OWN cache (fine for non-cpu dims + tests); the agent injects a
    /// SHARED one via [`with_cpu_samples`](Self::with_cpu_samples) so the rate
    /// spans ticks.
    cpu_samples: CpuSampleCache,
}

impl<H: HostEnvironment> HostCluster<H> {
    pub fn new(env: H, envelopes: NodeEnvelopes, write_enabled: bool) -> Self {
        Self { env, envelopes, write_enabled, cpu_samples: new_cpu_sample_cache() }
    }
    /// SHADOW constructor — reads + decides, never writes.
    pub fn shadow(env: H, envelopes: NodeEnvelopes) -> Self {
        Self::new(env, envelopes, false)
    }
    /// Inject a SHARED cpu-sample cache (the agent passes its long-lived one so the
    /// cpu RATE is differenced across ticks, not recomputed from an empty cache).
    #[must_use]
    pub fn with_cpu_samples(mut self, cache: CpuSampleCache) -> Self {
        self.cpu_samples = cache;
        self
    }
    pub fn env(&self) -> &H {
        &self.env
    }
    pub fn writes_enabled(&self) -> bool {
        self.write_enabled
    }
}

/// Parse systemd's `CPUQuotaPerSecUSec` value (as `systemctl show --value` prints
/// it: a TIMESPAN like `12s`, `500ms`, `1min 30s`, `1s 500ms`, or `infinity`) into
/// microseconds-per-second. `Ok(None)` = `infinity` (unbounded). `Err(())` =
/// unparseable (a typed error, never a silent wrong cap). systemd's
/// `format_timespan` emits whole-integer components largest-unit-first, so each
/// space-separated token is `<integer><unit>`; we sum them. This keeps the cpu
/// dimension entirely on the systemctl interface (like `read_used`/`CPUUsageNSec`),
/// no cgroup-file dependency.
#[allow(clippy::result_unit_err)]
pub fn parse_cpu_quota_usec(s: &str) -> Result<Option<u64>, ()> {
    let s = s.trim();
    if s == "infinity" {
        return Ok(None);
    }
    let mut total: u64 = 0;
    let mut saw_token = false;
    for tok in s.split_whitespace() {
        saw_token = true;
        let split = tok.find(|c: char| c.is_alphabetic()).ok_or(())?;
        let (num, unit) = tok.split_at(split);
        let n: u64 = num.parse().map_err(|_| ())?;
        let mult: u64 = match unit {
            "us" => 1,
            "ms" => 1_000,
            "s" => 1_000_000,
            "min" => 60_000_000,
            "h" => 3_600_000_000,
            "d" => 86_400_000_000,
            _ => return Err(()),
        };
        total = total.checked_add(n.checked_mul(mult).ok_or(())?).ok_or(())?;
    }
    if saw_token { Ok(Some(total)) } else { Err(()) }
}

/// Pure: cpu RATE in millicores from a `CPUUsageNSec` delta over a wall interval.
/// `delta_nsec` cpu-nanoseconds consumed over `window_nsec` wall-nanoseconds is
/// `delta/window` cores ⇒ `×1000` millicores. `u128` intermediates so a busy
/// many-core unit (large delta) cannot overflow. `window_nsec == 0` ⇒ 0 (no window).
#[must_use]
pub fn cpu_millicores(delta_nsec: u64, window_nsec: u128) -> u64 {
    if window_nsec == 0 {
        return 0;
    }
    let milli = u128::from(delta_nsec) * 1000 / window_nsec;
    u64::try_from(milli).unwrap_or(u64::MAX)
}

/// **PR-4** pure: io RATE (bytes/s or ops/s) from a cumulative-counter `delta`
/// over a `window_nanos` wall interval. `delta` over `window_nanos` ns is
/// `delta·1e9/window` per second. `u128` intermediate; `window_nanos == 0` ⇒ 0.
#[must_use]
pub fn io_rate_per_sec(delta: u64, window_nanos: u128) -> u64 {
    if window_nanos == 0 {
        return 0;
    }
    u64::try_from(u128::from(delta) * 1_000_000_000 / window_nanos).unwrap_or(u64::MAX)
}

/// **PR-4** pure: extract OUR `device`'s cap from a systemd io-property value,
/// which lists `"<dev1> <val1> <dev2> <val2> …"` pairs. `None` if the device
/// isn't present (⇒ no cap for us → read_limit treats it as `u64::MAX`).
#[must_use]
pub fn parse_io_max_for_device(raw: &str, device: &str) -> Option<u64> {
    let mut it = raw.split_whitespace();
    while let (Some(dev), Some(val)) = (it.next(), it.next()) {
        if dev == device {
            return val.parse::<u64>().ok();
        }
    }
    None
}

#[async_trait]
impl<H: HostEnvironment> Cluster for HostCluster<H> {
    async fn read_used(&self, source: &MetricSource) -> Result<Sample, ProviderError> {
        let value = match source {
            MetricSource::Host(HostMetric::ArcSize) => self.env.read_arcstats_size()?,
            // PR-2: generic arcstats row + meminfo field (the `used` for ZFS/sysctl bands).
            MetricSource::Host(HostMetric::ArcKstat { row }) => self.env.read_arcstats_row(row)?,
            MetricSource::Host(HostMetric::MeminfoField { field }) => self.env.read_meminfo_field(field)?,
            MetricSource::Host(HostMetric::CgroupMemoryCurrent { unit }) => {
                self.env.read_cgroup_memory_current(unit)?
            }
            // CPU RATE: difference the cumulative `CPUUsageNSec` against the prior
            // tick's reading (cross-tick cache) → millicores. The FIRST observation
            // per unit has no prior sample, so there is no rate yet — hold with a
            // transient (the next tick differences against this one). No sleep, no
            // runtime dep; the rate spans the real host tick.
            MetricSource::Host(HostMetric::CgroupCpuUsage { unit }) => {
                let nsec_now = self.env.read_cpu_usage_nsec(unit)?;
                let now = Instant::now();
                let mut cache = self
                    .cpu_samples
                    .lock()
                    .map_err(|_| ProviderError::ApiTransient("cpu sample cache poisoned".into()))?;
                match cache.insert(unit.clone(), (nsec_now, now)) {
                    Some((nsec_prev, t_prev)) => {
                        let delta = nsec_now.saturating_sub(nsec_prev);
                        let window = now.saturating_duration_since(t_prev).as_nanos();
                        cpu_millicores(delta, window)
                    }
                    None => return Err(ProviderError::MetricsMissing),
                }
            }
            // PR-4: io RATE — difference the cumulative io-accounting counter
            // (IOReadBytes/IOWriteOperations/…) against the prior tick → bytes/s or
            // ops/s. Reuses the rate-sample cache with an io-namespaced key so io and
            // cpu never collide. First observation per (unit,field) has no prior →
            // hold transient (next tick differences against this one).
            MetricSource::Host(HostMetric::CgroupIoStat { unit, field }) => {
                let counter_now = self.env.read_unit_property_u64(unit, field.counter_property())?.unwrap_or(0);
                let now = Instant::now();
                let key = format!("io:{unit}:{}", field.as_str());
                let mut cache = self
                    .cpu_samples
                    .lock()
                    .map_err(|_| ProviderError::ApiTransient("io sample cache poisoned".into()))?;
                match cache.insert(key, (counter_now, now)) {
                    Some((prev, t_prev)) => {
                        let delta = counter_now.saturating_sub(prev);
                        let window = now.saturating_duration_since(t_prev).as_nanos();
                        io_rate_per_sec(delta, window)
                    }
                    None => return Err(ProviderError::MetricsMissing),
                }
            }
            // PR-3: PSI stall % (×100) — the throttle signal for a ThrottleAware band.
            MetricSource::Host(HostMetric::Psi { resource, kind }) => {
                self.env.read_psi_avg10(resource.as_str(), kind.as_str())?
            }
            // a k8s metric can never reach the host boundary — typed, never silent.
            MetricSource::Prometheus(_) | MetricSource::PodMetricsMax { .. } => {
                return Err(ProviderError::ApiPermanent(
                    "k8s metric source on HostCluster (route k8s dimensions to KubeCluster)".into(),
                ))
            }
        };
        // host reads are live (no scrape window) — always fresh.
        Ok(Sample { value, age_secs: 0 })
    }

    async fn read_limit(
        &self,
        _target: &Target,
        layout: &LimitLayout,
        _resource: &str,
    ) -> Result<u64, ProviderError> {
        match layout {
            LimitLayout::Host(knob @ HostKnob::ZfsArcMax) => {
                let v = self.env.read_sysfs_u64(ZFS_ARC_MAX_PATH)?;
                // `0` = ARC auto-sizing (no explicit cap) → treat as at the L2 ceiling.
                Ok(if v == 0 { self.envelopes.ceiling_for(knob)? } else { v })
            }
            LimitLayout::Host(knob @ HostKnob::CgroupProperty { unit, property }) => {
                // unbounded MemoryHigh (infinity) → start from the L2 ceiling and
                // let the band shrink it toward the setpoint.
                match self.env.read_unit_property_u64(unit, property)? {
                    Some(v) => Ok(v),
                    None => self.envelopes.ceiling_for(knob).map_err(Into::into),
                }
            }
            LimitLayout::Host(knob @ HostKnob::CgroupCpuQuota { unit }) => {
                // systemd prints `CPUQuotaPerSecUSec` as a TIMESPAN (`12s`,
                // `infinity`), NOT raw usec — so read the string + parse it. 1 core
                // = 1_000_000 usec/sec = 1000 millicores ⇒ millicores = usec/sec /
                // 1000. Unset OR infinity ⇒ start from the L2 cpu ceiling and let the
                // band shrink it toward the setpoint.
                match self.env.read_unit_property_str(unit, "CPUQuotaPerSecUSec")? {
                    None => self.envelopes.ceiling_for(knob).map_err(Into::into),
                    Some(raw) => match parse_cpu_quota_usec(&raw) {
                        Ok(Some(usec_per_sec)) => Ok(usec_per_sec / 1000),
                        Ok(None) => self.envelopes.ceiling_for(knob).map_err(Into::into),
                        Err(()) => Err(ProviderError::ApiPermanent(format!(
                            "unparseable CPUQuotaPerSecUSec {raw:?} for {unit}"
                        ))),
                    },
                }
            }
            // PR-2 keystones: read the single-u64 sysfs/procfs file directly.
            LimitLayout::Host(HostKnob::Sysctl { key }) => {
                Ok(self.env.read_sysfs_u64(&sysctl_path(key))?)
            }
            LimitLayout::Host(HostKnob::ZfsParam { param }) => {
                Ok(self.env.read_sysfs_u64(&zfs_param_path(param))?)
            }
            // PR-4: read the per-device io.max cap from the systemd property
            // ("<dev> <val> …" pairs). Unset / no-match for our device → u64::MAX
            // (no cap) so the band snaps it down to the CRD ceiling on the first tick.
            LimitLayout::Host(HostKnob::CgroupIoMax { unit, device, field }) => {
                match self.env.read_unit_property_str(unit, field.cap_property())? {
                    Some(raw) => Ok(parse_io_max_for_device(&raw, device).unwrap_or(u64::MAX)),
                    None => Ok(u64::MAX),
                }
            }
            _ => Err(ProviderError::ApiPermanent(
                "k8s layout on HostCluster (route k8s dimensions to KubeCluster)".into(),
            )),
        }
    }

    async fn field_owners(
        &self,
        _target: &Target,
        _layout: &LimitLayout,
        _resource: &str,
        _logical_field: &str,
    ) -> Result<Vec<breathe_provider::FieldOwner>, ProviderError> {
        // Host levers have no Kubernetes managedFields and no competing writer:
        // the runtime `zfs_arc_max` / transient `MemoryHigh` are breathe-only
        // (nodeBudget owns disjoint static fields). An empty owner set ⇒ the
        // single-writer guard always proceeds, never a phantom Conflict.
        Ok(Vec::new())
    }

    async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError> {
        let LimitLayout::Host(knob) = &patch.layout else {
            return Err(ProviderError::ApiPermanent(
                "k8s layout on HostCluster apply (route k8s dimensions to KubeCluster)".into(),
            ));
        };
        // SAFETY WALL 2: refuse any value above the static L2 ceiling, even if the
        // brain's safety_clamp was skipped or the CR was mis-authored. A host lever
        // can never be pushed past the nodeBudget partition.
        let ceiling = self.envelopes.ceiling_for(knob)?;
        if patch.value > ceiling {
            return Err(ProviderError::ApiPermanent(format!(
                "host value {} exceeds L2 ceiling {} for {:?} — refused",
                patch.value, ceiling, knob
            )));
        }
        // SHADOW: decide + report, never mutate the host.
        if !self.write_enabled {
            return Ok(AppliedReceipt { source_hash: [0u8; 16] });
        }
        match knob {
            HostKnob::ZfsArcMax => self.env.write_sysfs_u64(ZFS_ARC_MAX_PATH, patch.value)?,
            HostKnob::CgroupProperty { unit, property } => {
                self.env.set_unit_property_u64(unit, property, patch.value)?;
            }
            HostKnob::CgroupCpuQuota { unit } => {
                // CPUQuota is a PERCENTAGE: 1000 millicores = 1 core = 100%. Render
                // millicores → percent (floor; sub-1% precision is immaterial for a
                // bandwidth cap). `set-property --runtime <unit> CPUQuota=<p>%`.
                let percent = patch.value / 10;
                self.env.set_unit_property_str(unit, "CPUQuota", &format!("{percent}%"))?;
            }
            // PR-2 keystones: write the single-u64 sysfs/procfs file directly.
            HostKnob::Sysctl { key } => self.env.write_sysfs_u64(&sysctl_path(key), patch.value)?,
            HostKnob::ZfsParam { param } => self.env.write_sysfs_u64(&zfs_param_path(param), patch.value)?,
            // PR-4: set the per-device io.max cap — `IOWriteBandwidthMax="<dev> <val>"`
            // sets just this device's field, leaving the others untouched.
            HostKnob::CgroupIoMax { unit, device, field } => {
                self.env.set_unit_property_str(unit, field.cap_property(), &format!("{device} {}", patch.value))?;
            }
        }
        Ok(AppliedReceipt { source_hash: [0u8; 16] })
    }
}

// ─────────────────────── the host descriptors ──────────────────────

/// **ARC** — bidirectional; the ZFS ARC ceiling. `used` = arcstats `size`,
/// `limit` = `zfs_arc_max`, carved within `nodeBudget.arcMaxGiB`. This is the
/// safest host lever (shrinking ARC frees page-cache immediately + is instantly
/// revertible), so it is the first to go live (M4).
#[derive(Default)]
pub struct ArcDescriptor;
impl DimensionDescriptor for ArcDescriptor {
    fn id(&self) -> DimensionId {
        DimensionId::Arc
    }
    fn directionality(&self) -> Directionality {
        Directionality::Bidirectional
    }
    fn field_manager(&self) -> &'static str {
        "breathe/arc"
    }
    fn logical_field(&self) -> &'static str {
        "host.zfs.arc_max"
    }
    fn resource(&self) -> &'static str {
        "memory"
    }
    fn semantics(&self) -> ApplySemantics {
        ApplySemantics::ContinuousReconciliation
    }
    fn layout(&self, _target: &Target) -> LimitLayout {
        LimitLayout::Host(HostKnob::ZfsArcMax)
    }
    fn metric_source(&self, _target: &Target) -> MetricSource {
        MetricSource::Host(HostMetric::ArcSize)
    }
}

/// **Cgroup memory** — bidirectional; a systemd unit's transient `MemoryHigh`
/// high-water. `used` = the unit's cgroup `memory.current`, `limit` = its
/// `MemoryHigh`, carved within `nodeBudget`'s per-unit `memoryMaxGiB`. The unit
/// is the band's `Target.name` (e.g. `nix-daemon.service`).
#[derive(Default)]
pub struct CgroupMemoryDescriptor;
impl DimensionDescriptor for CgroupMemoryDescriptor {
    fn id(&self) -> DimensionId {
        DimensionId::Cgroup
    }
    fn directionality(&self) -> Directionality {
        Directionality::Bidirectional
    }
    fn field_manager(&self) -> &'static str {
        "breathe/cgroup"
    }
    fn logical_field(&self) -> &'static str {
        "host.cgroup.memory_high"
    }
    fn resource(&self) -> &'static str {
        "memory"
    }
    fn semantics(&self) -> ApplySemantics {
        ApplySemantics::ContinuousReconciliation
    }
    fn layout(&self, target: &Target) -> LimitLayout {
        LimitLayout::Host(HostKnob::CgroupProperty {
            unit: target.name.clone(),
            property: "MemoryHigh".into(),
        })
    }
    fn metric_source(&self, target: &Target) -> MetricSource {
        MetricSource::Host(HostMetric::CgroupMemoryCurrent { unit: target.name.clone() })
    }
}

/// **Cgroup CPU** — bidirectional; a systemd unit's transient `CPUQuota` bandwidth
/// cap, the host-plane peer of `pod-cpu-resize`. `used` = the unit's cpu RATE in
/// millicores (differenced from cumulative `CPUUsageNSec`), `limit` = its
/// `CPUQuota` in millicores, carved within `nodeBudget`'s per-unit cpu territory.
/// The unit is the band's `Target.name` (e.g. `nix-daemon.service`). `RestartFree`
/// — a live cgroup bandwidth change never restarts the unit, so it ticks at the
/// fast golden cadence.
#[derive(Default)]
pub struct CgroupCpuDescriptor;
impl DimensionDescriptor for CgroupCpuDescriptor {
    fn id(&self) -> DimensionId {
        DimensionId::CgroupCpu
    }
    fn directionality(&self) -> Directionality {
        Directionality::Bidirectional
    }
    fn field_manager(&self) -> &'static str {
        "breathe/cgroup-cpu"
    }
    fn logical_field(&self) -> &'static str {
        "host.cgroup.cpu_quota"
    }
    fn resource(&self) -> &'static str {
        "cpu"
    }
    fn semantics(&self) -> ApplySemantics {
        ApplySemantics::ContinuousReconciliation
    }
    fn layout(&self, target: &Target) -> LimitLayout {
        LimitLayout::Host(HostKnob::CgroupCpuQuota { unit: target.name.clone() })
    }
    fn metric_source(&self, target: &Target) -> MetricSource {
        MetricSource::Host(HostMetric::CgroupCpuUsage { unit: target.name.clone() })
    }
}

/// **HostParam** — the GENERIC, data-driven host-parameter descriptor (PR-2).
/// Unlike the hand-written [`ArcDescriptor`]/[`CgroupMemoryDescriptor`] (each a
/// fixed knob), this ONE descriptor carries its `knob` (`Sysctl{key}` /
/// `ZfsParam{param}`), its `metric` (`MeminfoField`/`ArcKstat`), and its
/// `directionality` as DATA — so every sysctl / ZFS-param band (`vm.dirty_bytes`,
/// `zfs_arc_min`, `net.core.rmem_max`, `vm.min_free_kbytes`, …) is an *instance*,
/// not new code. The value is a bare `u64` read/written straight through the
/// sysfs/procfs seam (no Unit codec — host params are integers, not k8s
/// quantities). The realization of the census's "two generic arms collapse the
/// whole sysctl/ZFS family to data."
pub struct HostParamDescriptor {
    pub knob: HostKnob,
    pub metric: HostMetric,
    pub dir: Directionality,
}

impl HostParamDescriptor {
    /// A bidirectional sysctl band by dotted key + its `/proc/meminfo` `used` field.
    #[must_use]
    pub fn sysctl(key: impl Into<String>, meminfo_field: impl Into<String>, dir: Directionality) -> Self {
        Self {
            knob: HostKnob::Sysctl { key: key.into() },
            metric: HostMetric::MeminfoField { field: meminfo_field.into() },
            dir,
        }
    }
    /// A ZFS-parameter band by param name + its arcstats `used` row.
    #[must_use]
    pub fn zfs_param(param: impl Into<String>, arcstats_row: impl Into<String>, dir: Directionality) -> Self {
        Self {
            knob: HostKnob::ZfsParam { param: param.into() },
            metric: HostMetric::ArcKstat { row: arcstats_row.into() },
            dir,
        }
    }
}

impl DimensionDescriptor for HostParamDescriptor {
    fn id(&self) -> DimensionId {
        DimensionId::HostParam
    }
    fn directionality(&self) -> Directionality {
        self.dir
    }
    fn field_manager(&self) -> &'static str {
        // Host levers have no k8s managedFields + each band writes a DISTINCT
        // file, so a shared manager never creates contention (field_owners is
        // empty for the host boundary). Disjointness is by file, not by manager.
        "breathe/host-param"
    }
    fn logical_field(&self) -> &'static str {
        "host.param"
    }
    fn resource(&self) -> &'static str {
        "memory"
    }
    fn semantics(&self) -> ApplySemantics {
        ApplySemantics::ContinuousReconciliation
    }
    fn layout(&self, _target: &Target) -> LimitLayout {
        LimitLayout::Host(self.knob.clone())
    }
    fn metric_source(&self, _target: &Target) -> MetricSource {
        MetricSource::Host(self.metric.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use breathe_provider::{BandProvider, ResourceProvider};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    const GI: u64 = 1024 * 1024 * 1024;

    /// A programmable in-memory [`HostEnvironment`] — the testability seam. Records
    /// every write so a test can assert shadow mode wrote nothing / live mode wrote
    /// exactly the clamped value. `cpu_usage_nsec` is a QUEUE of successive
    /// cumulative readings (each `read_cpu_usage_nsec` pops one) so a test can drive
    /// the cross-tick rate; `str_writes` records `set_unit_property_str` calls.
    #[derive(Default)]
    struct MockHostEnv {
        arc_size: u64,
        sysfs: Mutex<BTreeMap<String, u64>>,
        cgroup_current: BTreeMap<String, u64>,
        unit_property: BTreeMap<(String, String), Option<u64>>,
        unit_property_str: BTreeMap<(String, String), String>,
        writes: Mutex<Vec<(String, u64)>>,
        cpu_usage_nsec: Mutex<VecDeque<u64>>,
        str_writes: Mutex<Vec<(String, String)>>,
        arcstats: BTreeMap<String, u64>,
        meminfo: BTreeMap<String, u64>,
        psi: BTreeMap<(String, String), u64>,
    }

    impl MockHostEnv {
        fn writes(&self) -> Vec<(String, u64)> {
            self.writes.lock().unwrap().clone()
        }
        fn str_writes(&self) -> Vec<(String, String)> {
            self.str_writes.lock().unwrap().clone()
        }
    }

    impl HostEnvironment for MockHostEnv {
        fn read_arcstats_size(&self) -> Result<u64, HostError> {
            Ok(self.arc_size)
        }
        fn read_sysfs_u64(&self, path: &str) -> Result<u64, HostError> {
            Ok(self.sysfs.lock().unwrap().get(path).copied().unwrap_or(0))
        }
        fn write_sysfs_u64(&self, path: &str, value: u64) -> Result<(), HostError> {
            self.sysfs.lock().unwrap().insert(path.to_string(), value);
            self.writes.lock().unwrap().push((path.to_string(), value));
            Ok(())
        }
        fn read_cgroup_memory_current(&self, unit: &str) -> Result<u64, HostError> {
            Ok(self.cgroup_current.get(unit).copied().unwrap_or(0))
        }
        fn read_unit_property_u64(&self, unit: &str, property: &str) -> Result<Option<u64>, HostError> {
            Ok(self.unit_property.get(&(unit.to_string(), property.to_string())).copied().flatten())
        }
        fn set_unit_property_u64(&self, unit: &str, property: &str, value: u64) -> Result<(), HostError> {
            self.writes.lock().unwrap().push((format!("{unit}:{property}"), value));
            Ok(())
        }
        fn read_cpu_usage_nsec(&self, _unit: &str) -> Result<u64, HostError> {
            self.cpu_usage_nsec
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| HostError::Parse("no CPUUsageNSec queued".into()))
        }
        fn read_unit_property_str(&self, unit: &str, property: &str) -> Result<Option<String>, HostError> {
            Ok(self.unit_property_str.get(&(unit.to_string(), property.to_string())).cloned())
        }
        fn set_unit_property_str(&self, unit: &str, property: &str, value: &str) -> Result<(), HostError> {
            self.str_writes.lock().unwrap().push((format!("{unit}:{property}"), value.to_string()));
            Ok(())
        }
        fn read_arcstats_row(&self, row: &str) -> Result<u64, HostError> {
            self.arcstats.get(row).copied().ok_or_else(|| HostError::Parse("no such arcstats row".into()))
        }
        fn read_meminfo_field(&self, field: &str) -> Result<u64, HostError> {
            self.meminfo.get(field).copied().ok_or_else(|| HostError::Parse("no such meminfo field".into()))
        }
        fn read_psi_avg10(&self, resource: &str, kind: &str) -> Result<u64, HostError> {
            self.psi.get(&(resource.to_string(), kind.to_string())).copied()
                .ok_or_else(|| HostError::Parse("no such PSI".into()))
        }
    }

    fn envelopes() -> NodeEnvelopes {
        let mut cgroup = BTreeMap::new();
        cgroup.insert("nix-daemon.service".to_string(), 12 * GI); // nodeBudget memoryMaxGiB = 12
        let mut cpu = BTreeMap::new();
        cpu.insert("nix-daemon.service".to_string(), 8000); // nodeBudget cpu territory = 8 cores
        NodeEnvelopes { arc_max_bytes: 6 * GI, cgroup_max_bytes: cgroup, cgroup_cpu_max_millicores: cpu }
    }

    fn node_target() -> Target {
        Target { namespace: String::new(), name: "rio".into(), kind: "Node".into(), api_version: String::new(), container: None, pod_selector: None }
    }
    fn unit_target(unit: &str) -> Target {
        Target { namespace: String::new(), name: unit.into(), kind: "HostUnit".into(), api_version: String::new(), container: None, pod_selector: None }
    }

    #[test]
    fn pr2_path_helpers_map_keys_to_procfs_sysfs() {
        assert_eq!(sysctl_path("vm.dirty_bytes"), "/proc/sys/vm/dirty_bytes");
        assert_eq!(sysctl_path("net.core.rmem_max"), "/proc/sys/net/core/rmem_max");
        assert_eq!(zfs_param_path("zfs_arc_min"), "/sys/module/zfs/parameters/zfs_arc_min");
    }

    #[tokio::test]
    async fn pr2_sysctl_and_zfsparam_keystones_read_and_write_via_generic_arms() {
        use breathe_provider::Cluster;
        let env = MockHostEnv::default();
        env.sysfs.lock().unwrap().insert(sysctl_path("vm.dirty_bytes"), 200 * GI / 1024); // 200Mi
        env.sysfs.lock().unwrap().insert(zfs_param_path("zfs_arc_min"), GI);
        let cluster = HostCluster::new(env, envelopes(), true); // write-enabled

        // READ a generic sysctl + a generic ZFS param through the one arm.
        let dirty = cluster
            .read_limit(&node_target(), &LimitLayout::Host(HostKnob::Sysctl { key: "vm.dirty_bytes".into() }), "memory")
            .await
            .unwrap();
        assert_eq!(dirty, 200 * GI / 1024);
        let arc_min = cluster
            .read_limit(&node_target(), &LimitLayout::Host(HostKnob::ZfsParam { param: "zfs_arc_min".into() }), "memory")
            .await
            .unwrap();
        assert_eq!(arc_min, GI);

        // WRITE a generic sysctl — lands at the mapped procfs path (no L2 wall:
        // host-global params are governed by the band ceiling, ceiling_for=MAX).
        let patch = SsaPatch {
            target: node_target(),
            field_manager: "breathe/sysctl".into(),
            layout: LimitLayout::Host(HostKnob::Sysctl { key: "vm.dirty_bytes".into() }),
            resource: "memory".into(),
            value: 256 * GI / 1024,
        };
        cluster.apply(&patch).await.unwrap();
        assert!(
            cluster.env().writes().iter().any(|(p, v)| p == &sysctl_path("vm.dirty_bytes") && *v == 256 * GI / 1024),
            "the generic Sysctl arm wrote the mapped procfs path"
        );
    }

    #[tokio::test]
    async fn pr2_generic_metrics_read_arcstats_rows_and_meminfo_fields() {
        use breathe_provider::Cluster;
        let mut env = MockHostEnv::default();
        env.arcstats.insert("dnode_size".into(), 64 * GI / 1024);
        env.meminfo.insert("Dirty".into(), 50 * GI / 1024); // mock stores bytes
        let cluster = HostCluster::shadow(env, envelopes());
        let dnode = cluster.read_used(&MetricSource::Host(HostMetric::ArcKstat { row: "dnode_size".into() })).await.unwrap();
        assert_eq!(dnode.value, 64 * GI / 1024);
        let dirty = cluster.read_used(&MetricSource::Host(HostMetric::MeminfoField { field: "Dirty".into() })).await.unwrap();
        assert_eq!(dirty.value, 50 * GI / 1024);
    }

    #[tokio::test]
    async fn pr3_psi_metric_reads_the_stall_signal() {
        use breathe_provider::Cluster;
        let mut env = MockHostEnv::default();
        env.psi.insert(("io".into(), "some".into()), 1234); // 12.34% stall ×100
        let cluster = HostCluster::shadow(env, envelopes());
        let s = cluster
            .read_used(&MetricSource::Host(HostMetric::Psi { resource: PsiResource::Io, kind: PsiKind::Some }))
            .await
            .unwrap();
        assert_eq!(s.value, 1234, "PSI avg10 ×100 is the throttle signal");
        // the enums map to the right procfs basenames + line prefixes.
        assert_eq!(PsiResource::Memory.as_str(), "memory");
        assert_eq!(PsiKind::Full.as_str(), "full");
        assert_eq!(super::psi_path("io"), "/proc/pressure/io");
    }

    #[test]
    fn pr4_io_helpers_parse_per_device_caps_and_compute_rates() {
        // systemd lists "<dev> <val> <dev> <val>" — pick OUR device.
        assert_eq!(parse_io_max_for_device("8:0 10000000 259:0 50000000", "259:0"), Some(50_000_000));
        assert_eq!(parse_io_max_for_device("8:0 10000000", "259:0"), None);
        // rate = delta·1e9/window_nanos. 100MB over 1s = 100MB/s.
        assert_eq!(io_rate_per_sec(100_000_000, 1_000_000_000), 100_000_000);
        assert_eq!(io_rate_per_sec(5, 0), 0); // no window
        // the four fields map to the right systemd cap + counter properties.
        assert_eq!(IoMaxField::Wbps.cap_property(), "IOWriteBandwidthMax");
        assert_eq!(IoMaxField::Wbps.counter_property(), "IOWriteBytes");
        assert_eq!(IoMaxField::Riops.cap_property(), "IOReadIOPSMax");
        assert_eq!(IoMaxField::Riops.counter_property(), "IOReadOperations");
    }

    #[tokio::test]
    async fn pr4_cgroup_io_max_reads_per_device_cap_and_writes_it() {
        use breathe_provider::Cluster;
        // current IOWriteBandwidthMax for two devices; we carve 259:0 (wbps).
        let mut env = MockHostEnv::default();
        env.unit_property_str.insert(
            ("nix-daemon.service".into(), "IOWriteBandwidthMax".into()),
            "8:0 10000000 259:0 50000000".into(),
        );
        let cluster = HostCluster::new(env, envelopes(), true);
        let knob = HostKnob::CgroupIoMax {
            unit: "nix-daemon.service".into(),
            device: "259:0".into(),
            field: IoMaxField::Wbps,
        };
        let v = cluster.read_limit(&unit_target("nix-daemon.service"), &LimitLayout::Host(knob.clone()), "memory").await.unwrap();
        assert_eq!(v, 50_000_000, "reads OUR device's wbps cap");
        // apply writes "<device> <value>" via the IOWriteBandwidthMax property.
        let patch = SsaPatch {
            target: unit_target("nix-daemon.service"),
            field_manager: "breathe/io".into(),
            layout: LimitLayout::Host(knob),
            resource: "memory".into(),
            value: 40_000_000,
        };
        cluster.apply(&patch).await.unwrap();
        assert!(
            cluster.env().str_writes().iter().any(|(p, v)| p == "nix-daemon.service:IOWriteBandwidthMax" && v == "259:0 40000000"),
            "wrote the per-device wbps cap"
        );
    }

    #[tokio::test]
    async fn host_param_sysctl_band_observes_and_decides_through_the_generic_descriptor() {
        // The PR-2 payoff: a vm.dirty_bytes band is ONE generic descriptor
        // instance (no new code) and breathes through the SAME BandProvider as
        // memory/arc/cgroup. used = meminfo Dirty, limit = the live sysctl value.
        let mut env = MockHostEnv::default();
        env.meminfo.insert("Dirty".into(), 180 * GI / 1024); // 180Mi dirty
        env.sysfs.lock().unwrap().insert(sysctl_path("vm.dirty_bytes"), 200 * GI / 1024); // limit 200Mi
        let desc = HostParamDescriptor::sysctl("vm.dirty_bytes", "Dirty", Directionality::Bidirectional);
        assert_eq!(desc.id(), DimensionId::HostParam);
        assert_eq!(desc.directionality(), Directionality::Bidirectional);
        let provider = BandProvider::new(HostCluster::shadow(env, envelopes()), desc);
        let obs = provider.observe(&node_target()).await.unwrap();
        assert_eq!(obs.used, 180 * GI / 1024);
        assert_eq!(obs.capacity, 200 * GI / 1024, "capacity = the live sysctl value");
        assert!(obs.owners.is_empty(), "host levers have no competing owner");
    }

    #[tokio::test]
    async fn host_param_zfs_band_can_restrict_directionality_to_grow_only() {
        // zfs_arc_min is a PROTECTION floor — GrowOnly: the family is bidirectional,
        // this instance restricts. Proves per-instance directionality flows as data.
        let desc = HostParamDescriptor::zfs_param("zfs_arc_min", "arc_meta_min", Directionality::GrowOnly);
        assert_eq!(desc.directionality(), Directionality::GrowOnly);
        assert!(matches!(desc.layout(&node_target()), LimitLayout::Host(HostKnob::ZfsParam { .. })));
    }

    #[tokio::test]
    async fn arc_observe_reads_size_and_current_cap_through_the_generic_provider() {
        let env = MockHostEnv { arc_size: 5 * GI, ..Default::default() };
        // current zfs_arc_max = 6 GiB (the L2 ceiling)
        env.sysfs.lock().unwrap().insert(ZFS_ARC_MAX_PATH.to_string(), 6 * GI);
        let provider = BandProvider::new(HostCluster::shadow(env, envelopes()), ArcDescriptor);
        let obs = provider.observe(&node_target()).await.unwrap();
        assert_eq!(obs.used, 5 * GI);
        assert_eq!(obs.capacity, 6 * GI);
        assert!(obs.owners.is_empty(), "host levers have no competing owner");
    }

    #[tokio::test]
    async fn shadow_mode_decides_but_writes_nothing() {
        let env = MockHostEnv { arc_size: 3 * GI, ..Default::default() };
        env.sysfs.lock().unwrap().insert(ZFS_ARC_MAX_PATH.to_string(), 6 * GI);
        let cluster = HostCluster::shadow(env, envelopes());
        let patch = SsaPatch {
            target: node_target(),
            field_manager: "breathe/arc".into(),
            layout: LimitLayout::Host(HostKnob::ZfsArcMax),
            resource: "memory".into(),
            value: 4 * GI,
        };
        cluster.apply(&patch).await.unwrap();
        assert!(cluster.env().writes().is_empty(), "shadow mode must not write the host");
    }

    #[tokio::test]
    async fn live_mode_writes_within_ceiling() {
        let env = MockHostEnv::default();
        let cluster = HostCluster::new(env, envelopes(), true);
        let patch = SsaPatch {
            target: node_target(),
            field_manager: "breathe/arc".into(),
            layout: LimitLayout::Host(HostKnob::ZfsArcMax),
            resource: "memory".into(),
            value: 4 * GI, // ≤ 6 GiB ceiling
        };
        cluster.apply(&patch).await.unwrap();
        assert_eq!(cluster.env().writes(), vec![(ZFS_ARC_MAX_PATH.to_string(), 4 * GI)]);
    }

    #[tokio::test]
    async fn the_second_safety_wall_refuses_over_ceiling_and_writes_nothing() {
        let env = MockHostEnv::default();
        let cluster = HostCluster::new(env, envelopes(), true);
        let patch = SsaPatch {
            target: node_target(),
            field_manager: "breathe/arc".into(),
            layout: LimitLayout::Host(HostKnob::ZfsArcMax),
            resource: "memory".into(),
            value: 8 * GI, // > 6 GiB ceiling — must be refused
        };
        let err = cluster.apply(&patch).await.unwrap_err();
        assert!(matches!(err, ProviderError::ApiPermanent(_)), "over-ceiling host write must be refused");
        assert!(cluster.env().writes().is_empty(), "a refused write must not touch the host");
    }

    #[tokio::test]
    async fn cgroup_descriptor_addresses_the_unit_from_the_target() {
        let d = CgroupMemoryDescriptor;
        let t = unit_target("nix-daemon.service");
        match d.layout(&t) {
            LimitLayout::Host(HostKnob::CgroupProperty { unit, property }) => {
                assert_eq!(unit, "nix-daemon.service");
                assert_eq!(property, "MemoryHigh");
            }
            other => panic!("expected a cgroup MemoryHigh lever, got {other:?}"),
        }
        match d.metric_source(&t) {
            MetricSource::Host(HostMetric::CgroupMemoryCurrent { unit }) => {
                assert_eq!(unit, "nix-daemon.service");
            }
            other => panic!("expected cgroup memory.current, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unbounded_memory_high_reads_as_the_l2_ceiling() {
        // MemoryHigh unset (infinity) ⇒ read_limit starts from the L2 ceiling so
        // the band shrinks it toward the setpoint rather than seeing capacity 0.
        let env = MockHostEnv::default(); // unit_property empty → None → unbounded
        let cluster = HostCluster::shadow(env, envelopes());
        let layout = LimitLayout::Host(HostKnob::CgroupProperty {
            unit: "nix-daemon.service".into(),
            property: "MemoryHigh".into(),
        });
        let cap = cluster.read_limit(&unit_target("nix-daemon.service"), &layout, "memory").await.unwrap();
        assert_eq!(cap, 12 * GI, "unbounded MemoryHigh ⇒ the unit's L2 envelope");
    }

    #[tokio::test]
    async fn cgroup_apply_to_a_unit_without_an_envelope_is_refused() {
        let env = MockHostEnv::default();
        let cluster = HostCluster::new(env, envelopes(), true);
        let patch = SsaPatch {
            target: unit_target("unknown.service"),
            field_manager: "breathe/cgroup".into(),
            layout: LimitLayout::Host(HostKnob::CgroupProperty {
                unit: "unknown.service".into(),
                property: "MemoryHigh".into(),
            }),
            resource: "memory".into(),
            value: GI,
        };
        // no L2 envelope for `unknown.service` ⇒ no ceiling ⇒ refuse (never write blind).
        assert!(cluster.apply(&patch).await.is_err());
        assert!(cluster.env().writes().is_empty());
    }

    #[test]
    fn systemctl_invocation_wraps_in_nsenter_when_a_pid_is_set() {
        // bare-metal / tests: no nsenter — run systemctl directly.
        let (prog, argv) = SystemdSysfsEnv::systemctl_invocation(None, "systemctl", &["show", "x", "--value"]);
        assert_eq!(prog, "systemctl");
        assert_eq!(argv, vec!["show", "x", "--value"]);

        // in-pod: enter the host's namespaces and run the host's systemctl.
        let (prog, argv) = SystemdSysfsEnv::systemctl_invocation(
            Some(1),
            "/run/current-system/sw/bin/systemctl",
            &["set-property", "--runtime", "nix-daemon.service", "MemoryHigh=10G"],
        );
        assert_eq!(prog, "nsenter");
        assert_eq!(
            argv,
            vec![
                "-t", "1", "-m", "-u", "-i", "-n", "-p", "--",
                "/run/current-system/sw/bin/systemctl",
                "set-property", "--runtime", "nix-daemon.service", "MemoryHigh=10G",
            ]
        );
    }

    #[tokio::test]
    async fn k8s_source_on_host_cluster_is_a_typed_error() {
        let cluster = HostCluster::shadow(MockHostEnv::default(), envelopes());
        let err = cluster
            .read_used(&MetricSource::PodMetricsMax { resource: "memory".into(), pod_prefix: "x".into(), selector: None })
            .await
            .unwrap_err();
        assert!(matches!(err, ProviderError::ApiPermanent(_)));
    }

    // ── Phase 3: host cgroup-cpu band (a new RestartFree host dimension) ──────

    #[test]
    fn cpu_millicores_converts_nsec_delta_over_a_window() {
        // 1 core for 1s: 1e9 cpu-ns over 1e9 wall-ns → 1000 millicores.
        assert_eq!(cpu_millicores(1_000_000_000, 1_000_000_000), 1000);
        // 2 cores → 2000; half a core → 500.
        assert_eq!(cpu_millicores(2_000_000_000, 1_000_000_000), 2000);
        assert_eq!(cpu_millicores(500_000_000, 1_000_000_000), 500);
        // zero window is guarded (no divide-by-zero).
        assert_eq!(cpu_millicores(1_000_000_000, 0), 0);
    }

    #[tokio::test]
    async fn cgroup_cpu_rate_warms_up_then_differences_across_ticks() {
        // CPUUsageNSec advances 1e9→2e9 across the two ticks; the same cluster's
        // cross-tick cache holds the first sample.
        let env = MockHostEnv::default();
        *env.cpu_usage_nsec.lock().unwrap() = [1_000_000_000u64, 2_000_000_000].into();
        let cluster = HostCluster::shadow(env, envelopes());
        let src = MetricSource::Host(HostMetric::CgroupCpuUsage { unit: "nix-daemon.service".into() });
        // FIRST tick: no prior sample ⇒ warming (a transient), no rate yet.
        assert!(matches!(cluster.read_used(&src).await, Err(ProviderError::MetricsMissing)));
        // SECOND tick: differences against the first ⇒ a real rate (exact value is
        // wall-time dependent; the math is proven by cpu_millicores_converts…).
        let s = cluster.read_used(&src).await.unwrap();
        assert_eq!(s.age_secs, 0, "host reads are fresh");
    }

    #[test]
    fn parse_cpu_quota_timespan_to_usec() {
        // the real-world forms systemd's `show --value` emits for CPUQuotaPerSecUSec.
        assert_eq!(parse_cpu_quota_usec("12s"), Ok(Some(12_000_000))); // 12 cores
        assert_eq!(parse_cpu_quota_usec("500ms"), Ok(Some(500_000))); // half a core
        assert_eq!(parse_cpu_quota_usec("1s 500ms"), Ok(Some(1_500_000))); // 1.5 cores
        assert_eq!(parse_cpu_quota_usec("1min 30s"), Ok(Some(90_000_000)));
        assert_eq!(parse_cpu_quota_usec("250us"), Ok(Some(250)));
        assert_eq!(parse_cpu_quota_usec("infinity"), Ok(None)); // unbounded
        // garbage / empty / unknown unit ⇒ a typed error, never a silent cap.
        assert_eq!(parse_cpu_quota_usec("nonsense"), Err(()));
        assert_eq!(parse_cpu_quota_usec(""), Err(()));
        assert_eq!(parse_cpu_quota_usec("12x"), Err(()));
    }

    #[tokio::test]
    async fn cgroup_cpu_quota_reads_the_timespan_as_millicores() {
        let layout = LimitLayout::Host(HostKnob::CgroupCpuQuota { unit: "nix-daemon.service".into() });
        // systemd prints "1s 500ms" = 1_500_000 usec/sec = 1.5 cores → 1500 millicores.
        let mut up = BTreeMap::new();
        up.insert(("nix-daemon.service".to_string(), "CPUQuotaPerSecUSec".to_string()), "1s 500ms".to_string());
        let cluster = HostCluster::shadow(MockHostEnv { unit_property_str: up, ..Default::default() }, envelopes());
        assert_eq!(cluster.read_limit(&unit_target("nix-daemon.service"), &layout, "cpu").await.unwrap(), 1500);
        // "infinity" (no quota) → start from the L2 cpu ceiling (8000m).
        let mut inf = BTreeMap::new();
        inf.insert(("nix-daemon.service".to_string(), "CPUQuotaPerSecUSec".to_string()), "infinity".to_string());
        let unbounded = HostCluster::shadow(MockHostEnv { unit_property_str: inf, ..Default::default() }, envelopes());
        assert_eq!(unbounded.read_limit(&unit_target("nix-daemon.service"), &layout, "cpu").await.unwrap(), 8000);
        // unset (no property at all) likewise falls back to the ceiling.
        let unset = HostCluster::shadow(MockHostEnv::default(), envelopes());
        assert_eq!(unset.read_limit(&unit_target("nix-daemon.service"), &layout, "cpu").await.unwrap(), 8000);
    }

    #[tokio::test]
    async fn cgroup_cpu_apply_renders_percent_shadow_safe_and_ceiling_bound() {
        let layout = LimitLayout::Host(HostKnob::CgroupCpuQuota { unit: "nix-daemon.service".into() });
        let patch = |value| SsaPatch {
            target: unit_target("nix-daemon.service"),
            field_manager: "breathe/cgroup-cpu".into(),
            layout: layout.clone(),
            resource: "cpu".into(),
            value,
        };
        // LIVE: 1500 millicores → CPUQuota=150% (a string set-property, not u64).
        let live = HostCluster::new(MockHostEnv::default(), envelopes(), true);
        live.apply(&patch(1500)).await.unwrap();
        assert_eq!(live.env().str_writes(), vec![("nix-daemon.service:CPUQuota".to_string(), "150%".to_string())]);
        assert!(live.env().writes().is_empty(), "cpu quota goes through the STRING set-property");
        // SAFETY WALL 2: over the 8000m L2 ceiling ⇒ refused, nothing written.
        let over = HostCluster::new(MockHostEnv::default(), envelopes(), true);
        assert!(matches!(over.apply(&patch(9000)).await.unwrap_err(), ProviderError::ApiPermanent(_)));
        assert!(over.env().str_writes().is_empty(), "a refused cpu write touches nothing");
        // SHADOW: decides but writes nothing.
        let shadow = HostCluster::shadow(MockHostEnv::default(), envelopes());
        shadow.apply(&patch(1500)).await.unwrap();
        assert!(shadow.env().str_writes().is_empty(), "shadow never writes the host");
    }
}
