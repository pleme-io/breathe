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

use std::collections::BTreeMap;

use async_trait::async_trait;
use breathe_provider::{
    AppliedReceipt, ApplySemantics, Cluster, DimensionDescriptor, DimensionId, Directionality,
    HostKnob, HostMetric, LimitLayout, MetricSource, ProviderError, Sample, SsaPatch, Target,
};

/// `/sys/module/zfs/parameters/zfs_arc_max` — the live ARC ceiling (bytes).
pub const ZFS_ARC_MAX_PATH: &str = "/sys/module/zfs/parameters/zfs_arc_max";
/// `/proc/spl/kstat/zfs/arcstats` — ARC kstats (the `size` row is current bytes).
pub const ZFS_ARCSTATS_PATH: &str = "/proc/spl/kstat/zfs/arcstats";

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
}

/// The real implementation over std `fs` + `systemctl` (argv, never a shell).
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemdSysfsEnv;

impl SystemdSysfsEnv {
    /// Run `systemctl <args…>` and return trimmed stdout. NO shell, NO `format!`
    /// of a command line — argv is built with `Command::arg` (the typed surface).
    fn systemctl(args: &[&str]) -> Result<String, HostError> {
        let out = std::process::Command::new("systemctl")
            .args(args)
            .output()
            .map_err(|e| HostError::Io(e.to_string()))?;
        if !out.status.success() {
            return Err(HostError::Command {
                argv: format!("systemctl {}", args.join(" ")),
                code: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
            });
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }
}

impl HostEnvironment for SystemdSysfsEnv {
    fn read_arcstats_size(&self) -> Result<u64, HostError> {
        let text = std::fs::read_to_string(ZFS_ARCSTATS_PATH).map_err(|e| HostError::Io(e.to_string()))?;
        // arcstats rows are `name  type  data`; we want the `size` row's data.
        for line in text.lines() {
            let mut it = line.split_whitespace();
            if it.next() == Some("size") {
                let raw = it.last().ok_or_else(|| HostError::Parse("arcstats size has no data column".into()))?;
                return raw.parse::<u64>().map_err(|e| HostError::Parse(e.to_string()));
            }
        }
        Err(HostError::Parse("arcstats has no `size` row".into()))
    }

    fn read_sysfs_u64(&self, path: &str) -> Result<u64, HostError> {
        let raw = std::fs::read_to_string(path).map_err(|e| HostError::Io(e.to_string()))?;
        raw.trim().parse::<u64>().map_err(|e| HostError::Parse(e.to_string()))
    }

    fn write_sysfs_u64(&self, path: &str, value: u64) -> Result<(), HostError> {
        std::fs::write(path, value.to_string()).map_err(|e| HostError::Io(e.to_string()))
    }

    fn read_cgroup_memory_current(&self, unit: &str) -> Result<u64, HostError> {
        // `systemctl show <unit> -p MemoryCurrent --value` → bytes (or "[not set]").
        let v = Self::systemctl(&["show", unit, "-p", "MemoryCurrent", "--value"])?;
        v.trim().parse::<u64>().map_err(|e| HostError::Parse(format!("MemoryCurrent={v:?}: {e}")))
    }

    fn read_unit_property_u64(&self, unit: &str, property: &str) -> Result<Option<u64>, HostError> {
        let v = Self::systemctl(&["show", unit, "-p", property, "--value"])?;
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
        Self::systemctl(&["set-property", "--runtime", unit, &assignment]).map(|_| ())
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
}

impl NodeEnvelopes {
    /// The L2 ceiling for a host lever (the value a write may never exceed).
    pub fn ceiling_for(&self, knob: &HostKnob) -> Result<u64, HostError> {
        match knob {
            HostKnob::ZfsArcMax => Ok(self.arc_max_bytes),
            HostKnob::CgroupProperty { unit, .. } => self
                .cgroup_max_bytes
                .get(unit)
                .copied()
                .ok_or_else(|| HostError::NoEnvelope(unit.clone())),
        }
    }
}

// ─────────────────────────── HostCluster ───────────────────────────

/// The host `Cluster`. `write_enabled = false` is the SHADOW mode (M3/M4): it
/// reads + decides + reports `appliedValue` but performs no host mutation, so the
/// full loop can be observed on a live node before a single byte is written.
pub struct HostCluster<H: HostEnvironment> {
    env: H,
    envelopes: NodeEnvelopes,
    write_enabled: bool,
}

impl<H: HostEnvironment> HostCluster<H> {
    pub fn new(env: H, envelopes: NodeEnvelopes, write_enabled: bool) -> Self {
        Self { env, envelopes, write_enabled }
    }
    /// SHADOW constructor — reads + decides, never writes.
    pub fn shadow(env: H, envelopes: NodeEnvelopes) -> Self {
        Self::new(env, envelopes, false)
    }
    pub fn env(&self) -> &H {
        &self.env
    }
    pub fn writes_enabled(&self) -> bool {
        self.write_enabled
    }
}

#[async_trait]
impl<H: HostEnvironment> Cluster for HostCluster<H> {
    async fn read_used(&self, source: &MetricSource) -> Result<Sample, ProviderError> {
        let value = match source {
            MetricSource::Host(HostMetric::ArcSize) => self.env.read_arcstats_size()?,
            MetricSource::Host(HostMetric::CgroupMemoryCurrent { unit }) => {
                self.env.read_cgroup_memory_current(unit)?
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

#[cfg(test)]
mod tests {
    use super::*;
    use breathe_provider::{BandProvider, ResourceProvider};
    use std::sync::Mutex;

    const GI: u64 = 1024 * 1024 * 1024;

    /// A programmable in-memory [`HostEnvironment`] — the testability seam. Records
    /// every write so a test can assert shadow mode wrote nothing / live mode wrote
    /// exactly the clamped value.
    #[derive(Default)]
    struct MockHostEnv {
        arc_size: u64,
        sysfs: Mutex<BTreeMap<String, u64>>,
        cgroup_current: BTreeMap<String, u64>,
        unit_property: BTreeMap<(String, String), Option<u64>>,
        writes: Mutex<Vec<(String, u64)>>,
    }

    impl MockHostEnv {
        fn writes(&self) -> Vec<(String, u64)> {
            self.writes.lock().unwrap().clone()
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
    }

    fn envelopes() -> NodeEnvelopes {
        let mut cgroup = BTreeMap::new();
        cgroup.insert("nix-daemon.service".to_string(), 12 * GI); // nodeBudget memoryMaxGiB = 12
        NodeEnvelopes { arc_max_bytes: 6 * GI, cgroup_max_bytes: cgroup }
    }

    fn node_target() -> Target {
        Target { namespace: String::new(), name: "rio".into(), kind: "Node".into(), api_version: String::new(), container: None }
    }
    fn unit_target(unit: &str) -> Target {
        Target { namespace: String::new(), name: unit.into(), kind: "HostUnit".into(), api_version: String::new(), container: None }
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

    #[tokio::test]
    async fn k8s_source_on_host_cluster_is_a_typed_error() {
        let cluster = HostCluster::shadow(MockHostEnv::default(), envelopes());
        let err = cluster
            .read_used(&MetricSource::PodMetricsMax { resource: "memory".into(), pod_prefix: "x".into() })
            .await
            .unwrap_err();
        assert!(matches!(err, ProviderError::ApiPermanent(_)));
    }
}
