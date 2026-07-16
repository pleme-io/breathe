//! `breathe-provider` — the provider/plugin spine: the `Cluster` Environment
//! trait, the `DimensionDescriptor` trait, and the **one generic
//! [`BandProvider`]** that implements [`ResourceProvider`] for every dimension.
//!
//! The compounding shape (theory/BREATHE.md §3): the observe/assign/release
//! *orchestration* is solved exactly once, in `BandProvider`; a new dimension
//! supplies only its genuinely-specific data via a `DimensionDescriptor`
//! (metric query, owned field, directionality, owner layout). A provider never
//! sees `decide`/`BandConfig` — `BandProvider` calls the proven band law's
//! inputs but the deciding lives entirely in `breathe-core`/`breathe-control`.

use async_trait::async_trait;

pub use breathe_control::{Directionality, FieldOwner, Observation, StorageCapability};

/// Typed category atom — keys the registry, equals the catalog `:name`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DimensionId {
    Memory,
    Storage,
    Cpu,
    Replica,
    /// HOST: ZFS ARC max (`/sys/module/zfs/parameters/zfs_arc_max`).
    Arc,
    /// HOST: a systemd unit's transient cgroup memory high-water (`MemoryHigh`).
    Cgroup,
    /// HOST: a systemd unit's transient cgroup cpu bandwidth cap (`CPUQuota`) —
    /// the host-plane peer of `pod-cpu-resize`, carved live with zero restart.
    CgroupCpu,
    /// HOST: a GENERIC sysctl / ZFS-parameter band (PR-2). One id for the whole
    /// family — the specific knob (`vm.dirty_bytes`, `zfs_arc_min`, …) + metric +
    /// directionality are carried as DATA on the descriptor, so a new sysctl/ZFS
    /// band is a catalog row + a CR, not a new dimension id. RestartFree.
    HostParam,
    /// K8S-PLANE: a GENERIC k8s-CR / app-protocol band (Step-6/8/12). One id for
    /// the whole family — the layout (`CrField`/`DestinationRuleField`/
    /// `NamespaceEnvelope`/`ControllerSetpoint`/`ConfigFile`/`ApiCall`) + metric +
    /// directionality are DATA on the descriptor, reconciled via `KubeCluster`'s
    /// generic CR-path SSA (or a routed actuator). A new such band is a CR.
    KubeParam,
    /// APP-PLANE: a GENERIC application-actuator band (Step-9/13). One id for the
    /// whole family — the layout (`ConfigFile`/`ApiCall`) + which actuator services
    /// it (ConfigReload / redis-CLI / JMX-Jolokia / app-admin-RPC) are DATA on the
    /// CR (`AppLayoutSpec`), dispatched by the `ActuatorCluster` sum type. `used` is
    /// read from the k8s metrics plane (the actuators have no read path), the limit
    /// is carved on the app's own knob. A new such band is a CR.
    AppParam,
}

impl DimensionId {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Storage => "storage",
            Self::Cpu => "cpu",
            Self::Replica => "replica",
            Self::Arc => "arc",
            Self::Cgroup => "cgroup",
            Self::CgroupCpu => "cgroup-cpu",
            Self::HostParam => "host-param",
            Self::KubeParam => "kube-param",
            Self::AppParam => "app-param",
        }
    }

    /// True for dimensions whose I/O boundary is the HOST (systemd/sysfs via
    /// `HostCluster`) rather than the Kubernetes API (`KubeCluster`).
    #[must_use]
    pub fn is_host(self) -> bool {
        matches!(self, Self::Arc | Self::Cgroup | Self::CgroupCpu | Self::HostParam)
    }
}

impl std::fmt::Display for DimensionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// HOW a dimension's SUPPRESSED DEMAND becomes observable — the typed declaration
/// every dimension MUST make so no future dimension can be carve-blind the way CPU
/// was (the pangea-operator 2026-06 starve). A dimension whose `used` is HARD-CAPPED
/// (the cgroup throttles at the limit) hides its true demand from the usage metric;
/// this enum names the non-blind signal that reveals it. Adding a dimension forces a
/// conscious choice of variant — a `DimensionDescriptor` declares it via
/// [`DimensionDescriptor::suppressed_demand`], and the catalog declares it as a
/// required `DimensionSpec` field, so the build fails if either is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuppressedDemand {
    /// MEMORY: the working set can spike ABOVE the soft `memory.high` (revealing
    /// demand) while the hard `memory.max` holds — so the demonstrated-peak floor
    /// already sees the real demand. No separate throttle read is needed: suppressed
    /// demand is visible in the primary `used`/peak path. The default.
    WorkingSetExceedsSoftLimit,
    /// CPU: usage is HARD-CAPPED by the cgroup CFS quota, so the usage metric can
    /// NEVER exceed the limit — the suppressed demand shows up ONLY as CFS throttling
    /// (`container_cpu_cfs_throttled_periods_total` / `cpu.stat`). The descriptor
    /// supplies a [`DimensionDescriptor::throttle_source`]; its non-zero scalar drives
    /// `Observation.throttle_signal`, lifting demand above the cap so the proven floor
    /// refuses a shrink and the band grows out of the throttle. THE CPU-blindness fix.
    CfsThrottling,
    /// STORAGE: grow-only — there is no shrink to ratchet, so suppressed demand is a
    /// non-issue by construction (the down-cliff is unrepresentable). Declared for
    /// completeness; carries no throttle read.
    GrowOnly,
    /// OBSERVE-ONLY: a dimension that is never mutated, so there is no carve to
    /// suppress. No throttle read. Declared for completeness so the catalog
    /// partition is total. (No shipped dimension uses it today — replica became a
    /// first-class horizontal carve; kept for a future observe-only dimension.)
    NotApplicable,
}

impl SuppressedDemand {
    /// Stable label for catalog rendering / logging.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WorkingSetExceedsSoftLimit => "working-set-exceeds-soft-limit",
            Self::CfsThrottling => "cfs-throttling",
            Self::GrowOnly => "grow-only",
            Self::NotApplicable => "not-applicable",
        }
    }
    /// `true` iff this signal is observed by a SEPARATE throttle read (vs already
    /// being in the primary `used`/peak path or having no suppressed-demand hole).
    /// The catalog reflection test asserts a dimension declaring `CfsThrottling`
    /// supplies a `throttle_source`, and one that doesn't, doesn't.
    #[must_use]
    pub fn needs_throttle_source(self) -> bool {
        matches!(self, Self::CfsThrottling)
    }
}

impl std::fmt::Display for SuppressedDemand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A reconcile target — the owner object whose limit a band controls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub namespace: String,
    pub name: String,
    /// `Deployment` | `StatefulSet` | `Cluster` (CNPG) | `PersistentVolumeClaim` |
    /// `EphemeralRunner` (any owner-less pod group resolved by `pod_selector`).
    pub kind: String,
    pub api_version: String,
    pub container: Option<String>,
    /// When set, breathe resolves the band's pods DIRECTLY by this k8s label
    /// selector (`k=v,k2=v2`) instead of via an owner's `spec.selector.matchLabels`
    /// — the **label-selected pod-group carve**. The path for ephemeral / owner-less
    /// pod sets whose name is not stable and which have no single resolvable
    /// workload owner (GitHub ARC `EphemeralRunner`s, bare pods, Job pods). A
    /// selector ALWAYS carves in-place (`PodResize`) — there is no template to roll —
    /// scoped to `namespace`; the metric reads the same selector. `None` ⇒ the
    /// owner-selector path (Deployment/StatefulSet/CNPG), unchanged.
    pub pod_selector: Option<String>,
}

/// A writable HOST lever — the address `HostCluster` writes a breathe decision
/// to. Disjoint by construction from what `nodeBudget` (the static L2 partition)
/// owns: breathe writes the *runtime* `zfs_arc_max` parameter and *transient*
/// (`--runtime`) cgroup properties; nodeBudget owns the boot modprobe ceiling,
/// the static unit `MemoryMax`, and the cpuset pin. They never write the same
/// field, so the two layers compose without contention (the L1-within-L2 contract).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostKnob {
    /// `/sys/module/zfs/parameters/zfs_arc_max` — the live ARC ceiling, in bytes.
    ZfsArcMax,
    /// A systemd unit's transient cgroup property, e.g.
    /// (`nix-daemon.service`, `MemoryHigh`) applied via `systemctl set-property
    /// --runtime`. Never the unit file (that is nodeBudget's static `MemoryMax`).
    CgroupProperty { unit: String, property: String },
    /// A systemd unit's transient cgroup CPU bandwidth cap (`CPUQuota`) — set via
    /// `systemctl set-property --runtime <unit> CPUQuota=<percent>%`. A DISTINCT
    /// knob from [`CgroupProperty`] because its value semantics differ: the band
    /// value is MILLICORES (read from `CPUQuotaPerSecUSec`, written as a percentage),
    /// not bytes. Bounded by `nodeBudget`'s cpu territory, never the static cpuset.
    CgroupCpuQuota { unit: String },
    /// **PR-2 keystone:** a generic single-`u64` sysctl, addressed by its dotted
    /// `key` (`vm.dirty_bytes`, `net.core.rmem_max`, `fs.file-max`). The actuator
    /// maps `key` → `/proc/sys/<key with dots→slashes>` and reads/writes one
    /// integer. This ONE arm collapses the entire `vm.*`/`net.*`/`fs.*` sysctl
    /// family into catalog DATA — a new sysctl band is one descriptor + one
    /// catalog row, zero new code.
    Sysctl { key: String },
    /// **PR-2 keystone:** a generic ZFS module parameter, addressed by its bare
    /// `param` name (`zfs_arc_min`, `zfs_arc_dnode_limit`). The actuator maps
    /// `param` → `/sys/module/zfs/parameters/<param>`. Generalizes
    /// [`ZfsArcMax`](Self::ZfsArcMax) so every ZFS sysfs param is a catalog row.
    ZfsParam { param: String },
    /// **PR-4:** a systemd unit's PER-DEVICE `io.max` cap — one of the four
    /// disjoint fields (`rbps`/`wbps`/`riops`/`wiops`) breathe carves
    /// independently. Set via `IO{Read,Write}{Bandwidth,IOPS}Max="<device>
    /// <value>"`. RestartFree (a live cgroup io cap never restarts the unit).
    CgroupIoMax { unit: String, device: String, field: IoMaxField },
    /// **Part 1 (SOFT k8s carve):** a k8s POD's cgroup-v2 `memory.high` (SOFT,
    /// reclaim — exceeding it throttles, NEVER kills) — the efficiency-carve target
    /// for a memory band, written by the privileged host-agent DaemonSet directly to
    /// the pod's cgroup file. The k8s `limits.memory` (`memory.max`, HARD/kill) is
    /// left at the never-OOM peak ceiling; this carves ONLY the soft reclaim limit, so
    /// an efficiency shrink can never OOM-kill the workload. `qos`/`pod_uid`/
    /// `container_runtime_id` address the pod's cgroup path (the host-agent resolves
    /// the container id from the live pod status). `driver` selects the kubelet's
    /// cgroup-driver path layout (systemd `.slice`/`.scope` vs cgroupfs flat) — the
    /// SAME `(qos, uid, ctr)` resolves to a DIFFERENT path under each driver, so it
    /// is a typed field, never an assumption. This is the pod-scope mirror of the
    /// host/cgroup `MemoryHigh` lever (already shipped). RestartFree (a live
    /// memory.high write never restarts the container — reclaim, not kill).
    PodCgroupMemoryHigh { driver: CgroupDriver, qos: String, pod_uid: String, container_runtime_id: String },
}

/// The kubelet's cgroup driver — the closed set that selects a pod's cgroup-v2
/// path LAYOUT. The two drivers nest the kubepods tree DIFFERENTLY, so the same
/// `(qos, pod_uid, container_runtime_id)` maps to a different `memory.high` file
/// under each. A closed enum (never a free string) makes "which layout" a total
/// function: a path is produced for exactly the two drivers k8s supports, and a
/// future driver is a compile-error-forcing new arm, not a silent wrong path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum CgroupDriver {
    /// systemd driver (kubelet `cgroupDriver: systemd`, the NixOS/containerd
    /// default + rio's live driver): `kubepods.slice/[<qos>.slice/]kubepods-<qos>-
    /// pod<uid_underscored>.slice/cri-containerd-<ctr>.scope/memory.high`.
    #[default]
    Systemd,
    /// cgroupfs driver (`cgroupDriver: cgroupfs`): `kubepods/[<qos>/]pod<uid>/
    /// <ctr>/memory.high` — flat, no `.slice`/`.scope`, the pod UID's dashes KEPT.
    Cgroupfs,
}

/// **PR-4:** which of the four disjoint `io.max` sub-knobs a [`HostKnob::CgroupIoMax`]
/// carves (and which io-accounting counter its rate metric differences). `bps`
/// fields are [`Unit::BytesPerSec`]; `iops` fields are [`Unit::Iops`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMaxField {
    /// Read bandwidth (bytes/s).
    Rbps,
    /// Write bandwidth (bytes/s).
    Wbps,
    /// Read IOPS.
    Riops,
    /// Write IOPS.
    Wiops,
}

impl IoMaxField {
    /// The systemd unit property that SETS this per-device cap.
    #[must_use]
    pub fn cap_property(self) -> &'static str {
        match self {
            Self::Rbps => "IOReadBandwidthMax",
            Self::Wbps => "IOWriteBandwidthMax",
            Self::Riops => "IOReadIOPSMax",
            Self::Wiops => "IOWriteIOPSMax",
        }
    }
    /// The systemd io-accounting CUMULATIVE counter this field's RATE differences.
    #[must_use]
    pub fn counter_property(self) -> &'static str {
        match self {
            Self::Rbps => "IOReadBytes",
            Self::Wbps => "IOWriteBytes",
            Self::Riops => "IOReadOperations",
            Self::Wiops => "IOWriteOperations",
        }
    }
    /// Short label (`rbps`/`wbps`/`riops`/`wiops`) — the rate-cache key + logging.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rbps => "rbps",
            Self::Wbps => "wbps",
            Self::Riops => "riops",
            Self::Wiops => "wiops",
        }
    }
}

/// Where a HOST dimension reads its `used` scalar from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostMetric {
    /// ZFS ARC current size from `/proc/spl/kstat/zfs/arcstats` (`size` row), bytes.
    ArcSize,
    /// cgroup v2 `memory.current` for a systemd unit's slice, bytes.
    CgroupMemoryCurrent { unit: String },
    /// A systemd unit's cpu usage RATE in millicores — derived from two reads of
    /// the cumulative `CPUUsageNSec` over a sample window (`HostCluster` computes
    /// the rate; the env exposes the cumulative counter).
    CgroupCpuUsage { unit: String },
    /// **PR-2:** a named row of `/proc/spl/kstat/zfs/arcstats` (`size`,
    /// `dnode_size`, `arc_meta_used`, …) in bytes — generalizes
    /// [`ArcSize`](Self::ArcSize). The `used` signal for any ZFS-param band.
    ArcKstat { row: String },
    /// **PR-2:** a field of `/proc/meminfo` (`Dirty`, `Writeback`, `MemFree`, …),
    /// reported in bytes (meminfo prints kB; the env converts). The `used` signal
    /// for `vm.dirty_bytes`, `min_free_kbytes`, `rmem/wmem` bands.
    MeminfoField { field: String },
    /// **PR-4:** a systemd unit's io RATE — the cumulative io-accounting counter
    /// (`IOReadBytes`/`IOWriteOperations`/…) the `HostCluster` differences over the
    /// sample window into a `BytesPerSec` / `Iops` rate. The `used` signal for an
    /// `io.max` band. (Unit-aggregate today; per-device io.stat is a refinement.)
    CgroupIoStat { unit: String, field: IoMaxField },
    /// **PR-3:** PRESSURE-STALL INFORMATION — the `avg10` stall percentage (×100,
    /// so `12.34%` → `1234`) from `/proc/pressure/<resource>`, the throttle signal
    /// that turns every soft / rate-shaped band [`ThrottleAware`](breathe_control)
    /// (cpu, io, memory reclaim). The single highest-leverage observe-side upgrade
    /// of the census: a non-zero stall means the resource is being throttled NOW.
    Psi { resource: PsiResource, kind: PsiKind },
}

/// **PR-3:** which `/proc/pressure/<resource>` file a [`HostMetric::Psi`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PsiResource {
    Cpu,
    Memory,
    Io,
}

impl PsiResource {
    /// The procfs basename (`cpu`/`memory`/`io`) for this resource's pressure file.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Memory => "memory",
            Self::Io => "io",
        }
    }
}

/// **PR-3:** which PSI line a [`HostMetric::Psi`] reads. `some` = at least one task
/// stalled (latency); `full` = ALL non-idle tasks stalled (throughput collapse).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PsiKind {
    Some,
    Full,
}

impl PsiKind {
    /// The PSI line prefix (`some`/`full`) this kind reads.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Some => "some",
            Self::Full => "full",
        }
    }
}

/// Where a managed quantity lives on a target object — interpreted by the
/// `Cluster` impl when reading/patching. The *dimension* + the *owner kind*
/// together pick the layout (memory on a Deployment is `PodTemplate`; memory on
/// a CNPG `Cluster` is `ClusterTopLevel`; storage is always `PvcRequest`). The
/// `Host` arm carries the host lever for the `HostCluster` impl — `KubeCluster`
/// rejects it with a typed error (it can never legitimately receive one).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LimitLayout {
    /// CNPG `Cluster`: `spec.resources.limits.<res>`.
    ClusterTopLevel,
    /// Deployment/StatefulSet: `spec.template.spec.containers[name].resources.limits.<res>`.
    /// Writing it ROLLS the workload (the controller re-creates pods).
    PodTemplate { container: Option<String> },
    /// IN-PLACE resize of the live pods via the `pods/{name}/resize` subresource
    /// (k8s ≥ 1.33) — the homeostasis keystone: carve the running container's
    /// cgroup with NO restart, exactly as `HostCluster` carves a host unit's
    /// cgroup. Reads + writes the LIVE pods (found by the owner's selector), not
    /// the template, so it never rolls; a re-created pod starts at the template
    /// default and the band re-converges it in-place on the next tick. QoS is
    /// preserved (a Guaranteed pod stays Guaranteed). Distinct from `PodTemplate`
    /// precisely because `d(restart)/d(carve) = 0`.
    PodResize { container: Option<String> },
    /// PVC: `spec.resources.requests.storage` (grow-only).
    PvcRequest,
    /// CNPG `Cluster`: `spec.storage.size` (grow-only) — the storage analogue of
    /// `ClusterTopLevel`. The DB operator owns the raw PVC's `requests.storage`,
    /// so breathe carves the operator's storage field and lets it expand the
    /// instance PVCs (managed-database disk homeostasis).
    ClusterStorage,
    /// HOST: a systemd/sysfs lever — written by `HostCluster`, not the k8s API.
    Host(HostKnob),
    /// **Step-9/12:** a config-file value edited + reloaded via a signal —
    /// pgbouncer `RELOAD`, nginx/PostgreSQL `SIGHUP`, or a restart. The
    /// `ConfigReloadCluster` actuator owns the edit; `reload` sets the restart cost.
    ConfigFile { path: String, key: String, reload: ConfigReload },
    /// **Step-9:** a protocol API call — Redis `CONFIG SET`, Kafka `AdminClient`,
    /// NATS JetStream edit. RestartFree (the value applies live). The
    /// `ApiCallCluster` actuator owns the connection.
    ApiCall { endpoint: String, command: String },
    /// **Step-12:** a field of an operator-owned k8s CR (CNPG/VictoriaMetrics/
    /// VictoriaLogs/OpenSearch), written via the existing `KubeCluster` SSA.
    /// `restart_free` distinguishes a live-reconciled field from one that rolls.
    CrField { api_version: String, kind: String, name: String, field_path: String, restart_free: bool },
    /// **Step-6:** an Istio `DestinationRule` connection-pool field, written via
    /// `KubeCluster` SSA — RestartFree (Envoy live-reloads the cluster config).
    DestinationRuleField { name: String, field_path: String },
    /// **Step-8:** a namespace `ResourceQuota`/`LimitRange` envelope field — the
    /// Densa namespace wall. RestartFree (admission-gating, no workload restart).
    NamespaceEnvelope { namespace: String, kind: NamespaceEnvelopeKind, field_path: String },
    /// **Step-8:** a controller setpoint — HPA `target.averageUtilization`, PDB
    /// `minAvailable`. breathe becomes a meta-controller over the autoscaler.
    /// RestartFree (a setpoint edit; the controller acts).
    ControllerSetpoint { api_version: String, kind: String, name: String, field_path: String },
    /// **Step-5:** pod network bandwidth — the `kubernetes.io/{egress,ingress}-
    /// bandwidth` annotation (rolls) OR a host-tc HTB class (`host_tc=true`,
    /// RestartFree — the golden path). `direction` selects egress/ingress.
    PodNetworkBandwidth { direction: NetDirection, host_tc: bool },
    /// **HORIZONTAL:** the workload's `.spec.replicas` count — the typed replica
    /// actuator for a `ReplicaBand`. `kind` is the owner (`Deployment` /
    /// `StatefulSet`); the value written is a bare replica count. Asymmetric restart
    /// cost: a scale-OUT leaves every surviving pod undisturbed (`RestartFree`,
    /// like `node-add`), while a scale-IN sheds a pod (`RestartRequiring`, the
    /// `replica-scale-down` action) — so a `RestartFreeOnly` band scales out freely
    /// and gates scale-in until its `DisruptionPolicy` permits the shed. The
    /// horizontal peer of the vertical `PodResize`/`PodTemplate` limit layouts.
    Replica { kind: String },
}

/// **Step-9/12:** how a [`LimitLayout::ConfigFile`] value takes effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigReload {
    /// `SIGHUP` re-reads the file live (PostgreSQL `work_mem`, nginx) — RestartFree.
    Sighup,
    /// A protocol `RELOAD` command (pgbouncer) — RestartFree.
    Reload,
    /// Requires a process restart (PostgreSQL `shared_buffers`) — RestartRequiring.
    Restart,
}

impl ConfigReload {
    /// The restart cost this reload mechanism implies.
    #[must_use]
    pub fn disruption_class(self) -> DisruptionClass {
        match self {
            Self::Sighup | Self::Reload => DisruptionClass::RestartFree,
            Self::Restart => DisruptionClass::RestartRequiring,
        }
    }
}

/// **Step-8:** which namespace envelope a [`LimitLayout::NamespaceEnvelope`] carves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceEnvelopeKind {
    ResourceQuota,
    LimitRange,
}

/// **Step-5:** network carve direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetDirection {
    Egress,
    Ingress,
}

/// How a category's `assign` lands (GALHO `ApplySemantics`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplySemantics {
    Transactional,
    ContinuousReconciliation,
    PartialProgress,
}

/// The RESTART COST of an action — the most load-bearing typed property in
/// breathe, because *without-restart* is the whole value: a restart-free action
/// can be driven through the standard tick at any cadence (near-real-time
/// management of the live workload), while a restart-requiring one must be gated.
/// Three honest classes, not two:
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisruptionClass {
    /// NEVER restarts — the live container/host resource is re-sized in place
    /// (host cgroup/sysfs lever, a pod cpu resize either way, a pod memory
    /// resize *up*, CSI online-expand, a survivor of a scale event). Tickable at
    /// any frequency: this is the set breathe drives toward real-time.
    RestartFree,
    /// Restart-free in the safe direction, restart-GATED in the other — a pod
    /// memory *shrink* is in-place only if the container's `resizePolicy` for
    /// memory is `NotRequired`; with `RestartContainer` it restarts. The actuator
    /// must read the policy and either carve in place or honor the gate.
    RestartConditional,
    /// ALWAYS re-creates the workload (pod-template write, CNPG `Cluster`
    /// top-level, image/env change, a drain+reschedule). Disruptive — but often
    /// the ONLY path (CNPG resize, k8s <1.33, NUMA re-placement) and sometimes
    /// worth it. Gated by [`DisruptionPolicy`].
    RestartRequiring,
}

impl DisruptionClass {
    /// True only for [`RestartFree`](Self::RestartFree) — drivable through ticks
    /// at any cadence with zero workload disturbance.
    #[must_use]
    pub fn is_restart_free(self) -> bool {
        matches!(self, Self::RestartFree)
    }
    /// True when the action can (possibly) restart the workload.
    #[must_use]
    pub fn may_restart(self) -> bool {
        !matches!(self, Self::RestartFree)
    }

    /// Refine a class by the target's observed `resizePolicy`. A
    /// [`RestartConditional`](Self::RestartConditional) class arises ONLY from a
    /// `PodResize` memory/byte SHRINK (every other carve is already `RestartFree`
    /// or `RestartRequiring`), so when the container declares
    /// `resizePolicy[<resource>] = NotRequired` (`shrink_restart_free`) the kubelet
    /// resizes it in place and the shrink becomes `RestartFree` — a `NotRequired`
    /// workload then breathes bidirectionally on golden rails. Every other class is
    /// returned unchanged; a `false` flag (the conservative default, incl. the k8s
    /// default `RestartContainer`) leaves the conditional class intact.
    #[must_use]
    pub fn refined_by_resize_policy(self, shrink_restart_free: bool) -> DisruptionClass {
        match self {
            Self::RestartConditional if shrink_restart_free => Self::RestartFree,
            other => other,
        }
    }
}

/// The FLAG that makes "without restart" controllable + explicit. Set per band /
/// per node; the actuator refuses any action whose [`DisruptionClass`] the policy
/// does not permit (returning a typed deferral, never a silent roll). The default
/// is the cautious one — never restart a workload unless explicitly allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum DisruptionPolicy {
    /// Only `RestartFree` actions — the workload is NEVER disturbed. A carve that
    /// would require (even conditionally) a restart is deferred + surfaced. The
    /// strictest, real-time-safe default.
    #[default]
    RestartFreeOnly,
    /// `RestartFree` + `RestartConditional` — allow an in-place memory shrink even
    /// where the resizePolicy may restart the container, but still never a full
    /// template roll.
    AllowConditional,
    /// Any action, including a full re-create — for workloads where the carve is
    /// only reachable by a roll (CNPG, k8s <1.33) and the disruption is acceptable.
    AllowRestart,
}

/// How a node pool's capacity is FILLED — the node-tier placement posture breathe
/// SETS and the scheduler binds against (the owns-vs-yields seam: breathe owns
/// the policy + emits the scoring hint; it never binds a pod). The node-tier peer
/// of [`Directionality`]/[`DisruptionPolicy`] — a typed choice, never a free knob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum FillPolicy {
    /// Bin-pack tight — fill a node to the band before provisioning the next.
    /// Low node count + cost, higher correlated-failure blast radius. Correct for
    /// stateless / batch / CI / dev where a node loss reschedules cheaply. The
    /// efficiency-first default (matches the 80/20 reclaim ethos).
    #[default]
    Pack,
    /// Distribute across failure domains — more nodes, lower per-node util, low
    /// blast radius. Correct for quorum members / stateful primaries / HA where a
    /// node or zone loss must not take a majority.
    Spread,
}

impl FillPolicy {
    /// The kube-scheduler `NodeResourcesFit` scoringStrategy this policy implies —
    /// the hint breathe SURFACES for the scheduler profile. breathe never binds a
    /// pod; it emits this and the scheduler (configured with the matching profile)
    /// does the binding. `Pack`→`MostAllocated` (fill tight), `Spread`→`LeastAllocated`.
    #[must_use]
    pub fn scheduler_scoring(self) -> &'static str {
        match self {
            Self::Pack => "MostAllocated",
            Self::Spread => "LeastAllocated",
        }
    }
    /// `true` for the default (`Pack`) — used as a serde `skip_serializing_if` so a
    /// pool at the default omits the field.
    #[must_use]
    pub fn is_pack(&self) -> bool {
        matches!(self, Self::Pack)
    }
}

impl std::fmt::Display for FillPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Pack => "pack",
            Self::Spread => "spread",
        })
    }
}

impl DisruptionPolicy {
    /// The default (golden) policy — used as a serde `skip_serializing_if` so a
    /// band at the default omits the field (keeps the strict typed-gRPC surface
    /// safe with an api-server that predates the field).
    #[must_use]
    pub fn is_restart_free_only(&self) -> bool {
        matches!(self, Self::RestartFreeOnly)
    }

    /// Whether this policy permits an action of the given restart cost.
    #[must_use]
    pub fn permits(self, class: DisruptionClass) -> bool {
        match self {
            Self::RestartFreeOnly => class == DisruptionClass::RestartFree,
            Self::AllowConditional => class != DisruptionClass::RestartRequiring,
            Self::AllowRestart => true,
        }
    }
}

/// Per-restart-class cooldown windows — golden berths cost nothing to occupy (a
/// `RestartFree` carve cools only ~one scrape interval, so the loop tracks the
/// band in near-real-time), while a ceiling crossing is expensive and stays
/// damped. This is what turns the catalog's `restart_free ⟺ tickable` promise
/// into actual loop cadence; a uniform cooldown discards it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassCooldowns {
    pub restart_free: u64,
    pub restart_conditional: u64,
    pub restart_requiring: u64,
}

impl Default for ClassCooldowns {
    fn default() -> Self {
        // restart_free ≈ a scrape interval (real-time); crossings stay long.
        Self { restart_free: 15, restart_conditional: 120, restart_requiring: 600 }
    }
}

impl ClassCooldowns {
    #[must_use]
    pub fn for_class(&self, class: DisruptionClass) -> u64 {
        match class {
            DisruptionClass::RestartFree => self.restart_free,
            DisruptionClass::RestartConditional => self.restart_conditional,
            DisruptionClass::RestartRequiring => self.restart_requiring,
        }
    }
    /// Structural invariant: free ≤ conditional ≤ requiring (golden is cheapest).
    #[must_use]
    pub fn well_ordered(&self) -> bool {
        self.restart_free <= self.restart_conditional && self.restart_conditional <= self.restart_requiring
    }
}

/// A carve's position relative to the GOLDEN region (the no-restart action
/// space). A `RestartFree` carve keeps every intermediate limit a comfortable,
/// always-restable berth — `GoldenPreserving`; anything restart-bearing is a
/// `CeilingCrossing` out of golden (the eclusa §XVIII line, drawn at the layout).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeTier {
    /// The live workload is undisturbed — golden.
    GoldenPreserving,
    /// Crossing out of golden carries this restart cost.
    CeilingCrossing(DisruptionClass),
}

impl EdgeTier {
    #[must_use]
    pub fn is_golden(self) -> bool {
        matches!(self, Self::GoldenPreserving)
    }
}

impl DisruptionClass {
    /// Project a restart cost onto the golden/ceiling line: only `RestartFree`
    /// preserves golden; any restart-bearing class is a crossing.
    #[must_use]
    pub fn edge_tier(self) -> EdgeTier {
        match self {
            Self::RestartFree => EdgeTier::GoldenPreserving,
            other => EdgeTier::CeilingCrossing(other),
        }
    }
}

impl LimitLayout {
    /// The layout's coarse worst-case restart cost — `PodResize` collapses to
    /// `RestartConditional` (a memory shrink may restart). For the PRECISE
    /// per-direction class of a specific carve use [`action_class`](Self::action_class).
    #[must_use]
    pub fn disruption_class(&self) -> DisruptionClass {
        match self {
            Self::PvcRequest | Self::ClusterStorage | Self::Host(_) => DisruptionClass::RestartFree,
            Self::PodResize { .. } => DisruptionClass::RestartConditional,
            Self::PodTemplate { .. } | Self::ClusterTopLevel => DisruptionClass::RestartRequiring,
            // App/k8s-plane layouts — restart cost is intrinsic to the mechanism.
            Self::ConfigFile { reload, .. } => reload.disruption_class(),
            Self::ApiCall { .. }
            | Self::DestinationRuleField { .. }
            | Self::NamespaceEnvelope { .. }
            | Self::ControllerSetpoint { .. } => DisruptionClass::RestartFree,
            Self::CrField { restart_free, .. } => {
                if *restart_free { DisruptionClass::RestartFree } else { DisruptionClass::RestartRequiring }
            }
            // host-tc carve never rolls; the pod-annotation fallback re-creates pods.
            Self::PodNetworkBandwidth { host_tc, .. } => {
                if *host_tc { DisruptionClass::RestartFree } else { DisruptionClass::RestartRequiring }
            }
            // Coarse worst-case: a scale-IN sheds a pod (RestartRequiring). The
            // per-direction `action_class` refines a scale-OUT to RestartFree.
            Self::Replica { .. } => DisruptionClass::RestartRequiring,
        }
    }

    /// The PRECISE restart cost of the SPECIFIC carve `(direction, resource)` —
    /// the fact `disruption_class()` throws away. A `PodResize` carve is
    /// `RestartFree` for cpu (either direction) AND for a memory GROW; only a
    /// memory (or other byte-resource) SHRINK is `RestartConditional` (it may
    /// restart per the container's `resizePolicy`). `PvcRequest`/`Host` are always
    /// `RestartFree`; the template-write layouts are always `RestartRequiring`.
    /// This is what lets growth be eager (golden) while only a reclaiming shrink
    /// can require a crossing.
    #[must_use]
    pub fn action_class(&self, growing: bool, resource: &str) -> DisruptionClass {
        match self {
            Self::PvcRequest | Self::ClusterStorage | Self::Host(_) => DisruptionClass::RestartFree,
            Self::PodResize { .. } => {
                if resource == "cpu" || growing {
                    DisruptionClass::RestartFree
                } else {
                    DisruptionClass::RestartConditional
                }
            }
            Self::PodTemplate { .. } | Self::ClusterTopLevel => DisruptionClass::RestartRequiring,
            // The app/k8s-plane layouts have no per-direction restart nuance —
            // their restart cost is the mechanism's, identical to `disruption_class`.
            Self::ConfigFile { .. }
            | Self::ApiCall { .. }
            | Self::CrField { .. }
            | Self::DestinationRuleField { .. }
            | Self::NamespaceEnvelope { .. }
            | Self::ControllerSetpoint { .. }
            | Self::PodNetworkBandwidth { .. } => self.disruption_class(),
            // HORIZONTAL: a scale-OUT (grow) leaves survivors undisturbed
            // (RestartFree); a scale-IN (shrink) sheds a pod (RestartRequiring).
            // `resource` is "replicas"; the direction is what matters.
            Self::Replica { .. } => {
                if growing { DisruptionClass::RestartFree } else { DisruptionClass::RestartRequiring }
            }
        }
    }
}

/// A metric reading + the age of the underlying sample (freshness gate input).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sample {
    pub value: u64,
    pub age_secs: u64,
}

/// Where a dimension's `used` reading comes from. `PodMetricsMax` is the
/// always-on metrics-server (`metrics.k8s.io`) — the live working-set/cpu that
/// `kubectl top` shows, present on any cluster with metrics-server (core)
/// regardless of whether a TSDB is running. `Prometheus` is a PromQL endpoint
/// (historical / volume stats). breathe defaults memory+cpu to `PodMetricsMax`
/// so it never depends on a scale-to-zero TSDB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricSource {
    /// Raw PromQL against a Prometheus-compatible endpoint (storage / historical).
    Prometheus(String),
    /// Max container `resource` (memory bytes / cpu millicores) across a pod group,
    /// read live from metrics-server. When `selector` is set the group is the pods
    /// matching that label selector (the label-selected carve — ARC runners); else
    /// it is the pods whose name starts with `pod_prefix` (the owner's pods).
    PodMetricsMax { resource: String, pod_prefix: String, selector: Option<String> },
    /// HOST: read directly from procfs/sysfs/cgroup via `HostCluster`.
    /// `KubeCluster` rejects this with a typed error.
    Host(HostMetric),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReceipt {
    pub from: u64,
    pub to: u64,
    pub source_hash: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseReceipt {
    pub baseline: Option<u64>,
    pub source_hash: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedReceipt {
    pub source_hash: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderError {
    TargetNotFound,
    MetricsMissing,
    NoCapacityField,
    /// A label-selected pod group currently matches ZERO pods — the band's target
    /// is DORMANT (scaled to zero), not broken. Distinct from `MetricsMissing`
    /// (pods exist but their usage is unreadable): an ephemeral runner / Job /
    /// KEDA-to-zero workload legitimately has no pod most of the time, so this is a
    /// benign resting state the loop reports as `Dormant`, never an error.
    NoTargetPods,
    ApiTransient(String),
    ApiPermanent(String),
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TargetNotFound => f.write_str("target not found"),
            Self::MetricsMissing => f.write_str("metrics missing"),
            Self::NoCapacityField => f.write_str("no capacity field (no limit set)"),
            Self::NoTargetPods => f.write_str("no pods in the label-selected group (dormant)"),
            Self::ApiTransient(m) => write!(f, "transient API error: {m}"),
            Self::ApiPermanent(m) => write!(f, "permanent API error: {m}"),
        }
    }
}

impl std::error::Error for ProviderError {}

/// The SSA field a provider owns (the guard input + status surface).
#[derive(Debug, Clone)]
pub struct OwnedField {
    pub manager: String,
    pub path: String,
}

/// A typed Server-Side-Apply patch. **True SSA only** — carries the `layout` so
/// the `Cluster` impl builds the right nested patch, and the `resource`
/// (`memory`/`cpu`/`storage`) for the leaf key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsaPatch {
    pub target: Target,
    pub field_manager: String,
    pub layout: LimitLayout,
    pub resource: String,
    pub value: u64,
}

/// The side-effecting boundary. Real impl is `KubeCluster`; tests pass
/// `MockCluster`. Dimension-agnostic: `query` runs raw PromQL, `read_limit`
/// reads a quantity at a layout, `field_owners` extracts ownership of a
/// fieldsV1 path, `apply` performs true SSA.
#[async_trait]
pub trait Cluster: Send + Sync {
    /// Read the dimension's `used` scalar from its [`MetricSource`].
    async fn read_used(&self, source: &MetricSource) -> Result<Sample, ProviderError>;
    async fn read_limit(
        &self,
        target: &Target,
        layout: &LimitLayout,
        resource: &str,
    ) -> Result<u64, ProviderError>;
    async fn field_owners(
        &self,
        target: &Target,
        layout: &LimitLayout,
        resource: &str,
        logical_field: &str,
    ) -> Result<Vec<FieldOwner>, ProviderError>;
    async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError>;

    /// Whether an in-place SHRINK of `resource` at `layout` on `target` is
    /// restart-free — `true` iff `layout` is a `PodResize` AND every resized pod's
    /// container declares `resizePolicy[<resource>] = NotRequired`. The default is
    /// the CONSERVATIVE answer (`false` — assume a shrink may restart), so a
    /// cluster impl that does not read the live pod policy (host/storage/mock)
    /// never spuriously claims a golden shrink. Only `KubeCluster` overrides it.
    async fn read_resize_restart_free(
        &self,
        target: &Target,
        layout: &LimitLayout,
        resource: &str,
    ) -> Result<bool, ProviderError> {
        let _ = (target, layout, resource);
        Ok(false)
    }

    /// The target's LIVE declared `resources.requests.<resource>` (max across the
    /// pod group), in the resource's base unit — the inviolable shrink floor (a
    /// limit below the request is invalid in k8s + unsafe). Folded into the effective
    /// `BandConfig.request_floor_bytes` by `reconcile_one`, so the declared request is
    /// honored even when the operator omitted `requestFloor` from the band CR. The
    /// default is `0` (no request) for cluster impls without live-pod access
    /// (host/mock); only `KubeCluster` reads the live pods' `requests`.
    async fn read_request_floor(
        &self,
        target: &Target,
        layout: &LimitLayout,
        resource: &str,
    ) -> Result<u64, ProviderError> {
        let _ = (target, layout, resource);
        Ok(0)
    }

    /// Whether `target`'s pods recently (re)started or are crash-looping — the
    /// restart half of the no-starve signal (a crash-loop means the current low
    /// `used` is a symptom, not proof of safe slack, so a shrink is held). The
    /// default is the conservative `false` (assume stable) for cluster impls without
    /// live-pod access (host/mock); only `KubeCluster` reads the live pod restart
    /// status. Read-only; never mutates.
    async fn read_restarting(
        &self,
        target: &Target,
        layout: &LimitLayout,
        resource: &str,
    ) -> Result<bool, ProviderError> {
        let _ = (target, layout, resource);
        Ok(false)
    }

    /// The discovered STORAGE CAPABILITY of `target`'s backing StorageClass —
    /// `Ok(None)` for every cluster impl / layout with no PVC/StorageClass
    /// concept (host/mock, and every non-storage layout). This is the CAPABILITY-
    /// DISCOVERY half of the fail-fast fix: a `GrowOnly` dimension whose
    /// StorageClass cannot online-expand and/or cannot report per-volume usage
    /// would otherwise grind for days (`data-mysql-0-storage` stuck `Conflict`,
    /// `rustfs-data-storage` stuck `MetricUnrepresentable` — the SAME
    /// `local-path` root cause, two different phases). `KubeCluster` overrides
    /// this ONLY for `LimitLayout::PvcRequest`/`ClusterStorage`, exactly like
    /// `read_resize_restart_free` exists ONLY for `PodResize`. The default is
    /// deliberately the WEAKEST answer (`None` = "unknown, don't gate") so a
    /// cluster impl that can't determine the class never spuriously blocks a
    /// carve — see [`breathe_control::StorageCapability`].
    async fn read_storage_capability(
        &self,
        target: &Target,
        layout: &LimitLayout,
    ) -> Result<Option<StorageCapability>, ProviderError> {
        let _ = (target, layout);
        Ok(None)
    }
}

/// Whether a band's layout has a GOLDEN path to its setpoint — the eclusa
/// reachability question made mechanical + typed. Because the band law is
/// monotone-convergent and every intermediate value is a never-OOM berth
/// (`safety_clamp`), golden reachability reduces to a pure question about the
/// carve actions: does every direction the band may move stay `RestartFree`?
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetpointReachability {
    /// Every carve toward the setpoint is `RestartFree` — golden end to end.
    GoldenToSetpoint,
    /// Reaching the setpoint needs a carve that crosses out of golden (names the
    /// ceiling) — the band can still PARK golden, but only converges to setpoint
    /// once the operator's `DisruptionPolicy` permits the crossing.
    RequiresCrossing { ceiling: DisruptionClass, layout: LimitLayout },
}

/// Does `layout` have a golden path to the setpoint for `resource`, given the
/// directions `dir` lets the band move? `ObserveOnly` never carves ⇒ trivially
/// golden; `GrowOnly` checks only the grow direction; `Bidirectional` needs BOTH
/// grow and shrink to be `RestartFree`. Policy-independent: golden-ness is a
/// property of the action space, not of whether the operator permits a crossing.
#[must_use]
pub fn setpoint_reachability(layout: &LimitLayout, dir: Directionality, resource: &str) -> SetpointReachability {
    let directions: &[bool] = match dir {
        Directionality::Bidirectional => &[true, false],
        Directionality::GrowOnly => &[true],
        Directionality::ObserveOnly => &[],
    };
    for &growing in directions {
        let class = layout.action_class(growing, resource);
        if !class.edge_tier().is_golden() {
            return SetpointReachability::RequiresCrossing { ceiling: class, layout: layout.clone() };
        }
    }
    SetpointReachability::GoldenToSetpoint
}

/// The per-dimension data + small layout logic — everything that is genuinely
/// dimension-specific. The observe/assign/release orchestration lives once in
/// [`BandProvider`], so a new dimension is *only* an impl of this trait + a
/// catalog row. It can carry no band logic (no `decide`/`BandConfig`).
pub trait DimensionDescriptor: Send + Sync + 'static {
    /// Construct the descriptor for a cluster that can (`resize_capable`) or
    /// cannot carve a pod-backed workload in place (`pods/resize`, k8s ≥1.33).
    /// This is the K1 "breathe never rolls" default: a dimension that *can* carve
    /// zero-disruption (memory/cpu via `PodResize`) prefers it whenever the
    /// cluster supports it; dimensions that are already zero-disruption
    /// (storage/host) or always roll ignore the capability. Default = ignore it
    /// (`Self::default()`); memory/cpu override to flip on `in_place`.
    fn with_resize_capability(resize_capable: bool) -> Self
    where
        Self: Sized + Default,
    {
        let _ = resize_capable;
        Self::default()
    }

    fn id(&self) -> DimensionId;
    fn directionality(&self) -> Directionality;
    /// SSA field manager (disjoint across dimensions → memory ⟂ cpu, breathe ⟂ KEDA).
    fn field_manager(&self) -> &'static str;
    /// Stable logical field label (layout-independent) — both the guard's
    /// `owned_field().path` and the stamped `FieldOwner.field` use this.
    fn logical_field(&self) -> &'static str;
    /// The leaf resource key in `limits`/`requests` (`memory`/`cpu`/`storage`).
    fn resource(&self) -> &'static str;
    fn semantics(&self) -> ApplySemantics;
    /// Where this dimension's limit lives on the given target.
    fn layout(&self, target: &Target) -> LimitLayout;
    /// The PromQL whose scalar is the dimension's `used`.
    fn metric_source(&self, target: &Target) -> MetricSource;

    /// How this dimension OBSERVES its SUPPRESSED DEMAND — the signal that proves a
    /// workload wants more than its (hard-capped) `used` can show. This is the
    /// structural fix for the CPU-blindness ratchet (the pangea-operator 2026-06
    /// starve): a CFS-throttled workload's usage can never exceed its limit, so the
    /// usage metric alone ratchets a bursty/idle CPU band to its floor. Each
    /// [`SuppressedDemand`] variant says HOW the suppressed demand becomes visible;
    /// for [`SuppressedDemand::CfsThrottling`] the descriptor returns a throttle
    /// [`MetricSource`] (the `PromQL` `rate(container_cpu_cfs_throttled_periods_total)`
    /// — mirrors the storage dimension's `PromQL` path) whose non-zero scalar the
    /// reconcile layer maps onto `Observation.throttle_signal`.
    ///
    /// The DEFAULT is `WorkingSetExceedsSoftLimit` with NO throttle source — the
    /// memory/storage case, where suppressed demand is already visible (the working
    /// set spikes ABOVE the soft limit and folds into the peak; storage is grow-only).
    /// A new dimension MUST consciously pick its variant: the catalog reflection test
    /// fails the build if a [`DimensionId`] declares a `kind` whose contract this
    /// descriptor cannot honor, so no future dimension can be carve-blind the way CPU
    /// was. CPU overrides this to `CfsThrottling` + a throttle source.
    fn suppressed_demand(&self) -> SuppressedDemand {
        SuppressedDemand::WorkingSetExceedsSoftLimit
    }

    /// The optional throttle [`MetricSource`] whose non-zero scalar the reconcile
    /// layer maps onto `Observation.throttle_signal`. `Some` ONLY for a dimension
    /// whose [`suppressed_demand`](Self::suppressed_demand) is observed by a separate
    /// throttle read (CPU's CFS-throttling `PromQL`); `None` (the default) for every
    /// dimension whose suppressed demand is already in the primary `used`/peak path
    /// (memory's over-soft-limit spike) or has no suppressed-demand hole (grow-only
    /// storage, observe-only replica). `target` lets the query be entity-scoped.
    fn throttle_source(&self, target: &Target) -> Option<MetricSource> {
        let _ = target;
        None
    }
}

/// The spine — the dyn interface `breathe-core` reconciles through.
#[async_trait]
pub trait ResourceProvider: Send + Sync + 'static {
    fn id(&self) -> DimensionId;
    fn directionality(&self) -> Directionality;
    fn owned_field(&self) -> OwnedField;
    fn semantics(&self) -> ApplySemantics;
    /// The layout (plane) this dimension carves `target` at — carries the restart
    /// class. The loop reads this to NAME the action it is about to take.
    fn layout_for(&self, target: &Target) -> LimitLayout;
    /// The leaf resource key (`memory`/`cpu`/`storage`) — for the per-direction class.
    fn resource_key(&self) -> &str;
    /// The PRECISE restart class of the carve this provider would make on `target`
    /// in the `growing` direction. The loop consults this against the band's
    /// `DisruptionPolicy` before committing a carve (the golden-edge gate).
    fn action_class(&self, target: &Target, growing: bool) -> DisruptionClass {
        self.layout_for(target).action_class(growing, self.resource_key())
    }
    /// Whether this provider has a golden (restart-free) path to the setpoint.
    fn setpoint_reachability(&self, target: &Target) -> SetpointReachability {
        setpoint_reachability(&self.layout_for(target), self.directionality(), self.resource_key())
    }
    async fn observe(&self, target: &Target) -> Result<Observation, ProviderError>;
    async fn assign(&self, target: &Target, to_value: u64)
        -> Result<AssignReceipt, ProviderError>;
    async fn release(&self, target: &Target) -> Result<ReleaseReceipt, ProviderError>;
}

/// **The one generic provider.** Implements [`ResourceProvider`] for every
/// dimension; the dimension's specifics come from its `DimensionDescriptor`.
/// Adding a dimension never touches this code — that is the whole compounding
/// claim, made by one type.
pub struct BandProvider<C: Cluster + 'static, D: DimensionDescriptor> {
    cluster: C,
    descriptor: D,
}

impl<C: Cluster + 'static, D: DimensionDescriptor> BandProvider<C, D> {
    pub fn new(cluster: C, descriptor: D) -> Self {
        Self { cluster, descriptor }
    }
    /// Borrow the cluster (tests assert applied patches).
    pub fn cluster(&self) -> &C {
        &self.cluster
    }
}

#[async_trait]
impl<C: Cluster + 'static, D: DimensionDescriptor> ResourceProvider for BandProvider<C, D> {
    fn id(&self) -> DimensionId {
        self.descriptor.id()
    }
    fn directionality(&self) -> Directionality {
        self.descriptor.directionality()
    }
    fn owned_field(&self) -> OwnedField {
        OwnedField {
            manager: self.descriptor.field_manager().to_string(),
            path: self.descriptor.logical_field().to_string(),
        }
    }
    fn semantics(&self) -> ApplySemantics {
        self.descriptor.semantics()
    }
    fn layout_for(&self, target: &Target) -> LimitLayout {
        self.descriptor.layout(target)
    }
    fn resource_key(&self) -> &str {
        self.descriptor.resource()
    }

    async fn observe(&self, target: &Target) -> Result<Observation, ProviderError> {
        let layout = self.descriptor.layout(target);
        let source = self.descriptor.metric_source(target);
        let used = match self.cluster.read_used(&source).await {
            // A `PodMetricsMax` read with NO selector is the owner-name-PREFIX scan
            // (Deployment/StatefulSet/CNPG owners) — by design it reports
            // `MetricsMissing` on zero matching pods (an owner with no pods is
            // abnormal), which is ambiguous: the owner may simply have no pods YET
            // (transient — the original diagnosis stands), or the targetRef itself
            // may be dangling (a stale/typo'd owner — task #217's root cause).
            // Disambiguate with the SAME owner GET `read_limit` performs below (never
            // duplicated — this literally calls it): a 404 there means the owner is
            // genuinely gone and `read_limit` already maps that to `TargetNotFound`,
            // which `?` propagates here so it self-heals the instant the real owner
            // appears; any other outcome means the owner exists and the original
            // `MetricsMissing` diagnosis stands. A `selector`-scoped read (label-
            // selected pod groups — ARC runners) already reports the distinct
            // `NoTargetPods`/`Dormant`, never reaches this arm.
            Err(ProviderError::MetricsMissing)
                if matches!(&source, MetricSource::PodMetricsMax { selector: None, .. }) =>
            {
                self.cluster.read_limit(target, &layout, self.descriptor.resource()).await?;
                return Err(ProviderError::MetricsMissing);
            }
            other => other?,
        };
        let capacity = self.cluster.read_limit(target, &layout, self.descriptor.resource()).await?;
        let owners = self
            .cluster
            .field_owners(target, &layout, self.descriptor.resource(), self.descriptor.logical_field())
            .await?;
        // Restart-cost refinement input: is an in-place shrink of this resource
        // restart-free on this target (resizePolicy NotRequired)? Conservative
        // false everywhere the Cluster impl does not read a live pod policy.
        let memory_shrink_restart_free = self
            .cluster
            .read_resize_restart_free(target, &layout, self.descriptor.resource())
            .await?;
        // The target's LIVE declared requests.<resource> — the inviolable shrink
        // floor, sourced from the running pods so it is honored even when the band CR
        // omitted it. `0` for cluster impls with no live-pod access (host/mock).
        let request_floor = self
            .cluster
            .read_request_floor(target, &layout, self.descriptor.resource())
            .await?;
        // SUPPRESSED-DEMAND READ (the CPU-blindness fix): for a dimension whose
        // suppressed demand is observed by a SEPARATE throttle read (cpu's CFS
        // throttling), read it here and project onto `throttle_signal`. The throttle
        // is read through the SAME `read_used` seam the storage dimension already uses
        // for PromQL — one path, no new I/O primitive. A throttle-read FAILURE is
        // fail-SAFE: it maps to `0` (no signal), so the band proceeds on usage exactly
        // as before — a missing throttle metric never forces nor blocks a carve.
        let throttle_signal = match self.descriptor.throttle_source(target) {
            Some(src) => self.cluster.read_used(&src).await.map_or(0, |s| s.value),
            None => 0,
        };
        // The restart half of the no-starve signal — `false` for impls without
        // live-pod access (host/mock); `KubeCluster` reads the live restart status.
        let restarting = self
            .cluster
            .read_restarting(target, &layout, self.descriptor.resource())
            .await
            .unwrap_or(false);
        // CAPABILITY-DISCOVERY READ (the fail-fast fix): only a `PvcRequest`/
        // `ClusterStorage` layout on `KubeCluster` ever returns `Some` here —
        // every other layout/impl keeps the default `Ok(None)`. Fail-SAFE like
        // the throttle read above: a transient StorageClass-lookup failure
        // (RBAC, a momentary API blip) maps to `None` (unknown ⇒ don't gate),
        // so a read hiccup never spuriously blocks a carve that was otherwise fine.
        let storage_capability = self
            .cluster
            .read_storage_capability(target, &layout)
            .await
            .unwrap_or(None);
        Ok(Observation {
            used: used.value,
            // The provider is HISTORY-FREE: it reports the instantaneous peak (==
            // the current sample). The reconcile layer (which carries the cross-tick
            // trailing-window peak from the band status) folds the real demonstrated
            // peak in before the decision (see `breathe_core::reconcile_one`).
            peak_used: used.value,
            capacity,
            owners,
            staleness_secs: used.age_secs,
            memory_shrink_restart_free,
            // The provider is RESTART-HISTORY-FREE: it reports "warmup not applicable"
            // (`u64::MAX` ⇒ always past warmup). The reconcile layer raises it to the
            // real observed-since-restart age (from the band's warmup-start epoch /
            // pod startTime) before the decision, exactly as it folds in `peak_used`.
            observed_for_secs: u64::MAX,
            request_floor,
            throttle_signal,
            restarting,
            storage_capability,
        })
    }

    async fn assign(&self, target: &Target, to_value: u64) -> Result<AssignReceipt, ProviderError> {
        let layout = self.descriptor.layout(target);
        let from = self.cluster.read_limit(target, &layout, self.descriptor.resource()).await?;
        if to_value == from {
            return Ok(AssignReceipt { from, to: to_value, source_hash: [0u8; 16] });
        }
        let patch = SsaPatch {
            target: target.clone(),
            field_manager: self.descriptor.field_manager().to_string(),
            layout,
            resource: self.descriptor.resource().to_string(),
            value: to_value,
        };
        let applied = self.cluster.apply(&patch).await?;
        Ok(AssignReceipt { from, to: to_value, source_hash: applied.source_hash })
    }

    async fn release(&self, _target: &Target) -> Result<ReleaseReceipt, ProviderError> {
        Ok(ReleaseReceipt { baseline: None, source_hash: [0u8; 16] })
    }
}

// ============================================================================
// M0 — the resource-ether shape lift (the breathe provisioning extension).
//
// `Forma` is the infra-scale SIBLING of `DimensionId`: where a dimension slices
// a resource WITHIN a fixed envelope (memory in a pod), a forma provisions the
// envelope ITSELF (a node, a spot seat, a GPU). The two are orthogonal and
// compose; BOTH project to a scalar `(used, capacity)` the SAME band law carves.
// M0 ships the typed seed + the K2 keystone proof (the band law is shape-blind —
// it converges on a node COUNT exactly as on bytes, into the deadband). The
// provisioning I/O (`provision`/`deprovision` actually mutating) lands at M2,
// gated on magma. Validated admission + the auction land as breathe-admission /
// breathe-auction. Canonical spec: docs/PROVISIONING.md.
// ============================================================================

/// A SHAPE of resource — the infra-scale peer of [`DimensionId`]. M0 ships only
/// the seed shape `NodeOnDemand`; M3+ add `NodeSpot` / `Accelerator` /
/// `ServerlessSlot` / `EdgePlacement` / `JitBuilder` / … (docs/PROVISIONING.md §8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum Forma {
    /// An on-demand cloud node: `used` = node demand (scheduled + pending),
    /// `capacity` = the node-pool ceiling (the `Densa` envelope). Provisioned via
    /// a magma `Plan` at M2 — never a direct cloud-API call.
    NodeOnDemand,
    /// A spot/interruptible node — cheaper, interruption-prone; replaced via
    /// `Reformar` (spot→on-demand) on interruption (census #101).
    NodeSpot,
    /// EBS provisioned IOPS (`ec2 ModifyVolume`) — rate-shaped, modify-cadence-bound (#102).
    ProvisionedIops,
    /// EBS/EFS provisioned throughput (bytes/s) — rate-shaped (#103).
    ProvisionedThroughput,
    /// DynamoDB RCU/WCU (`UpdateTable`) — exact-fit rate band (#104).
    DynamoCapacity,
    /// Reserved/committed capacity (Savings Plans / RIs) — GROW-ONLY (term-locked) (#105).
    Commitment,
    /// GPU / accelerator count (GPU node-pool / Karpenter NodeClaim) (#107).
    Accelerator,
    /// Lambda provisioned concurrency (`PutProvisionedConcurrencyConfig`) (#113).
    ServerlessSlot,
    /// Edge/zone capacity reservation (ODCR `CreateCapacityReservation`) (#114).
    ZoneCapacity,
    /// Placement at an edge location (per-zone) (#114).
    EdgePlacement,
    /// Load-balancer capacity units (ALB/NLB LCU) — ramp dead-time, PREDICTIVE (#111).
    LbCapacity,
    /// NAT egress bandwidth budget (bits/s + cost) (#110).
    EgressBandwidth,
    /// JIT builder capacity — ASG 0→N wake (`cordel builder-wake`); TRUE-ZERO floor (#115).
    JitBuilder,
    /// Log-ingestion / sampling rate — per-class drop floor (#112).
    LogIngestion,
}

impl Forma {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NodeOnDemand => "node-on-demand",
            Self::NodeSpot => "node-spot",
            Self::ProvisionedIops => "provisioned-iops",
            Self::ProvisionedThroughput => "provisioned-throughput",
            Self::DynamoCapacity => "dynamo-capacity",
            Self::Commitment => "commitment",
            Self::Accelerator => "accelerator",
            Self::ServerlessSlot => "serverless-slot",
            Self::ZoneCapacity => "zone-capacity",
            Self::EdgePlacement => "edge-placement",
            Self::LbCapacity => "lb-capacity",
            Self::EgressBandwidth => "egress-bandwidth",
            Self::JitBuilder => "jit-builder",
            Self::LogIngestion => "log-ingestion",
        }
    }
}

impl std::fmt::Display for Forma {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The per-shape data + projection logic — the infra-scale peer of
/// [`DimensionDescriptor`]. A new shape is an impl of this + a catalog row; it
/// carries NO band logic (no `decide`/`BandConfig`), exactly like a dimension.
pub trait FormaDescriptor: Send + Sync + 'static {
    fn forma(&self) -> Forma;
    fn directionality(&self) -> Directionality;
    /// The provisioning dead-time: how long one `provision(1)` takes to become
    /// usable capacity. The predictor MUST forecast ≥ this far ahead or
    /// provisioning is always late (BREATHABILITY-MATH §5.3, thesis P8). Seconds.
    fn relief_latency_secs(&self) -> u64;
    /// The unit one `provision(1)` adds (`"node"`, `"gpu"`, `"slot"`).
    fn unit(&self) -> &'static str;
}

/// **Step-15:** a data-driven `FormaDescriptor` — the abstracting peer of
/// `HostParamDescriptor`. Instead of one struct per Forma, the whole Forma
/// universe is the [`FORMA_CATALOG`] table of these; a new cloud shape is a
/// ROW, not new code. The Provedor (the magma `Plan` actuator) is per-deployment
/// (`SimProvedor`/`KwokProvedor` for testing, a magma backend for real — T4,
/// externally gated); this carries only the typed shape + dead-time + unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormaSpec {
    pub forma: Forma,
    pub directionality: Directionality,
    pub relief_latency_secs: u64,
    pub unit: &'static str,
}

impl FormaDescriptor for FormaSpec {
    fn forma(&self) -> Forma {
        self.forma
    }
    fn directionality(&self) -> Directionality {
        self.directionality
    }
    fn relief_latency_secs(&self) -> u64 {
        self.relief_latency_secs
    }
    fn unit(&self) -> &'static str {
        self.unit
    }
}

/// **Step-15:** the whole Forma universe as DATA — every cloud carve shape with
/// its directionality, provisioning dead-time (the predictor's look-ahead floor),
/// and unit. Bidirectional unless the resource is irreversible (`Commitment` is
/// term-locked → GrowOnly). A new shape is one row here + a Provedor; the band
/// law is shape-blind, so it converges on a GPU count or LB unit exactly as a node.
pub const FORMA_CATALOG: &[FormaSpec] = &[
    FormaSpec { forma: Forma::NodeOnDemand, directionality: Directionality::Bidirectional, relief_latency_secs: 180, unit: "node" },
    FormaSpec { forma: Forma::NodeSpot, directionality: Directionality::Bidirectional, relief_latency_secs: 120, unit: "node" },
    FormaSpec { forma: Forma::ProvisionedIops, directionality: Directionality::Bidirectional, relief_latency_secs: 60, unit: "iops" },
    FormaSpec { forma: Forma::ProvisionedThroughput, directionality: Directionality::Bidirectional, relief_latency_secs: 60, unit: "bytes-per-sec" },
    FormaSpec { forma: Forma::DynamoCapacity, directionality: Directionality::Bidirectional, relief_latency_secs: 30, unit: "capacity-unit" },
    FormaSpec { forma: Forma::Commitment, directionality: Directionality::GrowOnly, relief_latency_secs: 3600, unit: "cents" },
    FormaSpec { forma: Forma::Accelerator, directionality: Directionality::Bidirectional, relief_latency_secs: 300, unit: "gpu" },
    FormaSpec { forma: Forma::ServerlessSlot, directionality: Directionality::Bidirectional, relief_latency_secs: 60, unit: "slot" },
    FormaSpec { forma: Forma::ZoneCapacity, directionality: Directionality::Bidirectional, relief_latency_secs: 120, unit: "instance" },
    FormaSpec { forma: Forma::EdgePlacement, directionality: Directionality::Bidirectional, relief_latency_secs: 120, unit: "instance" },
    FormaSpec { forma: Forma::LbCapacity, directionality: Directionality::Bidirectional, relief_latency_secs: 180, unit: "lcu" },
    FormaSpec { forma: Forma::EgressBandwidth, directionality: Directionality::Bidirectional, relief_latency_secs: 120, unit: "bits-per-sec" },
    FormaSpec { forma: Forma::JitBuilder, directionality: Directionality::Bidirectional, relief_latency_secs: 120, unit: "builder" },
    FormaSpec { forma: Forma::LogIngestion, directionality: Directionality::Bidirectional, relief_latency_secs: 30, unit: "percent" },
];

/// Look up a Forma's spec (its descriptor data) from the catalog.
#[must_use]
pub fn forma_spec(forma: Forma) -> Option<&'static FormaSpec> {
    FORMA_CATALOG.iter().find(|s| s.forma == forma)
}

/// The shape's current `(used, capacity)` scalars — the band law's two inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormaSample {
    /// Demand for this shape (scheduled + pending), in `unit`s.
    pub used: u64,
    /// The provisioned ceiling for this shape (the `Densa` envelope), in `unit`s.
    pub capacity: u64,
}

/// Proof of a (idempotent) provision/deprovision action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProvisionReceipt {
    /// Observe-only (`dry_run`) — nothing was mutated; what the M0 seed + any
    /// shadow forma return. `would` is the signed unit delta it WOULD have applied.
    DryRun { would: i64 },
    /// A real provision/deprovision was dispatched (M2+): `delta` units via the
    /// attested `plan_id` (a magma `Plan` BLAKE3 id — never a direct cloud call).
    Applied { delta: i64, plan_id: String },
    /// No-op — already at the requested count (idempotent).
    NoOp,
}

/// The provisioning I/O boundary — the infra-scale peer of the [`Cluster`]
/// trait. Real impls (M2+) emit a magma `Plan`; the M0 seed + tests observe
/// only. It OBSERVES the shape's `(used, capacity)` and PROVISIONS/DEPROVISIONS
/// units — but CANNOT re-decide: it receives "grow by N" and returns proof +
/// readiness. Idempotent provision; graceful (cordon→drain) deprovision.
#[async_trait]
pub trait Provedor: Send + Sync {
    /// The shape's current `(used, capacity)` for the band law.
    async fn observe(&self) -> Result<FormaSample, ProviderError>;
    /// Idempotently provision `n` more units. Observe-only impls mutate nothing
    /// and return [`ProvisionReceipt::DryRun`].
    async fn provision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError>;
    /// Gracefully deprovision `n` units (cordon→drain, PDB-aware). Observe-only
    /// impls return [`ProvisionReceipt::DryRun`].
    async fn deprovision(&self, n: u64) -> Result<ProvisionReceipt, ProviderError>;
}

/// The M0 seed descriptor — `Forma::NodeOnDemand`, bidirectional, node-grained.
/// A node add is restart-free for existing pods (the node joins; nothing rolls);
/// a node removal is a PDB-aware drain. `relief_latency` is the cloud's
/// node-boot-to-Ready time (minutes — the dead-time the predictor looks ahead by).
#[derive(Debug, Clone, Copy, Default)]
pub struct NodeOnDemandDescriptor;

impl FormaDescriptor for NodeOnDemandDescriptor {
    fn forma(&self) -> Forma {
        Forma::NodeOnDemand
    }
    fn directionality(&self) -> Directionality {
        Directionality::Bidirectional
    }
    fn relief_latency_secs(&self) -> u64 {
        180 // ~3 min node boot→Ready; refined per-provider at M2
    }
    fn unit(&self) -> &'static str {
        "node"
    }
}

#[cfg(test)]
mod tests {
    use super::{DisruptionClass, DisruptionPolicy, HostKnob, LimitLayout};
    use super::{forma_spec, Directionality, Forma, FormaDescriptor, FORMA_CATALOG};

    #[test]
    fn forma_catalog_covers_the_whole_universe_with_no_duplicates() {
        // every Forma variant resolves to exactly one catalog spec (a bijection),
        // and the spec's data drives the descriptor (the data-driven FormaSpec).
        let all = [
            Forma::NodeOnDemand, Forma::NodeSpot, Forma::ProvisionedIops, Forma::ProvisionedThroughput,
            Forma::DynamoCapacity, Forma::Commitment, Forma::Accelerator, Forma::ServerlessSlot,
            Forma::ZoneCapacity, Forma::EdgePlacement, Forma::LbCapacity, Forma::EgressBandwidth,
            Forma::JitBuilder, Forma::LogIngestion,
        ];
        assert_eq!(FORMA_CATALOG.len(), all.len(), "catalog row count == Forma variant count");
        for f in all {
            let spec = forma_spec(f).expect("every Forma has a catalog spec");
            assert_eq!(spec.forma(), f);
            assert!(spec.relief_latency_secs() > 0, "{f} has a provisioning dead-time");
            assert!(!spec.unit().is_empty());
        }
        // the one irreversible shape is GrowOnly; an on-demand node breathes both ways.
        assert_eq!(forma_spec(Forma::Commitment).unwrap().directionality(), Directionality::GrowOnly);
        assert_eq!(forma_spec(Forma::NodeOnDemand).unwrap().directionality(), Directionality::Bidirectional);
        assert_eq!(Forma::JitBuilder.as_str(), "jit-builder");
    }

    #[test]
    fn layouts_classify_by_restart_cost() {
        assert_eq!(LimitLayout::PvcRequest.disruption_class(), DisruptionClass::RestartFree);
        assert_eq!(LimitLayout::Host(HostKnob::ZfsArcMax).disruption_class(), DisruptionClass::RestartFree);
        // PodResize is honestly RestartConditional (memory-shrink may restart).
        assert_eq!(LimitLayout::PodResize { container: None }.disruption_class(), DisruptionClass::RestartConditional);
        assert_eq!(LimitLayout::PodTemplate { container: None }.disruption_class(), DisruptionClass::RestartRequiring);
        assert_eq!(LimitLayout::ClusterTopLevel.disruption_class(), DisruptionClass::RestartRequiring);
    }

    #[test]
    fn pod_resize_is_strictly_less_disruptive_than_pod_template() {
        // the keystone: the SAME carve is RestartRequiring via the template but
        // only RestartConditional via resize — never a forced roll.
        let roll = LimitLayout::PodTemplate { container: Some("app".into()) }.disruption_class();
        let live = LimitLayout::PodResize { container: Some("app".into()) }.disruption_class();
        assert_eq!(roll, DisruptionClass::RestartRequiring);
        assert!(roll.may_restart() && live.may_restart());
        assert_ne!(live, DisruptionClass::RestartRequiring); // resize never forces a full roll
    }

    #[test]
    fn edge_tier_is_golden_iff_restart_free() {
        use DisruptionClass::{RestartConditional, RestartFree, RestartRequiring};
        assert!(RestartFree.edge_tier().is_golden());
        assert!(!RestartConditional.edge_tier().is_golden());
        assert!(!RestartRequiring.edge_tier().is_golden());
        assert_eq!(RestartRequiring.edge_tier(), super::EdgeTier::CeilingCrossing(RestartRequiring));
    }

    #[test]
    fn action_class_is_per_direction_and_per_resource() {
        use DisruptionClass::{RestartConditional, RestartFree, RestartRequiring};
        let resize = LimitLayout::PodResize { container: None };
        // memory: grow is golden/RestartFree, shrink may restart → conditional.
        assert_eq!(resize.action_class(true, "memory"), RestartFree);
        assert_eq!(resize.action_class(false, "memory"), RestartConditional);
        // cpu never restarts, either direction.
        assert_eq!(resize.action_class(true, "cpu"), RestartFree);
        assert_eq!(resize.action_class(false, "cpu"), RestartFree);
        // host + pvc always restart-free; template always requires a roll.
        assert_eq!(LimitLayout::PvcRequest.action_class(false, "storage"), RestartFree);
        assert_eq!(LimitLayout::Host(HostKnob::ZfsArcMax).action_class(false, "memory"), RestartFree);
        assert_eq!(LimitLayout::PodTemplate { container: None }.action_class(true, "memory"), RestartRequiring);
    }

    #[test]
    fn resize_policy_refines_only_the_conditional_shrink() {
        use DisruptionClass::{RestartConditional, RestartFree, RestartRequiring};
        // The ONLY class resizePolicy refines: a conditional memory shrink with
        // NotRequired becomes golden; with RestartContainer (false) it stays.
        assert_eq!(RestartConditional.refined_by_resize_policy(true), RestartFree);
        assert_eq!(RestartConditional.refined_by_resize_policy(false), RestartConditional);
        // Every other class is invariant under the flag (no spurious downgrade of a
        // template roll, no change to an already-golden grow).
        assert_eq!(RestartRequiring.refined_by_resize_policy(true), RestartRequiring);
        assert_eq!(RestartFree.refined_by_resize_policy(true), RestartFree);
        // Composed with the per-direction class: a NotRequired pod's memory shrink
        // is golden end to end; a grow already was.
        let resize = LimitLayout::PodResize { container: None };
        assert_eq!(resize.action_class(false, "memory").refined_by_resize_policy(true), RestartFree);
        assert_eq!(resize.action_class(false, "memory").refined_by_resize_policy(false), RestartConditional);
    }

    #[test]
    fn setpoint_reachability_names_the_golden_paths() {
        use super::{setpoint_reachability, Directionality, DisruptionClass, SetpointReachability};
        // cpu in-place: golden both directions.
        assert_eq!(
            setpoint_reachability(&LimitLayout::PodResize { container: None }, Directionality::Bidirectional, "cpu"),
            SetpointReachability::GoldenToSetpoint
        );
        // storage online-expand (grow-only): golden.
        assert_eq!(
            setpoint_reachability(&LimitLayout::PvcRequest, Directionality::GrowOnly, "storage"),
            SetpointReachability::GoldenToSetpoint
        );
        // memory in-place, bidirectional: the SHRINK is a conditional crossing.
        assert_eq!(
            setpoint_reachability(&LimitLayout::PodResize { container: None }, Directionality::Bidirectional, "memory"),
            SetpointReachability::RequiresCrossing { ceiling: DisruptionClass::RestartConditional, layout: LimitLayout::PodResize { container: None } }
        );
        // CNPG top-level: any carve is a full crossing.
        assert!(matches!(
            setpoint_reachability(&LimitLayout::ClusterTopLevel, Directionality::Bidirectional, "memory"),
            SetpointReachability::RequiresCrossing { ceiling: DisruptionClass::RestartRequiring, .. }
        ));
    }

    #[test]
    fn disruption_policy_gates_actions_by_class() {
        use DisruptionClass::{RestartConditional, RestartFree, RestartRequiring};
        // RestartFreeOnly (the default): only restart-free actions pass.
        assert_eq!(DisruptionPolicy::default(), DisruptionPolicy::RestartFreeOnly);
        assert!(DisruptionPolicy::RestartFreeOnly.permits(RestartFree));
        assert!(!DisruptionPolicy::RestartFreeOnly.permits(RestartConditional));
        assert!(!DisruptionPolicy::RestartFreeOnly.permits(RestartRequiring));
        // AllowConditional: free + conditional, never a full roll.
        assert!(DisruptionPolicy::AllowConditional.permits(RestartConditional));
        assert!(!DisruptionPolicy::AllowConditional.permits(RestartRequiring));
        // AllowRestart: everything.
        assert!(DisruptionPolicy::AllowRestart.permits(RestartRequiring));
    }

    #[test]
    fn fill_policy_defaults_pack_and_maps_the_scheduler_hint() {
        use super::FillPolicy;
        // Pack is the efficiency-first default; the enum maps to the scheduler
        // NodeResourcesFit scoringStrategy breathe surfaces (it never binds pods).
        assert_eq!(FillPolicy::default(), FillPolicy::Pack);
        assert!(FillPolicy::Pack.is_pack());
        assert!(!FillPolicy::Spread.is_pack());
        assert_eq!(FillPolicy::Pack.scheduler_scoring(), "MostAllocated");
        assert_eq!(FillPolicy::Spread.scheduler_scoring(), "LeastAllocated");
        assert_eq!(FillPolicy::Spread.to_string(), "spread");
    }
}

/// A programmable in-memory [`Cluster`] for tests — the typed-spec-triplet
/// testability seam. Records every SSA patch; programmable used/limit/owners.
#[cfg(feature = "mock")]
pub mod mock {
    use super::{
        AppliedReceipt, Cluster, FieldOwner, LimitLayout, MetricSource, ProviderError, Sample,
        SsaPatch, StorageCapability, Target,
    };
    use async_trait::async_trait;
    use std::sync::Mutex;

    pub struct MockCluster {
        pub used: Sample,
        pub limit: u64,
        pub owners: Vec<FieldOwner>,
        /// What `read_resize_restart_free` returns (default false = conservative;
        /// set true to model a `resizePolicy[memory] = NotRequired` pod).
        pub resize_restart_free: bool,
        /// When set, `read_used` returns this error instead of a sample — models a
        /// dormant target (`NoTargetPods`) or a metric outage (`MetricsMissing`) so
        /// the reconcile loop's error/dormant arms are testable.
        pub read_used_error: Option<ProviderError>,
        /// When set, `read_limit` returns this error instead of `Ok(limit)` — models
        /// a dangling targetRef (`TargetNotFound`, the owner GET 404s) so
        /// `observe()`'s `MetricsMissing`-vs-`TargetNotFound` disambiguation (task
        /// #217) is testable against the mock, the same way `read_used_error` models
        /// a metric-side failure.
        pub read_limit_error: Option<ProviderError>,
        /// What `read_request_floor` returns — the live declared `requests.<resource>`
        /// (default 0 = none). Set it to model a pod with a declared request floor
        /// that a shrink may never carve beneath (Part 3).
        pub request_floor: u64,
        /// The throttle scalar a SEPARATE throttle-source `read_used` returns (default
        /// 0 = no throttle). Set it to model a CFS-throttled CPU workload — the
        /// suppressed-demand signal that closes the CPU-blindness ratchet. Returned for
        /// ANY `read_used` whose source is the registered `throttle_source` value.
        pub throttle_signal: u64,
        /// What `read_restarting` returns (default false = stable). Set it to model a
        /// recently-restarted / crash-looping target whose shrink must be held.
        pub restarting: bool,
        /// The `MetricSource` a descriptor's `throttle_source` returns for this mock —
        /// when `read_used` is called with this exact source, the mock returns
        /// `throttle_signal` instead of `used` (so a CPU descriptor's throttle read is
        /// driveable end-to-end against the mock).
        pub throttle_source: Option<MetricSource>,
        /// What `read_storage_capability` returns (default `None` = unknown, don't
        /// gate). Set it to model a discovered StorageClass — `Some(cap)` where
        /// `cap.is_supported()` is false drives the `TickPlan::CapabilityMissing`
        /// gate end-to-end against the mock.
        pub storage_capability: Option<StorageCapability>,
        applied: Mutex<Vec<SsaPatch>>,
    }

    impl MockCluster {
        #[must_use]
        pub fn new(used: u64, age_secs: u64, limit: u64, owners: Vec<FieldOwner>) -> Self {
            Self {
                used: Sample { value: used, age_secs },
                limit,
                owners,
                resize_restart_free: false,
                read_used_error: None,
                read_limit_error: None,
                request_floor: 0,
                throttle_signal: 0,
                restarting: false,
                throttle_source: None,
                storage_capability: None,
                applied: Mutex::new(Vec::new()),
            }
        }
        /// Model a CFS-throttled workload (the CPU-blindness case): a non-zero throttle
        /// signal returned via the registered `throttle_source`. The default
        /// `throttle_source` (`MetricSource::Prometheus("throttle")`) matches what a
        /// test descriptor returns, so the suppressed-demand read fires end-to-end.
        #[must_use]
        pub fn with_throttle(mut self, signal: u64) -> Self {
            self.throttle_signal = signal;
            self.throttle_source
                .get_or_insert_with(|| MetricSource::Prometheus("throttle".into()));
            self
        }
        /// Register the exact `MetricSource` the descriptor's `throttle_source` returns,
        /// so `read_used` of that source yields `throttle_signal` (not `used`).
        #[must_use]
        pub fn with_throttle_source(mut self, src: MetricSource) -> Self {
            self.throttle_source = Some(src);
            self
        }
        /// Model a recently-restarted / crash-looping target (a shrink must be held).
        #[must_use]
        pub fn with_restarting(mut self, v: bool) -> Self {
            self.restarting = v;
            self
        }
        /// Model a pod whose `resizePolicy` makes an in-place shrink restart-free.
        #[must_use]
        pub fn with_resize_restart_free(mut self, v: bool) -> Self {
            self.resize_restart_free = v;
            self
        }
        /// Model a pod with a declared `requests.<resource>` floor (Part 3) — a
        /// shrink may never carve the limit beneath this even if the band CR omits it.
        #[must_use]
        pub fn with_request_floor(mut self, v: u64) -> Self {
            self.request_floor = v;
            self
        }
        /// Make `read_used` fail with `e` — model a dormant target (`NoTargetPods`)
        /// or a metric outage (`MetricsMissing`).
        #[must_use]
        pub fn with_read_used_error(mut self, e: ProviderError) -> Self {
            self.read_used_error = Some(e);
            self
        }
        /// Make `read_limit` fail with `e` — model the owner GET 404ing
        /// (`TargetNotFound`), so `observe()`'s zero-match-prefix-scan
        /// disambiguation (task #217) is testable end-to-end against the mock.
        #[must_use]
        pub fn with_read_limit_error(mut self, e: ProviderError) -> Self {
            self.read_limit_error = Some(e);
            self
        }
        /// Model a discovered StorageClass capability — set `Some(cap)` where
        /// `cap.is_supported()` is false to drive `TickPlan::CapabilityMissing`
        /// end-to-end against the mock (the local-path fail-fast fix).
        #[must_use]
        pub fn with_storage_capability(mut self, cap: Option<StorageCapability>) -> Self {
            self.storage_capability = cap;
            self
        }
        #[must_use]
        pub fn applied(&self) -> Vec<SsaPatch> {
            self.applied.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Cluster for MockCluster {
        async fn read_used(&self, source: &MetricSource) -> Result<Sample, ProviderError> {
            // A read of the registered THROTTLE source returns the throttle scalar
            // (the suppressed-demand signal), never the primary `used` — so a CPU
            // descriptor's separate throttle read is driveable end-to-end. The error
            // injection applies only to the PRIMARY used read (a throttle outage is
            // fail-safe to 0 in the provider, exercised separately).
            if let Some(ts) = &self.throttle_source
                && source == ts
            {
                return Ok(Sample { value: self.throttle_signal, age_secs: 0 });
            }
            match &self.read_used_error {
                Some(e) => Err(e.clone()),
                None => Ok(self.used),
            }
        }
        async fn read_limit(
            &self,
            _t: &Target,
            _layout: &LimitLayout,
            _resource: &str,
        ) -> Result<u64, ProviderError> {
            match &self.read_limit_error {
                Some(e) => Err(e.clone()),
                None => Ok(self.limit),
            }
        }
        async fn field_owners(
            &self,
            _t: &Target,
            _layout: &LimitLayout,
            _resource: &str,
            _logical: &str,
        ) -> Result<Vec<FieldOwner>, ProviderError> {
            Ok(self.owners.clone())
        }
        async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError> {
            self.applied.lock().unwrap().push(patch.clone());
            Ok(AppliedReceipt { source_hash: [0u8; 16] })
        }
        async fn read_resize_restart_free(
            &self,
            _t: &Target,
            _layout: &LimitLayout,
            _resource: &str,
        ) -> Result<bool, ProviderError> {
            Ok(self.resize_restart_free)
        }
        async fn read_request_floor(
            &self,
            _t: &Target,
            _layout: &LimitLayout,
            _resource: &str,
        ) -> Result<u64, ProviderError> {
            Ok(self.request_floor)
        }
        async fn read_restarting(
            &self,
            _t: &Target,
            _layout: &LimitLayout,
            _resource: &str,
        ) -> Result<bool, ProviderError> {
            Ok(self.restarting)
        }
        async fn read_storage_capability(
            &self,
            _t: &Target,
            _layout: &LimitLayout,
        ) -> Result<Option<StorageCapability>, ProviderError> {
            Ok(self.storage_capability.clone())
        }
    }
}

#[cfg(test)]
mod forma_seed {
    //! The **K2 keystone proof**: the band law is SHAPE-BLIND. `decide` converges
    //! on a node COUNT exactly as on bytes, into the deadband `[shrink_below,
    //! grow_above]` (BREATHABILITY-MATH §3.3 — the attractor is the band INTERVAL,
    //! NOT the point `setpoint`). This is the entire basis for `Forma` reusing
    //! `breathe-control` verbatim (docs/PROVISIONING.md §1.1).
    use super::{Forma, FormaDescriptor, NodeOnDemandDescriptor};
    use breathe_control::{decide, BandConfig, Decision};

    /// A node-count band config. `*_bytes` is the unit-blind field name (the band
    /// law never knows it is bytes); here the unit is *nodes*.
    fn node_count_cfg(floor: u64, ceiling: u64) -> BandConfig {
        BandConfig {
            grow_above: 0.85,
            shrink_below: 0.70,
            setpoint: 0.80,
            grow_factor: 1.25,
            shrink_factor: 0.90,
            floor_bytes: floor,
            ceiling_bytes: ceiling,
            request_floor_bytes: 0,
            warmup_seconds: 0,
            metric_missing_policy: breathe_control::MetricMissingPolicy::RestoreHeadroom,
        }
    }

    /// Iterate the band law to its fixed region; return `(settled_limit, last)`.
    fn converge(demand: u64, mut limit: u64, cfg: &BandConfig) -> (u64, Decision) {
        let mut last = Decision::Hold;
        for _ in 0..200 {
            last = decide(demand, limit, cfg);
            match last {
                Decision::Grow { to, .. } | Decision::Shrink { to, .. } => {
                    if to == limit {
                        break;
                    }
                    limit = to;
                }
                _ => break, // Hold / AtCeiling / NoSafeShrink / NoLimit — settled
            }
        }
        (limit, last)
    }

    #[test]
    fn seed_descriptor_is_node_on_demand() {
        let d = NodeOnDemandDescriptor;
        assert_eq!(d.forma(), Forma::NodeOnDemand);
        assert_eq!(Forma::NodeOnDemand.as_str(), "node-on-demand");
        assert_eq!(Forma::NodeOnDemand.to_string(), "node-on-demand");
        assert_eq!(d.unit(), "node");
        assert!(d.relief_latency_secs() > 0, "relief latency must be > 0 (P8 dead-time)");
    }

    #[test]
    fn band_law_converges_on_node_count_into_the_deadband() {
        let cfg = node_count_cfg(1, 100);
        for &demand in &[1u64, 2, 3, 5, 8, 13, 21, 40, 75] {
            for &l0 in &[1u64, demand.max(1), 100] {
                let (limit, last) = converge(demand, l0.max(1), &cfg);
                let util = demand as f64 / limit as f64;
                // Per MATH §3.3: settles IN the deadband, not at the setpoint.
                let in_band = util <= cfg.grow_above + 1e-9 && util >= cfg.shrink_below - 1e-9;
                let at_wall = matches!(last, Decision::AtCeiling { .. } | Decision::NoSafeShrink { .. })
                    || limit == cfg.ceiling_bytes
                    || limit == cfg.floor_bytes;
                assert!(
                    in_band || at_wall,
                    "demand={demand} l0={l0} → limit={limit} util={util:.3} last={last:?} \
                     (want in-band [{},{}] or a wall)",
                    cfg.shrink_below, cfg.grow_above
                );
                // never-over-commit: the settled limit covers the demand (the
                // provisioning peer of never-OOM), unless the floor binds below it.
                assert!(
                    limit >= demand || limit == cfg.floor_bytes,
                    "over-commit breach: demand={demand} > limit={limit}"
                );
            }
        }
    }

    #[test]
    fn node_count_and_bytes_settle_at_the_same_utilization() {
        // Shape-blindness made literal: the same starting ratio on a node COUNT
        // and on BYTES settles at the same in-band utilization (the law sees only
        // the ratio). 90/100 (over grow_above) must grow both identically.
        let cfg = node_count_cfg(1, 1_000_000);
        let (limit_nodes, _) = converge(90, 100, &cfg);
        let (limit_bytes, _) = converge(90_000, 100_000, &cfg);
        let util_nodes = 90.0 / limit_nodes as f64;
        let util_bytes = 90_000.0 / limit_bytes as f64;
        assert!(
            (util_nodes - util_bytes).abs() < 0.05,
            "shape-blindness broken: node util {util_nodes:.3} != byte util {util_bytes:.3}"
        );
        // and both landed inside the deadband
        assert!(util_nodes <= cfg.grow_above + 1e-9 && util_nodes >= cfg.shrink_below - 1e-9);
    }
}
