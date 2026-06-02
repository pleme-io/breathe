# breathe — the resource-homeostasis substrate

> **Frame.** This document is the architecture for `breathe`: the typed,
> attested substrate that holds every enrolled pleme-io workload inside a
> tight resource-utilization band, across *many* resident problem
> categories (memory, storage, CPU, …), with *one* running architecture
> and *one* proven balancing law. It is the homeostatic realization of
> [Pillar 11 (JIT Infrastructure)](THEORY.md#vii3-jit-infrastructure-pillar-11)
> and the per-compute-unit half of [`BREATHABILITY.md`](BREATHABILITY.md):
> `BREATHABILITY.md` covers *scale-to-zero between workloads*; `breathe`
> covers *right-sizing within a live workload*. It operates under
> [Constructive Substrate Engineering](CONSTRUCTIVE-SUBSTRATE-ENGINEERING.md)
> and is a [Viggy](CONTINUOUS-SOLUTION-MACHINE.md) controller: every limit
> change is a continuously-attested theorem, not an assertion.

> **Naming.** `breathe` is the verb the system performs — a workload
> *breathes* when its allocation rises and falls with demand while live
> pages never approach the eviction floor. Foundational crates keep the
> Japanese/Brazilian-Portuguese convention where they name new primitives;
> `breathe` is an operator-facing verb and stays English (cf. `tend`,
> `carve`).

---

## 0. Reading guide — what is proven vs. what is new

Two pure cores already exist, are unit-tested, and ship in production
charts. **`breathe` is mostly an extraction + a single new trait, not a
rewrite.** This is load-bearing: the entire "smallest new surface" thesis
rests on these being verbatim lifts, and they are.

| Artifact | Status | Path |
|---|---|---|
| Bidirectional band law `decide()` + `BandConfig` + `Decision` | **proven, 10 unit tests** (band edges, shrink-safety clamp, ceiling/floor circuit-breakers, grow/shrink convergence) | `helmworks/charts/pleme-memory-elastic/image/src/control.rs` |
| Single-writer guard `competing_memory_manager()` + `FieldOwner` | **proven, 3 unit tests** | same file |
| `MemoryBand` CRD (group already `breathe.pleme.io/v1`) + `band_config()` quantity parse | **authored** | `helmworks/charts/pleme-memory-elastic/image/src/crd.rs` |
| Storage grow-only watcher (PVC online-resize, cooldown anchor, typed `Event`s, ceiling circuit-breaker) | **production CronJob** | `helmworks/charts/pleme-storage-elastic/image/src/main.rs` |
| Catalog reflection template (`SubstrateDomain` / `MaturityGate` / `load_canonical` / `topological_order` / `transitive_dependencies`) | **proven, reused verbatim** | `sui/sui-spec/src/catalog.rs` + `specs/catalog.lisp` |
| Convergence controller skeleton (`TargetController`, `Severity`, `Decision`, `RemediationPolicy`, `TypedAction`, `Reconciler`, `OutcomeChain`) | **substrate** | `promessa/promessa-types/*` + `engenho-promessa-controllers/*` |
| Pangea-operator best-controller skeleton (generation filter, diff-gated status, error policy, settling/escalation, policy gate) | **production** | `pangea-operator/.../controller/*.rs` |

**One architecture-comprehension fact governs the whole design.** The
real `promessa-types::TargetController` trait is **pure `diff` /
`classify` / `decide` only — it has no `tick()`**. The seven-beat loop
(Observe → Diff → Classify → Decide → Act → Attest → Tick) is **engine
machinery that `breathe-core` *composes*, not a default method it
*inherits*.** Any text that says "tick is inherited from
`TargetController`" is wrong. `breathe-core` owns the loop; it *calls*
the controller's pure trio and the providers' I/O legs.

---

## 1. Destination — the homeostasis primitive

The absolute-best long-term shape of `breathe` is **one running
controller process, one proven dimension-agnostic balancing law, and a
catalog of pluggable resident-problem-category providers — such that
every enrolled workload is held, per category, inside a typed
utilization band (default 80% used / 20% headroom) by gentle, bounded,
convergent steps, and every step is a signed entry in a verifiable
attestation chain.** A workload "breathes" when its memory limit, CPU
limit, and storage request each rise on pressure and fall on slack
*independently and atomically*, while live pages and live storage never
cross the eviction floor by construction. The deliverable is not "the
limit got bigger" — it is **"this workload provably held its 80/20 band
for the last 30 days, here is the chain."** Adding a new category
(network-I/O headroom, ephemeral-storage, GPU memory) is strictly
additive and mechanically gated: one `(defdimension …)` Lisp form, one
typed Rust border, one `ResourceProvider` impl, one catalog row — and the
build *fails* if any of the four is missing. The controller code never
grows when a dimension is added; the catalog does. The balancing math is
solved once and can never be touched by a provider, so a new dimension
**cannot regress convergence**.

---

## 2. The central resource-balancing core

### 2.1 What is shared vs. per-dimension

| Concern | Owner | Why |
|---|---|---|
| The band law `decide(used, capacity, &BandConfig) -> Decision` | **shared** (`breathe-control`) | Dimension-agnostic by construction: `used`/`capacity` are scalars; bytes, millicores, and PVC-bytes all project into the same two numbers. Solve once. |
| The single-writer guard `competing_field_manager(owners, ours)` | **shared** | One invariant — "never two writers on one field" — generalized from `competing_memory_manager` by parameterizing the owned field. |
| `Directionality` clamp (`Shrink` on a `GrowOnly` dim ⇒ `NoSafeShrink`) | **shared** (one pure fn) | Storage = "the band with shrink disabled," achieved with *zero* storage-specific control code. |
| Cooldown gate, dry-run gate, status-receipt mapping | **shared** | Operational policy is identical across categories. |
| `observe` / `assign` / `release` I/O | **per-dimension** (`ResourceProvider` impl) | Only the *mechanism* differs: SSA-patch `limits.memory` vs. PVC resize vs. in-place CPU resize. |
| The metric, the owned field path, the base unit | **per-dimension** | Memory: working-set bytes / `limits.memory`. CPU: millicores / `limits.cpu`. Storage: used bytes / `requests.storage`. |

The crucial property: **a provider never sees `BandConfig` or
`decide()`.** It receives a fully-computed target value (`to_value`) and
translates it to a platform mutation. It cannot re-decide, cannot widen
the band, cannot subvert the safety clamp. This is the galho
`IaCSystem` / iac-forge `Backend` "non-overloadable algebra" pattern
applied to resource homeostasis.

### 2.2 The proven band law (verbatim, `breathe-control`)

Lifted unchanged from `pleme-memory-elastic/image/src/control.rs` — only
the module path changes. The 13 tests move with it.

```rust
pub struct BandConfig {
    pub grow_above: f64,    // util > this ⇒ grow   (default 0.85)
    pub shrink_below: f64,  // util < this ⇒ shrink (default 0.70)
    pub setpoint: f64,      // shrink-safety lands here (default 0.80)
    pub grow_factor: f64,   // limit *= this on grow  (default 1.25)
    pub shrink_factor: f64, // limit *= this on shrink (default 0.90)
    pub floor_bytes: u64,   // never shrink below      (default 256Mi)
    pub ceiling_bytes: u64, // never grow above        (default 16Gi)
}

pub enum Decision {
    Hold,
    Grow { from: u64, to: u64 },
    Shrink { from: u64, to: u64 },
    AtCeiling { current: u64 },
    NoSafeShrink { current: u64 },
    NoLimit,
}

/// Pure: (working_set, current_limit, cfg) → Decision.
/// Shrink is clamped to max(gentle_step, working_set/setpoint, floor),
/// so a shrink can never push live pages over the grow edge — no OOM,
/// no shrink→grow flapping. Proven by control.rs::tests.
pub fn decide(working_set: u64, current_limit: u64, cfg: &BandConfig) -> Decision { /* … */ }

pub struct FieldOwner { pub manager: String, pub owns_field: bool }

/// The single-writer invariant, generalized: returns a competing manager
/// that owns the dimension's field, so the caller yields instead of fighting.
pub fn competing_field_manager(owners: &[FieldOwner], ours: &str) -> Option<String> { /* … */ }
```

The **one new pure function** the core gains (trivially unit-testable):

```rust
pub enum Directionality { Bidirectional, GrowOnly, ObserveOnly }

/// GrowOnly categories (storage) never shrink; ObserveOnly categories
/// (KEDA-owned replicas) never mutate. Enforced centrally, not per-provider.
pub fn clamp_to_directionality(d: Decision, dir: Directionality) -> Decision {
    match (dir, &d) {
        (Directionality::GrowOnly, Decision::Shrink { from, .. }) =>
            Decision::NoSafeShrink { current: *from },
        (Directionality::ObserveOnly, Decision::Grow { from, .. })
      | (Directionality::ObserveOnly, Decision::Shrink { from, .. }) =>
            Decision::Hold, // observe-only: never write the field
        _ => d,
    }
}
```

### 2.3 The reconcile loop (`breathe-core` — the seven-beat tick, composed not inherited)

```rust
/// One BreatheController per dimension binds the band law to the
/// promessa-types::TargetController PURE trio (diff/classify/decide).
/// breathe-core owns the LOOP; it calls this trio + the provider I/O legs.
impl TargetController for BreatheController {
    type Spec     = BandSpec;        // the CRD spec (band cfg + targetRef)
    type Snapshot = Observation;     // what the provider's observe() projects
    type Drift    = Decision;        // the band law's Decision IS the drift
    const KIND: PromessaTargetKind = PromessaTargetKind::Custom; // "breathe"; → canonical 6th kind at M3

    fn diff(&self, spec: &BandSpec, snap: &Observation) -> Decision {
        // single-writer guard FIRST — yield, never fight
        if competing_field_manager(&snap.owners, &spec.field_manager()).is_some() {
            return Decision::NoLimit; // surfaced as phase=Conflict by classify+status
        }
        let cfg = spec.band_config().unwrap_or_default();
        clamp_to_directionality(decide(snap.used, snap.capacity, &cfg), self.directionality)
    }
    fn classify(&self, d: &Decision) -> Severity { /* Hold→Cosmetic, Grow|Shrink→Functional, Conflict|NoLimit→Critical */ }
    fn decide(&self, spec: &BandSpec, sev: Severity, d: &Decision) -> Decision /* promessa::Decision */ { /* route via RemediationPolicy */ }
}
trait_laws_obeyed!(BreatheController); // proptest suite: diff_deterministic, classify_monotonic,
                                       // decide_deterministic, act_idempotent_on_noop, tick_converges, attest_canonical, …
```

```text
breathe-core::reconcile_one(band: &Band, prov: &dyn ResourceProvider) -> TickReceipt
 1. OBSERVE   obs = prov.observe(&band.target).await?          // (used, capacity, owners)
 2. DIFF      // single-writer guard, then the proven band law
              if let Some(other) = competing_field_manager(&obs.owners, OUR_MGR(prov)) {
                  return TickReceipt::conflict(other);          // status.phase=Conflict, no write
              }
              let decision = clamp_to_directionality(
                  decide(obs.used, obs.capacity, &band.band_config()?),
                  prov.directionality());
 3. CLASSIFY  let sev = severity_of(&decision);                // pure, monotonic
 4. DECIDE    let verdict = route(sev, band.remediation, decision);  // Viggy P0→P3 ladder
 5. ACT       if within_cooldown(band) { return TickReceipt::cooldown(); }
              match verdict {
                  AutoCorrect(Grow{to}|Shrink{to}) if !band.dry_run => {
                      let r = prov.assign(&band.target, to).await?;   // ONLY mutation, atomic-to-category
                      attest(&r);                                     // 6. ATTEST (state-change only)
                  }
                  RequireApproval(act) => open_pr_or_alert(act),      // shadow → approval ladder
                  Alert | NoAction    => {}                           // observable, no mutation
              }
 7. TICK      status_patch_if_changed(band, &decision);        // diff-gated SSA status
              Action::requeue(cfg.refresh_interval)            // level-triggered, never await_change
```

`tick_converges` is now a **trait law** (proptest), so the convergence
guarantee that lived in `control.rs::tests` is promoted to a substrate
invariant checked for every dimension.

---

## 3. The provider / plugin architecture

### 3.1 The `ResourceProvider` trait (the spine)

Lives in its own crate **`breathe-provider`** with **no dependency on the
controller**, so each `dimension-*` crate is an external impl —
mirroring `galho-types::IaCSystem`. The algebra is non-overloadable: the
trait exposes only category-atomic I/O and never sees `decide`/`BandConfig`.

```rust
// crate: breathe-provider   (Send + Sync + 'static, object-safe via async_trait)
use async_trait::async_trait;

/// The side-effecting boundary every provider's I/O goes through. Real impl
/// is `KubeCluster`; tests pass `MockCluster`. This Environment trait is what
/// makes each provider (= the interpreter half of the typed-spec triplet) mockable.
#[async_trait]
pub trait Cluster: Send + Sync {
    async fn metric(&self, t: &Target, kind: MetricKind) -> Result<u64, ProviderError>;
    async fn current_allocation(&self, t: &Target, dim: DimensionId) -> Result<u64, ProviderError>;
    async fn field_owners(&self, t: &Target, field: &str) -> Result<Vec<FieldOwner>, ProviderError>;
    async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError>; // SSA, field-mgr scoped
}

#[async_trait]
pub trait ResourceProvider: Send + Sync + 'static {
    /// Stable category atom — equals the catalog `:name` and keys the registry.
    fn id(&self) -> DimensionId;                       // "memory" | "storage" | "cpu" | "replica"

    /// What this category may do. The LOOP enforces it via clamp_to_directionality;
    /// providers carry no band logic. Storage=GrowOnly, mem/cpu=Bidirectional, replica=ObserveOnly.
    fn directionality(&self) -> Directionality;

    /// SSA field manager + the dotted field path this provider owns. The
    /// single-writer guard checks ownership of THIS field. Disjoint paths across
    /// dimensions ⇒ memory/cpu/storage bands never fight each other.
    fn owned_field(&self) -> OwnedField;               // { manager: "breathe/memory", path: ["resources","limits","memory"] }

    /// Apply-semantics this category exposes (GALHO ApplySemantics): lets the
    /// loop interpret/dispatch correctly + surface real disruption in the attestation.
    fn semantics(&self) -> ApplySemantics;             // Transactional | ContinuousReconciliation | PartialProgress

    /// OBSERVE — read-only. Project the target into the band law's two scalars
    /// (used, capacity) in this category's base unit + the field owners.
    /// Atomic = a single read of one target. NEVER mutates.
    async fn observe(&self, t: &Target) -> Result<Observation, ProviderError>;

    /// ASSIGN — the ONE mutation. Carve/return `to_value` (base units) for `t`,
    /// ATOMICALLY for this resident problem category. Idempotent:
    /// assign(to == current) == AlreadyConverged.
    async fn assign(&self, t: &Target, to_value: u64) -> Result<AssignReceipt, ProviderError>;

    /// RELEASE — de-enrollment / finalizer path. Return the category to an
    /// unmanaged baseline (drop the SSA field-manager claim). Atomic; idempotent.
    /// GrowOnly providers (storage) never shrink: release is bookkeeping only.
    async fn release(&self, t: &Target) -> Result<ReleaseReceipt, ProviderError>;
}

pub struct Observation { pub used: u64, pub capacity: u64, pub owners: Vec<FieldOwner> }
pub struct AssignReceipt  { pub from: u64, pub to: u64, pub source_hash: [u8; 16] } // BLAKE3-128, audit
pub struct ReleaseReceipt { pub baseline: Option<u64>, pub source_hash: [u8; 16] }

pub enum ProviderError {
    TargetNotFound,
    MetricsMissing,
    NoCapacityField,                 // no denominator ⇒ band law returns NoLimit
    ApiTransient(String),            // retry fast
    ApiPermanent(String),            // escalate + AnomalyChain
}
```

`Observation { used, capacity, owners }` is **the single
dimension-agnostic adapter point** — every category projects into it, so
the proven `decide(used, capacity, cfg)` runs unchanged across all
dimensions. The whole "dimension-blind core" claim is made by one struct.

### 3.2 Registration — static, feature-gated, typed dispatch

No `inventory`, no dynamic plugin loading (matches terraform-forge /
crossplane-forge: each backend is a feature-gated impl). The registry is
keyed by a **typed `DimensionId` enum**, not a free `String`, so an
unknown dimension fails to *compile*, not at startup:

```rust
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum DimensionId { Memory, Storage, Cpu, Replica }   // adding a variant is a substrate edit

pub struct Registry(HashMap<DimensionId, Box<dyn ResourceProvider>>);

impl Registry {
    pub fn build(cfg: &BreatheConfig) -> Self {
        let mut r = HashMap::new();
        #[cfg(feature = "dimension-memory")]  r.insert(DimensionId::Memory,  Box::new(MemoryProvider::new(cfg)));
        #[cfg(feature = "dimension-storage")] r.insert(DimensionId::Storage, Box::new(StorageProvider::new(cfg)));
        #[cfg(feature = "dimension-cpu")]     r.insert(DimensionId::Cpu,     Box::new(CpuProvider::new(cfg)));
        #[cfg(feature = "dimension-replica")] r.insert(DimensionId::Replica, Box::new(ReplicaProvider::new(cfg)));
        Self(r)
    }
}
```

The core calls a provider only through the trait; it dispatches on the
`DimensionId` carried by each `Band` CR. A catalog entry whose provider
is not registered is caught by the **CATALOG REFLECTION matrix test at
build time** (§6), so a missing provider can never reach runtime.

### 3.3 The three (+one) providers

| | **MemoryProvider** | **StorageProvider** | **CpuProvider** | **ReplicaProvider** |
|---|---|---|---|---|
| `observe` metric | `container_memory_working_set_bytes` (Prometheus/kubelet); capacity = `limits.memory` | `kubelet_volume_stats_used_bytes` / `…_capacity_bytes` per PVC | `rate(container_cpu_usage_seconds_total)` → millicores; capacity = `limits.cpu` | read `status.replicas` + KEDA `ScaledObject` (read-only) |
| `assign` field | SSA-patch `resources.limits.memory` (+requests floor), mgr `breathe/memory`; owner rolls | SSA-patch `spec.resources.requests.storage` upward; CSI online-resize, no restart | SSA-patch `resources.limits.cpu` millicores; in-place on `InPlacePodVerticalScaling`-capable clusters, else roll | **none** — returns `AlreadyConverged`; never writes `/scale` |
| `release` | drop `breathe/memory` SSA claim; owner reclaims its declared limit | no-op (grow-only; never shrinks a PVC) | drop `breathe/cpu` SSA claim | no-op |
| `directionality` | `Bidirectional` | `GrowOnly` | `Bidirectional` | `ObserveOnly` |
| `semantics` | `Transactional` (owner roll, all-or-nothing) | `ContinuousReconciliation` (CSI reconciles async) | `PartialProgress` (in-place) / `Transactional` (roll fallback) | — |
| atomicity to category | the SSA patch is one new ReplicaSet generation; kubelet throttles actual allocation; `competing_field_manager` refuses the target if VPA owns `limits.memory` | one PVC request bump → one CSI `ExpandVolume`; cooldown annotation prevents N+1 while mid-resize | one millicore patch = one cgroup cpu-quota change; guard on `limits.cpu` | atomic by **abstention** — never contends with KEDA |

Memory and storage reuse the **production code paths verbatim**
(`pleme-memory-elastic` SSA patch, `pleme-storage-elastic` probe +
resize); only the trait wrapper is new.

---

## 4. The enrollment contract — the typed CRD family

**Recommendation: per-dimension CRDs sharing one spec shape**, not a
single `dimension`-keyed CRD. Rationale, in order of weight:

1. **`MemoryBand` already exists, is authored, and its CRD group is
   already `breathe.pleme.io/v1`.** Per-dimension CRDs keep the proven
   memory path *byte-identical* — no migration, no risk of two
   controllers reconciling memory during a `MemoryBand→ResourceBand`
   cutover (the single-writer-at-controller-level hazard the
   `dimension`-keyed design carries).
2. **Typed dispatch over stringly-typed selection.** `kind: MemoryBand`
   is a compile-time-distinct type; `dimension: "memory"` is a string
   parsed at runtime. The substrate prizes compile-time invalidity.
3. **Per-dimension RBAC and printcolumns.** A `StorageBand` needs `patch`
   on PVCs; a `MemoryBand` needs `patch` on Deployments/StatefulSets.
   Distinct kinds let RBAC be scoped exactly.

All three share the **`BandSpec` shape** (a Rust struct reused across the
three `CustomResource` derives), so the *spec authoring surface* is
identical while the *kinds* stay distinct:

```rust
// shared, reused by MemoryBand / StorageBand / CpuBand
#[serde(rename_all = "camelCase")]
pub struct BandSpec {
    pub target_ref: TargetRef,        // { kind, name, apiVersion?, container? } — verbatim from crd.rs
    #[serde(default = "d_setpoint")]      pub setpoint: f64,        // 0.80
    #[serde(default = "d_grow_above")]    pub grow_above: f64,      // 0.85
    #[serde(default = "d_shrink_below")]  pub shrink_below: f64,    // 0.70
    #[serde(default = "d_grow_factor")]   pub grow_factor: f64,     // 1.25
    #[serde(default = "d_shrink_factor")] pub shrink_factor: f64,   // 0.90
    #[serde(default = "d_floor")]         pub floor: String,        // per-dim unit: "256Mi" | "100m" | "10Gi"
    #[serde(default = "d_ceiling")]       pub ceiling: String,
    #[serde(default = "d_cooldown")]      pub cooldown_seconds: u64,// 600
    #[serde(default)]                     pub dry_run: bool,        // shadow mode
    #[serde(default)]                     pub remediation: RemediationTier, // Alert(default) | RequireApproval | AutoCorrect
}
```

```rust
#[derive(CustomResource, …)]
#[kube(group="breathe.pleme.io", version="v1", kind="MemoryBand", namespaced,
       status="BandStatus", shortname="mband", category="breathe", /* printcolumns */)]
pub struct MemoryBandSpec(BandSpec);   // ← MemoryBand stays exactly as today

#[derive(CustomResource, …)]
#[kube(group="breathe.pleme.io", version="v1", kind="StorageBand", namespaced,
       status="BandStatus", shortname="sband", category="breathe")]
pub struct StorageBandSpec(BandSpec);

#[derive(CustomResource, …)]
#[kube(group="breathe.pleme.io", version="v1", kind="CpuBand", namespaced,
       status="BandStatus", shortname="cband", category="breathe")]
pub struct CpuBandSpec(BandSpec);
```

`band_config()` (the quantity parser already in `crd.rs`) is lifted to
`BandSpec` and reused per-dimension (bytes for memory/storage, millicores
for cpu).

**Status = the per-tick receipt** (the existing `MemoryBandStatus`,
renamed `BandStatus`, plus chain anchors):

```rust
#[serde(rename_all = "camelCase")]
pub struct BandStatus {
    pub phase: Option<String>,            // Holding|Growing|Shrinking|AtCeiling|Cooldown|Conflict|NoLimit|TargetNotFound|MetricsMissing
    pub last_util: Option<String>,        // "0.81"
    pub current_limit: Option<String>,    // "2Gi" / "1500m"
    pub last_decision: Option<String>,    // "Shrink 2Gi->1843Mi"
    pub last_change_epoch: Option<i64>,   // cooldown anchor
    pub conflict_manager: Option<String>, // the field-manager we yielded to
    pub outcome_chain_head: Option<String>, // BLAKE3 head of THIS band's attestation chain
    pub last_attested_epoch: Option<i64>,
    pub observed_generation: Option<i64>, // generation-filter anchor
}
```

**Optional aggregate (M3): `BreathePromise`.** A Viggy `(defpromessa …)`
CR asserting "target X holds 80/20 across *all* its enrolled dimensions
for window W," reconciled by composing the per-dimension `BandStatus`es
(`PromessaLattice` meet: in-band ⟺ every dimension in-band). This is the
operator- and regulator-facing object `kensa verify` targets. Pure
composition — the controller still reconciles individual bands.

**Audit story.** `kubectl get mband,sband,cband -A` is the complete,
auditable answer to "what is breathing, in which category, at what
utilization." Nothing implicit is ever managed — enrollment is one typed
CR. `status.outcomeChainHead` links each band to its signed history.

---

## 5. shikumi typed config

Two layers, both `shikumi::TieredConfig`; neither reimplements
`ConfigStore`, hot-reload, generation tracking, or env-prefix discovery.

**Controller-operational config** (`breathe-core::BreatheConfig`) — loop
cadence, budgets, attestation wiring. **Per-target band knobs live in the
CRD, never here** (one source of truth):

```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct BreatheConfig {
    pub refresh_interval_secs: u64,        // level-triggered requeue floor; prescribed_default 300
    pub reconcile_workers: usize,          // bounded concurrency; default 4 (shigoto global budget)
    pub max_concurrent_reconciles: u32,    // BudgetTree.global — blast-radius cap; default 16
    pub enabled_dimensions: Vec<DimensionId>, // [Memory] at M0; widens per phase (runtime mirror of cfg features)
    pub remediation_floor: RemediationTier, // global Viggy floor: Alert during shadow → AutoCorrect at GA
    pub metrics_query_url: String,         // Prometheus/VictoriaMetrics endpoint for observe()
    pub outcome_chain_bucket: String,      // S3/R2 object store for the attestation chain
    pub timeout_secs: u64,                 // per-tick wall-clock bound → SIGTERM→SIGKILL (never-hang)
    pub max_changes_per_minute: u32,       // reconcile-rate containment (anti watch-loop)
    pub escalate_after_secs: u64,          // settling tracker → PauseAndAlert
    pub audit_path: PathBuf,               // AuditFileEmitter JSONL
}

impl shikumi::TieredConfig for BreatheConfig {
    fn bare() -> Self { /* every field enumerated, no ..Default; no dims, Alert-only, 300s, 4 workers */ }
    fn discovered() -> Self { /* bare + probe cluster: KEDA present? VPA present? auto-detect metrics URL + CSI allowVolumeExpansion */ }
    fn prescribed_default() -> Self { /* fleet opinion: 80/20, Alert floor, 300s, 16 budget; serde round-trip pinned */ }
    /* extend(base): operator YAML overlay */
}
```

Loaded once at startup via
`BreatheConfig::resolve_from_env("BREATHE_TIER")` into a
`ConfigStore<BreatheConfig>` (`load_and_watch` ⇒ lock-free `.get()`,
ArcSwap, generation counter, `last_reload_error`). `generation()` and
`last_reload_error()` surface in band status for legibility — `breathe`
eats its own dogfood (the `hashfix::HashfixConfig` pattern).

**Per-provider config** is each provider crate's own `TieredConfig`
(e.g. `dimension-memory::ProviderConfig` — metric source URL, SSA field
manager suffix), composed via its own `ConfigStore`. No provider
reimplements the store.

---

## 6. tatara-lisp authoring + self-describing catalog

`breathe` is a **catalog-bearing typed substrate** (≥3 typed domains ⇒
CATALOG REFLECTION is mandatory, no waiver). Two authoring layers, both
the TYPED-SPEC + INTERPRETER TRIPLET.

### 6.1 `(defdimension …)` — the self-describing dimension catalog

`breathe/specs/dimensions.lisp` + `breathe/src/dimensions/catalog.rs`,
modeled **1:1 on `sui-spec/catalog.rs` + `specs/catalog.lisp`**.
`DimensionSpec` = `SubstrateDomain` + three typed fields
(`directionality`, `metric`, `owned_field`); `MaturityGate` reused
unchanged.

```lisp
;; breathe/specs/dimensions.lisp — one (defdimension …) per authored dimension module.
;; A new dimension lands its entry here in the SAME commit. :depends-on declares the DAG.

(defdimension
  :name               "memory"
  :authoring-keywords ("defmemory-band")
  :gate               Working
  :directionality     Bidirectional
  :metric             "container_memory_working_set_bytes"
  :owned-field        "resources.limits.memory"
  :purpose            "bidirectional working-set band; safe-min clamp prevents OOM"
  :vendor-mirror      "k8s VPA (limit-only, single-writer) / kubelet working-set"
  :depends-on         ("replica"))            ; replica-ceiling context informs grow-vs-scale

(defdimension
  :name               "storage"
  :authoring-keywords ("defstorage-band")
  :gate               Working
  :directionality     GrowOnly
  :metric             "kubelet_volume_stats_used_bytes"
  :owned-field        "spec.resources.requests.storage"
  :purpose            "grow-only online PVC resize"
  :vendor-mirror      "CSI VolumeExpansion / pleme-storage-elastic"
  :depends-on         ())

(defdimension
  :name               "cpu"
  :authoring-keywords ("defcpu-band")
  :gate               M2Typed
  :directionality     Bidirectional
  :metric             "container_cpu_usage_seconds_total"
  :owned-field        "resources.limits.cpu"
  :purpose            "in-place millicore band"
  :vendor-mirror      "InPlacePodVerticalScaling"
  :depends-on         ("replica"))

(defdimension
  :name               "replica"
  :authoring-keywords ("defreplica-observer")
  :gate               Informational
  :directionality     ObserveOnly
  :metric             "kube_deployment_status_replicas"
  :owned-field        "spec.replicas"
  :purpose            "observe-only; composes with KEDA, never writes /scale"
  :vendor-mirror      "KEDA ScaledObject"
  :depends-on         ())
```

```rust
#[derive(DeriveTataraDomain, Serialize, Deserialize, Debug, Clone)]
#[tatara(keyword = "defdimension")]   // kebab↔snake serde inherited (:owned-field → owned_field)
pub struct DimensionSpec {
    pub name: String,
    pub authoring_keywords: Vec<String>,
    pub gate: MaturityGate,            // Working | M2Typed | M3Typed | M4Typed | Informational — reused from sui-spec
    pub directionality: Directionality,
    pub metric: String,
    pub owned_field: String,
    pub purpose: String,
    pub vendor_mirror: String,
    pub depends_on: Vec<String>,
}
```

`catalog.rs` reuses **verbatim**: `CANONICAL_DIMENSIONS_LISP =
include_str!("../specs/dimensions.lisp")`, `load_canonical()`,
`lookup()`, `maturity_histogram()`, `topological_order()` (Kahn over
`depends_on`), `transitive_dependencies()`. The catalog **is** the
controller's static dimension config: `BreatheConfig.enabled_dimensions`
is validated against it at startup; an unknown dimension fails loud.

### 6.2 Per-target band authoring (the instance triplet)

```lisp
(defmemory-band  :target (deployment "lilitu-api")   :setpoint 0.80 :ceiling "8Gi")
(defstorage-band :target (pvc "postgres-data")       :ceiling "1Ti")
(defcpu-band     :target (deployment "hanabi")       :floor "100m" :ceiling "2000m")

;; composition macro — "make this workload breathe" = one form, fanned out (Pillar 12)
(defbreathe-all :target (deployment "hanabi") :dimensions (memory cpu))
```

The triplet mapping:

| Triplet artifact | For the dimension catalog | For a per-target band |
|---|---|---|
| **Typed Rust border** | `DimensionSpec` `#[derive(DeriveTataraDomain)]` | `BandSpec` (the CRD spec) |
| **Authored Lisp spec** | `specs/dimensions.lisp` | `(defmemory-band …)` forms |
| **Working interpreter** (mockable via Environment trait) | `catalog.rs` load/topo-sort | render-to-CR emitter; the `Cluster`-trait-backed reconcile loop is the *runtime* interpreter of the band law |

### 6.3 Mandatory CATALOG REFLECTION tests (fail the build)

- every `(defdimension)` `:name` has exactly one **registered
  `ResourceProvider`** AND one loadable module (matrix row);
- every authoring-keyword is globally unique;
- every `:depends-on` target exists; the DAG is acyclic
  (`topological_order` solvable);
- `maturity_histogram` sums to the catalog size (partition complete);
- one **verification-matrix** test exercises every dimension's
  observe→decide→assign against `MockCluster` and *fails when a new
  `(defdimension)` lands without a row*.

This is generation-over-composition made literal: the controller code is
fixed; the catalog grows. An on-cluster read-only `DimensionCatalog` CR
(M3) projects `specs/dimensions.lisp` so `kubectl get dimensioncatalog
-o yaml` shows the loaded DAG + maturity gates — reflection on the
cluster, not just in the binary.

---

## 7. blackmatter / helmworks deployment

`breathe` is a cluster controller, deployed **helmworks-first**; the Nix
module trio ships from the source repo (per the Fleet-Controller rule)
and closes one of the CONFIGURATION-MANAGEMENT trio gaps.

### 7.1 Two operational modes, justified by directionality

| Mode | Dimensions | Template | Why |
|---|---|---|---|
| **long-lived Deployment** (`replicas: 1` + leader election, generation-filtered watch + requeue floor) | memory, cpu | `deployment.yaml` | bidirectional homeostasis needs continuous cooldown state + fast convergence + live status receipts |
| **single-shot CronJob** (`concurrencyPolicy: Forbid`, `*/1 * * * *`) | storage | `cronjob.yaml` (verbatim from `pleme-storage-elastic`) | grow-only, no shrink-flap risk, cheap to rescan, idempotent |

One chart family selects the template off the dimension's directionality
(BREATHABILITY §VII invariants 1 + 3).

### 7.2 The chart — `helmworks/charts/pleme-breathe`

- depends on `pleme-lib ~0.15+` (`clusterRBAC`, `health-probe`,
  `security-context`, `podDisruptionBudget` primitives — no hand-rolled
  RBAC);
- `deployment.yaml` + `cronjob.yaml` + the three CRDs
  (`MemoryBand`/`StorageBand`/`CpuBand`) + optional `BreathePromise` +
  `rbac.yaml` + `values.yaml`;
- **RBAC**: `ClusterRole` `get/list/watch` on
  Deployments/StatefulSets/PVCs/CNPG Clusters; **`patch` (SSA) only on
  the dimension-owned fields**; `get` on KEDA `ScaledObject` (read-only,
  replica composition); ownership of `*bands` + `breatheoperatorpolicies`
  + `dimensioncatalogs`; conditionally rendered via
  `{{- if .Values.rbac.create }}`;
- **resources** `requests: {cpu: 50m, memory: 64Mi}` and **`limits` SET**
  — controllers cannot free-run (the lava-operator 2026-06-02 lesson);
  `breathe` self-enrolls a `MemoryBand` on its own Deployment (dogfood);
- `imagePullSecrets: [ghcr-pull]` (pre-created by Flux/external-secrets).

### 7.3 GitOps-native delivery + minimal consumer footprint

- HelmRelease at
  `k8s/clusters/rio/infrastructure/breathe/{release.yaml,kustomization.yaml}`,
  `apiVersion: helm.toolkit.fluxcd.io/v2`, **`suspend: true`** until CRDs
  + cofre signer key + metrics endpoint exist, `interval: 10m`;
  `dependsOn`: observability (metrics) + storage-elastic (it folds in);
  wired into `infrastructure/kustomization.yaml` in dependency order
  (cert-manager → observability → storage-elastic → **breathe**);
- **enrolling a workload = commit a `MemoryBand` manifest**; suspending a
  dimension = edit `BreatheOperatorPolicy` + commit. Never `kubectl edit`
  (GitOps-native rule);
- **AUTO-RELEASE**: chart + image ship via
  `substrate/.github/workflows/auto-release.yml` (3-line shim) on merge
  to `main` — image to `ghcr.io/pleme-io`, chart to the OCI helm
  registry;
- **Nix module trio**: `module/{home-manager,nixos,darwin}/default.nix`
  surfacing `programs.breathe.settings` (the `BreatheConfig` schema →
  `~/.config/breathe/breathe.yaml` via the frost `yamlGenerator`
  pattern), imported into `pleme-io/nix/modules/` via flake input;
- **Bootstrap cluster: rio** (homelab; `pangea-database` + storage-elastic
  already run there).

---

## 8. Best controller behaviors embedded

Each is inherited from the pangea-operator production skeleton and tied to
where it lives.

| # | Behavior | How / where enforced |
|---|---|---|
| 1 | **Single-writer SSA** | `competing_field_manager(&obs.owners, OUR_MGR)` runs **first** in `diff` (DIFF beat). If another manager owns the dimension's `owned_field()`, yield → `status.phase=Conflict`, `conflictManager` set, **no write**. All mutations via `PatchParams::apply("breathe/<dim>")` + `Patch::Apply` ⇒ disjoint field managers. |
| 2 | **Level-triggered** | Every reconcile path returns `Action::requeue(refresh_interval)`; **never `await_change()`**. Two work sources only: spec mutations + scheduled refresh. Robust to event loss. |
| 3 | **Diff-gated status** | `status_patch_if_changed` (the `status_patch.rs` chokepoint): semantic-compare observable fields, skip PATCH if unchanged. Plus generation predicate filter on the watch stream ⇒ status writes never refire the reconciler. |
| 4 | **Never-hang / deterministic-flow** | `BreatheConfig.timeout_secs` wall-clock bound on every provider I/O → SIGTERM(30s)→SIGKILL. Every error is a typed `ProviderError`/typed `Decision` (`NoLimit`/`NoSafeShrink`/`AtCeiling`/`MetricsMissing`), **never a silent hang**. Settling tracker (consecutive-no-change + decision fingerprint) fires `PauseAndAlert`. |
| 5 | **Never-die** | Unified error policy: `ApiTransient` → fast requeue; `ApiPermanent`/`NoCapacityField` → long requeue + AnomalyChain emission. Deterministic phase FSM in status; no silent crash. |
| 6 | **Reconcile-rate containment** | shigoto `BudgetTree.global = max_concurrent_reconciles`; `reconcile_workers` bounds parallelism; per-target `cooldown_seconds` damps carve frequency; `max_changes_per_minute` + a `reconcile-rate > 1/s` alert catch watch-loop regression. Per-target floor/ceiling soft-tenancy ⇒ one loud workload can't starve siblings. |
| 7 | **Viggy seven-beat tick** | `breathe-core::reconcile_one` **composes** Observe(provider)→Diff(guard+band law)→Classify→Decide(RemediationPolicy)→Act(provider.assign)→Attest(OutcomeChain)→Tick(requeue). Author writes only the pure trio + the three I/O legs. **NOT inherited** from `TargetController` (which has no `tick()`). |
| 8 | **OutcomeChain attestation** | Every `assign`/`release` appends a BLAKE3+Ed25519 entry over `{target, dimension, used, capacity, from, to, verdict, epoch}`; every Conflict/NoLimit/NoSafeShrink → AnomalyChain. `status.outcomeChainHead` surfaces the head. `kensa verify outcome-chain --target X --dimension memory` proves the band held over a window. **Attest state-changes + a periodic in-band heartbeat, not every Hold** — keeps signing load bounded (a flagged tradeoff, resolved toward bounded). |
| 9 | **Graceful degradation** | `MetricsMissing`/`NoLimit`/`TargetNotFound` are typed Hold-equivalents that skip+surface, never crash. `dry_run`/`Alert` is the **default first-enroll tier** (shadow: observe+attest, no carve). Two-layer policy gate (`BreatheOperatorPolicy` global + per-dimension suspend) freezes reconciliation without losing CR state; finalizer plumbing + owner-refs handle clean teardown. |

---

## 9. Single-writer + GitOps-cede + isolation model

Five isolation layers, each with a concrete mechanism:

| Layer | Invariant | Mechanism |
|---|---|---|
| **Authority** (who may mutate?) | Only `breathe`, only the fields it owns, only when no competing manager exists. | SSA field-manager `breathe/<dim>` + `competing_field_manager` guard; on conflict, **cede** to the incumbent (`phase=Conflict`). |
| **Target** (whose state?) | Each `Band` reconciled independently; no shared mutable state across targets. | Per-CR reconcile unit (a shigoto `RecordingJob`); per-target band config; soft-tenancy floor/ceiling. |
| **Self** (controller resources) | The controller cannot starve the cluster or itself. | `replicas:1` + leader election; bounded `reconcile_workers`; pod `requests`+`limits` SET; `breathe` self-enrolls a `MemoryBand` (eats its own discipline). |
| **Failure** (blast radius) | A failing dimension blocks only its own band, never the controller or sibling bands. | Per-dimension provider isolation; typed `ProviderError`; transient→retry, permanent→escalate+AnomalyChain; settling→PauseAndAlert. |
| **Mutual-exclusion** (cross-controller) | `breathe` ⟂ VPA ⟂ KEDA ⟂ Flux/Helm on field ownership. | **Disjoint SSA field partition** — `breathe` owns `limits.*`/`requests.storage`; KEDA owns `replicas`; Flux/Helm own the base manifest. **GitOps-cede rule**: `breathe`-managed fields MUST be absent from the base chart values (a renderer check, §10.1), else `breathe` and Flux war on the same field every reconcile. |

**The GitOps-cede rule is load-bearing.** `competing_field_manager` only
sees `managedFields` *mid-tick*; a Helm/Flux redeploy that resets
`limits.memory` is not a "field owner" the guard can detect, producing a
perpetual breathe↔Flux diff war. The fix is structural: the chart
renderer asserts that any field `breathe` manages is **omitted from the
base manifest** (left to `breathe`'s SSA apply). `breathe XOR VPA-Auto`
on the same field is documented as mutually exclusive — running both
yields `phase=Conflict` (correct, fail-loud) and *no* breathe management,
which must be surfaced loudly.

---

## 10. Composition with KEDA's scale dimension

`breathe` owns the **per-pod resource envelope** (vertical: `limits.cpu`,
`limits.memory`, `requests.storage`); KEDA owns the **replica count**
(horizontal: `spec.replicas` via HPA/ScaledObject). They write **disjoint
SSA fields**, so they never oscillate the same value — composition by
field partition, the single best idea across the candidate designs.

```text
                 vertical band (breathe)          horizontal scale (KEDA)
 owns:           resources.limits.{cpu,memory}    spec.replicas / HPA
                 spec.resources.requests.storage  (ScaledObject)
 writer:         breathe/{memory,cpu,storage}     keda-operator
 conflict?:      NO — disjoint field managers; competing_field_manager
                 treats keda-operator as a NON-competing owner (it owns
                 replicas, not limits)
```

The `ReplicaProvider` (`Directionality::ObserveOnly`) **composes by
abstention**: it reads `status.replicas` + the `ScaledObject` read-only,
returns `AlreadyConverged` from `assign` (never writes `/scale`), and —
via the catalog `depends_on` edge `memory ← replica`, `cpu ← replica` —
feeds replica-ceiling context into the band inputs of the memory/cpu
providers ("at replica ceiling ⇒ prefer grow over scale-out"). A
workload that needs both gets vertical `breathe` + horizontal KEDA on
orthogonal axes, neither fighting the other.

### 10.1 Renderer invariants (breathability §VII, extended for breathe)

The chart renderer refuses to emit a workload that violates:

1. a `breathe`-managed field is **absent from the base manifest values**
   (GitOps-cede; §9);
2. a target enrolled in a `breathe` band **and** KEDA shows **disjoint
   field managers** (vertical ⟂ horizontal), else fail with a hint;
3. an `elastic-{memory,cpu}` tier declares a band policy + cooldown; an
   `elastic-storage` tier declares an expansion policy + ceiling;
4. cooldowns are tameshi-attested (idle = no carve after
   `cooldown + slack`).

---

## 11. Phased path M0 → M3

**Operating Principle #0 — destination first.** The destination is §1:
one process, one proven law, a self-describing catalog of category
providers, every step attested. The phases are the shortest path to it.

### M0 — Extract the spine; self-describe from birth; zero behavior change
*(The proven core is already green and lives on rio's `pangea-database`,
which is the live OOM-auto-heal need — task #22/#24.)*
- Lift `decide` + `BandConfig` + `Decision` + `competing_memory_manager`
  + `FieldOwner` into **`breathe-control`** verbatim; the 13 tests move
  unchanged. Add the one new pure fn `clamp_to_directionality` + tests.
  Add `used`/`capacity` cpu(millicores)/storage(bytes) cases proving
  dimension-agnosticism.
- Define **`breathe-provider`** (`ResourceProvider` + `Cluster` +
  `Observation`/`AssignReceipt`/`Directionality`/`ApplySemantics`).
- Author **`breathe/specs/dimensions.lisp`** (all four `(defdimension)`
  forms) + **`catalog.rs`** (clone `sui-spec`, swap type) **with the
  CATALOG REFLECTION matrix + four invariant tests**. The build fails if
  a dimension lacks a provider/border/row.
- `MemoryProvider` wraps the existing SSA patch behind the trait;
  `MockCluster` drives `decide` with no real cluster.
- **Deliverable:** the catalog *is* the substrate; adding a dimension is
  mechanically gated. `pleme-memory-elastic` is the first
  `ResourceProvider` consumer with **zero band-law change**.
- **Trigger to M1:** the matrix test is green and `MemoryProvider`
  observe→decide→assign passes against `MockCluster`.

### M1 — Memory live on rio under the composed Viggy loop (shadow → live)
- Build **`breathe-core`**: `BreatheController: TargetController` binding
  the band law to `diff/classify/decide`; `trait_laws_obeyed!`; the
  seven-beat `reconcile_one` loop (generation filter, diff-gated SSA
  status, BudgetTree, timeout→SIGTERM/SIGKILL, policy gate, settling).
- `KubeCluster` impl (SSA patch, `field_owners` from `managedFields`,
  Prometheus scrape). `BreatheConfig: TieredConfig` + `ConfigStore`
  hot-reload + Nix module trio. `pleme-breathe` Deployment chart,
  `suspend: true → false` on rio. **Default `remediation = Alert`
  (shadow: observe+attest, no carve).**
- Wire OutcomeChain (BLAKE3+Ed25519, state-change + heartbeat) +
  AnomalyChain on Conflict. Self-enroll the controller's own Deployment.
- **Deliverable:** memory breathing on `pangea-database`, every decision
  attested, no live mutation until the shadow window proves the chains.
- **Trigger to M2:** shadow window shows the memory chain converges +
  holds the band with no spurious carves; promote `pangea-database` to
  `remediation = AutoCorrect`.

### M2 — Storage + CPU as pure 3-method exercises
- Port `pleme-storage-elastic` to **`StorageProvider`**
  (`Directionality::GrowOnly` — shrink coerced to `NoSafeShrink` by the
  core, not the provider); keep its CronJob deploy mode.
- Add **`CpuProvider`** (`Bidirectional`, millicores, in-place resize via
  `InPlacePodVerticalScaling` with a roll fallback declared via
  `semantics()`) — **proves band-law dimension-agnosticism: new provider
  + new convention, zero new control logic.** Flip `cpu`'s catalog gate
  `M2Typed → Working`.
- `StorageBand`/`CpuBand` CRDs (sharing `BandSpec`). KEDA ⟂ field-manager
  renderer invariant (§10.1).
- **Deliverable:** three dimensions, live homeostasis on rio; "add a
  dimension" provably never touches `decide`.
- **Trigger to M3:** all three dimensions hold their bands on rio for a
  multi-week window; the `Custom` `PromessaTargetKind` has been used by
  memory, storage, and cpu (the three-uses rule fires).

### M3 — Provable outcomes, aggregate promise, fleet rollout
- Ship **`BreathePromise`** aggregate CR (`(defpromessa …)` asserting
  "target holds 80/20 across all dimensions for window W" via
  `PromessaLattice` meet). `kensa verify outcome-chain` becomes the
  audit/SLA/regulator surface.
- Promote `breathe` from `PromessaTargetKind::Custom` to a **canonical
  6th kind** (substrate ticket; the three-uses rule has fired).
- `ReplicaProvider` (ObserveOnly) + the `depends_on` cross-dimension
  context edge. On-cluster read-only `DimensionCatalog` CR. `breathe-
  inventory` CLI iterating the catalog. Roll to additional clusters via
  GitOps.
- **Deliverable:** continuously-attested resource homeostasis as a
  first-class Viggy promise, auditable by an external party with only the
  public verification key.

---

## 12. The dimensions catalog

| dimension | observe (metric → used/capacity) | assign (field, mechanism) | directionality | default ratio (band) | atomicity to category |
|---|---|---|---|---|---|
| **memory** | `container_memory_working_set_bytes` / `limits.memory` (bytes) | SSA-patch `resources.limits.memory`, mgr `breathe/memory`; owner rolls | `Bidirectional` | 80% setpoint, grow >0.85, shrink <0.70, floor 256Mi, ceiling 16Gi | Transactional — one ReplicaSet generation; safe-min clamp ⇒ no OOM; `competing_field_manager` refuses if VPA owns the field |
| **storage** | `kubelet_volume_stats_used_bytes` / `…_capacity_bytes` (bytes) | SSA-patch `spec.resources.requests.storage` upward; CSI online-resize | `GrowOnly` | 80% trigger, expand ×1.25 (→64% post), ceiling circuit-breaker | ContinuousReconciliation — one PVC bump → one CSI `ExpandVolume`; cooldown annotation; **irreversible** (directionality forbids shrink at the type level) |
| **cpu** | `rate(container_cpu_usage_seconds_total)` → millicores / `limits.cpu` | SSA-patch `resources.limits.cpu` millicores; in-place (`InPlacePodVerticalScaling`) or roll fallback | `Bidirectional` | 80% setpoint, grow >0.85, shrink <0.70, floor 100m, ceiling per-target | PartialProgress (in-place) / Transactional (roll) — one cgroup cpu-quota change; safe-min clamp ⇒ never throttle live demand |
| **replica** | `status.replicas` + KEDA `ScaledObject` (read-only) | **none** — `AlreadyConverged`; never writes `/scale` | `ObserveOnly` | n/a (KEDA owns the band) | by abstention — feeds replica-ceiling context to memory/cpu via `depends_on`; never contends with KEDA |

---

## 13. Risks (honest register)

1. **Cross-dimension coherence is unmodeled until M3.** Each
   `(target × dimension)` band is reconciled independently; a workload
   can hold its memory band while CPU-starving, or grow storage while
   memory OOMs. The M3 `BreathePromise` lattice-meet *detects*
   all-in-band; it does not yet *prioritize* (e.g. shrink cpu to fund
   memory). Genuinely-coupled resources need a joint plan, not N
   independent band laws — deferred, and a real gap under bursty load.
2. **Bidirectional shrink causes pod recreation** on clusters without
   `InPlacePodVerticalScaling`. The safe-min clamp + cooldown + dry-run-
   first mitigate; a churny workload with a too-tight band could still
   see avoidable rolls. The provider surfaces roll-vs-in-place in the
   attestation so operators see real disruption.
3. **Storage assign is irreversible** (CSI grow-only). A runaway grow on
   a misreporting metric permanently enlarges a PVC — money leak. The
   ceiling circuit-breaker + `GrowOnly` bound it; set ceilings
   conservatively. Metric-freshness gating before `AutoCorrect` is
   required.
4. **Single-writer is field-granular, not concept-granular.** A
   controller mutating the limit via a *different* mechanism (Helm
   redeploy, GitOps drift on the base manifest) isn't a `managedFields`
   owner mid-tick; `breathe` would fight it next reconcile. Closed by the
   GitOps-cede renderer invariant (§9/§10.1) — load-bearing, not optional.
5. **Attestation cost.** Signing every tick × every (target × dimension)
   across the fleet is high-cardinality. Resolved by attesting
   **state-changes + a periodic in-band heartbeat**, not every Hold —
   bounding load while keeping the chain meaningful.
6. **`Custom` → canonical 6th `PromessaTargetKind`** is a substrate-wide
   commitment (ripples through every `ActionExecutor`/`AnomalyController`
   switch). Held until M3, after the three-uses rule has fired across
   memory/storage/cpu — never promoted prematurely.
7. **Metric fidelity drives the band.** A *wrong* scrape could carve in
   the wrong direction; the mock-tested band law can't catch a bad
   metric. `MetricsMissing → Hold` handles *absent* metrics safely;
   *wrong* metrics need freshness + sanity gating before `AutoCorrect`.

---

## 14. See also

- [`BREATHABILITY.md`](BREATHABILITY.md) — scale-to-zero between workloads
  (the sibling half; §VII renderer invariants extended here).
- [`CONTINUOUS-SOLUTION-MACHINE.md`](CONTINUOUS-SOLUTION-MACHINE.md) — the
  Viggy Method; `breathe` is a convergence controller + PROVABLE OUTCOMES
  consumer.
- [`GALHO.md`](GALHO.md) §III — the `IaCSystem`/`ApplySemantics`
  non-overloadable-algebra pattern the provider trait mirrors.
- `sui/sui-spec/src/catalog.rs` + `specs/catalog.lisp` — the CATALOG
  REFLECTION template reused verbatim.
- `helmworks/charts/pleme-memory-elastic/image/src/{control,crd}.rs` — the
  proven band law + single-writer guard + `MemoryBand` CRD (M0 lift).
- `helmworks/charts/pleme-storage-elastic/image/src/main.rs` — the proven
  grow-only storage watcher (M2 `StorageProvider`).
- `promessa/promessa-types/src/controller.rs` — the real
  `TargetController` (pure `diff`/`classify`/`decide`; **no `tick()`** —
  the loop is composed, not inherited).
- `pangea-operator/.../controller/*.rs` — the production best-controller
  skeleton (generation filter, diff-gated status, error policy, settling,
  policy gate).

---

## 15. Adversarial review — accepted corrections (M0 deltas)

A three-lens adversarial pass (atomicity/correctness, isolation/containment,
idiom/compounding) produced 29 findings. The decomposition (one band law +
one non-overloadable provider trait + per-dimension I/O) was validated by all
three reviewers. The corrections below are **binding on M0** — several earlier
sections over-claimed and are superseded here where they conflict.

1. **Provenance honesty.** Only the **pure core** is a verbatim lift —
   `decide` / `BandConfig` / `Decision` / the single-writer guard / `FieldOwner`
   (now **23 unit tests**, not the "12/13" earlier text claims). The **I/O legs**
   (`field_owners()` from `metadata.managedFields`, the SSA apply, the Prometheus
   scrape, owner resolution) **do not exist yet** and are NEW at M0/M1. §0/§3.3/§12's
   "reuses production code paths verbatim / only the trait wrapper is new" is wrong
   and is corrected to this.
2. **All mutations are true SSA `Patch::Apply`** (`PatchParams::apply("breathe/<dim>").force()`),
   **never `Patch::Merge`** — a trait law asserted by `MockCluster.apply` rejecting
   non-SSA patches. Real `managedFields` ownership is the entire basis of the
   single-writer + disjoint-field-partition model; the production `pleme-storage-elastic`
   uses `Merge` today and its M2 port to SSA is a real (small) behavior change, not a
   verbatim lift.
3. **The single-writer guard is field-granular** (shipped: `competing_field_manager(owners, ours, field)`
   + `FieldOwner { manager, field }`). A flat per-object bool cannot tell a memory
   writer from a replica writer; the field-path equality is what makes breathe ⟂ KEDA
   and memory ⟂ cpu provable. Tested (`keda_on_replicas_is_not_a_memory_competitor`).
4. **Metric freshness gates every mutation** (shipped: `Observation.staleness_secs` +
   `TickPlan::Stale`). The never-OOM proof holds only on a fresh sample; a scrape gap
   reading `used≈0` must coerce to a surfaced `Stale`, never a Shrink-to-floor. Tested
   (`plan_refuses_to_mutate_on_stale_metric`).
5. **CNPG is a permanent co-writer on the M0 anchor.** `pangea-database` is a CNPG
   `Cluster`; its operator continuously reconciles `spec.resources`. breathe therefore
   patches the **`Cluster` CR's `spec.resources`** (which CNPG propagates to its pods on
   a rolling update) and **Flux must cede that field** — NOT the pods directly (CNPG
   would revert them). M0 ships with `dryRun`/shadow first to prove this coexistence
   before any live carve.
6. **`OutcomeChain` is not turn-key.** Today it is hardwired to Akeyless validation CRs.
   breathe needs a **generic appender extracted first** (solve-once / load-bearing fix)
   plus the now-real tameshi Ed25519 signer. M1 dependency, named honestly — not "wire it".
7. **`breathe-core` owns its loop.** `reconciler-engine` rejects `TypedAction::Custom`,
   so breathe-core composes the Act+Attest legs itself (consistent with "the seven-beat
   tick is composed, not inherited"); it does not route Act through the engine dispatcher.
8. **"Atomic to category" = atomic at the API-object SSA-apply level**, not
   allocation-level. A memory/cpu `Apply` rolls a ReplicaSet (bounded, PDB-respecting);
   the phrase is tightened everywhere it appears.
9. **GitOps-cede is enforced at admission, not by a cross-repo renderer.** A validating
   policy breathe owns rejects/annotates a base manifest that sets a breathe-managed
   field for an enrolled target — plus a runtime detector comparing last-applied (from
   the chain) vs observed to catch a Flux reset between ticks.

## 16. Redistribution structure (the standing-promise product)

`breathe` is a **point of redistribution for the homeostasis concept**, not a
single tool. It ships as a complete, controller-enforced product across three
repos, each via its native AUTO-RELEASE path:

| Repo | Holds | Redistributes as |
|---|---|---|
| **`pleme-io/breathe`** (new ecosystem) | the Rust workspace — `breathe-control` / `-core` / `-provider`, `dimension-{memory,storage,cpu,replica}`, the controller binary, `specs/dimensions.lisp` + `catalog.rs` | **crates.io** (every member) + **ghcr** controller image, via AUTO-RELEASE on merge |
| **`pleme-io/helmworks`** | the `pleme-breathe` umbrella chart — the three `*Band` CRDs + Deployment/CronJob + RBAC, consuming `pleme-lib` | **OCI** `ghcr.io/pleme-io/charts/pleme-breathe` |
| **`pleme-io/k8s`** | per-cluster `HelmRelease` + the `*Band` CRs enrolling targets (pangea-database first) | GitOps (FluxCD reconciles rio) |

The redistributable unit is a **standing promise**: install the chart, write a
`MemoryBand`, and the 80/20 band is *maintained over time* by the controller +
attested in the chain — not applied once. This is the caixa-SDLC / AUTO-RELEASE
idiom applied to a homeostasis substrate.
