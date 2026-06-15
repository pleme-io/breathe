# breathe — resource-homeostasis substrate

> skip-auto-release: internal-only — breathe is **PRIVATE** during the
> akeyless ephemeral-environment integration. Its crates must NOT publish to
> public crates.io (that would leak private code) and its controller image is
> a **private** ghcr package. There is intentionally no `auto-release.yml`.
> When breathe goes public, drop this waiver, add the 3-line auto-release shim,
> and publish the `pleme-breathe` chart to public helmworks.

> skip-format-ban: young repo, not yet wired to the substrate `with-format-ban`
> clippy.toml. New code still obeys ★★ TYPED EMISSION by hand: limit values
> render through `Quantity`'s `Display` impl (the typed surface); JSON-pointer
> and PromQL `format!`s are the documented exception (pointer/query strings,
> not platform-emitted syntax) and migrate to a typed builder when one lands.

One running controller, one **proven dimension-agnostic band law**
(`breathe-control`), a catalog of pluggable resident-problem-category
descriptors — every enrolled workload held in a typed utilization band
(default 80% used / 20% headroom) by gentle convergent steps, single-writer by
construction, never-OOM by construction. Architecture of record:
`docs/BREATHE.md`. Born from the live rio pangea-database OOMKill need.

**What's actually novel (tier-honest, do not overclaim):** breathe is a novel
*synthesis*, not a novel *primitive* — its in-place `pods/resize` actuation is
commodity 2026 k8s (KEP-1287 GA, VPA InPlace, ScaleOps). The defensible claims
are the **conjunction** of band-control-on-the-limit + cooperative-by-default
`managedFields`-yield + the unified pod/host/node scope (the host-OS leg has no
prior art). The single-writer guard is `only-mitigated` (per-reconcile yield),
**not** `unrepresentable` — a force-applying peer can still construct a conflict.
Full adversarial prior-art ledger + the must-not-claim caveats:
[`docs/BREATHABILITY-PRIOR-ART.md`](docs/BREATHABILITY-PRIOR-ART.md).

## Crate map

| Crate | Owns |
|---|---|
| `breathe-control` | the band law (`decide`/`plan_tick`), single-writer guard, directionality clamp, **`Unit`/`Quantity` codec**. **Dependency-free, pure, fully unit-tested.** |
| `breathe-provider` | `Cluster` + `DimensionDescriptor` traits, the one generic `BandProvider`, `MockCluster` |
| `breathe-core` | the composed `reconcile_one` seven-beat loop |
| `breathe-dimensions` | the concrete descriptors (memory/cpu → metrics-server; storage → PromQL) |
| `breathe-crd` | the `MemoryBand`/`CpuBand`/`StorageBand` CRDs (one `band_kind!` macro) + `crdgen` |
| `breathe-kube` | `KubeCluster` (true SSA), `managed_fields` parser |
| `breathe-catalog` | `(defdimension)` catalog + CATALOG REFLECTION tests |
| `breathe-controller` | the multi-dimension watch binary (one generic `reconcile<Band, Descriptor>`) |

## The one invariant that makes a dimension correct: units

The band law is **unit-agnostic** — its `(used, capacity, floor, ceiling)` are
opaque `u64`s. A dimension stops being unit-agnostic at exactly two edges:
**parse** (k8s quantity string → scalar) and **render** (scalar → k8s
quantity). Both go through `breathe_control::Unit` / `Quantity`:

- memory / storage → `Unit::Bytes` (`2Gi` → 2147483648; renders as a bare int).
- cpu → `Unit::Millicores` (`250m` → 250, `2` cores → 2000; **renders with the
  `m` suffix** — a bare `"250"` would be read by k8s as 250 *cores*).

`Unit::for_resource(resource)` is the single dispatch; it is used by
`band_config_of` (floor/ceiling parse), `KubeCluster::read_limit` /
`pod_metrics_max` (parse), and `KubeCluster::apply` (render). **Adding a new
unit is one match arm in `breathe-control` and nowhere else.** Never write a
limit through `value.to_string()` — go through `Quantity`.

## Adding a dimension

One `DimensionDescriptor` impl (`breathe-dimensions`) + one catalog row
(`breathe-catalog`; the reflection test fails if either is missing) + a
`band_kind!` line (`breathe-crd`, passing the dimension's `Unit` + default
floor/ceiling). The controller code never grows. **Never touch `decide`.**

## Pod resolution: owner-selector vs label-selected group

A band resolves the pods it carves two ways, on one seam (`KubeCluster::owner_pods`):

- **owner-selector** (default): GET the `targetRef` owner (Deployment / StatefulSet /
  CNPG `Cluster`) and list pods by its `spec.selector.matchLabels`.
- **label-selected group** (`targetRef.podSelector` set): list pods DIRECTLY by that
  k8s label selector — no owner GET. The path for **ephemeral / owner-less pod sets**
  whose name is not stable and which have no single resolvable owner: GitHub ARC
  `EphemeralRunner`s (`actions.github.com/scale-set-name=<set>`), bare pods, Job pods.
  A `podSelector` band is ALWAYS `PodResize` (in-place, zero restart — there is no
  template to roll), scoped to the band's namespace; the metric reads the SAME
  selector (PodMetrics mirror their pod's labels). `targetRef.name` then serves only
  as the human label. This is how a CI runner is held breathable: a fresh runner pod
  appears per job under a stable selector, breathe carves its live limit in band.

  RBAC: the controller needs `pods` + `pods/resize` `patch` for any LIVE in-place
  carve (chart `templates/rbac.yaml`) — granted fleet-wide, not runner-specific.

  **Dormant** (scaled-to-zero): a `podSelector` group with zero matching pods is a
  benign resting state, NOT an error — an ephemeral runner is absent between builds.
  The metric read returns `ProviderError::NoTargetPods`, the loop maps it to
  `TickReceipt::Dormant` (phase `Dormant`, `Ready=True`, `Converged=True`, counted
  at-rest in the overview, re-checked at the fast cadence). Only an empty SELECTOR
  group is dormant; an owner (Deployment/CNPG) with no pods is still `MetricsMissing`
  / `Error` (genuinely abnormal). Generalizes to Job pods + KEDA-to-zero workloads.

## rio go-live status (2026-06-13)

- **memory — LIVE on pangea-database.** breathe owns `Cluster.spec.resources.limits.memory`
  (floor 2Gi / ceiling 4Gi), seeded via the Helm-`null` cede, holding `AtFloor`,
  auto-grows on pressure. **OOMKill class closed.** **M0 predictive ON** here
  (`predictive: true`, `predictiveLookaheadSeconds: 60`) — the dormant
  `PredictiveGrow<BandLaw>` grows headroom on working-set velocity *before* the
  spike, strictly safer for OOM than reactive (only ever raises the limit
  earlier; still through `safety_clamp`; the never-OOM oracle covers it via
  `safety_gate_contains_the_predictive_law`). Default off fleet-wide.
- **cpu — LIVE on pangea-database** (floor 500m / ceiling 2). Same CNPG Cluster,
  `limits.cpu`, `breathe/cpu` field manager (disjoint from `breathe/memory`).
- **storage — code-complete + CLUSTER-AWARE, correctly PARKED on rio.** The
  `ClusterStorage` layout carves a CNPG `Cluster`'s `spec.storage.size` (the one
  declarative field the operator owns + reconciles to every instance PVC) and
  aggregates the instance-PVC metric (`<name>-[0-9]+`); the pangea-database band
  reads `spec.storage.size=10Gi` correctly. `local-path` now has
  `allowVolumeExpansion: true`, **but the deeper blocker stands and is now
  understood + guarded:** local-path PVCs have **no per-volume accounting**, so
  `kubelet_volume_stats` reports the *whole node filesystem* (466 G used / 972 G
  cap) for a 10 Gi volume. The new **`MetricUnrepresentable` guard** makes that a
  typed, observable, never-carves terminal (`used > capacity` on a `GrowOnly`
  band proves the metric isn't per-entity), so a ceded local-path band can never
  run away to ceiling on the lie. On rio the storage bands sit in **Conflict**
  (operator-owned fields, single-writer-first) — correct + safe. **Named trigger
  to go live:** a CSI storageClass with **per-volume accounting + enforced
  quotas** (Longhorn / Ceph-RBD / EBS), then cede the field as memory did.
- **Program backlog:** the full carve-vector enumeration (96 vectors, 7 hazard
  classes, 15-PR sequenced plan) + the shipped ledger live in
  `theory/BREATHABILITY-PROGRAM.md`.

## Build + ship

- Toolchain: nix-store rustc (`nix develop` / the flake's devShell).
- Image: `.github/workflows/image.yml` builds the Nix→OCI controller and pushes
  a **private** `:latest` via `skopeo copy --dest-creds` + a v2 registries.conf
  (nixpkgs skopeo rejects the runner's v1 conf — do NOT `skopeo login`).
- Deployed to rio via the **vendored** chart at `k8s/clusters/rio/infrastructure/breathe/chart/`
  (pulled by the flux-system GitRepository — no OCI/private-auth). `:latest` +
  `Always`, so a new build needs a `kubectl rollout restart deploy/breathe-controller`
  to pick up.
- Escape hatch: HelmRelease `suspend: true`, or any band's `dryRun: true`
  (observe + attest, never carve) — one line each.
