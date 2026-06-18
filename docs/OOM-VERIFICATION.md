# OOM-from-carve — tier-honest verification

> **The bright line** (per `theory/UNREPRESENTABILITY.md` §II): a `Result::Err` /
> runtime clamp / "held" decision is **mitigation**; a compile error / absent code
> path / reclaim-instead-of-kill is **unrepresentability**. This doc grades each part
> of the "make carve-induced OOM impossible by construction" work against that line
> and **never rounds up**. Honest tiers, strongest first:
>
> | tier | meaning |
> |---|---|
> | `truly-unrep` | no expressible program/state can OOM via this path |
> | `parse-time-rejected` | the bad input is refused at a parse/typed boundary before it flows |
> | `only-mitigated` | a runtime guard ("held"/clamp/`Err`) sits in front of the harm |
> | `pending-deploy` | the typed core + writer are shipped + tested; the **live** wiring needs the cluster to verify |

The live incident (2026-06): the Authentik worker (a k8s Deployment under a
`MemoryBand`) was carved 662MB→448MB toward the 0.8 setpoint on its 40%-IDLE reading.
Blueprint discovery is a transient ~600MB boot spike that OOM-killed the pod DURING
the spike — **before metrics-server ever scraped it**, so the demonstrated-peak floor
(`safety_clamp`, keyed on `update_peak`) only ever saw idle, carved to idle, and the
pod OOM-looped forever. An un-observed spike defeats the peak-floor.

---

## Part 1 — memory.high (SOFT/reclaim) is the efficiency-carve target; memory.max (HARD/kill) is only a never-OOM ceiling

**The architectural fix.** A k8s `resources.limits.memory` IS the cgroup `memory.max` —
a HARD cap; exceeding it OOM-kills with no reclaim. cgroup v2 `memory.high` is a SOFT
cap; exceeding it reclaims + throttles, **never kills**. So an efficiency shrink must
target `memory.high`; the HARD `memory.max` is governed by the never-OOM peak ceiling
ONLY and is never lowered for efficiency. The host/cgroup leg already carves systemd
`MemoryHigh` (soft) — host/cgroup carves are therefore **already** OOM-impossible; the
k8s `MemoryBand` carve (which writes `memory.max` via the pod-resize API) was the gap.

| Sub-claim | Tier | Where | Why this tier (adversarial) |
|---|---|---|---|
| The soft/hard distinction is a closed typed set; "can a target OOM-kill?" is a total function of the type | **truly-unrep** | `breathe-control::CarveSemantics{Soft,Hard}` + `can_oom()` | A `Soft` target has no code path that lowers `memory.max`; only `Hard.can_oom()` is `true`. A non-`Hard` carve cannot express a kill. |
| An efficiency shrink NEVER lowers the HARD `memory.max` (kill) limit — the kill ceiling is monotone-non-decreasing under efficiency pressure | **truly-unrep** (pure core) | `breathe-control::plan_dual_carve` — a hard-plane `Shrink` (for an in-ceiling limit) is rewritten to `NoSafeShrink` | The planner has no branch that emits a hard `Shrink` for efficiency. Proven by `efficiency_shrink_carves_soft_never_lowers_hard` + the conformance oracle `dual_carve_both_planes_stay_within_their_floors` (adversarial `ShrinkToZero` law). |
| The soft floor never drops below the request/config floor; the hard floor never below the demonstrated-peak `safe_min` — BOTH through the SAME `safety_clamp` | **truly-unrep** for the floor algebra; the *future* working set is **C2-ceiling** | `breathe-control::{safe_min, soft_min, safety_clamp}` (single source of truth) | `safe_min`/`soft_min` are the only floors; `safety_clamp` is the only gate every law funnels through. A future spike is unknowable at compile time (the C2 external-world ceiling), so the honest claim is "never below the DEMONSTRATED peak + the declared request" — structural, not a hope. |
| The k8s efficiency carve's ROUTING never lowers `memory.max` — the routed HARD target is, by construction, `≥` the live limit (only a grow or an over-ceiling snap-down) | **truly-unrep** (pure routing) | `breathe-control::{plan_k8s_memory_carve, K8sMemoryCarve, K8sMemoryCarve::never_lowers_kill_ceiling}` | The routing decision has NO code path that emits a HARD target below the live `memory.max`: an efficiency shrink is suppressed to `hard_target: None` (hold) by `plan_dual_carve`; only a grow or an over-ceiling snap-down ever becomes a `hard_target`. Proven for ALL inputs incl. an adversarial `ShrinkToZero` law by `k8s_carve_never_lowers_the_kill_ceiling` (+ `k8s_efficiency_carve_routes_soft_holds_hard`, `k8s_authentik_replay_holds_the_kill_ceiling`). Reuses the ONE `safety_clamp` — unforked. |
| The SOFT carve's pod→cgroup coordinate EXTRACTION refuses a malformed pod at the parse boundary (never a wrong cgroup path) | **parse-time-rejected** | `breathe-kube::pod_cgroup::{pod_coords_from_value, container_id_from_status, node_name_from_pod, PodCoordError}` | A pod missing `metadata.uid` / `status.qosClass` / a running container's `containerID` yields a typed `PodCoordError` BEFORE any coordinate flows to a writer — an un-started container is refused, never resolved to a bogus path. The container-id scheme-strip + QoS mapping + per-container pick are pure + tested (8 cases). |
| The SOFT carve writes the pod's `memory.high` (SOFT) cgroup file directly — under the CORRECT cgroup-driver layout — NEVER `memory.max` | **truly-unrep** (writer + path) | `breathe-host::{pod_cgroup_memory_high_path_with_driver, CgroupDriver, PodQosClass}` + `HostKnob::PodCgroupMemoryHigh` apply/read in `HostCluster` | The path mapper now dispatches on a typed `CgroupDriver` (`Systemd` + `Cgroupfs` arms — both tested; rio's systemd is the default + the live path), and the writer targets a path that ALWAYS ends `/memory.high`, never `memory.max` (`systemd_and_cgroupfs_drivers_diverge_for_the_same_coordinates`, `pod_cgroup_memory_high_path_cgroupfs_driver_is_the_flat_layout`, `pod_memory_high_apply_writes_the_soft_reclaim_file_not_the_hard_limit`). The closed driver enum makes "wrong layout for the cluster's driver" a typed input, not a silent assumption. |
| The controller→host-agent DISPATCH carries ONLY the soft target (a `desiredBytes`, never a hard value) | **truly-unrep** (pure dispatch payload) | `breathe-crd::PodMemoryHigh` + `breathe-controller::pod_memory_high::{build_pod_memory_high_dispatch, soft_target_for}` | The `PodMemoryHigh` dispatch CR has no field that can express a `memory.max` write; its `desiredBytes` is fed only `K8sMemoryCarve.soft_target`. Proven by `dispatch_carries_only_the_soft_target_never_a_hard_value`, `soft_target_routes_an_efficiency_carve_and_never_a_hard_value`. |
| The LIVE end-to-end convergence: the controller SSA-applies the dispatch CR per managed pod, the apiserver stores it, the host-agent on the node reconciles the actual cgroupfs write, and a real CRI/QoS read resolves against a live kubelet | **pending-deploy** | `breathe-controller::pod_memory_high::ensure_soft_carve_dispatch` + `reconcile_memory` (controller) + `reconcile_pod_memory_high` (host-agent) + `KubeCluster::resolve_pod_soft_carve_targets` | The wiring is SHIPPED (the controller routes a MemoryBand's efficiency carve to a per-pod `PodMemoryHigh` dispatch; the host-agent reconciles it via the shipped `HostKnob::PodCgroupMemoryHigh` writer, shadow-gated by the node's `BreatheNodePool.writeEnabled`). What needs the LIVE cluster to VERIFY: (a) the pod list + the SSA apply against a real apiserver; (b) the actual `/sys/fs/cgroup/.../memory.high` write on a real node + a real CRI container-id resolution; (c) the cgroupfs-driver path against a non-systemd cluster (rio is systemd, so only that arm is live-exercised). The PURE routing/extraction/payload above are `truly-unrep`/`parse-time`; this row is the irreducible C2 external-world observation ceiling — the cluster must confirm the bytes landed. |

**Net Part-1 tier:** the **pure soft/hard algebra + the k8s ROUTING decision + the
pod-cgroup coordinate extraction + the driver-aware writer + the dispatch payload are
all `truly-unrep` / `parse-time-rejected` at the LIBRARY level** — no expressible
program lowers `memory.max` for efficiency, resolves a malformed pod to a wrong path,
or dispatches a HARD value. The **LIVE end-to-end convergence (controller SSA-apply →
apiserver → host-agent cgroup write on the node) is `pending-deploy`** — the C2
external-world ceiling: the cluster must confirm the `memory.high` bytes actually
landed. The **host/cgroup plane is already `truly-unrep`** (it has always carved
`MemoryHigh`/soft).

---

## Part 2 — warmup-hold (closes the un-observed-boot-spike hole; would have prevented the incident)

A workload observed for fewer than `warmup_seconds` since its last (re)start has not
demonstrated a full duty cycle, so its idle reading is not yet proof the slack is safe
to reclaim. A shrink during warmup is HELD; a grow is never held.

| Sub-claim | Tier | Where | Why this tier |
|---|---|---|---|
| A workload restarted < `warmup_seconds` ago is NEVER shrunk, no matter how idle | **only-mitigated** | `breathe-control::clamp_to_warmup` → `Decision::Warmup` → `TickPlan::Warmup` (a held decision, not an absent path) | A "held" shrink is a runtime gate (the honest tier per §II — a held decision is mitigation, not unrepresentability). It is exhaustively proven by `warmup_workload_is_never_shrunk_no_matter_how_idle` (4 idle levels × 5 sub-warmup ages, always `Warmup`, never `Act{Shrink}`) and end-to-end by `reconcile_holds_a_warming_up_workload_and_carves_nothing`. **Tier-honest:** this *delays* the carve until a boot spike would be observed (then it folds into the peak floor) — it does not make a too-early carve unrepresentable, it refuses it at runtime. |
| A GROW during warmup still acts (refusing headroom at boot would itself OOM) | **truly-unrep** (the gate has no grow branch) | `clamp_to_warmup` matches only `Decision::Shrink` | A grow has no code path through the warmup hold. Proven by `warmup_never_blocks_a_grow` + `reconcile_warmup_never_blocks_a_grow`. |
| Restart detection resets the warmup clock so a fresh boot spike is always seen before a carve resumes | **only-mitigated** | `breathe-runtime::warmup_state` (a lower live limit vs the prior tick ⇒ reset) | A runtime heuristic (capacity-collapse ⇒ restart). Honest: it is a detection, not a proof; a restart that does NOT lower the observed capacity is not detected (named limitation). The controller persists `warmup_start_epoch` in status. |

**Net Part-2 tier:** `only-mitigated` (a held shrink + a runtime restart heuristic) —
**but it is the piece that would have prevented THIS incident**: the authentik boot
spike lands inside the warmup window, so the band holds (never carves to idle) until
the spike has been observed and folded into the never-OOM peak floor. Replicated as a
regression test: `authentik_warmup_never_carves_before_the_boot_spike_is_seen`.

---

## Part 3 — requestFloor sourced from the LIVE pod (not only the band CR)

`BandConfig.request_floor_bytes` was honored by `safety_clamp` but only ever populated
from the band CR's hand-authored `requestFloor`. Now the controller also reads the
target's **live** `resources.requests.<resource>` and folds `max(spec, live)` in.

| Sub-claim | Tier | Where | Why this tier |
|---|---|---|---|
| A shrink can never carve below the declared `requests.<resource>` floor | **only-mitigated** | `safety_clamp`'s `safe_min` (`.max(request_floor_bytes)`) | A runtime clamp (the §II honest tier for a value floor). Proven by `shrink_never_below_request_floor`. |
| The floor is sourced from the LIVE pod even when the band CR omits it | **only-mitigated** (end-to-end wired + tested) | `Cluster::read_request_floor` (default 0) → `KubeCluster` override reads live pods' `requests.memory` (max) → `Observation.request_floor` → `reconcile_one` folds `max(cfg, live)` | Proven end-to-end by `reconcile_honors_the_live_request_floor_even_when_the_cr_omits_it` (band CR floor = 0, live pod request = 1Gi ⇒ the live floor binds). The KubeCluster read is unit-of-the-resource-aware (cpu millicores / else bytes) and covers `PodResize`/`PodTemplate`/`ClusterTopLevel`. |

**Net Part-3 tier:** `only-mitigated` — the value floor is a runtime clamp by nature
(a request floor is a number, not a type), but it is now **wired end-to-end** from the
live pod, closing the "operator forgot to declare requestFloor" gap.

---

## Honest summary — what IS and ISN'T OOM-impossible-by-construction

- **The pure soft/hard carve algebra IS unrepresentable-OOM** (`truly-unrep`): no
  expressible efficiency carve lowers `memory.max`; the kill ceiling only rises.
- **The k8s ROUTING decision IS unrepresentable-OOM** (`truly-unrep`):
  `plan_k8s_memory_carve`/`K8sMemoryCarve` have no code path that routes a HARD target
  below the live `memory.max` — the efficiency carve is routed to the SOFT plane, the
  HARD plane only ever grows or holds (`never_lowers_kill_ceiling` holds for all inputs,
  adversarial laws included).
- **The pod→cgroup coordinate extraction IS parse-time-rejected**: a malformed/un-started
  pod is refused with a typed `PodCoordError` before any coordinate reaches a writer.
- **The driver-aware `memory.high` writer + the dispatch payload ARE `truly-unrep`**: the
  path always ends `/memory.high` (never `memory.max`) under the correct typed
  `CgroupDriver` (systemd + cgroupfs both tested); the `PodMemoryHigh` dispatch CR has no
  field that can express a HARD write.
- **The host/cgroup plane IS already OOM-impossible** (it carves `MemoryHigh`/soft).
- **The LIVE end-to-end convergence is `pending-deploy`** (the irreducible C2
  external-world ceiling): the controller→host-agent wiring is shipped
  (`reconcile_memory` routes a MemoryBand's efficiency carve to a per-pod `PodMemoryHigh`
  dispatch; `reconcile_pod_memory_high` writes the cgroup file, shadow-gated by the
  node's `BreatheNodePool.writeEnabled`), but the actual apiserver list + SSA apply + the
  `/sys/fs/cgroup/.../memory.high` write on a real node need the cluster to confirm the
  bytes landed.
- **Warmup-hold + requestFloor are `only-mitigated`** (held decisions + value clamps),
  but warmup-hold is the specific runtime fix that would have prevented the authentik
  incident, and it is the path that keeps the band safe until the `pending-deploy`
  live convergence is verified on rio.

**The remaining work — now NARROWED to live verification, not library construction**
(named, not hidden): deploy the routing + dispatch to rio and confirm, on-cluster, that
(1) the controller emits a `PodMemoryHigh` per managed authentik pod on an efficiency
carve, (2) the host-agent writes the pod's `/sys/fs/cgroup/.../memory.high` to the routed
soft bytes while `limits.memory` (`memory.max`) is untouched, and (3) a real CRI
container-id + QoS resolves against the live kubelet. The cgroupfs-driver path arm is
SHIPPED + tested but only the systemd arm (rio's driver) is live-exercised; a non-systemd
cluster is the named follow-on. A `CgroupDriver` config knob on `BreatheConfig` (today the
controller defaults the dispatch to `systemd`) is the small remaining config surface for
cgroupfs clusters.
