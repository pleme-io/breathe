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
| The k8s efficiency carve writes the pod's `memory.high` (SOFT) cgroup file directly, NEVER `memory.max` | **pending-deploy** | `breathe-host::{pod_cgroup_memory_high_path, PodQosClass}` + `HostKnob::PodCgroupMemoryHigh` apply/read in `HostCluster` | The typed path mapper + the host-agent writer are **shipped + unit-tested** (`pod_cgroup_memory_high_path_is_the_systemd_driver_layout`, `pod_memory_high_apply_writes_the_soft_reclaim_file_not_the_hard_limit`). What is **NOT yet wired/verified without the live cluster**: (a) resolving the live pod's CRI container-runtime-id + QoS from the apiserver and routing a `MemoryBand`'s soft carve to the host-agent DaemonSet; (b) the cgroupfs-driver path variant (only the **systemd**-driver layout — rio's driver — is implemented; a `CgroupDriver` arm is the named follow-on). Until (a) ships, the k8s memory band still carves `memory.max` (HARD) and relies on Part 2 + the peak-floor for never-OOM. **This is the one piece that is not yet OOM-impossible-by-construction for the k8s plane.** |

**Net Part-1 tier:** the **pure soft/hard algebra is `truly-unrep`**; the **k8s live
write of `memory.high` is `pending-deploy`** (typed core + host-agent writer shipped &
tested; the apiserver→host-agent pod-cgroup routing is the remaining live wiring). The
**host/cgroup plane is already `truly-unrep`** (it has always carved `MemoryHigh`/soft).

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
- **The host/cgroup plane IS already OOM-impossible** (it carves `MemoryHigh`/soft).
- **The k8s plane's live `memory.high` write is `pending-deploy`**: the typed path
  mapper + host-agent writer ship & test green, but the apiserver→host-agent pod-cgroup
  routing (resolve the live container-runtime-id + QoS, hand the soft carve to the
  DaemonSet) is the remaining live wiring. **Until it ships, the k8s memory band still
  carves `memory.max` and is NOT yet OOM-impossible by construction** — it relies on
  Part 2 (warmup-hold) + the demonstrated-peak floor (C2-ceiling) for never-OOM, which
  is `only-mitigated`, not `truly-unrep`.
- **Warmup-hold + requestFloor are `only-mitigated`** (held decisions + value clamps),
  but warmup-hold is the specific runtime fix that would have prevented the authentik
  incident, and it is the path that keeps the band safe until the `pending-deploy`
  soft-carve wiring lands.

**The remaining work to reach `truly-unrep` on the k8s plane** (named, not hidden):
route a `MemoryBand`'s efficiency carve to `HostKnob::PodCgroupMemoryHigh` (resolve the
live pod UID + CRI container id + QoS from the apiserver, dispatch to the host-agent
DaemonSet), pin the k8s `limits.memory` at the peak-floor ceiling, and add the
cgroupfs-driver path arm. That converts the k8s efficiency carve from "writes
memory.max (kill)" to "writes memory.high (reclaim)" — the same `truly-unrep` the
host/cgroup plane already enjoys.
