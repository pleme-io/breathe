# breathe — the grand breathable-resource substrate

> **Frame.** [`BREATHE.md`](BREATHE.md) is the **L1 workload core**: one
> unit-agnostic band law holding a single workload's dimension inside an
> 80/20 band, attested. **This document** is the grand expansion the
> operator scoped (2026-06-02): the same band law lifted into a **three-tier
> substrate** — workload-dimension goals over node + cluster capacity rules
> + slicing, over pooled resources — unified by central typed patterns,
> configurable by a catalog of pluggable control/scheduling/pooling
> strategies, controlling workloads across a cluster **over time** for
> maximum utilization. The thesis, verified in code: this is a **pure
> additive extension** of the eight existing breathe crates (>90% reuse,
> tests green), not a rewrite. `breathe` is held **private** (this doc lives
> in `breathe/docs/`, not public `theory/`) pending the akeyless
> ephemeral-env integration.

> **Governing discipline — layered + explicit.** Resource control is
> declared in three composing layers, never scattered magic numbers (see
> the `feedback_layered_explicit_resource_control` operator note). Each
> layer is explicit; layers compose: **L1** workload bands ride inside the
> **L2** capacity partition, which sits on the **L3** pooled-resource
> substrate. A contended workload is never left unbounded — that is how
> rio's kine starved under a build storm.

---

## 1. Destination

A node (and a set of nodes) is a **homeostatic organism**. The operator
declares *goals* — hold each dimension at its band (80/20 default,
configurable per dimension) — and the substrate *breathes*: every workload
consumes as much as it needs **but only in relationship to the bounds other
workloads leave it**, the whole node tracking maximum utilization without
ever crossing a never-cross floor. The same proven band law governs at
three nested scales (workload, node+cluster capacity, pooled resources);
every algorithm is a pluggable typed *strategy* property-tested against the
band law as the conformance oracle; the safety scaffolding is **lifted, not
touched**. Promises ("kine never starves," "the node never swaps to the
QLC," "the cache warms without overwhelming atticd") become continuously
attested theorems, not assertions.

`attic` and `rio` are the first two instances, and the proof the core
crosses the Kubernetes↔host boundary unchanged.

---

## 2. The three-tier model

| Tier | Owns | Controlled variable | Default band | Status |
|---|---|---|---|---|
| **L1 — Workload** | a workload's dimension | a limit/request/concurrency field | 80/20 | **exists** (BREATHE.md) — lift `decide` behind a `ControlLaw` trait |
| **L2 — Capacity** | a node's, then a cluster's, total capacity | an *envelope* (a partition of the budget) per workload | per-dimension (kine 0.65, elastic 0.80) | **new** — `breathe-capacity` + `breathe-cluster` |
| **L3 — Pool** | discrete acquirable units | the warm/free ratio | 80/20 on the warm ratio | **new** — `breathe-pool` |

- **L1 (workload)** — unchanged core. `decide` lifts behind a `ControlLaw`
  trait (band law as default + oracle); the *safe-minimum* clamp hoists into
  a shared `safety` gate so a band can only breathe **within** the capacity
  envelope handed down from L2.
- **L2 (capacity)** — a **capacity vector** and a **demand vector** drive a
  *placement* strategy, a *fair-share allocation* strategy, and an *eviction*
  response. A strategy emits only an **envelope** (a budget), never a raw
  mutation; L1 bands breathe inside the envelope.
- **L3 (pool)** — discrete units (spot builders, warm pods, arena blocks)
  are **acquired / used / released**. The (free, warm) count *is* a (used,
  capacity) pair, so the **band law holds the warm ratio** and **Little's Law
  sizes the warm count** from arrival rate × hold time. When the units are
  *nodes*, `acquire` re-enters the L2 capacity tier (the recursion Powers'
  PCT predicts).

---

## 3. Central patterns (the typed atoms)

Seven typed atoms key every registry across all three tiers — the
interfaces that make every catalog algorithm interchangeable:

| Pattern | Role |
|---|---|
| `Dimension` | the typed atom keying every registry across the three tiers |
| `Band` | the 80/20 band, reused at workload, node, and pool scale |
| `Tier` | the proportional response bracket as a function of % deviance |
| `Signal` | the typed input — utilization, congestion, trend, or forecast |
| `Actuator` | the mutation surface, carrying directionality + a cost model |
| `Strategy` | the pluggability trait, gated by property-test laws vs. the oracle |
| `Pool` | the acquire/use/release peer of the resource provider |

---

## 4. The core move — lift the control law

Today `breathe-control::decide(used, capacity, &BandConfig) -> Decision` is
**one fixed law**: a deadband (`shrink_below` 0.70 / `grow_above` 0.85 around
`setpoint` 0.80) with a multiplicative bang-bang response (`grow_factor`
1.25 / `shrink_factor` 0.90), a safe-minimum anti-overshoot clamp, and
floor/ceiling circuit-breakers (`AtCeiling` / `NoSafeShrink`). That is
itself a recognizable control law at **one point** in a large design space.

The move: lift `decide` behind a typed `ControlLaw` strategy trait, so each
dimension selects the law that fits its **actuator economics** and **signal
quality** — *without touching the proven scaffolding*: the single-writer
field guard (`competing_field_manager`), `clamp_to_directionality`
(GrowOnly/ObserveOnly), the `staleness_secs` freshness gate, the cooldown
gate, the safe-minimum clamp, and the same observable `Decision` enum.

> **The law decides the *target*; everything that makes a target safe to
> apply stays shared and unchanged.** This is the layered/explicit principle
> as code: explicit shared safety frame ← pluggable explicit strategies ←
> explicit tiered deviance response. The band law remains the **default** and
> the **conformance oracle** — every new strategy is property-tested to
> agree with it in-band and to never violate the safety invariants.

---

## 5. The deviance + jitter law

Response is **proportional to % deviance** from the band, in tiers, with the
band itself as the deadband, and a universal damping decorator wrapping any
inner law before the safety gates:

- **Four response brackets** keyed to deviance: *hold* (in-band) → *gentle
  nudge* (just out) → *full step* → *aggressive grow + escalation* (far out).
- **The band IS the deadband** — with a **dwell count** (N consecutive
  out-of-band ticks before acting) and a **cooldown** (settling time). This
  is control-theory **hysteresis** (Hellerstein/Diao/Parekh/Tilbury 2004):
  the 0.70–0.85 gap prevents flapping; *the deadband width must exceed the
  metric-noise amplitude*.
- **Damping decorator** — exponential smoothing (EWMA) on the signal + a
  per-tick **slew cap** on the actuation, applied *before* the safety gates.
  De-synchronizes fleet actions (randomized jitter, à la `RandomizedDelaySec`).
- **breathe is a P-only saturated controller** → structurally **windup-immune**;
  the floor/ceiling circuit-breakers (`AtCeiling`/`NoSafeShrink`) *are* the
  anti-windup mechanism. `tick_converges` is a discrete-time Lyapunov
  contraction proof.

Grounded in Hellerstein *Feedback Control of Computing Systems*, Åström,
Ziegler–Nichols, AIMD, and the autonomic control loop.

---

## 6. Band law reinforced — the academic grounding

The band law is **unchanged in code**; every constant is now a *derived
quantity*, not an asserted one:

1. **80% setpoint = the M/M/1 queueing knee.** Mean wait grows as ρ/(1−ρ);
   queue length ρ²/(1−ρ) goes 0.7→1.6, **0.8→3.2**, 0.9→8.1, 0.95→18 — the
   knee. Kingman's heavy-traffic VUT shows the knee **drops under load
   variability**, so rio's lumpy bursts justify the **kine / QLC bands at
   0.65–0.70**, not 0.80. The Universal Scalability Law proves a contended
   node **retrogrades past N_max**, so backing off *recovers* throughput.
   *(Kingman 1961; Little 1961, L=λW; Vilaplana 2011 arXiv:1106.2380; Gunther USL.)*
2. **AIMD = the back-off shape.** grow 1.25 (multiplicative-up probe) +
   shrink 0.90 (multiplicative-down) + safe-min clamp is the **unique linear
   class converging to efficiency *and* fairness** under decentralized
   feedback (Chiu & Jain 1989, Prop. 3: a_i>0, b_d∈(0,1)). **Back-off harder
   than you grow is a stability requirement, not a heuristic**, and AIMD
   fairness justifies criticality-weighted back-off of the noisy neighbor.
3. **Tiers = MAPE-K.** `reconcile_one`'s seven beats *are* IBM's
   Monitor-Analyze-Plan-Execute-over-Knowledge loop (Kephart & Chess 2003).
   *lean* = a Vegas/BBR early **delay** signal + Google-SRE adaptive
   throttling; *panic* = a Nygard **circuit-breaker** + load-shed by
   criticality; per-dimension isolation = a Nygard **Bulkhead**.
4. **Deadband = hysteresis; cooldown = settling-time damping.** breathe being
   P-only + saturated makes it windup-immune by construction.
5. **Panic = homeostatic overload; band-not-threshold is mandatory.** The
   system has qualitatively distinct regimes (Cannon 1932 range-keeping;
   allostasis = predictive set-point; allostatic load = the carve cost
   breathe minimizes). The **multi-dimensional whole-node** design is
   *mandated* by **Ashby's Law of Requisite Variety** + the **Conant–Ashby
   Good Regulator Theorem** ("every good regulator must be a model of the
   system" — the typed dimension catalog *is* that mandatory model). The
   node-coordinator is **Beer's Viable System Model** (System 2/3);
   pool→node recursion is **Powers' Perceptual Control Theory**.

---

## 7. rio — the first non-Kubernetes instance (the unlock)

**Verified in code:** the band law is already **dimension- and
unit-agnostic** (opaque `u64` used/capacity; `Unit ∈ {Bytes, Millicores, …}`).
So rio is **purely additive**: a new **`HostCluster` provider family**
(`SystemdCgroup`, `ZfsArc`, `NixConcurrency`, `AtticPush`) parallel to the
existing `KubeCluster`, sharing `decide`, `clamp_to_directionality`,
`competing_field_manager`, and `reconcile_one` **with zero band-law change**.
rio is the first non-K8s breathe consumer and the proof the core crosses the
K8s↔host boundary unchanged. This closes the helmworks-identified
"no host/systemd target" gap *additively*.

A **node-coordinator** (Beer VSM System 2/3) owns the shared budget
(~29 GiB RAM, 32 vCPU, QLC-IOPS, pool bandwidth) and arbitrates by
**criticality**: kine is inelastic `CRITICAL_PLUS`; builds + atticd are
elastic `SHEDDABLE`. It funds the inelastic tier from the elastic tier.

### 7.1 The five-tier ladder (each grounded)

| Tier | Trigger (signal) | Action | Theory |
|---|---|---|---|
| **normal** (Hold) | all signals in deadband (cpu/io/mem PSI under setpoint; no `pswpin`) | no actuation; heartbeat-only attestation (never sign every Hold) | MAPE-K steady state; queueing-knee operating point |
| **lean** (additive ease-off) | a dimension crosses `grow_above` on an **early delay signal** (rising PSI, ARC headroom shrinking) *before any hard error* | gentle AIMD ease-off of the **lowest-criticality elastic** dimension only; Alert/shadow → AutoCorrect | Vegas/BBR delay gradient; PSI (Meta oomd) |
| **back-off** (multiplicative-decrease) | PSI sustained past cooldown, or two dims in reactive scope, or kine `io.pressure full avg10 > 10%` with an attributable competitor | AIMD MD on elastics in **strict criticality order** (tend-prebuild slice → atticd push → build `IOWriteIOPSMax`); shrink `zfs_arc_max` **first** under memory pressure; **never kine** | AIMD MD; SRE throttling; Bulkhead; USL retrograde recovery |
| **panic** (circuit-breaker + load-shed) | **loss signal**: `pswpin > 0` (live pages swapping to QLC = rio's true eviction floor), or a floor/ceiling breaker trips, or kine/apiserver past SLO | refuse new builds (`max-jobs → floor 4`, pause tend slice), pause atticd push, hard-clamp ARC to its 3–4 GB floor, hold kine untouched; backpressure demand→0; fire kine-health VACUUM if db-size band exceeded | Nygard CB; SRE load-shed; Reactive Streams backpressure; never-swap clamp |
| **alert** (Pillar-11 human-on-the-loop) | MAPE-K Plan space exhausted: no convergence after shedding all SHEDDABLE load for `> escalate_after_secs` | signed AnomalyChain entry + ntfy (rio VictoriaMetrics/Logs/ntfy sink); surface dimension + decision fingerprint; freeze carving; hand to operator | Sheridan–Verplank LOA (human-on-the-loop is the safe terminus); MAPE-K Plan exhaustion; Pillar-11 |

### 7.2 rio dimensions (L1 bands within the L2 partition)

Signals are **Linux PSI** (cpu/io/mem `some`/`full` `avg10`), `pswpin` (the
true eviction floor), and per-cgroup `io.stat`. Actuators are **live** (no
rebuild) via `systemctl set-property --runtime`, `nix.conf` reload, and
`/sys/module/zfs/parameters/zfs_arc_max`.

| Dimension | Signal | Actuator (live) | Band | Phase |
|---|---|---|---|---|
| **nix-build concurrency** (master throttle) | whole-node PSI fan-in; prebuild-slice `cpu`/`io.pressure` | tend-prebuild `--max-inflight`/`--max-jobs`; slice `CPUWeight`/`CPUQuota`/`MemoryHigh`/`IOWeight`; `nix.conf cores` | floor 4, ceiling 32; hold whole-node PSI in band; ×0.7 on tier-2 | **M0** |
| **RAM + swap + ZFS ARC** (never-swap clamp) | `memory.pressure`, `pswpin` | `zfs_arc_max` via `/sys`; slice `MemoryHigh`/`MemoryMax` | shrink ARC **first**; never-swap floor | **M0** |
| **atticd push** (congestion) | per-batch loss / Connection-refused (AIMD signal) | in-`seibi` push concurrency + inter-batch delay | floor 1, ceiling 8, ride the moving ceiling | **M0** (attic instance) |
| **QLC build IOPS** | `io.pressure` on the build slice / nvme0 await | slice `IOWriteIOPSMax` on the QLC device | keep below the knee | M1 |
| **tend-prebuild slice** | slice PSI | `CPUWeight`/`CPUQuota`/`IOWeight` | elastic, first-to-shed | M1 |
| **kine / k3s** (the protected gate) | kine `io.pressure`, SQLite write-latency, :6443 health | *observe-only*; never throttled — funded *from* the elastics | inelastic 0.65 | M0 (observe) |

---

## 8. attic — the canonical congestion-signal instance

atticd exposes **no** server-side rate/concurrency/admission knob (every
write failure is a bare HTTP 500), and the safe rate is **time-varying**
(rio builds on the same node). So a fixed-rate token bucket can't fit — the
controller must be **client-side AIMD that finds the moving ceiling online**:

- **Inner loop (ships now, zero new infra):** a typed `BreathController` in
  `seibi/src/attic_push.rs`, injected at the existing per-batch ok/err site,
  adapting `jobs` + inter-batch delay: **slow-start → additive-increase** as
  atticd recovers, **collapse-to-floor + exponential backoff + circuit-breaker**
  the instant it errors. Failed paths **re-enqueued, not dropped** (cache
  warms fully). Authored as the typed-spec triplet (Rust border + pure
  `observe()` + a `Pusher` Environment trait tests mock) — mirroring
  `DiskPressureState`. Pillar-11 hook: ntfy `rio-warning` when the breaker
  opens. *(Already half-built: `--best-effort` landed; the AIMD loop is the
  next change.)*
- **Outer loop (M2+):** seibi exports a push-throughput metric; a breathe
  `AtticPush` dimension (Bidirectional) holds the steady ceiling in the 80/20
  band and attests it via OutcomeChain. **breathe sets the band; the inner
  AIMD rides under it.**
- **L3 fix already landed:** atticd store is on the Samsung `pool` (not the
  QLC), and **NAR chunk sizes raised ~8×** (`5d818f1`) — ~8× fewer SQLite
  rows/files/round-trips, removing most Connection-refused storms; the AIMD
  handles the residual.

---

## 9. Capacity, cluster, and temporal tiers (L2, M2+)

- **Capacity (node):** a `capacity_vector` and `demand_vector` drive
  **DRF** (Dominant Resource Fairness) packing + an eviction response;
  overcommit is the utilization lever, preemption the safety valve.
- **Cluster:** pack demand vectors to each node's band ceiling; fair-share
  across nodes; **work-stealing flattens cross-node deviance**; predictive
  pre-warming + MPC act *before* pressure; node/cluster Band CRs make the
  fleet provable.
- **Kubernetes node enrollment (the L2 k8s face):** a **`BreatheNodePool`**
  CRD declares "these nodes are breathe-managed" (selector/labels/explicit
  list) with an enroll→drain→release lifecycle; breathe inventories *all*
  dimensions (capacity/reserved/allocatable/used/headroom) into a pool model;
  a **supervisory meta-controller** (hierarchical control, **3–10× timescale
  separation**) continuously **tunes the inner controllers' parameters**
  (band setpoint/factors/cooldown, KEDA/HPA targets, VPA bounds) — *never the
  resources directly* — writing only `breathe/meta-tuning`-owned fields so
  HPA/VPA/KEDA never fight (the disjoint-field single-writer invariant).
  *(Koopman time-scale separation; gain-scheduling for nested systems;
  HPA-vs-VPA conflict avoidance.)*

---

## 10. Pooling tier (L3, M3)

`acquire` mints/wakes units **by latency class** (instant warm pool → spot,
seconds-to-minutes) and **errors typed when exhausted** (never hangs). `use`
is an RAII guard that returns the unit on drop. `release` reclaims by policy
(eager → scale-to-zero). The (free, warm) ratio is **held at the band**; the
cost model + **Little's Law** size each pool per its own dimension. When the
units are nodes, `acquire` re-enters the capacity tier — composes with
`pangea-jit-builders` (ASGs at desired=0, woken on demand).

---

## 11. Reuse ledger

Pure extension; all **eight** existing crates verbatim, tests green.

- `breathe-control` **gains**: the `ControlLaw` strategy trait; the band-law
  wrapper (default + oracle); the extracted shared `safety` clamp; the
  property-test conformance laws.
- **New crates**: `breathe-laws` (PID/AIMD/adaptive + damping decorator),
  `breathe-capacity` (node DRF), `breathe-cluster` (fleet packing/fairness),
  `breathe-pool` (acquire/use/release spine), `breathe-host` (the
  `HostCluster` family: SystemdCgroup/ZfsArc/NixConcurrency/AtticPush),
  `breathe-nodepool` (the K8s enrollment CRD), `breathe-meta-tuning` (the
  supervisory controller).
- New Band/NodePool/Cluster CRDs reuse the existing `band_kind!` macro
  (Catalog-Reflection invariant: code ⇔ catalog ⇔ CI matrix).

**>90% reuse.** The band law, the single-writer guard, the freshness/cooldown
gates, the directionality clamp, and the attestation chain are untouched.

---

## 12. Phased plan

| M | Deliverable |
|---|---|
| **M0** | The strategy lift (`ControlLaw` trait + band-law wrapper + extracted safety clamp + deviance ladder); the `HostCluster` family; **one live rio dimension — build-vs-disk concurrency co-driven with the attic AIMD congestion law**; rio L2 partition made **explicit** (CPUQuota/MemoryHigh/MemoryMax/IOWeight on atticd, nix-daemon, tend, summing to the node budget with ~20% headroom — today only tend.prebuild is bounded). |
| **M1** | The control-law catalog crate (P/PI/PID + the damping decorator); QLC-IOPS + tend-slice dimensions. |
| **M2** | The capacity tier (`breathe-capacity` DRF) + the K8s `BreatheNodePool` enrollment + the supervisory meta-tuning controller; the breathe `AtticPush` outer band. |
| **M3** | The pooling tier (`breathe-pool` spine + the spot/JIT capacity pool). |
| **M4** | Adaptive + joint: gain-scheduling, self-tuning, fuzzy, the joint planner with prescriptive promessas. |
| **M5** | The learned law behind a veto cage; cluster federation; the canonical (still-private) doc finalized. |

---

## 13. Risks + open questions

**Risks.** Strategy proliferation guarded only by the property-test laws · the
safety clamp must apply *after every* law · overcommit needs a proven
priority ladder first · the learned law's veto cage must be the conservative
band law · joint mutation needs disjoint atomic field managers · spot
recursion risks a wake storm · pool sizing needs a usable arrival-rate
estimate.

**Open questions.** Cross-tier attestation-chain granularity · is the joint
planner a new scope or a decorator · how to break the spot→capacity recursion
cycle · can self-tuning choose the *law* inside a cage · does fairness
degenerate cleanly at single-tenant · where does per-workload *value* (for
preemption priority) come from.

---

*Synthesized 2026-06-02 from five analyses (attic, rio + band-law grounding,
the grand 3-tier substrate, helmworks chart patterns, K8s node-enrollment +
meta-tuning). Held private per the breathe privacy note.*
