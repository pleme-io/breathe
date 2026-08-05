# REALLOCATION — data-aware packing, and why breathe cannot do it yet

Companion to `PROVISIONING.md` (which answers *should this pool grow*) and
`BREATHE.md` (which answers *how much should this workload get*). This document
answers the third question neither of them owns:

> **Given the workloads that exist and the nodes that exist, is this the
> arrangement we would choose?**

Today the answer on camelot is measurably no, and the reason is not that breathe
decided badly. It is that breathe has no opinion at all.

## 1. Measured starting state (camelot-eks, 2026-08-05)

Read from the live cluster, not from memory:

```
nodes                     27          cluster CPU 2%   MEM 6%
pods                     267
total CPU requests      13.5 cores    across 267 pods
total MEM requests      22.4 Gi
```

13.5 cores of requests are spread over roughly 224 vCPU of node capacity. The
entire cluster's declared demand fits on two of its own nodes.

Where the 27 came from:

```
13  builder        karpenter   m6a.2xlarge   ON-DEMAND   CI burst, self-draining
 2  controllers    karpenter   c7i-flex.xl   spot
 2  general        karpenter   c6a.large     spot
 6  camelot-eks-nixbuild   managed nodegroup   r5.xlarge x4, m5a.2xlarge, r6i.xlarge
 2  camelot-eks-controllers managed nodegroup  c5.xlarge, t3.xlarge
 1  camelot-eks-base        managed nodegroup  m6a.2xlarge
 1  camelot-eks-system      managed nodegroup  t4g.large
```

Two distinct pathologies, and they need different fixes:

- **Burst waste.** 14 `builder` nodes appeared within 30 seconds of a
  15-job CI dispatch. Inspected mid-life, they held **zero non-DaemonSet pods**:
  the jobs had already finished. `consolidationPolicy: WhenEmpty` +
  `consolidateAfter: 5m` means a node outlives sub-minute work by 5x. The pool
  is pinned `karpenter.sh/capacity-type In ['on-demand']`, so the burst is billed
  at full rate against a stated 100%-spot CI posture.
- **Standing waste.** 10 managed-nodegroup nodes are a fixed `desired` count.
  Karpenter does not manage them, so no consolidation policy will ever reclaim
  one. `camelot-eks-nixbuild` is 6 permanently-on nodes carrying 19 real pods
  between them, one of them carrying zero.

## 2. What breathe is actually doing right now: nothing

```
memorybands   60     all writeIntent <unset>, effectiveGate = shadow
cpubands      51
storagebands   4
requestbands   0
densas         0
quinhaopools   0
breathenodepools 0
isolationbands 0
```

**115 bands, none of them writing.** Every memory band resolves to `shadow`
because `writeIntent` was never set. So breathe cannot be the cause of the
sprawl — it has carved nothing. It is equally not the cure, because the four
dimensions that bear on placement (`RequestBand`, `Densa`, `QuinhaoPool`,
`BreatheNodePool`) have **zero declared instances**.

The machinery is not missing. `breathe-nodewaste` exists and its own header
documents this exact failure, measured three days earlier:

> five `m6a.2xlarge` ON-DEMAND nodes in the builder pool sat at 19-42m CPU
> holding ARC runner pods that had claimed no job — $1.73/hr, invisible to
> Karpenter's `WhenEmpty` consolidation because the pods existed, and invisible
> to breathe because breathe had no node-level opinion.

So does `breathe-auction` (predict → optimize → auction), `breathe-admission`
(typestate-proven resource admission), `breathe-provision` (the closed
observe → predict → decide → act loop), and `Densa` itself
(`bounds: Vec<FormaBound>{floor, ceiling}`, `reserve`, `pool_capacity`,
`cost_sla_cents`). **The substrate is built and undeclared.** This plan is
mostly about declaring it, in an order where each step proves itself.

## 3. The four levers, and why all four are required

Packing is not one decision. It is four, and pulling any one alone regresses:

| lever | question | breathe primitive | declared today |
|---|---|---|---|
| **L1 requests** | what does this pod *claim*? | `RequestBand` | 0 |
| **L2 placement** | where does a *new* pod land? | admission / scoring | none |
| **L3 defragmentation** | can we *empty* a node by moving pods? | none | none |
| **L4 node lifecycle + shape** | should this node exist, and in what shape? | `nodewaste`, `Densa`, auction | 0 |

The dependency is strict and is the reason the phases below are ordered:

- **L1 is the substrate for everything else.** The scheduler and Karpenter both
  bin-pack on *requests*, never on usage. If requests are wrong, L2 places
  wrongly, L3 defragments into a node that then OOMs, and L4 buys the wrong
  shape. Right-sizing requests is not a cost optimization here — it is the
  precondition for the other three being safe.
- **L4 without L3 is a stall.** `WhenEmpty` can only reclaim a node that
  something else emptied. On camelot the burst nodes emptied themselves because
  CI pods are ephemeral; the nixbuild nodes never will, because their pods are
  long-lived and nothing moves them.
- **L3 without L1 is dangerous.** Repacking on wrong requests is how you turn
  idle waste into an outage.

## 4. What "fully data aware" has to mean

The phrase has to cash out as a named, obtainable signal set, or it is
decoration. Absorbed inputs, and where each already comes from:

**Live, available now**
- per-container CPU/memory *usage* over a window — VictoriaMetrics
  (`BREATHE_PROMETHEUS_URL=http://vmsingle-…monitoring.svc:8429`)
- per-container *requests and limits* — kube-state-metrics
- node allocatable, capacity, and current allocation — kubelet / node status
- node shape, capacity-type, zone, nodepool — Karpenter labels + nodeclaims
- pod controller kind, PDBs, `do-not-disrupt`, affinity/anti-affinity, topology
  spread, tolerations — API server
- DaemonSet footprint per node (the floor no packing can remove) — API server

**A dependency worth stating plainly:** every usage signal above flows through
lareira, and lareira was only made healthy on **2026-08-05** — node-exporter and
kube-state-metrics were both `ImagePullBackOff` for hours that day after the Zot
PVC was recreated onto an empty volume. Any window of "historical usage" older
than that is missing its node and object layers. **Phase 1 must not begin until
a full clean observation window exists**, or every band calibrates against a
hole and the p99 it learns is fiction.

**Needed, not yet wired**
- instance price by shape and capacity-type (on-demand vs spot) — required for
  `Densa.cost_sla_cents` to mean anything; `breathe-catalog/src/cost.rs` is the
  seam
- spot interruption rate by shape/zone — the risk term the auction already
  reserves a slot for
- workload *class* (interruptible CI vs control-plane) — camelot already
  expresses this as `pleme.io/workload=critical` and the `critical` nodepool

**Deliberately excluded, and why:** no forecasting of future demand in v1. The
auction's own header is honest that the cross-forma Pareto problem is deferred,
and a packing loop that acts on a prediction it cannot validate is how a
homeostasis system oscillates. v1 acts on measured present state only.

## 5. Phases

Each phase has a gate that must hold before the next begins. The gate is what
makes this a plan rather than a wish.

### Phase 0 — Trustworthy observation (prerequisite)

- Confirm a clean, unbroken metric window covering every node and workload.
- Reconcile the three views that must agree: kube-state-metrics requests, live
  pod specs, and node allocation. A disagreement here is a data bug, and finding
  it now costs nothing.
- **Gate:** N days of gap-free series for 100% of workloads. No band leaves
  shadow before this holds.

### Phase 1 — RequestBand in shadow: measure the claim-vs-use error

- Declare a `RequestBand` per workload. Shadow only, writes nothing.
- Emit, per workload, the distribution of `request / p99(usage)`.
- **This is the phase that produces the actual finding.** Today's aggregate is
  13.5 cores requested cluster-wide; that number is small enough that request
  bloat may turn out *not* to be camelot's problem, in which case L1 is cheap
  insurance rather than the win, and the plan's weight shifts to L3/L4. Let the
  data decide which, rather than assuming.
- **Gate:** every workload has a recommendation with a stated confidence, and
  the aggregate delta is quantified.

### Phase 2 — Node-level opinion: nodewaste live, read-only

- Declare `BreatheNodePool` for each Karpenter pool and each managed nodegroup
  (there are currently **zero**, so nodewaste has nothing to reason over).
- Run `breathe-nodewaste` in report mode: per node, cost per hour against real
  work delivered, with the DaemonSet floor subtracted so an "empty" node is
  correctly identified as empty.
- **Gate:** nodewaste independently re-derives the two pathologies in §1 without
  being told. If it cannot see what a human saw with `kubectl`, it is not ready
  to act.

### Phase 3 — Act on the cheap, reversible levers

Ordered by blast radius, smallest first. Each is independently revertible:

1. **Route short CI to spot; leave long Nix builds on on-demand.**

   An earlier revision of this document recommended flipping the whole
   `builder` pool to spot and called it the best saving-to-risk ratio in the
   plan. **That was wrong, and the repo already said so.** The pool's own
   comment records a CI-observed correction from 2026-07-23: a *running*
   40-minute non-checkpointable Nix build on a spot node dies on reclaim, and
   "the hardened-images vector build reclaimed twice at ~40min mid-compile,
   losing the whole build each time." Flipping the pool would reintroduce a
   failure that was already measured twice, on the very build this repo runs.

   The same comment states the intended design: *"Short/restartable builds keep
   spot (the controllers pool above)."* **That intent is not realized.** All
   four ARC runner scale sets — `camelot-builder-eks`,
   `camelot-builder-pleme-eks` (max 18), `-arm64`, `camelot-pace-ramdisk` —
   select `pleme.io/workload: nix-build`, which is the on-demand builder pool.
   There is no spot-backed runner scale set, so **every** CI job lands on
   on-demand regardless of class.

   Measured job durations, which is what makes this a routing bug rather than a
   capacity-type bug:

   ```
   vendor-mirror.yml             29 jobs   median  2.5m   max   4.0m
   image-release.yml            209 jobs   median  0.0m   max  92.0m   (bimodal)
   sql-apply-image-release.yml    6 jobs   median  0.1m   max   8.0m
   breathe-band-lint.yml          2 jobs   median  0.2m   max   0.2m
   ```

   The distribution is overwhelmingly short, with a thin tail of genuine
   long builds. A 2.5-minute mirror job currently provisions an 8-vCPU
   on-demand node that lives at least 7.5 minutes (`consolidateAfter: 5m`);
   fifteen of them at once is the 14-node burst in §1.

   So the change is a **spot-backed runner scale set for short, restartable,
   idempotent jobs**, with `runs-on:` routing per workflow — and the on-demand
   builder pool left exactly as it is, protecting the long-build case the
   incident was about.

2. **Shorten `consolidateAfter` on `builder`** to match real job duration.
3. **Right-size or scale-to-zero `camelot-eks-nixbuild`.** 6 always-on nodes for
   19 pods is the standing bleed. Managed nodegroups need an explicit desired
   count change; nothing reclaims them automatically.

- **Gate:** node count and cost drop, with zero CI job failures attributable to
  the change. Spot interruption rate is measured, not assumed.

### Phase 4 — RequestBand writes

- Promote request bands from shadow using the existing `calibrateThenWrite`
  posture and the `authorizedBy` witness. Control-plane workloads last.
- **Gate:** no OOMKill and no CPU-throttle regression across a full cycle.

### Phase 5 — Defragmentation (L3), the genuinely new capability

The only lever with no existing primitive. Design constraints, all derived from
what the cluster already enforces:

- Never evict what cannot move: respect PDBs, `karpenter.sh/do-not-disrupt`
  (camelot's ARC runner pods carry it), local storage, and single-replica
  control-plane pods.
- Target selection is *emptiable* nodes, not merely underused ones. Moving one
  pod off a node that keeps nine others has bought nothing — the reclaim only
  pays when the node reaches zero.
- Simulate before acting: prove the destination can hold the evictee at its
  *post-Phase-4* request, then act. A defrag that triggers a new node provision
  is a regression.
- **Gate:** shadow-simulate for a full cycle and report the nodes it *would*
  have emptied, checked against what actually went idle.

### Phase 6 — Shape selection (`Densa`)

- Declare `Densa` per pool: `FormaBound{floor, ceiling}` per shape, `reserve`,
  `pool_capacity`, `cost_sla_cents` — the CRD already models all of it.
- Feed real prices so `cost_sla_cents` is a constraint rather than a field.
- **Gate:** recommended shapes beat the current fixed choice on measured cost at
  equal or better headroom.

## 6. What this plan refuses to do

- **No writes before a clean observation window.** Stated once in Phase 0 and
  enforced in every later gate.
- **No prediction in v1.** Present-state only.
- **No eviction of a pod the cluster has declared immovable**, regardless of how
  much it would save.
- **No silent apply.** Every acting phase is plan-first, operator-approved
  (Edict #20), which is why Phase 3's items are ordered by revertibility.
- **No claim that breathe caused the current sprawl.** It is in shadow and has
  written nothing; the sprawl is Karpenter and managed-nodegroup lifecycle. Any
  future write-up that reverses this should re-read §2 first.

## 7. The honest one-line summary

breathe has the primitives for data-aware packing — `RequestBand`, `Densa`,
`QuinhaoPool`, `BreatheNodePool`, nodewaste, admission, auction — and **zero
declared instances of any of them**, with all 115 existing bands in shadow. The
work is not to build a packing engine. It is to declare, calibrate and promote
one that is already written, in an order where each step is provable, starting
from a metric spine that only became trustworthy today.
