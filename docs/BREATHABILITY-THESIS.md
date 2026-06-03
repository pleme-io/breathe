# The Breathability Thesis — Reinforced, Adversarially Honest

> **PRIVATE design doc.** Canonical reinforcement of the breathability idea, stress-tested across six lenses (control theory, hard/soft resources, finite contended provider, Kubernetes control-surface, Viggy systems-programming, adversarial-invalidity). Read `BREATHE.md` for mechanism; read this for *what the thesis is and is not*.

---

## 1. The thesis, stated precisely

**Breathability relocates the resource-provisioning concern from the app onto a provably-partitioned infrastructure substrate, so the app's relationship to resources collapses to "it exists."** The substrate continuously sizes each resource to live demand; the app stops carrying allocation logic, back-pressure-for-headroom, or capacity arithmetic — those move down to a layer that holds the *whole* demand vector and can therefore globally optimize (Conant–Ashby: only the layer that models all tenants can regulate the shared resource; an app provably cannot, because it cannot see its co-tenants to trade against them).

The operator's "provided-for on average, then reason about variability on top" is **not one claim — it is a decomposition**. Every provision splits into three terms:

```
provision(app, dim, t) = controlled-mean(t)      ← the band law tracks demand's slow component
                       + absorbed-variance(t)     ← static headroom/floor sized from demand variance
                       + escalated-anomaly(t)     ← the residual the controller hands back, typed
                         ───────────────────────
                         all bounded by a finite partitioned provider envelope E
```

The three terms have **different operators and different timescales**: the mean is *averaged* (a time-integral, controlled by the band law's eventual consistency); the variance is *provisioned statically from a worst-case* (a peak, never an average — it sets the floor and the buffer); the anomaly is *escalated* (the tail no controller can absorb is routed, never silently dropped). Conflating them is the central error this doc exists to prevent.

**Formal contract (one sentence).** For an elastic resource `r` with usable denominator (`capacity>0`), GIVEN (a) a fresh sample (`age ≤ max_staleness`), (b) sole field-ownership, (c) the app's true required allocation `A(t)` is band-limited with bandwidth `≪ 1/(refresh+cooldown)`, and (d) the static floor `F_r ≥ sup_window(working_set/setpoint)` covers the peak-plus-staleness-growth, THEN breathe holds `used/limit ∈ [shrink_below, grow_above]` on every window `W ≥ τ_settle` AND guarantees `used ≤ limit` at each sample instant (the `safe_min = working_set/setpoint` clamp), AND when `Σ_tenants A_i(t) > E` it returns `AtCeiling` and escalates a typed signal — never silently under-provides; it does **not** guarantee sub-sample-instant liveness, shrink cost, joint feasibility across coupled dimensions, or any tail-latency bound.

---

## 2. Validity envelope — where "provided-for on average" HOLDS

The thesis is **literally true, and a stability theorem rather than a hope**, inside a precisely bounded regime. Five independent conditions, synthesized across the control-theory, hard/soft, and finite-provider lenses:

**V1 — The plant is algebraically static, so the loop is stable for any bounded gain.** `util = working_set/limit` is a *divide*, not a dynamical system: zeroth-order, no plant time constant, no integrator, no phase lag. Instability can only enter through sampling/actuation discretization, never through the plant. *This is the single most clarifying fact* — and it is **why one model-free 80/20 band law works identically on memory, storage, cpu, ARC, and cgroups with zero per-workload tuning**: there is nothing to model, the plant is a divide. The deadband is a relay-with-hysteresis around a static plant; standard result: it cannot limit-cycle while the post-correction point lands strictly inside the deadband and the disturbance does not traverse the band within one cooldown. Proven discretely by `repeated_*_ticks_converge_into_band_and_hold` (Decision::Hold is a fixed point) — a discrete-time Lyapunov contraction, P-only + saturated ⇒ windup-immune.

**V2 — The disturbance is slower than the loop (sub-Nyquist).** "Maintained on average" is rigorous as a time-average over a window `W ≥ τ_settle`, where `τ_settle = ceil(log_a(L*/L₀)) · cooldown` (for `a=1.25`, ~3–4 ticks to double; with `cooldown=600s`, a 4× demand jump settles in ≈40 min). The band holds on average **iff** disturbance bandwidth `ω_d < 1/(2·τ_settle)` — the loop must sample-and-correct at least twice per disturbance cycle. This is exactly the regime of slow demand drift: diurnal load, customer growth, dataset accretion, cache warm-up. Here "the app simply EXISTS, continuously provided-for" is a precise, achievable contract.

**V3 — The resource is elastic, or its floor is provisioned statically from the peak.** Resources partition by recovery operator (hard/soft lens):
- **Soft** (cpu, replicas): depletion is a throttle, recovery is automatic/lossless, the OS scheduler already integrates over time. Floor `F=0`; the *whole* allocation is on-average. The thesis is complete and literal.
- **Hard-down/soft-up** (storage): growth is monotone-irreversible (CSI grow-only). The down-cliff is unrepresentable by type; the thesis holds as a one-sided headroom-provisioning promise (when to grow before the write-cliff).
- **Hard** (memory): OOM is instantaneous, lossy, controller-irreversible. The thesis holds **only above a dynamic floor** `F = working_set/setpoint` recomputed every fresh tick — which is *not* an average, it is an instantaneous conservative bound. `safety_clamp` is the shipped witness: it shrinks *up to* `max(working_set/setpoint, floor)`, never to mean/setpoint.

**V4 — The provider envelope has slack and a strict criticality order.** The L2 `nodeBudget` never-swap invariant `arc + reserve + Σ(elastic_max) + Σ(critical_min) ≤ RAM` is asserted at **eval time** — a bad partition fails `nixos-rebuild`, never reaches prod. Inside this envelope breathe breathes the elastic headroom; the floors were proven to fit before any tick ran. With one protected consumer per lever and a strict criticality total order (rio: kine CRITICAL_PLUS funded by shedding builds+atticd SHEDDABLE first), arbitration is a degenerate-but-correct priority queue — no fairness paradox.

**V5 — Two independent safety walls make per-tenant overshoot unrepresentable.** WALL 1: `safety_clamp` clamps every proposal to `[safe_min, ceiling]` *before* write — proven gain-independent across adversarial laws (GrowToMax, ShrinkToZero) by `safety_gate_contains_any_law`. WALL 2: `HostCluster::apply` independently refuses any value `> NodeEnvelopes.ceiling_for(knob)`, even if WALL 1 was skipped or the CR is mis-authored. Disjoint SSA field-managers + the `field_owners` guard mean breathe provably never fights HPA/VPA/KEDA. The gain choice is therefore a **performance knob only (speed vs jitter), never a stability knob** — exactly what you want when the operator hot-swaps PID/AIMD/predictive laws under one gate.

> **The defining duality.** Breathe's strength (one band law, no model, every dimension) and its danger (the transient kills you regardless of the average) are the *same property* — the static plant has no inertia, so it has no buffer to absorb a transient. A plant with a time constant low-pass-filters a spike and gives the controller time; here the spike hits the instantaneous constraint directly. The validity envelope is precisely the region where the disturbance is slow enough that "no buffer" never matters.

---

## 3. Invalidity boundary — where it breaks, what breathe must NEVER claim

**The single word doing all the damage in the thesis is "COMPLETELY."** The mechanism is sound; the marketing quantifier is not. The honest replacement: breathe pushes the **mean and the foreseen-variance** concern off the app onto a provably-partitioned finite envelope; the **adversarial tail** (envelope exhaustion, indivisible contention, provisioning latency, incomparable-tenant arbitration, sensor faults, coupling) is an **explicitly typed, escalation-terminating shared concern** — never silently dropped, never fully absent.

### Workload classes that must NOT adopt breathability as their provision mechanism

| Class | Why it breaks | breathe must NEVER claim | Honest mitigation |
|---|---|---|---|
| **Tail-latency / p99 interactive** | Setpoint is 0.80 of *mean*; a p99 burst needs >1.0 before the next tick grows it. The loop's reaction time (`interval+cooldown`) is orders of magnitude slower than a request's latency budget — breathing *cannot* be the latency defense. `phase=Holding @ 0.81` is true about the mean, silent about the tail the user feels. | That an in-band average implies a held latency SLO. | Static Guaranteed QoS + headroom; enroll as `ObserveOnly`; asymmetric band (grow on p99, shrink on mean) narrows the gap from "every burst" to "bursts faster than the scrape window". |
| **Hard-real-time / deadline** | No averaging is permissible — one missed deadline is a correctness failure, not a degraded average. Any breathing perturbs scheduling; a shrink may roll a pod. | That a deadline-bearing task can be breathed at all. | `criticality: HardRealTime` ⇒ band is `ObserveOnly`, renderer *refuses* to emit an AutoCorrect band (UNREPRESENTABILITY). |
| **Stateful with a performance cliff** (DB/cache — breathe's own CNPG anchor) | breathe's world-model is `(used, capacity)`; `resident_set ≠ useful_working_set`. It shrinks during a quiet window, the never-OOM proof is green, but it just evicted a hot buffer pool — next query hits disk, a throughput *cliff*. breathe degrades the DB while every invariant is green. | That a shrink whose only proof is no-OOM-at-this-sample is therefore free. | `shrink_cost` predicate per stateful kind (refuse shrink if cache-hit-ratio high / `statefulWorkingSet:true`) → `NoSafeShrink` with typed reason. |
| **Correctness invariants** (parser, 2PC, B-tree balance) | Not `(desired, observed, action-to-close-gap)` — no continuous controlled variable, no band, no gentle step. The answer is proof/unrepresentability, not a controller. | That Viggy-control is the frame for *all* systems programming. | UNREPRESENTABILITY (compile-time absence); breathe's never-OOM clamp is a **C2 ceiling** (external-world observation) mitigation — a runtime sibling of, never a substitute for, type-level absence. |

### Failure modes inside the elastic regime (tier-honest)

| Failure | Trigger | Tier today | Honest mitigation |
|---|---|---|---|
| **Burst-OOM** | demand slew `dw/dt > (grow_above−setpoint)·L/θ`; the kill outruns even one corrective step | **only-mitigated** (safety_clamp prevents *self*-inflicted OOM, not demand-driven) | Predictive/feed-forward grow on `dw/dt`; asymmetric loop (fast grow, slow shrink) |
| **Dead-time-induced oscillation** | operator picks high-gain law to chase fast disturbance; `θ`=5–15 min caps usable bandwidth at `ω_c≈1/θ` | **unguarded** (type system enforces SAFETY corollary, NOT the stability-margin clause) | Typed `BandConfig` invariant bounding max per-tick gain by `f(cooldown, scrape)` → destabilizing law *unrepresentable* |
| **Fresh-but-wrong metric** | misconfigured cAdvisor / label change reads `used≈0`; counter-reset read as drop | **only-mitigated** (freshness gate handles *absent*, not *wrong*) | Innovation-consistency gate: bounded per-tick deviance vs attested history + corroborating PSI signal → `Implausible→Hold` |
| **Correlated cluster-wide burst** | cache stampede / fleet deploy / retry storm — every elastic peaks together; the "trough to borrow from" was a lie | **only-mitigated** (L2 ceiling refuses; lowest elastic's "on-average" degrades to "provided-for when its betters idle") | Promote L2 from refuse-wall to fair-share/priority **allocator**; typed `EnvelopeSaturation` anomaly → admission control / shed |
| **Metric blackout during a rising spike** | scrape gap begins *inside* a spike; hold-last-limit freezes the band exactly when demand climbs | **mitigated** (correct for slow leak, wrong for spike-in-blackout) | Conservative grow-on-staleness for hard dims with rising recent trend |
| **Cooldown-resonant periodic disturbance** | demand period ≈ 2·cooldown; loop chases one half-cycle out of phase; settling tracker never fires (state *is* changing) | **undetected** | Autocorrelation on decision history → dither cooldown / widen deadband to break phase-lock |
| **Indivisible-resource contention** | shared device GC / NUMA bandwidth / kernel-lock — not cgroup-attributable; IOWeight can't isolate QLC write-amplification | **ceiling-bound C4/C5** (best fix is L3 physical separation: kine→pool/kine, atticd→pool) | Gate-0 question per new dimension: "cleanly partitionable at my actuator granularity?" |
| **Coupled dimensions** | memory+cpu both saturate; growing cpu fund-starves memory; N independent bands have no joint plan | **deferred to M3** (lattice-meet DETECTS all-in-band, never PRIORITIZES) | Authored L2 joint planner over a criticality lattice (see §7) |
| **Moral hazard** | teams read "the app simply exists" literally, drop their own load-shedding; all resilience concentrates in one controller (`replicas:1`) | **contract gap** | Enrollment contract: an AutoCorrect band requires the app to *also* declare its degradation posture — breathe is the **floor** of resilience, not its replacement |

> **The category error, named.** Averaging is the correct operator for the L1 efficiency/cost objective (`∫util dt` tracks the band whenever `ω_d < controller bandwidth`). It is a *category error* for the L0 liveness objective: OOM is violated *pointwise* and fails *discontinuously* — the time-average over the surrounding hour is irrelevant to the dead process. The fix is not a better average; it is to make the grow direction **predictive and asymmetric**, so the loop's only fast action is the one that buys headroom and the dead-time can only ever cost money, never the process.

---

## 4. The decomposition in depth

### Mean — controlled by the band law

The slow component of demand is tracked by the deadband AIMD. `decide(working_set, current_limit, &BandConfig) → Decision` is dimension- and unit-agnostic because `used/capacity` are opaque `u64`. The steady-state trajectory — the actual sequence of limit values over 30 days — is the **output**, never the input. It converges to the unique fixed point `util* = setpoint` in `N = ceil(log_a(L*/L₀))` actuations and holds (V1). This term is *on-average* and only on-average; it is the entire content of "the band law".

### Variance — absorbed by statically-sized buffers (the operator's "reason about variability on top")

This is the term the thesis names but the substrate has not yet fully *typed*. **The order is backwards in the naive reading**: for hard resources you reason about the variance *first* (it sets the floor and the buffer), and only the residual above the variance-derived floor is averageable. The buffer is **not** controlled — it is provisioned from a worst-case so the mean-controller has room to act before the cliff:

```
headroom(dim) = f(provision_latency θ, demand_variance σ)
              ≈ peak_working_set − mean_working_set                       (cover the spike)
              + (dw/dt)_max · θ                                            (cover what the loop can't yet see)
              + safety_margin                                             (the ~20% L2 slack)
```

For **soft** dims, `σ` costs only latency ⇒ buffer may be 0. For **hard** dims, the buffer is the floor: `F_d ≥ working_set/setpoint + growth_rate·max_staleness`. The staleness term is the named gap — today the floor is `working_set/setpoint` on the last fresh sample, which is blind between scrapes. **The node-level analog**: `Σ_d F_d + Σ_elastic max(h_d) ≤ Capacity` must be a *compile-time provable inequality* — the shipped never-swap assertion is exactly this. "Reason about the variability of the providing layer" *is* this node-level sum: the buffer that absorbs correlated bursts is the L2 slack, and when the slack is exhausted the variance term overflows into the anomaly term.

### Anomaly — escalated by the taxonomy + ladder

The residual variance no buffer absorbs is **routed, never dropped**. An anomaly is *optimally* handled iff it lands on the **lowest ladder rung whose mechanism keeps loss bounded below the next rung's intervention cost** — a two-term cost (residual utilization loss + actuation/disruption cost), not the allocation itself:

| Rung | Trigger | Mechanism | LOA |
|---|---|---|---|
| **Absorb** | in-band fluctuation | deadband hysteresis — zero actuation, zero allostatic load | autonomous |
| **Lean** | out-of-band, recoverable | gentle AIMD ease-off of lowest-criticality elastic only — graceful degradation, *not* allocation-optimal | autonomous |
| **Back-off** | repeated correction not converging | widen deadband / extend cooldown — break thrash | autonomous |
| **Panic** | loss signal: `pswpin>0` (true eviction floor), ceiling/floor breaker trip | circuit-break + load-shed by criticality, **never touch kine** | autonomous |
| **Alert** | MAPE-K Plan space exhausted (no convergence after shedding all SHEDDABLE for `>escalate_after_secs`) | signed AnomalyChain entry + ntfy + freeze carving + hand to operator | human-on-the-loop (Sheridan–Verplank safe terminus) |

`classify` is pure and **monotone** (`classify_monotonic` proptest — a more-severe Decision never routes to a lower tier). "Optimally handling all anomalies" is therefore **bounded-optimal-modulo-escalation**: optimal *within the plan space*, with the `Alert` rung being the correct, honest terminus when the plan space is provably exhausted — the runbook returning is a *feature* (the safe LOA terminus), not an embarrassment. Many anomalies (genuinely-coupled OOM-while-cpu-saturated) have *only* escalation as their optimum, and the ladder reaches it.

---

## 5. The Kubernetes control lattice

The breathability contract is `A(w)` invariant under control: `d(restart)/d(carve) = 0`. **breathe has proven this — but only on the host plane, and that proof pinpoints why the k8s plane fails.** On the host, breathe writes a live cgroup/sysfs value and the daemon keeps running (`d(restart)/d(carve)=0`). On k8s, breathe writes the *pod template*, rolling the workload (`d(restart)/d(carve)=1`) — same controller, same band law, same SSA mechanism; the only difference is the actuator targets desired-state, not the live container.

| Surface | What / disruption | Coverage | Ideal |
|---|---|---|---|
| **Host cgroup/sysfs** (ARC `zfs_arc_max`, unit `MemoryHigh`) | truly in-place, zero restart; seconds | **DRIVEN** (live on rio, 4 dims, ceiling-refusal within nodeBudget) | extend to more units (atticd, tend, kubelet), host `cpu.max`, host `io.max` — the k8s plane should match it |
| **PVC online resize** (CSI ExpandVolume) | in-place; minutes | **DRIVEN** (grow-only CronJob) | cooldown anchor + ceiling breaker at StorageClass/quota max |
| **Pod requests/limits + QoS** | template-write = **restart**; minutes | DRIVEN but roll-bound | preserve QoS as a typed invariant; carve requests+limits together |
| **Pod resize subresource** (`pods/resize`, GA 1.33) | **in-place** (cpu/mem-up zero-restart; mem-down `RestartContainer`-gated, QoS-affecting) | **NOT-YET** (`apply` always patches template; `CpuDescriptor` *falsely* claims `PartialProgress`) | **★ THE MISSING ISOMORPHISM (§ below)** |
| HPA replicas | scale = new/terminated pods; tens of s | NOT-DRIVEN (`ObserveOnly`) | keep abstention; tune HPA `targetUtilization` on a disjoint field |
| VPA | Auto evicts; minutes–hours | NOT-DRIVEN (breathe *is* a VPA-class controller) | supersede on enrolled workloads; observe-and-defer elsewhere |
| PriorityClass + preemption | preemption = eviction; seconds | NOT-DRIVEN | map critical/elastic → PriorityClasses so the scheduler sheds elastic first |
| PodDisruptionBudget | in-place gate; continuous | NOT-DRIVEN | when breathe must roll, respect/author a PDB so a carve-roll never breaches availability |
| Kubelet eviction thresholds (`evictionHard/Soft`) | pod kill; seconds (true floor) | NOT-DRIVEN (reads `pswpin` instead) | treat `evictionHard` as the hard floor, ceiling `C` below it — carve down before kubelet kills |
| Node allocatable / kube/system-reserved | kubelet restart; hours | PARTIAL (drives the host cgroup that *is* system-reserved, not kubelet's declaration) | make kubelet `system-reserved` track breathe's live host envelope |
| LimitRange / ResourceQuota | in-place admission; hours–days | NOT-DRIVEN | read as hard outer clamp on carve range / namespace analog of `C(node,d)`; carve-over-quota refused-typed or triggers envelope growth |
| CPU/topology manager (NUMA, CCD territories) | kubelet+pod restart; hours | host-side DRIVEN (`AllowedCPUs`), k8s-side NOT-DRIVEN | extend the silicon-territory model into the kubelet for CCD-aligned exclusive cores |
| Cluster Autoscaler / Karpenter / NodePool (**grow the envelope**) | in-place for existing pods; minutes | NOT-YET (`BreatheNodePool` exists, growth M2+) | the ceiling escape valve — drive NodePool limits as a slow outer band, recursing the band law to cluster scale |

**★ Highest-leverage next surface — in-place pod resize (`pods/resize`).** It is **not a feature breathe could add; it is the missing isomorphism** that makes the k8s plane structurally identical to the already-proven host plane. The host actuator targets the live cgroup; `pods/resize` makes the k8s carve hit the running container's cgroup directly, exactly as the host carve hits the running unit's cgroup. It does not *extend* breathe — it *completes the symmetry breathe was built around*. Only after it lands is the operator's thesis true on Kubernetes, not just on the host. Concretely: a `PodResizeSubresource` layout + `Cluster::resize()` PATCHing the per-pod resize endpoint, looping the owner's pods, reading `resizePolicy`, with a QoS-preservation guard (a Guaranteed pod carved in-place must not silently drop to Burstable). Until it lands, breathe is *worse* than a static limit for any sub-minute-oscillating Deployment (restart-thrash). **Cluster-scale corollary**: at the L2 ceiling, the band stays satisfiable only with a *second* in-place lever on the envelope itself (NodePool growth) — without it breathe is per-node, not global, homeostatic.

---

## 6. Breathability as systems programming (the Viggy framing)

Classical systems programming **hand-writes the steady-state trajectory**: it enumerates the desired path (allocate N, on pressure 1.25N, on OOM page out, on recovery shrink) as imperative code. Viggy/breathe systems programming **inverts this**: the programmer authors a *triple* and the trajectory is derived.

```
CLASSICAL:  author τ : t → A(t)              (the steady-state path IS the program)
VIGGY:      author (κ, σ, E), derive τ = (reconcile_one)^∞
              κ = ControlLaw::propose         (direction + magnitude only)
              σ = classify : Decision → Severity   (the anomaly taxonomy)
              E = escalation ladder           (Absorb < Lean < Back-off < Panic < Alert)
```

- **A band IS a `(defpromessa)`.** A `MemoryBand` CR declaring 80/20 over target X *is* the Viggy promise "X holds util in [0.70,0.85] for window W". M3 `BreathePromise` makes it literal — reconciled by the PromessaLattice meet (in-band ⟺ every dimension in-band).
- **The loop IS the Seven-Beat Convergence Tick.** `reconcile_one` = Observe (`provider.observe → used/capacity/owners`) → Diff (single-writer guard, then `decide` — the Decision *is* the drift) → Classify (severity) → Decide (RemediationPolicy route) → Act (`provider.assign`, one atomic SSA Patch) → Attest (OutcomeChain BLAKE3+Ed25519 over `{target,dimension,used,capacity,from,to,verdict,epoch}`) → Tick (level-triggered requeue, never `await_change`). breathe-core *owns* the loop (composes it from `promessa-types::TargetController`'s pure diff/classify/decide + three I/O legs), it does not inherit.
- **Provision becomes a continuously-attested theorem.** The moment OutcomeChain signs state-changes + a periodic in-band heartbeat, `kensa verify outcome-chain --target X --dimension memory --window [t0,t1]` *proves* the band held over the window to an external party holding only the public key. The operator's "eventual-consistency fact that can be built upon as long as it's maintained on average" is then mechanically checkable — a falsifiable theorem, not an asserted slogan.

> **The deepest restatement: prove the safe set once, derive the trajectory forever.** In the Viggy inversion the **safety proof** is what becomes reusable and the **steady-state path** is what becomes disposable — the exact inverse of classical systems programming, where the path is the carefully-authored artifact and safety is bolted on per-case. `safety_gate_contains_any_law` proves the never-OOM/never-overshoot invariant *once* over the cross-product of every utilization state × seven control laws (including adversarial ones proposing `u64::MAX` and 0). That single proof is the entire reusable substrate; which law, which dimension, which trajectory are all swappable *because the proof does not depend on them*. The program shrinks to (the safe-set proof) + (a law that stays inside it) + (a taxonomy of how to leave it gracefully) + (a ladder back in). **And this immediately explains where the frame breaks**: exactly where you cannot prove a safe set once and reuse it — coupled dimensions (the safe set is a joint region no single band's proof covers), fresh-but-wrong sensors (the proof is conditioned on an input it cannot validate), and correctness concerns (the "safe set" is a single point reachable only by construction — UNREPRESENTABILITY's job, not control's). **Scope line, stated for the doc:** Viggy-control is the right frame for resource/capacity/congestion/posture concerns (continuous controlled variable + monotone gap-closing actuator + truthful sensor); UNREPRESENTABILITY is the right frame for correctness concerns. They are **siblings, not one swallowing the other** — and the controller's own authored parts (law/classifier/ladder) should themselves be unrepresentability-hardened.

---

## 7. What to reinforce next in breathe — prioritized backlog

Ordered by leverage (biggest invalidity-gap closure first). Each closes a `where_invalid` row and/or extends the control lattice; deduped across all six lenses.

**P0 — In-place pod resize (`pods/resize`, GA 1.33).** The missing isomorphism (§5). Add `PodResizeSubresource` + `Cluster::resize()` patching the per-pod endpoint with a `resizePolicy`/QoS guard. *Until this lands the k8s plane is not breathable — it is restart-thrashing.* Highest single leverage; it completes the symmetry rather than extending the surface.

**P1 — Predictive, asymmetric grow (close the burst-OOM gap).** Split the single setpoint into a **fast asymmetric loop** (grows act on `dw/dt` via a feed-forward term — pre-grow to cover `r·θ` of headroom — and are fast) and a **slow symmetric loop** (shrinks stay conservative). The kill is one-sided: you can never be too generous on grow, only too slow. Add a `working_set_rate` argument to `ControlLaw::propose`. Closes the L0-liveness category error directly.

**P2 — Sanity/innovation gate before AutoCorrect on irreversible dims.** Today only `TickPlan::Stale` (freshness) gates; a fresh-but-wrong `used≈0` defeats the never-OOM clamp (it trusts the same `working_set`) and runs a confident Shrink-to-floor — catastrophic for grow-only storage (permanent money leak). Add a corroborating signal (PSI alongside working_set) + a bounded per-tick deviance vs the OutcomeChain's last attested `used` → `Implausible→Hold` + AnomalyChain. Upgrades the controller from "safe only when the sensor is truthful" to "safe under a bounded class of sensor faults" — the precondition the whole inversion silently assumes.

**P3 — Authored L2 joint planner over a criticality lattice (coupling).** Be explicit: per-dimension steady-state is *derived*; cross-dimension arbitration under contention is *authored* (the honest exception to "steady-state is derived"). Ship `breathe-capacity`'s DRF allocator as a node-scope supervisor that, on memory pressure, executes the typed order (shrink ARC → shed elastics in criticality order → never touch critical floors) as a **single joint TickPlan**, emitting an *envelope* per dimension; the L1 band breathes within it. Soft dims tick independently; hard dims under pressure route through the joint planner. Source per-workload preemption **value** from the promessa (a tenant's value = the business SLA it backs) so preemption order is *derived*, not hand-authored.

**P4 — Type the dead-time / gain stability margin.** `safety_clamp` proves the SAFETY corollary but nothing proves the STABILITY-MARGIN clause — an operator can author `ProportionalLaw{gain:1.0}` or too-large `grow_factor` that oscillates against loop dead-time. Add a `Refined<f64>` `BandConfig` invariant bounding max effective per-tick gain by `f(cooldown, scrape_interval)`, checked at parse time, so a destabilizing law is *unrepresentable* rather than merely unwise.

**P5 — Typed `ResourceClass` + `DeadlineGated` markers in the catalog.** Split recovery-class from directionality (memory and cpu are both Bidirectional, but memory is Hard and cpu is Soft). Add `ResourceClass {Soft | HardDownSoftUp | Hard}` with a CATALOG-REFLECTION invariant: Soft ⇒ floor is anti-flap-only; Hard ⇒ `floor_bytes` is peak-derived AND the dim appears in the L2 never-swap sum. Add `DeadlineGated` so a soft dim on a liveness/lease critical path (kine's CPU/IO) *inherits* a hard floor from the deadline — "CPU is soft UNLESS it gates a deadline." Makes "is this floor sound?" a compile-time question.

**P6 — Typed `criticality` + tail-aware observation (latency safety).** Add a required `BandSpec.criticality {Elastic | LatencyCritical | HardRealTime}`; LatencyCritical/HardRealTime ⇒ `ObserveOnly`, renderer refuses an AutoCorrect band (UNREPRESENTABILITY). Extend `observe()` to project a recent p99/max (`PodMetricsMax` already exists): grow on p99, shrink on mean.

**P7 — Promote L2 from refuse-wall to fair-share allocator; eval-time critical-tier invariant.** When `Σ per-target targets > node capacity`, run a fair-share/priority projection (the node-level safe-min) so correlated-burst contention is resolved by explicit policy, not first-writer-wins. Add a **separate** eval-time assertion `Σ critical_min + reserve ≤ |C| − safety_headroom` so the most dangerous failure (critical tier alone overflows the envelope) is caught at `nixos-rebuild`, not discovered live at the alert rung. Emit a typed `EnvelopeSaturation` anomaly (≥N bands AtCeiling in one window) that escalates to admission control / shedding rather than silently holding.

**P8 — Provisioning-latency-aware placement (L3 cost envelope).** Tag each dimension with `relief_latency` and each tenant with `slo_window`; a tenant whose `slo_window < relief_latency` MUST be backed by a warm-reserved (instant) envelope, never a spot-acquire one. Makes "continuously provided-for" true-by-construction for latency-critical tenants and honestly "eventually provided-for" for batch tenants.

**P9 — Anti-resonance guard + cost-ceiling for ratchet dims.** Autocorrelation on decision history detects cooldown-resonant periodic disturbance → dither cooldown / widen deadband to break phase-lock (the settling tracker can't see it — state *is* changing). Give `HardDownSoftUp` a `cost_ceiling` + a distinct `RatchetGrow` decision (not symmetric Grow) so storage's irreversible spend is a first-class money-floor in the attestation surface.

**P10 — Window-level promise attestation + typed `ProvisionGuarantee` contract.** Ship the M3 `BreathePromise` PromessaLattice meet + a periodic in-band heartbeat so "target X held 80/20 across ALL enrolled dimensions for window W" is a single `kensa verify --window` theorem (the per-tick chain only proves individual carves). Define a per-tenant `ProvisionGuarantee { setpoint, envelope, on_exhaustion: Backpressure | Reject | Escalate }` so "the concern is handed back, never dropped" is a *typed* contract — the app then drops ad-hoc resilience for the in-envelope case and codes exactly one typed degraded-mode for the out-of-envelope case. State the Nyquist limit (`1/(refresh+cooldown)`) in the promise object so an auditor never reads a mean-utilization promise as a latency SLA.

**Doc discipline (land in `BREATHE.md` verbatim).** Retire "COMPLETELY". State the qualified thesis: *"breathe provides-for the ELASTIC HEADROOM `h_d` on average; it provisions the FLOOR `F_d` from the peak. For soft resources `F_d=0` and the whole allocation is on-average; for hard resources only `h_d = a_d − F_d` is on-average, `F_d` is reactive-conservative and statically summed under the never-swap invariant. on-average is a property of headroom, never of a floor. The adversarial tail is an explicitly typed, escalation-terminating shared concern — never silently dropped."* This makes the thesis falsifiable and forecloses the dangerous reading where a hard resource is held at a green mean while its variance touches the cliff.