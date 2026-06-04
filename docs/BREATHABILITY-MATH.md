# The Mathematical Foundation of Breathability

> **PRIVATE design doc.** The formal companion to
> [`BREATHABILITY-THESIS.md`](./BREATHABILITY-THESIS.md) (what the thesis is
> and is not) and [`BREATHE.md`](./BREATHE.md) (the mechanism). This document
> states the *law* the entire breathe resource-ether obeys, as precise
> mathematical objects with the `breathe-control` / `breathe-core` code symbols
> that implement them, and grades every claim by rigor tier. **An oversold
> theorem is worse than a flagged gap**; where the math does not close, this
> doc says so in the same sentence.

> **Provenance.** Formalized by an 8-lens parallel pass (control theory, invariant
> sets, pointwise-vs-average, stochastic/queueing, EVT, anomaly escalation,
> hierarchical composition) and then **adversarially soundness-audited** against
> the live `breathe-control` source + numerical simulation of the band law's
> actual fixed point (2026-06-04). The audit caught one false `proved` theorem
> (the §3.3 fixed point is the deadband *interval*, not the `setpoint` point —
> grow→`u*≈0.742`, shrink→`u*≈0.712`), a nonexistent test citation (`κ`), and
> three smuggled assumptions (queueing-knee scope, discrete-step swing, EVT
> non-stationarity); all are corrected inline and tagged `(soundness audit
> 2026-06-04)`. The pointwise-vs-average spine (§2) and the clamp safety result
> (§2.3, code-verified) passed unchanged.

---

## §0. The destination, stated once

> **Breathing provision is the forward-invariant deadband of a
> relay-with-hysteresis controller acting on an algebraically-static plant,
> defended pointwise by a universal safety clamp, lifted by static-partition
> forward-invariance into a hierarchical control-invariant set, and bounded by a
> tail-quantile envelope — so that "the app simply exists, continuously
> provided-for" is a falsifiable stability theorem with a checkable precondition,
> not a hope.**

*(The attractor is the deadband interval `[shrink_below, grow_above]`, not the
point `setpoint` — §3.3. "Fixed point" everywhere in this doc means "the band the
limit settles into and Holds," a region of positive measure, never a single
utilization value.)*

Everything below derives this statement, term by term, and then says exactly
where it stops being a theorem and becomes an engineering heuristic. The
single most clarifying fact, carried throughout: **the plant is a divide,**
`u = w/L` **— a zeroth-order algebraic map, not a differential equation.**
Stability therefore never depends on plant dynamics (there are none); it
depends only on the discretization (dead-time, gain, sampling). One model-free
band law is correct on memory, storage, cpu, ARC, and cgroups *because there
is nothing to model.*

**The three pillars, and the one bright line between them.** Provision splits
into three terms (§1) acting on three timescales; the controller (§3) owns the
*mean* over an averaging window; the envelope (§4–§5) absorbs the *variance*
pointwise from a peak; the ladder (§6) routes the *anomaly* the first two
cannot. The bright line — restated from `UNREPRESENTABILITY.md §II` and never
rounded up in this doc — is: **a `Result::Err` is mitigation; a compile error
or an absent code path is unrepresentability.** The never-OOM guarantee is a
*pointwise clamp* (§2), not an absence; it is graded `only-mitigated` for
demand-driven OOM and we say so every time it appears.

---

## §1. The decomposition, stated formally

### §1.1 The three-term object

For an enrolled workload `i`, an elastic dimension `dim`, and time `t`, the
**provision** is the limit `L_i(dim, t) ∈ ℝ₊` the controller writes. It
decomposes:

```
L_i(dim, t)  =  M_i(dim, t)  +  H_i(dim)  +  𝟙_anom(t) · A_i(dim, t)
                ─────────       ────────      ──────────────────────
                controlled       absorbed-      escalated-anomaly
                  mean           variance        (residual, routed)
```

with three mathematically distinct objects, three operators, three timescales:

| Term | Object | Operator | Timescale | Code symbol |
|---|---|---|---|---|
| `M_i(dim,t)` | controlled mean: the band-law trajectory, on-average inside `[setpoint·C, grow_above·C]` over any window `W ≥ τ_settle` | **time-average** (a `∫` over `τ_settle`) | minutes–days | `decide()` / `BandLaw::propose` (`lib.rs:289`, `:269`) |
| `H_i(dim) = F_d` | absorbed variance: static headroom + floor, provisioned from a **peak**, not an average | **supremum** (a peak over a window) | set once per fresh sample (reactive `safe_min`) / once per eval (`floor_bytes`) | `safety_clamp` `safe_min` (`lib.rs:200`); `BandConfig.floor_bytes` (`lib.rs:41`) |
| `A_i(dim,t)` | escalated anomaly: the tail no buffer absorbs, **routed never dropped** | **escalation** (a typed signal) | event-driven | `Decision::{AtCeiling, NoSafeShrink}`; future `AnomalyChain` |

> **The central error this decomposition exists to prevent: conflating the
> three operators.** The mean is *averaged*. The variance is *peaked* (an
> instantaneous worst-case, never a mean — `safe_min = ⌈w/setpoint⌉` is
> recomputed from the **current** sample, not from `E[w]`). The anomaly is
> *escalated*. Using `average` where the object demands `sup` is the precise
> category error of §2.

### §1.2 The three components are not (yet) proved orthogonal

The decomposition is a clean **conceptual partition** that matches the code:
`BandLaw` moves `M`, `floor_bytes`/`safe_min` set `H`, `AtCeiling` routes `A`.
But the claim that every byte of provision lands in **exactly one** term (mutual
exclusivity, exhaustiveness, unique attribution) is **not** a proved lemma — it
is a lattice-theoretic obligation deferred to the attestation layer (§9, claim
**C-DECOMP**). The honest reading: the three *mechanisms* are disjoint in code;
their *measures* are not yet proved to sum without double-counting.

---

## §2. The pointwise-vs-average theorem (the conceptual spine)

### §2.1 Statement

Let `D(t) ∈ ℝ₊` be instantaneous demand (working set) and `L(t) ∈ ℝ₊` the
limit. Define utilization `u(t) := D(t)/L(t)`.

> **Theorem (Liveness is an L∞ constraint).** The OOM-safety constraint is the
> supremum-norm, pointwise, almost-sure inequality
> ```
>   sup_{t∈[0,T]}  D(t)  ≤  L(t)         (L∞ — the safety constraint)
> ```
> and is **strictly stronger** than the time-averaged inequality
> ```
>   (1/T) ∫₀ᵀ D(τ) dτ  ≤  L            (L¹ — insufficient).
> ```
> For any demand with `σ²_D > 0`, mean-provisioning `L := E[D]` leaves
> `P(D > L) = P(D > μ_D) > 0`; for `D ~ N(μ,σ²)`, exactly `0.5`. A single
> instant `t*` with `D(t*) > L(t*)` is fatal and **irreversible** (the kernel
> sends `SIGKILL`); no time-average over the surrounding interval un-kills the
> process.

### §2.2 The per-dimension partition (the quotable line)

> **OOM is violated pointwise and fails discontinuously; cost is violated
> on-average and fails continuously. They demand different norms.**

This bifurcates the dimension catalog by *recovery operator*, not by units:

| Class | Kernel semantics | Constraint norm | Floor | breathe stance |
|---|---|---|---|---|
| **Hard** (memory) | `memory.max` is a wall: alloc fails → OOM-kill, instantaneous + lossy + controller-irreversible | **L∞ (pointwise)** | `F_d ≥ sup_W(w/setpoint)`, *not* an average | clamp `safe_min`; `RestartConditional` shrink |
| **Hard-down/soft-up** (storage) | grow is monotone-irreversible (CSI grow-only); the down-cliff is unrepresentable | **L∞ one-sided** | peak-derived, grow-only | `GrowOnly` directionality |
| **Soft** (cpu, replicas) | `cpu.max` is a throttle: the scheduler **integrates over time**, work is deferred not lost | **L¹ (average) suffices** | `F_d = 0`; whole allocation on-average | `Bidirectional`, `PartialProgress` |

This is why the same `decide()` is unit-agnostic (it operates on bare `u64`
scalars — the plant is a divide), yet the *consequence* of being wrong differs:
a soft-resource shrink costs latency; a hard-resource shrink without the
pointwise clamp costs the process.

### §2.3 What the clamp actually guarantees (tier-honest)

The clamp closes **self-inflicted** OOM, not **demand-driven** OOM:

> **Lemma (`safety_clamp` is pointwise-safe for the observed sample).**
> Given a `Shrink` proposal to `raw < L`, with
> `safe_min := ⌈D(t)/setpoint⌉`, the clamp returns
> `to := max(raw, safe_min, F_d)`. Then post-shrink utilization
> `u' = D(t)/to ≤ D(t)/safe_min = D(t)/⌈D(t)/setpoint⌉ ≤ setpoint < 1`,
> so `D(t) ≤ to` **at the instant of this sample**. *(Proved — `lib.rs:199–206`;
> `safety_clamp_lifts_shrink_to_safe_min` `lib.rs:1071`.)*

The guarantee is **conditioned on `D(t)` being fresh and truthful** and holds
**for this sample**. A spike between samples (`D(t+δ) > L`) is **not** prevented
by the clamp — that is the burst-OOM gap (`only-mitigated`, §8). Per the bright
line: this is a runtime clamp, *mitigation*, never absence. We never write
"never-OOM" without this qualifier.

---

## §3. The controller

### §3.1 The plant and the relay

The plant is the algebraic map `u = w/L`. There is no state, no integrator, no
pole, no phase lag. The controller is a **relay with hysteresis** (a deadband
AIMD) over that static plant:

```
BandLaw::propose(w, L, cfg):
    u := w / L
    if u > grow_above   →  Target(⌈L · grow_factor⌉)       # multiplicative grow, a=1.25
    if u < shrink_below  →  Target(⌊L · shrink_factor⌋)      # multiplicative shrink, b=0.90
    else                 →  Hold
```

with `shrink_below ≤ setpoint ≤ grow_above` (default `0.70 ≤ 0.80 ≤ 0.85`;
`BandConfig::validate` `lib.rs:98`). Every proposal — from `BandLaw` or any
adversarial law — passes through `safety_clamp` before becoming a `Decision`.

### §3.2 Chatter elimination (the determinism tie)

The deadband width `Δ := grow_above − shrink_below = 0.15` is the
**relay-with-hysteresis anti-chatter mechanism** (Khalil 2002; Hellerstein et
al. 2004). A pure relay (`Δ=0`) at the setpoint limit-cycles on any sensor
noise; the hysteresis band makes `Hold` a *fixed region* rather than a fixed
point of measure zero. The correctness requirement is:

```
   Δ  >  2 · σ_noise  +  δ_step          (deadband exceeds noise AND the discrete-step swing)
```

This is the mathematical content of the chatter/determinism fix: **`Hold` must
be the unique attractor inside the band, and a tick that lands in the band must
deterministically re-emit `Hold` (`dL = 0`)** so the limit stops moving. When
`Δ` is too small the band flaps — the trajectory is non-deterministic and the
convergence proof (§3.3) is void. **Two** terms must fit inside `Δ`, not one:

- `2·σ_noise` — peak-to-peak metric noise (the usual relay-hysteresis condition).
- `δ_step` — the **discrete multiplicative-step swing**. One grow step changes
  `u` by factor `1/a ≈ 0.8`, i.e. `Δu` up to ~`0.15` in a single tick —
  *comparable to the deadband width `Δ = 0.15` itself*. If a step from just
  outside the band overshoots to just outside the opposite edge, the law steps
  *through* the band and oscillates. The default `shrink_factor = 0.90` is gentle
  enough (the shrink witness takes 15 small steps) to land inside; a coarser
  factor would chatter. This is a real, currently-undeclared precondition:
  **the deadband must dominate the step granularity, not only sensor noise.**

`Δ > 2σ_noise + δ_step` is a **calibration precondition**, not a typed invariant
(`σ_noise` unmodeled — §8, P2; `δ_step` derivable from `grow_factor`/`shrink_factor`
but not yet checked by `validate` — §8, P4).

### §3.3 Convergence

> **Theorem (Finite convergence into the held deadband).** For constant fresh
> `(D, C)`, iterating `decide` from any `L₀ ∈ [F_d, C_d]` reaches the
> forward-invariant deadband `[shrink_below, grow_above]` in
> ```
>   grow side  (u₀ > grow_above):  N = ⌈ log_a   ( u₀ / grow_above  ) ⌉   a = grow_factor  (≈1.25)
>   shrink side (u₀ < shrink_below): N = ⌈ log_(1/b)( shrink_below / u₀ ) ⌉   b = shrink_factor (≈0.90)
> ```
> steps, after which `u* ∈ [shrink_below, grow_above]` so neither edge triggers
> and `Decision = Hold` (`dL = 0`) persists. *(Proved — the multiplicative step
> is a contraction toward the band; witnesses `repeated_grow_ticks_converge_into_band`
> (asserts `util ≤ grow_above`), `repeated_shrink_ticks_converge_into_band_and_hold`.)*

> **⚠ The fixed point is the deadband INTERVAL `[shrink_below, grow_above]`, NOT
> the point `setpoint`.** The default `BandLaw`'s multiplicative step stops the
> instant `u` re-enters the band, landing wherever the step first crosses the
> edge — numerically `u* ≈ 0.742` on grow and `u* ≈ 0.712` on shrink for the
> default `0.70/0.80/0.85` config, *generically not* `setpoint = 0.80`. The
> setpoint is the target only of (a) the safety clamp's `safe_min = ⌈D/setpoint⌉`,
> which **binds only** when a shrink would otherwise overshoot below it (at the
> default fixed point it does not bind — converges at `843Mi`, `safe_min = 750Mi`),
> and (b) the `ProportionalLaw` (`lib.rs:293`). Neither governs the default law's
> fixed point. **The attractor is a band, not a setpoint** — every downstream
> "at setpoint" phrasing (§5.3, §7.1, §7.3) reads "in-band." (Soundness audit
> 2026-06-04: the earlier "`u* ≈ setpoint`, `L* = ⌈D/setpoint⌉`" statement was a
> false `proved` theorem — corrected here.)

Settling time (grow side, the safety-critical direction):
`τ_settle = N · T_cooldown = ⌈log_a(u₀/grow_above)⌉ · T_cooldown` (≈ 40 min for a
4× demand jump at `a=1.25`, `T_cooldown=600s`).

### §3.4 Assumptions of the convergence argument

1. **Static plant (V1).** The plant is `u = w/L`; a relay-with-hysteresis
   around a static nonlinearity with saturation cannot limit-cycle if the
   post-correction point lands strictly inside the deadband and the disturbance
   does not traverse the band within one cooldown. *(Standard result —
   Hellerstein/Khalil; Nyquist margin = ∞ on the zero-state plant.)*
2. **Sub-Nyquist disturbance (V2).** `ω_d < 1/(2·τ_settle)` — the loop samples
   and corrects ≥ 2× per disturbance cycle, else aliasing/oscillation.
   *(Standard result — discrete-time sampling stability.)*
3. **Authoring-sane gain.** `grow_factor > 1`, `shrink_factor ∈ (0,1)` — checked
   by `validate`. The **stability margin** under dead-time (`P4`) is *not*
   checked: a high-gain/low-cooldown config that passes `validate` can still
   resonate (§8, P4).
4. **Asymmetry.** AIMD with `b` harder than `a` (back-off ≥ growth) gives
   efficiency + fairness on shared bottlenecks (Chiu & Jain 1989, Prop. 3).
   The asymmetric **predictive** variant (`PredictiveGrow`, `lib.rs:371`)
   pre-grows on `dD/dt` so the only fast action buys headroom — the dead-time
   can then cost money, never the process.

---

## §4. The validity envelope (the control-invariant set)

### §4.1 The robust forward-invariant box

Let the per-dimension envelope be the box `E_d = [F_d, C_d]` and the joint
envelope `E = ∏_d E_d ⊆ ℝ₊^D`. Define the **safe set**

```
   S  :=  { state :  ∀d,  L_d ∈ [F_d, C_d]   ∧   u_d ∈ [shrink_below, grow_above] }.
```

> **Theorem (Forward invariance = "always achieving state").** Under the band
> law with `safety_clamp` and bounded sub-Nyquist disturbance, `S` is
> forward-invariant for a single dimension in isolation: `state(t) ∈ S ⇒
> state(t') ∈ S ∀t' > t`. *(Proved per-dimension — the clamp maps in-band
> utilization to in-band utilization; `Hold` is the attractor; `safety_clamp`
> caps grow at `C_d` and lifts shrink to `safe_min ≤ F_d ≤ L`. Witness:
> `safety_gate_contains_any_law`, `lib.rs:1086`, over adversarial laws ×
> random observations.)*

"Always achieving state" is *precisely* forward-invariance of `S`: once in the
band, the system is in the band for all future time. It is **not** a claim about
sub-sample instants (§2) and **not** a claim about the joint product under
contention (§7, gap).

### §4.2 Floor-from-peak as a return level

The floor is not a comfort constant — it is a **return level**. For demand with
a GEV/Pareto tail `F_GEV(x) = exp(−(1+ξ(x−μ)/σ)^{−1/ξ})`, the `α`-return level
over horizon `W` is

```
   L_α(W)  =  μ + σ · Λ_α(ξ)        (ξ>0 heavy-tailed: unbounded growth;
                                      ξ=0 Gumbel: log-linear in W),
```

and the floor is provisioned as `F_d = ⌈ L_α(W) / setpoint ⌉`. Then:

> **Lemma (Return-level floor contains the envelope).** If `D(t) ≤ L_α(W)`,
> then `safe_min(t) = ⌈D(t)/setpoint⌉ ≤ ⌈L_α(W)/setpoint⌉ = F_d`, so every
> shrink lands at-or-above `safe_min`, blocking the OOM cliff for the sample
> **and** preventing slow drift below the floor. *(Proved as algebra; the EVT
> *fit* that produces `L_α` is offline and unshipped — §8.)*

The Gaussian myth `μ + kσ` is the special case `ξ=0`; real workloads show
`ξ ≈ 0.3–0.7` (cache misses, batch runtimes), where `μ+kσ` under-provisions
catastrophically.

> **⚠ Stationarity precondition (soundness audit 2026-06-04).** A GEV/GPD
> return-level fit assumes the block maxima are drawn from a **stationary** (and,
> for the simplest estimators, iid-across-blocks) process. Real demand is
> **non-stationary** — §5.3 itself names diurnal drift and dataset accretion. So
> `L_α(W)` is valid only **block-wise** (within a quasi-stationary window) and
> must be re-fit per regime, or the GEV needs a **time-varying location parameter**
> `μ(t)` (de-trended maxima). A single global return level over a diurnal process
> systematically under-estimates the daytime peak and over-estimates the night
> floor. Added as V-row in §8 (the EVT fit is unshipped, so this is a design
> constraint on the fit, not a current defect).

### §4.3 The checkable invariance inequality (never-swap)

Forward invariance of `S` is only meaningful if `S` *fits in the hardware*.
That is the **never-swap invariant**, asserted at eval time (`nixos-rebuild`),
never at runtime:

```
   Σ_d F_d  +  reserve  +  Σ_elastic max(h_d)   ≤   node_capacity      (V4)
```

This is a **compile-time provable inequality** (a `nixos-rebuild` failure, not a
production incident). It guarantees the floors were proven to fit before any
tick ran. Its weakness: `F_d` is reactive (rises with observed peak), so a
runtime peak-growth can push `Σ F_d` past capacity with **no escalation today**
(§8, P7).

---

## §5. The stochastic + tail layer

### §5.1 Demand process and variance absorption

Model demand as a process `{D(t)}` with mean `μ_D`, variance `σ²_D`, bandwidth
`ω_d`. The **absorbed-variance headroom** must cover both the steady fluctuation
and the unsampled growth within one loop:

```
   h_d  ≥  (W_peak − W_mean)  +  max_{[t,t+τ_loop]} |dW/dt| · τ_loop,
            └ peak-to-mean ┘     └ growth during the response latency ┘
```

with `τ_loop = τ_sample + τ_cooldown`. This is a **heuristic** engineering bound
(§9): the demand distribution shape is unspecified, no concentration inequality
(Chernoff/Hoeffding) is proven.

### §5.2 Why the setpoint is `< 1` (the ρ→1 result)

> **Result (Queueing knee — *work-conserving dimensions only*).** In M/M/1, mean
> occupancy `L = ρ²/(1−ρ)` with `ρ = λ/μ`. At `ρ = 0.80`, `L ≈ 3.2`; as `ρ→1`,
> `L→∞` **superlinearly**. For a **work-conserving (soft / L¹) dimension** — cpu,
> network, a queue — the setpoint `0.80` sits at this knee: beyond it latency and
> congestion escalate without bound. *(Standard result — Little 1961; Kingman
> 1961 heavy-traffic: under variability `σ>0` the knee drops left, justifying
> `setpoint ≤ 0.70` for bursty loads.)*

> **⚠ Scope (soundness audit 2026-06-04).** This Kingman/M-M-1 argument applies
> **only** to work-conserving dimensions, where breathe's utilization `u = D/L`
> coincides with the queueing `ρ = λ/μ`. For the **hard / memory (L∞) class** the
> two are *different ratios* (memory occupancy is not an arrival/service rate),
> and the queueing latency-knee is **not** the governing reason for `setpoint < 1`.
> There, `setpoint < 1` is forced by the §2.4 pointwise-headroom argument: a
> static plant has **no buffer**, so the next micro-burst breaches `used ≤ limit`
> at the instant before the loop can react. **Memory: `setpoint < 1` by §2 (no
> buffer). CPU: `setpoint < 1` by Kingman (the knee).** Importing the queueing
> theorem onto memory would be applying a result outside its model.

A setpoint of exactly `1.0` is unrepresentable for every dimension (`validate`
rejects it).

### §5.3 Timescale separation (the on-average precondition)

> **Result (Nyquist on the utilization signal).** "Held on average over window
> `W ≥ τ_settle`" holds **iff** `ω_d < 1/(2·τ_settle)`. *(Standard result —
> sampling theorem applied to `u(t)`.)* When violated, the loop sees one phase
> per cycle, chases phase-lagged, and the on-average contract fails. This is
> the regime split: diurnal drift / dataset accretion / warm-up **hold**;
> sub-minute microbursts / DoS / synchronized fleet restarts **break**.

### §5.4 The chance-constraint ↔ tail-quantile link

The per-tick OOM admission probability and the EVT tail level are the **same
parameter**:

```
   ε  :=  P(D(t) > F_d | fresh, owned)  ≤  1 − α,
   P(∃ OOM over n ticks)  ≤  1 − (1−(1−α))ⁿ  ≈  n(1−α)   for  n(1−α) ≪ 1.
```

Choosing the tail level `α` of the EVT fit **is** choosing the chance-constraint
`ε`; the floor `F_d = ⌈L_α(W)/setpoint⌉` closes the loop between the
stochastic-programming view (`P(g ≤ 0) ≥ 1−ε`) and the pointwise control view
(`safe_min` clamp).

> **⚠ Independence caveat (soundness audit 2026-06-04).** The union bound
> `P(∃ OOM over n) ≈ n(1−α)` assumes **per-tick independence** of OOM events.
> This is false under autocorrelated demand: a single burst spans many ticks, so
> the same excursion is counted repeatedly and the events are positively
> correlated. The bound is therefore an **iid-tick approximation, over-optimistic
> under correlation** — the true multi-tick OOM probability is *lower* than
> `n(1−α)` when excursions cluster (fewer independent trials), but the *time* the
> system spends over-limit during one clustered excursion is *longer*. Treat
> `n(1−α)` as an order-of-magnitude budget, not a tight bound.

This connection is presently **conceptual** — `α` is not yet a `BandConfig`
field, the EVT fit is unshipped, and `ε` is not surfaced (§8).

---

## §6. The anomaly layer (the discrete complement)

### §6.1 The detector and the in/out-of-envelope boundary

The continuous controller (§3) owns the regime `ε(t)=0` (in-envelope); a
discrete detector fires when demand exits the validity envelope:

```
   ε(t) := 𝟙( |Δu_t| > θ_jump  ∨  D(t) > C_d − F  ∨
              decision ∉ {Hold,Grow,Shrink}  ∨  staleness > τ_stale ).
```

> **Domain-separation (cited).** `ε=0` ⇒ the band-law contraction theorem (§3.3)
> applies and only deadband absorption is needed; `ε>0` ⇒ the variance/anomaly
> term dominates and explicit routing is required. `ε` operationalizes the V1–V5
> precondition boundary. *(Today `ε` is **implicit** in the `TickPlan`/`Decision`
> enums — `Stale`, `AtCeiling`, `NoSafeShrink` — not a standalone typed signal;
> §8.)*

### §6.2 The escalation ladder as a hybrid controller

The discrete complement is a five-tier monotone ladder, a switched controller
over the continuous loop:

```
   κ : (Decision, ε, {PSI, pswpin, AtCeiling, staleness}) → Severity
   Severity = Absorb ≤ Lean ≤ Back-off ≤ Panic ≤ Alert         (total order)
   τ : Severity → Action × {continue | escalate | freeze | hand-to-human}
```

`Absorb` = zero actuation; `Lean`/`Back-off` = bounded carve via the *same*
`safety_clamp` gate; `Panic` = circuit-break (derivative-zero backpressure);
`Alert` = freeze + `AnomalyChain` + human. The ladder recapitulates MAPE-K
(Kephart & Chess 2003).

### §6.3 Hybrid-system stability (dwell-time / hysteresis)

> **Claim (No Zeno, no thrash).** Tier switches occur only when
> `κ(t) ≠ κ(t−1)` **and** `dwell_count(κ_cand) ≥ N_dwell(κ_cand)`. With a
> minimum dwell on each state and each tier's action either derivative-bounded
> (`Absorb`/`Panic`) or `safety_clamp`-contained (`Lean`/`Back-off`) or
> mutation-free (`Alert`), the switched system is a piecewise-continuous Lyapunov
> system with no chattering between tiers. *(Proof-sketch — finite-state machine
> with bounded dwell over a bounded resource is globally stable; the
> single-bit input `ε` makes it a deterministic DFA, acyclic once `Alert` is
> reached.)*

A stability-respecting dwell satisfies `N_dwell(κ) ≥ ⌈1 + log_a(2)⌉ ≈ 3–4` ticks
for `a=1.25` (the AIMD convergence time — confirm the decision is stable before
escalating). This bound is **heuristic** (Chiu & Jain grounding) and **not yet a
parse-time invariant** (§8).

### §6.4 Alert as the honest terminus

> `Alert` is **not a failure** — it is the proof that the plan space `Π`
> (shrink-by-criticality, extend cooldown, circuit-break) is **exhausted**, and
> deferral to human agency is the correct, bounded-rational terminus (Sheridan–
> Verplank LOA). The classify function `κ` is **conjectured monotone** (a worse
> `Decision` never routes lower) — *doc-asserted, untested: `κ` does not yet
> exist as typed Rust in `breathe-control` and there is no `classify_monotonic`
> test (soundness audit 2026-06-04 — the earlier "proptest-witnessed" attribution
> was false). The mapping lives in docs + the controller orchestration layer* (§8).

---

## §7. The hierarchical / global theorem

### §7.1 Three timescale-separated loops

The same band law runs at three scales (Koopman timescale separation):

```
   L1 (workload):  state_i(t+1) = reconcile_one(band_i, provider_i)    τ ≈ 60 s
   L2 (node):      E_n(t+τ₂)    = update_envelope(Σ demand, policy)    τ₂ ≈ 3–10·τ₁
   L3 (pool):      warm(t+τ₃)   = alloc(λ · response_time)             τ₃ ≈ minutes–hours
```

At each level the **inner loop converges into its band before the outer loop
updates** (the 3–10× separation), so the levels compose without interaction — a
standard gain-scheduling result. *(Per §3.3: the inner loop converges to a band
*interval*, so L2 sees its inner state as `u ∈ [shrink_below, grow_above]`, not a
settled point `u = setpoint` — the outer loop must treat the inner target as the
band, not the setpoint. This weakens but does not break the separation: the inner
state is bounded and stationary-in-band before the outer tick.)* `AtCeiling` at L1
becomes the `used` signal of the L2
envelope band; `EnvelopeSaturation` at L2 becomes the grow signal of the L3
pool band (sized by Little's Law). This is the **K2 cascade**, and it is
**zero-disruption only because of the K1 keystone** (in-place pod resize,
`pods/resize` GA k8s ≥1.33): the inner carve hits the running container cgroup,
so the outer capacity carve is also restart-free for existing pods.

### §7.2 Compositional invariance

> **Theorem (Disjoint-field composition).** If each dimension owns a disjoint
> SSA field path (`competing_field_manager` guard, `lib.rs:434`) and the
> partition `Σ_d C_d = E_fleet` is static and eval-verified (§4.3), then the
> product `∏_d S_d` is forward-invariant under the concurrent L1 dynamics, and
> by induction the fleet aggregate is in-band whenever each node is in-band.
> *(Proof-sketch — cross-product of forward-invariant subsystems composed via
> static constraints is forward-invariant; `keda_on_replicas_is_not_a_memory_
> competitor` witnesses field-granular disjointness.)*

The two safety walls compose: WALL 1 (`safety_clamp`, gain-independent, any law)
and WALL 2 (`HostCluster::apply` / k8s admission independently refusing
`> C_n`) are disjoint code paths — **both** must fail for a physical breach.

### §7.3 The global statement, and its honest scope

> **Global homeostasis (asymptotic, in-envelope).** The composed
> L1⊕L2⊕L3 system reaches a stable in-band region — every workload **in-band**
> (`u ∈ [shrink_below, grow_above]`, §3.3 — *not* a setpoint point), every node
> in-band, the pool's (free,warm) ratio stable by Little's Law — and holds it, on
> the entire fleet, **with no central price/pressure signal**:
> coordination is *declarative* (each node computes its L2 envelope from its
> local demand vector + capacity), matching the Conant–Ashby Good-Regulator
> theorem (only a model of the system regulates it — here the typed dimension
> catalog is the model).

**Does the global loop need a coordinating price signal?** For **uncoupled or
decoupled** dimensions on a **static eval-verified partition** — **no**: each
greedy band reaches a Walrasian/Pareto allocation independently, and the global
invariant holds by induction with zero synchronization (the system scales to
arbitrarily many nodes). A price/pressure signal becomes necessary **only** when
dimensions are **coupled** (memory + cpu both gating one bottleneck) or the
envelope is **exhausted under correlated burst** — then the typed
`EnvelopeSaturation` anomaly must route to an L2 fair-share **allocator** over a
criticality lattice. **That allocator is unshipped** (P3/P7); today coupled
contention escalates (`Alert`) rather than allocating fairly. So the "no central
signal" property is **true exactly on the uncoupled/non-exhausted regime** and
is a *deferred* allocator everywhere else — stated, not papered over.

---

## §8. Validity conditions & where the model breaks (consolidated)

The theorems above hold **only** inside the envelope `V1…V10`. Each row is a
precondition; violating it voids the named theorem.

| # | Condition | Theorem it protects | Breaks when |
|---|---|---|---|
| **V1** | Static algebraic plant `u = w/L` (no integrator/pole) | §3 stability, gain-independence | a controlled variable has its own integrator (managing rate-of-OOM, not OOM state) |
| **V2** | Sub-Nyquist demand `ω_d < 1/(2·τ_settle)` | §3.3 convergence, §5.3 on-average | sub-second bursts, DoS, synchronized fleet restart |
| **V3** | Resource elastic (soft `F=0`) **or** floor peak-provisioned (hard `F_d ≥ sup_W(w/setpoint)`) | §2 pointwise safety, §4.2 return-level | hard resource with under-set floor (→ OOM); hidden integrator (queue depth) |
| **V4** | Envelope slack + criticality order: `Σ F_d + reserve + Σ elastic_max ≤ capacity` (eval-time) | §4.3 invariance fits hardware | floors don't fit (eval catch); multiple top-criticality contend (no arbiter) |
| **V5** | Two independent safety walls (clamp ∥ provider ceiling) | §2.3, §7.2 never-breach | provider doesn't enforce ceiling; competing field-manager writes the field |
| **V6** | Fresh sample `staleness ≤ max_staleness` | §2.3 clamp soundness | scraper down/lagging; counter reset; cAdvisor mis-gauge (fresh-but-wrong, P2) |
| **V7** | Sole field ownership (single-writer per path) | §7.2 composition | VPA/HPA/KEDA co-write; manual `kubectl patch` races |
| **V8** | `T_cooldown` ≥ time-to-see-effect | §3 no-thrash | cooldown ≪ plant lag (loop chases pre-carve metrics) |
| **V9** | Authoring-sane gain, no dead-time resonance | §3.4 stability margin | high-gain + low-cooldown limit-cycles (P4, **unguarded**) |
| **V10** | Decoupled dimensions (no shared exhaustible resource) | §7.2 disjoint composition | memory+cpu joint bottleneck (P3, **deferred**) |
| **V11** | Deadband dominates the **discrete-step swing**: `Δ > 2σ_noise + δ_step`, `δ_step` from `grow_factor`/`shrink_factor` | §3.2 chatter elimination, §3.3 convergence | a coarse grow/shrink factor steps *through* the band (one step `Δu ≈ 0.15` ≈ band width) — **unguarded by `validate`** (§8, P4) |
| **V12** | EVT fit is **stationary / block-wise** (or `μ(t)` de-trended) | §4.2 return-level floor | diurnal/accreting demand fit with one global return level → daytime peak under-estimated (the fit is unshipped — design constraint, not current defect) |
| **V13** | OOM-over-window budget treats ticks as the **iid approximation** it is | §5.4 chance-constraint union bound | autocorrelated bursts span many ticks → `n(1−α)` is an order-of-magnitude budget, not a tight bound |

**Named residual gaps (the honest backlog):**

- **P4 (dead-time flap margin) — `unguarded`.** `validate` checks SAFETY
  (well-ordered band, sane factors) but **not** the stability-margin clause. A
  `Refined<f64>` bounding effective per-tick gain by `f(cooldown, scrape,
  grow_factor)` would make destabilizing configs *unrepresentable*; until then,
  a config that passes `validate` can resonate.
- **P2 (fresh-but-wrong) — `only-mitigated`.** Freshness catches *absent*, not
  *wrong*. A counter-reset `D≈0` defeats `safe_min`. Needs an
  innovation-consistency gate (bounded per-tick deviance vs `OutcomeChain` +
  corroborating PSI → `Implausible→Hold`).
- **P3 (coupled dimensions) — `deferred to M3`.** Independent bands detect
  all-in-band (lattice meet) but never *prioritize* under joint contention.
  Needs an authored L2 joint planner over a criticality lattice.
- **P1 (burst-OOM) — `only-mitigated`.** `safety_clamp` blocks self-inflicted
  OOM, not demand-driven. `PredictiveGrow` pre-empts on `dD/dt` but with a
  constant `lookahead` and no rate-error margin; a zero-rate cold-cache spike
  can OOM before the second sample.
- **P7 (floor-growth feasibility) — `runtime-unchecked`.** `F_d` is reactive;
  a rising peak can push `Σ F_d` past capacity at runtime with no typed signal.
- **P5 (ResourceClass typing) — `untyped`.** Soft/Hard recovery class is not a
  first-class catalog attribute; QoS (Guaranteed↔Burstable) is not gated.
- **P10 (window attestation) — `per-tick only`.** `OutcomeChain` attests
  individual carves; "held in band over window `W` across all dimensions"
  requires a `PromessaLattice` meet of per-dim receipts — unshipped.
- **EVT fit — `offline/unshipped`.** `L_α(W)`, `ξ`, `α`, `ε` are not yet
  `BandConfig` fields; the GEV fit happens (if at all) outside breathe; the
  floor is a hand-authored scalar today.
- **`ε` detector + `κ` classify — `doc-only`.** Neither the envelope-exit
  signal `ε` nor the monotone classify `κ` exists as typed Rust in
  `breathe-control`; both are implicit in `Decision`/`TickPlan` + orchestration.

---

## §9. Rigor ledger

**Brutally honest.** `proved` = property/unit-test-witnessed in
`breathe-control`/`breathe-core` (not machine-checked theorem-prover proof —
that gap is itself flagged). `cited` = standard control/queueing/EVT result
applied. `sketch` = argued, not formalized. `heuristic` = engineering bound,
distribution unmodeled. `conjecture` = asserted, unproven.

| ID | Claim | Tier |
|---|---|---|
| **C1** | Static plant ⇒ relay-with-hysteresis stable for any bounded gain (§3.4) | cited |
| **C2** | `safety_clamp` makes never-OOM/never-overshoot universal across **any** law (incl. adversarial), gain-independent (§2.3, §4.1) | **proved** (property test; *self-inflicted* OOM only — demand-driven is `only-mitigated`) |
| **C3** | Band law converges in `N=⌈log_a(u₀/grow_above)⌉` steps into the held **deadband** `[shrink_below, grow_above]` (NOT to the point `setpoint`) (§3.3) | **proved** (contraction-into-band; *the earlier "`u*≈setpoint`" form was a false `proved` theorem — corrected by the 2026-06-04 soundness audit, verified numerically: grow→0.742, shrink→0.712*) |
| **C4** | `reconcile_one` enforces single-writer ∧ freshness ∧ cooldown ∧ golden-edge **before** any mutation (§3, §6) | **proved** (ordered gates, tests) |
| **C5** | `RestartFreeOnly` ⇒ every receipt golden (zero ceiling crossings); a crossing is witnessed + typed (§6, §7) | **proved** (`golden_continuity_…`) |
| **C6** | In-place resize w/ `resizePolicy=NotRequired` is RestartFree ⇒ bidirectional pod-plane breathing (K1 keystone) (§7.1) | **proved** (`reconcile_acts_on_a_not_required_memory_shrink…`) |
| **C7** | OOM is an L∞ (pointwise, a.s.) constraint, strictly stronger than L¹ (§2.1) | cited |
| **C8** | Mean-provisioning leaves tail risk `P(D>μ)>0`; `ess-sup ≈ μ + kσ`, `k≥1.64` (§2.1) | cited |
| **C9** | `setpoint=0.80` = M/M/1 queueing knee (§5.2) | cited (**work-conserving/soft dims only**; for memory, `setpoint<1` is forced by §2.4 pointwise-headroom, not Kingman) |
| **C10** | On-average holds **iff** `ω_d < 1/(2·τ_settle)` (Nyquist) (§5.3) | cited |
| **C11** | AIMD (`a>1, b∈(0,1)`, back-off≥growth) ⇒ efficiency + fairness (§3.4) | cited |
| **C12** | `Δ > 2σ_noise + δ_step` eliminates chatter; `Hold` is a fixed region (§3.2) | cited (precondition; `σ_noise` unmodeled AND the discrete-step term `δ_step` must also fit in the band — V11) |
| **C13** | Single-dimension safe set `S` is forward-invariant (§4.1) | **proved** (per-dimension) |
| **C14** | Return-level floor `F_d=⌈L_α/setpoint⌉` contains the envelope `D≤L_α` (§4.2) | **proved (algebra)** / EVT fit **unshipped** |
| **C15** | EVT return level grows log-`W` / unbounded for `ξ>0`, not `μ+kσ` (§4.2) | cited |
| **C16** | Chance-constraint `ε ≈ 1−α` ↔ tail quantile; n-tick union bound (§5.4) | sketch (link conceptual; `α` not a field; **union bound is iid-tick — over-optimistic under autocorrelated bursts**, V13) |
| **C17** | Variance headroom `h_d ≥ (peak−mean)+max|dW/dt|·τ_loop` (§5.1) | **heuristic** (distribution unmodeled) |
| **C18** | Escalation ladder `κ` is monotone; `Absorb≤…≤Alert` total order (§6) | **conjecture** (doc-asserted, **untested** — no `classify_monotonic` test exists; `κ` not yet typed Rust. The earlier "proptest-witnessed" attribution was false — soundness audit 2026-06-04) |
| **C19** | Hybrid switched system is stable (dwell-time, no Zeno) (§6.3) | sketch |
| **C20** | `Alert` ⇒ plan space exhausted ⇒ correct human terminus (§6.4) | **heuristic** (no cost-minimization optimality proof) |
| **C21** | Hierarchical L1⊕L2⊕L3 stable under 3–10× timescale separation (§7.1) | cited (separation ratio not derived from params) |
| **C22** | Disjoint-field product is forward-invariant; fleet in-band by induction (§7.2) | sketch (uncoupled only) |
| **C23** | Global homeostasis needs **no** central price signal (§7.3) | sketch (**uncoupled/non-exhausted regime only**; coupled ⇒ unshipped allocator) |
| **C24** | Joint product `∏_d S_d` forward-invariant under **contention** | **conjecture** (no joint planner; P3) |
| **C25** | Three-level composition converges in **finite** time (not just asymptotic) | **conjecture** (no inter-level delay bound) |
| **C-DECOMP** | The three terms (mean/variance/anomaly) are orthogonal, exhaustive, uniquely attributed (§1.2) | **conjecture** (lattice obligation, P10) |
| **C26** | All `proved` rows are **property/unit-test** witnessed, **not** Coq/Lean machine-checked | meta-gap (formalization deferred) |

---

## §10. Forward consequences — how this math constrains the resource-ether

The law is not decoration; it is the **type discipline** the global
resource-ether must obey. Each consequence below is a design constraint with the
section that forces it.

1. **The predictor must respect envelope invariance (§4, §3.4).** Any
   forecasting/feed-forward layer (`PredictiveGrow` and successors) may only
   **pre-grow within `[F_d, C_d]`** and must route every proposal through
   `safety_clamp`. A predictor that proposes outside the box, or that bypasses
   the clamp, is **forbidden by construction** — prediction is a *speed* knob on
   the mean term, never a new safety surface. Rate-estimation error biases
   *toward over-provision* (money), never under (process).

2. **The auction/allocator must price within the chance-constraint (§5.4, §7.3).**
   When the L2 fair-share allocator (P3/P7) ships, a tenant's bid is a tuple
   `(setpoint, F_d via α, C_d, criticality)`; the allocator assigns floors and
   ceilings so that `Σ F_d ≤ capacity` (the never-swap invariant, §4.3) and
   prices marginal headroom at the **tail level `α` the tenant chose** — a
   tenant demanding `α=0.999` pays for a higher return-level floor. Fairness is a
   **derived** property (uniform setpoint over an eval-verified partition), never
   hand-authored per tenant. A coupled-dimension bottleneck must route through
   the criticality-lattice allocator, not through independent greedy bands.

3. **The validation pipeline enforces floor-from-peak before admission (§2.2,
   §4.2, §4.3).** An admission webhook MUST refuse any hard-resource band whose
   `floor_bytes < ⌈peak_used/setpoint⌉` over the declared window, and MUST refuse
   any node whose `Σ F_d + reserve + Σ elastic_max > capacity`. Moral hazard
   (`floor=0`, "the app just exists") is rejected with a typed reason:
   **the floor must be tail-risk-justified.** This is the eval-time `nixos-rebuild`
   gate generalized to a runtime admission gate.

4. **The cooldown/gain pair must clear the dead-time margin before it ships
   (§3.4, §8 P4).** The path-of-least-resistance config (high gain to "chase the
   spike") is the canonical instability. The destination is a `Refined<f64>`
   `BandConfig` invariant that computes `max_safe_gain = f(cooldown, scrape,
   grow_factor)` and **rejects destabilizing configs at parse time** — promoting
   P4 from `unguarded` to `truly-unrepresentable`. Until that lands, every
   authored band carries a `pending-unrep: P4` note.

5. **Every emitted limit is a tick in an attested theorem, not a metric (§6,
   §9 P10/C-DECOMP).** Each `Grow`/`Shrink`/`Hold` carve signs
   `{target, dim, used, capacity, from, to, decision, edge_tier, epoch}` into
   `OutcomeChain`. The window-level promise — "target `X` held `[shrink_below,
   grow_above]` across **all** enrolled dimensions over `[t₀,t₁]`" — is a
   `PromessaLattice` meet of per-dim receipts, verifiable by
   `kensa verify outcome-chain` **without re-running the controller**. Provision
   becomes a *continuously-attested theorem*; the chain is the proof, every
   dashboard a mere view. The decomposition's orthogonality (C-DECOMP) is
   discharged *here*, as the lattice obligation, or it is not discharged at all.

6. **The ε/κ boundary must become typed before the ether scales (§6.1, §8).**
   The envelope-exit detector `ε` and the monotone classifier `κ` must be lifted
   from doc + orchestration into typed Rust in `breathe-control`, so the
   in-envelope↔out-of-envelope boundary and the escalation monotonicity are
   *machine-checked*, not asserted. Until then, the hybrid-stability claims
   (C18–C20) stay `sketch` and the global theorem (§7.3) rests on an unverified
   classifier.

7. **Coupling must be declared, or the disjoint-composition theorem is a lie
   (§7.2, §8 V10).** The catalog must carry a typed coupling annotation; a
   dimension that shares an exhaustible physical resource (NUMA bandwidth,
   device GC, kernel lock) MUST NOT claim disjoint-field independence. The
   Gate-0 question for every new dimension — *"cleanly partitionable at my
   actuator granularity?"* — is the admission predicate for §7.2 to apply.

> **The compounding move.** Every gap in §8 that is `only-mitigated` /
> `unguarded` / `conjecture` is a remediation-queue item whose destination is a
> **typed absence** (P4 → parse-time gain rejection; P2 → innovation gate;
> C-DECOMP → lattice meet). The model demands an *absence* where today there is a
> *mitigation*; per `UNREPRESENTABILITY.md §II` and the Prime Directive, time
> pressure is not an acceptable reason to ship the mitigation where the math
> calls for the absence. This document is the falsifiable contract that makes
> each such gap visible, graded, and owed.

---

*Companion: [`BREATHABILITY-THESIS.md`](./BREATHABILITY-THESIS.md) (what the
thesis is/is not) · [`BREATHE.md`](./BREATHE.md) (mechanism) ·
[`BREATHABLE-SUBSTRATE.md`](./BREATHABLE-SUBSTRATE.md) (catalog + tiers). Bright
line: `theory/UNREPRESENTABILITY.md §II`. Code anchors: `breathe-control/src/lib.rs`
(`decide` :289, `safety_clamp` :183, `BandLaw::propose` :269, `PredictiveGrow`
:371, `competing_field_manager` :434, `plan_tick` :533, `validate` :98) ·
`breathe-core/src/lib.rs` (`reconcile_one`).*
