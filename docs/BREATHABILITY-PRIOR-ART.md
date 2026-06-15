# Breathability — Prior Art & What Is Actually Novel

> **Verdict: a novel *synthesis*, not a novel *primitive*.** breathe's actuation
> mechanism is commodity 2026 Kubernetes; its distinctiveness is a **control
> model** and a **cooperation model** layered on top, plus one scope union with
> no prior art at all. This doc is deliberately adversarial and tier-honest —
> it concedes everything that is prior art by name, so the small set of genuine
> claims survives scrutiny.

## How this was assessed

An adversarial ecosystem audit (2026-06-15): five parallel research sweeps
(k8s-core, commercial rightsizers, OSS recommenders, control-theoretic
research, and the cooperative-composition + host/node-scope angle) feeding
per-property refutation passes, each agent instructed to **default to refuting
novelty** — to hunt for the tool that already does what breathe does. The
finding below is what survived that hunt, with the closest prior art named for
every property. Method bias is toward *under*-claiming.

## The prior-art ledger

breathe makes six claimed-distinctive moves (P1–P6). Graded honestly:

| # | Property | Grade | Closest prior art |
|---|---|---|---|
| **P1** | In-place LIMIT carve via `pods/resize`, zero restart | **PRIOR ART — concede** | **KEP-1287 In-Place Pod Resize** (GA k8s 1.35, Dec 2025) *is* the subresource breathe uses; VPA `InPlaceOrRecreate`/`InPlace`, ScaleOps, StormForge, Cast AI, kube-startup-cpu-boost all actuate in-place today. Table stakes. |
| **P2** | Setpoint/deadband **band** control on the **limit**, live usage | **NOVEL-SYNTHESIS** (high conf.) | The control law (PID/deadband) is textbook prior art (e.g. **SHOWAR**, SoCC 2021 — on *replicas*). VPA = P95 decaying-histogram on **requests**; StormForge sets limit = request × fixed LRR ratio. No surveyed tool runs a convergent band loop *on the limit*. Distinct on two legs: **band ≠ percentile**, **limit ≠ requests**. |
| **P3** | **Cooperative-by-default**: read SSA `managedFields` each reconcile, **yield** to any other field-manager | **NOVEL-SYNTHESIS — the sharpest** (high conf.) | Mechanism is off-the-shelf (**SSA / KEP-555**). Closest *static* yield: **Crossplane `Observe`/`ManagementPolicies`**. Closest *behavioral* analog: **Cast AI** ("detect third-party owner → skip workload") — but it *inverts* the default (takes over by default; skips only on detected incompatibility; mechanism undocumented). The ecosystem default is the **opposite**: force-overwrite on conflict, sole-ownership, the unsolved HPA/VPA death-spiral worked around by static dimension-separation or a *new* combined controller (GKE MPA). No controller adopts **read-managedFields-then-yield as its default composition stance**. |
| **P4** | Disruption cost as a **type**, gated by policy before acting | **PRIOR ART as a generic pattern** (high conf.) | **Karpenter disruption budgets** (GA) type the disruption *reason* (`Drifted/Underutilized/Empty/Expiration`) and gate per-reason before acting — a direct counterexample to any universal-negative framing. Container `resizePolicy` (`NotRequired`/`RestartContainer`) is itself a typed restart-class; PDBs + VPA `EvictionRequirements` gate disruption by policy. Only breathe's *precise composition* (controller-computed per-action `DisruptionClass` from `resizePolicy` → never-evict default on in-place limit carves) is uncombined elsewhere. |
| **P5** | One band law across **pods + ephemeral groups + host (ZFS ARC, cgroup) + node-count** | **GENUINELY-NOVEL** (high conf.) | Three disjoint single-scope stacks: VPA/MPA/ScaleOps/Kubex (**pods**), Karpenter/Cluster-Autoscaler (**nodes**), Cluster-Node-Tuning-Operator/TuneD/systemd/OpenZFS (**host**, static profiles, *no feedback loop*). k8s-core docs themselves call these "largely independent control loops … limited coordination across layers." **The host-OS leg is decisive: nothing in the ecosystem homeostatically band-controls systemd cgroups or ZFS ARC — there is no ingredient waiting to be combined.** This is the only property the audit could not refute even in principle. |
| **P6** | Safety-by-construction: never-OOM clamp, freshness gate, metric-unrepresentable guard, shadow-first, metrics-server-only | **Largely prior art individually** | kubelet's *best-effort* (explicitly not guaranteed) OOM-avoidance on memory-limit decrease is a partial analog; bounds + metrics-server reads are conventional. Shadow/dry-run, freshness gate, metric-unrepresentable guard have no surveyed analog, but the framing is mildly distinctive, not load-bearing. |

## The novel core

breathe's defensible novelty is **the conjunction of P2 + P3 + P5 layered on a
commodity actuation primitive** — not any single mechanism:

- **P2** — a convergent deadband+cooldown loop holding a utilization **band on
  the LIMIT**, reading live metrics-server usage, where the field instead runs
  percentile-of-history / ML on **requests**.
- **P3** *(sharpest — independently reproduced as the strongest differentiator
  across all four research passes)* — **cooperative-by-default**: yield to any
  other field-manager via `managedFields`, where the entire ecosystem assumes
  sole ownership and treats the HPA/VPA fight as unsolved-by-cooperation.
- **P5** *(the only "genuinely-novel" grade)* — **one band law across pods +
  ephemeral groups + host + node**, where the host-OS feedback-control leg has
  no prior art anywhere.

## Tier-honesty — the cooperative guard is *mitigation*, not *unrepresentable*

Per the org's [`UNREPRESENTABILITY`](https://github.com/pleme-io/theory/blob/main/UNREPRESENTABILITY.md)
bright line (a `Result::Err`/runtime-yield is *mitigation*; a compile error /
absent path is *unrepresentability* — never round up):

**The single-writer guard's "two-writers-fighting is unrepresentable" claim is
`only-mitigated`, not `truly-unrepresentable`.** breathe reads `managedFields`
each reconcile and *decides* to yield — a per-reconcile runtime check. A
concurrent **force-applying peer** (`force: true` strips other managers' fields —
the documented SSA default) or a **race window** between breathe's read and its
write can still construct the conflicting state. What breathe makes
unrepresentable is *breathe itself fighting* (it has no code path to write a
field it observed another manager owns) — a real and useful guarantee, but
scoped to breathe's own behavior, not a cluster-wide invariant. The doctrine
claim is: **"breathe is a good citizen by construction,"** not **"conflict is
impossible."** Reserve "unrepresentable" for what earns it.

## Closest overall prior art (name it, don't hide it)

- **Complete controller:** **VPA in `InPlace`/`InPlaceOrRecreate` mode**
  (kubernetes/autoscaler, KEP-4016, riding KEP-1287 GA in 1.35). Same
  `pods/resize` actuation; can manage limits under
  `controlledValues: RequestsAndLimits`; `InPlace` *never falls back to
  eviction* (defers + retries) — matching breathe's defer-not-evict spirit. It
  differs on **every** distinctive axis: percentile-of-history (not band),
  requests-primary (not limit-held), sole-ownership/force-overwrite (not
  managedFields-yield), pod-only (not unified host+node), and fires `/resize`
  blindly delegating restart to the kubelet (not a controller-computed
  restart-cost gate).
- **Closest commercial:** **ScaleOps** — strongest verified "in-place, no
  restarts, no evictions, no rollouts" + HPA/KEDA-coexistence framing — yet
  still request-rightsizing (not band-on-limit), co-manage/own (not
  SSA-yield), pod-only (no host scope).

## What this doc must NOT claim (or it is immediately falsifiable)

1. **In-place actuation is not novel.** It is GA k8s core (KEP-1287, 1.35) and
   shipped by every major rightsizer. Say so early.
2. **"No autoscaler types-and-gates disruption" is false** — Karpenter
   disruption budgets refute it. Narrow P4 to breathe's precise composition;
   drop any universal-negative framing.
3. **P3 rests partly on a negative** (no tool *documents* managedFields-yield).
   Undocumented commercial internals (Cast AI / ScaleOps / nOps) could do it
   silently — which is exactly why P3 is *novel-synthesis*, not airtight
   *genuinely-novel*. State the residual uncertainty.
4. **The host leg is earlier-stage** than the pod+node legs. The unified-scope
   claim (P5) is strongest on pod+node; flag host (`breathe-host`) as an
   implementation-maturity caveat — the *design* is unified, the *host
   actuation* is younger.
5. **The control theory is not invented here.** Setpoint+deadband, PID-in-k8s
   (SHOWAR), and predictive-grow-for-replicas are all prior art — only their
   application to the *limit* (vertical, in-place, continuous) is uncommon.
6. **Do not brand "homeostasis" as a new category.** The concept is established
   in control-theory-for-computing; using it descriptively is fine, claiming it
   as breathe's invention is not.

## The honest one-paragraph framing

> breathe is a **cooperative, in-place, band-converging resource controller**.
> Its actuation (in-place `pods/resize`, zero restart) is commodity 2026 k8s
> (KEP-1287 GA, VPA InPlace, ScaleOps, StormForge). What is **not found in the
> surveyed 2026 ecosystem** is the synthesis: a deadband control loop holding a
> utilization band on the *limit* from live usage (P2); a *cooperative-by-default*
> stance that reads `managedFields` and yields to any other writer, so breathe
> composes rather than displaces (P3 — the sharpest, no documented analog); and
> one band law spanning pods, ephemeral pod groups, **host** cgroup/ZFS, and
> node-count (P5 — the host-OS feedback leg has no prior art at all). Closest
> prior art is VPA-InPlace (complete controller) and ScaleOps (commercial); both
> diverge on band-on-limit, SSA-yield, and host scope. Claim "novel synthesis,"
> not "novel primitive" — and frame the gaps as "not in the 2026 ecosystem,"
> not "first ever."
