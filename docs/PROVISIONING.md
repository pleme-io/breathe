# The breathe provisioning extension — the resource ether

> **PRIVATE design doc.** The canonical spec for lifting breathe from
> *slicing resources within a node* to *provisioning the resources themselves* —
> nodes, spot, accelerators, zones, serverless, the hardware-solution layer —
> validating every candidate through a readiness pipeline before it can be used,
> and driving the whole loop with a predictive, cost-bounded auction. This is the
> "computing ether" vision.
>
> **Packaging decision (made 2026-06-04, after an adversarial design critique).**
> This is **not a new substrate.** It is breathe's own deferred backlog —
> [`BREATHABILITY-THESIS.md`](./BREATHABILITY-THESIS.md) §7 items **P3** (joint
> planner), **P7** (fair-share L2 allocator), **P8** (provisioning-latency-aware
> placement), **K2** (recurse the band law to cluster scale), **P10** (window
> attestation) — landing *inside the breathe workspace* with breathe's existing
> vocabulary. The earlier design proposed a separate `eter-*` substrate with a
> fourth vocabulary (`éter`/`forma`/`vassoura`/…); the critique showed the reuse
> is so total (≥90%) that a new vocabulary would only *narrow* the substrate by
> forcing a permanent breathe↔ether name-map — the exact anti-pattern the org
> Compounding Directive forbids (*solve once, in one place*). So: **zero new
> top-level names.** `DimensionId` gains a sibling `Forma`; `BreatheNodePool`
> grows into the envelope band; `BreathePromise` covers the provisioning promise;
> and only the genuinely-new ~10% — *validated admission* and *the auction* — land
> as two new crates (`breathe-admission`, `breathe-auction`) in the breathe
> workspace.
>
> **The formal law this design obeys is [`BREATHABILITY-MATH.md`](./BREATHABILITY-MATH.md)**
> — its §10 imposes seven typed constraints, restated in §7 below. Read
> [`BREATHE.md`](./BREATHE.md) for the band-law mechanism this extends.

---

## §0. The destination, stated once

> **breathe holds one promise for an entire fleet: there are always enough
> *validated* resources to meet demand, inside a cost + readiness + compliance
> budget, and the proof is queryable.** It generalizes the one proven
> dimension-blind band law from slicing memory/cpu/storage *within* a node down
> the stack to *provisioning the node, the spot mix, the accelerator share, the
> zone, the serverless slot* — and one level further, to the hardware-solution
> layer where a candidate is an instance type, a rack, a region, an auction bid.
> Every controllable infrastructure quantity is a **shape** (`Forma`) that
> projects to two scalars `(used, capacity)`, drives the *same* `breathe-control`
> band law, and dispatches through *one* typed provider boundary. No raw resource
> is ever consumed: every candidate flows through a **validated-admission FSM**
> that admits it only after attested gates pass, so *holding* a validated
> resource **is** the proof it cleared admission. Above the band law sits a
> **predict → optimize → auction** engine; below it, the cluster is mutated only
> via GitOps-native typed actions, every tick attested into an OutcomeChain.

The bright line from [`UNREPRESENTABILITY.md`](https://github.com/pleme-io/theory)
§II is carried throughout: **a `Result::Err` is mitigation; a missing method or a
sealed constructor is unrepresentability.** Never rounded up.

---

## §1. The lift — `Forma` ⟂ `Dimension`

breathe today controls a **dimension**: a scalar resource *sliced within* a fixed
envelope (memory in a pod, ARC on a host). The lift adds a **forma**: a scalar
resource that *provisions the envelope itself* (a node, a spot seat, a GPU). The
two are orthogonal and compose:

```
   Dimension  — slices WITHIN an envelope   (memory 8Gi→10Gi inside a node)
   Forma      — provisions THE envelope      (node count 2→3 under a budget)
                                             ↑ the band law's `used`/`capacity`
                                               are now (pending_pods, max_nodes)
```

A `Forma::NodeOnDemand` carving node count `2→3` runs the **identical pure
function** `breathe_control::decide(used, capacity, &BandConfig)` as a memory band
carving `8Gi→10Gi`. The band law is dimension-blind *and* shape-blind: it sees two
opaque `u64`. This is **K2** ("recurse the band law to cluster scale"), and it is
the keystone reuse.

### §1.1 The corrected keystone claim (reused vs newly-authored)

The first design overstated this as "the ether adds no new control theory."
**That is false, and the honesty matters** (a control-theory claim that turns out
wrong poisons everything built on it). The precise decomposition:

- **REUSED VERBATIM — the band law's *safety-clamp* invariant**, for *per-forma
  scalar carves*. `safety_clamp` (`breathe-control:183`) guarantees never-overshoot
  / never-over-commit for *any* law over the 6-arm `Decision` (proved by
  `safety_gate_contains_any_law`). A node-count grow `2→3` is exactly such a
  scalar carve; it inherits the proof. **Note (per `BREATHABILITY-MATH.md` §3.3):**
  the band law's fixed point is the deadband *interval* `[shrink_below, grow_above]`,
  **not** the point `setpoint` — so "node count settles at setpoint utilization"
  is wrong; it settles *in-band*.

- **NEWLY-AUTHORED — the cross-forma *auction* (`breathe-auction`).** The moment
  cost, spot-vs-on-demand arbitrage, tier selection, or coupled-dimension
  contention enters — *the entire reason the auction exists* — the decision is no
  longer `breathe_control::decide` on one scalar. It is a function over a forecast,
  a Pareto-ranked candidate set, and a budget. The band-law safety proof **does
  not cover it.** This is the thesis's own §154 scope line: *coupled dimensions
  have no derivable joint safe-set; the arbitration is authored.* The auction
  carries its own, weaker, explicitly-named safety story (§5, §6).

So: **the scalar-carve half is proven by reuse; the auction half is new
arbitration we must justify separately.** Anyone who reads "breathe provisions
nodes" must know which half they are standing on.

---

## §2. The typed primitive set (breathe-internal vocabulary)

New names appear *only* for genuinely-new mechanisms. The shape lift extends
existing breathe crates; admission and the auction are the two new crates.

### §2.1 The shape lift (extends `breathe-provider` / `breathe-kube`)

| Name | Kind | Purpose |
|---|---|---|
| **`Forma`** | enum (sibling of `DimensionId`) | The shape atom: `NodeOnDemand`, `NodeSpot`, `Accelerator`, `ServerlessSlot`, `EdgePlacement`, `ZoneCapacity`, `JitBuilder`, `Custom(&'static str)`. Keys the catalog + provider dispatch + CRD. |
| **`FormaDescriptor`** | trait (peer of `DimensionDescriptor`) | Projects raw infra metrics → scalar `(used, capacity)` for the band law and `Decision` → a provision mutation. Declares `capacity_source`, `used_source`, `directionality`, `cost_model`, `relief_latency`, `readiness_signals`. |
| **`Provedor`** | trait (peer of the `Cluster` trait) | The async I/O boundary: `observe → Observation`, `provision(n) → ProvisionReceipt`, `deprovision(n)` (cordon→drain, PDB-aware), `wait_ready(ids, timeout)`. **Idempotent** provision; **cannot re-decide** (receives "grow by N"). |
| `LimitLayout::{NodePool, InstanceSelector, DeviceQuota}` | enum arms (extend `breathe-provider`) | The ~3 new dispatch arms `breathe-kube` adds beside `ClusterTopLevel`/`PodResize`/`Host`. |

### §2.2 The catalog (extends `breathe-catalog`, per CATALOG REFLECTION)

| Name | Kind | Purpose |
|---|---|---|
| **`FormaSpec`** | catalog row (peer of `DimensionSpec`) | Self-describing shape entry: id, directionality, recovery `ResourceClass`, maturity gate, `depends_on` DAG, `relief_latency`, `cost_model`. Substrate-invariant test: every authored `Forma` has a row; keywords unique; DAG acyclic. |
| **`Cascata`** | struct (parse-time-acyclic) | The typed fallback lattice: `NodeSpot --QuotaExhausted--> NodeOnDemand --BudgetExhausted--> ShedReplicas`. Fallback is a first-class value, validated acyclic on load — never catch-block shadow code. |

### §2.3 Validated admission (`breathe-admission` — the headline novelty)

The user's load-bearing insight: it is **not enough** that the algorithm selects a
node that fits the resource math — that node must **pass a validation + readiness
pipeline** before it can join the pool a demanding workload draws from. This is
the one place real new type-discipline is built.

| Name | Kind | Purpose |
|---|---|---|
| **`FaseRecurso`** | enum (closed legal-state set) | `{Descoberto, Provisionando, Provisionado, Validando, Pronto, Admitido, Cordoado, Drenando, Aposentado, Rejeitado, Expirado}`. **Note the last two: `Rejeitado` (a gate said Reject) and `Expirado` (a gate timed out) are *reject/timeout terminals*** — the original design omitted them, which made the convergence claim (§9) vacuously broken. They are present from M1. |
| **`Recurso<F: FaseRecurso>`** | phantom-typestate struct | `F` encodes the phase; only the forward-method to a *legal* next phase exists. An illegal transition (`Rejeitado → Pronto`) is an **absent method (E0599)**, not a runtime guard. No mutable `current_phase` field. |
| **`Portao`** | trait (an admission gate) | Nine kinds: `CapacidadeProof`, `ConformanceBinding`, `HealthLiveness`, `SchedulerReadiness`, `NodeCondition`, `AttestationBinding`, `AffinityFeasibility`, `QuotaCheck`, `CostEnvelope`. Each → a `ReciboGate{decision: Pass|Defer|Reject, blake3, ed25519}`. **Partial-failure rule:** a gate that times out → `Expirado`; a gate that rejects → `Rejeitado`; a deferred gate requeues with a bounded budget, then `Expirado`. No resource sits in `Validando` forever. |
| **`Admitido<T>`** | sealed proof-carrying wrapper | Sole constructor is *inside* the admitter, after every gate seals. Fields private. `Admitido<Recurso<Node>>` **is** the cryptographic proof the node cleared admission. |
| **`Viveiro`** | struct + CRD (the valid pool; PT *nursery*) | `ReadyPool<Admitido<Recurso<F>>>`. breathe bands + the scheduler read **only** from the `Viveiro`, never the raw kube Node list. Inserting a bare `Recurso<F>` is a **type error**. This is the headline unrepresentability: "an unvalidated resource is usable" cannot be expressed. |
| **`Admissor`** | trait (the FSM driver; peer of `reconcile_one`) | `plan → observe → validate(gates) → classify → transition → attest → tick`. Runs as an engenho controller; gates wire to `sekiban` admission webhooks. |

### §2.4 The predictive auction (`breathe-auction` — newly-authored arbitration)

| Name | Kind | Purpose |
|---|---|---|
| **`Previsor`** | trait (pure) | `predict(workload_pressure, capacity_pressure) → Previsao`. Honors the §2 category-error boundary: **memory point-in-time (never averaged), cpu rate-of-change OK.** Its *own* stability is a named concern — §5. |
| **`Otimizador`** | trait (pure) | `optimize(previsao, inventory, catalog, envelope) → OrderedSet<Proposta>` ranked on `Pareto<(cost, -latency, buffer)>`. **This is the thesis's P3/P7 joint planner** (DRF/criticality-lattice allocator). |
| **`Leiloeiro`** | trait (pure) | `decide(previsao, propostas, state, spec) → DecisaoForma ∈ {Manter, Crescer{forma,δ}, Encolher{δ,drain}, Reformar{old,new}, EnvelopeExausto{demand,escalation}}`. The deterministic pick-or-escalate, routed through breathe's existing `RemediationPolicy` lattice. |

`Previsor`/`Otimizador`/`Leiloeiro` are **pure** (no I/O) — the same load-bearing
line that keeps breathe's convergence proof intact; only `Provedor` touches the
world.

### §2.5 The envelope + promise (extend `breathe-crd`)

| Name | Kind | Purpose |
|---|---|---|
| **`Densa`** | CRD (the envelope; grows `BreatheNodePool`) | The capacity-envelope ceiling per workload-class (the thesis's L2 / **P7**): `{criticality, dimensions: Vec<DimensionBound>, cost_sla, dependency_chain}`. The hard wall breathe bands carve *within* (L1 ⊂ L2). `sekiban` reads it: `used + request > Densa.capacity ⇒ Reject{EnvelopeExhausted}`. |
| **`Semeadura`** | CRD (operator intent; PT *sowing*) | `{target_envelope, tiers, validation_policy, cost_boundary, clusters}`. "40% headroom, 90% SLA, never exceed $5k/mo." breathe *proves* it via the chain. |
| **`BreathePromise`** (extended) | `(defpromessa …)` | The Viggy promessa binding the loop (the thesis's **P10**). Reuses `TargetController` (diff/classify/decide pure; observe/act async), `RemediationPolicy`, `EscalationLadder`, `PromessaDependency::Meet` (the cost gate is a Meet-dependency on a `CostBudget` promise). |

---

## §3. Composition — reuse map (≥90%)

| Existing primitive | Relationship |
|---|---|
| **`breathe-control`** | **Reused verbatim, zero change.** `decide`/`safety_clamp`/`PredictiveGrow`/`SlewLimited` run identically on node count as on bytes. The §1.1 safety-clamp reuse claim. |
| **`breathe-provider` / `breathe-kube`** | **Extended.** `Forma`/`FormaDescriptor`/`Provedor` are siblings to `DimensionId`/`DimensionDescriptor`/`Cluster`; `KubeCluster` gains 3 `LimitLayout` arms. |
| **`BREATHABILITY-THESIS.md` L2** | **Becomes `Densa`.** The static refuse-wall becomes the typed fair-share envelope; `Otimizador` is the thesis's deferred joint planner (P3). The never-swap invariant lifts to cluster scale. |
| **`nodeBudget` (nix)** | The eval-time floor *under* `Densa`. `CapacidadeProof` cross-checks `node.allocatable` against the eval-time never-swap proof. |
| **`magma`** | The cloud-provisioning executor for `Provedor::provision`. `NodeOnDemandProvedor` emits a `magma` `Plan` (attested), **not** a direct `aws asg set-desired-capacity` (GITOPS-NATIVE). *See §8 — magma's provisioning path is draft, which gates M2.* |
| **`engenho` + `pangea-operator` + `sekiban`** | The `Admissor` runs as an engenho controller (canonical 2nd consumer); `Portao` gates wire to `sekiban` ValidatingAdmissionPolicy webhooks. |
| **`pangea-jit-builders`** | The canonical `Forma::JitBuilder` — cordel's `builder-wake` (ASG `desired=0→N`) becomes one `Provedor` impl. No new mechanism; breathe *types* the existing JIT pattern. |
| **`commitment-forge` / `attribution-forge` / `shinryu`** | **Consumed, not re-invented.** The auction's cost model reads the existing commitment/attribution data plane (an RI-covered node is free-at-margin; a spot node cheap-but-fragile); the `Previsor` sources from shinryu's analytical/forecast plane, not raw PromQL. (Critique C2 — do not author a parallel `cost_limit_cents`.) |
| **The Viggy promessa + OutcomeChain + denshin (NATS)** | `BreathePromise` reuses the promessa machinery; provisioning/admission/auction events flow on the NATS nervous system. *Honest: denshin's NATS bridge is unshipped — federation (M8) gates on it.* |

---

## §4. K8s integration — consume / compose / Viggy-replace

| Tool | Relationship | How |
|---|---|---|
| **HPA** | Consume (signal) | `Previsor` reads `HPA.status.desiredReplicas` → implied pending pods. Never writes HPA's fields. HPA decides replicas; breathe provisions nodes to host them. |
| **VPA** | Consume (signal) | Ingests `VPA.status.recommendation`. `CapacidadeProof` *rejects at admission* if VPA's new request would breach `Densa`/never-swap — a bad VPA size becomes parse-time-rejected, not a post-facto OOM. |
| **Cluster Autoscaler** | Viggy-replace | CA is reactive; breathe is predictive. Both writing ASG `desired` = race → disable CA; the band law owns node count. |
| **Karpenter** | **Compose (M1–M5), replace only later** | **The M1–M5 stance is "predictive layer *above* Karpenter"**: breathe emits "need N nodes of tier T"; Karpenter's NodeClaim/consolidation/bin-packing/interruption-handling finalizes. *Replacing* Karpenter means re-implementing its hardest-won logic (consolidation, AMI/subnet/SG selection, interruption) — deferred until the auction mechanics (§5) actually exist. (Critique C1.) |
| **KEDA** | Compose | breathe reacts to KEDA's *induced* pending pods, not its `ScaledObject`. KEDA drives event→replica; breathe provisions nodes. |
| **Scheduler + node readiness** | **Compose via taints** | The `Viveiro` admission must not fight the kube-scheduler (kubelet owns `Node.status.conditions`). So: an un-admitted node carries a `breathe.pleme.io/unadmitted:NoSchedule` **taint**; the `Admissor` removes it on `Admitido`. Pods schedule only against admitted nodes *because the taint repels them* until admission — reconciling the two authorities without breathe writing `Node.status`. (Critique C3.) |
| **PodDisruptionBudget** | Compose (gate) | `Encolher`/drain respects `PDB.minAvailable` via the Eviction API. Draining a PDB-blocked pod is gated, never a raw delete. |
| **Descheduler** | Viggy-replace | `DecisaoForma::Encolher` + the fallback order = typed consolidation via the Eviction API. |

---

## §5. The hard, deferred sub-problems (named, not hidden)

The auction and predictor are **typed envelopes around genuinely-unsolved
problems.** Naming them is the honest move; pretending they are settled is the
sin. Each is an explicit milestone, not a footnote.

1. **Spot bid strategy + interruption (M3).** A validated spot node is **not** a
   proof it survives 90 seconds — admission-validity is point-in-time, spot
   durability is future-tense (the same category split as §2's pointwise-OOM).
   The auction must specify: the bid (max-price cap / market-follow / percentile
   of on-demand), the **interruption FSM** (the 2-min AWS / 30-sec GCP termination
   signal → `Cordoado → Drenando` drain), and the rebalance-recommendation
   handling. Until then `Forma::NodeSpot` is observe-only.
2. **Multi-cloud / hardware-plurality arbitrage (M5–M6).** The vision's "instance
   type, a rack, a region" needs heterogeneous shapes the M0 enum defers to
   `Custom`: **heterogeneous accelerator selection** (A100 vs H100 vs MIG slice
   vs inferentia, by $/FLOP), **bare-metal / NUMA topology**, **arch arbitrage**
   (arm64/Graviton — today's biggest cost lever). These are named catalog entries
   with their own `Provedor` + `FormaDescriptor`, landed one at a time.
3. **The predictor's *own* stability (M2+).** The band law's static-plant proof
   **does not transfer to a forecaster** — a predictor is a dynamical system with
   lag that can oscillate and chase noise. Constraints (from `BREATHABILITY-MATH`):
   the forecast horizon must be ≥ `relief_latency` (the P8 dead-time point — you
   must predict at least as far ahead as provisioning takes, or you are always
   late); mispredict asymmetry follows the band law's fast-grow/slow-shrink (an
   over-provision costs money, an under-provision starves — bias toward money);
   and a noisy forecast driving a spot auction = **cost-thrash**, which must be
   bounded by a dwell/hysteresis on `DecisaoForma` (the §6 hybrid-stability dwell).

---

## §6. Unrepresentability ledger (tier-honest)

Per `UNREPRESENTABILITY.md` §II — *a `Result::Err` is mitigation, a missing method
is unrepresentability.* Graded against **shipped** Rust; where the code is unwritten,
the row says `designed / as-shipped: does-not-exist-yet` (the eclusa M1/M4 precedent).

| # | Illegal state | Mechanism | Honest tier |
|---|---|---|---|
| 1 | **Unvalidated resource is usable** (headline) | `Viveiro` accepts only `Admitido<T>`; sole ctor post-seal; fields private | **designed: truly-unrep** / **as-shipped: does-not-exist-yet** (lands M1; *not* an achieved tier until then) |
| 2 | **Illegal lifecycle transition** | phantom typestate `Recurso<F>`; forward-method to a non-legal phase absent → E0599 | **library path: designed truly-unrep**; **wire/controller path: parse-time-rejected** (phantom types erase at the CRD serde boundary — reconstructing `Recurso<F>` is a `try_from` check, eclusa §III.5 precedent) |
| 3 | **Spot forma carrying a `Critical` workload** | refined ctor: `spot_fraction>0 ∧ workload.Critical ⇒` shape refused | parse-time-rejected |
| 4 | **Cost overspend without a human gate** | `EnvelopeExausto ⇒` only `{RequireApproval, Escalate}` (no `AutoCorrect` arm on that variant) | **decision variant: truly-unrep**; **live spot-price drift: only-mitigated** (next-tick `Encolher` — a C2 external-world ceiling, *split honestly, not rounded up*) |
| 5 | **`Cascata` fallback cycle** | acyclic check at parse-time on catalog load | parse-time-rejected |
| 6 | **`format!()` of cloud/HCL/YAML in the provision path** | TYPED EMISSION: `magma` `Plan` / `NixValue` / `serde_yaml::Value`; clippy `disallowed_macros` | parse-time-rejected (lint) / truly-unrep where the AST is the only path |
| 7 | **Two formas writing one k8s field** | disjoint `field_manager`; SSA Conflict → `TickReceipt::Conflict` → requeue | parse-time-rejected (config) / only-mitigated at the live SSA boundary (C3 wire ceiling) |
| 8 | **Stale observation drives a decision** | `staleness > max ⇒ Stale` receipt | only-mitigated (runtime gate — C1, no dependent type for freshness) |
| 9 | **Node drained while holding a guaranteed pod** | DisruptionPolicy gate + PDB-aware Eviction API + proptest on every shrink | only-mitigated (runtime PDB check — C4 irreducibly-shared resource) |
| 10 | **Latency-critical workload on a slow-provision forma** | `workload.slo_window < forma.relief_latency ∧ no warm fallback ⇒` reject | only-mitigated (runtime parser check — C2 external-world ceiling) |
| 11 | **Convergence: every `FaseRecurso` reaches a good terminal** | BFS reachability CI test over the FSM (now incl. `Rejeitado`/`Expirado`) | only-mitigated — a CI **forcing-function** (C1: Rust has no dependent-type reachability proof; *stated, never rounded to unrep*) |

**The two permanent ceilings** (no compile error reaches them): live cost/spot
drift + "is the node actually gone" are **external-world observation** (C2/C5);
cluster-wide convergence is a **graph-reachability quantifier** (C1, CI
forcing-function). Chasing a compile error past these is wasted effort.

---

## §7. Obeying the math (`BREATHABILITY-MATH.md` §10)

The seven constraints the formal law imposes, each a hard design rule here:

1. **Predictor stays within envelope invariance + routes through `safety_clamp`.**
   `Previsor`/`PredictiveGrow` may only pre-grow within `[F_d, C_d]`; a proposal
   outside the box, or bypassing the clamp, is forbidden by construction.
2. **The auction prices within the chance-constraint.** A tenant's bid is
   `(setpoint, F_d via tail-quantile α, C_d, criticality)`; the `Otimizador`
   assigns floors so the residual OOM risk stays ≤ the declared `ε`.
3. **Validation enforces floor-from-peak before admission.** `CapacidadeProof`
   checks `node.allocatable ≥ Σ F_d` (floors provisioned from the EVT return level,
   not the mean) *before* `Admitido` — the never-swap proof at admission.
4. **Cooldown/gain clears the dead-time margin.** Per-forma `relief_latency`
   feeds the cooldown so a config can't resonate (the math's V9/P4 stability margin).
5. **Every limit is an attested-theorem tick.** Each `ProvisionReceipt` is
   BLAKE3-chained + Ed25519-signed; `kensa verify` walks the chain.
6. **`ε`/`κ` must become typed.** The chance-constraint `ε` and the classify `κ`
   (today doc-only) become `BandConfig`/`Densa` fields as the auction lands.
7. **Coupling must be declared.** Coupled dimensions (memory+cpu on one bottleneck)
   have no derivable joint safe-set — they route to the `Otimizador` allocator, not
   to independent bands. An undeclared coupling is the one thing the band law cannot
   absorb.

**And the corrected fact carried from the math:** the band law settles
*in-band* (`u ∈ [shrink_below, grow_above]`), never at the point `setpoint` — so no
provisioning decision may assume "every node sits at setpoint utilization."

---

## §8. Phased path M0 → M8

Each phase is shippable; M0–M1 are pure substrate (no rio behavior change); the
first operator value is M2 — **gated honestly on magma**.

- **M0 — Seed (zero-risk, additive).** Add `Forma` to `breathe-provider` (sibling
  enum, `NodeOnDemand` only) + `FormaDescriptor`/`Provedor` traits. One
  `NodeOnDemandProvedor` that **observes only** (counts Ready nodes, reads
  `BreatheNodePool.status`), `dry_run=true` — emits `DecisaoForma` to the log,
  never acts. proptest: `breathe_control::decide` converges on node count exactly
  as on bytes (into the deadband, §1.1). **Zero cluster mutation.** Shadow on rio.
- **M1 — `breathe-admission` + the `Viveiro`.** `FaseRecurso` phantom typestate
  **with the `Rejeitado`/`Expirado` terminals**, `Admitido<T>` sealed ctor,
  `Recurso<F>`, `Portao` trait, the 9 gate kinds (stubs returning typed
  `NotImplemented`), `CapacidadeProof` real (allocatable + never-swap). The
  unadmitted-node **taint** mechanism (§4). 3 compile-fail tests (illegal
  transitions; raw-resource-into-pool). The BFS reachability CI test (§6 row 11).
  *This is the hard type-discipline milestone.*
- **M2 — first rio value: predictive cost-bounded attested node provisioning.**
  Wire `Crescer → NodeOnDemandProvedor.provision()` via a `magma` `Plan`; the cost
  gate (consuming `attribution-forge`/`commitment-forge`, §3) enters the decision;
  `ProvisionReceipt` attested into the chain. Flip rio's `NodeOnDemand` live behind
  a `RequireApproval` gate. **⚠ Gated on magma:** magma's apply path is draft
  (still shells to tofu). If magma's node-provisioning isn't real, M2 either slips
  to a named magma milestone *or* ships an interim tofu-shim path **tier-marked
  `only-mitigated`** until magma lands — never silently assuming the capability.
- **M3 — `Forma::NodeSpot` + the interruption FSM + `Cascata`.** The spot bid
  strategy + 2-min-notice drain (§5.1); `NodeSpot --QuotaExhausted--> NodeOnDemand`.
- **M4 — the `Otimizador` joint planner (the thesis P3/P7 — the project's center
  of gravity, *not* a midpoint milestone).** Pareto/DRF allocator over the
  criticality lattice; `PromessaDependency::Meet` wires node-capacity to gate
  memory carves; all 9 admission gates real.
- **M5 — heterogeneous hardware shapes.** `Accelerator` (per-SKU $/FLOP),
  bare-metal/NUMA, arch arbitrage (§5.2) — one `FormaDescriptor`+`Provedor` each.
- **M6 — `(defprovision …)` authoring + shikumi config + the HM/NixOS module trio.**
- **M7 — observability + `kensa verify --window 30d`** (the auditor-facing verb).
- **M8 — multi-cluster federation. *Gated on the denshin NATS bridge.***

---

## §9. Convergence claims (mechanical) + what's NOT guaranteed

- **Every reachable `FaseRecurso` reaches a good terminal** (`Admitido` or a clean
  `Aposentado`/`Rejeitado`/`Expirado`) — a **CI BFS forcing-function**, not a
  compile-time proof (C1). The reject/timeout terminals (added M1) are what make
  this *non-vacuous*.
- **The band law's never-over-commit corollary** (the provisioning peer of
  never-OOM) holds for per-forma scalar carves by the §1.1 reuse.
- **NOT guaranteed:** acceptance-termination under two named livelocks —
  **cost-thrash** (a noisy forecast oscillating the spot auction; bounded by the
  §5.3 dwell) and **hot-base re-plan** (the envelope moving under an in-flight
  provision). These gate on the `Otimizador` dwell + a merge-train-style provision
  serialization, and are stated, not papered over.

---

## §10. Per-repo waiver + compliance

`skip-provisioning:` at the top of a repo's `CLAUDE.md` for non-cluster repos /
pure-library / build-time-only. **Compliance rule:** every PR that adds or changes
fleet resource-provisioning adopts this extension or carries the waiver. **Time
pressure is not acceptable.** The canonical law is `BREATHABILITY-MATH.md`; this
doc is the construction that obeys it.
