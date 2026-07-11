# breathe-spread — the arch × auction × spot configuration spread as a first-class configurable dimension

> **Renamed from `breathe-auction`** (2026-07-11, executing
> `theory/CORRENTEZA.md` §1.2's named follow-up,
> `pending-correnteza: rename-breathe-auction-spread-lock`). Two crates shared
> the literal package name `breathe-auction`: this one (the config-spread
> lock) and `breathe/breathe-auction` (the node-count/forma elasticity engine
> — `Leiloeiro`/`Previsor`, live-wired into `breathe-provision::reconcile_forma`
> and `breathe-controller::node_forma`). Cargo refuses two same-named local
> packages in one resolve graph, which blocked `breathe-controller` from
> depending on both at once — hit for real wiring the correnteza M0 node-claim
> slice. The elasticity engine kept the name; this crate took `breathe-spread`
> (its own primary type is literally `AuctionSpread`), per this doc's own
> recommendation below.

The COMPUTE/AUCTION companion to the breathability variant/invariant lock
(`crates/breathe-invariant`). Breathability locks the *dimension carve* (memory /
cpu / storage / replica / database → a setpoint, dual-purpose, default-on); this
crate locks the orthogonal *compute floor* — the typed configurable PERMUTATION
SPACE every compute pool is one point in:

> **arch × spot-strategy × auction-ladder × perf-class × placement × interruption**

— every combination configurable, with a MOLDING default per use-case, under the
**never-on-demand** hard law and the **breathability dual-purpose** (cost AND
resiliency, one mechanism).

## The spread is a first-class configurable dimension

Before this lock, the six knobs lived in three unjoined languages (Rust const data
in `breathe-catalog::builder`, Ruby node-groups in `pangea-architectures`, the spot
catalog in `pangea-spot`) and an operator reconstructed a "molding" by hand. The
spread makes the permutation space itself a typed object: one `AuctionSpread`
value IS a pool's whole compute posture; the three `MOLDINGS` are the defaults; the
seven-clause invariant + the verification matrix keep every combination honest.

## The six axes (each a closed enum — anti-posture values are unrepresentable)

| Axis | Arms | Default | Notes |
|---|---|---|---|
| **arch** | `cost-optimized` · `pinned-arm64` · `pinned-amd64` | **cost-optimized** | multi-arch AUTOBUMP images make arch a FREE cost lever — the auction lands on the cheapest-deepest arch (`resolve_arch`), self-adjusting. A pin is the exception + must name a reason. |
| **spot-strategy** | `capacity-optimized` · `price-capacity-optimized` · `diversified` | capacity-optimized | mirrors `Pangea::Spot::Allocation`; `lowest-price`/`prioritized` have no arm. |
| **auction-ladder** | `evolving-degrade` · `flat-pool` | evolving-degrade | the DegradeTier total-order ladder (`breathe-catalog::builder`) — always places, never on-demand. |
| **perf-class** | `cost-floor` · `time-floor` | per use-case | spot-only. `guaranteed-wake`/`dedicated` REMOVED (they were on-demand). |
| **placement** | `single-az` · `multi-az` | storage-derived | single-instance-EBS ⇒ single-AZ; stateless / per-replica-EBS ⇒ multi-AZ. |
| **interruption** | `retirada-graceful-drain` · `retirada-node-drain` · `retry-on-reclaim` | per use-case | retirada skeleton shipped; drain agent is a LiveTODO. |

**Capacity is NOT an axis.** 100 % spot is an invariant, not a knob — there is no
on-demand arm (truly-unrep in Rust; parse-rejected at the Ruby boundary via
`CamelotBuilderNodeGroup::reject_on_demand!`).

## Arch is cost-optimized — and LOUD where the cost answer is surprising

Every conflict resolves by COST. Because the image is multi-arch, arch is free, so
the ladder picks the cheaper arch. The current cost answer resolves to a genuine
three-way split (proven not a hardcode — flip the price signal, the arch flips):

- **builder → arm** — −37 %/build-hr + ~18 % faster wall-clock (proven; CGO=0 pure-Go, arm-native). Expected win.
- **floor → x86** — the shipped CamelotNodeGroup DEFAULTS are already `m5/m5a/m6i/m6a`. **ARM LOSES HERE** — Graviton m7g/m8g large-spot is **+19 % pricier** than the cheap 2019-gen m5a x86 large-spot NOW. The spread SAYS SO loudly + inline, with the auto-flip trigger.
- **eyes → arm** — t4g burstable < t3 x86 at tiny sizes. Expected.

The `CostRationale` (number + why + `auto_flip_when`) travels WITH each decision;
the matrix FAILS THE BUILD if an x86 choice is not flagged loud (names a % + says
arm loses) — the operator's "be vocal where arm is not winning" as a CI gate.

## Tier-honest ledger (`AXIS_LEDGER`)

| Axis | State | Tier | Note |
|---|---|---|---|
| capacity (never-on-demand) | Shipped | truly-unrep-lib + parse-wire | no on-demand arm; `reject_on_demand!` |
| arch (cost-optimized) | Shipped | parse-time-rejected | per-arch node groups + multi-arch images; cost-resolved, self-adjusting |
| spot-strategy | **Design (gap)** | ceiling-C1 | EFFECTIVE on the ASG/EC2-Fleet lane; **DROPPED on the EKS-managed-NG lane** (`IgnoredOnManagedNg`) — EKS MNG does not expose `SpotAllocationStrategy`; now CI-visible |
| auction-ladder | Shipped | ceiling-C1 | DegradeTier total-order proven (always-place / never-on-demand) |
| perf-class | Shipped | truly-unrep-lib + parse-wire | cost-floor/time-floor; guaranteed-wake/dedicated removed |
| placement (AZ) | **Design** | ceiling-C2 | single-instance-EBS ⇒ single-AZ enforced; multi-AZ per-replica is the destination (single-AZ shipped interim); real subnet AZ is plan-time-only |
| interruption (retirada) | **Design** | only-mitigated | InterruptionHandler skeleton shippable; drain agent + NATS publish is a NAMED LiveTODO; retry-on-reclaim (builders) is structurally complete |

## Reference note (for the contextualizify surfaces)

The BREATHABILITY doctrine's 100 %-spot / flex-window section
(`theory/BREATHABILITY.md` §II.6) and the `/breathability` + `/camelot` skills
should point at **`pleme-io/breathe/crates/breathe-spread`** as the CANONICAL LOCK
of the arch × auction × spot spread — the compute peer of the `breathe-invariant`
dimension lock. The doctrine PROSE is the model; this crate is the typed contract
it points at (same relationship as breathability ↔ breathe-invariant).

**Follow-up (same as breathe-invariant):** fold into the breathe workspace members
(or add its own `auto-release.yml`) once the concurrent band-crate work lands, so
the matrix runs in breathe CI + ships via AUTO-RELEASE.
