# breathe — the resource-homeostasis substrate

One running controller, one proven dimension-agnostic band law, and a catalog
of pluggable resident-problem-category providers. Every enrolled workload is
held — per category — inside a typed utilization band (default **80% used /
20% headroom**) by gentle, bounded, convergent steps, and every step is a
signed entry in a verifiable attestation chain.

> Architecture of record: [`theory/BREATHE.md`](https://github.com/pleme-io/theory/blob/main/BREATHE.md).
> `breathe` is **private** while it integrates with akeyless ephemeral-environment
> generation / instantiation / long-term-existence control.

## The crates

| Crate | Role |
|---|---|
| `breathe-control` | the proven, **dependency-free** band law + field-granular single-writer guard + directionality clamp + the pure `plan_tick` reconcile heart. Solve-once: every dimension projects into `(used, capacity)` and runs this exact law. |
| `breathe-provider` | the `ResourceProvider` trait (atomic per-category `observe`/`assign`/`release`, **never sees `decide`**) + the `Cluster` Environment trait (the mockable testability seam) + `MockCluster` (the `mock` feature). |
| `breathe-core` | the composed reconcile loop — binds the band law to a provider's I/O. breathe-core **owns** the loop; it is not inherited. |
| `breathe-catalog` | the self-describing `(defdimension …)` dimensions catalog + CATALOG REFLECTION tests. Adding a dimension **fails the build** without a catalog row. |
| `dimension-memory` | the first provider: observe working-set/limit, carve `resources.limits.memory` via **true SSA** (the owner rolls). |

## Invariants (do not regress — see `theory/BREATHE.md` §15)

- **SSA-Apply only** — every mutation is `Patch::Apply` with a per-dimension field
  manager, never `Merge`. Only real `managedFields` ownership backs the
  single-writer model.
- **Field-granular single-writer** — yield to any other manager owning the same
  field path; disjoint paths never fight (breathe ⟂ KEDA, memory ⟂ cpu).
- **Freshness-gated** — a stale metric sample never carves.
- **The band law is sacred** — a provider receives a computed target value and
  can never re-decide, widen the band, or subvert the shrink-safety clamp.

## Test

```sh
cargo test            # 32 tests: band law + convergence + single-writer + wiring + catalog reflection
```
