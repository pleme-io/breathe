# breathe — the resource-homeostasis substrate

One running controller, one proven dimension-agnostic band law, and a catalog
of pluggable resident-problem-category providers. Every enrolled workload is
held — per category — inside a typed utilization band (default **80% used /
20% headroom**) by gentle, bounded, convergent steps, and every step is a
signed entry in a verifiable attestation chain.

> Architecture of record: [`theory/BREATHE.md`](https://github.com/pleme-io/theory/blob/main/BREATHE.md).
> `breathe` is **public**. It integrates with akeyless ephemeral-environment
> generation / instantiation / long-term-existence control, and that
> integration is not a reason to keep the repo closed — nothing here carries
> akeyless-side identifiers or credentials.
>
> Corrected 2026-08-06: this line read "**private**" and was stale. Visibility
> is not a matter of intent here, it is observable — and it was observed the
> expensive way. `.github/workflows/image.yml` records that pinning a job to
> the self-hosted `camelot-builder-pleme-eks` label left **4 runs queued
> forever from 2026-07-27**, because GitHub never assigns a PUBLIC
> repository's jobs to a self-hosted runner (a fork PR could otherwise execute
> arbitrary code on the cluster). A private repo would simply have run them.
>
> That matters beyond a doc fix: repo visibility silently decides runner
> routing, whether org-level `vars.*` resolve, and whether GitHub-hosted
> minutes are free or metered. A README asserting the wrong one sends the next
> reader — human or agent — down the wrong branch of all three.

## The crates

| Crate | Role |
|---|---|
| `breathe-control` | the proven, **dependency-free** band law + field-granular single-writer guard + directionality clamp + the pure `plan_tick` reconcile heart. Solve-once: every dimension projects into `(used, capacity)` and runs this exact law. |
| `breathe-provider` | the `ResourceProvider` trait (atomic per-category `observe`/`assign`/`release`, **never sees `decide`**) + the `Cluster` Environment trait (the mockable testability seam) + `MockCluster` (the `mock` feature). |
| `breathe-core` | the composed reconcile loop — binds the band law to a provider's I/O. breathe-core **owns** the loop; it is not inherited. |
| `breathe-catalog` | the self-describing `(defdimension …)` dimensions catalog + CATALOG REFLECTION tests. Adding a dimension **fails the build** without a catalog row. |
| `breathe-dimensions` | the shipped dimension providers (memory/cpu/storage): observe working-set/limit, carve `resources.limits.*` via **true SSA** (the owner rolls). |
| `breathe-facade` | the one typed `BreatheStore` seam every operator surface drives — MCP, REST, GraphQL and gRPC all dispatch on `breathe_provider::DimensionId`, so all **ten** band kinds are reachable from all four. |

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
cargo test --workspace   # band law + convergence + single-writer + wiring + catalog reflection
                         # + the authorization-axis and surface-coherence gates
```
