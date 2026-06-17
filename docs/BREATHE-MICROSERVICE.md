# breathe → a scalable Urdume microservice — the refactor plan

> Authored 2026-06-16 from a 10-agent analysis + 3-agent adversarial verification.
> This doc is the **hardened** plan: the design synthesis with every verifier
> correction folded in. Where the first-pass design was wrong, the correction is
> called out inline (▲ VERIFIER FIX).

The destination is named first (Operating Principle #0), then the phased path.

---

## 0. The destination

breathe becomes **ONE Urdume-compliant Rust service binary whose elasticity is a
pure function of shikumi config.** The identical image runs:

- **very small** — single pod, CRD-spec + RAM, **zero external infra**, byte-identical
  to today's rio reality; and
- **extreme** — N sharded controller replicas behind a Redis leader-lease + band
  work-sharding, with all durable decision/attestation/forecaster/cooldown/cost/
  node-lifecycle state in a Postgres backend (SeaORM-destination, SQLx-floor — §5),
  a Redis hot-cache + a shared fleet-wide mutation-RPC budget,

…with **no band-law or loop code path difference** between the two — only the
`BreatheBackend` enum arm and a handful of bounded numeric knobs change.

**Preserved untouched:** the dimension-agnostic band law (`breathe-control`), the
seven-beat `reconcile_one` (`breathe-core`), the one generic `BandProvider`, the
`band_kind!` CRD family, the `Cluster`/`DimensionDescriptor` seams. The refactor
wraps them in three things they lack: (1) a **config-selected storage backend**,
(2) a **coordination layer** (leader-election → sharding) that restores the
per-object-serialization the chart's `replicaCount:1 + Recreate` singleton
hard-codes today, and (3) the full **Urdume L0–L9** trama set.

Runtime fork: decisively the **container Deployment** shape (stateful, long-running,
Postgres-backed) via the substrate `rust-service-flake` path — NOT wasm Servico/function
(interim until `caixa-core` grows a typed `:runtime container` slot).

**The one structural property that changes:** the singleton constraint becomes a
config-selected elasticity spectrum. Everything else is preserved.

---

## 1. Source-of-truth law (the central tension — resolved in code, not prose)

Per the magma-operator precedent (`theory/MAGMA-OPERATOR-BACKEND.md`):

| State | Authority | Role |
|---|---|---|
| **DESIRED** (band spec, knobs) | **CRD `spec`** (GitOps) | Postgres NEVER authors it |
| **EXECUTION/DECISION** (counters, decision chain, forecaster window, cooldown clock, cost integral, node ledger) | **Postgres** (or InMem in small tier) | durable authority |
| **service config** (scale, pools, secrets) | **shikumi YAML** (+ `BREATHE_` env) | §2 |
| CRD `status` | a **lossy, eventually-consistent projection** | best-effort patched so `kubectl wait`/Grafana keep working; rebuilt from the store |

**Precedence (written invariant):** `env > shikumi-YAML > CRD-overlay > prescribed_default`
for service config; **CRD-spec authoritative for desired**, **store authoritative for
decision**, **CRD status authored by nothing but the projector**.

### ▲ VERIFIER FIX 1 — the counter authority must be singular *in code*, not prose

The first-pass design said "DB authoritative, status is a projection" while leaving
`status_for` doing a **read-modify-write off prior CRD status**
(`breathe-runtime/src/lib.rs:311` — `s.carves_total = prior_n(..) + delta`). If the
DB writer *also* increments, there are **two independent accumulators** that diverge on
any missed/retried status patch. The magma precedent stores a derived *pointer* in
status, not a mutated counter — it does NOT cover this.

**Fix (a real code change, not glue):** move the `+delta` increment for
`carves_total/deferrals_total/conflicts_total` **out of `status_for` and into the
`DecisionLog::append` writer** (the same tx that appends the decision row). `status_for`
**reads** the post-increment value from the `band_registry` row (carried on
`BandRecord`). In the InMem tier the InMem `DecisionLog` does the identical fold, so
**both tiers read the projected counter from the store and neither does a status-side
read-modify-write.** Second accumulator eliminated; status is a true projection in both
tiers.

### ▲ VERIFIER FIX 2 — CRD status is a *lossy* projection, say so

`decision_log` is the full attestation history; `status.history` (cap `HISTORY_MAX=16`)
+ the status counters are a **bounded view**. Do not call status "rebuildable/faithful";
it is a lossy, eventually-consistent (≤1-tick lag) projection of a deeper DB history.

**Cross-system atomicity caveat:** a k8s status patch cannot join a PG tx. Resolution
is **DB-authoritative + idempotent re-projection**, not two-phase commit. A status-patch
failure no longer loses state (the DB has it); status lags by ≤1 tick.

---

## 2. The small↔extreme scale model — one binary, selected entirely in shikumi

One typed `shikumi::TieredConfig` root (`breathe-config` crate, `#[serde(deny_unknown_fields)]`,
`BREATHE_` env overlay, `ConfigStore::load_and_watch` + ArcSwap + notify hot-reload,
HM/NixOS/Darwin trio). The `scale` section is **typed enums + bounded numerics — never
a bool, never a hidden fallback** (★★ MAGMA-NATIVE "config decides"):

```
scale.store:        InMemory | Postgres { dsn: SecretRefShape, pool_max, pool_min }
scale.cache:        None     | Redis    { url: SecretRefShape, ttl_secs }
scale.coordination: SingleReplica | LeaderElection { lease_ms } | Sharded { replicas, hash }
scale.window:       bounded numeric (default 6)
scale.reconcile_workers: bounded numeric    # ▲ FIX 4: BREATHE.md:687 self-protection knob
scale.dimensions:   [mem, cpu, storage, overview, ...]   # which axes this instance owns
```

**`prescribed_default` = VERY SMALL** (today's rio reality, byte-identical):
`store=InMemory, cache=None, coordination=SingleReplica, window=6,
dimensions=[mem,cpu,overview]`. In this tier the InMem store **IS** the authority; **zero
new infra dependency** — deployable on a bare single-node cluster. PgRedis is strictly opt-in.

**EXTREME** = `store=Postgres, cache=Redis, coordination=Sharded{replicas:N, hash:rendezvous}`.
The SAME `reconcile_one` runs; startup resolves `BreatheBackend::PgRedis`. Each replica owns
a band hash-range (`breathe:shard:{id}`) so exactly one replica reconciles a given band; a
band that migrates shards keeps its forecaster window + cooldown clock because both are
DB-durable (re-read on handoff). The fleet honours **one** shared `breathe:ratebucket`
(samba LeakyBucket).

**INTERMEDIATE** = `LeaderElection` — the cheapest scale step (no sharding): only the leader
runs the Controller streams (warm-standby HA). **This is already breathe's documented
design-of-record** (`docs/BREATHE.md:610,:687`) — M3 *implements* it, doesn't invent it.

Hot-reload is tier-honest: safe numerics (window, requeue, cooldowns) hot-apply via
`on_reload`; store/coordination changes log `requires-restart` (a pod-spec change,
GitOps-reconciled). Unimplemented arms return a **TYPED not-yet-implemented error**, never a
silent fallback. The chart drops `replicaCount:1 + Recreate` and renders replicas/strategy/
backend-env from `.Values.scale` — the deploy artifact is a projection of the config.

### ▲ VERIFIER FIX 3 — sharding is THROUGHPUT, not a new safety mechanism

The double-carve defense is the EXISTING per-reconcile `managedFields` yield + the
disjoint-SSA-field-partition (`BREATHE.md:689`), **not** the Redis shard map (graded
never-authority). A stale shard map / netsplit / lagging reassignment can let two replicas
both think they own a band — but they carve the **same field under the same field-manager**
and the loser **yields**, so double-reconcile is **harmless, not impossible**. Therefore:

- M4's headline is **"extreme throughput, safety-bounded by the same only-mitigated SSA
  guard"** — NOT "proven safe." Do not round up only-mitigated to unrepresentable.
- The **type-level fencing-token** fix is a named, out-of-scope backlog row (§7).
- "Proven extreme" cannot be *earned* until **BU10** (real cloud node birth via magma Plan
  Provedor) exists — that is the only path where two writers are *unrecoverable*. Until then,
  cap the validation claim at **"kwok/DryRun multi-replica shadow."**

---

## 3. The data layer — Postgres/SeaORM + Redis, three traits, one config switch

**Storage abstraction = new crate `breathe-store`, async traits = the Environment-trait seam
breathe already proves.** `reconcile_one` takes `&dyn DecisionLog + &dyn SampleCache`
**exactly as it takes `&dyn ResourceProvider` today** — band law + loop unchanged, glue picks
the backend. Two impls per trait, selected by shikumi: **InMem** (`Mutex<Vec>`/`HashMap` =
today's behavior, byte-identical) | **PgRedis** (append-only `INSERT` with BLAKE3 prev-hash in
the SAME tx as the counter bump; Redis TTL keys write-through to PG).

### ▲ VERIFIER FIX 5 — split the two storage ROLES; don't overload `BreatheStore`

`breathe-facade::BreatheStore` is a **CRD-projection facade** (reads live kube objects,
patches spec) — it is NOT a durable store. **Keep it as-is.** Make `DecisionLog`/`SampleCache`/
the `band_registry`-write a **separate durable-store trait set**. API band reads serve the CRD
projection by default; only decision/forecaster/cost/node-ledger reads go to PG; durable
decision history is a **new** endpoint backed by `DecisionLog`, not an overloaded
`get_band_record` on the facade. (This is what removes the dual-source-of-truth.)

### Postgres entity set (SeaORM `DeriveEntityModel`, schema `breathe`, **add-only**)

1. **`band_registry`** PK(kind,namespace,name) — durable mirror of spec rollup + the cumulative
   `carves_total/deferrals_total/conflicts_total/current_limit/current_phase/last_util/
   generation/observed_generation` that today live in `BandStatus` and must survive restart.
   **Carries the `epoch` token** (▲ FIX 7).
2. **`decision_log`** APPEND-ONLY content-addressed chain — a NEW richer row built from the
   `TickOutcome` keystone (`breathe-core/src/lib.rs:123`), a **4th consumer** alongside
   `status_for/event_for/metrics_for` (zero drift). Columns: id, band_ref FK, seq (monotone
   /band), occurred_at, receipt_kind (11 `TickReceipt` arms), from/to_limit, observed_used/
   capacity/util, freshness_secs, action_class, edge_tier, policy, dry_run,
   `content_hash=BLAKE3(canonical row)`, `prev_hash`.
   - ▲ **FIX 6 (attestation evidence):** this is **not** a 1:1 `TrendSample` lift (TrendSample
     at `crd:79` carries only `{time,util,limit,phase,decision}`). The `[0u8;16]` placeholder is
     in the **apply receipts** (`AppliedReceipt`/`AssignReceipt` across provider/apicall/jmx/kube/
     host/configreload), NOT in CRD history. The BLAKE3 chain fills **both** the missing
     apply-receipt hash AND the missing decision chain — two surfaces, one chain. This is
     breathe's first attestation surface (it has none today).
   - ▲ **FIX 9 (concurrency guard):** `UNIQUE(band_ref, seq)` + the append reads the tail under
     `SELECT ... FOR UPDATE` on the `band_registry` row (same tx). A forked `prev_hash` becomes a
     constraint violation, not a silent chain split; the row-lock serializes concurrent appenders
     during a transient dual-leader / shard-handoff overlap — a **DB-enforced single-appender**
     for the chain specifically.
3. **`forecaster_window`** (+ child `forecaster_sample`) — durable `LinearTrendPrevisor` state so
   restart resumes prediction, not cold-start.
   - ▲ **FIX 8 (two predictive stores, not one):** there are **two** predictive surfaces. (a)
     per-BAND mem/cpu prediction reads `prior_used` from `obj.status().observed_used`
     (`main.rs:215`) — **already durable in CRD status**, survives restart with NO Postgres; this
     is the live rio surface (pangea-database). (b) per-POOL node demand uses the RAM-volatile
     `LinearTrendPrevisor` window (`Ctx.forecasters Mutex<HashMap>`). **Only (b) is the
     `forecaster_window` table.** Scope the "restart loses prediction" risk to the pool path ONLY;
     the per-band path is byte-identical in both tiers, and the small-tier "zero Postgres" claim is
     therefore **stronger than stated**.
4. **`cooldown_state`** — band_ref, last_change_epoch, cooldown_until_epoch.
   - ▲ **FIX (cooldown source):** `Band::last_change_epoch()` reads from status today
     (`crd:606,797,1008`); `in_cooldown` is computed from it (`main.rs:201`). Route `in_cooldown`
     through `cooldown_state` in **both** tiers (status keeps a `cooldown_remaining_seconds`
     projection field for kubectl/Grafana but it is NOT the gate input). This is a Band-trait
     accessor rebind — the roadmap's "loop unchanged" softens to **"band LAW unchanged; the
     cooldown/predictive/counter ACCESSORS rebind to the store seam."**
5. **`node_pool_lifecycle`** (+ child `provisioned_unit`) — the kwok/magma provision ledger.
6. **`densa_envelope`** — namespace, bounds(jsonb), reserve, cost_sla_cents, cost_spent_cents
   (the durable monotone cost integral for BurnRateBand).
7. **`band_overview_rollup`** — materialized aggregate from `band_registry`.
8. **`catalog_projection`** OPTIONAL — regenerated from the compile-time `CATALOG`/`FLORESTA`
   const on every startup, read-only. **Authority stays in code** (catalog reflection tests
   unchanged); drop the table if cross-process catalog SQL isn't actually needed.

### Redis layer (cache + ephemeral-shared, TTL'd, reconstructible, **NEVER authority**)

`breathe:sample:{ref}` (Observation + rate window, TTL≈max_staleness so stale==miss) ·
`breathe:band:{ref}` (hot snapshot for sub-ms MCP/REST reads) · `breathe:overview:{cluster}` ·
`breathe:leader` (SET NX PX lease) · `breathe:shard:{id}` (rendezvous-hash assignment) ·
`breathe:ratebucket:{provider}` (ONE shared samba LeakyBucket) · `breathe:forecaster:{pool}`
(write-through hot mirror). *(Distinct from `breathe-apicall`'s redis-as-actuation-target role.)*

### Central data structures (one typed owner each, written through the trait)

`BandRecord` (merged desired+durable view, replaces the status-counter read-back) ·
`DecisionEntry` (from `TickOutcome`) · `SampleWindow` (the `LinearTrendPrevisor` `VecDeque`
behind `SampleCache`) · `OverviewAggregate`. The controller never touches sqlx/redis directly.

### ▲ VERIFIER FIX (prior_used unification — the "no code path difference" guarantee)

Route `prior_used` through `sample_cache.prior_used(&band_ref)` in **both** tiers (InMem returns
the last RAM sample = today's status read; Pg returns the forecaster_window tail) so
`reconcile_one` **never reads `obj.status().observed_used` directly**. The small tier's predictive
value then comes from the same logical seam as extreme — preserving the "no code path difference"
claim. **Gate it with a cross-tier property test** (§5 M0/M2): identical observation sequence →
identical PredictiveGrow decisions under InMem vs a testcontainers PgRedis.

### Add-only schema discipline (Urdume L0)

Never DROP/DELETE a column/table — retire via `#[deprecated]` + stop-writing migration +
build-time error-on-use; the column stays forever holding data. Expand-only decomposition
(add→backfill→dual-write→cut→deprecate); idempotent `IF NOT EXISTS` + checksummed; **shinka
`DatabaseMigration` CRD + `shinka-wait` initContainer** gate pod start on `phase==Ready` (the one
truly-unrepresentable pod-level gate).

---

## 4. Urdume L0–L9 mapping

| L | Trama | Status | What breathe needs |
|---|---|---|---|
| **L0** | data spine | **BUILD** (the heart) | the 8 SeaORM/SQLx entities + add-only migrator + shinka gate; behind `scale.store`; InMem keeps small byte-identical |
| **L1** | shikumi config | **BUILD** | `breathe-config` crate (`BreatheServiceConfig` to avoid the CRD name collision); replaces the env reads + the `BreatheConfig`-CRD load at `main.rs:346`; `pending-shikumi:M1` until landed |
| **L2** | runtime foundation | **BUILD interim** | `RUN_MODE` dispatch {Api\|Migrate\|Worker\|Promote}; dual-port serve; aggregated /health+/ready (pg+redis); graceful drain; coordination (lease/shard) wraps the `join!`. **ServiceBuilder is axum-skew-blocked → hand-rolled main()+tsunagu is the shipped path, ServiceBuilder the named destination** |
| **L3** | API spine | **HAVE (extend)** | `breathe-api-server` already serves REST+GraphQL+gRPC over the one `BreatheStore` — **ahead of lilitu/hanabi (GraphQL-only)**. Promote `spec/breathe.openapi.yaml` to canonical + `forge-gen.toml` → fan out MCP/SDKs/docs. **▲ FIX: schedule the api-server DEPLOYMENT (M3/M5) — it exists in code but is undeployed/unreferenced by any chart** |
| **L4** | BFF/federation | **WAIVER** | `skip-urdume-L4: not-a-federated-edge` — in-cluster control plane, not a product edge. (If the rio portal ever consumes its GraphQL, author an async-graphql subgraph.) |
| **L5** | caixa SDLC | **WAIVER (blocked upstream)** | `caixa-core::kind` has **no `:runtime` slot** → a Postgres-backed Deployment can't be a `(defcaixa :kind Servico)` yet. Ship via the `rust-service` skill / substrate `rust-service-flake`; `pending-caixa:runtime-slot`. Becomes `(defcaixa :kind Servico :runtime container)` when the slot lands |
| **L6** | delivery | **HAVE off-standard → BUILD** | today a vendored chart (`skip-auto-release`, private). When public: chart → `pleme-lib (>=0.27.0)` one-liners + `compliance.overlays`; `rust-service-flake` dockerTools image (NO Dockerfile) on `rio-build-01`; 3-line AUTOBUMP. **Until public: keep the vendored chart, drop `replicaCount:1+Recreate`, render from `.Values.scale`** |
| **L7** | mesh | **WAIVER** | `skip-urdume-L7: single-servico`. A NetworkPolicy (via pleme-lib) is the only mesh-adjacent artifact (aresta is unproven, parked) |
| **L8** | identity+secrets | **BUILD thin, interim** | PG dsn + Redis url born via cofre `SecretRef` → ESO/SOPS → secretKeyRef, never a literal; front any human api-server endpoint with saguao, read `x-user-*` headers only (never authn in-process). **Interim: ESO suspended on rio → SOPS Secrets; vigia unwired** |
| **L9** | observability+outcomes | **HAVE strong → BUILD thin** | already rich (breathe MCP, ShadowWouldApply, rio dashboards). Add `init_observability('breathe')` + `define_metrics!` on the API axum app; one-line `pleme-lib.observabilityBundle`; reconcile the rio dashboards onto a Monitorable Pangea architecture (no hand-authored Grafana JSON). **DESTINATION: breathe's never-OOM + convergence SLA as `(defpromessa)` → PromessaController → OutcomeChain → `kensa verify`** — but those are fleet M1+; shipped continuous-correctness = FluxCD + the new `decision_log` BLAKE3 chain (breathe's first attestation surface) |

**Net: 8/10 tramas built + 2 legitimate waivers (L4/L7).** L5 is a fleet-wide blocker breathe
must not solve alone. The verifier graded Urdume-fidelity **sound**, conditioned on accepting
the `:runtime`-slot gap.

---

## 5. Roadmap (each milestone a shippable increment)

- **M0 — `breathe-store` crate + the three traits + InMem impl** (zero behavior change, zero new
  infra). The unblock: every later milestone is a new trait impl behind a config arm. Thread
  `&dyn DecisionLog + &dyn SampleCache` through `reconcile_one` exactly as `&dyn ResourceProvider`.
  Build `DecisionEntry` from the `TickOutcome` keystone. **▲ FIX: move the counter `+delta` into the
  InMem DecisionLog fold; route `prior_used`+`in_cooldown` through the store seam (accessor rebind,
  not glue). ▲ Add a GOLDEN behavior-preservation test: same input sequence → identical TickOutcome/
  carve decisions, InMem-vs-status-read.** No Postgres/Redis/sea-orm yet.

- **M1 — `breathe-config` (shikumi) + the scale enum surface.** Variable config COMPLETELY in
  shikumi; the scale spectrum is typed + hot-reloadable even though only `SingleReplica/InMemory` is
  wired. `BreatheServiceConfig` root; `prescribed_default=VERY-SMALL`; the `scale.*` enums (+
  `reconcile_workers`); PG/Redis as `SecretRefShape`; 5-test ladder + Nix trio. **▲ FIX: pin the
  four-way precedence rule as a written invariant; resolve the `BreatheServiceConfig`-vs-`BreatheConfig`
  collision** (CRD stays as a thin overlay or is retired — decide). Unimplemented arms → typed error.
  Clears `pending-shikumi:M1`.

- **M2 — durable backend (PgRedis store half) + migrations.** Decision/attestation/forecaster(pool)/
  cooldown/cost/node-lifecycle state survives restart + is shareable; opt-in behind
  `scale.store=Postgres`. Append-only `INSERT` + BLAKE3 `prev_hash` in the **same tx** as the counter
  bump (`UNIQUE(band_ref,seq)` + `FOR UPDATE` guard — ▲ FIX 9). **▲ DECISION (resolve BEFORE M2):
  SQLx `query_as!` is the shipped floor; SeaORM is the named destination.** breathe would be the FIRST
  fleet consumer to make SeaORM the durable SSoT on an RC pin (`sea-orm 2.0.0-rc.30` + lilitu's
  PG-55P04 one-migration-at-a-time workaround) — ship the chain+counters on SQLx now, cut to SeaORM at
  2.0 stable via a typed migrator arm. **▲ Add a cross-tier property test: identical observations →
  identical decisions, InMem vs testcontainers PgRedis.**

- **M3 — Redis cache + leader-election** (the cheapest scale step → warm-standby HA). Run `replicas>1`
  safely without splitting a cache; drop `replicaCount:1+Recreate`; render replicas/strategy/backend-env
  from `.Values.scale`. Only the leader runs the Controller streams. **Implements the already-documented
  topology (`BREATHE.md:610,687`), not a new invention.** Tier-honest: leader-election does NOT make the
  single-writer guard unrepresentable (a stale-elected old leader can overlap). **▲ Schedule the
  api-server deployment here** (second container / sidecar / separate Servico — decide).

- **M4 — band-sharding** (true horizontal throughput, the extreme tier). `Sharded{replicas,hash:rendezvous}`;
  each replica filters to its owned hash-range (restores per-object serialization). Shard-reassignment
  re-reads cooldown+forecaster from the DB. **▲ FIX 7: add a shard-handoff fence — bump an `epoch` token
  in `band_registry`; the old owner's `patch_status` + `DecisionLog::append` assert `token==current` in
  the same tx, so a losing replica's late write is REJECTED, not clobbering.** Validate on a multi-replica
  rio bed against the breathe MCP + ShadowWouldApply. **Headline: "extreme throughput, safety-bounded by
  the same only-mitigated SSA guard" — NOT "proven safe" (gated on BU10 for the dangerous surface).**

- **M5 — Urdume delivery + observability finish (L6/L9)** when breathe goes public. Drop
  `skip-auto-release`; chart → `pleme-lib` named-templates + `compliance.overlays`; `rust-service-flake`
  dockerTools image on `rio-build-01`; 3-line AUTOBUMP. `init_observability` + `define_metrics!` on the
  API app; one-line `observabilityBundle`; reconcile the rio dashboards onto a Monitorable architecture.
  Author the never-OOM + convergence SLA as a `(defpromessa)` skeleton (shipped proof = FluxCD + the M2
  `decision_log` chain). cofre `SecretRef` for PG/Redis (SOPS-interim). Mark every interim tier-honestly.

---

## 6. Preserved invariants (tier-honest — never round up)

1. **ONE `safety_clamp` envelope** (never-OOM). The pure band law is untouched by every milestone;
   the storage/coordination layer wraps `reconcile_one`, never reaches inside the deciding.
2. **SINGLE-WRITER stays `only-mitigated`** (per-reconcile `managedFields` yield). Leader-election (M3)
   + sharding (M4) **restore the per-object-serialization assumption** the counter/cooldown logic
   depends on, but do **NOT** make a force-applying-peer conflict unrepresentable. The fencing-token
   type-fix is out of scope (§7).
3. **CATALOG REFLECTION** — the compile-time `CATALOG`/`FLORESTA` const + reflection tests stay the
   single authority; `catalog_projection` is a read-only regenerated projection, never a second source.
4. **SHADOW-FIRST `dryRun`** — `effective_dry_run = band.dryRun || !pool.writeEnabled` unchanged; M4
   multi-replica validation observes ShadowWouldApply via the MCP before any LIVE flip; `decision_log`
   records `dry_run` per row (shadow decisions are durably attested).
5. **TYPED-SPEC TRIPLET** — every new interpreter (`DecisionLog`/`SampleCache`/PgBacked) ships behind an
   Environment-style async trait with InMem mocks; no test needs a real Postgres/Redis (testcontainers
   for integration only); unimplemented scale arms return a typed error, never a silent `Ok`.
6. **Cluster / DimensionDescriptor seams** unchanged — the generic `BandProvider<C,D>` + `band_kind!`
   keep working; the storage backend is a **4th `&dyn` argument**, orthogonal to these seams.
7. **CRD spec = GitOps desired authority** (never Postgres-authored); **CRD status = lossy projection**
   (`kubectl wait --for=condition=Ready` keeps working).
8. **ADD-ONLY / NEVER-DROP schema** — retirement is deprecate + error-on-use; the column stays forever.

---

## 7. Hardening backlog (named, out-of-scope — burn down later)

- **Type-level fencing token** for the single-writer guard (would promote `only-mitigated →`
  truly-unrepresentable for the double-carve on a shared field). Tracked exactly as eclusa /
  `UNREPRESENTABILITY` track their `only-mitigated` rows.
- **BU10 — magma Plan Provedor** (real cloud node birth). The only mutation surface where two writers
  are *unrecoverable*; "proven extreme" is unearnable until it lands. Externally gated on draft magma.
- **ServiceBuilder adoption** (L2) once the axum-0.7↔0.8 skew is fixed.
- **caixa `:runtime container` slot** (L5) — a fleet-wide `caixa-core` change, not breathe's to land.
- **OutcomeChain / PromessaController** (L9 fifth-beat) — upgrade the `decision_log` BLAKE3 chain to the
  full tameshi/sekiban Ed25519 OutcomeChain when it ships fleet-wide.

---

## 8. Decisions needed (resolve before the cited milestone)

1. **L1 (before M1):** `BreatheServiceConfig` (shikumi) vs `BreatheConfig` (CRD) — keep the CRD as a
   thin GitOps overlay, or retire it once shikumi owns config? Pin the four-way precedence rule.
2. **L0 (before M2):** SeaORM (RC `2.0.0-rc.30`) now vs SQLx `query_as!` floor + SeaORM-at-stable cutover.
   Recommend: **SQLx floor now, SeaORM destination** (breathe shouldn't be the first fleet consumer to
   bet the SSoT on an RC).
3. **Scope:** the refactor targets ONLY `breathe-controller`'s scale; the host-agent stays a per-node
   DaemonSet with its in-process cache (confirm). If host reconcile is ever folded in, its cpu/io rate
   cache becomes a split-state problem needing the same Redis treatment.
4. **Status lag:** confirm no consumer (Grafana alerts, `kubectl wait`) breaks on a ≤1-tick status lag.
5. **History depth:** is `>16`-sample decision history needed (a reason to make `decision_log`
   authoritative even in the small tier), or is the cap-16 ring + Postgres-as-extreme-durability enough?
6. **Public timing:** the go-public date gates M5 — or migrate to a private OCI `pleme-lib` chart first?

---

*Compliance markers to commit to `breathe/CLAUDE.md` per the Urdume/org per-PR rule:*
`pending-shikumi:M1`, `pending-caixa:runtime-slot`, `skip-urdume-L4: not-a-federated-edge`,
`skip-urdume-L7: single-servico`, `skip-iac-merge: single-operator-no-team`,
and (until public) the existing `skip-auto-release: internal-only`.
