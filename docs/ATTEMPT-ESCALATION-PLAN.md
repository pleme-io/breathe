# Attempt escalation — implementation plan

The build plan for [`theory/BREATHABILITY-NODE.md` §2.5](../../theory/BREATHABILITY-NODE.md)
(attempt-scoped escalation), with the private estate CI as the first instance. Companion to
[`REALLOCATION.md`](REALLOCATION.md) §3, which is where the cost case lives.

**Goal.** An attempt starts on the cheapest capacity its tolerance permits.
When an attempt dies *because its seat was reclaimed*, the next attempt of that
same work lands on stable capacity — on-demand if that is what it takes. Cheap
first attempt, guaranteed landing, cost optimized across the attempt chain.

## 0. The precondition that outranks everything else

**Karpenter's interruption controller is not running on one cluster.** Not "reacting
slowly" — not instantiated. `settings.interruptionQueue` is empty, and the chart
emits `INTERRUPTION_QUEUE` only inside a `{{- with .Values.settings.interruptionQueue }}`
guard, so the controller is never constructed. The repo's own note (karpenter
`release.yaml`, corrected 2026-07-26) states the consequence exactly:

> the ENTIRE 2-minute EC2 spot warning is discarded unread on every single
> reclaim: no cordon, no drain, no pre-emptive replacement.

Also lost: the earlier Rebalance Recommendation, AWS Health scheduled-change
notices, and the terminal-state sweep that garbage-collects a NodeClaim whose
instance died out-of-band.

This is not a nice-to-have for this plan; it is the plan's **only source of
capacity-side truth**. §2.5 requires attribution from the capacity side and
explicitly forbids inferring reclaim from exit status. With no interruption
controller there is no capacity-side signal at all, so attribution is
unimplementable and **routing short jobs to spot would be strictly worse than
today**: jobs would die with no drain, no warning, and no way to tell a reclaim
from a genuine failure.

The queue and EventBridge fan-in are already declared as GitOps
(`private-estate-eks-karpenter-interruption-infrastructuretemplate.yaml`, 10 resources)
and the controller role's `AllowInterruptionQueueActions` grant is rendered. Both
are `suspend: true` on a named blocker: `private-estate-eks-operator`'s IAM permissions
**boundary** allows no `sqs:*` and no `events:*`, so magma cannot create them.
Widening it needs a human-SSO apply against
`workspaces/private-estate-eks-operator-iam/`.

**Gate 0:** interruption controller live, and a real reclaim observed producing a
cordon + drain. Nothing downstream ships before this. It is also independently
worth doing — the cluster already runs spot pools at a measured **7.7%
interruption per launch** (50 evictions / 648 launches / 50h) with the grace
period unused.

## 1. Why short CI jobs are the right first instance

Measured job durations:

```
vendor-mirror.yml             29 jobs   median  2.5m   max   4.0m
image-release.yml            209 jobs   median  0.0m   max  92.0m   (bimodal)
sql-apply-image-release.yml    6 jobs   median  0.1m   max   8.0m
breathe-band-lint.yml          2 jobs   median  0.2m   max   0.2m
```

The 7.7% figure is per *launch* over a ~50h lifetime; a 2.5-minute job occupies a
vanishingly small slice of that window, so its exposure is far below 7.7%. Short
jobs are the low-risk end of the spot trade, and they are also idempotent by
construction here (pull-scan-mirror, lint, gate).

Long non-checkpointable Nix builds stay on-demand, unchanged. That is not
caution, it is a measured incident: a 40-minute build "reclaimed twice at ~40min
mid-compile, losing the whole build each time" (builder nodepool comment,
2026-07-23).

## 2. The packing half — right-sized runners

The current ARC runner requests `cpu: 3`, `memory: 24Gi`, `ephemeral-storage: 50Gi`.
An `m6a.2xlarge` has ~30Gi allocatable, so **one runner fits per node by memory**.
That single field is why 15 jobs produced 14 nodes.

A short-job runner needs a fraction of it. Recon confirms the short path runs
`zot-pull-scan` / `oci-image-push` / `doca` — Rust, registry-to-registry, **no
docker** — so it also does not need `containerMode: dind`.

**Deliverable:** `runner-scale-set-pleme-short.yaml`, a sibling HelmRelease
(`gha-runner-scale-set` 0.12.1, the existing one is 85 non-comment lines), with
`minRunners: 0`, small requests, no dind, and a nodeSelector/toleration onto a
spot pool.

**Sizing is measured, not guessed — done 2026-08-05** against the metric spine
(`max_over_time`, 24h, `namespace="builder-ci", container="runner"`):

```
the self-hosted builder pool-…-tg58x     0.80 Gi     1.15 cores
the self-hosted builder pool-…-s8hlp     0.35 Gi
private-estate-builder-eks-…-4f5d2           0.59 Gi     0.96 cores
private-estate-builder-eks-…-b8xsz           5.58 Gi     2.44 cores   ← long-build set
```

The pleme short-job runners peak at **0.28–0.80 Gi**. Against a **24 Gi**
request that is **30–80x oversized**, and even the heaviest observed runner
(5.58 Gi, on the long-build set) sits 4x under it. CPU peaks
0.96–2.53 cores against a 3-core request, so CPU is roughly right and **memory
is the entire mis-sizing**.

Proposed short-job shape: **request `memory: 2Gi`** (>2x headroom over the
observed 0.80 Gi peak) and **`cpu: 1`** with a higher limit for burst. Packing
effect: memory stops being the binding constraint and CPU takes over at roughly
**7 runners per 8-vCPU node**, so a 15-job burst becomes 2–3 nodes instead of 14.

## 3. The escalation half

### 3.1 Attribution — capacity side only

The correlation chain, each link naming a real object:

```
EC2 spot ITN / Rebalance Recommendation
  → EventBridge → SQS private-estate-eks-karpenter-interruption
  → Karpenter interruption controller: cordon + drain, events on Node/NodeClaim
  → node name
  → the ephemeral runner pod evicted from it
  → ARC EphemeralRunner CR → GitHub (run_id, job, attempt)
```

Attribution is a *join*, not a heuristic: an attempt is reclaim-killed **iff** its
node carries an interruption event whose window contains the pod's termination.

**The receipt for why this matters** (measured 2026-08-05): on one cluster,
`check-x-crypto-advisories` failed twice with exit **130** and
`The runner has received a shutdown signal` — which reads exactly like a reclaim.
It was on an **on-demand** node, where reclaim is impossible. An exit-status
heuristic would have escalated both, spent on-demand money on work that was never
reclaimed, and buried the real cause.

**Verified 2026-08-05 — the join key exists and the weak fallback is not needed.**
`EphemeralRunner.status` (`actions.github.com/v1alpha1`) exposes:

```
workflowRunId      integer     ← the GitHub run_id, the re-dispatch key
jobRequestId       integer
jobRepositoryName  string      ← owner/repo
jobWorkflowRef     string
jobDisplayName     string
runnerId, runnerName
```

So the final link resolves directly: the evicted pod's `EphemeralRunner` yields
`jobRepositoryName` + `workflowRunId`, which is exactly the pair
`rerun-failed-jobs` takes. No listener-log correlation, no heuristic matching.

### 3.2 Escalation — re-dispatch to stable

GitHub does not auto-retry a job whose runner vanished; it marks it failed. So
escalation is two mechanisms, both already available:

- **Re-dispatch:** `POST /repos/{owner}/{repo}/actions/runs/{run_id}/rerun-failed-jobs`,
  which increments `run_attempt`.
- **Routing by attempt:** the workflow selects its label from the attempt, so a
  retry lands on the stable set:

  ```yaml
  runs-on: ${{ github.run_attempt == 1 && 'private-estate-short-pleme-eks' || (vars.PRIVATE_BUILDER_RUNNER || 'ubuntu-latest') }}
  ```

This needs no new scheduler. Attempt 1 is cheap; any reclaim-attributed retry is
pinned to on-demand. It satisfies §2.5's monotonicity (attempt N+1 never cheaper),
resets across chains (a new run starts at attempt 1), and terminates at on-demand,
already §2's "never-fail ceiling".

**Bounded by construction:** re-dispatch fires only on a reclaim-attributed
failure, at most once per attempt chain, and only escalates. A job failing on its
own merits is never re-dispatched by this path.

### 3.3 Where the detector lives

breathe, as the attempt-tier peer of `Reformar` — it reads the same
`interruption_tolerance` bid field and the same capacity events. `Reformar` moves
the *forma*; this moves the *attempt*. Existing crates already cover the
neighbouring concerns (`breathe-nodewaste` node-level judgement,
`breathe-provider` capacity observation), so this is a new reconciler in an
established shape, not a new service.

## 4. Order, with gates

| # | step | gate |
|---|---|---|
| 0 | IAM boundary widened; interruption queue consumed | a real reclaim produces cordon + drain |
| 1 | spot pool for short CI (own taint/label) | pool provisions and scales to zero |
| 2 | right-sized short runner scale set | measured requests; N runners per node, N > 1 |
| 3 | attribution reconciler, **report-only** | correctly labels reclaim vs non-reclaim over a real window, including at least one on-demand failure it must NOT claim |
| 4 | route ONE workflow (`vendor-mirror`) | jobs pass on spot; node count per burst drops |
| 5 | escalation re-dispatch enabled | a reclaimed job is re-dispatched exactly once and lands on-demand |
| 6 | route remaining short workflows | no increase in end-to-end failure rate |

Step 3 gating on a **true negative** is deliberate: the exit-130 case above is the
falsifying test, and an attribution engine that cannot pass it is not ready to
spend money.

## 5. What this plan will not do

- **No spot for long non-checkpointable builds.** The incident is measured; the
  on-demand builder pool and its 24Gi runner are untouched.
- **No escalation on non-reclaim failure.** A broken job stays broken and visible.
- **No inference of reclaim from exit status, signal, or timing alone.**
- **No routing to spot before Gate 0.** Without the interruption controller the
  trade is strictly negative.
