# breathe-catalog goldens

Generated artifacts, not hand-authored. Each golden is the byte-exact render of a
typed source in this crate, pinned by a test so a source edit that changes the
render fails CI until the golden (and any downstream consumer that carries it
verbatim) is re-synced.

## `camelot-global-breathe.golden.yaml`

The canonical `global.breathe` value block for the Camelot posture — the render of
`preset::CAMELOT` by `render::render_global_breathe`. Pinned by
`render::tests::render_matches_the_committed_golden`.

**Single source of truth.** This block is carried VERBATIM under `global.breathe`
in
`helmworks-akeyless/charts/lareira-akeyless-deployment/architectures/camelot.yaml`.
The chart's `global.breathe.preset: camelot` names this source (the kata binding);
the explicit `memory` / `cpu` / `replica` bands under it are this render's
materialization. To change the Camelot breathe posture, edit `src/preset.rs`
(+ `src/render.rs` if the block shape changes), re-render, update this golden, and
re-sync the chart block — the parity test and the chart's
`tests/camelot-breathe-parity_test.yaml` both fail until they agree.

**Tier-honest.** This is a rung-3 destination artifact (typed contract, offline,
green) — not live band rendering. `pleme-lib 0.16.0` (the vendored chart lib) has
no MemoryBand/CpuBand/ReplicaBand template, so this block is inert against the live
cluster today. Live band emission + reaping the live orphan `camelot-rabbitmq`
band are named LiveTODOs.
