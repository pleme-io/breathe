-- breathe-store M2 — the append-only decision log (breathe's first attestation
-- surface; the seed of the M5 tameshi/sekiban OutcomeChain).
--
-- One row per reconcile decision, linked into a per-band BLAKE3 chain
-- (`content_hash` = BLAKE3 over the canonical row ‖ prev_hash; the first row's
-- prev_hash is all-zeros). UNIQUE(band_ref, seq) is the single-appender
-- concurrency guard: a forked chain (two writers racing the same seq) is a
-- constraint violation (23505), not a silent split. Append-only — rows are
-- never updated or deleted.

CREATE TABLE IF NOT EXISTS breathe.decision_log (
    id            BIGSERIAL   PRIMARY KEY,
    band_ref      TEXT        NOT NULL,
    seq           BIGINT      NOT NULL,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- the TickReceipt kind tag ("Applied", "Conflict", "DryRunWouldApply", …).
    receipt_kind  TEXT        NOT NULL,
    -- which cumulative counter this decision advanced ("carve"/"deferral"/"conflict"/"noCount").
    counter_class TEXT        NOT NULL,
    -- the limit transition, when the receipt carried one.
    from_limit    BIGINT,
    to_limit      BIGINT,
    -- whether this tick was shadow-only.
    dry_run       BOOLEAN     NOT NULL,
    -- BLAKE3(canonical(row) ‖ prev_hash) — the chain link.
    content_hash  BYTEA       NOT NULL,
    -- the predecessor's content_hash (all-zeros for the first decision).
    prev_hash     BYTEA       NOT NULL,
    CONSTRAINT decision_log_band_seq_uniq UNIQUE (band_ref, seq)
);

CREATE INDEX IF NOT EXISTS decision_log_band_seq_idx
    ON breathe.decision_log (band_ref, seq);
