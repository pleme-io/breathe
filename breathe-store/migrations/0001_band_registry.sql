-- breathe-store M2 — the durable band registry.
--
-- One row per band: the cumulative counters that today live (lossily) on the
-- CRD status, plus the decision-chain head (`seq` + `last_hash`). This is the
-- durable AUTHORITY for the counters in the Postgres tier; the CRD status
-- becomes a projection of it.
--
-- Add-only discipline (Urdume L0): never DROP or repurpose a column — new
-- fields are new columns in a later migration; applied migrations are never
-- edited (shinka checksum reconciliation). Idempotent (IF NOT EXISTS).

CREATE SCHEMA IF NOT EXISTS breathe;

CREATE TABLE IF NOT EXISTS breathe.band_registry (
    -- kind/namespace/name (the BandRef), globally unique across band kinds.
    band_ref         TEXT        PRIMARY KEY,
    -- cumulative TickReceipt::Applied carves.
    carves_total     BIGINT      NOT NULL DEFAULT 0,
    -- cumulative deferred ceiling crossings.
    deferrals_total  BIGINT      NOT NULL DEFAULT 0,
    -- cumulative single-writer yields.
    conflicts_total  BIGINT      NOT NULL DEFAULT 0,
    -- monotone decision sequence for this band (the decision_log seq head).
    seq              BIGINT      NOT NULL DEFAULT 0,
    -- the tail decision's content_hash (the chain head); NULL before the first.
    last_hash        BYTEA,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
