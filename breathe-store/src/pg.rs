//! The Postgres durable [`DecisionLog`] tier (M2) — `band_registry` (the durable
//! counter authority) + an append-only `decision_log` BLAKE3 chain, written in
//! ONE transaction so the counter bump and the chain append are atomic (a
//! half-applied decision is unrepresentable). Runtime sqlx (no compile-time
//! `query!` macros → Nix-build-clean, the operator idiom matching magma /
//! pangea-operator); `SELECT … FOR UPDATE` is the single-appender guard and
//! `UNIQUE(band_ref, seq)` turns a forked chain into a constraint violation
//! rather than a silent split.

use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

use crate::{
    decision_content_hash, decision_content_hash_fields, BandRef, CumulativeCounters, DecisionEntry,
    DecisionLog, StoreError, GENESIS_HASH,
};

/// Map any displayable backend error into [`StoreError::Backend`].
fn be<E: std::fmt::Display>(e: E) -> StoreError {
    StoreError::Backend(e.to_string())
}

/// The Postgres-backed durable decision log + counter authority.
pub struct PgDecisionLog {
    pool: PgPool,
}

impl PgDecisionLog {
    /// Connect (bounded pool) and apply the embedded migrations (`band_registry`
    /// + `decision_log`). `sqlx::migrate!` embeds the `.sql` files at compile
    /// time — no database is needed at BUILD time, only at run/test time.
    ///
    /// # Errors
    /// [`StoreError::Backend`] if the connection or the migration fails.
    pub async fn connect(dsn: &str, pool_max: u32, pool_min: u32) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(pool_max.max(1))
            .min_connections(pool_min)
            .connect(dsn)
            .await
            // A `Configuration` (DSN-parse) error can embed the DSN string — and
            // hence the password — so it is NEVER echoed; other errors (io/auth/
            // protocol) carry no DSN and keep their diagnostics.
            .map_err(|e| match e {
                sqlx::Error::Configuration(_) => StoreError::Backend(
                    "postgres connect failed: invalid DSN (check store.postgres.dsn)".to_string(),
                ),
                other => StoreError::Backend(format!("postgres connect failed: {other}")),
            })?;
        sqlx::migrate!("./migrations").run(&pool).await.map_err(be)?;
        Ok(Self { pool })
    }

    /// Verify the per-band decision chain: walk every row in `seq` order,
    /// re-hashing each from its RAW stored fields and checking the `prev_hash`
    /// linkage (from genesis) + the `content_hash`, THEN cross-check that the
    /// chain tail matches the `band_registry` head (`seq` + `last_hash`). The
    /// head cross-check closes the tail-truncation gap (a walk-from-genesis alone
    /// accepts any valid shorter prefix). Returns `false` on a broken link, a
    /// tampered row, or a tail that doesn't match the registry head.
    ///
    /// Honest ceiling: an attacker with WRITE access to BOTH tables could rewrite
    /// the log and the registry head consistently — that is the M5 boundary the
    /// tameshi/sekiban Ed25519-signed OutcomeChain closes; M2's unsigned chain
    /// detects all single-table (log-only or registry-only) tampering.
    ///
    /// # Errors
    /// [`StoreError::Backend`] if the query fails.
    pub async fn verify_chain(&self, band: &BandRef) -> Result<bool, StoreError> {
        let rows = sqlx::query(
            "SELECT seq, receipt_kind, counter_class, from_limit, to_limit, dry_run, content_hash, prev_hash \
             FROM breathe.decision_log WHERE band_ref = $1 ORDER BY seq ASC",
        )
        .bind(band.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(be)?;

        let mut expected_prev = GENESIS_HASH;
        let mut last_seq: i64 = 0;
        for row in rows {
            let seq: i64 = row.try_get("seq").map_err(be)?;
            let receipt_kind: String = row.try_get("receipt_kind").map_err(be)?;
            let class_tag: String = row.try_get("counter_class").map_err(be)?;
            let from_limit: Option<i64> = row.try_get("from_limit").map_err(be)?;
            let to_limit: Option<i64> = row.try_get("to_limit").map_err(be)?;
            let dry_run: bool = row.try_get("dry_run").map_err(be)?;
            let content: Vec<u8> = row.try_get("content_hash").map_err(be)?;
            let prev: Vec<u8> = row.try_get("prev_hash").map_err(be)?;

            if prev.as_slice() != &expected_prev[..] {
                return Ok(false); // linkage broken
            }
            // Re-hash over the RAW stored fields — `class_tag` exactly as stored,
            // NOT round-tripped through from_tag (which is lossy and would miss a
            // tampered noCount tag / false-fail a newer writer's added tag).
            let recomputed = decision_content_hash_fields(
                band,
                seq,
                &receipt_kind,
                &class_tag,
                from_limit.map(|v| v as u64),
                to_limit.map(|v| v as u64),
                dry_run,
                &expected_prev,
            );
            if content.as_slice() != &recomputed[..] {
                return Ok(false); // content tampered
            }
            expected_prev = recomputed;
            last_seq = seq;
        }

        // Cross-check the registry head: the chain tail (last_seq + the last
        // content_hash) must equal what band_registry recorded — else rows were
        // truncated off the tail (a prefix that walks clean from genesis).
        let head = sqlx::query("SELECT seq, last_hash FROM breathe.band_registry WHERE band_ref = $1")
            .bind(band.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(be)?;
        match head {
            // No registry row ⇒ the chain must be empty.
            None => Ok(last_seq == 0 && expected_prev == GENESIS_HASH),
            Some(r) => {
                let reg_seq: i64 = r.try_get("seq").map_err(be)?;
                let reg_last: Option<Vec<u8>> = r.try_get("last_hash").map_err(be)?;
                let reg_head = reg_last
                    .and_then(|v| <[u8; 32]>::try_from(v.as_slice()).ok())
                    .unwrap_or(GENESIS_HASH);
                Ok(reg_seq == last_seq && reg_head == expected_prev)
            }
        }
    }
}

#[async_trait]
impl DecisionLog for PgDecisionLog {
    async fn append(
        &self,
        band: &BandRef,
        seed: CumulativeCounters,
        entry: DecisionEntry,
    ) -> Result<CumulativeCounters, StoreError> {
        let mut tx = self.pool.begin().await.map_err(be)?;

        // 1. Ensure the band's registry row exists, seeded from `seed` on first
        //    sight — the InMem→PG migration continues the count from the CRD
        //    status rather than resetting. On an existing row `seed` is ignored:
        //    band_registry is the durable authority.
        sqlx::query(
            "INSERT INTO breathe.band_registry \
               (band_ref, carves_total, deferrals_total, conflicts_total, seq, last_hash) \
             VALUES ($1, $2, $3, $4, 0, NULL) ON CONFLICT (band_ref) DO NOTHING",
        )
        .bind(band.as_str())
        .bind(seed.carves)
        .bind(seed.deferrals)
        .bind(seed.conflicts)
        .execute(&mut *tx)
        .await
        .map_err(be)?;

        // 2. Lock + read the durable state (FOR UPDATE — the single-appender guard).
        let row = sqlx::query(
            "SELECT carves_total, deferrals_total, conflicts_total, seq, last_hash \
             FROM breathe.band_registry WHERE band_ref = $1 FOR UPDATE",
        )
        .bind(band.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(be)?;
        let current = CumulativeCounters {
            carves: row.try_get("carves_total").map_err(be)?,
            deferrals: row.try_get("deferrals_total").map_err(be)?,
            conflicts: row.try_get("conflicts_total").map_err(be)?,
        };
        let cur_seq: i64 = row.try_get("seq").map_err(be)?;
        let last_hash: Option<Vec<u8>> = row.try_get("last_hash").map_err(be)?;
        let prev = last_hash
            .and_then(|v| <[u8; 32]>::try_from(v.as_slice()).ok())
            .unwrap_or(GENESIS_HASH);

        // 3. Fold + advance the chain (the single accumulation point is `fold`).
        let next = current.fold(&entry);
        let next_seq = cur_seq + 1;
        let content = decision_content_hash(band, next_seq, &entry, &prev);

        // 4. Append the decision_log row — UNIQUE(band_ref, seq) makes a forked
        //    chain a 23505 violation, not a silent split.
        sqlx::query(
            "INSERT INTO breathe.decision_log \
               (band_ref, seq, receipt_kind, counter_class, from_limit, to_limit, dry_run, content_hash, prev_hash) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(band.as_str())
        .bind(next_seq)
        .bind(&entry.receipt_kind)
        .bind(entry.class.tag())
        .bind(entry.from_limit.map(|v| v as i64))
        .bind(entry.to_limit.map(|v| v as i64))
        .bind(entry.dry_run)
        .bind(&content[..])
        .bind(&prev[..])
        .execute(&mut *tx)
        .await
        .map_err(be)?;

        // 5. Project the new counters + chain head onto the registry — SAME tx,
        //    so the counter bump and the chain append commit atomically.
        sqlx::query(
            "UPDATE breathe.band_registry \
             SET carves_total = $2, deferrals_total = $3, conflicts_total = $4, seq = $5, last_hash = $6, updated_at = now() \
             WHERE band_ref = $1",
        )
        .bind(band.as_str())
        .bind(next.carves)
        .bind(next.deferrals)
        .bind(next.conflicts)
        .bind(next_seq)
        .bind(&content[..])
        .execute(&mut *tx)
        .await
        .map_err(be)?;

        tx.commit().await.map_err(be)?;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CounterClass;

    /// The Postgres integration tests run against a real database when
    /// `BREATHE_TEST_PG_URL` (or `DATABASE_URL`) is set, and SKIP otherwise so the
    /// default `cargo test` stays DB-free. Verify locally with:
    ///   docker run -d --name breathe-pg -e POSTGRES_PASSWORD=postgres \
    ///     -e POSTGRES_DB=breathe -p 5434:5432 postgres:17-alpine
    ///   BREATHE_TEST_PG_URL=postgres://postgres:postgres@127.0.0.1:5434/breathe \
    ///     cargo test -p breathe-store --features postgres -- --nocapture
    fn test_url() -> Option<String> {
        std::env::var("BREATHE_TEST_PG_URL")
            .ok()
            .or_else(|| std::env::var("DATABASE_URL").ok())
    }

    fn entry(kind: &str, class: CounterClass) -> DecisionEntry {
        DecisionEntry {
            receipt_kind: kind.to_string(),
            class,
            from_limit: None,
            to_limit: None,
            dry_run: false,
        }
    }

    async fn clean(log: &PgDecisionLog, band: &BandRef) {
        sqlx::query("DELETE FROM breathe.decision_log WHERE band_ref = $1")
            .bind(band.as_str())
            .execute(&log.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM breathe.band_registry WHERE band_ref = $1")
            .bind(band.as_str())
            .execute(&log.pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pg_append_seeds_folds_chains_and_survives_restart() {
        let Some(url) = test_url() else {
            eprintln!("SKIP pg_append…: set BREATHE_TEST_PG_URL to run");
            return;
        };
        let log = PgDecisionLog::connect(&url, 5, 1).await.expect("connect + migrate");
        let band = BandRef::new("MemoryBand", "breathe-test", "restart");
        clean(&log, &band).await;

        // First append seeds from the CRD-status count (5 carves) → 6.
        let c1 = log
            .append(
                &band,
                CumulativeCounters { carves: 5, deferrals: 1, conflicts: 0 },
                entry("Applied", CounterClass::Carve),
            )
            .await
            .unwrap();
        assert_eq!(c1, CumulativeCounters { carves: 6, deferrals: 1, conflicts: 0 });

        // Second append: the seed is IGNORED (row exists) — folds from durable 6.
        let c2 = log
            .append(&band, CumulativeCounters::ZERO, entry("Conflict", CounterClass::Conflict))
            .await
            .unwrap();
        assert_eq!(c2, CumulativeCounters { carves: 6, deferrals: 1, conflicts: 1 });
        assert!(log.verify_chain(&band).await.unwrap(), "chain verifies");

        // RESTART survival: a fresh connection continues the durable count.
        let log2 = PgDecisionLog::connect(&url, 5, 1).await.unwrap();
        let c3 = log2
            .append(&band, CumulativeCounters::ZERO, entry("Applied", CounterClass::Carve))
            .await
            .unwrap();
        assert_eq!(c3.carves, 7, "durable count continues across restart, not reset");
        assert!(log2.verify_chain(&band).await.unwrap());

        clean(&log, &band).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pg_concurrent_appends_serialize_via_for_update() {
        let Some(url) = test_url() else {
            eprintln!("SKIP pg_concurrent…: set BREATHE_TEST_PG_URL to run");
            return;
        };
        let log = std::sync::Arc::new(PgDecisionLog::connect(&url, 8, 2).await.unwrap());
        let band = BandRef::new("MemoryBand", "breathe-test", "concurrent");
        clean(&log, &band).await;

        // N concurrent appends on the SAME band. FOR UPDATE serializes them, so
        // all succeed with sequential seqs + one unbroken chain (no 23505).
        let n: i64 = 16;
        let mut handles = Vec::new();
        for _ in 0..n {
            let log = log.clone();
            let band = band.clone();
            handles.push(tokio::spawn(async move {
                log.append(&band, CumulativeCounters::ZERO, entry("Applied", CounterClass::Carve))
                    .await
            }));
        }
        for h in handles {
            h.await.unwrap().expect("a concurrent append must not fail");
        }

        let total: i64 = sqlx::query("SELECT carves_total FROM breathe.band_registry WHERE band_ref = $1")
            .bind(band.as_str())
            .fetch_one(&log.pool)
            .await
            .unwrap()
            .try_get("carves_total")
            .unwrap();
        assert_eq!(total, n, "every concurrent carve counted exactly once");

        let rows: i64 = sqlx::query("SELECT COUNT(*) AS c FROM breathe.decision_log WHERE band_ref = $1")
            .bind(band.as_str())
            .fetch_one(&log.pool)
            .await
            .unwrap()
            .try_get("c")
            .unwrap();
        assert_eq!(rows, n, "exactly n decision rows, no forked chain");
        assert!(log.verify_chain(&band).await.unwrap(), "chain unbroken under concurrency");

        clean(&log, &band).await;
    }

    #[tokio::test]
    async fn pg_verify_chain_detects_a_tampered_counter_class() {
        let Some(url) = test_url() else {
            eprintln!("SKIP pg_verify_chain_detects…: set BREATHE_TEST_PG_URL to run");
            return;
        };
        let log = PgDecisionLog::connect(&url, 5, 1).await.unwrap();
        let band = BandRef::new("MemoryBand", "breathe-test", "tamper");
        clean(&log, &band).await;

        // A NoCount row (the majority class — Observed/Cooldown/Stale/Dormant).
        log.append(&band, CumulativeCounters::ZERO, entry("Observed", CounterClass::NoCount))
            .await
            .unwrap();
        assert!(log.verify_chain(&band).await.unwrap(), "a genuine chain verifies");

        // Tamper the stored counter_class to a non-canonical string — the exact
        // attack the adversarial pass found (from_tag would collapse it back to
        // "noCount" and miss it; hashing the RAW stored bytes catches it).
        sqlx::query("UPDATE breathe.decision_log SET counter_class = 'junk' WHERE band_ref = $1")
            .bind(band.as_str())
            .execute(&log.pool)
            .await
            .unwrap();
        assert!(
            !log.verify_chain(&band).await.unwrap(),
            "a mutated counter_class on a noCount row MUST be detected"
        );

        clean(&log, &band).await;
    }

    #[tokio::test]
    async fn pg_verify_chain_detects_tail_truncation() {
        let Some(url) = test_url() else {
            eprintln!("SKIP pg_verify_chain_detects_tail_truncation…: set BREATHE_TEST_PG_URL to run");
            return;
        };
        let log = PgDecisionLog::connect(&url, 5, 1).await.unwrap();
        let band = BandRef::new("MemoryBand", "breathe-test", "truncate");
        clean(&log, &band).await;

        // Two decisions, then delete the newest log row (tail truncation) while
        // band_registry still points (seq + last_hash) past it. A walk-from-genesis
        // alone would accept the shorter prefix — the registry-head cross-check
        // catches it.
        log.append(&band, CumulativeCounters::ZERO, entry("Applied", CounterClass::Carve))
            .await
            .unwrap();
        log.append(&band, CumulativeCounters::ZERO, entry("Conflict", CounterClass::Conflict))
            .await
            .unwrap();
        assert!(log.verify_chain(&band).await.unwrap(), "the full chain verifies");

        sqlx::query("DELETE FROM breathe.decision_log WHERE band_ref = $1 AND seq = (SELECT MAX(seq) FROM breathe.decision_log WHERE band_ref = $1)")
            .bind(band.as_str())
            .execute(&log.pool)
            .await
            .unwrap();
        assert!(
            !log.verify_chain(&band).await.unwrap(),
            "tail truncation MUST be detected via the registry head"
        );

        clean(&log, &band).await;
    }
}
