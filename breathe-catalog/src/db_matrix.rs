//! `db_matrix` — the per-engine DATABASE breathe matrix (BREATHABILITY §II.5).
//!
//! The generic [`DimensionId::AppParam`](breathe_provider::DimensionId::AppParam)
//! catalog row is a FAMILY: "any application-actuator knob, layout + actuator
//! carried as data". A relational/graph database is not held by a generic pod
//! band — it breathes on its OWN engine knobs (InnoDB buffer pool, Neo4j page
//! cache) under the correct REPLICA topology (a MySQL primary+replicas tier is
//! `masterSlave`; a Neo4j store is `persistent`). This module enumerates those
//! knobs as typed `AppParam` INSTANCES so the Camelot mysql/neo4j tiers breathe by
//! the right per-engine algorithm.
//!
//! Each row is one `AppParam` instance; the reflection tests below fail the build
//! if a row is not `AppParam`, if a knob collides, if an engine's topology is not a
//! real (stateful) [`REPLICA_TOPOLOGY_AXIS`](crate::REPLICA_TOPOLOGY_AXIS) arm, or
//! if the authored `specs/presets.lisp` drops a knob.

use breathe_provider::{DimensionId, Directionality};

/// A database engine breathe supports as a first-class tier. The ARCHITECTURE
/// view (topology class, discovery, failover) lives in
/// `breathe_invariant::database::DbEngine`; this is the ACTUATOR view (the concrete
/// per-engine `SET GLOBAL`/`CONFIG SET`/`setParameter` knobs). Both code the same
/// five engines — 5/5, closing the former 2/5 gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbEngine {
    /// MySQL / InnoDB — a primary + read-replicas tier (`masterSlave` topology).
    MySql,
    /// PostgreSQL — a primary + streaming read-replicas tier (`masterSlave` topology).
    Postgres,
    /// Redis — a master + replicas cache tier under Sentinel HA (`masterSlave` topology).
    Redis,
    /// MongoDB — a replica set with majority election (`fullyDistributed` topology).
    Mongo,
    /// Neo4j — a single-writer graph store, PVC-per-ordinal (`persistent` topology).
    Neo4j,
}

impl DbEngine {
    /// The kebab-case stable label (used in the authored lisp + as a stable id).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MySql => "mysql",
            Self::Postgres => "postgres",
            Self::Redis => "redis",
            Self::Mongo => "mongo",
            Self::Neo4j => "neo4j",
        }
    }
}

/// One database-engine knob, declared as a typed `AppParam` instance.
#[derive(Debug, Clone, Copy)]
pub struct DbKnobSpec {
    pub engine: DbEngine,
    /// The concrete engine knob carved (`innodb_buffer_pool_size`, …).
    pub knob: &'static str,
    /// The breathe dimension this knob is an instance of — ALWAYS
    /// [`DimensionId::AppParam`] (a DB knob is an application-actuator lever). A
    /// reflection test enforces this so a row can never drift to a wrong family.
    pub dimension: DimensionId,
    /// Which way breathe may move the knob.
    pub directionality: Directionality,
    /// The app-plane actuator that services the knob (which `ActuatorCluster` arm).
    pub actuator: &'static str,
    /// `true` ⇒ applying the knob requires a workload roll (RestartRequiring); the
    /// live carve gates on the DisruptionPolicy. `false` ⇒ a live `SET`-style write.
    pub requires_roll: bool,
    /// The REPLICA topology this engine's tier breathes under — a `crd_kind` token
    /// that MUST be a stateful arm of [`REPLICA_TOPOLOGY_AXIS`].
    pub topology_kind: &'static str,
    /// The metric that reveals this knob's suppressed demand (the non-blind signal).
    pub observe: &'static str,
    pub purpose: &'static str,
}

/// The database matrix. One row per (engine, knob). Adding an engine/knob is one
/// row here + one authored form in `specs/presets.lisp`; the reflection tests fail
/// the build otherwise.
pub const DB_MATRIX: &[DbKnobSpec] = &[
    // ── MySQL / InnoDB — primary + read-replicas (masterSlave) ──────────────────
    DbKnobSpec {
        engine: DbEngine::MySql,
        knob: "innodb_buffer_pool_size",
        dimension: DimensionId::AppParam,
        directionality: Directionality::Bidirectional,
        actuator: "mysql-admin-rpc (SET GLOBAL innodb_buffer_pool_size)",
        requires_roll: false, // online resize since MySQL 5.7 — a live SET GLOBAL
        topology_kind: "masterSlave",
        observe: "mysql_global_status_innodb_buffer_pool_reads / _read_requests (miss ratio)",
        purpose: "hold the InnoDB buffer pool at the working-set band (live SET GLOBAL)",
    },
    DbKnobSpec {
        engine: DbEngine::MySql,
        knob: "max_connections",
        dimension: DimensionId::AppParam,
        directionality: Directionality::Bidirectional,
        actuator: "mysql-admin-rpc (SET GLOBAL max_connections)",
        requires_roll: false, // dynamic global variable — a live SET GLOBAL
        topology_kind: "masterSlave",
        observe: "mysql_global_status_threads_connected / mysql_global_variables_max_connections",
        purpose: "hold the connection headroom at the band (live SET GLOBAL)",
    },
    // ── PostgreSQL — primary + streaming read-replicas (masterSlave) ────────────
    DbKnobSpec {
        engine: DbEngine::Postgres,
        knob: "shared_buffers",
        dimension: DimensionId::AppParam,
        directionality: Directionality::Bidirectional,
        actuator: "config-file + rolling restart (postgresql.conf shared_buffers)",
        requires_roll: true, // shared_buffers is set at boot — the carve needs a roll
        topology_kind: "masterSlave",
        observe: "pg_stat_bgwriter buffers_backend / cache-hit ratio (rising backend reads ⇒ buffers too small)",
        purpose: "hold PostgreSQL shared_buffers at the working-set band (rolling restart)",
    },
    DbKnobSpec {
        engine: DbEngine::Postgres,
        knob: "max_connections",
        dimension: DimensionId::AppParam,
        directionality: Directionality::Bidirectional,
        actuator: "config-file + rolling restart (postgresql.conf max_connections)",
        requires_roll: true, // max_connections is a boot-time GUC — needs a roll
        topology_kind: "masterSlave",
        observe: "pg_stat_activity count / current_setting('max_connections')",
        purpose: "hold the PostgreSQL connection headroom at the band (rolling restart)",
    },
    // ── Redis — master + replicas under Sentinel HA (masterSlave) ───────────────
    DbKnobSpec {
        engine: DbEngine::Redis,
        knob: "maxmemory",
        dimension: DimensionId::AppParam,
        directionality: Directionality::Bidirectional,
        actuator: "redis-admin-rpc (CONFIG SET maxmemory)",
        requires_roll: false, // CONFIG SET maxmemory is a live write
        topology_kind: "masterSlave",
        observe: "redis_memory_used_bytes / redis_memory_max_bytes + evicted_keys (rising evictions ⇒ maxmemory too small)",
        purpose: "hold the Redis maxmemory cache ceiling at the band (live CONFIG SET)",
    },
    DbKnobSpec {
        engine: DbEngine::Redis,
        knob: "maxclients",
        dimension: DimensionId::AppParam,
        directionality: Directionality::Bidirectional,
        actuator: "redis-admin-rpc (CONFIG SET maxclients)",
        requires_roll: false, // maxclients is a live CONFIG SET
        topology_kind: "masterSlave",
        observe: "redis_connected_clients / redis_config_maxclients",
        purpose: "hold the Redis connection headroom at the band (live CONFIG SET)",
    },
    // ── MongoDB — replica-set majority election (fullyDistributed) ──────────────
    DbKnobSpec {
        engine: DbEngine::Mongo,
        knob: "wiredTigerEngineRuntimeConfig",
        dimension: DimensionId::AppParam,
        directionality: Directionality::Bidirectional,
        actuator: "mongo-admin-rpc (setParameter wiredTigerEngineRuntimeConfig cache_size — live SET)",
        requires_roll: false, // wiredTiger cache_size is a live runtime setParameter
        topology_kind: "fullyDistributed",
        observe: "wiredTiger bytes-currently-in-cache / maximum-bytes-configured (rising ratio ⇒ cache too small)",
        purpose: "hold the WiredTiger cache at the working-set band (live setParameter)",
    },
    DbKnobSpec {
        engine: DbEngine::Mongo,
        knob: "net.maxIncomingConnections",
        dimension: DimensionId::AppParam,
        directionality: Directionality::Bidirectional,
        actuator: "config-file + rolling restart (mongod.conf net.maxIncomingConnections)",
        requires_roll: true, // maxIncomingConnections is a boot-time setting — needs a roll
        topology_kind: "fullyDistributed",
        observe: "mongodb_connections{state=current} / {state=available}",
        purpose: "hold the MongoDB connection headroom at the band (rolling restart)",
    },
    // ── Neo4j — single-writer graph store, PVC-per-ordinal (persistent) ─────────
    DbKnobSpec {
        engine: DbEngine::Neo4j,
        knob: "dbms.memory.pagecache.size",
        dimension: DimensionId::AppParam,
        directionality: Directionality::Bidirectional,
        actuator: "config-file + rolling restart (neo4j.conf pagecache)",
        requires_roll: true, // neo4j page cache is set at boot — the carve needs a roll
        topology_kind: "persistent",
        observe: "neo4j_page_cache_hit_ratio (rising misses ⇒ cache too small)",
        purpose: "hold the Neo4j page cache at the band (dbms.memory.pagecache.size)",
    },
];

/// Every engine the matrix covers (the domain side of the coverage check). 5/5 —
/// MySQL, PostgreSQL, Redis, MongoDB, Neo4j.
pub const ALL_DB_ENGINES: [DbEngine; 5] =
    [DbEngine::MySql, DbEngine::Postgres, DbEngine::Redis, DbEngine::Mongo, DbEngine::Neo4j];

/// The rows for one engine.
#[must_use]
pub fn rows_for(engine: DbEngine) -> impl Iterator<Item = &'static DbKnobSpec> {
    DB_MATRIX.iter().filter(move |r| r.engine == engine)
}

#[cfg(test)]
mod tests {
    use super::{rows_for, DbEngine, ALL_DB_ENGINES, DB_MATRIX};
    use crate::{RequiresTarget, REPLICA_TOPOLOGY_AXIS};
    use breathe_provider::DimensionId;

    /// EVERY matrix row is an `AppParam` instance — a DB knob is an application-
    /// actuator lever, never a generic pod band. Fails the build if a row drifts to
    /// a wrong dimension family.
    #[test]
    fn every_row_is_an_app_param() {
        for r in DB_MATRIX {
            assert_eq!(r.dimension, DimensionId::AppParam, "{} must be an AppParam instance", r.knob);
        }
    }

    /// All five engines are covered — the matrix is 5/5, not half-authored (the
    /// former 2/5 MySQL+Neo4j gap is closed).
    #[test]
    fn all_engines_are_covered() {
        for e in ALL_DB_ENGINES {
            assert!(rows_for(e).next().is_some(), "no matrix rows for {}", e.as_str());
        }
        assert_eq!(ALL_DB_ENGINES.len(), 5, "the db_matrix codes 5/5 engines");
    }

    /// (engine, knob) is unique — no duplicate knob for the same engine.
    #[test]
    fn engine_knob_pairs_are_unique() {
        for (i, a) in DB_MATRIX.iter().enumerate() {
            for b in &DB_MATRIX[i + 1..] {
                assert!(!(a.engine == b.engine && a.knob == b.knob), "duplicate {} knob {}", a.engine.as_str(), a.knob);
            }
        }
    }

    /// THE topology-coupling invariant: every engine breathes under a topology that
    /// is a real, STATEFUL arm of the replica axis. A DB is never `nonPersistent` —
    /// it has data, so its topology must require a StatefulSet.
    #[test]
    fn every_engine_topology_is_a_stateful_axis_arm() {
        for r in DB_MATRIX {
            let arm = REPLICA_TOPOLOGY_AXIS
                .iter()
                .find(|a| a.crd_kind == r.topology_kind)
                .unwrap_or_else(|| panic!("{}'s topology {} is not a real axis arm", r.knob, r.topology_kind));
            assert!(
                matches!(arm.requires_target, RequiresTarget::Kind("StatefulSet")),
                "{}: a database tier must breathe under a stateful topology, not {}",
                r.knob,
                r.topology_kind
            );
        }
    }

    /// Each engine breathes under the right topology arm: MySQL/PostgreSQL/Redis are
    /// primary+replicas tiers (masterSlave), MongoDB is a majority-election replica
    /// set (fullyDistributed), Neo4j is a single-writer store (persistent). Pins the
    /// per-engine algorithm to the right arm.
    #[test]
    fn engines_use_the_expected_topology() {
        for e in [DbEngine::MySql, DbEngine::Postgres, DbEngine::Redis] {
            for r in rows_for(e) {
                assert_eq!(r.topology_kind, "masterSlave", "{} breathes the read-replicas (masterSlave)", e.as_str());
            }
        }
        for r in rows_for(DbEngine::Mongo) {
            assert_eq!(r.topology_kind, "fullyDistributed", "MongoDB is a majority-election replica set (fullyDistributed)");
        }
        for r in rows_for(DbEngine::Neo4j) {
            assert_eq!(r.topology_kind, "persistent", "Neo4j is a persistent single-writer store");
        }
    }

    /// A knob that needs a roll must name a rolling actuator; a live-SET knob must
    /// not. Keeps the RestartRequiring/RestartFree honesty on each row.
    #[test]
    fn roll_requirement_matches_the_actuator() {
        for r in DB_MATRIX {
            if r.requires_roll {
                assert!(r.actuator.contains("restart"), "{} requires a roll but its actuator is not a rolling one", r.knob);
            } else {
                assert!(r.actuator.contains("SET"), "{} is a live carve but its actuator is not a SET-style one", r.knob);
            }
        }
    }

    /// The authored lisp names every knob — the Lisp ↔ Rust cross-check (the same
    /// include_str convention the dimensions catalog uses). Knob strings are
    /// mutually non-substring, so a bare `contains` is unambiguous.
    #[test]
    fn db_matrix_is_declared_in_the_lisp() {
        const PRESETS_LISP: &str = include_str!("../../specs/presets.lisp");
        assert!(PRESETS_LISP.contains(":db-matrix"), "the presets lisp must declare :db-matrix");
        for r in DB_MATRIX {
            assert!(PRESETS_LISP.contains(r.knob), "the lisp :db-matrix is missing the {} knob", r.knob);
            assert!(PRESETS_LISP.contains(r.engine.as_str()), "the lisp :db-matrix is missing the {} engine", r.engine.as_str());
        }
    }
}
