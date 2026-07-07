;;;; breathe/specs/presets.lisp
;;;;
;;;; The self-describing breathe-PRESET catalog, authored as tatara-lisp data.
;;;; A preset is a NAMED BUNDLE that arms a whole fleet's band-set from one typed
;;;; value (Pillar 12 — declare, don't author per-workload). This is the authoring
;;;; surface (the spec) for `breathe-catalog::preset` (the Rust border) +
;;;; `BreatheDefaults::resolve` (the pure interpreter) — the TYPED-SPEC triplet.
;;;; The CATALOG REFLECTION tests in breathe-catalog cross-check this file against
;;;; the Rust tables via include_str!; the build fails if the two drift.
;;;;
;;;; A preset never carries band LAW — it selects, per workload TOPOLOGY-CLASS, the
;;;; band-set (vertical setpoint, replica floor + topology, whether a StorageBand
;;;; applies) plus the shared spot placement + flex-window. The proven
;;;; dimension-agnostic band law (breathe-control) still does the deciding.
;;;;
;;;; Canonical (and today only) instance: :camelot — the aggressive 80/20,
;;;; shadow-first, 100%-spot posture for the Camelot ephemeral akeyless SaaS.

(defmeta-catalog breathe-presets
  :description "Named breathe-posture bundles — one preset reference arms a workload's whole band-set."
  :reflection (every-workload-class-has-a-profile
               profiles-cover-every-topology-arm
               storage-couples-to-the-stateful-topology
               preset-declared-in-the-lisp))

;;; ── The Camelot preset — aggressive 80/20, shadow-first, 100% spot ───────────
;;; Born SHADOW-FIRST: every band attests what it WOULD carve (:dry-run t,
;;; :mode :shadow) but mutates nothing until explicitly live-applied. Correct +
;;; honest with no live cluster; the flex-window auction is a LiveTODO.

(defbreathe-preset :camelot
  :name        "camelot"
  ;; The shared posture every armed band inherits.
  :setpoint    0.8                 ; 80% used / 20% headroom — the aggressive band
  :dry-run     t                   ; shadow-first: attest, never carve, until live
  :mode        :shadow             ; PromotionMode::Shadow (observe + attest forever until promoted)
  :ha-replica-floor 2              ; every workload rests at ≥ 2 (HA); a class may raise, never lower

  ;; The 100%-spot placement stamped on every armed workload — pins onto the
  ;; tainted, isolated Camelot node group and auctions it entirely from spot.
  :placement
   (:node-selector-role "camelot"  ; nodeSelector role → the Camelot node group
    :toleration-key      "camelot-only" ; tolerate the taint that isolates Camelot
    :spot-fraction       1.0)      ; 100% spot — even the databases

  ;; The flex-window cost envelope — the diversified instance-family menu the live
  ;; auction widens across, bounded by a monthly $ variance budget (the number is
  ;; an INTERIM setpoint the live auction tunes). See :cost-budget below.
  :flex-window
   (:monthly-usd-variance-budget 400.0
    :instance-families ("m6i" "m6a" "m7i" "m5" "m5a" "c6i" "c6a" "r6i" "r6a"))

  ;; The per-workload-class profiles. The four classes cover all four replica
  ;; topology arms; the stateful three carry storage, the stateless one does not;
  ;; the quorum class raises its floor to an odd quorum ≥ 3.
  :profiles
   ((:class "stateless-service"   :topology "nonPersistent"    :replica-floor 2 :storage nil
      :note "auth/bis/uam/gator/kfm — interchangeable pods; free HPA scaling, HA floor only")
    (:class "relational-database" :topology "masterSlave"      :replica-floor 2 :storage t
      :note "mysql — only the read-replicas breathe; the primary is never scaled away")
    (:class "persistent-store"    :topology "persistent"       :replica-floor 2 :storage t
      :note "neo4j — single-writer, PVC-per-ordinal; a scale-in is HELD for drain")
    (:class "quorum-store"        :topology "fullyDistributed" :replica-floor 3 :storage t
      :note "distributed object/metadata store — odd quorum ≥ 3, majority-safe steps")))

;;; ── The per-engine DATABASE matrix (BREATHABILITY §II.5) ─────────────────────
;;; A DB is not held by a generic pod band — it breathes on its OWN engine knobs
;;; under the correct replica topology. Each row is a DimensionId::AppParam
;;; INSTANCE (an application-actuator lever). Mirror of breathe-catalog::db_matrix.

(defbreathe-db-matrix breathe-db-matrix
  :db-matrix
   ((:engine "mysql" :knob "innodb_buffer_pool_size" :dimension :app-param
      :directionality :bidirectional :topology "masterSlave" :requires-roll nil
      :actuator "mysql-admin-rpc (SET GLOBAL innodb_buffer_pool_size)"
      :observe  "mysql_global_status_innodb_buffer_pool_reads / _read_requests"
      :purpose  "hold the InnoDB buffer pool at the working-set band (live SET GLOBAL)")
    (:engine "mysql" :knob "max_connections" :dimension :app-param
      :directionality :bidirectional :topology "masterSlave" :requires-roll nil
      :actuator "mysql-admin-rpc (SET GLOBAL max_connections)"
      :observe  "mysql_global_status_threads_connected / max_connections"
      :purpose  "hold the connection headroom at the band (live SET GLOBAL)")
    ;; ── PostgreSQL — primary + streaming read-replicas (masterSlave) ────────────
    (:engine "postgres" :knob "shared_buffers" :dimension :app-param
      :directionality :bidirectional :topology "masterSlave" :requires-roll t
      :actuator "config-file + rolling restart (postgresql.conf shared_buffers)"
      :observe  "pg_stat_bgwriter buffers_backend / cache-hit ratio"
      :purpose  "hold PostgreSQL shared_buffers at the working-set band (rolling restart)")
    (:engine "postgres" :knob "max_connections" :dimension :app-param
      :directionality :bidirectional :topology "masterSlave" :requires-roll t
      :actuator "config-file + rolling restart (postgresql.conf max_connections)"
      :observe  "pg_stat_activity count / max_connections"
      :purpose  "hold the PostgreSQL connection headroom at the band (rolling restart)")
    ;; ── Redis — master + replicas under Sentinel HA (masterSlave) ───────────────
    (:engine "redis" :knob "maxmemory" :dimension :app-param
      :directionality :bidirectional :topology "masterSlave" :requires-roll nil
      :actuator "redis-admin-rpc (CONFIG SET maxmemory)"
      :observe  "redis_memory_used_bytes / redis_memory_max_bytes + evicted_keys"
      :purpose  "hold the Redis maxmemory cache ceiling at the band (live CONFIG SET)")
    (:engine "redis" :knob "maxclients" :dimension :app-param
      :directionality :bidirectional :topology "masterSlave" :requires-roll nil
      :actuator "redis-admin-rpc (CONFIG SET maxclients)"
      :observe  "redis_connected_clients / redis_config_maxclients"
      :purpose  "hold the Redis connection headroom at the band (live CONFIG SET)")
    ;; ── MongoDB — replica-set majority election (fullyDistributed) ──────────────
    (:engine "mongo" :knob "wiredTigerEngineRuntimeConfig" :dimension :app-param
      :directionality :bidirectional :topology "fullyDistributed" :requires-roll nil
      :actuator "mongo-admin-rpc (setParameter wiredTigerEngineRuntimeConfig cache_size — live SET)"
      :observe  "wiredTiger bytes-in-cache / maximum-bytes-configured"
      :purpose  "hold the WiredTiger cache at the working-set band (live setParameter)")
    (:engine "mongo" :knob "net.maxIncomingConnections" :dimension :app-param
      :directionality :bidirectional :topology "fullyDistributed" :requires-roll t
      :actuator "config-file + rolling restart (mongod.conf net.maxIncomingConnections)"
      :observe  "mongodb_connections{state=current} / {state=available}"
      :purpose  "hold the MongoDB connection headroom at the band (rolling restart)")
    ;; ── Neo4j — single-writer graph store, PVC-per-ordinal (persistent) ─────────
    (:engine "neo4j" :knob "dbms.memory.pagecache.size" :dimension :app-param
      :directionality :bidirectional :topology "persistent" :requires-roll t
      :actuator "config-file + rolling restart (neo4j.conf pagecache)"
      :observe  "neo4j_page_cache_hit_ratio"
      :purpose  "hold the Neo4j page cache at the band (dbms.memory.pagecache.size)")))

;;; ── The 100%-spot cost budget (Viggy defpromessa template) ───────────────────
;;; ATTESTS the cost posture on an OutcomeChain — never compile-proven. The
;;; reconciling PromessaController is a LiveTODO; this is the authored template.

(defpromessa :camelot-100pct-spot
  :kind        :cost-budget
  :target-monthly-usd-variance 400.0
  :spot-fraction 1.0
  :attest      "100% spot within the monthly $ variance budget — attested on OutcomeChain, never compile-proven")
