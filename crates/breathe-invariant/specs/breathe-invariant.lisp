;;; breathe-invariant.lisp — the /vocabulary-bridging surface for the
;;; breathability variant/invariant contract.
;;;
;;; The typed Rust border (src/) is authoritative; this authored lisp is the
;;; vocabulary bridge — a (defbreathe-invariant …) form declaring the six
;;; clauses + a (defband …) form per dimension, so the contract is a first-class
;;; term in the fleet vocabulary (typescape + JP/BR-PT naming). The lib test
;;; `the_contract_is_declared_in_the_lisp` include_str!'s this file and asserts
;;; every band keyword, band name, and clause rule-name appears here — so the
;;; Rust catalog and this lisp can never drift (the same convention
;;; breathe-catalog::db_matrix uses for its presets.lisp).

(defbreathe-invariant breathability
  :statement
    "every resource dimension a workload consumes is continuously CARVED to its
     utilization setpoint, by DEFAULT, fleet-wide — and the same carve is
     simultaneously a cost control AND an availability/resiliency maximizer,
     tuned continuously over time"
  :doctrine "theory/BREATHABILITY.md"
  :clauses
    ((:rule breathe-carved-by-a-band       :tier ceiling-c1        :of "every consumed dimension is carved by a typed *Band")
     (:rule breathe-carve-to-setpoint      :tier parse-time-rejected :of "carving drives utilization to a sealed setpoint")
     (:rule breathe-default-on-fleet-wide  :tier ceiling-c1        :of "breathability is on by default, not opt-in")
     (:rule breathe-models-stay-current    :tier ceiling-c1        :of "a doctrine-claimed dimension must be a shipped/landing Band (the 155GB gate)")
     (:rule breathe-discovery-molded       :tier ceiling-c2        :of "the carve config is discovered, not hand-tuned")
     (:rule breathe-dual-purpose           :tier ceiling-c1        :of "every Band is a cost control AND a resiliency maximizer, together not traded"))
  :sibling-contracts (gen-pdc gen-secattest))

;; ── the five dimension bands (the VARIANT catalog) ───────────────────────────
;; Each (defband …) is one lattice point: band + setpoint + carve-algorithm +
;; discovery + maturity + the DUAL cost/resiliency effects. Maturity is
;; tier-honest; a claimed :gap carries a :pending note (models-stay-current).

(defband-memory MemoryBand
  :setpoint 0.80 :carve multiplicative-band :discovery kanchi-discovered :maturity shipped
  :cost "right-size the pod memory limit down to the working set"
  :resiliency "hold setpoint headroom before the OOM cliff")

(defband-cpu CpuBand
  :setpoint 0.80 :carve multiplicative-band :discovery kanchi-discovered :maturity shipped
  :cost "right-size the cpu limit down to the working set"
  :resiliency "keep headroom before CFS throttle — latency stays low under burst")

(defband-storage StorageBand
  :setpoint 0.80 :carve grow-only-predictive :discovery kanchi-discovered :maturity landing
  :cost "provision-minimal — no over-provisioned PVC (the 155GB waste carved away)"
  :resiliency "grow-on-demand before it fills — a disk-full outage is pre-empted")

(defband-replica ReplicaBand
  :setpoint 0.80 :carve replica-topology-scale :discovery kanchi-discovered :maturity shipped
  :cost "scale-down-when-idle to the floor — no idle replicas billed"
  :resiliency "floor-2 HA + topology-correct scale — survive a node loss")

;; DatabaseBand — LANDING. The architecture-aware, discovery-molded,
;; failover-safe-100%-spot carve. The typed contract ships in
;; breathe-invariant::database (the (defreplication-topology) forms below); the
;; live metric reader + promote/drain actuator are the C2 destination.
(defband-database DatabaseBand
  :setpoint 0.80 :carve architecture-aware-engine :discovery architecture-discovered :maturity landing
  :cost "right-size per-engine caches + connection headroom + 100% spot even the primary (safely)"
  :resiliency "discover the live topology + hold failover-safe replicas (never scale the primary; promote-before-reclaim on a primary's spot loss; never cross a quorum majority) + never-starve the buffer pool")

;; ── the DatabaseBand architecture vocabulary (the /vocabulary-bridging surface) ──
;; The typed border (src/database.rs) is authoritative; these forms are the lisp
;; vocabulary bridge. The database module's `the_database_contract_is_declared_in_the_lisp`
;; test include_str!'s this file and asserts every engine, class kind, failover
;; state, and permutation axis appears — so the Rust border and the lisp cannot drift.

;; The four coupled properties (BREATHABILITY.md §II.5):
;;   1. architecture-aware  — which pod is primary / reader / voter
;;   2. discovery-molded    — read the live engine topology, mold the carve
;;   3. failover-safe spot  — promote-before-reclaim even the primary
;;   4. configurable perms  — topology × placement × spot × replica × failover
(defreplication-topology database-architecture
  ;; the three stateful classes (couple to REPLICA_TOPOLOGY_AXIS crd_kind)
  :classes ((:class master-slave      :crd-kind "masterSlave"      :primary designated :scales read-replicas)
            (:class fully-distributed :crd-kind "fullyDistributed" :primary elected    :scales quorum-odd-steps)
            (:class persistent        :crd-kind "persistent"       :primary ordinal-0  :scales never))
  ;; the discovered live shapes (which pod is primary vs readers vs voters)
  :discovered-shapes (single-writer primary-readers quorum)
  ;; the discovered per-pod role
  :roles (primary reader voter)
  ;; the failover-safe-spot FSM — the promote-before-reclaim closed loop; the
  ;; old-primary reclaim is authorized ONLY through a promotion-receipt witness.
  :failover-fsm
    (:states (steady primary-reclaim-signaled promoting-replica failed-over old-primary-reclaimed reclaim-blocked)
     :good-terminals (steady old-primary-reclaimed)
     :load-bearing-edge (promoting-replica promotion-succeeded -> failed-over reclaim-old-primary)
     :never-lose-primary "reclaim-old-primary is emitted ONLY after a promotion; a single-writer / no-target reclaim is BLOCKED (retirada holds the node)")
  ;; the configurable permutation lattice axes (CSP-gated legality)
  :permutation-axes
    (:spot     (no-spot spot-readers-only spot-even-primary)
     :replica  (never-scale scale-readers-freely quorum-odd-steps)
     :failover (no-failover promote-before-reclaim quorum-re-elect)
     :constraint "spot-even-primary REQUIRES a failover-safe policy (else the primary is lost un-gracefully)")
  ;; the 5-engine architecture matrix (5/5 — MySQL/Postgres/Redis/Mongo/Neo4j)
  :engines ((:engine mysql    :class master-slave      :cache "innodb_buffer_pool_size"     :pool "max_connections")
            (:engine postgres :class master-slave      :cache "shared_buffers"             :pool "max_connections")
            (:engine redis    :class master-slave      :cache "maxmemory"                  :pool "maxclients")
            (:engine mongo    :class fully-distributed :cache "wiredTigerEngineRuntimeConfig" :pool "net.maxIncomingConnections")
            (:engine neo4j    :class persistent        :cache "dbms.memory.pagecache.size" :pool "dbms.connector.bolt.thread_pool_max_size")))

;; The ISOLATION posture — the SEAL that BOUNDS the carve. requests-floor /
;; limits-ceiling / QoS-class / placement-isolation carved per workload-class.
;; Unlike the others this dimension is BOTH carved AND a constraint on the other
;; carves: the seal-floor lower-bounds mem/cpu (carve never strips the seal).
;; The critical-must-be-sealed invariant is the isolation analog of the 155GB
;; models-stay-current gate — a critical workload cannot be BestEffort/no-floor.
(defband-isolation IsolationBand
  :setpoint 0.80 :carve constrained-isolation-assignment :discovery interference-discovered :maturity landing
  :variants ((:class critical :qos guaranteed  :placement anti-affinity :seal required)
             (:class standard :qos burstable   :placement co-locate     :seal reserved-floor)
             (:class batch    :qos best-effort :placement co-locate     :seal none)
             (:class noisy    :qos burstable   :placement anti-affinity :seal capped-isolated-away))
  :overlays (default kanchi-discovered contextual override)
  :cost "right-size requests/limits toward the working set without over-reserving isolation — batch bin-packs BestEffort, standard runs Burstable"
  :resiliency "seal a critical / interference-sensitive workload (guaranteed floor + Guaranteed QoS + anti-affinity) so a noisy neighbor can never throttle or evict it, and the floor bounds the carve so cost never strips the seal")
