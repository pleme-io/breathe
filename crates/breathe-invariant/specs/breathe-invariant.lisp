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

(defband-database DatabaseBand
  :setpoint 0.80 :carve architecture-aware-engine :discovery architecture-discovered :maturity gap
  :pending "architecture-aware discovering DatabaseBand is a Gap — db_matrix carves engine knobs as AppParam instances; the discovery+failover-safe-spot Band is unbuilt"
  :cost "right-size per-engine caches + connection headroom"
  :resiliency "discover + hold failover-safe replicas + never-starve the buffer pool")
