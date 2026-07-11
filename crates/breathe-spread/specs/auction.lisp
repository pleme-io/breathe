;;; auction.lisp — the tatara-lisp vocabulary bridge for the arch × auction × spot
;;; configuration spread (the compute/auction companion to the breathability
;;; (defbreathe-invariant) lock). One `(defauction-spread …)` form per use-case
;;; molding; the Rust border (breathe-spread::spread::MOLDINGS) is the type, this
;;; is the authored spec, and the matrix cross-checks the two never drift
;;; (/vocabulary-bridging). Naming: the auction concept is `leilão` (BR-PT) in the
;;; org vocabulary; the descriptive form-name matches the sibling lock.

;;; ── THE HARD LAW + THE INVARIANT (named once) ────────────────────────────────
;;; capacity is NOT an axis: 100% spot, never-on-demand — no on-demand arm exists
;;; (truly-unrep in Rust, parse-rejected at the Ruby boundary via reject_on_demand!).
(defauction-invariant :arch-spot-auction
  :hard-law        never-on-demand           ; capacity is not a knob — 100% spot always
  :dual-purpose    (cost-and-resiliency)      ; every spread is BOTH, one mechanism (breathability lift)
  :clauses (never-on-demand
            arch-native-cost-optimized
            evolving-degrade-total-order
            dual-purpose
            placement-safe
            cost-justified-where-abnormal
            models-stay-current)
  ;; every conflict resolves by COST; where the answer is counter-intuitive it is
  ;; LOUD + justified inline (arm-loses at the floor SAYS SO with the number).
  :cost-rule "resolve-by-cost; loud-and-inline-justified-where-abnormal")

;;; ── AXIS VOCABULARY (the six permutation dimensions) ─────────────────────────
(defauction-axes
  :arch          (cost-optimized pinned-arm64 pinned-amd64)   ; DEFAULT cost-optimized (multi-arch image → free)
  :spot-strategy (capacity-optimized price-capacity-optimized diversified) ; no lowest-price / prioritized
  :ladder        (evolving-degrade flat-pool)                 ; DEFAULT evolving-degrade (always-places)
  :perf-class    (cost-floor time-floor)                      ; spot-only; guaranteed-wake/dedicated REMOVED (on-demand)
  :placement     (single-az multi-az)                         ; DERIVED from storage binding
  :interruption  (retirada-graceful-drain retirada-node-drain retry-on-reclaim))

;;; ── THE MOLDINGS (a spread default per use-case) ─────────────────────────────

;; SaaS-steady — the CamelotNodeGroup floor. Cost-optimized arch → x86 at the floor
;; (arm-loses here: Graviton large-spot +19% pricier than m5a NOW; x86 for cost).
(defauction-spread :saas-steady
  :arch          cost-optimized      ; resolves amd64 (x86) — the LOUD floor case
  :arm-loses     "floor: Graviton m7g/m8g large-spot +19% vs m5a x86 NOW → x86 for cost; auto-flips to arm when Graviton crosses"
  :spot-strategy capacity-optimized
  :ladder        evolving-degrade
  :perf-class    cost-floor
  :placement     multi-az            ; per-replica stateful destination (single-az shipped interim)
  :interruption  retirada-graceful-drain
  :cost          "100% spot cheapest deep pool + cost-floor nodes + scale-down-idle to HA floor"
  :resiliency    "capacity-optimized deepest pool + multi-AZ per-replica + retirada drain + HA floor 2"
  :realizer      "pangea-architectures::CamelotNodeGroup")

;; Build-burst — the CamelotBuilderNodeGroup + super-cache-ci pool. Cost-optimized
;; arch → arm at the builder (the expected win: -37%/build-hr + ~18% faster).
(defauction-spread :build-burst
  :arch          cost-optimized      ; resolves arm64 — expected, not flagged
  :spot-strategy capacity-optimized  ; evolves to price-capacity-optimized at :deep/:deepest
  :ladder        evolving-degrade
  :perf-class    time-floor          ; best build times (biggest latest-gen, big RAMDISK, max-parallel)
  :placement     multi-az            ; stateless builders — deep independent pools
  :interruption  retry-on-reclaim    ; idempotent + cache-backed — no drain agent needed
  :cost          "100% spot, floor-0 scale-to-zero (near-free idle), cost-optimal arm compute"
  :resiliency    "evolving-degrade always places; multi-AZ deep pools; retry-on-reclaim survives a mid-build reclaim"
  :realizer      "pangea-architectures::CamelotBuilderNodeGroup + breathe-catalog::builder")

;; Eyes-tiny — the observability tap. Cost-optimized arch → arm at tiny sizes
;; (t4g burstable < t3 x86 — arm winning small is the norm).
(defauction-spread :eyes-tiny
  :arch          cost-optimized      ; resolves arm64 (t4g burstable) — expected
  :spot-strategy capacity-optimized
  :ladder        flat-pool           ; tiny — one small size, a preference order buys nothing
  :perf-class    cost-floor
  :placement     single-az           ; the lone eyes volume (single-instance-EBS) — genuinely correct
  :interruption  retirada-graceful-drain
  :cost          "tiny 100% spot footprint (t4g burstable) — near-free"
  :resiliency    "single-AZ keeps the lone eyes volume on a landing node; retirada drain; observation continuity"
  :realizer      "pangea-architectures::AzTopology + the tendril tap")
