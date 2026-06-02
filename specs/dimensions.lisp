;;;; breathe/specs/dimensions.lisp
;;;;
;;;; The self-describing breathe dimensions catalog, authored as tatara-lisp
;;;; data. This is the authoring surface for `breathe-catalog::CATALOG`; the
;;;; Rust border (breathe-catalog/src/lib.rs) is the typed mirror and the
;;;; CATALOG REFLECTION tests are the enforcement. Adding a dimension is one
;;;; (defdimension …) form here + one typed row there + one ResourceProvider
;;;; impl — the build fails if any of the three is missing.
;;;;
;;;; A dimension never carries band logic. :directionality tells the loop which
;;;; mutations are legal; the proven dimension-agnostic band law (breathe-control)
;;;; does the deciding. The owned :field is what the field-granular single-writer
;;;; guard checks — disjoint :fields across dimensions never fight (breathe ⟂ KEDA,
;;;; memory ⟂ cpu).

(defmeta-catalog breathe-dimensions
  :description "Every resident-problem-category breathe can hold at a utilization band."
  :reflection (every-dimension-has-a-provider-and-row
               authoring-keywords-globally-unique
               dependency-dag-acyclic
               maturity-histogram-partitions-catalog))

(defdimension-memory
  :name        "memory"
  :maturity    :working
  :directionality :bidirectional      ; freely returnable; shrink clamped ≥ ws/setpoint ⇒ never OOM
  :observe     "container_memory_working_set_bytes / limits.memory (bytes)"
  :field       "resources.limits.memory"
  :manager     "breathe/memory"
  :assign      :ssa-apply             ; owner rolls one bounded ReplicaSet generation
  :semantics   :transactional
  :depends-on  ("replica")            ; replica-ceiling context (M3 typed channel)
  :purpose     "hold container memory at the band by carving resources.limits.memory")

(defdimension-storage
  :name        "storage"
  :maturity    :m2typed
  :directionality :grow-only          ; data persists; online-resize is irreversible
  :observe     "kubelet_volume_stats_used_bytes / _capacity_bytes (bytes)"
  :field       "spec.resources.requests.storage"
  :manager     "breathe/storage"
  :assign      :ssa-apply             ; CSI online-resize, no pod restart
  :semantics   :continuous-reconciliation
  :depends-on  ()
  :purpose     "grow PVC capacity at 80% (data persists; never shrink)")

(defdimension-cpu
  :name        "cpu"
  :maturity    :m2typed
  :directionality :bidirectional      ; safe-min clamp ⇒ never throttle live demand
  :observe     "rate(container_cpu_usage_seconds_total) → millicores / limits.cpu"
  :field       "resources.limits.cpu"
  :manager     "breathe/cpu"
  :assign      :ssa-apply             ; in-place (InPlacePodVerticalScaling) or roll fallback
  :semantics   :partial-progress
  :depends-on  ("replica")
  :purpose     "hold cpu at the band by carving resources.limits.cpu (millicores)")

(defdimension-replica
  :name        "replica"
  :maturity    :informational
  :directionality :observe-only       ; KEDA owns spec.replicas; breathe never writes it
  :observe     "status.replicas + KEDA ScaledObject (read-only)"
  :field       "spec.replicas"
  :manager     "keda-operator"        ; the OTHER manager — breathe yields this field by construction
  :assign      :none
  :semantics   :observe-only
  :depends-on  ()
  :mirrors     "KEDA ScaledObject"
  :purpose     "observe replica count; compose with KEDA via disjoint fields (never write)")

;;; ── HOST dimensions ────────────────────────────────────────────────────────
;;; Boundary = the HostCluster impl (systemd/sysfs), NOT the k8s API. The
;;; interpreter (breathe-host::HostCluster) rides WITHIN the static nodeBudget L2
;;; envelopes: it writes ONLY runtime params + `--runtime` transient cgroup
;;; properties, disjoint from what nix owns (boot modprobe / static MemoryMax /
;;; cpusets). The applied value is refused above the BreatheNodePool ceiling — the
;;; second safety wall, independent of the band law's safety_clamp.

(defdimension-arc
  :name        "arc"
  :maturity    :working
  :directionality :bidirectional      ; shrink frees page cache immediately; safe-min clamp protects it
  :observe     "arcstats.size / zfs_arc_max (bytes)"
  :field       "host.zfs.arc_max"
  :manager     "breathe/arc"
  :assign      :host-sysfs            ; write /sys/module/zfs/parameters/zfs_arc_max
  :semantics   :continuous-reconciliation
  :depends-on  ()
  :mirrors     "/sys/module/zfs/parameters/zfs_arc_max"
  :ceiling     "BreatheNodePool.arcMaxGiB (= nodeBudget.arcMaxGiB, the boot modprobe cap)"
  :purpose     "hold the ZFS ARC at the band by carving zfs_arc_max within nodeBudget.arcMaxGiB")

(defdimension-cgroup
  :name        "cgroup"
  :maturity    :working
  :directionality :bidirectional      ; transient MemoryHigh; never the static unit MemoryMax (nix owns that)
  :observe     "<unit> cgroup memory.current / MemoryHigh (bytes)"
  :field       "host.cgroup.memory_high"
  :manager     "breathe/cgroup"
  :assign      :host-set-property     ; systemctl set-property --runtime <unit> MemoryHigh=
  :semantics   :continuous-reconciliation
  :depends-on  ()
  :mirrors     "systemctl set-property --runtime <unit> MemoryHigh"
  :ceiling     "BreatheNodePool.cgroupMaxGiB[<unit>] (= nodeBudget per-unit memoryMaxGiB)"
  :purpose     "hold a unit's working set at the band by carving transient MemoryHigh within its envelope")
