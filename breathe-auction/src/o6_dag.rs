//! Proves theory/BREATHABILITY.md §II.6.8's O6 claim against shigoto's REAL
//! `Dag`/`JobId` API — a worked example for the exact incident that motivated
//! O6 (the `platforms/camelot.yaml` `controllers` pool floored every node's
//! pod density at 17, silently, because the node-pool-shape decision and the
//! Network/ENI ceiling it implies were never structurally ordered ahead of
//! the CPU/Memory bands that assume a pod-density ceiling).
//!
//! **Scoped honestly: this is a worked example, dev-only, not production
//! wiring.** It shows that shigoto's already-shipped `Dag::waves()` resolves
//! O6's cross-surface edges in the correct order — the mechanism the theory
//! doc names is real and behaves as claimed — but no controller in this
//! crate (or anywhere in the fleet) actually builds this Dag from a live
//! tick yet. That integration is the named follow-up.

#[cfg(test)]
mod tests {
    use shigoto_dag::Dag;
    use shigoto_types::{JobId, JobKindId, JobScope, JobSubject};

    /// One surface's per-tick `decide()` call, as a typed shigoto JobId —
    /// scoped to one node pool (the O6 edges below are all within ONE
    /// breathable autonomous zone, per §II.6.8's naming).
    fn surface_decide(pool: &str, surface: &str) -> JobId {
        JobId {
            scope: JobScope::Workspace(pool.into()),
            kind: JobKindId::new("breathe.surface-decide"),
            subject: JobSubject::Pinned(surface.into()),
        }
    }

    /// The camelot `controllers` pool's O6 edges, as theory/BREATHABILITY.md
    /// §II.6.8 states them: `NodePoolShapeDecide -> NetworkEniCeilingCheck ->
    /// {CpuBandDecide, MemoryBandDecide}`.
    fn camelot_controllers_zone() -> Dag {
        let mut d = Dag::new();
        let shape = surface_decide("camelot-controllers", "node-pool-shape");
        let eni = surface_decide("camelot-controllers", "network-eni-ceiling");
        let cpu = surface_decide("camelot-controllers", "cpu-band");
        let mem = surface_decide("camelot-controllers", "memory-band");

        d.add_edge(shape.clone(), eni.clone());
        d.add_edge(eni.clone(), cpu.clone());
        d.add_edge(eni, mem);
        d
    }

    #[test]
    fn node_pool_shape_resolves_before_the_eni_ceiling_check() {
        // The root of the incident: NodePoolShape's instance-family choice
        // must be known BEFORE the ENI ceiling it implies can be checked.
        let d = camelot_controllers_zone();
        let waves = d.waves(None).unwrap();
        let shape = surface_decide("camelot-controllers", "node-pool-shape");
        let eni = surface_decide("camelot-controllers", "network-eni-ceiling");

        assert_eq!(waves[0], vec![shape.clone()], "NodePoolShape is the only wave-0 job");
        assert!(waves[1].contains(&eni), "the ENI ceiling check is gated on wave 0");
        assert!(!waves[0].contains(&eni), "ENI ceiling never resolves before NodePoolShape");
    }

    #[test]
    fn cpu_and_memory_bands_never_resolve_before_the_eni_ceiling_is_known() {
        // The exact structural fix the incident needed: a CPU/Memory band
        // that tried to validate its pod-density assumption before the ENI
        // ceiling was known is EXACTLY the bug (a floored ceiling a human had
        // to notice by hand). Wired as an O6 Dag, that ordering is structural,
        // not a hope.
        let d = camelot_controllers_zone();
        let waves = d.waves(None).unwrap();
        let eni = surface_decide("camelot-controllers", "network-eni-ceiling");
        let cpu = surface_decide("camelot-controllers", "cpu-band");
        let mem = surface_decide("camelot-controllers", "memory-band");

        let eni_wave = waves.iter().position(|w| w.contains(&eni)).unwrap();
        let cpu_wave = waves.iter().position(|w| w.contains(&cpu)).unwrap();
        let mem_wave = waves.iter().position(|w| w.contains(&mem)).unwrap();

        assert!(cpu_wave > eni_wave, "CpuBandDecide must resolve strictly after the ENI ceiling check");
        assert!(mem_wave > eni_wave, "MemoryBandDecide must resolve strictly after the ENI ceiling check");
        // Cpu and Memory are NOT structurally coupled to each other (only to
        // the shared ENI upstream) — they land in the same wave, proving the
        // Dag doesn't over-serialize siblings that have no real O6 edge
        // between them (matching II.6.2's O1 field-partition — they still
        // run concurrently, just gated on the same real upstream).
        assert_eq!(cpu_wave, mem_wave, "unrelated siblings stay concurrent, not falsely serialized");
    }

    #[test]
    fn three_waves_total_for_this_zone() {
        let d = camelot_controllers_zone();
        let waves = d.waves(None).unwrap();
        assert_eq!(waves.len(), 3, "shape -> eni -> {{cpu, memory}} is exactly 3 waves");
        assert_eq!(d.node_count(), 4);
        assert_eq!(d.edge_count(), 3);
    }
}
