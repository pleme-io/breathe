//! Emit the band CRD YAML from the typed Rust definitions — generation, not
//! hand-authoring. `cargo run -p breathe-crd --bin crdgen > crds/bands.yaml`.

use breathe_crd::{
    ArcBand, BreatheCloudPool, BreatheConfig, BreatheNodePool, BreatheOverview, CgroupBand, CgroupCpuBand, CpuBand,
    Densa, HostParamBand, KubeParamBand, MemoryBand, PodMemoryHigh, QuinhaoPool, StorageBand,
};
use kube::CustomResourceExt;

fn main() {
    let crds = [
        MemoryBand::crd(),
        CpuBand::crd(),
        StorageBand::crd(),
        // host dimensions + the node enrollment charter
        ArcBand::crd(),
        CgroupBand::crd(),
        CgroupCpuBand::crd(),
        // PR-2 — the generic sysctl / ZFS-param band (every vector is an instance)
        HostParamBand::crd(),
        // Step-6/8/12 — the generic k8s-CR / app band (Istio/ResourceQuota/CR fields)
        KubeParamBand::crd(),
        BreatheNodePool::crd(),
        // Part 1 — the SOFT-k8s-carve controller→host-agent dispatch (pod memory.high)
        PodMemoryHigh::crd(),
        // BU2 — the node-count Forma enrollment (Forma ⇄ Densa envelope)
        BreatheCloudPool::crd(),
        // the fleet-overview dashboard object + the fleet config
        BreatheOverview::crd(),
        BreatheConfig::crd(),
        // the per-namespace capacity + cost envelope (the L2 wall bands carve within)
        Densa::crd(),
        // the hierarchical-vector fair-share allocator (groups→users split the band)
        QuinhaoPool::crd(),
    ];
    for crd in crds {
        print!("---\n{}", serde_yaml::to_string(&crd).expect("serialize CRD"));
    }
}
