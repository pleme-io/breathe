//! Emit the band CRD YAML from the typed Rust definitions — generation, not
//! hand-authoring. `cargo run -p breathe-crd --bin crdgen > crds/bands.yaml`.

use breathe_crd::{
    ArcBand, BreatheConfig, BreatheNodePool, BreatheOverview, CgroupBand, CgroupCpuBand, CpuBand, MemoryBand,
    StorageBand,
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
        BreatheNodePool::crd(),
        // the fleet-overview dashboard object + the fleet config
        BreatheOverview::crd(),
        BreatheConfig::crd(),
    ];
    for crd in crds {
        print!("---\n{}", serde_yaml::to_string(&crd).expect("serialize CRD"));
    }
}
