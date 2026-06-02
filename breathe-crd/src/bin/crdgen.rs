//! Emit the band CRD YAML from the typed Rust definitions — generation, not
//! hand-authoring. `cargo run -p breathe-crd --bin crdgen > crds/bands.yaml`.

use breathe_crd::{CpuBand, MemoryBand, StorageBand};
use kube::CustomResourceExt;

fn main() {
    for crd in [MemoryBand::crd(), CpuBand::crd(), StorageBand::crd()] {
        print!("---\n{}", serde_yaml::to_string(&crd).expect("serialize CRD"));
    }
}
