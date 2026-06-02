//! Emit the MemoryBand CRD YAML from the typed Rust definition — generation,
//! not hand-authoring. `cargo run -p breathe-crd --bin crdgen > crds/memoryband.yaml`.

use breathe_crd::MemoryBand;
use kube::CustomResourceExt;

fn main() {
    print!("{}", serde_yaml::to_string(&MemoryBand::crd()).expect("serialize CRD"));
}
