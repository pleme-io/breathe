//! `breathe-kube` — the Kubernetes [`breathe_provider::Cluster`] implementation.
//!
//! Two halves: the pure `managed_fields` ownership parser (the field-granular
//! single-writer guard's data source — fully testable without a cluster), and
//! `KubeCluster` (the kube-rs I/O: true-SSA apply, owner-spec reads, Prometheus
//! metric scrapes). The pure half lands first so the load-bearing ownership
//! logic is proven before any cluster I/O.

pub mod kube_cluster;
pub mod managed_fields;
pub mod pod_cgroup;
pub mod replica_env;

pub use kube_cluster::KubeCluster;
pub use replica_env::KubeReplicaEnv;
pub use managed_fields::{
    cnpg_cluster_limit_segments, field_owners, pod_template_limit_segments, replicas_segments,
};
pub use pod_cgroup::{
    container_id_from_status, node_name_from_pod, pod_coords_from_value, PodCgroupCoords, PodCoordError,
};
