use crate::quantity::{Bytes, Millicores};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

const POOL_LABELS: [&str; 5] = [
    "cloud.google.com/gke-nodepool",
    "kubernetes.azure.com/agentpool",
    "agentpool",
    "eks.amazonaws.com/nodegroup",
    "karpenter.sh/nodepool",
];

#[derive(Debug, thiserror::Error)]
pub enum ObserveError {
    #[error("{0} is not a kubectl List (no .items array)")]
    NotAList(&'static str),
    #[error("node {0} has no parseable status.allocatable cpu/memory")]
    NodeAllocatable(String),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeState {
    pub name: String,
    pub pool: String,
    pub schedulable: bool,
    pub alloc_cpu: Millicores,
    pub alloc_mem: Bytes,
    pub req_cpu: Millicores,
    pub req_mem: Bytes,
    pub daemon_cpu: Millicores,
    pub daemon_mem: Bytes,
}

impl NodeState {
    #[must_use]
    pub fn free_cpu(&self) -> u64 {
        self.alloc_cpu.0.saturating_sub(self.req_cpu.0)
    }

    #[must_use]
    pub fn free_mem(&self) -> u64 {
        self.alloc_mem.0.saturating_sub(self.req_mem.0)
    }

    #[must_use]
    pub fn room_for_one_pod_cpu(&self) -> u64 {
        self.alloc_cpu.0.saturating_sub(self.daemon_cpu.0)
    }

    #[must_use]
    pub fn room_for_one_pod_mem(&self) -> u64 {
        self.alloc_mem.0.saturating_sub(self.daemon_mem.0)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterState {
    pub nodes: Vec<NodeState>,
    pub pending_cpu: Millicores,
    pub pending_mem: Bytes,
}

impl ClusterState {
    #[must_use]
    pub fn pool(&self, name: &str) -> Vec<&NodeState> {
        self.nodes.iter().filter(|n| n.pool == name).collect()
    }

    pub fn schedulable(&self) -> impl Iterator<Item = &NodeState> {
        self.nodes.iter().filter(|n| n.schedulable)
    }

    pub fn from_kubectl(nodes: &Value, pods: &Value) -> Result<Self, ObserveError> {
        let node_items = nodes["items"].as_array().ok_or(ObserveError::NotAList("nodes"))?;
        let pod_items = pods["items"].as_array().ok_or(ObserveError::NotAList("pods"))?;

        let mut by_name: BTreeMap<String, NodeState> = BTreeMap::new();
        for n in node_items {
            let name = n["metadata"]["name"].as_str().unwrap_or_default().to_string();
            let labels = &n["metadata"]["labels"];
            let pool = POOL_LABELS
                .iter()
                .find_map(|k| labels[*k].as_str())
                .unwrap_or("unlabelled")
                .to_string();
            let alloc = &n["status"]["allocatable"];
            let alloc_cpu = alloc["cpu"].as_str().and_then(Millicores::parse);
            let alloc_mem = alloc["memory"].as_str().and_then(Bytes::parse);
            let (Some(alloc_cpu), Some(alloc_mem)) = (alloc_cpu, alloc_mem) else {
                return Err(ObserveError::NodeAllocatable(name));
            };
            let schedulable = !n["spec"]["unschedulable"].as_bool().unwrap_or(false);
            by_name.insert(
                name.clone(),
                NodeState { name, pool, schedulable, alloc_cpu, alloc_mem, ..NodeState::default() },
            );
        }

        let mut state = ClusterState::default();
        for p in pod_items {
            let phase = p["status"]["phase"].as_str().unwrap_or("");
            if matches!(phase, "Succeeded" | "Failed") {
                continue;
            }
            let (cpu, mem) = pod_requests(&p["spec"]);
            let daemon = p["metadata"]["ownerReferences"]
                .as_array()
                .is_some_and(|o| o.iter().any(|r| r["kind"] == "DaemonSet"));
            match p["spec"]["nodeName"].as_str().and_then(|n| by_name.get_mut(n)) {
                Some(node) => {
                    node.req_cpu.0 += cpu;
                    node.req_mem.0 += mem;
                    if daemon {
                        node.daemon_cpu.0 += cpu;
                        node.daemon_mem.0 += mem;
                    }
                }
                None if !daemon => {
                    state.pending_cpu.0 += cpu;
                    state.pending_mem.0 += mem;
                }
                None => {}
            }
        }
        state.nodes = by_name.into_values().collect();
        Ok(state)
    }
}

fn container_requests(c: &Value) -> (u64, u64) {
    let r = &c["resources"]["requests"];
    (
        r["cpu"].as_str().and_then(Millicores::parse).map_or(0, |m| m.0),
        r["memory"].as_str().and_then(Bytes::parse).map_or(0, |b| b.0),
    )
}

fn pod_requests(spec: &Value) -> (u64, u64) {
    let sum = spec["containers"].as_array().into_iter().flatten().map(container_requests).fold(
        (0, 0),
        |(a, b), (c, m)| (a + c, b + m),
    );
    let init = spec["initContainers"].as_array().into_iter().flatten().map(container_requests).fold(
        (0, 0),
        |(a, b), (c, m)| (a.max(c), b.max(m)),
    );
    let overhead = &spec["overhead"];
    let oc = overhead["cpu"].as_str().and_then(Millicores::parse).map_or(0, |m| m.0);
    let om = overhead["memory"].as_str().and_then(Bytes::parse).map_or(0, |b| b.0);
    (sum.0.max(init.0) + oc, sum.1.max(init.1) + om)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn folds_pod_requests_onto_nodes_and_separates_daemonsets() {
        let nodes = json!({"items": [{
            "metadata": {"name": "n1", "labels": {"cloud.google.com/gke-nodepool": "pool-a"}},
            "status": {"allocatable": {"cpu": "7910m", "memory": "28Gi"}}
        }]});
        let pods = json!({"items": [
            {"metadata": {"ownerReferences": [{"kind": "DaemonSet"}]},
             "spec": {"nodeName": "n1", "containers": [{"resources": {"requests": {"cpu": "100m", "memory": "128Mi"}}}]},
             "status": {"phase": "Running"}},
            {"metadata": {}, "spec": {"nodeName": "n1", "containers": [
                {"resources": {"requests": {"cpu": "1", "memory": "1Gi"}}},
                {"resources": {"requests": {"cpu": "500m"}}}]}, "status": {"phase": "Running"}},
            {"metadata": {}, "spec": {"containers": [{"resources": {"requests": {"cpu": "2", "memory": "2Gi"}}}]},
             "status": {"phase": "Pending"}},
            {"metadata": {}, "spec": {"nodeName": "n1", "containers": [{"resources": {"requests": {"cpu": "9"}}}]},
             "status": {"phase": "Succeeded"}}
        ]});
        let s = ClusterState::from_kubectl(&nodes, &pods).unwrap();
        let n = &s.nodes[0];
        assert_eq!(n.pool, "pool-a");
        assert_eq!(n.req_cpu, Millicores(1600));
        assert_eq!(n.daemon_cpu, Millicores(100));
        assert_eq!(s.pending_cpu, Millicores(2000));
        assert_eq!(n.free_mem(), 28 * 1024 * 1024 * 1024 - (1024 + 128) * 1024 * 1024);
    }
}
