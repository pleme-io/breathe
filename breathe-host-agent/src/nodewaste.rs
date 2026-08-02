//! The node-waste reporter — breathe's answer to "is this node earning its cost".
//!
//! Every band this agent reconciles carves WITHIN the host. None of them can say
//! whether the host should exist. On 2026-08-02 that gap was measured on
//! camelot-eks: five `m6a.2xlarge` ON-DEMAND nodes sat at 19-42m CPU holding ARC
//! runner pods that had claimed no job, invisible to Karpenter's `WhenEmpty`
//! consolidation (the pods existed) and invisible to breathe (no node-level
//! opinion existed). This closes that.
//!
//! Read-only by construction. It emits gauges and nothing else — it never
//! cordons, drains, evicts or patches. The verdict is a signal for the auction
//! and retirada layers to act on, and for an operator to see; turning a verdict
//! into a node deletion is a separate, deliberate decision.
//!
//! Usage comes from the host's own `/proc`, mounted at `HOST_ROOT`, not from
//! metrics-server. metrics-server is a cluster-wide poll that answers when asked,
//! and it was in place while those five nodes idled unnoticed. The kubelet's own
//! host knows continuously.

use std::{sync::Arc, time::Duration};

use breathe_nodewaste::{classify, NodeUsage, Phase, PodFact, Thresholds, Verdict};
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    api::{Api, ListParams},
    Client, ResourceExt,
};

/// One `/proc/stat` reading. CPU is a counter, so a rate needs two samples.
#[derive(Clone, Copy)]
struct CpuTicks {
    busy: u64,
    total: u64,
}

fn read_cpu_ticks(host_root: &str) -> Option<CpuTicks> {
    let raw = std::fs::read_to_string(format!("{host_root}/proc/stat")).ok()?;
    let line = raw.lines().next()?;
    let mut it = line.split_whitespace();
    if it.next()? != "cpu" {
        return None;
    }
    let vals: Vec<u64> = it.filter_map(|v| v.parse::<u64>().ok()).collect();
    if vals.len() < 4 {
        return None;
    }
    let total: u64 = vals.iter().sum();
    // idle = field 3, iowait = field 4. Everything else is busy.
    let idle = vals[3] + vals.get(4).copied().unwrap_or(0);
    Some(CpuTicks { busy: total.saturating_sub(idle), total })
}

/// Memory in use as a percent of capacity, from `MemTotal - MemAvailable`.
/// `MemAvailable` is the right numerator: it already excludes reclaimable page
/// cache, so a node whose RAM is mostly cache does not read as resident.
fn read_mem_pct(host_root: &str) -> Option<u32> {
    let raw = std::fs::read_to_string(format!("{host_root}/proc/meminfo")).ok()?;
    let mut total = 0u64;
    let mut avail = 0u64;
    for line in raw.lines() {
        let mut it = line.split_whitespace();
        match it.next() {
            Some("MemTotal:") => total = it.next()?.parse().ok()?,
            Some("MemAvailable:") => avail = it.next()?.parse().ok()?,
            _ => {}
        }
    }
    if total == 0 {
        return None;
    }
    Some(u32::try_from(total.saturating_sub(avail) * 100 / total).unwrap_or(100))
}

fn pod_facts(pods: &[Pod]) -> Vec<PodFact> {
    pods.iter()
        .map(|p| {
            let owner = p
                .owner_references()
                .first()
                .map(|o| o.kind.as_str())
                .unwrap_or_default();
            let phase = p
                .status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .unwrap_or("Unknown");
            PodFact {
                daemonset: owner == "DaemonSet",
                stateful: owner == "StatefulSet",
                phase: match phase {
                    "Running" => Phase::Running,
                    "Pending" => Phase::Pending,
                    "Succeeded" => Phase::Succeeded,
                    "Failed" => Phase::Failed,
                    _ => Phase::Unknown,
                },
                terminating: p.metadata.deletion_timestamp.is_some(),
            }
        })
        .collect()
}

/// Emit one sample. Split out so the gauge surface is one place.
fn emit(node: &str, v: &breathe_nodewaste::NodeVerdict) {
    metrics::gauge!("breathe_node_millicpu", "node" => node.to_owned())
        .set(f64::from(v.usage.millicpu));
    metrics::gauge!("breathe_node_mem_percent", "node" => node.to_owned())
        .set(f64::from(v.usage.mem_pct));
    metrics::gauge!("breathe_node_holders", "node" => node.to_owned())
        .set(v.holders as f64);
    metrics::gauge!("breathe_node_live_pods", "node" => node.to_owned())
        .set(v.live as f64);
    // The load-bearing one. A single series an alert or the auction layer can
    // read without re-deriving any of the rules that make it correct.
    metrics::gauge!(
        "breathe_node_wasteful",
        "node" => node.to_owned(),
        "verdict" => v.verdict.as_str(),
    )
    .set(if v.verdict.is_waste() { 1.0 } else { 0.0 });
}

/// Run forever, sampling this node.
///
/// A failed sample is skipped, never fatal: an agent that exits on a transient
/// read error stops watching the node it exists to watch, which is strictly
/// worse than a gap in the series.
pub async fn run(client: Client, node_name: String, interval: Duration) {
    let host_root = std::env::var("HOST_ROOT").unwrap_or_default();
    let pods: Api<Pod> = Api::all(client.clone());
    let nodes: Api<Node> = Api::all(client);
    let field = format!("spec.nodeName={node_name}");
    let thresholds = Thresholds::default();

    // Held across ticks so CPU can be a rate rather than a meaningless total.
    let mut prev = read_cpu_ticks(&host_root);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

        // `known` is false unless BOTH readings land. Without that a failed read
        // reports 0m/0% and every node classifies idle the moment /proc hiccups,
        // turning a read error into a fleet-wide waste signal.
        let usage = match (read_cpu_ticks(&host_root), read_mem_pct(&host_root), prev) {
            (Some(now), Some(mem_pct), Some(before)) => {
                let dt = now.total.saturating_sub(before.total);
                let db = now.busy.saturating_sub(before.busy);
                prev = Some(now);
                if dt == 0 {
                    NodeUsage::default()
                } else {
                    // ticks are per-CPU-aggregate, so busy/total * 1000 is
                    // millicores-per-core; scale by core count for node millicpu.
                    let cores = u64::from(std::thread::available_parallelism().map_or(1u32, |n| {
                        u32::try_from(n.get()).unwrap_or(1)
                    }));
                    let millicpu = u32::try_from(db * 1000 * cores / dt).unwrap_or(u32::MAX);
                    NodeUsage { millicpu, mem_pct, known: true }
                }
            }
            (Some(now), _, _) => {
                prev = Some(now);
                NodeUsage::default()
            }
            _ => NodeUsage::default(),
        };

        let listed = match pods.list(&ListParams::default().fields(&field)).await {
            Ok(l) => l.items,
            Err(e) => {
                tracing::warn!(error = %e, "nodewaste: listing this node's pods failed; skipping tick");
                continue;
            }
        };

        let age = match nodes.get(&node_name).await {
            Ok(n) => n
                .metadata
                .creation_timestamp
                .as_ref()
                .and_then(|t| {
                    // k8s Time wraps a chrono DateTime; go through SystemTime so
                    // this crate needs no chrono dependency of its own.
                    std::time::SystemTime::from(t.0)
                        .elapsed()
                        .ok()
                })
                .unwrap_or_default(),
            Err(_) => Duration::default(),
        };

        let verdict = classify(&pod_facts(&listed), usage, age, thresholds);
        if verdict.verdict.is_waste() {
            tracing::info!(
                node = %node_name,
                verdict = verdict.verdict.as_str(),
                millicpu = verdict.usage.millicpu,
                mem_pct = verdict.usage.mem_pct,
                holders = verdict.holders,
                live = verdict.live,
                "node is not earning its cost"
            );
        }
        emit(&node_name, &verdict);
    }
}

/// Spawn the reporter. Returns immediately; the task owns its own failures.
pub fn spawn(client: Client, node_name: String, interval: Duration) {
    if node_name.is_empty() {
        tracing::warn!("nodewaste: NODE_NAME is empty, reporter not started");
        return;
    }
    let _ = Arc::new(());
    tokio::spawn(run(client, node_name, interval));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_daemonset_pod_is_recognised_as_one() {
        // Misreading owner kind here would make every node look occupied, since
        // every node runs the CNI, kube-proxy and this agent itself.
        let json = serde_json::json!({
            "metadata": {"name": "cni", "ownerReferences": [{"apiVersion":"apps/v1","kind":"DaemonSet","name":"aws-node","uid":"x","controller":true}]},
            "status": {"phase": "Running"}
        });
        let pod: Pod = serde_json::from_value(json).unwrap();
        let f = &pod_facts(&[pod])[0];
        assert!(f.daemonset, "a DaemonSet pod must not hold a node");
        assert!(!f.stateful);
    }

    #[test]
    fn a_terminating_pod_is_not_live() {
        let json = serde_json::json!({
            "metadata": {"name": "stuck", "deletionTimestamp": "2026-08-02T00:00:00Z",
                         "ownerReferences": [{"apiVersion":"batch/v1","kind":"Job","name":"j","uid":"y","controller":true}]},
            "status": {"phase": "Running"}
        });
        let pod: Pod = serde_json::from_value(json).unwrap();
        let f = &pod_facts(&[pod])[0];
        assert!(f.terminating);
        assert!(!f.is_live(), "a wedged pod blocks WhenEmpty while doing no work");
    }

    #[test]
    fn a_statefulset_pod_marks_the_node_stateful() {
        let json = serde_json::json!({
            "metadata": {"name": "mysql-0", "ownerReferences": [{"apiVersion":"apps/v1","kind":"StatefulSet","name":"mysql","uid":"z","controller":true}]},
            "status": {"phase": "Running"}
        });
        let pod: Pod = serde_json::from_value(json).unwrap();
        assert!(pod_facts(&[pod])[0].stateful, "state is never idle waste");
    }

    #[test]
    fn a_pod_with_no_owner_still_holds_the_node() {
        // A bare pod has no controller to recreate it, so it holds its node just
        // as firmly as a Deployment's does.
        let json = serde_json::json!({"metadata": {"name": "bare"}, "status": {"phase": "Running"}});
        let pod: Pod = serde_json::from_value(json).unwrap();
        let f = &pod_facts(&[pod])[0];
        assert!(!f.daemonset && !f.stateful && f.is_live());
    }

    #[test]
    fn verdict_of_a_jobless_runner_node_is_waste() {
        let json = serde_json::json!({
            "metadata": {"name": "runner", "ownerReferences": [{"apiVersion":"batch/v1","kind":"Job","name":"j","uid":"u","controller":true}]},
            "status": {"phase": "Running"}
        });
        let pod: Pod = serde_json::from_value(json).unwrap();
        let v = classify(
            &pod_facts(&[pod]),
            NodeUsage { millicpu: 21, mem_pct: 4, known: true },
            Duration::from_secs(3 * 3600),
            Thresholds::default(),
        );
        assert_eq!(v.verdict, Verdict::Idle);
        assert!(v.verdict.is_waste(), "this is the measured camelot shape");
    }
}
