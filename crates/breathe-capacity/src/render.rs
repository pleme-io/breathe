use crate::plan::{Advisory, Plan, Refusal, Step};
use std::fmt::Write;

fn advisory(a: &Advisory) -> String {
    match a {
        Advisory::ChangeWindowRequired => "production: every step lands inside an approved change window, run by the window operator".into(),
        Advisory::NodeScaleUpExpected { step, pool, nodes } => {
            format!("step {step}: the autoscaler must add {nodes} node(s) to {pool}; the rollout waits on them")
        }
        Advisory::HpaReplicaShift { step, before, after } => {
            format!("step {step}: the HPA is expected to move from {before} to {after} replicas, because it scales on utilisation of the request")
        }
        Advisory::LimitStillTight { limit, needed } => {
            format!("the final memory limit {limit} is under observed p95 plus headroom ({needed}); raise further after this lands")
        }
        Advisory::StartupDbFanout { service } => {
            format!("{service}: new pods open their DB pool at startup; watch for connection timeouts during the rollout")
        }
        Advisory::HpaCeilingUnbacked { max_replicas, placeable } => {
            format!("hpaMaxReplicas {max_replicas} exceeds what the pools can place at their ceilings ({placeable})")
        }
        Advisory::PendingPodsPresent { cpu, mem } => {
            format!("pods are already Pending ({cpu} cpu, {mem} memory); resolve them before adding demand")
        }
    }
}

#[must_use]
pub fn refusal(r: &Refusal) -> String {
    match r {
        Refusal::NotAnIncrease { current, requested } => {
            format!("{requested} is not above the current {current}; this planner only grows")
        }
        Refusal::MinAboveMax { min, max } => format!("hpaMinReplicas {min} would exceed hpaMaxReplicas {max}; raise the max first"),
        Refusal::SingleReplica { replicas } => {
            format!("only {replicas} replica(s): a restart is an outage; raise hpaMinReplicas to 2 first")
        }
        Refusal::NoSchedulableNodes { pools } => format!("no schedulable nodes in pools {}", pools.join(", ")),
        Refusal::PodExceedsNodeShape { step, cpu, mem, best_cpu, best_mem } => format!(
            "step {step}: a {cpu}/{mem} pod fits no node (largest room {best_cpu}/{best_mem}); a larger machine type is a node-pool change first"
        ),
        Refusal::PoolCeilingInModule { step, pools } => format!(
            "step {step}: pools {} cannot hold the demand and their ceiling is fixed in the module; that is a module change first",
            pools.join(", ")
        ),
        Refusal::DbConnectionBudget { step, peak_pods, conns_per_pod, budget } => format!(
            "step {step}: {peak_pods} pods at peak x {conns_per_pod} connections exceeds the budget of {budget}"
        ),
    }
}

#[must_use]
pub fn markdown(p: &Plan) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# {} {:?} {}: {} step(s)\n", p.tenant, p.environment, p.service, p.steps.len());
    for s in &p.steps {
        match s {
            Step::PoolCeiling { index, pool, from, to } => {
                let _ = writeln!(out, "{index}. node pool `{pool}` max nodes {from} -> {to} (Terragrunt; lands and applies before the next step)");
            }
            Step::PodSizing { index, edits, prediction, .. } => {
                let changes: Vec<String> = edits.iter().map(|e| format!("`{}` {} -> {}", e.key, e.from, e.to)).collect();
                let _ = writeln!(
                    out,
                    "{index}. {} | replicas {} -> {}, peak {} pods, surge {}, {} placeable now, {} node(s) to add",
                    changes.join(", "),
                    prediction.replicas_before,
                    prediction.replicas_after,
                    prediction.peak_pods,
                    prediction.surge_pods,
                    prediction.placeable_now,
                    prediction.nodes_to_add
                );
            }
        }
    }
    if !p.advisories.is_empty() {
        out.push_str("\nAdvisories:\n");
        for a in &p.advisories {
            let _ = writeln!(out, "- {}", advisory(a));
        }
    }
    out
}
