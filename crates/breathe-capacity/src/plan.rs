use crate::model::{Keys, Policy, PoolDecl, Raise, Request, Sizing, Tier};
use crate::observe::ClusterState;
use crate::quantity::{Bytes, Millicores};
use crate::values::Edit;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "refusal")]
pub enum Refusal {
    NotAnIncrease { current: String, requested: String },
    MinAboveMax { min: u32, max: u32 },
    SingleReplica { replicas: u32 },
    NoSchedulableNodes { pools: Vec<String> },
    PodExceedsNodeShape { step: u32, cpu: String, mem: String, best_cpu: String, best_mem: String },
    PoolCeilingInModule { step: u32, pools: Vec<String> },
    DbConnectionBudget { step: u32, peak_pods: u32, conns_per_pod: u32, budget: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "advisory")]
pub enum Advisory {
    ChangeWindowRequired,
    NodeScaleUpExpected { step: u32, pool: String, nodes: u32 },
    HpaReplicaShift { step: u32, before: u32, after: u32 },
    LimitStillTight { limit: String, needed: String },
    StartupDbFanout { service: String },
    HpaCeilingUnbacked { max_replicas: u32, placeable: u32 },
    PendingPodsPresent { cpu: String, mem: String },
    NoPodsObserved { service: String },
    UndeclaredPool { pool: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum Step {
    PoolCeiling { index: u32, pool: String, from: u32, to: u32 },
    PodSizing { index: u32, edits: Vec<Edit>, before: Sizing, after: Sizing, prediction: Prediction },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Prediction {
    pub replicas_before: u32,
    pub replicas_after: u32,
    pub surge_pods: u32,
    pub peak_pods: u32,
    pub placeable_now: u32,
    pub nodes_to_add: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    pub tenant: String,
    pub environment: Tier,
    pub service: String,
    pub steps: Vec<Step>,
    pub advisories: Vec<Advisory>,
}

fn scale(v: u64, pct: u32) -> u64 {
    (u128::from(v) * u128::from(pct)).div_ceil(100).try_into().unwrap_or(u64::MAX)
}

fn ladder(from: u64, to: u64, max_step_pct: u32) -> Vec<u64> {
    let mut out = Vec::new();
    let mut cur = from;
    while cur < to {
        cur = scale(cur, max_step_pct).max(cur + 1).min(to);
        out.push(cur);
    }
    out
}

fn sizings(current: Sizing, raise: Raise, policy: &Policy) -> Result<Vec<Sizing>, Refusal> {
    let not_inc = |c: String, r: String| Refusal::NotAnIncrease { current: c, requested: r };
    let ratio = policy
        .limit_to_request_pct
        .unwrap_or_else(|| u32::try_from(current.mem_limit.0 * 100 / current.mem_request.0.max(1)).unwrap_or(200));
    match raise {
        Raise::MemoryRequest(to) => {
            let to = to.round_up_mi();
            if to <= current.mem_request {
                return Err(not_inc(current.mem_request.render(), to.render()));
            }
            Ok(ladder(current.mem_request.0, to.0, policy.max_step_pct)
                .into_iter()
                .map(|r| {
                    let req = Bytes(r).round_up_mi();
                    let lim = Bytes(scale(req.0, ratio)).round_up_mi().max(current.mem_limit).max(req);
                    Sizing { mem_request: req, mem_limit: lim, ..current }
                })
                .collect())
        }
        Raise::CpuRequest(to) => {
            let to = to.round_up_10();
            if to <= current.cpu_request {
                return Err(not_inc(current.cpu_request.render(), to.render()));
            }
            Ok(ladder(current.cpu_request.0, to.0, policy.max_step_pct)
                .into_iter()
                .map(|c| Sizing { cpu_request: Millicores(c).round_up_10(), ..current })
                .collect())
        }
        Raise::MinReplicas(to) => {
            if to <= current.hpa_min {
                return Err(not_inc(current.hpa_min.to_string(), to.to_string()));
            }
            if to > current.hpa_max {
                return Err(Refusal::MinAboveMax { min: to, max: current.hpa_max });
            }
            Ok(ladder(current.hpa_min.into(), to.into(), policy.max_step_pct)
                .into_iter()
                .map(|m| Sizing { hpa_min: u32::try_from(m).unwrap_or(u32::MAX), ..current })
                .collect())
        }
        Raise::MaxReplicas(to) => {
            if to <= current.hpa_max {
                return Err(not_inc(current.hpa_max.to_string(), to.to_string()));
            }
            Ok(vec![Sizing { hpa_max: to, ..current }])
        }
    }
}

fn edits(svc: &str, keys: &Keys, a: Sizing, b: Sizing) -> Vec<Edit> {
    let mut out = Vec::new();
    let mut push = |key: &String, from: String, to: String| {
        if from != to {
            out.push(Edit { service: svc.to_string(), key: key.clone(), from, to });
        }
    };
    push(&keys.cpu_request, a.cpu_request.render(), b.cpu_request.render());
    push(&keys.mem_request, a.mem_request.render(), b.mem_request.render());
    push(&keys.mem_limit, a.mem_limit.render(), b.mem_limit.render());
    push(&keys.hpa_min, a.hpa_min.to_string(), b.hpa_min.to_string());
    push(&keys.hpa_max, a.hpa_max.to_string(), b.hpa_max.to_string());
    out
}

fn hpa_desired(replicas: u32, before: Sizing, after: Sizing, req: &Request) -> u32 {
    let n = u64::from(replicas);
    let mem_load = req.observed.mem_p95.map_or_else(|| n * scale(before.mem_request.0, before.hpa_mem_pct), |u| n * u.0);
    let cpu_load = req.observed.cpu_p95.map_or_else(|| n * scale(before.cpu_request.0, before.hpa_cpu_pct), |u| n * u.0);
    let per_mem = scale(after.mem_request.0, after.hpa_mem_pct).max(1);
    let per_cpu = scale(after.cpu_request.0, after.hpa_cpu_pct).max(1);
    let want = mem_load.div_ceil(per_mem).max(cpu_load.div_ceil(per_cpu));
    u32::try_from(want).unwrap_or(u32::MAX).clamp(after.hpa_min, after.hpa_max)
}

#[derive(Debug, Clone)]
struct Slot {
    pool: String,
    free_cpu: u64,
    free_mem: u64,
    room_cpu: u64,
    room_mem: u64,
}

#[derive(Clone)]
struct Shadow<'a> {
    pools: &'a [PoolDecl],
    slots: Vec<Slot>,
    counts: Vec<u32>,
    added: Vec<u32>,
}

#[derive(Debug, Default)]
struct Demand {
    pods: u32,
    cpu: u64,
    mem: u64,
    steady_cpu: u64,
    steady_mem: u64,
}

const MAX_NODES_PER_STEP: u32 = 1000;

impl<'a> Shadow<'a> {
    fn new(cluster: &ClusterState, pools: &'a [PoolDecl]) -> Self {
        let slots = cluster
            .schedulable()
            .filter(|n| pools.iter().any(|p| p.name == n.pool))
            .map(|n| Slot {
                pool: n.pool.clone(),
                free_cpu: n.free_cpu(),
                free_mem: n.free_mem(),
                room_cpu: n.room_for_one_pod_cpu(),
                room_mem: n.room_for_one_pod_mem(),
            })
            .collect();
        let counts = pools.iter().map(|p| u32::try_from(cluster.pool(&p.name).len()).unwrap_or(u32::MAX)).collect();
        Self { pools, slots, counts, added: vec![0; pools.len()] }
    }

    fn best_room(&self) -> (u64, u64) {
        self.slots.iter().map(|s| (s.room_cpu, s.room_mem)).max_by_key(|&(c, m)| (m, c)).unwrap_or((0, 0))
    }

    fn fits(cpu: u64, mem: u64, free_cpu: u64, free_mem: u64) -> u64 {
        let by_cpu = free_cpu.checked_div(cpu).unwrap_or(u64::MAX);
        let by_mem = free_mem.checked_div(mem).unwrap_or(u64::MAX);
        by_cpu.min(by_mem)
    }

    fn place(&mut self, pods: u32, cpu: u64, mem: u64) -> Vec<usize> {
        let mut at = Vec::new();
        for (i, s) in self.slots.iter_mut().enumerate() {
            while at.len() < pods as usize && Self::fits(cpu, mem, s.free_cpu, s.free_mem) >= 1 {
                s.free_cpu -= cpu;
                s.free_mem -= mem;
                at.push(i);
            }
        }
        at
    }

    fn unplace(&mut self, at: &[usize], cpu: u64, mem: u64) {
        for &i in at {
            self.slots[i].free_cpu += cpu;
            self.slots[i].free_mem += mem;
        }
    }

    fn consume(&mut self, mut cpu: u64, mut mem: u64) -> (u64, u64) {
        for s in &mut self.slots {
            let c = cpu.min(s.free_cpu);
            let m = mem.min(s.free_mem);
            s.free_cpu -= c;
            s.free_mem -= m;
            cpu -= c;
            mem -= m;
        }
        (cpu, mem)
    }

    fn per_node(&self, pool: &str, cpu: u64, mem: u64) -> u64 {
        self.slots.iter().filter(|s| s.pool == pool).map(|s| Self::fits(cpu, mem, s.room_cpu, s.room_mem)).max().unwrap_or(0)
    }

    fn grow(&mut self, i: usize, cpu: u64, mem: u64) -> bool {
        let name = &self.pools[i].name;
        let Some(t) = self
            .slots
            .iter()
            .filter(|s| &s.pool == name && Self::fits(cpu, mem, s.room_cpu, s.room_mem) >= 1)
            .max_by_key(|s| (s.room_mem, s.room_cpu))
            .cloned()
        else {
            return false;
        };
        self.slots.push(Slot { free_cpu: t.room_cpu, free_mem: t.room_mem, ..t });
        self.counts[i] += 1;
        self.added[i] += 1;
        true
    }

    fn satisfy(&mut self, d: &Demand, ceiling: impl Fn(usize, &PoolDecl) -> u32) -> Option<Vec<usize>> {
        let mut at = self.place(d.pods, d.cpu, d.mem);
        let (mut sc, mut sm) = self.consume(d.steady_cpu, d.steady_mem);
        let mut grown = 0;
        while (at.len() < d.pods as usize || sc > 0 || sm > 0) && grown < MAX_NODES_PER_STEP {
            let next = (0..self.pools.len()).find(|&i| self.counts[i] < ceiling(i, &self.pools[i]) && self.per_node(&self.pools[i].name, d.cpu, d.mem) > 0);
            let Some(i) = next else { break };
            if !self.grow(i, d.cpu, d.mem) {
                break;
            }
            grown += 1;
            let more = self.place(d.pods - u32::try_from(at.len()).unwrap_or(0), d.cpu, d.mem);
            at.extend(more);
            (sc, sm) = self.consume(sc, sm);
        }
        (at.len() == d.pods as usize && sc == 0 && sm == 0).then_some(at)
    }
}

fn attempt<'a>(sh: &Shadow<'a>, d: &Demand, lift: Option<usize>) -> Option<(Shadow<'a>, Vec<usize>)> {
    let mut s = sh.clone();
    let at = s.satisfy(d, |i, p| if Some(i) == lift { u32::MAX } else { p.max_nodes })?;
    Some((s, at))
}

#[allow(clippy::too_many_lines)]
pub fn plan(req: &Request, current: Sizing, cluster: &ClusterState) -> Result<Plan, Vec<Refusal>> {
    let mut refusals = Vec::new();
    let mut advisories = Vec::new();
    let mut steps = Vec::new();

    if req.environment == Tier::Production {
        advisories.push(Advisory::ChangeWindowRequired);
    }
    if req.startup_db_fanout {
        advisories.push(Advisory::StartupDbFanout { service: req.service.clone() });
    }
    if cluster.pending_cpu.0 > 0 || cluster.pending_mem.0 > 0 {
        advisories.push(Advisory::PendingPodsPresent { cpu: cluster.pending_cpu.render(), mem: cluster.pending_mem.render() });
    }
    let (running, hosting) = req
        .chart
        .pod_label
        .as_ref()
        .map_or((0, Vec::new()), |l| cluster.hosting(req.namespace.as_deref(), &l.key, &format!("{}{}", l.prefix, req.service)));
    if req.chart.pod_label.is_some() && hosting.is_empty() {
        advisories.push(Advisory::NoPodsObserved { service: req.service.clone() });
    }
    let replicas_now = req.observed.replicas.unwrap_or(running);
    let pools: Vec<PoolDecl> = if hosting.is_empty() {
        req.pools.clone()
    } else {
        hosting
            .iter()
            .map(|h| match req.pools.iter().find(|d| d.matches(h)) {
                Some(d) => PoolDecl { name: h.clone(), max_nodes: d.max_nodes, ceiling_in_module: d.ceiling_in_module },
                None => {
                    advisories.push(Advisory::UndeclaredPool { pool: h.clone() });
                    let n = u32::try_from(cluster.pool(h).len()).unwrap_or(u32::MAX);
                    PoolDecl { name: h.clone(), max_nodes: n, ceiling_in_module: true }
                }
            })
            .collect()
    };
    if replicas_now < 2 || current.hpa_min < 2 {
        refusals.push(Refusal::SingleReplica { replicas: replicas_now.min(current.hpa_min) });
    }

    let ladder = match sizings(current, req.raise, &req.policy) {
        Ok(l) => l,
        Err(r) => {
            refusals.push(r);
            return Err(refusals);
        }
    };

    let mut shadow = Shadow::new(cluster, &pools);
    if shadow.slots.is_empty() {
        refusals.push(Refusal::NoSchedulableNodes { pools: pools.iter().map(|p| p.name.clone()).collect() });
        return Err(refusals);
    }

    let mut before = current;
    let mut replicas = replicas_now;
    let mut index = 0u32;
    for after in ladder {
        index += 1;
        let (cpu, mem) = (after.cpu_request.0, after.mem_request.0);
        let (best_cpu, best_mem) = shadow.best_room();
        if cpu > best_cpu || mem > best_mem {
            refusals.push(Refusal::PodExceedsNodeShape {
                step: index,
                cpu: after.cpu_request.render(),
                mem: after.mem_request.render(),
                best_cpu: Millicores(best_cpu).render(),
                best_mem: Bytes(best_mem).render(),
            });
            break;
        }

        let replicas_after = hpa_desired(replicas, before, after, req);
        let surge = u32::try_from(scale(u64::from(replicas_after), req.policy.surge_pct)).unwrap_or(1).max(1);
        let extra = replicas_after.saturating_sub(replicas);
        let kept = u64::from(replicas.min(replicas_after));
        let peak = replicas.max(replicas_after) + surge;

        if let (Some(conns), Some(db)) = (req.observed.db_conns_per_pod, req.db) {
            let budget = u32::try_from(scale(db.max_connections.into(), req.policy.db_safety_pct)).unwrap_or(u32::MAX);
            if peak.saturating_mul(conns) > budget {
                refusals.push(Refusal::DbConnectionBudget { step: index, peak_pods: peak, conns_per_pod: conns, budget });
            }
        }

        let demand = Demand {
            pods: surge + extra,
            cpu,
            mem,
            steady_cpu: kept * cpu.saturating_sub(before.cpu_request.0),
            steady_mem: kept * mem.saturating_sub(before.mem_request.0),
        };
        let placeable_now = u32::try_from(shadow.clone_place_count(&demand)).unwrap_or(0);
        let added_before = shadow.added.clone();
        let at = if let Some((s, at)) = attempt(&shadow, &demand, None) {
            shadow = s;
            at
        } else {
            let lifted = pools.iter().position(|p| !p.ceiling_in_module).and_then(|r| attempt(&shadow, &demand, Some(r)).map(|x| (r, x)));
            let Some((r, (s, at))) = lifted else {
                refusals.push(Refusal::PoolCeilingInModule { step: index, pools: pools.iter().map(|p| p.name.clone()).collect() });
                break;
            };
            steps.push(Step::PoolCeiling { index, pool: pools[r].name.clone(), from: pools[r].max_nodes, to: s.counts[r] });
            index += 1;
            shadow = s;
            at
        };
        shadow.unplace(&at[..at.len().min(surge as usize)], cpu, mem);

        let mut nodes_to_add = 0;
        for (i, p) in pools.iter().enumerate() {
            let n = shadow.added[i] - added_before[i];
            if n > 0 {
                nodes_to_add += n;
                advisories.push(Advisory::NodeScaleUpExpected { step: index, pool: p.name.clone(), nodes: n });
            }
        }
        if replicas_after != replicas {
            advisories.push(Advisory::HpaReplicaShift { step: index, before: replicas, after: replicas_after });
        }
        steps.push(Step::PodSizing {
            index,
            edits: edits(&req.service, &req.chart.keys, before, after),
            before,
            after,
            prediction: Prediction {
                replicas_before: replicas,
                replicas_after,
                surge_pods: surge,
                peak_pods: peak,
                placeable_now,
                nodes_to_add,
            },
        });
        before = after;
        replicas = replicas_after;
    }

    if let Some(p95) = req.observed.mem_p95 {
        let needed = Bytes(scale(p95.0, req.policy.usage_headroom_pct)).round_up_mi();
        if before.mem_limit < needed {
            advisories.push(Advisory::LimitStillTight { limit: before.mem_limit.render(), needed: needed.render() });
        }
    }
    if let Raise::MaxReplicas(max) = req.raise {
        let (c, m) = (before.cpu_request.0, before.mem_request.0);
        let free: u64 = shadow.slots.iter().map(|s| Shadow::fits(c, m, s.free_cpu, s.free_mem)).sum();
        let headroom: u64 = req
            .pools
            .iter()
            .enumerate()
            .map(|(i, p)| u64::from(p.max_nodes.saturating_sub(shadow.counts[i])) * shadow.per_node(&p.name, c, m))
            .sum();
        let placeable = u32::try_from(u64::from(replicas) + free + headroom).unwrap_or(u32::MAX);
        if placeable < max {
            advisories.push(Advisory::HpaCeilingUnbacked { max_replicas: max, placeable });
        }
    }

    if refusals.is_empty() {
        Ok(Plan { tenant: req.tenant.clone(), environment: req.environment, service: req.service.clone(), steps, advisories })
    } else {
        Err(refusals)
    }
}

impl Shadow<'_> {
    fn clone_place_count(&self, d: &Demand) -> usize {
        self.slots.iter().map(|s| usize::try_from(Self::fits(d.cpu, d.mem, s.free_cpu, s.free_mem)).unwrap_or(usize::MAX)).fold(0usize, usize::saturating_add).min(d.pods as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DbBudget, Observed};
    use crate::observe::NodeState;

    const GI: u64 = 1024 * 1024 * 1024;

    fn node(pool: &str, free_cpu: u64, free_gi: u64) -> NodeState {
        NodeState {
            name: format!("{pool}-{free_cpu}-{free_gi}"),
            pool: pool.into(),
            schedulable: true,
            alloc_cpu: Millicores(7900),
            alloc_mem: Bytes(28 * GI),
            req_cpu: Millicores(7900 - free_cpu),
            req_mem: Bytes((28 - free_gi) * GI),
            daemon_cpu: Millicores(300),
            daemon_mem: Bytes(GI),
        }
    }

    fn cluster(nodes: Vec<NodeState>) -> ClusterState {
        ClusterState { nodes, ..ClusterState::default() }
    }

    fn sizing() -> Sizing {
        Sizing {
            cpu_request: Millicores(1000),
            mem_request: Bytes(2 * GI),
            mem_limit: Bytes(4 * GI),
            hpa_min: 2,
            hpa_max: 10,
            hpa_mem_pct: 50,
            hpa_cpu_pct: 70,
        }
    }

    fn request(raise: Raise, max_nodes: u32, in_module: bool) -> Request {
        Request {
            tenant: "tenant-a".into(),
            environment: Tier::Staging,
            service: "api".into(),
            namespace: None,
            startup_db_fanout: false,
            chart: crate::values::tests::chart(),
            raise,
            observed: Observed { replicas: Some(3), mem_p95: None, cpu_p95: None, db_conns_per_pod: None },
            pools: vec![PoolDecl { name: "pool-a".into(), max_nodes, ceiling_in_module: in_module }],
            db: None,
            policy: Policy::default(),
        }
    }

    fn pod_steps(p: &Plan) -> Vec<&Sizing> {
        p.steps.iter().filter_map(|s| if let Step::PodSizing { after, .. } = s { Some(after) } else { None }).collect()
    }

    #[test]
    fn memory_raise_climbs_a_bounded_ladder_and_keeps_the_limit_ratio() {
        let c = cluster(vec![node("pool-a", 6000, 20), node("pool-a", 6000, 20)]);
        let p = plan(&request(Raise::MemoryRequest(Bytes(6 * GI)), 10, false), sizing(), &c).unwrap();
        let s = pod_steps(&p);
        assert_eq!(s.iter().map(|z| z.mem_request.render()).collect::<Vec<_>>(), ["4Gi", "6Gi"]);
        assert_eq!(s.iter().map(|z| z.mem_limit.render()).collect::<Vec<_>>(), ["8Gi", "12Gi"]);
    }

    #[test]
    fn identical_inputs_give_an_identical_plan() {
        let c = cluster(vec![node("pool-a", 3000, 6), node("pool-a", 2000, 4)]);
        let r = request(Raise::CpuRequest(Millicores(3500)), 6, false);
        assert_eq!(plan(&r, sizing(), &c), plan(&r, sizing(), &c));
    }

    #[test]
    fn refuses_anything_that_is_not_a_raise() {
        let c = cluster(vec![node("pool-a", 6000, 20)]);
        let e = plan(&request(Raise::MemoryRequest(Bytes(GI)), 10, false), sizing(), &c).unwrap_err();
        assert!(matches!(e[0], Refusal::NotAnIncrease { .. }));
    }

    #[test]
    fn refuses_a_single_replica_service() {
        let c = cluster(vec![node("pool-a", 6000, 20)]);
        let mut r = request(Raise::CpuRequest(Millicores(1200)), 10, false);
        r.observed.replicas = Some(1);
        assert!(plan(&r, sizing(), &c).unwrap_err().iter().any(|x| matches!(x, Refusal::SingleReplica { .. })));
    }

    #[test]
    fn refuses_a_pod_larger_than_any_node() {
        let c = cluster(vec![node("pool-a", 6000, 20)]);
        let e = plan(&request(Raise::MemoryRequest(Bytes(40 * GI)), 10, false), sizing(), &c).unwrap_err();
        assert!(e.iter().any(|x| matches!(x, Refusal::PodExceedsNodeShape { .. })));
    }

    #[test]
    fn expects_autoscaler_nodes_within_the_pool_ceiling() {
        let c = cluster(vec![node("pool-a", 500, 1), node("pool-a", 500, 1)]);
        let p = plan(&request(Raise::CpuRequest(Millicores(1200)), 10, false), sizing(), &c).unwrap();
        assert!(p.advisories.iter().any(|a| matches!(a, Advisory::NodeScaleUpExpected { .. })));
        assert!(p.steps.iter().all(|s| matches!(s, Step::PodSizing { .. })));
    }

    #[test]
    fn a_full_pool_needs_a_ceiling_step_first_or_a_module_change() {
        let c = cluster(vec![node("pool-a", 500, 1), node("pool-a", 500, 1)]);
        let p = plan(&request(Raise::CpuRequest(Millicores(1200)), 2, false), sizing(), &c).unwrap();
        assert!(matches!(&p.steps[0], Step::PoolCeiling { from: 2, to, .. } if *to > 2));
        let e = plan(&request(Raise::CpuRequest(Millicores(1200)), 2, true), sizing(), &c).unwrap_err();
        assert!(e.iter().any(|x| matches!(x, Refusal::PoolCeilingInModule { .. })));
    }

    #[test]
    fn predicts_the_hpa_scaling_down_when_requests_grow_without_load() {
        let c = cluster(vec![node("pool-a", 7000, 20), node("pool-a", 7000, 20)]);
        let mut r = request(Raise::CpuRequest(Millicores(2000)), 10, false);
        r.observed.replicas = Some(6);
        r.observed.mem_p95 = Some(Bytes(GI / 2));
        let p = plan(&r, sizing(), &c).unwrap();
        assert!(p.advisories.contains(&Advisory::HpaReplicaShift { step: 1, before: 6, after: 3 }));
    }

    #[test]
    fn refuses_a_rollout_peak_over_the_connection_budget() {
        let c = cluster(vec![node("pool-a", 7000, 20), node("pool-a", 7000, 20)]);
        let mut r = request(Raise::MinReplicas(8), 10, false);
        r.observed.db_conns_per_pod = Some(15);
        r.db = Some(DbBudget { max_connections: 170 });
        assert!(plan(&r, sizing(), &c).unwrap_err().iter().any(|x| matches!(x, Refusal::DbConnectionBudget { .. })));
    }
}

#[cfg(test)]
mod observed_tests {
    use super::*;
    use crate::model::{Observed, PodLabel};
    use crate::observe::{NodeState, PodState};

    const GI: u64 = 1 << 30;

    fn node(name: &str, pool: &str) -> NodeState {
        NodeState {
            name: name.into(),
            pool: pool.into(),
            schedulable: true,
            alloc_cpu: Millicores(4000),
            alloc_mem: Bytes(16 * GI),
            ..NodeState::default()
        }
    }

    fn pod(app: &str, node: &str) -> PodState {
        PodState { namespace: "ns".into(), labels: [("app".to_string(), app.to_string())].into(), node: Some(node.into()), running: true }
    }

    #[test]
    fn replicas_and_pools_come_from_the_workload_pods_and_undeclared_pools_cannot_grow() {
        let cluster = ClusterState {
            nodes: vec![node("n1", "general-a"), node("n2", "arm-b"), node("n3", "other")],
            pods: vec![pod("svc-api", "n1"), pod("svc-api", "n2"), pod("svc-worker", "n3")],
            ..ClusterState::default()
        };
        let mut chart = crate::values::tests::chart();
        chart.pod_label = Some(PodLabel { key: "app".into(), prefix: "svc-".into() });
        let req = Request {
            tenant: "t".into(),
            environment: Tier::Staging,
            service: "api".into(),
            namespace: Some("ns".into()),
            startup_db_fanout: false,
            chart,
            raise: Raise::CpuRequest(Millicores(1200)),
            observed: Observed::default(),
            pools: vec![PoolDecl { name: "general-*".into(), max_nodes: 5, ceiling_in_module: false }],
            db: None,
            policy: Policy::default(),
        };
        let current = crate::values::tests::chart().defaults;
        let p = plan(&req, Sizing { cpu_request: Millicores(1000), ..current }, &cluster).unwrap();
        assert!(p.advisories.contains(&Advisory::UndeclaredPool { pool: "arm-b".into() }));
        let Step::PodSizing { prediction, .. } = &p.steps[0] else { panic!("pod step first") };
        assert_eq!(prediction.replicas_before, 2);
    }
}
