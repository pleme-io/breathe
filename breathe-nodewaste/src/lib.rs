//! `breathe-nodewaste` — is a NODE earning its cost?
//!
//! Every band dimension breathe has carves *within* a host: memory, cpu, arc,
//! cgroup, host-param. None of them can answer whether the host should exist at
//! all. On 2026-08-02 that gap was measured on camelot: five `m6a.2xlarge`
//! ON-DEMAND nodes in the builder pool sat at 19-42m CPU holding ARC runner pods
//! that had claimed no job — $1.73/hr, invisible to Karpenter's `WhenEmpty`
//! consolidation because the pods existed, and invisible to breathe because
//! breathe had no node-level opinion. Everything that eventually found them was
//! a human running a command; between runs, nothing watched.
//!
//! # Why the obvious answers are all wrong
//!
//! Each of these cost a wrong answer first, and each is a test below.
//!
//! **Pod phase is not work.** A `Running` pod is not a working pod. Of the seven
//! builder nodes, two were genuinely building at 61% and 92% CPU and five held
//! `Running` runners that had claimed nothing. Phase-only classification called
//! all seven busy.
//!
//! **Low CPU is not idle.** The first cut used CPU alone and flagged mysql, nats
//! and pangea-operator — databases idle at tens of milliCPU while holding
//! gigabytes resident. Nine nodes flagged, most of them false. A detector that
//! calls a database waste is switched off within a week, and then it protects
//! nothing.
//!
//! **State is never idle.** A node holding a `StatefulSet` pod is holding data,
//! however quiet it looks.
//!
//! **A reservation can be load-bearing.** pangea-operator reserves 2 cores and
//! uses 94m at rest, which reads as 21x waste. That reservation was added to end
//! an 84-restart probe-timeout loop, because it bursts to two cores while
//! planning. Usage-at-rest is not licence to shrink a request — so this crate
//! reports waste and deliberately exposes no API that prescribes a request
//! change. The distinction is the whole reason it is safe to act on.
//!
//! # What a verdict is for
//!
//! [`Verdict::Idle`] and [`Verdict::Stranded`] both mean "this node costs money
//! and returns nothing", but they differ in remedy, which is why they are not
//! one variant. A stranded node is held by pods that will never work again, so
//! it can be reaped as soon as those are cleaned up. An idle node holds live
//! pods that simply have nothing to do — reaping it is a scheduling decision,
//! not a cleanup.

#![forbid(unsafe_code)]

use std::time::Duration;

/// Below this, a node is not doing CPU work. An ARC runner that has claimed no
/// job sits in single-digit milliCPU; one executing a build sits in the
/// thousands. The gap is three orders of magnitude, so the threshold is not
/// delicate.
pub const IDLE_MILLICPU: u32 = 150;

/// Below this, nothing meaningful is resident. Paired with [`IDLE_MILLICPU`]
/// because CPU alone flags every database — see the crate docs.
pub const IDLE_MEM_PCT: u32 = 25;

/// How long a node may look idle before it counts. A runner legitimately idles
/// for seconds between claiming a pod and claiming a job; flagging that would
/// fire on every normal scale-up, which is how a detector becomes noise.
pub const IDLE_GRACE: Duration = Duration::from_secs(15 * 60);

/// What a node is doing, from the node's own point of view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Something is genuinely running.
    Working,
    /// No non-DaemonSet pods at all. Karpenter's `WhenEmpty` will reap it; this
    /// is NOT charged as waste, because counting a node that consolidation is
    /// already taking would double-book it.
    Empty,
    /// Live pods, no work, and no state. Costs full price; `WhenEmpty` cannot
    /// reap it because the pods exist.
    Idle,
    /// Held only by pods that are finished, failed or terminating. Invisible to
    /// `WhenEmpty` forever; only `expireAfter` will ever remove it.
    Stranded,
}

impl Verdict {
    /// Does this node cost money and return nothing?
    #[must_use]
    pub const fn is_waste(self) -> bool {
        matches!(self, Self::Idle | Self::Stranded)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Working => "working",
            Self::Empty => "empty",
            Self::Idle => "idle",
            Self::Stranded => "stranded",
        }
    }
}

/// One pod's contribution to whether its node is busy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodFact {
    /// A DaemonSet pod follows nodes; it never holds one. Karpenter ignores them
    /// when judging emptiness and so must we, or every node looks occupied.
    pub daemonset: bool,
    /// A `StatefulSet` pod means data lives here.
    pub stateful: bool,
    /// `Running`, `Pending`, `Succeeded`, `Failed`, `Unknown`.
    pub phase: Phase,
    /// A pod stuck Terminating still blocks `WhenEmpty` while doing no work.
    /// This is exactly how a wedged runner keeps a node alive for a month.
    pub terminating: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Running,
    Pending,
    Succeeded,
    Failed,
    Unknown,
}

impl PodFact {
    /// A pod counts as live only if it could still do something. Terminating
    /// excludes it regardless of phase.
    #[must_use]
    pub const fn is_live(&self) -> bool {
        if self.terminating {
            return false;
        }
        matches!(self.phase, Phase::Running | Phase::Pending | Phase::Unknown)
    }
}

/// What the node itself reports. `known` distinguishes "measured zero" from "not
/// measured": without it, every node reads as idle the moment metrics break,
/// which would turn a metrics outage into a fleet-wide reap signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NodeUsage {
    pub millicpu: u32,
    pub mem_pct: u32,
    pub known: bool,
}

/// Tunables, so a cluster with a different workload shape can move the lines
/// without forking the logic.
#[derive(Debug, Clone, Copy)]
pub struct Thresholds {
    pub idle_millicpu: u32,
    pub idle_mem_pct: u32,
    pub grace: Duration,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self { idle_millicpu: IDLE_MILLICPU, idle_mem_pct: IDLE_MEM_PCT, grace: IDLE_GRACE }
    }
}

/// The verdict plus enough context to act on it or argue with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeVerdict {
    pub verdict: Verdict,
    /// Non-DaemonSet pods holding this node.
    pub holders: usize,
    /// Of those, how many could still do work.
    pub live: usize,
    pub stateful: bool,
    pub usage: NodeUsage,
}

/// Classify one node.
///
/// `age` is how long the node has existed, which gates [`Verdict::Idle`] only.
#[must_use]
pub fn classify(pods: &[PodFact], usage: NodeUsage, age: Duration, t: Thresholds) -> NodeVerdict {
    let holders: Vec<&PodFact> = pods.iter().filter(|p| !p.daemonset).collect();
    let live = holders.iter().filter(|p| p.is_live()).count();
    let stateful = holders.iter().any(|p| p.stateful);

    // Three conditions, all necessary. Drop any one and this misfires: without
    // memory it flags databases, without the stateful check it flags a quiet
    // datastore, without `known` a metrics outage flags the whole fleet, and
    // without grace it fires on every scale-up.
    let looks_idle = usage.known
        && usage.millicpu < t.idle_millicpu
        && usage.mem_pct < t.idle_mem_pct
        && !stateful
        && age > t.grace;

    let verdict = if holders.is_empty() {
        Verdict::Empty
    } else if live == 0 {
        Verdict::Stranded
    } else if looks_idle {
        Verdict::Idle
    } else {
        Verdict::Working
    };

    NodeVerdict { verdict, holders: holders.len(), live, stateful, usage }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: Duration = Duration::from_secs(3600);

    fn pod(phase: Phase) -> PodFact {
        PodFact { daemonset: false, stateful: false, phase, terminating: false }
    }
    fn busy() -> NodeUsage {
        NodeUsage { millicpu: 7317, mem_pct: 14, known: true }
    }
    fn quiet() -> NodeUsage {
        NodeUsage { millicpu: 21, mem_pct: 4, known: true }
    }

    #[test]
    fn a_running_but_jobless_runner_is_idle_not_working() {
        // The measured shape: five m6a.2xlarge on-demand at 19-42m CPU, each
        // holding a Running ARC runner that had claimed no job.
        let v = classify(&[pod(Phase::Running)], quiet(), 3 * HOUR, Thresholds::default());
        assert_eq!(
            v.verdict,
            Verdict::Idle,
            "pod phase is not evidence of work; treating it as such is what let five \
             on-demand nodes idle unnoticed"
        );
        assert!(v.verdict.is_waste());
    }

    #[test]
    fn a_busy_runner_is_working() {
        let v = classify(&[pod(Phase::Running)], busy(), 3 * HOUR, Thresholds::default());
        assert_eq!(v.verdict, Verdict::Working, "7317m CPU is a build in flight");
    }

    #[test]
    fn a_database_at_low_cpu_is_not_idle() {
        // CPU alone flagged mysql, nats and pangea-operator on real camelot data.
        let mut p = pod(Phase::Running);
        p.stateful = true;
        let usage = NodeUsage { millicpu: 38, mem_pct: 40, known: true };
        let v = classify(&[p], usage, 30 * HOUR, Thresholds::default());
        assert_ne!(
            v.verdict,
            Verdict::Idle,
            "a database idles at tens of milliCPU while holding gigabytes resident; \
             flagging that is the false positive that gets a detector switched off"
        );
    }

    #[test]
    fn high_memory_alone_keeps_a_node_off_the_idle_list() {
        let usage = NodeUsage { millicpu: 31, mem_pct: 70, known: true };
        let v = classify(&[pod(Phase::Running)], usage, 30 * HOUR, Thresholds::default());
        assert_eq!(v.verdict, Verdict::Working, "70% resident is work even at 31m CPU");
    }

    #[test]
    fn daemonset_pods_do_not_hold_a_node() {
        let ds = PodFact { daemonset: true, stateful: false, phase: Phase::Running, terminating: false };
        let v = classify(&[ds.clone(), ds], quiet(), 4 * HOUR, Thresholds::default());
        assert_eq!(
            v.verdict,
            Verdict::Empty,
            "Karpenter ignores DaemonSets when judging emptiness, so a node carrying only \
             them is empty and consolidation will take it"
        );
        assert!(!v.verdict.is_waste(), "an empty node is already leaving; charging it double-books");
    }

    #[test]
    fn a_node_held_only_by_finished_pods_is_stranded() {
        let v = classify(
            &[pod(Phase::Succeeded), pod(Phase::Failed)],
            quiet(),
            30 * HOUR,
            Thresholds::default(),
        );
        assert_eq!(
            v.verdict,
            Verdict::Stranded,
            "WhenEmpty will never reap this because pods exist, and only expireAfter can — \
             which on the builder pools was 720h until it was bounded to 48h"
        );
    }

    #[test]
    fn a_pod_stuck_terminating_does_no_work() {
        let mut p = pod(Phase::Running);
        p.terminating = true;
        let v = classify(&[p], quiet(), 12 * HOUR, Thresholds::default());
        assert_eq!(
            v.verdict,
            Verdict::Stranded,
            "a wedged runner blocks WhenEmpty while doing nothing; that is how a node \
             survives a month"
        );
    }

    #[test]
    fn a_freshly_started_idle_node_gets_grace() {
        let v = classify(&[pod(Phase::Running)], quiet(), Duration::from_secs(60), Thresholds::default());
        assert_eq!(
            v.verdict,
            Verdict::Working,
            "a runner idles for seconds between claiming a pod and claiming a job; flagging \
             that fires on every scale-up"
        );
    }

    #[test]
    fn unmeasured_is_not_idle() {
        let v = classify(&[pod(Phase::Running)], NodeUsage::default(), 30 * HOUR, Thresholds::default());
        assert_eq!(
            v.verdict,
            Verdict::Working,
            "without metrics the classifier must not read a zero as idle, or a metrics \
             outage becomes a fleet-wide reap signal"
        );
    }
}
