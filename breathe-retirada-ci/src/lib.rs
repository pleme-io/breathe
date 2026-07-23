//! `breathe-retirada-ci` — retirada's spot-reclaim signal, applied to
//! job-shaped (CI) workloads.
//!
//! `theory/BREATHABILITY.md`'s `retirada` names the drain-ahead signal
//! for REPLICA-shaped workloads: `breathe-control::replica` scales a
//! replica set OUT ahead of a pending spot reclaim, covering the
//! doomed replicas before they're lost (§ `spot_reclaim_pending` in
//! `breathe-control/src/{replica,lifecycle}.rs`). A CI job has no
//! floor to scale out ahead of — it's one attempt, not a replica set —
//! so the job-shaped sibling of retirada is necessarily reactive, not
//! preemptive: classify why the job's runner pod disappeared, and for
//! exactly the legitimate-reclaim case, retry that ONE job on the
//! existing on-demand runner override.
//!
//! A GH Actions job that fails because its runner pod's node was
//! legitimately reclaimed by the auction ether (breathe's
//! predict→optimize→auction decision layer, `breathe-auction`
//! nee `breathe-spread`) is a distinct, typed failure class from a
//! genuine break in the job's own steps — this crate classifies which
//! is which and, for the legitimate-reclaim class ONLY, decides the
//! one remediation: redispatch that job, escalated onto the
//! already-existing on-demand runner tier (e.g. `pleme-io/breathe`'s
//! own `image.yml` `workflow_dispatch.inputs.runnerOverride`,
//! `ubuntu-24.04` — never a fresh spot pool for the retry, which would
//! just re-enter the same reclaim dynamics that caused this).
//!
//! **Tier-honest (2026-07-23): this is the typed decision core only.**
//! `classify` and `observe_classify_and_remediate` are real and tested
//! against mocks. Nothing here is deployed as a live watcher against a
//! real cluster — [`ClusterObserver`]/[`WorkflowDispatcher`] have no
//! production implementation yet (a real one needs RBAC to watch pods
//! in the ARC runner namespace plus a GH PAT with `actions:write`,
//! both live-cluster-mutating decisions that need an explicit
//! operator go-ahead before deploying, per the org's own
//! confirm-before-shared-system-mutation discipline).

use serde::{Deserialize, Serialize};

/// Why a runner pod's process disappeared mid-job — the fact this
/// whole crate exists to classify accurately, because only ONE of
/// these arms should ever trigger an automatic retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PodExitCause {
    /// The runner pod's own process exited (its steps ran to
    /// completion or failure) — whatever GH reports for the job
    /// conclusion IS the real outcome. Never retry automatically.
    ProcessExited,
    /// The pod was evicted/deleted WITHOUT its process exiting on its
    /// own, and the node it ran on was independently marked for spot
    /// reclaim (a Karpenter `Disrupted`/`Drifted` taint, or an AWS
    /// spot-interruption termination notice) in the same window — the
    /// auction ether's legitimate callback; retirada's signature for
    /// job-shaped workloads.
    LegitimateSpotReclaim,
    /// The pod was evicted/deleted for some other reason (manual
    /// delete, OOM-kill, a k8s-level admission/resource issue) — real,
    /// but NOT the auction ether. Never auto-retry: a human (or a
    /// different typed remediation arm) needs to look.
    OtherEviction,
}

/// The typed facts one job's runner pod produced, gathered from k8s
/// pod/node events — the input [`classify`] decides on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerPodOutcome {
    pub cause: PodExitCause,
    /// The GH Actions workflow file this job's run belongs to
    /// (`.github/workflows/<this>.yml`), needed to redispatch.
    pub workflow_file: String,
    /// The exact ref/sha the failed run built — the redispatch MUST
    /// target the same commit, never `HEAD` at retry time (a `main`
    /// that moved between the failure and the retry must not silently
    /// widen what gets shipped as "the retry of run N").
    pub head_sha: String,
    /// `"owner/name"`.
    pub repo: String,
    /// How many times THIS job has already been auto-retried by this
    /// mechanism — the fail-safe bound (never an infinite retry loop
    /// if a node pool is genuinely wedged and reclaiming everything
    /// it's handed, spot or on-demand).
    pub prior_auto_retries: u32,
}

/// The one remediation this crate ever decides — restricted on
/// purpose to a closed sum, so a caller can never invent a third,
/// unreviewed action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemediationDecision {
    /// Redispatch `workflow_file` at `head_sha` (on `repo`), with the
    /// runner escalated to `runner_override` — the existing GH-hosted
    /// on-demand fallback, never a fresh spot pool.
    RedispatchOnDemand {
        repo: String,
        workflow_file: String,
        head_sha: String,
        runner_override: String,
    },
    /// Do nothing automatically — a named reason for observability /
    /// the reactive-nervous-system's anomaly surface.
    NoAction { reason: NoActionReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NoActionReason {
    /// `cause` was `ProcessExited` — the job's own outcome is real.
    NotAnEviction,
    /// `cause` was `OtherEviction` — a real eviction, but not the
    /// auction ether's doing; needs a human or a different arm.
    NonAuctionEviction,
    /// `prior_auto_retries` already at or past the configured
    /// ceiling — the fail-safe bound; escalate to a human instead of
    /// looping.
    RetryCeilingReached,
}

/// The configured escalation target + retry ceiling — typed config,
/// never a hardcoded string. This is exactly the "auction ether" →
/// "which on-demand label" mapping the org's tag/query/control
/// discipline (`theory/CAMELOT-RUNNER-CONTROL.md`) requires be a
/// single typed source, not a scattered literal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetiradaCiPolicy {
    pub on_demand_runner_label: String,
    pub max_auto_retries: u32,
}

impl Default for RetiradaCiPolicy {
    fn default() -> Self {
        Self {
            on_demand_runner_label: "ubuntu-24.04".to_owned(),
            max_auto_retries: 1,
        }
    }
}

/// The pure decision — classify `outcome` against `policy` into
/// exactly one [`RemediationDecision`]. No I/O; every side effect
/// (observing the pod/node, dispatching the retry) is the caller's
/// job via [`ClusterObserver`]/[`WorkflowDispatcher`] below.
#[must_use]
pub fn classify(outcome: &RunnerPodOutcome, policy: &RetiradaCiPolicy) -> RemediationDecision {
    match outcome.cause {
        PodExitCause::ProcessExited => RemediationDecision::NoAction {
            reason: NoActionReason::NotAnEviction,
        },
        PodExitCause::OtherEviction => RemediationDecision::NoAction {
            reason: NoActionReason::NonAuctionEviction,
        },
        PodExitCause::LegitimateSpotReclaim => {
            if outcome.prior_auto_retries >= policy.max_auto_retries {
                RemediationDecision::NoAction {
                    reason: NoActionReason::RetryCeilingReached,
                }
            } else {
                RemediationDecision::RedispatchOnDemand {
                    repo: outcome.repo.clone(),
                    workflow_file: outcome.workflow_file.clone(),
                    head_sha: outcome.head_sha.clone(),
                    runner_override: policy.on_demand_runner_label.clone(),
                }
            }
        }
    }
}

/// Injectable seam over the k8s side: given a runner pod, determine
/// why its process disappeared. A real impl wraps the k8s API (pod
/// events + the owning Node's conditions/taints); tests use a mock.
#[async_trait::async_trait]
pub trait ClusterObserver {
    async fn observe_runner_pod(
        &self,
        namespace: &str,
        pod_name: &str,
    ) -> anyhow::Result<RunnerPodOutcome>;
}

/// Injectable seam over the GH side: apply a [`RemediationDecision`].
/// A real impl wraps `gh workflow run` / the GitHub API; tests use a
/// mock that records what would have been dispatched.
#[async_trait::async_trait]
pub trait WorkflowDispatcher {
    async fn redispatch(
        &self,
        repo: &str,
        workflow_file: &str,
        head_sha: &str,
        runner_override: &str,
    ) -> anyhow::Result<()>;
}

/// The whole loop: observe, classify, act — only for
/// `RedispatchOnDemand`. Returns the decision made either way, so a
/// caller can log/attest it regardless of whether it acted.
///
/// # Errors
///
/// Propagates [`ClusterObserver::observe_runner_pod`] or
/// [`WorkflowDispatcher::redispatch`]'s error untouched.
pub async fn observe_classify_and_remediate(
    observer: &dyn ClusterObserver,
    dispatcher: &dyn WorkflowDispatcher,
    policy: &RetiradaCiPolicy,
    namespace: &str,
    pod_name: &str,
) -> anyhow::Result<RemediationDecision> {
    let outcome = observer.observe_runner_pod(namespace, pod_name).await?;
    let decision = classify(&outcome, policy);
    if let RemediationDecision::RedispatchOnDemand {
        repo,
        workflow_file,
        head_sha,
        runner_override,
    } = &decision
    {
        dispatcher
            .redispatch(repo, workflow_file, head_sha, runner_override)
            .await?;
    }
    Ok(decision)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn outcome(cause: PodExitCause, prior_auto_retries: u32) -> RunnerPodOutcome {
        RunnerPodOutcome {
            cause,
            workflow_file: "image.yml".into(),
            head_sha: "deadbeef".into(),
            repo: "pleme-io/breathe".into(),
            prior_auto_retries,
        }
    }

    #[test]
    fn process_exited_never_retries() {
        let d = classify(
            &outcome(PodExitCause::ProcessExited, 0),
            &RetiradaCiPolicy::default(),
        );
        assert_eq!(
            d,
            RemediationDecision::NoAction {
                reason: NoActionReason::NotAnEviction
            }
        );
    }

    #[test]
    fn other_eviction_never_retries() {
        let d = classify(
            &outcome(PodExitCause::OtherEviction, 0),
            &RetiradaCiPolicy::default(),
        );
        assert_eq!(
            d,
            RemediationDecision::NoAction {
                reason: NoActionReason::NonAuctionEviction
            }
        );
    }

    #[test]
    fn legitimate_spot_reclaim_dispatches_on_demand_retry() {
        let policy = RetiradaCiPolicy::default();
        let d = classify(&outcome(PodExitCause::LegitimateSpotReclaim, 0), &policy);
        assert_eq!(
            d,
            RemediationDecision::RedispatchOnDemand {
                repo: "pleme-io/breathe".into(),
                workflow_file: "image.yml".into(),
                head_sha: "deadbeef".into(),
                runner_override: policy.on_demand_runner_label.clone(),
            }
        );
    }

    #[test]
    fn retry_ceiling_reached_stops_auto_retry() {
        let policy = RetiradaCiPolicy {
            max_auto_retries: 1,
            ..RetiradaCiPolicy::default()
        };
        // prior_auto_retries == max_auto_retries -> ceiling reached, no more auto-retry.
        let d = classify(&outcome(PodExitCause::LegitimateSpotReclaim, 1), &policy);
        assert_eq!(
            d,
            RemediationDecision::NoAction {
                reason: NoActionReason::RetryCeilingReached
            }
        );
    }

    #[test]
    fn first_retry_is_allowed_when_ceiling_is_one() {
        let policy = RetiradaCiPolicy {
            max_auto_retries: 1,
            ..RetiradaCiPolicy::default()
        };
        let d = classify(&outcome(PodExitCause::LegitimateSpotReclaim, 0), &policy);
        assert!(matches!(d, RemediationDecision::RedispatchOnDemand { .. }));
    }

    struct MockObserver(RunnerPodOutcome);
    #[async_trait::async_trait]
    impl ClusterObserver for MockObserver {
        async fn observe_runner_pod(
            &self,
            _namespace: &str,
            _pod_name: &str,
        ) -> anyhow::Result<RunnerPodOutcome> {
            Ok(self.0.clone())
        }
    }

    #[derive(Default)]
    struct MockDispatcher {
        calls: Mutex<Vec<(String, String, String, String)>>,
    }
    #[async_trait::async_trait]
    impl WorkflowDispatcher for MockDispatcher {
        async fn redispatch(
            &self,
            repo: &str,
            workflow_file: &str,
            head_sha: &str,
            runner_override: &str,
        ) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push((
                repo.to_owned(),
                workflow_file.to_owned(),
                head_sha.to_owned(),
                runner_override.to_owned(),
            ));
            Ok(())
        }
    }

    #[tokio::test]
    async fn orchestration_dispatches_only_on_legitimate_reclaim() {
        let observer = MockObserver(outcome(PodExitCause::LegitimateSpotReclaim, 0));
        let dispatcher = MockDispatcher::default();
        let policy = RetiradaCiPolicy::default();

        let decision = observe_classify_and_remediate(
            &observer,
            &dispatcher,
            &policy,
            "arc-runners",
            "camelot-builder-pleme-abc123",
        )
        .await
        .unwrap();

        assert!(matches!(
            decision,
            RemediationDecision::RedispatchOnDemand { .. }
        ));
        let calls = dispatcher.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "dispatcher must be called exactly once");
        assert_eq!(
            calls[0],
            (
                "pleme-io/breathe".into(),
                "image.yml".into(),
                "deadbeef".into(),
                "ubuntu-24.04".into(),
            )
        );
    }

    #[tokio::test]
    async fn orchestration_never_dispatches_for_a_real_job_failure() {
        let observer = MockObserver(outcome(PodExitCause::ProcessExited, 0));
        let dispatcher = MockDispatcher::default();
        let policy = RetiradaCiPolicy::default();

        let decision = observe_classify_and_remediate(
            &observer,
            &dispatcher,
            &policy,
            "arc-runners",
            "camelot-builder-pleme-abc123",
        )
        .await
        .unwrap();

        assert_eq!(
            decision,
            RemediationDecision::NoAction {
                reason: NoActionReason::NotAnEviction
            }
        );
        assert!(
            dispatcher.calls.lock().unwrap().is_empty(),
            "a real job failure must NEVER trigger a dispatch"
        );
    }

    #[tokio::test]
    async fn orchestration_never_dispatches_past_the_retry_ceiling() {
        let observer = MockObserver(outcome(PodExitCause::LegitimateSpotReclaim, 1));
        let dispatcher = MockDispatcher::default();
        let policy = RetiradaCiPolicy {
            max_auto_retries: 1,
            ..RetiradaCiPolicy::default()
        };

        let decision = observe_classify_and_remediate(
            &observer,
            &dispatcher,
            &policy,
            "arc-runners",
            "camelot-builder-pleme-abc123",
        )
        .await
        .unwrap();

        assert_eq!(
            decision,
            RemediationDecision::NoAction {
                reason: NoActionReason::RetryCeilingReached
            }
        );
        assert!(dispatcher.calls.lock().unwrap().is_empty());
    }
}
