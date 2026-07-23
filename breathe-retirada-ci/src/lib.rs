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
//! **Corrected 2026-07-23, against real evidence, not assumption.** The
//! first version of this crate modeled ONLY [`PodExitCause`] — a runner
//! pod that started, then got evicted mid-job. Two real, independently
//! diagnosed incidents the same day (`pleme-io/breathe` run
//! `29930406877`, `pleme-io/hardened-images` run `30007679929`'s
//! `verify-aarch64-build` job) turned out to be a DIFFERENT failure
//! mode entirely: the GitHub Jobs API showed `runner_id: 0` /
//! `runner_name: ""` for the job's ENTIRE lifetime — no runner was ever
//! assigned, not once, until GitHub's own 24h queue-timeout
//! auto-cancelled it. Neither incident was a mid-job spot reclaim; both
//! were pure capacity starvation (the amd64 pool couldn't keep up for
//! 24h straight; the org's only arm64 runner was offline). See
//! [`QueuedJobObservation`]/[`classify_queue_starvation`] — the
//! confirmed-real failure mode this crate has actually observed in
//! production, needing only the GH Jobs API (no k8s RBAC at all,
//! unlike the pod-eviction path below). [`PodExitCause`] remains
//! modeled (a real mid-job spot reclaim is still a real possibility)
//! but is UNCONFIRMED against any actual incident so far — don't round
//! it up to "the" failure mode; it's "a" failure mode.
//!
//! **Tier-honest (2026-07-23): the typed decision cores are real and
//! tested against mocks for both failure modes.** The pod-eviction path
//! ([`ClusterObserver`]) has no production implementation — a real one
//! needs RBAC to watch pods in the ARC runner namespace, a live-cluster
//! decision needing explicit operator go-ahead. The queue-starvation
//! path ([`JobQueueObserver`]) needs only the GitHub Jobs API + a GH
//! token with `actions:write` — the SAME credential already used
//! throughout this repo's own CI — so it is realistically deployable as
//! a plain scheduled GitHub Actions workflow, no cluster access
//! whatsoever. Still not deployed as of this commit — [`WorkflowDispatcher`]
//! has no production implementation either — but the deployment blocker
//! for THIS path is materially smaller than the pod-eviction path's.

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
    /// A runner HAS been assigned to this job — the queue-starvation
    /// classifier only ever fires before assignment; once a runner
    /// claims the job, its outcome is a different question entirely
    /// (the pod-eviction classifier's job, not this one's).
    RunnerAlreadyAssigned,
    /// The job hasn't been queued long enough yet to call it
    /// starvation rather than a normal cold-start Wake — never fire
    /// early; `theory/BREATHABILITY.md`'s lifecycle-breath names
    /// cold-start latency as expected, not an anomaly.
    StillWithinNormalWake,
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

// ---------------------------------------------------------------------
// Queue starvation — the CONFIRMED-REAL failure mode (see module docs).
// ---------------------------------------------------------------------

/// The typed facts about one queued job — everything
/// [`classify_queue_starvation`] needs, sourced entirely from the
/// GitHub Jobs API (`GET /repos/{owner}/{repo}/actions/jobs/{job_id}`):
/// `runner_id == 0` / `runner_name == ""` for a job's whole lifetime is
/// the exact, load-bearing signal that no runner was ever assigned.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedJobObservation {
    pub job_id: u64,
    pub job_name: String,
    pub repo: String,
    pub workflow_file: String,
    pub head_sha: String,
    /// `true` iff the GH Jobs API has ever reported a non-zero
    /// `runner_id` for this job — the fact this whole classifier turns
    /// on.
    pub runner_ever_assigned: bool,
    /// How long the job has been sitting since its `created_at`.
    pub queued_for: std::time::Duration,
    pub prior_auto_retries: u32,
}

/// How long is "too long" before a queued-with-no-runner job is real
/// starvation rather than a normal cold-start Wake. Separate from
/// [`RetiradaCiPolicy`]'s retry ceiling (shared) — this threshold has
/// nothing to do with pod eviction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStarvationPolicy {
    pub starvation_threshold: std::time::Duration,
}

impl Default for QueueStarvationPolicy {
    fn default() -> Self {
        Self {
            // 10 minutes: comfortably above a spot-node Wake cold-start
            // (single-digit minutes, observed 2026-07-23), and far
            // below GitHub's own 24h auto-cancel — catches the real
            // starvation class early instead of waiting a full day for
            // GitHub itself to give up.
            starvation_threshold: std::time::Duration::from_secs(600),
        }
    }
}

/// The pure decision for the queue-starvation path — the sibling of
/// [`classify`] for the pod-eviction path. No I/O.
#[must_use]
pub fn classify_queue_starvation(
    obs: &QueuedJobObservation,
    starvation_policy: &QueueStarvationPolicy,
    retry_policy: &RetiradaCiPolicy,
) -> RemediationDecision {
    if obs.runner_ever_assigned {
        return RemediationDecision::NoAction {
            reason: NoActionReason::RunnerAlreadyAssigned,
        };
    }
    if obs.queued_for < starvation_policy.starvation_threshold {
        return RemediationDecision::NoAction {
            reason: NoActionReason::StillWithinNormalWake,
        };
    }
    if obs.prior_auto_retries >= retry_policy.max_auto_retries {
        return RemediationDecision::NoAction {
            reason: NoActionReason::RetryCeilingReached,
        };
    }
    RemediationDecision::RedispatchOnDemand {
        repo: obs.repo.clone(),
        workflow_file: obs.workflow_file.clone(),
        head_sha: obs.head_sha.clone(),
        runner_override: retry_policy.on_demand_runner_label.clone(),
    }
}

/// Injectable seam over the GitHub side — reads a queued job's current
/// state. Needs ONLY a GH token with read access to Actions runs; no
/// k8s access whatsoever, unlike [`ClusterObserver`]. A real impl wraps
/// `GET /repos/{owner}/{repo}/actions/jobs/{job_id}`; tests use a mock.
#[async_trait::async_trait]
pub trait JobQueueObserver {
    async fn observe_queued_job(
        &self,
        repo: &str,
        run_id: u64,
        job_id: u64,
    ) -> anyhow::Result<QueuedJobObservation>;
}

/// The whole queue-starvation loop: observe, classify, act — only for
/// `RedispatchOnDemand`. Sibling of [`observe_classify_and_remediate`]
/// for the pod-eviction path.
///
/// # Errors
///
/// Propagates [`JobQueueObserver::observe_queued_job`] or
/// [`WorkflowDispatcher::redispatch`]'s error untouched.
pub async fn observe_queue_and_remediate(
    observer: &dyn JobQueueObserver,
    dispatcher: &dyn WorkflowDispatcher,
    starvation_policy: &QueueStarvationPolicy,
    retry_policy: &RetiradaCiPolicy,
    repo: &str,
    run_id: u64,
    job_id: u64,
) -> anyhow::Result<RemediationDecision> {
    let obs = observer.observe_queued_job(repo, run_id, job_id).await?;
    let decision = classify_queue_starvation(&obs, starvation_policy, retry_policy);
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

    // ---- Queue starvation — grounded against the two real incidents ----

    use std::time::Duration;

    fn queue_obs(runner_ever_assigned: bool, queued_for: Duration) -> QueuedJobObservation {
        QueuedJobObservation {
            job_id: 88958322865,
            job_name: "breathe-api-server image -> ghcr.io / Build & push image".into(),
            repo: "pleme-io/breathe".into(),
            workflow_file: "image.yml".into(),
            head_sha: "8355d03fa452970a9f1341fe37fd7242cb986311".into(),
            runner_ever_assigned,
            queued_for,
            prior_auto_retries: 0,
        }
    }

    #[test]
    fn matches_the_real_breathe_incident_29930406877() {
        // The real, observed shape: runner_id == 0 for the job's ENTIRE
        // 24h lifetime (this test uses 25h, past GitHub's own 24h
        // auto-cancel, matching the actual observed completed_at -
        // started_at delta). Must classify as a real starvation
        // redispatch, not a normal Wake.
        let obs = queue_obs(false, Duration::from_secs(25 * 3600));
        let decision =
            classify_queue_starvation(&obs, &QueueStarvationPolicy::default(), &RetiradaCiPolicy::default());
        assert_eq!(
            decision,
            RemediationDecision::RedispatchOnDemand {
                repo: "pleme-io/breathe".into(),
                workflow_file: "image.yml".into(),
                head_sha: "8355d03fa452970a9f1341fe37fd7242cb986311".into(),
                runner_override: "ubuntu-24.04".into(),
            },
            "this classifier would have caught the real breathe incident directly, \
             instead of it silently riding out a full 24h to GitHub's own timeout"
        );
    }

    #[test]
    fn matches_the_real_hardened_images_arm64_incident_30007679929() {
        // verify-aarch64-build: created_at == started_at, cancelled
        // ~3h12m later, zero runner ever assigned (the org's only
        // arm64 runner was offline). Same classification shape,
        // different repo/workflow.
        let obs = QueuedJobObservation {
            job_id: 89_207_579_000, // approximate -- the real id wasn't captured for this exact job
            job_name: "verify-aarch64-build".into(),
            repo: "pleme-io/hardened-images".into(),
            workflow_file: "image-release.yml".into(),
            head_sha: "unknown-in-this-fixture".into(),
            runner_ever_assigned: false,
            queued_for: Duration::from_secs(3 * 3600 + 12 * 60),
            prior_auto_retries: 0,
        };
        let decision =
            classify_queue_starvation(&obs, &QueueStarvationPolicy::default(), &RetiradaCiPolicy::default());
        assert!(matches!(decision, RemediationDecision::RedispatchOnDemand { .. }));
    }

    #[test]
    fn a_freshly_queued_job_is_not_starvation_yet() {
        // Cold-start Wake latency (observed this session: single-digit
        // minutes) must NEVER trigger a redispatch -- only real,
        // sustained starvation past the threshold does.
        let obs = queue_obs(false, Duration::from_secs(90));
        let decision =
            classify_queue_starvation(&obs, &QueueStarvationPolicy::default(), &RetiradaCiPolicy::default());
        assert_eq!(
            decision,
            RemediationDecision::NoAction {
                reason: NoActionReason::StillWithinNormalWake
            }
        );
    }

    #[test]
    fn a_job_that_got_a_runner_is_never_starvation_regardless_of_queue_time() {
        // Once a runner claims the job, whatever happens next is the
        // pod-eviction classifier's question, never this one's -- even
        // if it somehow queued a long time before being claimed.
        let obs = queue_obs(true, Duration::from_secs(2 * 3600));
        let decision =
            classify_queue_starvation(&obs, &QueueStarvationPolicy::default(), &RetiradaCiPolicy::default());
        assert_eq!(
            decision,
            RemediationDecision::NoAction {
                reason: NoActionReason::RunnerAlreadyAssigned
            }
        );
    }

    #[test]
    fn queue_starvation_respects_the_same_retry_ceiling() {
        let mut obs = queue_obs(false, Duration::from_secs(25 * 3600));
        obs.prior_auto_retries = 1;
        let policy = RetiradaCiPolicy {
            max_auto_retries: 1,
            ..RetiradaCiPolicy::default()
        };
        let decision = classify_queue_starvation(&obs, &QueueStarvationPolicy::default(), &policy);
        assert_eq!(
            decision,
            RemediationDecision::NoAction {
                reason: NoActionReason::RetryCeilingReached
            }
        );
    }

    struct MockQueueObserver(QueuedJobObservation);
    #[async_trait::async_trait]
    impl JobQueueObserver for MockQueueObserver {
        async fn observe_queued_job(
            &self,
            _repo: &str,
            _run_id: u64,
            _job_id: u64,
        ) -> anyhow::Result<QueuedJobObservation> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn queue_orchestration_dispatches_only_past_the_starvation_threshold() {
        let observer = MockQueueObserver(queue_obs(false, Duration::from_secs(25 * 3600)));
        let dispatcher = MockDispatcher::default();

        let decision = observe_queue_and_remediate(
            &observer,
            &dispatcher,
            &QueueStarvationPolicy::default(),
            &RetiradaCiPolicy::default(),
            "pleme-io/breathe",
            29_930_406_877,
            88_958_322_865,
        )
        .await
        .unwrap();

        assert!(matches!(
            decision,
            RemediationDecision::RedispatchOnDemand { .. }
        ));
        assert_eq!(dispatcher.calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn queue_orchestration_never_dispatches_during_normal_wake() {
        let observer = MockQueueObserver(queue_obs(false, Duration::from_secs(90)));
        let dispatcher = MockDispatcher::default();

        let decision = observe_queue_and_remediate(
            &observer,
            &dispatcher,
            &QueueStarvationPolicy::default(),
            &RetiradaCiPolicy::default(),
            "pleme-io/breathe",
            29_930_406_877,
            88_958_322_865,
        )
        .await
        .unwrap();

        assert_eq!(
            decision,
            RemediationDecision::NoAction {
                reason: NoActionReason::StillWithinNormalWake
            }
        );
        assert!(dispatcher.calls.lock().unwrap().is_empty());
    }
}
