//! `retirada-ci-watch` — the real, network-touching CLI wrapping
//! `breathe-retirada-ci`'s typed decision core against the real
//! GitHub Jobs API. Needs ONLY a GH token with `actions:write` on the
//! target repo — no k8s access, no RBAC, no cluster credentials.
//!
//! Meant to run as a scheduled step in the SAME repo it watches (using
//! that repo's own default `GITHUB_TOKEN`, least-privilege — never a
//! broad cross-repo PAT): each repo that wants this protection adds its
//! own thin scheduled-workflow shim that builds + runs this binary
//! against itself. See `.github/workflows/retirada-ci-watch.yml` in
//! this repo for the reference wiring.
//!
//! **Faster than GitHub's `schedule:` floor, honestly.** GitHub's own
//! `schedule` trigger has a real, documented floor of 5 minutes —
//! sub-minute cron syntax is accepted but not reliably honored (GitHub
//! silently coalesces/drops runs under load). `--loop-for-secs` works
//! around that floor the correct way: ONE scheduled invocation loops
//! internally (`--poll-interval-secs` between passes) for up to
//! `--loop-for-secs`, so the *effective* detection cadence is bounded
//! by the poll interval, not by GitHub's scheduler — the 5-minute
//! `schedule:` trigger becomes a keep-alive/safety-net that starts a
//! fresh loop shortly after the previous one's `--loop-for-secs` window
//! ends, never GitHub's own cron floor.
//!
//! **The algorithmically-adjusting knob.** By default the starvation
//! threshold is NOT the fixed `--starvation-threshold-secs` value —
//! it's [`breathe_retirada_ci::compute_adaptive_starvation_threshold`]
//! over real observed recent Wake latency (`--starvation-threshold-secs`
//! becomes the floor). Statistics, never ML, per the org's own
//! autorevivy doctrine. Disable with `--disable-adaptive-threshold` to
//! fall back to the fixed value.
//!
//! One pass = one round of: list queued jobs on `--workflow` matching
//! `--label-prefix`, classify each via [`breathe_retirada_ci::classify_queue_starvation`],
//! and for a confirmed, not-already-retried starvation, redispatch via
//! `workflow_dispatch` with `runnerOverride` set. Exits `0` regardless
//! of whether anything fired -- a scheduled run finding "nothing stuck"
//! is success, not a no-op error.

use std::time::Duration;

use anyhow::Context;
use breathe_retirada_ci::{
    NoActionReason, QueueStarvationPolicy, QueuedJobObservation, RemediationDecision,
    RetiradaCiPolicy, RunSummary, classify_queue_starvation, compute_adaptive_starvation_threshold,
    count_prior_auto_retries,
};
use clap::Parser;
use octocrab::Octocrab;
use octocrab::models::workflows::Status;

#[derive(Parser, Debug)]
#[command(about = "Watch a workflow for jobs starved of runner capacity, redispatch on-demand.")]
struct Args {
    /// `"owner/name"`.
    #[arg(long)]
    repo: String,

    /// Workflow file name, e.g. `image.yml`.
    #[arg(long)]
    workflow: String,

    /// Only jobs whose labels start with this are ever considered —
    /// never touches a job on a different runner pool.
    #[arg(long, default_value = "camelot-builder-pleme")]
    label_prefix: String,

    /// Fixed threshold when `--disable-adaptive-threshold` is set;
    /// otherwise the FLOOR the adaptive threshold never drops below.
    #[arg(long, default_value_t = 600)]
    starvation_threshold_secs: u64,

    /// How far above observed p95 Wake latency the adaptive threshold
    /// sits — headroom so a normal cold start never trips it.
    #[arg(long, default_value_t = 3.0)]
    adaptive_multiplier: f64,

    /// Use the fixed `--starvation-threshold-secs` instead of learning
    /// it from recent observed Wake latency.
    #[arg(long, default_value_t = false)]
    disable_adaptive_threshold: bool,

    #[arg(long, default_value_t = 1)]
    max_auto_retries: u32,

    /// The `workflow_dispatch` input name the target workflow exposes
    /// for its on-demand escape hatch.
    #[arg(long, default_value = "runnerOverride")]
    runner_override_input: String,

    #[arg(long, default_value = "ubuntu-24.04")]
    runner_override_value: String,

    /// Loop internally instead of exiting after one pass — the way to
    /// get faster-than-GitHub's-5-minute-schedule-floor detection
    /// without fighting the scheduler (see module docs). `0` (default)
    /// = run one pass and exit, matching the original behavior.
    #[arg(long, default_value_t = 0)]
    loop_for_secs: u64,

    /// Sleep between passes when `--loop-for-secs` > 0.
    #[arg(long, default_value_t = 60)]
    poll_interval_secs: u64,

    /// Report what would happen without dispatching anything.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let token = std::env::var("GITHUB_TOKEN").context("GITHUB_TOKEN must be set")?;
    let octo = Octocrab::builder().personal_token(token).build()?;

    let retry_policy = RetiradaCiPolicy {
        on_demand_runner_label: args.runner_override_value.clone(),
        max_auto_retries: args.max_auto_retries,
    };

    if args.loop_for_secs == 0 {
        let (inspected, redispatched) = run_pass(&octo, &args, &retry_policy).await?;
        tracing::info!("inspected {inspected} queued job(s), redispatched {redispatched}");
        return Ok(());
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.loop_for_secs);
    let mut total_inspected = 0u32;
    let mut total_redispatched = 0u32;
    let mut pass = 0u32;
    loop {
        pass += 1;
        let (inspected, redispatched) = run_pass(&octo, &args, &retry_policy).await?;
        total_inspected += inspected;
        total_redispatched += redispatched;
        tracing::info!(
            "pass {pass}: inspected {inspected}, redispatched {redispatched} \
             (loop totals: {total_inspected} / {total_redispatched})"
        );
        if tokio::time::Instant::now() + Duration::from_secs(args.poll_interval_secs) >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_secs(args.poll_interval_secs)).await;
    }
    tracing::info!(
        "loop window closed after {pass} pass(es): inspected {total_inspected}, \
         redispatched {total_redispatched} total"
    );
    Ok(())
}

/// One round: list queued jobs, classify, act. Returns
/// `(inspected, redispatched)`.
async fn run_pass(
    octo: &Octocrab,
    args: &Args,
    retry_policy: &RetiradaCiPolicy,
) -> anyhow::Result<(u32, u32)> {
    let (owner, repo) = args
        .repo
        .split_once('/')
        .context("--repo must be \"owner/name\"")?;

    let starvation_policy = if args.disable_adaptive_threshold {
        QueueStarvationPolicy {
            starvation_threshold: Duration::from_secs(args.starvation_threshold_secs),
        }
    } else {
        let wake_latencies = fetch_recent_wake_latencies(octo, owner, repo, &args.workflow).await?;
        let threshold = compute_adaptive_starvation_threshold(
            &wake_latencies,
            args.adaptive_multiplier,
            Duration::from_secs(args.starvation_threshold_secs),
        );
        tracing::debug!(
            "adaptive starvation threshold: {threshold:?} (from {} recent Wake sample(s))",
            wake_latencies.len(),
        );
        QueueStarvationPolicy {
            starvation_threshold: threshold,
        }
    };

    let mut redispatched = 0u32;
    let mut inspected = 0u32;

    for status_str in ["queued", "in_progress"] {
        let runs = octo
            .workflows(owner, repo)
            .list_runs(&args.workflow)
            .status(status_str)
            .per_page(50)
            .send()
            .await
            .context("list_runs")?;

        for run in runs.items {
            let jobs = octo
                .workflows(owner, repo)
                .list_jobs(run.id)
                .send()
                .await
                .context("list_jobs")?;

            for job in jobs.items {
                let labels_match = job
                    .labels
                    .iter()
                    .any(|l| l.starts_with(&args.label_prefix));
                if !labels_match || job.status != Status::Queued {
                    continue;
                }
                inspected += 1;

                let queued_for = chrono::Utc::now()
                    .signed_duration_since(job.created_at)
                    .to_std()
                    .unwrap_or(Duration::ZERO);

                // Stateless retry-ceiling: ask GitHub's own history how
                // many times this exact commit was already redispatched.
                let recent = recent_run_summaries(octo, owner, repo, &args.workflow).await?;
                let prior_auto_retries = count_prior_auto_retries(&run.head_sha, &recent);

                let obs = QueuedJobObservation {
                    job_id: job.id.into_inner(),
                    job_name: job.name.clone(),
                    repo: args.repo.clone(),
                    workflow_file: args.workflow.clone(),
                    head_sha: run.head_sha.clone(),
                    runner_ever_assigned: job.runner_id.is_some(),
                    queued_for,
                    prior_auto_retries,
                };

                let decision = classify_queue_starvation(&obs, &starvation_policy, retry_policy);
                report(&obs, &decision);

                if let RemediationDecision::RedispatchOnDemand {
                    workflow_file,
                    head_sha,
                    runner_override,
                    ..
                } = &decision
                {
                    redispatched += 1;
                    if args.dry_run {
                        tracing::warn!(
                            "DRY RUN — would dispatch {workflow_file} @ {head_sha} with \
                             {}={runner_override}",
                            args.runner_override_input,
                        );
                    } else {
                        dispatch(
                            octo,
                            owner,
                            repo,
                            workflow_file,
                            head_sha,
                            &args.runner_override_input,
                            runner_override,
                        )
                        .await?;
                    }
                }
            }
        }
    }

    Ok((inspected, redispatched))
}

fn report(obs: &QueuedJobObservation, decision: &RemediationDecision) {
    match decision {
        RemediationDecision::RedispatchOnDemand { .. } => {
            tracing::warn!(
                "STARVATION CONFIRMED: {} ({}) queued {:?} with no runner — redispatching",
                obs.job_name,
                obs.head_sha,
                obs.queued_for,
            );
        }
        RemediationDecision::NoAction {
            reason: NoActionReason::StillWithinNormalWake,
        } => {
            tracing::info!(
                "{} ({}) queued {:?} — still within normal Wake latency",
                obs.job_name,
                obs.head_sha,
                obs.queued_for,
            );
        }
        RemediationDecision::NoAction {
            reason: NoActionReason::RetryCeilingReached,
        } => {
            tracing::error!(
                "{} ({}) queued {:?}, already auto-retried the maximum — needs a human",
                obs.job_name,
                obs.head_sha,
                obs.queued_for,
            );
        }
        RemediationDecision::NoAction { reason } => {
            tracing::info!("{} ({}): {:?}", obs.job_name, obs.head_sha, reason);
        }
    }
}

/// Recent runs of `workflow`, typed down to exactly what
/// [`count_prior_auto_retries`] needs.
async fn recent_run_summaries(
    octo: &Octocrab,
    owner: &str,
    repo: &str,
    workflow: &str,
) -> anyhow::Result<Vec<RunSummary>> {
    let runs = octo
        .workflows(owner, repo)
        .list_runs(workflow)
        .per_page(50)
        .send()
        .await
        .context("list_runs (history)")?;
    Ok(runs
        .items
        .into_iter()
        .map(|r| RunSummary {
            head_sha: r.head_sha,
            event: r.event,
        })
        .collect())
}

/// Real observed Wake latency (`started_at - created_at`) for recent
/// jobs on `workflow` that DID get a runner assigned — the raw material
/// [`compute_adaptive_starvation_threshold`] learns the threshold from.
async fn fetch_recent_wake_latencies(
    octo: &Octocrab,
    owner: &str,
    repo: &str,
    workflow: &str,
) -> anyhow::Result<Vec<Duration>> {
    let runs = octo
        .workflows(owner, repo)
        .list_runs(workflow)
        .status("completed")
        .per_page(20)
        .send()
        .await
        .context("list_runs (wake-latency history)")?;

    let mut latencies = Vec::new();
    for run in runs.items {
        let jobs = octo
            .workflows(owner, repo)
            .list_jobs(run.id)
            .send()
            .await
            .context("list_jobs (wake-latency history)")?;
        for job in jobs.items {
            if job.runner_id.is_none() {
                continue;
            }
            let latency = job
                .started_at
                .signed_duration_since(job.created_at)
                .to_std()
                .unwrap_or(Duration::ZERO);
            latencies.push(latency);
        }
    }
    Ok(latencies)
}

async fn dispatch(
    octo: &Octocrab,
    owner: &str,
    repo: &str,
    workflow_file: &str,
    head_sha: &str,
    input_name: &str,
    input_value: &str,
) -> anyhow::Result<()> {
    // workflow_dispatch targets a REF (branch/tag), not a bare sha --
    // dispatch against the sha itself, which GitHub accepts as a ref
    // for any commit currently reachable from a branch.
    let mut inputs = serde_json::Map::new();
    inputs.insert(
        input_name.to_owned(),
        serde_json::Value::String(input_value.to_owned()),
    );
    octo.actions()
        .create_workflow_dispatch(owner, repo, workflow_file, head_sha)
        .inputs(serde_json::Value::Object(inputs))
        .send()
        .await
        .context("dispatch workflow_dispatch")?;
    tracing::warn!("dispatched {workflow_file} @ {head_sha} with {input_name}={input_value}");
    Ok(())
}
