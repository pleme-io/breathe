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
//! One pass = one invocation: list queued jobs on `--workflow` matching
//! `--label-prefix`, classify each via [`breathe_retirada_ci::classify_queue_starvation`],
//! and for a confirmed, not-already-retried starvation, redispatch via
//! `workflow_dispatch` with `runnerOverride` set. Exits `0` regardless
//! of whether anything fired -- a scheduled run finding "nothing stuck"
//! is success, not a no-op error.

use std::time::Duration;

use anyhow::Context;
use breathe_retirada_ci::{
    NoActionReason, QueueStarvationPolicy, QueuedJobObservation, RemediationDecision,
    RetiradaCiPolicy, RunSummary, classify_queue_starvation, count_prior_auto_retries,
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

    #[arg(long, default_value_t = 600)]
    starvation_threshold_secs: u64,

    #[arg(long, default_value_t = 1)]
    max_auto_retries: u32,

    /// The `workflow_dispatch` input name the target workflow exposes
    /// for its on-demand escape hatch.
    #[arg(long, default_value = "runnerOverride")]
    runner_override_input: String,

    #[arg(long, default_value = "ubuntu-24.04")]
    runner_override_value: String,

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
    let (owner, repo) = args
        .repo
        .split_once('/')
        .context("--repo must be \"owner/name\"")?;

    let token = std::env::var("GITHUB_TOKEN").context("GITHUB_TOKEN must be set")?;
    let octo = Octocrab::builder().personal_token(token).build()?;

    let starvation_policy = QueueStarvationPolicy {
        starvation_threshold: Duration::from_secs(args.starvation_threshold_secs),
    };
    let retry_policy = RetiradaCiPolicy {
        on_demand_runner_label: args.runner_override_value.clone(),
        max_auto_retries: args.max_auto_retries,
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
                let recent = recent_run_summaries(&octo, owner, repo, &args.workflow).await?;
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

                let decision = classify_queue_starvation(&obs, &starvation_policy, &retry_policy);
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
                            &octo,
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

    tracing::info!("inspected {inspected} queued job(s), redispatched {redispatched}");
    Ok(())
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
