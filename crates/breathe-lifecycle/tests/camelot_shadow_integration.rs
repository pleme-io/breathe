//! REAL, READ-ONLY, TERMINATE-INCAPABLE proof that `OrphanTracker` correctly
//! classifies Camelot's known real orphan against the live
//! `akeyless-development` AWS account (376129857990, us-east-2).
//!
//! # Why an integration test, not a live in-cluster controller
//!
//! `theory/CORRENTEZA.md` §10's phased path (M1) names the destination —
//! an `IsolationBand`-shaped CRD reconciled by pangea-operator — but two
//! independent, verified-live blockers make a real in-cluster deployment
//! of this specific piece premature today, not merely inconvenient:
//!
//! 1. **Camelot Mode-1 carries no working GitOps path.** Every workload on
//!    the cluster today (`akeyless-saas`, `camelot-lookout`, `pangea-operator`
//!    itself) was installed via hand-run `helm install` — confirmed live via
//!    `akeyless-k8s/clusters/camelot-mode1/README.md`, which documents the
//!    *entire* tree as `AUTHORED-NOT-YET-RECONCILED` (no `flux-system`
//!    namespace exists on the cluster at all). Landing a brand-new workload
//!    live would mean either hand-rolling a fifth imperative `helm install`
//!    (compounding the exact posture that tree exists to retire) or standing
//!    up Flux bootstrap as a side effect of this task — out of scope and a
//!    separately-gated decision (★★ GITOPS-NATIVE / ★★ PLATFORM-MEDIATED
//!    INFRASTRUCTURE).
//! 2. **No AWS credential surface exists in-cluster for this purpose.** The
//!    one CR that already declares the intended shape,
//!    `apps/camelot/camelot-agent-node-infrastructuretemplate.yaml`, names its
//!    own missing prerequisite explicitly: the `camelot-agent-node-operator`
//!    Secret it points `providerCredentials.aws.secretRef` at does not exist,
//!    and no ServiceAccount in the `camelot` namespace carries an IRSA
//!    `role-arn` annotation either. There is no sanctioned, typed way for an
//!    in-cluster process to read real AWS state on Camelot today.
//!
//! Per this task's own instructions, both being genuinely blocked (not
//! merely effortful) is the documented trigger for the fallback: prove the
//! logic via a tight, real integration test against live AWS + the real
//! (currently empty) declared-record store, instead of guessing at a
//! live deployment shape across two unresolved prerequisites this task did
//! not create and should not paper over.
//!
//! # Structural, not just procedural, safety
//!
//! `terminate_instance` below is a PERMANENT REFUSAL — it contains the only
//! reference to `TerminateInstances`-shaped intent in this entire file, and
//! that reference is a hard-coded `Err(..)` that never calls any AWS
//! mutating API. There is no flag to flip; the sweep action is structurally
//! absent from this binary's compiled code path. `grace_ticks` is additionally
//! set far above the single `tick()` call this test makes, so
//! `consecutive_ticks` can never reach the sweep threshold even if the
//! refusal above were somehow bypassed — belt-and-suspenders, not the sole
//! guard. Credentials come from the standard AWS SDK credential chain
//! (`AWS_PROFILE=akeyless-development`, the org's existing typed SSO profile
//! for this account — see `~/.aws/config`), never a hardcoded key.
//!
//! # Running it
//!
//! Skipped by default (including in any CI run) — requires a live SSO
//! session against the real account:
//!
//! ```text
//! AWS_PROFILE=akeyless-development RUN_CAMELOT_AWS_INTEGRATION=1 \
//!   cargo test --test camelot_shadow_integration -- --nocapture
//! ```

use std::collections::BTreeSet;

use aws_sdk_ec2::types::Filter;
use breathe_lifecycle::{
    DriftEnvironment, DriftError, InstanceId, NodeId, ObservedInstance, OrphanTracker,
};

/// The real `DriftEnvironment` for this proof. Two of its three methods are
/// genuine AWS/record-store reads; the third — `terminate_instance` — is a
/// structural refusal (see module doc). No other type in this file is
/// capable of mutating cloud state.
struct ShadowOnlyAwsEnvironment {
    client: aws_sdk_ec2::Client,
}

#[async_trait::async_trait]
impl DriftEnvironment for ShadowOnlyAwsEnvironment {
    /// A real, read-only `DescribeInstances` call, filtered to instances
    /// tagged `project=camelot` (this reconciler's own scope, per the
    /// module doc on `crate::drift`) in a live, non-terminal state.
    async fn observe_tagged_instances(&self) -> Result<Vec<ObservedInstance>, DriftError> {
        let resp = self
            .client
            .describe_instances()
            .filters(
                Filter::builder()
                    .name("tag:project")
                    .values("camelot")
                    .build(),
            )
            .filters(
                Filter::builder()
                    .name("instance-state-name")
                    .values("pending")
                    .values("running")
                    .build(),
            )
            .send()
            .await
            .map_err(|e| DriftError::CloudApi(e.to_string()))?;

        let mut out = Vec::new();
        for reservation in resp.reservations() {
            for instance in reservation.instances() {
                let Some(id) = instance.instance_id() else {
                    continue;
                };
                let lifecycle_id = instance
                    .tags()
                    .iter()
                    .find(|t| t.key() == Some("camelot.pleme.io/lifecycle-id"))
                    .and_then(|t| t.value())
                    .map(NodeId::new);
                out.push(ObservedInstance {
                    instance_id: InstanceId::new(id),
                    lifecycle_id,
                });
            }
        }
        Ok(out)
    }

    /// The honest real answer, not a stub chosen to force an outcome: no
    /// `breathe-lifecycle::fsm::Node<P>` record store has ever been deployed
    /// for Camelot (`theory/CORRENTEZA.md` §10.2 names this FSM↔claim wiring
    /// as a named, unbuilt future integration point). There is no declared-
    /// live registry anywhere in the fleet today, so the true state — right
    /// now, for real — is the empty set.
    async fn declared_live_node_ids(&self) -> Result<BTreeSet<NodeId>, DriftError> {
        Ok(BTreeSet::new())
    }

    /// STRUCTURAL REFUSAL. This harness runs in permanent shadow/observe-only
    /// mode: it never calls `TerminateInstances` or any other mutating EC2
    /// API, unconditionally, regardless of which instance is passed or how
    /// many consecutive ticks it has been marked. This is the ONE method in
    /// this file capable of destructive action, and it is wired to refuse.
    async fn terminate_instance(&self, instance_id: &InstanceId) -> Result<(), DriftError> {
        Err(DriftError::CloudApi(format!(
            "refused: this harness runs in permanent shadow mode and will never terminate \
             a real instance (would-be target: {instance_id})"
        )))
    }
}

#[tokio::test]
async fn camelot_known_orphan_is_flagged_against_real_aws() {
    if std::env::var("RUN_CAMELOT_AWS_INTEGRATION").ok().as_deref() != Some("1") {
        eprintln!(
            "skipped (default): set RUN_CAMELOT_AWS_INTEGRATION=1 and a live \
             AWS_PROFILE=akeyless-development SSO session to run this against real AWS"
        );
        return;
    }

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-2"))
        .load()
        .await;
    let client = aws_sdk_ec2::Client::new(&config);
    let env = ShadowOnlyAwsEnvironment { client };

    // grace_ticks is set far above the single tick this test performs — the
    // sweep threshold is structurally unreachable here even setting aside
    // the terminate_instance refusal above.
    let mut tracker = OrphanTracker::new(1_000).expect("grace_ticks >= 1 is a valid config");
    let report = tracker
        .tick(&env)
        .await
        .expect("tick against real AWS + the (empty) real declared-record store");

    eprintln!("real tick report against live akeyless-development/us-east-2: {report:?}");

    // The known hand-launched orphan: i-019af78a72a51590e, Name=camelot-dev-k3s,
    // tagged project=camelot / owner=luis / posture=spot-breathable, launched
    // via a bare RunInstances call (confirmed via CloudTrail, actor
    // luis.d@akeyless.io) — never through any declarative record. It carries
    // no camelot.pleme.io/lifecycle-id tag, so it must be flagged newly-marked
    // on this, its first-ever OrphanTracker tick.
    let known_orphan = InstanceId::new("i-019af78a72a51590e");
    assert!(
        report.newly_marked.contains(&known_orphan),
        "expected the known hand-launched camelot-dev-k3s instance to be flagged as a newly \
         marked orphan on this tick; got: {report:?}"
    );

    // Structural invariant, re-asserted at the test level: this harness must
    // never sweep anything, no matter what real AWS returns.
    assert!(
        report.swept.is_empty(),
        "invariant violated: this shadow-only harness must never sweep — got: {report:?}"
    );
}
