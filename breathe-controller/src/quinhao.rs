//! The fair-share allocator reconciler: drives `QuinhaoPoolSpec::allocate` in the
//! live controller, watching `QuinhaoPool` CRs. Each pool divides a band (the
//! pool's `capacity × setpoint`) among a forest of weighted claimants (groups →
//! users) per dimension, and publishes the computed grant ledger in `.status`.
//!
//! ADVISORY by construction: this arm carves NOTHING — it writes status only (the
//! grant ledger gaveta reads). The pool's own `StorageBand` still holds the 80%
//! band; `QuinhaoPool` only DIVIDES what that band reports as available. So there
//! is no actuation gate, no L2 ceiling, no DisruptionPolicy here — division adds
//! no safety surface (the band law caps; this partitions).
//!
//! The allocation is a PURE function of the claim forest (`spec.claims`) + the
//! resolved capacity, so a member joining/leaving/going idle is a re-derivation on
//! the next reconcile — the "resident flexibility that shifts accordingly".

use std::sync::Arc;

use breathe_crd::{QuinhaoPool, QuinhaoPoolStatus, StorageBand};
use breathe_runtime::now_secs;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
    Client, ResourceExt,
};
use metrics::gauge;
use tracing::{info, warn};

use crate::{Ctx, Error};

/// Read a referenced `StorageBand`'s `status.observedCapacity` (bytes) — the
/// destination coupling where the divider tracks the band that holds the pool.
/// Best-effort: any miss (no ref, no band, no status, API error) ⇒ `None`, and
/// the pool falls back to its explicit `poolCapacity` storage entry.
async fn storage_band_observed(client: &Client, cr: &QuinhaoPool) -> Option<u64> {
    let r = cr.spec.storage_band_ref.as_ref()?;
    let ns = r.namespace.clone().or_else(|| cr.namespace()).unwrap_or_default();
    let band = Api::<StorageBand>::namespaced(client.clone(), &ns).get_opt(&r.name).await.ok().flatten()?;
    let observed = band.status.and_then(|s| s.observed_capacity)?;
    u64::try_from(observed).ok()
}

/// Reconcile ONE `QuinhaoPool` — resolve capacity → allocate (pure) → publish the
/// grant ledger to `.status`. Advisory: no infrastructure is mutated.
pub async fn reconcile_quinhao_pool(cr: Arc<QuinhaoPool>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let ns = cr.namespace().unwrap_or_default();
    let name = cr.name_any();

    // Resolve the storage capacity from a referenced StorageBand if set (overrides
    // the explicit poolCapacity storage entry — the destination coupling).
    let observed = storage_band_observed(&ctx.client, &cr).await;

    // The whole allocation is pure — build the forest + capacity, run the fabric
    // allocator, fold into the typed status. No I/O, so the CR status, gaveta's
    // read, and the logs can never disagree about the grants.
    let mut status: QuinhaoPoolStatus = cr.spec.allocate(observed);
    status.observed_generation = cr.metadata.generation;
    status.last_seen_epoch = Some(now_secs());

    // Surface the per-dim band as gauges (the "watch it divide" view).
    for (dim, band) in &status.band {
        gauge!("breathe_quinhao_band", "pool" => name.clone(), "dim" => dim.clone()).set(*band as f64);
    }
    gauge!("breathe_quinhao_claims", "pool" => name.clone()).set(status.claim_count.unwrap_or(0) as f64);

    info!(
        pool = %name,
        phase = ?status.phase,
        claims = ?status.claim_count,
        reason = ?status.reason,
        dry_run = ?status.effective_dry_run,
        "QuinhaoPool reconciled (advisory — status-only)"
    );

    patch_status(&ctx.client, &ns, &name, &status).await;
    Ok(Action::requeue(ctx.requeue))
}

/// Patch a `QuinhaoPool`'s `.status` (namespaced, status subresource). Non-fatal —
/// a failed patch logs + continues (status is observability).
async fn patch_status(client: &Client, ns: &str, name: &str, status: &QuinhaoPoolStatus) {
    let api: Api<QuinhaoPool> = Api::namespaced(client.clone(), ns);
    let patch = serde_json::json!({ "status": status });
    if let Err(e) = api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch)).await {
        warn!(pool = %name, error = %e, "QuinhaoPool status patch failed (non-fatal)");
    }
}

/// Error policy for the quinhão-pool controller — back off + requeue.
pub fn error_policy_quinhao_pool(_cr: Arc<QuinhaoPool>, err: &Error, ctx: Arc<Ctx>) -> Action {
    warn!(error = %err, "QuinhaoPool reconcile error — backing off");
    Action::requeue(ctx.requeue)
}
