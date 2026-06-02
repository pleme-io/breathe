//! `breathe-runtime` — the controller-runtime glue shared by breathe's two
//! reconcile binaries: the **brain** (`breathe-controller`, k8s dimensions via
//! `KubeCluster`) and the **hands** (`breathe-host-agent`, host dimensions via
//! `HostCluster`). The decision math lives in `breathe-control`; the I/O lives in
//! the `Cluster` impls; this crate owns only the two things both processes must
//! do *identically* — map a `TickReceipt` to a `BandStatus`, and patch it onto
//! the band CR. Sharing it means the brain and the hands can never drift in how a
//! decision is reported (a `ShadowWouldApply` means the same thing on both).

use std::time::{SystemTime, UNIX_EPOCH};

use breathe_control::Decision;
use breathe_core::TickReceipt;
use breathe_crd::{Band, BandStatus};
use kube::{
    api::{Api, Patch, PatchParams},
    Client,
};
use serde_json::json;

/// Unix epoch seconds (monotonic enough for cooldown bookkeeping; 0 on error).
#[must_use]
pub fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

/// Map one reconcile receipt to the typed CR status — every branch observable,
/// none silent. This is the single source of truth for band status semantics
/// across both reconcile processes.
#[must_use]
pub fn status_for(receipt: &TickReceipt) -> BandStatus {
    let mut s = BandStatus::default();
    match receipt {
        TickReceipt::Conflict { manager } => {
            s.phase = Some("Conflict".into());
            s.conflict_manager = Some(manager.clone());
            s.last_decision = Some(format!("yielded to {manager}"));
        }
        TickReceipt::Stale { staleness_secs } => {
            s.phase = Some("Stale".into());
            s.last_decision = Some(format!("metric {staleness_secs}s stale — held"));
        }
        TickReceipt::Cooldown => s.phase = Some("Cooldown".into()),
        TickReceipt::Applied { from, to } => {
            s.phase = Some(if to > from { "Growing" } else { "Shrinking" }.into());
            s.current_limit = Some(to.to_string());
            s.last_decision = Some(format!("{from} -> {to}"));
            s.last_change_epoch = Some(now_secs());
        }
        TickReceipt::DryRunWouldApply { from, to } => {
            s.phase = Some("ShadowWouldApply".into());
            s.current_limit = Some(from.to_string());
            s.last_decision = Some(format!("dry-run: {from} -> {to}"));
        }
        TickReceipt::Observed { decision } => {
            s.phase = Some(
                match decision {
                    Decision::Hold => "Holding",
                    Decision::AtCeiling { .. } => "AtCeiling",
                    Decision::NoSafeShrink { .. } => "AtFloor",
                    Decision::NoLimit => "NoLimit",
                    Decision::Grow { .. } | Decision::Shrink { .. } => "Observed",
                }
                .into(),
            );
        }
        TickReceipt::Error { error } => {
            s.phase = Some("Error".into());
            s.last_decision = Some(error.to_string());
        }
    }
    s
}

/// A short typed error status (band-config parse failures, enrollment gaps).
#[must_use]
pub fn error_status(decision: impl Into<String>) -> BandStatus {
    let mut s = BandStatus::default();
    s.phase = Some("Error".into());
    s.last_decision = Some(decision.into());
    s
}

/// Patch a band CR's `status` subresource (merge — only the fields we set).
pub async fn patch_status<B: Band>(
    client: &Client,
    ns: &str,
    name: &str,
    status: &BandStatus,
) -> Result<(), kube::Error> {
    let api: Api<B> = Api::namespaced(client.clone(), ns);
    let patch = json!({ "status": status });
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applied_growth_vs_shrink_is_reported_directionally() {
        let grow = status_for(&TickReceipt::Applied { from: 100, to: 200 });
        assert_eq!(grow.phase.as_deref(), Some("Growing"));
        assert_eq!(grow.current_limit.as_deref(), Some("200"));
        let shrink = status_for(&TickReceipt::Applied { from: 200, to: 100 });
        assert_eq!(shrink.phase.as_deref(), Some("Shrinking"));
    }

    #[test]
    fn shadow_reports_what_would_have_happened_without_changing_the_limit() {
        let s = status_for(&TickReceipt::DryRunWouldApply { from: 100, to: 250 });
        assert_eq!(s.phase.as_deref(), Some("ShadowWouldApply"));
        // the reported current limit is the UNCHANGED value — shadow mutates nothing.
        assert_eq!(s.current_limit.as_deref(), Some("100"));
        assert!(s.last_decision.as_deref().unwrap().contains("250"));
    }

    #[test]
    fn conflict_records_the_yielded_to_manager() {
        let s = status_for(&TickReceipt::Conflict { manager: "helm".into() });
        assert_eq!(s.phase.as_deref(), Some("Conflict"));
        assert_eq!(s.conflict_manager.as_deref(), Some("helm"));
    }
}
