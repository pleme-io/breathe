//! `replica_env` — the KubeCluster-backed [`ReplicaEnvironment`] adapter for the
//! HORIZONTAL (replica) band.
//!
//! The horizontal band law (`breathe_control::replica`) reads its inputs through a
//! SYNC, I/O-free [`ReplicaEnvironment`] (the TYPED-SPEC triplet's Environment trait
//! — the testability contract). Kubernetes I/O is async, so the two are bridged the
//! only correct way: [`KubeCluster::observe_replica_env`] does ALL the async reads in
//! one pass and freezes them into a [`KubeReplicaEnv`] SNAPSHOT whose sync trait
//! methods just return the cached fields — structurally identical to
//! `MockReplicaEnvironment`, but populated from the live cluster instead of a test.
//!
//! What it observes:
//!   * `current_replicas` — the workload's live `.spec.replicas`, via the existing
//!     [`Cluster::read_limit`] over [`LimitLayout::Replica`] (the same typed reader
//!     the actuator's field is written through — no new read path).
//!   * `signal_value` — the driving PromQL as a RAW `f64` (via
//!     [`KubeCluster::query_scalar`]); a utilization ratio must NOT be `u64`-truncated.
//!   * `reclaim_pending` — the OPTIONAL spot-reclaim PromQL, best-effort (an absent or
//!     broken reclaim signal ⇒ `0`, never a hold — the driving signal is authoritative).
//!
//! `window_max_desired` stays `None` (the `ReplicaEnvironment` default): the
//! scale-down stabilization window is a documented follow-on that carries the raw
//! desired-count peak across ticks in status; the shipped anti-flap is the metric
//! tolerance dead-band + the per-direction velocity caps + the post-carve cooldown.

use breathe_control::replica::{ReplicaEnvironment, ReplicaError};
use breathe_provider::{Cluster, LimitLayout, MetricSource, ProviderError, Target};

use crate::KubeCluster;

/// A live-cluster SNAPSHOT of one [`crate::KubeCluster`]-observed horizontal tick —
/// implements the sync [`ReplicaEnvironment`] over cached fields (see the module
/// docs). Also carries the driving sample's `staleness_secs` so the reconcile layer
/// can apply the never-scale-on-a-stale-sample gate (that age is not part of the pure
/// `ReplicaEnvironment` contract, which is why it is a separate accessor).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KubeReplicaEnv {
    current_replicas: u32,
    signal_value: f64,
    reclaim_pending: u32,
    staleness_secs: u64,
}

impl KubeReplicaEnv {
    /// The observed `.spec.replicas` at snapshot time.
    #[must_use]
    pub fn current(&self) -> u32 {
        self.current_replicas
    }
    /// The raw driving-signal reading.
    #[must_use]
    pub fn signal(&self) -> f64 {
        self.signal_value
    }
    /// The observed pending spot-reclaim count (0 when not spot-aware).
    #[must_use]
    pub fn reclaim(&self) -> u32 {
        self.reclaim_pending
    }
    /// Age (seconds) of the driving metric sample — the freshness-gate input.
    #[must_use]
    pub fn staleness_secs(&self) -> u64 {
        self.staleness_secs
    }
}

impl ReplicaEnvironment for KubeReplicaEnv {
    fn current_replicas(&self) -> Result<u32, ReplicaError> {
        Ok(self.current_replicas)
    }
    fn signal_value(&self) -> Result<f64, ReplicaError> {
        Ok(self.signal_value)
    }
    fn reclaim_pending(&self) -> u32 {
        self.reclaim_pending
    }
    // window_max_desired: None (the trait default) — see the module docs.
}

impl KubeCluster {
    /// OBSERVE a `ReplicaBand`'s horizontal inputs in ONE async pass → a sync
    /// [`KubeReplicaEnv`] snapshot the pure interpreter (`plan_replica_tick`) walks.
    ///
    /// The driving `signal` MUST be a [`MetricSource::Prometheus`] (a work-rate /
    /// utilization PromQL); a non-PromQL source on the horizontal path is a typed
    /// [`ProviderError::ApiPermanent`], never a silent wrong value. The optional
    /// `reclaim` signal is best-effort: a missing/broken reclaim metric yields `0`
    /// pending (spot-awareness degrades gracefully), while a missing DRIVING signal
    /// propagates (the band holds rather than scale on nothing).
    ///
    /// # Errors
    /// Propagates the `.spec.replicas` read error (target not found / API transient)
    /// and the driving-signal query error (metrics missing / API transient); returns
    /// [`ProviderError::ApiPermanent`] for a non-PromQL driving source.
    pub async fn observe_replica_env(
        &self,
        target: &Target,
        layout: &LimitLayout,
        signal: &MetricSource,
        reclaim: Option<&MetricSource>,
    ) -> Result<KubeReplicaEnv, ProviderError> {
        // current `.spec.replicas` via the existing typed reader (LimitLayout::Replica).
        // read_limit returns u64; a replica count never approaches u32::MAX, but a
        // pathological read saturates rather than wraps (never a panic).
        let current_replicas = u32::try_from(self.read_limit(target, layout, "count").await?).unwrap_or(u32::MAX);

        // driving signal — RAW f64 (utilization ratios must not be u64-truncated).
        let (signal_value, staleness_secs) = match signal {
            MetricSource::Prometheus(promql) => self.query_scalar(promql).await?,
            other => {
                return Err(ProviderError::ApiPermanent(format!(
                    "horizontal band signal must be a PromQL work-rate/utilization query, got {other:?}"
                )))
            }
        };

        // optional spot-reclaim signal — best-effort: an absent/broken reclaim metric
        // is 0 pending, never a hold (the driving signal above is authoritative).
        let reclaim_pending = match reclaim {
            Some(MetricSource::Prometheus(promql)) => match self.query_scalar(promql).await {
                Ok((v, _)) if v.is_finite() && v > 0.0 => {
                    // ceil to cover the whole doomed set; cap into u32 range.
                    let n = v.ceil();
                    if n >= f64::from(u32::MAX) { u32::MAX } else { n as u32 }
                }
                _ => 0,
            },
            _ => 0,
        };

        Ok(KubeReplicaEnv { current_replicas, signal_value, reclaim_pending, staleness_secs })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_serves_the_replica_environment_contract() {
        let env = KubeReplicaEnv { current_replicas: 4, signal_value: 0.9, reclaim_pending: 1, staleness_secs: 3 };
        assert_eq!(env.current_replicas(), Ok(4));
        assert_eq!(env.signal_value(), Ok(0.9));
        assert_eq!(env.reclaim_pending(), 1);
        assert_eq!(env.window_max_desired(), None);
        assert_eq!(env.staleness_secs(), 3);
        // it drives the pure interpreter exactly like the mock does.
        let cfg = breathe_control::replica::ReplicaBandConfig { ceiling: 50, ..Default::default() };
        let d = breathe_control::replica::interpret_replica(&cfg, &env).expect("decides");
        assert_eq!(d, breathe_control::replica::ReplicaDecision::SpotScaleOut { from: 4, to: 5, reclaim: 1 });
    }
}
