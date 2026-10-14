use crate::model::{Chart, DbBudget, Observed, Policy, PoolDecl, Raise, Request, Tier};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const APP: &str = "breathe-capacity";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Target {
    pub tenant: String,
    pub environment: Tier,
    pub chart: String,
    pub values: PathBuf,
    #[serde(default)]
    pub context: Option<String>,
    #[serde(default)]
    pub namespace: Option<String>,
    pub pools: Vec<PoolDecl>,
    #[serde(default)]
    pub startup_db_fanout: Vec<String>,
    #[serde(default)]
    pub db: Option<DbBudget>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Run {
    pub target: Option<String>,
    pub service: Option<String>,
    #[serde(with = "serde_yaml::with::singleton_map")]
    pub raise: Option<Raise>,
    pub observed: Option<Observed>,
    pub db: Option<DbBudget>,
    pub nodes: Option<PathBuf>,
    pub pods: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CapacityConfig {
    pub repo_root: Option<PathBuf>,
    pub policy: Policy,
    pub charts: BTreeMap<String, Chart>,
    pub targets: BTreeMap<String, Target>,
    pub run: Run,
}

impl shikumi::TieredConfig for CapacityConfig {
    fn bare() -> Self {
        Self::default()
    }

    fn prescribed_default() -> Self {
        Self { policy: Policy::default(), ..Self::default() }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResolveError {
    #[error("no `run.{0}` set (flag --{1}, or a config layer)")]
    Missing(&'static str, &'static str),
    #[error("target {0:?} is not declared; declared targets: {1}")]
    UnknownTarget(String, String),
    #[error("target {0:?} names chart {1:?}, which no config layer declares")]
    UnknownChart(String, String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub request: Request,
    pub values: PathBuf,
    pub nodes: PathBuf,
    pub pods: PathBuf,
    pub context: Option<String>,
}

impl CapacityConfig {
    pub fn resolve(&self) -> Result<Resolved, ResolveError> {
        let r = &self.run;
        let name = r.target.clone().ok_or(ResolveError::Missing("target", "target"))?;
        let t = self.targets.get(&name).ok_or_else(|| {
            ResolveError::UnknownTarget(name.clone(), self.targets.keys().cloned().collect::<Vec<_>>().join(", "))
        })?;
        let chart = self
            .charts
            .get(&t.chart)
            .cloned()
            .ok_or_else(|| ResolveError::UnknownChart(name.clone(), t.chart.clone()))?;
        let service = r.service.clone().ok_or(ResolveError::Missing("service", "service"))?;
        let raise = r.raise.ok_or(ResolveError::Missing("raise", "memory-request|cpu-request|min-replicas|max-replicas"))?;
        let observed = r.observed.clone().unwrap_or_default();
        let values = self.repo_root.as_ref().map_or_else(|| t.values.clone(), |root| root.join(&t.values));
        Ok(Resolved {
            request: Request {
                tenant: t.tenant.clone(),
                environment: t.environment,
                namespace: t.namespace.clone(),
                startup_db_fanout: t.startup_db_fanout.contains(&service),
                service,
                chart,
                raise,
                observed,
                pools: t.pools.clone(),
                db: r.db.or(t.db),
                policy: self.policy,
            },
            values,
            nodes: r.nodes.clone().ok_or(ResolveError::Missing("nodes", "nodes"))?,
            pods: r.pods.clone().ok_or(ResolveError::Missing("pods", "pods"))?,
            context: t.context.clone(),
        })
    }
}
