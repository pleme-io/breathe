use crate::quantity::{Bytes, Millicores};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Tier {
    Production,
    Staging,
    Development,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sizing {
    pub cpu_request: Millicores,
    pub mem_request: Bytes,
    pub mem_limit: Bytes,
    pub hpa_min: u32,
    pub hpa_max: u32,
    pub hpa_mem_pct: u32,
    pub hpa_cpu_pct: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Keys {
    pub cpu_request: String,
    pub mem_request: String,
    pub mem_limit: String,
    pub hpa_min: String,
    pub hpa_max: String,
    pub hpa_mem_pct: String,
    pub hpa_cpu_pct: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Chart {
    pub root: String,
    pub keys: Keys,
    pub defaults: Sizing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Raise {
    MemoryRequest(Bytes),
    CpuRequest(Millicores),
    MinReplicas(u32),
    MaxReplicas(u32),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Observed {
    pub replicas: u32,
    #[serde(default)]
    pub mem_p95: Option<Bytes>,
    #[serde(default)]
    pub cpu_p95: Option<Millicores>,
    #[serde(default)]
    pub db_conns_per_pod: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolDecl {
    pub name: String,
    pub max_nodes: u32,
    #[serde(default)]
    pub ceiling_in_module: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DbBudget {
    pub max_connections: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Policy {
    pub max_step_pct: u32,
    pub limit_to_request_pct: Option<u32>,
    pub usage_headroom_pct: u32,
    pub surge_pct: u32,
    pub db_safety_pct: u32,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            max_step_pct: 200,
            limit_to_request_pct: None,
            usage_headroom_pct: 125,
            surge_pct: 25,
            db_safety_pct: 80,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub tenant: String,
    pub environment: Tier,
    pub service: String,
    #[serde(default)]
    pub startup_db_fanout: bool,
    pub chart: Chart,
    #[serde(with = "serde_yaml::with::singleton_map")]
    pub raise: Raise,
    pub observed: Observed,
    pub pools: Vec<PoolDecl>,
    #[serde(default)]
    pub db: Option<DbBudget>,
    #[serde(default)]
    pub policy: Policy,
}
