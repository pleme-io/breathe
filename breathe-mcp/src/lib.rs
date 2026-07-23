//! `breathe-mcp` — the MCP surface a model uses to drive a running breathe instance.
//!
//! The state + the mutation morphism live in [`breathe_facade`] (the CRDs ARE the
//! state — a typed facade, not a second store, shared by every surface). This
//! crate is only the rmcp tool wrapping: one `#[tool]` per facade operation, over
//! stdio. A tool's `patch` is the same mutation a `kubectl patch` performs, so it
//! never contends with the controller (which co-writes `status`).
//!
//! The tool set holds the full shadow-first lifecycle: list/get/author/patch
//! bands, flip per-band `dryRun` and the node `writeEnabled` master switch (the
//! two safety gates), read the self-describing catalog.

use std::sync::Arc;

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router, ServerHandler,
};
use serde::Deserialize;
use serde_json::{json, Value};

pub use breathe_facade::{BandKind, BreatheStore, KubeStore, StoreError};

// ─────────────────────────── tool inputs ───────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListBandsInput {
    #[schemars(description = "Band dimension: memory | cpu | storage | arc | cgroup")]
    pub kind: BandKind,
    #[serde(default)]
    #[schemars(description = "Namespace to scope to. Omitted = all namespaces.")]
    pub namespace: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BandRef {
    #[schemars(description = "Band dimension: memory | cpu | storage | arc | cgroup")]
    pub kind: BandKind,
    #[schemars(description = "Namespace of the band CR")]
    pub namespace: String,
    #[schemars(description = "Name of the band CR")]
    pub name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PatchBandInput {
    #[schemars(description = "Band dimension: memory | cpu | storage | arc | cgroup")]
    pub kind: BandKind,
    pub namespace: String,
    pub name: String,
    #[schemars(description = "Spec fields to merge-patch, e.g. {\"setpoint\":0.8,\"ceiling\":\"6Gi\"}")]
    pub spec: Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetDryRunInput {
    #[schemars(description = "Band dimension: memory | cpu | storage | arc | cgroup")]
    pub kind: BandKind,
    pub namespace: String,
    pub name: String,
    #[schemars(description = "true = SHADOW (decide + report, never mutate); false = LIVE carve")]
    pub dry_run: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PoolRef {
    #[schemars(description = "BreatheNodePool name (cluster-scoped)")]
    pub name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetWriteEnabledInput {
    #[schemars(description = "BreatheNodePool name")]
    pub name: String,
    #[schemars(description = "Node master write switch. false = whole node in SHADOW.")]
    pub write_enabled: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct Empty {}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PostureRef {
    #[schemars(description = "BreathePosture name (cluster-scoped)")]
    pub name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PatchPostureInput {
    #[schemars(description = "BreathePosture name")]
    pub name: String,
    #[schemars(
        description = "Spec fields to merge-patch, e.g. {\"setpoint\":0.8,\"disruptionPolicy\":\"allowConditional\"}"
    )]
    pub spec: Value,
    #[serde(default)]
    #[schemars(description = "true = preview only, apply nothing; false/omitted = apply now")]
    pub dry_run: Option<bool>,
}

// ─────────────────────────── the server ────────────────────────────

#[derive(Clone)]
pub struct BreatheMcp {
    store: Arc<dyn BreatheStore>,
    tool_router: ToolRouter<Self>,
}

fn ok(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|e| format!("{{\"error\":\"serialize: {e}\"}}"))
}
fn err(e: &StoreError) -> String {
    json!({ "error": e.to_string() }).to_string()
}
fn result(r: Result<Value, StoreError>) -> String {
    match r {
        Ok(v) => ok(&v),
        Err(e) => err(&e),
    }
}

#[tool_router]
impl BreatheMcp {
    #[must_use]
    pub fn new(store: Arc<dyn BreatheStore>) -> Self {
        Self { store, tool_router: Self::tool_router() }
    }

    #[tool(description = "List breathe bands of a dimension (memory/cpu/storage/arc/cgroup), optionally namespace-scoped. Returns the CRs incl. spec + status.")]
    pub async fn breathe_band_list(&self, Parameters(p): Parameters<ListBandsInput>) -> String {
        result(self.store.list_bands(p.kind, p.namespace).await)
    }

    #[tool(description = "Get one breathe band CR by kind/namespace/name (full spec + status).")]
    pub async fn breathe_band_get(&self, Parameters(p): Parameters<BandRef>) -> String {
        result(self.store.get_band(p.kind, p.namespace, p.name).await)
    }

    #[tool(description = "Merge-patch a band's spec (setpoint, growAbove, shrinkBelow, growFactor, shrinkFactor, floor, ceiling, cooldownSeconds, maxStalenessSeconds). Tune the homeostasis band.")]
    pub async fn breathe_band_patch(&self, Parameters(p): Parameters<PatchBandInput>) -> String {
        result(self.store.patch_band_spec(p.kind, p.namespace, p.name, p.spec).await)
    }

    #[tool(description = "Flip a band's dryRun: true = SHADOW (decide + report, never carve), false = LIVE. One of the two safety gates.")]
    pub async fn breathe_band_set_dry_run(&self, Parameters(p): Parameters<SetDryRunInput>) -> String {
        result(self.store.patch_band_spec(p.kind, p.namespace, p.name, json!({ "dryRun": p.dry_run })).await)
    }

    #[tool(description = "List BreatheNodePools (cluster-scoped enrollment: node, L2 ceilings, master write switch).")]
    pub async fn breathe_nodepool_list(&self, Parameters(_): Parameters<Empty>) -> String {
        result(self.store.list_pools().await)
    }

    #[tool(description = "Get one BreatheNodePool by name.")]
    pub async fn breathe_nodepool_get(&self, Parameters(p): Parameters<PoolRef>) -> String {
        result(self.store.get_pool(p.name).await)
    }

    #[tool(description = "Flip a node's writeEnabled master switch: false = whole node in SHADOW (no host writes regardless of per-band dryRun), true = LIVE. The node-level safety gate.")]
    pub async fn breathe_nodepool_set_write_enabled(&self, Parameters(p): Parameters<SetWriteEnabledInput>) -> String {
        result(self.store.patch_pool_spec(p.name, json!({ "writeEnabled": p.write_enabled })).await)
    }

    #[tool(description = "The self-describing breathe dimension catalog (the 6 dimensions, maturity, directionality, host-vs-k8s, dependencies).")]
    pub async fn breathe_catalog_list(&self, Parameters(_): Parameters<Empty>) -> String {
        ok(&self.store.catalog())
    }

    #[tool(description = "List every BreathePosture (cluster-scoped named default policy for setpoint/growAbove/growFactor/shrinkBelow/shrinkFactor/cooldownSeconds/maxStalenessSeconds/disruptionPolicy — the 8 fields a band can inherit via spec.postureRef instead of copy-pasting them).")]
    pub async fn breathe_posture_list(&self, Parameters(_): Parameters<Empty>) -> String {
        result(self.store.list_postures().await)
    }

    #[tool(description = "Get one BreathePosture by name, plus the live-computed list of bands (across all 5 dimensions) currently referencing it via spec.postureRef.")]
    pub async fn breathe_posture_get(&self, Parameters(p): Parameters<PostureRef>) -> String {
        let posture = match self.store.get_posture(p.name.clone()).await {
            Ok(v) => v,
            Err(e) => return err(&e),
        };
        // Live-computed, never a maintained status aggregate (per the design's
        // deliberate deferral — a maintained aggregate needs its own writer to
        // stay fresh; filtering `list_bands` client-side needs none).
        let mut referencing = Vec::new();
        for kind in [BandKind::Memory, BandKind::Cpu, BandKind::Storage, BandKind::Arc, BandKind::Cgroup] {
            let Ok(bands) = self.store.list_bands(kind, None).await else { continue };
            let Some(items) = bands.as_array() else { continue };
            for b in items {
                let refs_this = b.get("spec").and_then(|s| s.get("postureRef")).and_then(Value::as_str) == Some(p.name.as_str());
                if !refs_this {
                    continue;
                }
                let namespace = b.get("metadata").and_then(|m| m.get("namespace")).and_then(Value::as_str).unwrap_or_default();
                let name = b.get("metadata").and_then(|m| m.get("name")).and_then(Value::as_str).unwrap_or_default();
                referencing.push(json!({ "kind": kind.as_str(), "namespace": namespace, "name": name }));
            }
        }
        ok(&json!({ "posture": posture, "referencingBands": referencing }))
    }

    #[tool(description = "Merge-patch a BreathePosture's spec. dryRun:true previews the change without applying it. The response always carries fanOutWarning (every referencing band picks this up on its NEXT reconcile tick, never instantly) and gitOpsCaveat (a live patch on a Flux-managed object reverts on the next prune:true reconcile unless followed by a git commit).")]
    pub async fn breathe_posture_patch(&self, Parameters(p): Parameters<PatchPostureInput>) -> String {
        const FAN_OUT_WARNING: &str =
            "this patch affects every band referencing this posture on their NEXT reconcile tick, not instantly";
        const GIT_OPS_CAVEAT: &str = "if this object is Flux-managed with prune:true, this live patch will be \
             reverted on the next reconcile unless followed by a git commit";
        if p.dry_run == Some(true) {
            return ok(&json!({
                "wouldPatch": p.spec,
                "name": p.name,
                "dryRun": true,
                "fanOutWarning": FAN_OUT_WARNING,
                "gitOpsCaveat": GIT_OPS_CAVEAT,
            }));
        }
        match self.store.patch_posture_spec(p.name, p.spec).await {
            Ok(v) => ok(&json!({ "result": v, "fanOutWarning": FAN_OUT_WARNING, "gitOpsCaveat": GIT_OPS_CAVEAT })),
            Err(e) => err(&e),
        }
    }
}

#[tool_handler]
impl ServerHandler for BreatheMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            instructions: Some(
                "Drive a running breathe resource-homeostasis instance. breathe holds workloads at a \
                 utilization band by carving resource limits. List/get/patch bands (memory/cpu/storage in \
                 k8s; arc/cgroup on the host), BreatheNodePools, and BreathePostures (named default \
                 policies bands can share via spec.postureRef instead of duplicating the same 7-value \
                 tuple). Shadow-first: a band's dryRun and a node's writeEnabled are the two safety gates \
                 — observe ShadowWouldApply decisions before flipping either to LIVE. Host writes are \
                 always bounded by the nodepool's L2 ceiling. A BreathePosture can NEVER carry floor/ \
                 ceiling/targetRef/dryRun — a posture patch can never itself flip a band from shadow to \
                 live or widen a capacity bound."
                    .into(),
            ),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockStore {
        patches: Mutex<Vec<(String, Value)>>,
    }
    #[async_trait]
    impl BreatheStore for MockStore {
        async fn list_bands(&self, kind: BandKind, _ns: Option<String>) -> Result<Value, StoreError> {
            // The "memory" dimension carries one band referencing "platform-default"
            // and one that doesn't — real fixture data for
            // `breathe_posture_get`'s live-computed `referencingBands`.
            if kind == BandKind::Memory {
                return Ok(json!([
                    { "kind": kind.as_str(), "metadata": { "name": "app-mem", "namespace": "camelot" }, "spec": { "postureRef": "platform-default" } },
                    { "kind": kind.as_str(), "metadata": { "name": "other-mem", "namespace": "camelot" }, "spec": {} },
                ]));
            }
            Ok(json!([{ "kind": kind.as_str() }]))
        }
        async fn get_band(&self, kind: BandKind, ns: String, name: String) -> Result<Value, StoreError> {
            Ok(json!({ "kind": kind.as_str(), "namespace": ns, "name": name }))
        }
        async fn patch_band_spec(&self, _k: BandKind, _ns: String, name: String, spec: Value) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, spec.clone()));
            Ok(json!({ "spec": spec }))
        }
        async fn list_pools(&self) -> Result<Value, StoreError> {
            Ok(json!([{ "metadata": { "name": "rio" } }]))
        }
        async fn get_pool(&self, name: String) -> Result<Value, StoreError> {
            Ok(json!({ "name": name }))
        }
        async fn patch_pool_spec(&self, name: String, spec: Value) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, spec.clone()));
            Ok(json!({ "spec": spec }))
        }
        async fn list_postures(&self) -> Result<Value, StoreError> {
            Ok(json!([{ "metadata": { "name": "platform-default" } }]))
        }
        async fn get_posture(&self, name: String) -> Result<Value, StoreError> {
            Ok(json!({ "metadata": { "name": name } }))
        }
        async fn patch_posture_spec(&self, name: String, spec: Value) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, spec.clone()));
            Ok(json!({ "spec": spec }))
        }
        fn catalog(&self) -> Value {
            breathe_facade::catalog_json()
        }
    }

    #[tokio::test]
    async fn set_dry_run_patches_the_dry_run_spec_field() {
        let mock = Arc::new(MockStore::default());
        let mcp = BreatheMcp::new(mock.clone());
        let out = mcp
            .breathe_band_set_dry_run(Parameters(SetDryRunInput {
                kind: BandKind::Arc,
                namespace: "pangea-system".into(),
                name: "rio-arc".into(),
                dry_run: false,
            }))
            .await;
        assert!(out.contains("dryRun"));
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "dryRun": false }));
    }

    #[tokio::test]
    async fn set_write_enabled_patches_the_pool_master_switch() {
        let mock = Arc::new(MockStore::default());
        let mcp = BreatheMcp::new(mock.clone());
        let out = mcp
            .breathe_nodepool_set_write_enabled(Parameters(SetWriteEnabledInput { name: "rio".into(), write_enabled: true }))
            .await;
        assert!(out.contains("writeEnabled"));
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "writeEnabled": true }));
    }

    #[test]
    fn get_info_advertises_tools_and_instructions() {
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        let info = mcp.get_info();
        assert!(info.capabilities.tools.is_some());
        assert!(info.instructions.unwrap().contains("Shadow-first"));
    }

    #[tokio::test]
    async fn posture_list_returns_the_store_output() {
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        let out = mcp.breathe_posture_list(Parameters(Empty {})).await;
        assert!(out.contains("platform-default"));
    }

    #[tokio::test]
    async fn posture_get_surfaces_the_live_computed_referencing_bands() {
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        let out = mcp.breathe_posture_get(Parameters(PostureRef { name: "platform-default".into() })).await;
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["posture"]["metadata"]["name"], "platform-default");
        let refs = v["referencingBands"].as_array().unwrap();
        assert_eq!(refs.len(), 1, "only the one memory band that actually sets postureRef:platform-default matches");
        assert_eq!(refs[0]["kind"], "memory");
        assert_eq!(refs[0]["name"], "app-mem");
        assert_eq!(refs[0]["namespace"], "camelot");
    }

    #[tokio::test]
    async fn posture_patch_dry_run_previews_without_calling_the_store() {
        let mock = Arc::new(MockStore::default());
        let mcp = BreatheMcp::new(mock.clone());
        let out = mcp
            .breathe_posture_patch(Parameters(PatchPostureInput {
                name: "platform-default".into(),
                spec: json!({ "setpoint": 0.75 }),
                dry_run: Some(true),
            }))
            .await;
        assert!(mock.patches.lock().unwrap().is_empty(), "dryRun:true must never call patch_posture_spec");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["dryRun"], true);
        assert_eq!(v["wouldPatch"], json!({ "setpoint": 0.75 }));
        assert!(v["fanOutWarning"].as_str().unwrap().contains("NEXT reconcile tick"));
        assert!(v["gitOpsCaveat"].as_str().unwrap().contains("Flux"));
    }

    #[tokio::test]
    async fn posture_patch_applies_and_still_carries_the_warnings() {
        let mock = Arc::new(MockStore::default());
        let mcp = BreatheMcp::new(mock.clone());
        let out = mcp
            .breathe_posture_patch(Parameters(PatchPostureInput {
                name: "platform-default".into(),
                spec: json!({ "setpoint": 0.75 }),
                dry_run: None,
            }))
            .await;
        assert_eq!(mock.patches.lock().unwrap()[0], ("platform-default".to_string(), json!({ "setpoint": 0.75 })));
        let v: Value = serde_json::from_str(&out).unwrap();
        assert!(v["fanOutWarning"].as_str().unwrap().contains("NEXT reconcile tick"));
        assert!(v["gitOpsCaveat"].as_str().unwrap().contains("Flux"));
    }
}
