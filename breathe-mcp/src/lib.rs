//! `breathe-mcp` — the MCP surface a model uses to drive a running breathe instance.
//!
//! The state + the mutation morphism live in [`breathe_facade`] (the CRDs ARE the
//! state — a typed facade, not a second store, shared by every surface). This
//! crate is only the rmcp tool wrapping: one `#[tool]` per facade operation, over
//! stdio. A tool's `patch` is the same mutation a `kubectl patch` performs, so it
//! never contends with the controller (which co-writes `status`).
//!
//! The tool set holds the full authorization lifecycle: list/get/patch bands
//! across EVERY dimension, author `spec.writeIntent`, confirm a calibrating
//! band, flip the node `writeEnabled` master switch, read the self-describing
//! catalog.
//!
//! **Two corrections this crate used to get wrong, stated up front because they
//! are what an operator acts on:**
//!
//! 1. `spec.dryRun` is NOT a safety gate on any band kind except `host-param`
//!    and `kube-param` — the only two that still read it. It has been inert
//!    everywhere else since `breathe@76924b0` (2026-06-19); authorization is
//!    `spec.writeIntent`. The counts here are stated as *"every kind except
//!    those two"* rather than as a number, because the number was written
//!    "eight of ten" and went stale the day the eleventh dimension (`request`)
//!    landed. The old `breathe_band_set_dry_run` tool advertised the opposite
//!    and returned success for a patch that changed nothing — it now refuses on
//!    the kinds where it would be a no-op, and says what to use instead.
//! 2. There are ELEVEN band kinds, not five. `cgroup-cpu`, `host-param`,
//!    `kube-param`, `app-param` and `replica` ship, and were unreachable from
//!    this surface entirely; `request` landed later still.
//!
//! **What a tool RETURNS is a projection, not the CR.** A raw
//! `breathe_band_list(kind="memory")` against camelot-eks was **428,850 bytes**
//! for 52 bands — larger than the whole org `CLAUDE.md` — overflowing the
//! caller's result limit and spilling to disk, because a k8s CR is mostly
//! `managedFields`. Every read tool here now hands back
//! [`breathe_facade::project`]'s compact typed view by default and keeps the
//! whole CR one `view` argument away ([`ProjectionView`]). Nothing became
//! unreachable; the default just stopped being the 8 KB-per-band one.

use std::sync::Arc;

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router, ServerHandler,
};
use serde::Deserialize;
use serde_json::{json, Value};

pub use breathe_facade::{
    project, BreatheStore, DimensionId, KubeStore, ProjectionView, StoreError, DEFAULT_HISTORY_LIMIT,
};
pub use breathe_provider::IntentKind;

/// The one description every `kind` field carries.
///
/// It used to spell out all ten dimension names inline, in six places — a
/// hand-kept list that was already stale (eleven ship) and that duplicated,
/// verbatim, what [`DimensionId`]'s own schema says right beside it. The vocabulary
/// belongs to the enum; this says only which field it is.
const KIND_DESC: &str = "Which band dimension to address — see this field's own enum for the eleven values \
                         and what each carves.";

/// Shared by the read tools that project a payload.
///
/// Kept short deliberately: it is inlined into six tool schemas, so every extra
/// sentence is paid for six times per registration. The long form lives once, in
/// the server `instructions`, where a client reads it once for the whole server.
const VIEW_DESC: &str = "How much of each CR to return. Omitted = summary (compact typed view, ~300 B per band). \
                         detail adds the newest conditions + recent history. full = the whole CR minus \
                         apiserver bookkeeping. raw = the untouched CR (~430 KB for 52 bands — it may \
                         overflow the result limit, so reach for it deliberately).";

// ─────────────────────────── tool inputs ───────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListBandsInput {
    #[schemars(description = KIND_DESC)]
    pub kind: DimensionId,
    #[serde(default)]
    #[schemars(description = "Namespace to scope to. Omitted = all namespaces.")]
    pub namespace: Option<String>,
    #[serde(default)]
    #[schemars(description = VIEW_DESC)]
    pub view: Option<ProjectionView>,
    #[serde(default)]
    #[schemars(description = "With view=detail, how many of the newest status.history samples to keep. Omitted = 3.")]
    pub history_limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BandRef {
    #[schemars(description = KIND_DESC)]
    pub kind: DimensionId,
    #[schemars(description = "Namespace of the band CR")]
    pub namespace: String,
    #[schemars(description = "Name of the band CR")]
    pub name: String,
    #[serde(default)]
    #[schemars(description = VIEW_DESC)]
    pub view: Option<ProjectionView>,
    #[serde(default)]
    #[schemars(description = "How many of the newest status.history samples to keep. Omitted = 3.")]
    pub history_limit: Option<usize>,
}

/// A read tool whose only input is how much to return.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ViewInput {
    #[serde(default)]
    #[schemars(description = VIEW_DESC)]
    pub view: Option<ProjectionView>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PatchBandInput {
    #[schemars(description = KIND_DESC)]
    pub kind: DimensionId,
    pub namespace: String,
    pub name: String,
    #[schemars(description = "Spec fields to merge-patch, e.g. {\"setpoint\":0.8,\"ceiling\":\"6Gi\"}")]
    pub spec: Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetDryRunInput {
    #[schemars(description = KIND_DESC)]
    pub kind: DimensionId,
    pub namespace: String,
    pub name: String,
    #[schemars(
        description = "The spec.dryRun value to write. ONLY MEANINGFUL on host-param and kube-param \
                       bands, which still read it as a two-state shadow/effect switch. On every other \
                       kind this field has had no effect since 2026-06-19 and the call is \
                       refused — author writeIntent instead."
    )]
    pub dry_run: bool,
}

/// The tool-input schema for [`IntentKind`], generated from
/// [`IntentKind::ALL`].
///
/// `IntentKind` is a CRD field (schemars 0.8, via kube) *and* an MCP tool input
/// (schemars 1.x, via rmcp), and a single type cannot carry both derives. The
/// tempting fix — a four-arm mirror enum here — is precisely the duplication
/// that let the old five-arm `BandKind` fall five kinds behind the CRDs it
/// addressed. So the schema is built from the canonical list instead: adding an
/// arm in `breathe-provider` shows up here with no edit, and
/// `intent_schema_offers_exactly_the_canonical_arms` pins it.
fn intent_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let arms: Vec<Value> = IntentKind::ALL.iter().map(|k| json!(k.as_str())).collect();
    schemars::Schema::try_from(json!({ "type": "string", "enum": arms }))
        .expect("a string-enum object is a valid JSON schema")
}

/// Author `spec.writeIntent` — the authorization axis that actually gates
/// carving on every band kind.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetWriteIntentInput {
    #[schemars(description = KIND_DESC)]
    pub kind: DimensionId,
    pub namespace: String,
    pub name: String,
    #[schemars(
        schema_with = "intent_schema",
        description = "observe = never write, keep deciding and reporting (the honest hold). \
                       calibrateThenWrite = shadow until a clean-observation window passes, then write. \
                       write = write now; REQUIRES authorizedBy. \
                       frozen = never write, keep observing (folds the retired mode:suspended)."
    )]
    pub intent: IntentKind,
    #[serde(default)]
    #[schemars(description = "Seconds of clean observation before calibrateThenWrite promotes itself. Omitted = 1800.")]
    pub confirm_after_seconds: Option<u64>,
    #[serde(default)]
    #[schemars(
        description = "REQUIRED when intent=write: who authorized going live, ideally with when and why. \
                       An unattributed go-live is refused — it resolves to a shadow named IntentMalformed, \
                       never to an anonymous carve."
    )]
    pub authorized_by: Option<String>,
}

/// Set (or clear) the operator confirm annotation on a band.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ConfirmBandInput {
    #[schemars(description = KIND_DESC)]
    pub kind: DimensionId,
    pub namespace: String,
    pub name: String,
    #[schemars(
        description = "true = set breathe.pleme.io/confirmed=true, promoting a calibrateThenWrite band to \
                       writing NOW instead of waiting out its confirm window. false = remove the annotation."
    )]
    pub confirmed: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PoolRef {
    #[schemars(description = "BreatheNodePool name (cluster-scoped)")]
    pub name: String,
    #[serde(default)]
    #[schemars(description = VIEW_DESC)]
    pub view: Option<ProjectionView>,
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
    #[serde(default)]
    #[schemars(description = VIEW_DESC)]
    pub view: Option<ProjectionView>,
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

/// Apply a projection to a store payload, then render.
///
/// Composed as one helper rather than repeated per tool so a new read tool
/// cannot ship the raw CR by forgetting to project — the only way to return a
/// store payload from this surface is through here or through an explicit
/// `ProjectionView::Raw`.
fn projected(r: Result<Value, StoreError>, project: impl FnOnce(&Value) -> Value) -> String {
    match r {
        Ok(v) => ok(&project(&v)),
        Err(e) => err(&e),
    }
}

#[tool_router]
impl BreatheMcp {
    #[must_use]
    pub fn new(store: Arc<dyn BreatheStore>) -> Self {
        Self { store, tool_router: Self::tool_router() }
    }

    #[tool(description = "List breathe bands of a dimension, optionally namespace-scoped. Returns a COMPACT typed \
                          projection per band: name, namespace, targetRef, floor/ceiling/setpoint, current limit, \
                          utilization, phase, health, and status.effectiveGate (the resolved authorization verdict \
                          plus, when live, the witness that granted it). Apiserver bookkeeping is dropped — it is \
                          most of a CR's bytes and answers no question about a band. See `view` to widen.")]
    pub async fn breathe_band_list(&self, Parameters(p): Parameters<ListBandsInput>) -> String {
        let view = p.view.unwrap_or_default();
        let limit = p.history_limit.unwrap_or(DEFAULT_HISTORY_LIMIT);
        projected(self.store.list_bands(p.kind, p.namespace).await, |v| {
            project::project_band_list(p.kind, v, view, limit)
        })
    }

    #[tool(description = "Get one breathe band by kind/namespace/name. Returns the compact typed projection plus \
                          the newest condition per type and the last few status.history samples; \
                          view=full gives the whole CR minus apiserver bookkeeping and view=raw gives it untouched.")]
    pub async fn breathe_band_get(&self, Parameters(p): Parameters<BandRef>) -> String {
        // A single-object read defaults one rung RICHER than a list: the whole
        // point of naming one band is that you want its trajectory, and one
        // band's bounded history is ~400 bytes where fifty-two bands' is not.
        let view = p.view.unwrap_or(ProjectionView::Detail);
        let limit = p.history_limit.unwrap_or(DEFAULT_HISTORY_LIMIT);
        projected(self.store.get_band(p.kind, p.namespace, p.name).await, |v| {
            project::project_band(p.kind, v, view, limit)
        })
    }

    #[tool(description = "Merge-patch a band's spec (setpoint, growAbove, shrinkBelow, growFactor, shrinkFactor, floor, ceiling, cooldownSeconds, maxStalenessSeconds). Tune the homeostasis band.")]
    pub async fn breathe_band_patch(&self, Parameters(p): Parameters<PatchBandInput>) -> String {
        result(self.store.patch_band_spec(p.kind, p.namespace, p.name, p.spec).await)
    }

    #[tool(description = "Author a band's spec.writeIntent — THE authorization gate on every band kind. \
                          observe = never write; calibrateThenWrite = shadow, then write once a clean window passes; \
                          write = write now (requires authorizedBy); frozen = never write, keep observing. \
                          This supersedes dryRun, which is inert on 8 of the 10 kinds.")]
    pub async fn breathe_band_set_write_intent(&self, Parameters(p): Parameters<SetWriteIntentInput>) -> String {
        // Refuse an unattributed go-live HERE, at the surface, rather than
        // writing a CR that the controller will only ever resolve to a shadow
        // named IntentMalformed. Same verdict, one round-trip earlier, and the
        // operator learns why while they are still holding the decision.
        if p.intent == IntentKind::Write && p.authorized_by.as_deref().map(str::trim).unwrap_or_default().is_empty() {
            return json!({
                "error": "intent=write requires authorizedBy",
                "why": "a live carve must name who authorized it; the witness is carried into status.effectiveGate \
                        so 'why is this band writing?' is answerable from the CR alone",
                "wroteNothing": true,
            })
            .to_string();
        }
        let mut intent = json!({ "intent": p.intent });
        if let Some(secs) = p.confirm_after_seconds {
            intent["confirmAfterSeconds"] = json!(secs);
        }
        if let Some(by) = p.authorized_by {
            intent["authorizedBy"] = json!(by);
        }
        result(self.store.patch_band_spec(p.kind, p.namespace, p.name, json!({ "writeIntent": intent })).await)
    }

    #[tool(description = "Set or clear breathe.pleme.io/confirmed on a band. true promotes a calibrateThenWrite \
                          band to writing NOW rather than waiting out its confirm window; false removes it. \
                          The operator fast-path — previously reachable only from Rust source and kubectl.")]
    pub async fn breathe_band_confirm(&self, Parameters(p): Parameters<ConfirmBandInput>) -> String {
        // JSON `null` in a merge-patch REMOVES the key — the honest "un-confirm".
        let value = if p.confirmed { json!("true") } else { Value::Null };
        let ann = json!({ breathe_provider::CONFIRMED_ANNOTATION: value });
        result(self.store.annotate_band(p.kind, p.namespace, p.name, ann).await)
    }

    #[tool(description = "RETIRED for most kinds. Writes spec.dryRun, which is only read by host-param and \
                          kube-param bands; on every other kind it has had NO EFFECT since 2026-06-19 \
                          (breathe@76924b0) and this call is REFUSED rather than reported as success. \
                          Use breathe_band_set_write_intent instead.")]
    pub async fn breathe_band_set_dry_run(&self, Parameters(p): Parameters<SetDryRunInput>) -> String {
        if !p.kind.dry_run_is_honored() {
            return json!({
                "error": "spec.dryRun has no effect on this band kind",
                "kind": p.kind.as_str(),
                "retiredSince": "breathe@76924b0 (2026-06-19)",
                "why": "promotion_mode() resolves writeIntent > mode > the compiled ShadowConfirmEffect \
                        default for this kind; spec.dryRun is never consulted. Writing it would have \
                        changed nothing while reporting success.",
                "useInstead": "breathe_band_set_write_intent (intent=observe to hold, frozen to stop writing entirely)",
                "honoredBy": ["host-param", "kube-param"],
                "wroteNothing": true,
            })
            .to_string();
        }
        result(self.store.patch_band_spec(p.kind, p.namespace, p.name, json!({ "dryRun": p.dry_run })).await)
    }

    #[tool(description = "List BreatheNodePools (cluster-scoped enrollment: node, L2 ceilings, master write switch). \
                          Returns name + labels + the spec and status VERBATIM (a pool's authored content is a \
                          handful of scalars), with apiserver bookkeeping dropped; view=raw for the untouched CR.")]
    pub async fn breathe_nodepool_list(&self, Parameters(p): Parameters<ViewInput>) -> String {
        let view = p.view.unwrap_or_default();
        projected(self.store.list_pools().await, |v| project::project_object_list(v, view))
    }

    #[tool(description = "Get one BreatheNodePool by name (spec + status verbatim, apiserver bookkeeping dropped; \
                          view=raw for the untouched CR).")]
    pub async fn breathe_nodepool_get(&self, Parameters(p): Parameters<PoolRef>) -> String {
        let view = p.view.unwrap_or_default();
        projected(self.store.get_pool(p.name).await, |v| project::project_object(v, view))
    }

    #[tool(description = "Flip a BreatheNodePool's writeEnabled master switch. Scope is the HOST plane: it bounds host writes for                           bands enrolled on that pool. It is NOT a cluster-wide kill switch for k8s-plane bands — to stop one of those,                           author writeIntent=frozen (or spec.suspend to stop reconciling it entirely).")]
    pub async fn breathe_nodepool_set_write_enabled(&self, Parameters(p): Parameters<SetWriteEnabledInput>) -> String {
        result(self.store.patch_pool_spec(p.name, json!({ "writeEnabled": p.write_enabled })).await)
    }

    #[tool(description = "The self-describing breathe dimension catalog (every dimension, maturity, directionality, host-vs-k8s, dependencies). Zero-I/O — it is compiled in, not read from the cluster.")]
    pub async fn breathe_catalog_list(&self, Parameters(_): Parameters<Empty>) -> String {
        ok(&self.store.catalog())
    }

    #[tool(description = "List every BreathePosture (cluster-scoped named default policy for setpoint/growAbove/growFactor/shrinkBelow/shrinkFactor/cooldownSeconds/maxStalenessSeconds/disruptionPolicy — the 8 fields a band can inherit via spec.postureRef instead of copy-pasting them). Spec + status verbatim, apiserver bookkeeping dropped; view=raw for the untouched CR.")]
    pub async fn breathe_posture_list(&self, Parameters(p): Parameters<ViewInput>) -> String {
        let view = p.view.unwrap_or_default();
        projected(self.store.list_postures().await, |v| project::project_object_list(v, view))
    }

    #[tool(description = "Get one BreathePosture by name, plus the live-computed list of bands (across every dimension) currently referencing it via spec.postureRef. Spec + status verbatim, apiserver bookkeeping dropped; view=raw for the untouched CR.")]
    pub async fn breathe_posture_get(&self, Parameters(p): Parameters<PostureRef>) -> String {
        let view = p.view.unwrap_or_default();
        let posture = match self.store.get_posture(p.name.clone()).await {
            Ok(v) => project::project_object(&v, view),
            Err(e) => return err(&e),
        };
        // Live-computed, never a maintained status aggregate (per the design's
        // deliberate deferral — a maintained aggregate needs its own writer to
        // stay fresh; filtering `list_bands` client-side needs none).
        let mut referencing = Vec::new();
        for kind in DimensionId::ALL {
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
                 utilization band by carving resource limits.\n\n\
                 ELEVEN band dimensions: memory, cpu, storage, replica, request (k8s plane); arc, cgroup, \
                 cgroup-cpu, host-param (host plane); kube-param, app-param (generic CR / application knobs).\n\n\
                 READS ARE PROJECTED, NOT RAW. Every list/get returns a compact typed view by default \
                 (~300 bytes per band) with apiserver bookkeeping — managedFields, ownerReferences, the \
                 kubectl last-applied-configuration echo — dropped, and status.history/conditions bounded. \
                 Nothing is unreachable: view=detail adds the newest conditions + recent history, view=full \
                 is the whole CR minus that bookkeeping, view=raw is the untouched CR. Reach for raw \
                 deliberately — a 52-band raw list is ~430 KB and will overflow a tool-result limit.\n\n\
                 AUTHORIZATION is spec.writeIntent, and nothing else:\n\
                 - observe            never write; keep deciding and reporting\n\
                 - calibrateThenWrite shadow until a clean-observation window passes, then write\n\
                 - write              write now; REQUIRES authorizedBy naming who said so\n\
                 - frozen             never write, keep observing\n\
                 Read the resolved verdict from status.effectiveGate: it is either Shadow with a typed \
                 reason (which is NOT always 'the operator held it' — NotReady, Stale, Conflict and \
                 ConfirmPending all shadow a band by accident, not by intent) or Live with the witness \
                 that granted it. Author intent with breathe_band_set_write_intent; promote a calibrating \
                 band immediately with breathe_band_confirm.\n\n\
                 spec.dryRun IS NOT A SAFETY GATE on any kind except host-param and kube-param. It has \
                 since 2026-06-19 (breathe@76924b0); only host-param and kube-param still read it. If a \
                 band you expect to be held shows dryRun:true and no writeIntent, it is almost certainly \
                 WRITING — check status.effectiveGate rather than the spec field.\n\n\
                 A BreatheNodePool's writeEnabled bounds HOST-plane writes for bands enrolled on that \
                 pool; it is not a cluster-wide kill switch for k8s-plane bands. Host writes are also \
                 bounded by the pool's L2 ceiling. A BreathePosture structurally carries no floor, \
                 ceiling, targetRef, dryRun or writeIntent field, so a posture patch can never itself \
                 flip a band from shadow to live or widen a capacity bound."
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

    /// The apiserver bookkeeping every real CR arrives wearing, and which no
    /// projected response may carry. Present on the mock's band and pool fixtures
    /// so the tool-level tests below observe the SAME bytes a cluster would send —
    /// a mock that returns a clean CR could not fail the leak test.
    fn bookkeeping() -> Value {
        json!({
            "managedFields": [ { "manager": "kustomize-controller", "operation": "Apply",
                                 "fieldsV1": { "f:spec": { "f:floor": {}, "f:ceiling": {} } } } ],
            "ownerReferences": [ { "kind": "Kustomization", "name": "breathe-bands", "uid": "u" } ],
            "annotations": { project::LAST_APPLIED_ANNOTATION: "{\"spec\":{\"floor\":\"256Mi\"}}" },
        })
    }

    fn meta(name: &str) -> Value {
        let mut m = bookkeeping();
        m["name"] = json!(name);
        m["namespace"] = json!("camelot");
        m
    }

    #[derive(Default)]
    struct MockStore {
        patches: Mutex<Vec<(String, Value)>>,
    }
    #[async_trait]
    impl BreatheStore for MockStore {
        async fn list_bands(&self, kind: DimensionId, _ns: Option<String>) -> Result<Value, StoreError> {
            // The "memory" dimension carries one band referencing "platform-default"
            // and one that doesn't — real fixture data for
            // `breathe_posture_get`'s live-computed `referencingBands`.
            if kind == DimensionId::Memory {
                return Ok(json!([
                    { "kind": kind.as_str(), "metadata": meta("app-mem"),
                      "spec": { "postureRef": "platform-default", "floor": "256Mi", "ceiling": "4Gi",
                                "targetRef": { "kind": "Deployment", "name": "app" } },
                      "status": { "phase": "AtSetpoint", "currentLimit": "1Gi", "observedUtil": 0.78,
                                  "conditions": [ { "type": "Ready", "status": "True", "reason": "Ok",
                                                    "message": "held", "lastTransitionTime": "2026-07-27T00:00:00Z",
                                                    "observedGeneration": 7 } ],
                                  "history": [ { "time": "t1", "phase": "Grow" }, { "time": "t2", "phase": "Hold" },
                                               { "time": "t3", "phase": "Hold" }, { "time": "t4", "phase": "Hold" },
                                               { "time": "t5", "phase": "AtSetpoint" } ] } },
                    { "kind": kind.as_str(), "metadata": meta("other-mem"), "spec": {} },
                ]));
            }
            Ok(json!([{ "kind": kind.as_str(), "metadata": meta("b") }]))
        }
        async fn get_band(&self, kind: DimensionId, ns: String, name: String) -> Result<Value, StoreError> {
            let mut m = bookkeeping();
            m["name"] = json!(name);
            m["namespace"] = json!(ns);
            Ok(json!({
                "kind": kind.as_str(), "metadata": m,
                "spec": { "floor": "256Mi", "ceiling": "4Gi", "targetRef": { "kind": "Deployment", "name": "app" } },
                "status": { "phase": "AtSetpoint",
                            "history": [ { "time": "t1" }, { "time": "t2" }, { "time": "t3" },
                                         { "time": "t4" }, { "time": "t5" } ] },
            }))
        }
        async fn patch_band_spec(&self, _k: DimensionId, _ns: String, name: String, spec: Value) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, spec.clone()));
            Ok(json!({ "spec": spec }))
        }
        async fn annotate_band(&self, _k: DimensionId, _ns: String, name: String, ann: Value) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, ann.clone()));
            Ok(json!({ "metadata": { "annotations": ann } }))
        }
        async fn list_pools(&self) -> Result<Value, StoreError> {
            Ok(json!([{ "kind": "BreatheNodePool", "metadata": meta("rio"), "spec": { "writeEnabled": true } }]))
        }
        async fn get_pool(&self, name: String) -> Result<Value, StoreError> {
            Ok(json!({ "kind": "BreatheNodePool", "metadata": meta(&name), "spec": { "writeEnabled": true } }))
        }
        async fn patch_pool_spec(&self, name: String, spec: Value) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, spec.clone()));
            Ok(json!({ "spec": spec }))
        }
        async fn list_postures(&self) -> Result<Value, StoreError> {
            Ok(json!([{ "kind": "BreathePosture", "metadata": meta("platform-default"), "spec": { "setpoint": 0.8 } }]))
        }
        async fn get_posture(&self, name: String) -> Result<Value, StoreError> {
            Ok(json!({ "kind": "BreathePosture", "metadata": meta(&name), "spec": { "setpoint": 0.8 } }))
        }
        async fn patch_posture_spec(&self, name: String, spec: Value) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, spec.clone()));
            Ok(json!({ "spec": spec }))
        }
        fn catalog(&self) -> Value {
            breathe_facade::catalog_json()
        }
    }

    /// **The D3 row.** `breathe_band_set_dry_run` used to patch `dryRun` on any
    /// kind and report success; on every kind but those two that patch changed nothing,
    /// so the operator was told a safety gate had been applied when none had.
    /// It now refuses on exactly those kinds, writes nothing, and names the
    /// replacement.
    #[tokio::test]
    async fn set_dry_run_refuses_on_every_kind_that_does_not_read_it() {
        let mock = Arc::new(MockStore::default());
        let mcp = BreatheMcp::new(mock.clone());
        for kind in DimensionId::ALL.into_iter().filter(|k| !k.dry_run_is_honored()) {
            let out = mcp
                .breathe_band_set_dry_run(Parameters(SetDryRunInput {
                    kind,
                    namespace: "camelot".into(),
                    name: "b".into(),
                    dry_run: true,
                }))
                .await;
            let v: Value = serde_json::from_str(&out).unwrap();
            assert_eq!(v["wroteNothing"], true, "{kind} must not be told the patch landed");
            assert_eq!(v["kind"], kind.as_str());
            assert!(v["useInstead"].as_str().unwrap().contains("write_intent"), "{kind} must name the replacement");
        }
        assert!(
            mock.patches.lock().unwrap().is_empty(),
            "a refused set_dry_run must reach the store zero times — the old bug was a real patch that did nothing"
        );
    }

    /// …and it still WORKS on the two kinds that genuinely read `dryRun`. A
    /// blanket "dryRun is retired" would have been a second, opposite lie.
    #[tokio::test]
    async fn set_dry_run_still_applies_on_the_two_param_kinds() {
        for kind in [DimensionId::HostParam, DimensionId::KubeParam] {
            let mock = Arc::new(MockStore::default());
            let mcp = BreatheMcp::new(mock.clone());
            mcp.breathe_band_set_dry_run(Parameters(SetDryRunInput {
                kind,
                namespace: "camelot".into(),
                name: "b".into(),
                dry_run: true,
            }))
            .await;
            assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "dryRun": true }), "{kind} does read dryRun");
        }
    }

    #[tokio::test]
    async fn set_write_intent_writes_the_authored_intent() {
        let mock = Arc::new(MockStore::default());
        let mcp = BreatheMcp::new(mock.clone());
        mcp.breathe_band_set_write_intent(Parameters(SetWriteIntentInput {
            kind: DimensionId::Cpu,
            namespace: "camelot".into(),
            name: "coredns-cpu".into(),
            intent: IntentKind::Observe,
            confirm_after_seconds: None,
            authorized_by: None,
        }))
        .await;
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "writeIntent": { "intent": "observe" } }));
    }

    /// An unattributed go-live is refused at the surface, before it becomes a CR
    /// the controller would only ever resolve to a shadow anyway.
    #[tokio::test]
    async fn set_write_intent_refuses_an_unattributed_go_live() {
        let mock = Arc::new(MockStore::default());
        let mcp = BreatheMcp::new(mock.clone());
        for author in [None, Some(String::new()), Some("   ".into())] {
            let out = mcp
                .breathe_band_set_write_intent(Parameters(SetWriteIntentInput {
                    kind: DimensionId::Cpu,
                    namespace: "camelot".into(),
                    name: "b".into(),
                    intent: IntentKind::Write,
                    confirm_after_seconds: None,
                    authorized_by: author.clone(),
                }))
                .await;
            let v: Value = serde_json::from_str(&out).unwrap();
            assert_eq!(v["wroteNothing"], true, "authorizedBy={author:?} must not go live");
        }
        assert!(mock.patches.lock().unwrap().is_empty());
        // …and an attributed one does land, carrying the author verbatim.
        mcp.breathe_band_set_write_intent(Parameters(SetWriteIntentInput {
            kind: DimensionId::Cpu,
            namespace: "camelot".into(),
            name: "b".into(),
            intent: IntentKind::Write,
            confirm_after_seconds: None,
            authorized_by: Some("drzzln 2026-07-26 rightsizing".into()),
        }))
        .await;
        assert_eq!(
            mock.patches.lock().unwrap()[0].1,
            json!({ "writeIntent": { "intent": "write", "authorizedBy": "drzzln 2026-07-26 rightsizing" } })
        );
    }

    /// The operator fast-path is finally reachable from a surface, and clearing
    /// it REMOVES the key (merge-patch null) rather than setting "false", which
    /// the controller reads as unconfirmed either way but which would leave a
    /// misleading annotation behind.
    #[tokio::test]
    async fn confirm_sets_and_clears_the_operator_annotation() {
        let mock = Arc::new(MockStore::default());
        let mcp = BreatheMcp::new(mock.clone());
        let mk = |confirmed| ConfirmBandInput {
            kind: DimensionId::Memory,
            namespace: "camelot".into(),
            name: "b".into(),
            confirmed,
        };
        mcp.breathe_band_confirm(Parameters(mk(true))).await;
        mcp.breathe_band_confirm(Parameters(mk(false))).await;
        let p = mock.patches.lock().unwrap();
        assert_eq!(p[0].1, json!({ breathe_provider::CONFIRMED_ANNOTATION: "true" }));
        assert_eq!(p[1].1, json!({ breathe_provider::CONFIRMED_ANNOTATION: Value::Null }));
    }

    /// Every dimension is addressable from this surface. The five that were
    /// unreachable under the old five-arm enum are named explicitly, because
    /// that is the regression this guards.
    #[tokio::test]
    async fn every_shipped_dimension_is_reachable() {
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        for kind in DimensionId::ALL {
            let out = mcp
                .breathe_band_list(Parameters(ListBandsInput {
                    kind,
                    namespace: None,
                    view: None,
                    history_limit: None,
                }))
                .await;
            assert!(out.contains(kind.as_str()), "{kind} must be listable");
        }
        for once_invisible in ["cgroup-cpu", "host-param", "kube-param", "app-param", "replica"] {
            assert!(DimensionId::parse(once_invisible).is_some());
        }
    }

    /// **The cross-crate contract.** The surface hand-builds the `writeIntent`
    /// JSON body, so nothing else proves the key spellings (`confirmAfterSeconds`,
    /// `authorizedBy` — camelCase, not snake) match what `WriteIntentSpec`
    /// actually deserializes. A typo here would produce a CR the apiserver accepts
    /// and the controller reads as "no intent authored", silently falling back to
    /// the legacy chain — the same silent-inertness class as the root defect.
    #[tokio::test]
    async fn every_emitted_write_intent_parses_back_into_the_algebra() {
        use breathe_provider::{WriteIntent, WriteIntentSpec};
        let cases = [
            (IntentKind::Observe, None, None, WriteIntent::Observe),
            (IntentKind::Frozen, None, None, WriteIntent::Frozen),
            (
                IntentKind::CalibrateThenWrite,
                Some(900),
                None,
                WriteIntent::CalibrateThenWrite { confirm_after_seconds: 900 },
            ),
            (
                IntentKind::Write,
                None,
                Some("drzzln 2026-07-26".to_string()),
                WriteIntent::Write { authorized_by: "drzzln 2026-07-26".into() },
            ),
        ];
        for (intent, secs, by, expected) in cases {
            let mock = Arc::new(MockStore::default());
            let mcp = BreatheMcp::new(mock.clone());
            mcp.breathe_band_set_write_intent(Parameters(SetWriteIntentInput {
                kind: DimensionId::Memory,
                namespace: "camelot".into(),
                name: "b".into(),
                intent,
                confirm_after_seconds: secs,
                authorized_by: by,
            }))
            .await;
            let emitted = mock.patches.lock().unwrap()[0].1["writeIntent"].clone();
            let spec: WriteIntentSpec =
                serde_json::from_value(emitted.clone()).unwrap_or_else(|e| panic!("{emitted} must parse: {e}"));
            assert_eq!(spec.parse().unwrap(), expected, "emitted {emitted} resolved to the wrong intent");
        }
    }

    /// The intent tool-input schema offers exactly the canonical arms — the MCP
    /// cannot advertise a value the CRD would reject, or omit one it accepts.
    #[test]
    fn intent_schema_offers_exactly_the_canonical_arms() {
        let mut generator = schemars::SchemaGenerator::default();
        let schema = serde_json::to_value(intent_schema(&mut generator)).unwrap();
        let offered: Vec<&str> = schema["enum"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
        let canonical: Vec<&str> = IntentKind::ALL.iter().map(|k| k.as_str()).collect();
        assert_eq!(offered, canonical);
    }

    // REMOVED: `set_dry_run_patches_the_dry_run_spec_field`, which asserted that
    // setting dryRun on an ArcBand patches `spec.dryRun`. It did — and that was
    // the bug: the patch landed and changed nothing, because ArcBand has not read
    // dryRun since 2026-06-19. The test encoded the false model rather than the
    // behavior, so it stayed green through the whole defect. Its honest successors
    // are the two `set_dry_run_*` rows above, which split the kinds by whether
    // the field is actually read.

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
        assert!(info.instructions.unwrap().contains("spec.writeIntent"));
    }

    #[tokio::test]
    async fn posture_list_returns_the_store_output() {
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        let out = mcp.breathe_posture_list(Parameters(ViewInput { view: None })).await;
        assert!(out.contains("platform-default"));
    }

    #[tokio::test]
    async fn posture_get_surfaces_the_live_computed_referencing_bands() {
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        let out = mcp
            .breathe_posture_get(Parameters(PostureRef { name: "platform-default".into(), view: None }))
            .await;
        let v: Value = serde_json::from_str(&out).unwrap();
        // The posture arrives PROJECTED — `metadata.name` is lifted to `name`,
        // and the apiserver bookkeeping around it is gone.
        assert_eq!(v["posture"]["name"], "platform-default");
        assert_eq!(v["posture"]["spec"]["setpoint"], json!(0.8), "a posture's own spec is kept verbatim");
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

    // ─────────────────── the projection, at the tool boundary ───────────────────
    //
    // `breathe-facade::project` has its own unit tests over a typed CR fixture.
    // These are the DIFFERENT claim: that every read tool on this surface actually
    // routes through it. A projection nobody calls is the vacuous-guard failure
    // mode — a mechanism reporting success having evaluated nothing.

    /// Every read tool, enumerated. The rows are written out rather than derived
    /// so that adding a read tool without projecting it is a visible omission
    /// here, not an invisible one.
    async fn every_read_tool_output(mcp: &BreatheMcp) -> Vec<(&'static str, String)> {
        vec![
            (
                "breathe_band_list",
                mcp.breathe_band_list(Parameters(ListBandsInput {
                    kind: DimensionId::Memory,
                    namespace: None,
                    view: None,
                    history_limit: None,
                }))
                .await,
            ),
            (
                "breathe_band_get",
                mcp.breathe_band_get(Parameters(BandRef {
                    kind: DimensionId::Memory,
                    namespace: "camelot".into(),
                    name: "app-mem".into(),
                    view: None,
                    history_limit: None,
                }))
                .await,
            ),
            ("breathe_nodepool_list", mcp.breathe_nodepool_list(Parameters(ViewInput { view: None })).await),
            (
                "breathe_nodepool_get",
                mcp.breathe_nodepool_get(Parameters(PoolRef { name: "rio".into(), view: None })).await,
            ),
            ("breathe_posture_list", mcp.breathe_posture_list(Parameters(ViewInput { view: None })).await),
            (
                "breathe_posture_get",
                mcp.breathe_posture_get(Parameters(PostureRef { name: "platform-default".into(), view: None })).await,
            ),
        ]
    }

    /// **The defect.** A 52-band `breathe_band_list` returned 428,850 bytes of
    /// mostly-`managedFields` and overflowed the caller's result limit. No read
    /// tool's DEFAULT output may carry apiserver bookkeeping — including the two
    /// posture tools and the two nodepool tools, which had the same shape and were
    /// checked rather than assumed.
    #[tokio::test]
    async fn no_read_tool_returns_apiserver_bookkeeping_by_default() {
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        for (tool, out) in every_read_tool_output(&mcp).await {
            assert!(!out.contains("managedFields"), "{tool} returned managedFields by default");
            assert!(!out.contains("ownerReferences"), "{tool} returned ownerReferences by default");
            assert!(!out.contains("last-applied-configuration"), "{tool} returned the kubectl spec echo by default");
            // Not an empty answer, either — a tool that returned nothing would
            // also pass the three assertions above.
            assert!(out.len() > 40, "{tool} returned suspiciously little: {out}");
        }
    }

    /// The escape hatch is real: `view=raw` gives back the bytes the apiserver
    /// sent, bookkeeping included. This is a projection, not a capability removal
    /// — breathe's OWN single-writer guard reads `managedFields`, so making it
    /// unreachable would have been a regression dressed as a fix.
    #[tokio::test]
    async fn view_raw_still_reaches_every_byte() {
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        let out = mcp
            .breathe_band_list(Parameters(ListBandsInput {
                kind: DimensionId::Memory,
                namespace: None,
                view: Some(ProjectionView::Raw),
                history_limit: None,
            }))
            .await;
        assert!(out.contains("managedFields"), "view=raw must reach managedFields");
        assert!(out.contains("ownerReferences"));
        assert!(out.contains("last-applied-configuration"));

        // …and `full` is the middle rung: the whole CR, minus only the three
        // bookkeeping keys.
        let full = mcp
            .breathe_band_get(Parameters(BandRef {
                kind: DimensionId::Memory,
                namespace: "camelot".into(),
                name: "app-mem".into(),
                view: Some(ProjectionView::Full),
                history_limit: None,
            }))
            .await;
        let v: Value = serde_json::from_str(&full).unwrap();
        assert_eq!(v["metadata"].get("managedFields"), None);
        assert_eq!(v["spec"]["floor"], json!("256Mi"), "full keeps the whole spec");
        assert_eq!(v["status"]["history"].as_array().unwrap().len(), 5, "full does not bound history");
    }

    /// `status.history` is bounded on the compact views, and the bound is
    /// operator-settable.
    #[tokio::test]
    async fn band_get_bounds_history_and_honours_an_explicit_limit() {
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        let get = |limit| {
            let mcp = mcp.clone();
            async move {
                let out = mcp
                    .breathe_band_get(Parameters(BandRef {
                        kind: DimensionId::Memory,
                        namespace: "camelot".into(),
                        name: "app-mem".into(),
                        view: None, // default = detail
                        history_limit: limit,
                    }))
                    .await;
                serde_json::from_str::<Value>(&out).unwrap()
            }
        };
        let default = get(None).await;
        assert_eq!(default["view"], json!("detail"), "naming ONE band defaults to the richer view");
        let kept = default["band"]["history"].as_array().unwrap();
        assert_eq!(kept.len(), DEFAULT_HISTORY_LIMIT);
        assert_eq!(kept.last().unwrap()["time"], json!("t5"), "the NEWEST samples, not the oldest");
        assert_eq!(default["band"]["truncated"]["historyTotal"], json!(5), "it must say what it dropped");

        let widened = get(Some(5)).await;
        assert_eq!(widened["band"]["history"].as_array().unwrap().len(), 5);
        assert_eq!(widened["band"].get("truncated"), None, "nothing dropped ⇒ nothing to report");
    }

    /// A list answers the question a list is asked, in the fields an operator
    /// acts on — the projection is a reduction, not an amputation.
    #[tokio::test]
    async fn the_summary_list_still_answers_what_are_the_bands() {
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        let out = mcp
            .breathe_band_list(Parameters(ListBandsInput {
                kind: DimensionId::Memory,
                namespace: None,
                view: None,
                history_limit: None,
            }))
            .await;
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["dimension"], json!("memory"));
        assert_eq!(v["view"], json!("summary"));
        assert_eq!(v["count"], json!(2));
        let b = &v["bands"][0];
        assert_eq!(b["name"], json!("app-mem"));
        assert_eq!(b["namespace"], json!("camelot"));
        assert_eq!(b["target"]["name"], json!("app"));
        assert_eq!(b["floor"], json!("256Mi"));
        assert_eq!(b["ceiling"], json!("4Gi"));
        assert_eq!(b["currentLimit"], json!("1Gi"));
        assert_eq!(b["util"], json!(0.78));
        assert_eq!(b["phase"], json!("AtSetpoint"));
        // …and the envelope says how to get more, once, rather than per band.
        assert!(v["hint"].as_str().unwrap().contains("view=\"raw\"") || v["hint"].as_str().unwrap().contains("raw"));
    }

    /// **The schema budget, at this surface.** `DimensionId` is an input to six
    /// tools here and the manifest is re-sent per registration, so a re-inlined
    /// doc essay is paid for many times over. `breathe-provider`'s
    /// `dimension_id_schema_stays_small` caps the type; this caps what this crate
    /// actually ships, which is the number a caller pays.
    #[test]
    fn the_tool_manifest_stays_within_budget() {
        // 27,146 today (was 38,127), with ~5% headroom. Not "whatever it is now
        // plus a lot" — a budget with slack for a whole new bloated field is not
        // a budget.
        const BUDGET: usize = 28_500;
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        let tools = mcp.tool_router.list_all();
        let mut bytes = 0;
        for t in &tools {
            let n = serde_json::to_string(t).expect("a tool serializes").len();
            bytes += n;
            println!("TOOL {:44} {n:>7} bytes", t.name);
        }
        println!("TOOL MANIFEST {} tools, {bytes} bytes", tools.len());
        assert!(
            bytes <= BUDGET,
            "the tool manifest is {bytes} bytes, over the {BUDGET}-byte budget. It was 38,127 before the \
             DimensionId doc-comments stopped being inlined into every tool schema; a long #[doc] on a new \
             tool input or dimension arm is the usual cause. Add #[schemars(description = \"one sentence\")]."
        );
        assert!(tools.len() >= 13, "a tool disappearing must not be how this budget gets met");
    }
}
