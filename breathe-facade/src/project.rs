//! `project` — the typed, bounded projection every surface hands back instead of
//! the raw CR.
//!
//! # Why this exists
//!
//! A `breathe_band_list(kind="memory")` against camelot-eks returned **428,850
//! bytes / 13,055 lines** — 52 `MemoryBand` CRs at ~8.2 KB each, larger than the
//! whole org `CLAUDE.md`. It overflowed the caller's tool-result limit and spilled
//! to disk, which made the tool unusable for the one question it exists to answer:
//! *what are the memory bands, and are they healthy?*
//!
//! Almost none of those bytes were breathe's. The measured composition:
//!
//! | Payload | Present on | Value to a reader |
//! |---|---|---|
//! | `metadata.managedFields` | all 52 | none — pure apiserver bookkeeping |
//! | `metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"]` | any `kubectl apply`d CR | none — a second copy of the spec |
//! | `metadata.ownerReferences` | Flux/Helm-owned CRs | none for a band question |
//! | `status.history` | all | the last few samples, not the whole series |
//! | `status.conditions` | all | the current one per type |
//!
//! Two corrections to how that defect was first described, because a wrong cause
//! leads to a wrong fix:
//!
//! * **`status.history` is not unbounded.** `breathe_runtime`'s `HISTORY_MAX`
//!   already caps it at 16 samples and drains the oldest from the front, which is
//!   consistent with the ~11.9 samples/band measured live. It is bounded and still
//!   too large for a 52-row list — so the fix is a *view-side* bound, not a
//!   controller-side one, and nothing in the controller needed changing.
//! * **`status.conditions` compresses far less than it looks.** breathe writes one
//!   condition per type (Ready / Converged / Throttled / Stale / Conflict / …), so
//!   "keep the newest per type" — implemented below and correct to have — typically
//!   drops *nothing*. What actually makes conditions affordable is that the default
//!   `Summary` view omits them entirely; the per-type collapse is a bound on the
//!   duplicate case, not the saving.
//!
//! The answer is a **projection, not a capability removal**: [`ProjectionView`]
//! keeps every byte reachable — `Full` strips only the three bookkeeping keys
//! above, `Raw` is what the apiserver returned, unmodified. The default is
//! compact because the compact shape is what is asked for ~always, not because
//! the rest is unimportant.
//!
//! # Placement
//!
//! Here, not in `breathe-mcp`, for the reason this crate exists at all: the REST,
//! gRPC and GraphQL surfaces read the same `BreatheStore` and would each grow
//! their own copy otherwise (Operating Principle #1 — solve it once, in one
//! place). A surface's whole job is to pick a [`ProjectionView`] and call
//! [`project_band_list`] / [`project_band`] / [`project_object_list`] /
//! [`project_object`].
//!
//! **`pending-projection: breathe-api-server`.** `breathe-mcp` consumes this;
//! the REST (`GET /api/v1/bands/:kind`), GraphQL (`bands`) and gRPC (`ListBands`)
//! handlers in `breathe-api-server` still `respond(store.list_bands(…))` with the
//! raw CR and carry the identical 8 KB-per-band payload. They were left untouched
//! deliberately — the change that introduced this module was scoped to the MCP
//! surface — but the fix there is now a one-line call per handler plus a `?view=`
//! query parameter, not a design. Stated here rather than left implicit so the
//! remaining half is a known gap and not a discovery.
//!
//! # Typed emission
//!
//! Every byte emitted here comes from `serde_json::to_value` over a `Serialize`
//! struct, or from a `serde_json::json!` value builder — the two typed serializer
//! surfaces ★★ TYPED EMISSION sanctions. There is no `format!()` in this module
//! and no string concatenation of JSON.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub use breathe_provider::DimensionId;

/// The `kubectl apply` spec echo — a verbatim second copy of the spec, carried in
/// an annotation, worth nothing to a reader who already has the spec.
pub const LAST_APPLIED_ANNOTATION: &str = "kubectl.kubernetes.io/last-applied-configuration";

/// `status.history` samples a `Detail` projection carries.
///
/// Three, because the question a trajectory answers for an agent is "which way is
/// this moving, right now" — the controller already caps the stored series at 16
/// (`breathe_runtime`'s `HISTORY_MAX`), and 16 × 52 bands is most of what made the
/// list unreadable.
pub const DEFAULT_HISTORY_LIMIT: usize = 3;

/// How much of a CR a surface hands back.
///
/// The ordering is a widening ladder: every view is a superset of the one before
/// it, so raising it never loses a field and lowering it never invents one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
// The description is ONE short line on purpose. This enum is inlined into six
// MCP tool schemas, and each tool's own `view` field already carries the longer
// operator-facing guidance — so anything said twice here is paid for twelve
// times. Same lesson as DimensionId, applied before it could become a defect.
#[schemars(
    description = "summary = compact typed view (default) | detail = + newest conditions and recent history \
                   | full = whole CR minus apiserver bookkeeping | raw = the untouched CR."
)]
pub enum ProjectionView {
    // No doc comments on the variants ON PURPOSE: schemars inlines a variant's
    // Rust doc into the wire schema verbatim, and four inlined paragraphs here
    // would reproduce, in miniature, the DimensionId schema bloat this same
    // change is fixing. The meaning lives in the enum-level description above,
    // which is written once instead of once per tool.
    #[default]
    Summary,
    Detail,
    Full,
    Raw,
}

impl ProjectionView {
    /// Every view, for surfaces that enumerate rather than hand-write a subset.
    pub const ALL: [Self; 4] = [Self::Summary, Self::Detail, Self::Full, Self::Raw];

    /// The wire token, identical to the serde rename.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Summary => "summary",
            Self::Detail => "detail",
            Self::Full => "full",
            Self::Raw => "raw",
        }
    }

    /// True for the two views that reduce a CR to a typed struct rather than
    /// handing back a (possibly stripped) CR.
    #[must_use]
    pub fn is_compact(self) -> bool {
        matches!(self, Self::Summary | Self::Detail)
    }
}

// ─────────────────────────── the typed views ───────────────────────────

/// The workload a band governs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TargetView {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_selector: Option<String>,
}

/// One `status.conditions` entry, minus `observedGeneration` (a controller
/// bookkeeping counter, not an answer to any operator question).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConditionView {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last_transition_time: String,
}

/// What a projection dropped, and how much of it there was.
///
/// Emitted only when something was actually dropped, so a projection that lost
/// nothing costs nothing to say so. Without this an agent cannot tell "this band
/// has no history" from "this view hid the history" — which is the vacuous-guard
/// failure mode (`UNREPRESENTABILITY.md` §II.3) applied to a response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TruncationView {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub history_kept: usize,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub history_total: usize,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub conditions_kept: usize,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub conditions_total: usize,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(n: &usize) -> bool {
    *n == 0
}

/// The compact typed view of one band CR.
///
/// Every field answers a question an operator or agent actually asks of a band;
/// nothing here is apiserver bookkeeping. `None`/empty fields serialize away, so
/// a freshly-created band that has never reconciled projects to a handful of
/// bytes rather than a page of nulls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BandView {
    /// The dimension this band carves (`memory`, `cpu`, …).
    pub dimension: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<TargetView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub floor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ceiling: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setpoint: Option<f64>,
    /// The limit breathe currently holds (`status.currentLimit`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_limit: Option<String>,
    /// `status.observedUtil` — the ratio that drove the last tick.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub util: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_decision: Option<String>,
    /// The authored `spec.writeIntent.intent`, when one exists. Absent means the
    /// band rests on the legacy resolution chain — which `effectiveGate.witness`
    /// reports as `legacyDefault`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_intent: Option<String>,
    /// `status.effectiveGate`, carried VERBATIM — it is already small and it is
    /// the one field that answers "is this band writing, and why", so a
    /// re-typing here could only lose a field the controller added.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_gate: Option<Value>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suspended: bool,
    /// `Detail` only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<ConditionView>>,
    /// `Detail` only — the last [`DEFAULT_HISTORY_LIMIT`] `status.history`
    /// samples, verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated: Option<TruncationView>,
}

// ─────────────────────────── the projections ───────────────────────────

/// Remove the three pure-bookkeeping metadata keys from a CR, in place.
///
/// This is the ONLY thing [`ProjectionView::Full`] does to a CR: every key an
/// operator could act on survives. All three carry zero information a reader does
/// not already have — `managedFields` is the apiserver's field-ownership ledger,
/// `ownerReferences` is the Flux/Helm parent link, and the
/// `last-applied-configuration` annotation is a verbatim second copy of the spec
/// printed right below it.
///
/// `managedFields` genuinely matters in exactly one place — breathe's own
/// single-writer guard parses it — which is why [`ProjectionView::Raw`] exists
/// and this function is never applied to it.
pub fn strip_bookkeeping(cr: &mut Value) {
    let Some(meta) = cr.get_mut("metadata").and_then(Value::as_object_mut) else { return };
    meta.remove("managedFields");
    meta.remove("ownerReferences");
    if let Some(ann) = meta.get_mut("annotations").and_then(Value::as_object_mut) {
        ann.remove(LAST_APPLIED_ANNOTATION);
        if ann.is_empty() {
            meta.remove("annotations");
        }
    }
}

fn s(v: &Value, path: &[&str]) -> Option<String> {
    walk(v, path).and_then(Value::as_str).map(ToOwned::to_owned)
}
fn f(v: &Value, path: &[&str]) -> Option<f64> {
    walk(v, path).and_then(Value::as_f64)
}
fn walk<'a>(v: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut cur = v;
    for key in path {
        cur = cur.get(key)?;
    }
    Some(cur)
}

/// Keep the most recent condition per `type`, in first-seen type order.
///
/// Ordering is by `lastTransitionTime`, which the controller writes as RFC3339
/// UTC — a fixed-width, `Z`-suffixed encoding whose lexicographic order IS its
/// chronological order, so a string compare is exact here rather than an
/// approximation. Duplicate types are not expected from breathe's own controller
/// (it writes one per type); this bounds the case where they appear anyway.
#[must_use]
pub fn newest_condition_per_type(conditions: &[Value]) -> Vec<ConditionView> {
    let mut out: Vec<ConditionView> = Vec::new();
    for c in conditions {
        let view = ConditionView {
            type_: s(c, &["type"]).unwrap_or_default(),
            status: s(c, &["status"]).unwrap_or_default(),
            reason: s(c, &["reason"]).unwrap_or_default(),
            message: s(c, &["message"]).unwrap_or_default(),
            last_transition_time: s(c, &["lastTransitionTime"]).unwrap_or_default(),
        };
        match out.iter_mut().find(|k| k.type_ == view.type_) {
            Some(prev) if view.last_transition_time > prev.last_transition_time => *prev = view,
            Some(_) => {}
            None => out.push(view),
        }
    }
    out
}

/// Project one band CR into [`BandView`].
///
/// `history_limit` bounds `status.history` to its most recent N samples (the
/// controller appends, so the tail is newest); `0` drops the series entirely.
#[must_use]
pub fn band_view(dimension: DimensionId, cr: &Value, view: ProjectionView, history_limit: usize) -> BandView {
    let history_all: &[Value] = walk(cr, &["status", "history"]).and_then(Value::as_array).map_or(&[], Vec::as_slice);
    let conditions_all: &[Value] =
        walk(cr, &["status", "conditions"]).and_then(Value::as_array).map_or(&[], Vec::as_slice);

    let mut out = BandView {
        dimension: dimension.as_str().to_owned(),
        name: s(cr, &["metadata", "name"]).unwrap_or_default(),
        namespace: s(cr, &["metadata", "namespace"]),
        target: walk(cr, &["spec", "targetRef"]).map(|t| TargetView {
            kind: s(t, &["kind"]).unwrap_or_default(),
            name: s(t, &["name"]).unwrap_or_default(),
            container: s(t, &["container"]),
            pod_selector: s(t, &["podSelector"]),
        }),
        floor: s(cr, &["spec", "floor"]),
        ceiling: s(cr, &["spec", "ceiling"]),
        setpoint: f(cr, &["spec", "setpoint"]),
        current_limit: s(cr, &["status", "currentLimit"]),
        util: f(cr, &["status", "observedUtil"]),
        phase: s(cr, &["status", "phase"]),
        health: s(cr, &["status", "health"]),
        last_decision: s(cr, &["status", "lastDecision"]),
        write_intent: s(cr, &["spec", "writeIntent", "intent"]),
        effective_gate: walk(cr, &["status", "effectiveGate"]).cloned(),
        suspended: walk(cr, &["spec", "suspend"]).and_then(Value::as_bool).unwrap_or(false),
        conditions: None,
        history: None,
        truncated: None,
    };

    if view == ProjectionView::Detail {
        let kept_conditions = newest_condition_per_type(conditions_all);
        let start = history_all.len().saturating_sub(history_limit);
        let kept_history: Vec<Value> = history_all[start..].to_vec();
        let truncation = TruncationView {
            history_kept: kept_history.len(),
            history_total: history_all.len(),
            conditions_kept: kept_conditions.len(),
            conditions_total: conditions_all.len(),
        };
        // Say what was dropped, and only when something was.
        if truncation.history_kept < truncation.history_total || truncation.conditions_kept < truncation.conditions_total
        {
            out.truncated = Some(truncation);
        }
        out.conditions = Some(kept_conditions);
        out.history = Some(kept_history);
    }
    out
}

/// The hint every compact envelope carries — one string per response, never one
/// per band, so it costs bytes proportional to the call and not to the fleet.
///
/// It deliberately describes the dropped keys by ROLE rather than by name. The
/// exact key names belong in the tool schema (where an agent reads them once),
/// not in every response body — and naming them here made a substring assertion
/// like `!response.contains("managedFields")` pass or fail on the wording of a
/// help string rather than on whether a CR actually leaked.
const VIEW_HINT: &str = "compact projection — pass view:\"detail\" for the newest conditions + last few history \
                         samples, view:\"full\" for the whole CR minus apiserver bookkeeping (field-ownership \
                         metadata, owner links, the kubectl spec echo), view:\"raw\" for the untouched CR";

/// Project the `list_bands` payload.
///
/// A non-array payload (only reachable if the store's contract changes) is handed
/// back untouched under `raw`, rather than silently projected to an empty list —
/// dropping data on an unexpected shape is exactly the silent-wrong-answer this
/// module exists to avoid.
#[must_use]
pub fn project_band_list(dimension: DimensionId, list: &Value, view: ProjectionView, history_limit: usize) -> Value {
    if view == ProjectionView::Raw {
        return list.clone();
    }
    let Some(items) = list.as_array() else {
        let mut raw = list.clone();
        if view == ProjectionView::Full {
            strip_bookkeeping(&mut raw);
        }
        return raw;
    };
    if view == ProjectionView::Full {
        let stripped: Vec<Value> = items
            .iter()
            .map(|cr| {
                let mut c = cr.clone();
                strip_bookkeeping(&mut c);
                c
            })
            .collect();
        return json!({ "dimension": dimension.as_str(), "view": view.as_str(), "count": stripped.len(), "bands": stripped });
    }
    let bands: Vec<BandView> = items.iter().map(|cr| band_view(dimension, cr, view, history_limit)).collect();
    json!({
        "dimension": dimension.as_str(),
        "view": view.as_str(),
        "count": bands.len(),
        "bands": bands,
        "hint": VIEW_HINT,
    })
}

/// Project the `get_band` payload.
#[must_use]
pub fn project_band(dimension: DimensionId, cr: &Value, view: ProjectionView, history_limit: usize) -> Value {
    match view {
        ProjectionView::Raw => cr.clone(),
        ProjectionView::Full => {
            let mut c = cr.clone();
            strip_bookkeeping(&mut c);
            c
        }
        ProjectionView::Summary | ProjectionView::Detail => json!({
            "dimension": dimension.as_str(),
            "view": view.as_str(),
            "band": band_view(dimension, cr, view, history_limit),
            "hint": VIEW_HINT,
        }),
    }
}

// ───────────────── the generic (pool / posture) projection ─────────────────

/// The compact view of a CR whose spec + status are already small — a
/// `BreatheNodePool`, a `BreathePosture`.
///
/// These carry the SAME apiserver bookkeeping as a band (a Flux-managed
/// `BreathePosture` arrives with `managedFields`, `ownerReferences` and a
/// `last-applied-configuration` echo), but their authored content is a handful of
/// scalars — so the right projection keeps `spec` and `status` VERBATIM and drops
/// only the metadata. Re-typing their fields here would add a second place to
/// forget one, for no byte saving.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ObjectView {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<Value>,
}

/// Project one pool/posture CR.
#[must_use]
pub fn object_view(cr: &Value) -> ObjectView {
    ObjectView {
        name: s(cr, &["metadata", "name"]).unwrap_or_default(),
        namespace: s(cr, &["metadata", "namespace"]),
        kind: s(cr, &["kind"]),
        labels: walk(cr, &["metadata", "labels"]).cloned(),
        spec: cr.get("spec").cloned(),
        status: cr.get("status").cloned(),
    }
}

/// Project a `list_pools` / `list_postures` payload.
#[must_use]
pub fn project_object_list(list: &Value, view: ProjectionView) -> Value {
    if view == ProjectionView::Raw {
        return list.clone();
    }
    let Some(items) = list.as_array() else {
        let mut raw = list.clone();
        strip_bookkeeping(&mut raw);
        return raw;
    };
    if view == ProjectionView::Full {
        let stripped: Vec<Value> = items
            .iter()
            .map(|cr| {
                let mut c = cr.clone();
                strip_bookkeeping(&mut c);
                c
            })
            .collect();
        return json!({ "view": view.as_str(), "count": stripped.len(), "items": stripped });
    }
    let items: Vec<ObjectView> = items.iter().map(object_view).collect();
    json!({ "view": view.as_str(), "count": items.len(), "items": items, "hint": VIEW_HINT })
}

/// Project a `get_pool` / `get_posture` payload.
#[must_use]
pub fn project_object(cr: &Value, view: ProjectionView) -> Value {
    match view {
        ProjectionView::Raw => cr.clone(),
        ProjectionView::Full => {
            let mut c = cr.clone();
            strip_bookkeeping(&mut c);
            c
        }
        ProjectionView::Summary | ProjectionView::Detail => {
            serde_json::to_value(object_view(cr)).unwrap_or_else(|_| cr.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use breathe_crd::{BandStatus, Condition, MemoryBand, MemoryBandSpec, TrendSample};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ManagedFieldsEntry, ObjectMeta, OwnerReference, Time};
    use std::collections::BTreeMap;

    fn t(rfc3339: &str) -> Time {
        Time(rfc3339.parse().expect("fixture timestamp parses"))
    }

    /// A `MemoryBand` shaped like the ones camelot-eks actually returns:
    /// the CR body built from the REAL `breathe-crd` types (so no field name here
    /// can drift from the CRD), wrapped in the metadata a Flux-managed,
    /// kubectl-applied, twice-reconciled object carries.
    ///
    /// The `managedFields` entries are the real shape — one per field manager, each
    /// with a `fieldsV1` ownership tree — because that tree is the single largest
    /// contributor to the 8.2 KB-per-band payload and a token stand-in would make
    /// the before/after measurement a fiction.
    fn fixture_band(name: &str) -> Value {
        let fields_v1 = |keys: &[&str]| {
            let mut m = serde_json::Map::new();
            for k in keys {
                m.insert((*k).to_owned(), json!({}));
            }
            k8s_openapi::apimachinery::pkg::apis::meta::v1::FieldsV1(json!({
                "f:metadata": { "f:annotations": { ".": {}, "f:kubectl.kubernetes.io/last-applied-configuration": {} },
                                "f:labels": { ".": {}, "f:app.kubernetes.io/managed-by": {}, "f:kustomize.toolkit.fluxcd.io/name": {},
                                              "f:kustomize.toolkit.fluxcd.io/namespace": {} } },
                "f:spec": Value::Object(m),
            }))
        };
        let managed_fields = vec![
            ManagedFieldsEntry {
                api_version: Some("breathe.pleme.io/v1".into()),
                fields_type: Some("FieldsV1".into()),
                fields_v1: Some(fields_v1(&[
                    ".", "f:ceiling", "f:confirmAfterSeconds", "f:cooldownSeconds", "f:disruptionPolicy", "f:dryRun",
                    "f:floor", "f:growAbove", "f:growFactor", "f:maxStalenessSeconds", "f:postureRef",
                    "f:predictive", "f:predictiveLookaheadSeconds", "f:requestFloor", "f:setpoint", "f:shrinkBelow",
                    "f:shrinkFactor", "f:targetRef", "f:writeIntent",
                ])),
                manager: Some("kustomize-controller".into()),
                operation: Some("Apply".into()),
                subresource: None,
                time: Some(t("2026-07-27T04:11:02Z")),
            },
            ManagedFieldsEntry {
                api_version: Some("breathe.pleme.io/v1".into()),
                fields_type: Some("FieldsV1".into()),
                fields_v1: Some(fields_v1(&[".", "f:status"])),
                manager: Some("breathe-controller".into()),
                operation: Some("Update".into()),
                subresource: Some("status".into()),
                time: Some(t("2026-07-27T15:42:18Z")),
            },
        ];

        let mut labels = BTreeMap::new();
        labels.insert("app.kubernetes.io/managed-by".to_owned(), "flux".to_owned());
        labels.insert("kustomize.toolkit.fluxcd.io/name".to_owned(), "breathe-bands".to_owned());
        labels.insert("kustomize.toolkit.fluxcd.io/namespace".to_owned(), "flux-system".to_owned());

        let mut annotations = BTreeMap::new();
        annotations.insert(
            LAST_APPLIED_ANNOTATION.to_owned(),
            // A real echo is the serialized spec; length is what matters, and it
            // is generated from the same spec below rather than typed by hand.
            serde_json::to_string(&json!({
                "apiVersion": "breathe.pleme.io/v1", "kind": "MemoryBand",
                "metadata": { "name": name, "namespace": "camelot" },
                "spec": { "targetRef": { "kind": "Deployment", "name": name },
                          "floor": "256Mi", "ceiling": "4Gi", "setpoint": 0.8,
                          "growAbove": 0.9, "shrinkBelow": 0.6, "growFactor": 1.25, "shrinkFactor": 0.9,
                          "cooldownSeconds": 300, "maxStalenessSeconds": 120, "postureRef": "platform-default",
                          "writeIntent": { "intent": "write", "authorizedBy": "drzzln 2026-07-20 rightsizing" } },
            }))
            .expect("fixture spec echo serializes"),
        );
        annotations.insert("breathe.pleme.io/confirmed".to_owned(), "true".to_owned());

        // Built THROUGH the real `MemoryBandSpec` deserializer rather than as loose
        // JSON: every value here is type-checked against the shipped CRD type, and
        // every field left out picks up the same serde default a real CR would.
        let spec: MemoryBandSpec = serde_json::from_value(json!({
            "targetRef": { "kind": "Deployment", "name": name, "apiVersion": "apps/v1", "container": "app" },
            "postureRef": "platform-default",
            "setpoint": 0.8,
            "growAbove": 0.9,
            "shrinkBelow": 0.6,
            "growFactor": 1.25,
            "shrinkFactor": 0.9,
            "floor": "256Mi",
            "ceiling": "4Gi",
            "cooldownSeconds": 300,
            "maxStalenessSeconds": 120,
            "writeIntent": { "intent": "write", "authorizedBy": "drzzln 2026-07-20 rightsizing" },
        }))
        .expect("the fixture spec must parse into the REAL MemoryBandSpec");

        let mut band = MemoryBand::new(name, spec);
        band.metadata = ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some("camelot".to_owned()),
            uid: Some("6f1c9f4e-2b3a-4c7d-9a10-8e5b2d4f7c31".to_owned()),
            resource_version: Some("48211934".to_owned()),
            generation: Some(7),
            creation_timestamp: Some(t("2026-06-30T09:00:00Z")),
            labels: Some(labels),
            annotations: Some(annotations),
            managed_fields: Some(managed_fields),
            owner_references: Some(vec![OwnerReference {
                api_version: "kustomize.toolkit.fluxcd.io/v1".into(),
                kind: "Kustomization".into(),
                name: "breathe-bands".into(),
                uid: "0d2b7a51-9c44-4f18-b6ae-1f3c5d8e2a90".into(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..Default::default()
        };
        band.status = Some(BandStatus {
                phase: Some("AtSetpoint".into()),
                health: Some("Healthy".into()),
                last_util: Some("78%".into()),
                current_limit: Some("1Gi".into()),
                last_decision: Some("Hold".into()),
                observed_util: Some(0.783),
                observed_used: Some(840_531_968),
                observed_capacity: Some(1_073_741_824),
                observed_peak_used: Some(902_299_648),
                conditions: (0..7)
                    .map(|i| Condition {
                        type_: ["Ready", "Converged", "Throttled", "Stale", "Conflict", "TargetFound", "Supported"][i]
                            .to_owned(),
                        status: "True".into(),
                        reason: "ObservedHealthy".into(),
                        message: "the band observed a fresh metric and is holding the target at its setpoint".into(),
                        last_transition_time: "2026-07-27T15:42:18Z".into(),
                        observed_generation: Some(7),
                    })
                    .collect(),
                history: (0..12)
                    .map(|i| TrendSample {
                        time: "2026-07-27T15:42:18Z".into(),
                        util: Some(0.70 + f64::from(i) / 100.0),
                        limit: Some(1_073_741_824),
                        phase: "AtSetpoint".into(),
                        decision: Some("Hold".into()),
                    })
                    .collect(),
                ..Default::default()
            });
        serde_json::to_value(band).expect("a MemoryBand serializes")
    }

    fn fixture_list(n: usize) -> Value {
        Value::Array((0..n).map(|i| fixture_band(&["band-", &i.to_string()].concat())).collect())
    }

    // ── the defect ────────────────────────────────────────────────────────────

    /// The measurement this whole module exists for, run against the fixture so
    /// it is reproducible without a cluster. Printed, not just asserted, so a
    /// `--nocapture` run is the receipt.
    #[test]
    fn the_projection_is_a_large_measured_reduction() {
        let list = fixture_list(52);
        let raw = serde_json::to_string(&list).expect("list serializes").len();
        let summary = serde_json::to_string(&project_band_list(DimensionId::Memory, &list, ProjectionView::Summary, 3))
            .expect("summary serializes")
            .len();
        let detail = serde_json::to_string(&project_band_list(DimensionId::Memory, &list, ProjectionView::Detail, 3))
            .expect("detail serializes")
            .len();
        let full = serde_json::to_string(&project_band_list(DimensionId::Memory, &list, ProjectionView::Full, 3))
            .expect("full serializes")
            .len();
        println!("PROJECTION 52 memory bands: raw={raw} full={full} detail={detail} summary={summary}");
        println!("PROJECTION per band: raw={} summary={}", raw / 52, summary / 52);
        assert!(summary * 20 < raw, "summary must be at least a 20x reduction: {summary} vs {raw}");
        assert!(detail < raw / 2, "detail must still roughly halve the payload: {detail} vs {raw}");
        assert!(full < raw, "full must at least drop the bookkeeping: {full} vs {raw}");
    }

    /// Serialize the DATA half of a projected envelope — not the `hint`, which
    /// deliberately names the three stripped keys in prose so an agent knows what
    /// it is not being shown. Asserting on the envelope instead would make the
    /// leak test pass or fail on the wording of a help string.
    fn payload_bytes(envelope: &Value, key: &str) -> String {
        serde_json::to_string(envelope.get(key).unwrap_or(envelope)).expect("payload serializes")
    }

    /// The three bookkeeping keys are gone from every non-raw view — asserted on
    /// the SERIALIZED bytes, because "the struct has no field for it" and "the
    /// wire has no key for it" are different claims and only the second is what
    /// the caller pays for.
    #[test]
    fn no_projection_but_raw_emits_apiserver_bookkeeping() {
        let list = fixture_list(3);
        for view in [ProjectionView::Summary, ProjectionView::Detail, ProjectionView::Full] {
            let out = payload_bytes(&project_band_list(DimensionId::Memory, &list, view, 3), "bands");
            assert!(!out.contains("managedFields"), "{view:?} leaked managedFields");
            assert!(!out.contains("ownerReferences"), "{view:?} leaked ownerReferences");
            assert!(!out.contains(LAST_APPLIED_ANNOTATION), "{view:?} leaked the last-applied echo");
        }
        // …and raw is untouched, byte for byte — the escape hatch is real.
        let raw = project_band_list(DimensionId::Memory, &list, ProjectionView::Raw, 3);
        assert_eq!(raw, list, "raw must be the apiserver's own bytes");
        let raw_s = serde_json::to_string(&raw).expect("raw serializes");
        assert!(raw_s.contains("managedFields"), "raw must still carry managedFields — that is its whole job");
        assert!(raw_s.contains(LAST_APPLIED_ANNOTATION));
        assert!(raw_s.contains("ownerReferences"));
    }

    /// `full` keeps every authored field — it is a bookkeeping strip, not a
    /// projection. Guards against someone "helpfully" trimming spec fields there.
    #[test]
    fn full_keeps_every_authored_field() {
        let cr = fixture_band("b");
        let projected = project_band(DimensionId::Memory, &cr, ProjectionView::Full, 3);
        assert_eq!(projected["spec"], cr["spec"], "full must not touch spec");
        assert_eq!(projected["status"], cr["status"], "full must not touch status");
        assert_eq!(projected["metadata"]["uid"], cr["metadata"]["uid"]);
        assert_eq!(projected["metadata"]["resourceVersion"], cr["metadata"]["resourceVersion"]);
        // The one annotation that is NOT bookkeeping survives the strip.
        assert_eq!(projected["metadata"]["annotations"]["breathe.pleme.io/confirmed"], json!("true"));
    }

    // ── the bounds ────────────────────────────────────────────────────────────

    #[test]
    fn detail_bounds_history_to_the_newest_n_and_says_what_it_dropped() {
        let cr = fixture_band("b");
        let stored = cr["status"]["history"].as_array().expect("fixture has history").len();
        assert_eq!(stored, 12, "the fixture must carry more history than the limit, or this proves nothing");

        let v = band_view(DimensionId::Memory, &cr, ProjectionView::Detail, DEFAULT_HISTORY_LIMIT);
        let kept = v.history.as_ref().expect("detail carries history");
        assert_eq!(kept.len(), DEFAULT_HISTORY_LIMIT);
        // The NEWEST samples, i.e. the tail — the controller appends and drains
        // from the front (breathe_runtime's HISTORY_MAX), so newest is last.
        assert_eq!(kept, &cr["status"]["history"].as_array().expect("history")[stored - 3..].to_vec());
        let t = v.truncated.expect("a bounded projection must say it bounded something");
        assert_eq!(t.history_kept, 3);
        assert_eq!(t.history_total, 12);
    }

    #[test]
    fn a_history_limit_of_zero_drops_the_series_entirely() {
        let cr = fixture_band("b");
        let v = band_view(DimensionId::Memory, &cr, ProjectionView::Detail, 0);
        assert_eq!(v.history.expect("detail always names the series").len(), 0);
        assert_eq!(v.truncated.expect("dropping 12 samples is a truncation").history_total, 12);
    }

    #[test]
    fn a_projection_that_dropped_nothing_says_nothing() {
        let mut cr = fixture_band("b");
        cr["status"]["history"] = json!([]);
        cr["status"]["conditions"] = json!([]);
        let v = band_view(DimensionId::Memory, &cr, ProjectionView::Detail, 3);
        assert!(v.truncated.is_none(), "an empty series is not a truncation, and must not cost bytes to report");
    }

    #[test]
    fn summary_carries_no_history_or_conditions_at_all() {
        let cr = fixture_band("b");
        let v = band_view(DimensionId::Memory, &cr, ProjectionView::Summary, 3);
        assert!(v.history.is_none());
        assert!(v.conditions.is_none());
        // …and still answers the question a list is asked.
        assert_eq!(v.name, "b");
        assert_eq!(v.namespace.as_deref(), Some("camelot"));
        assert_eq!(v.target.expect("target").name, "b");
        assert_eq!(v.floor.as_deref(), Some("256Mi"));
        assert_eq!(v.ceiling.as_deref(), Some("4Gi"));
        assert_eq!(v.current_limit.as_deref(), Some("1Gi"));
        assert_eq!(v.phase.as_deref(), Some("AtSetpoint"));
        assert_eq!(v.util, Some(0.783));
    }

    #[test]
    fn conditions_collapse_to_the_newest_per_type() {
        let conditions = json!([
            { "type": "Ready", "status": "False", "reason": "Old",   "message": "stale", "lastTransitionTime": "2026-07-01T00:00:00Z" },
            { "type": "Ready", "status": "True",  "reason": "Fresh", "message": "ok",    "lastTransitionTime": "2026-07-27T00:00:00Z" },
            { "type": "Converged", "status": "True", "reason": "AtSetpoint", "message": "held", "lastTransitionTime": "2026-07-20T00:00:00Z" },
        ]);
        let kept = newest_condition_per_type(conditions.as_array().expect("array"));
        assert_eq!(kept.len(), 2, "two types in, two types out");
        assert_eq!(kept[0].type_, "Ready");
        assert_eq!(kept[0].reason, "Fresh", "the NEWEST Ready must win");
        assert_eq!(kept[1].type_, "Converged");
        // observedGeneration is a controller counter, never an operator answer.
        let s = serde_json::to_string(&kept).expect("serializes");
        assert!(!s.contains("observedGeneration"));
    }

    // ── the seams ─────────────────────────────────────────────────────────────

    #[test]
    fn an_unexpected_payload_shape_is_handed_back_rather_than_dropped() {
        // If the store's contract ever changes, a projection that silently
        // returned `[]` would be the worst possible failure: a confident empty
        // answer. It returns the payload instead.
        let odd = json!({ "unexpected": "shape", "metadata": { "managedFields": [1], "name": "x" } });
        let out = project_band_list(DimensionId::Cpu, &odd, ProjectionView::Summary, 3);
        assert_eq!(out["unexpected"], json!("shape"));
    }

    #[test]
    fn every_view_round_trips_its_wire_token() {
        for v in ProjectionView::ALL {
            let wire = serde_json::to_value(v).expect("serializes");
            assert_eq!(wire, json!(v.as_str()), "{v:?} serde label must equal as_str");
            assert_eq!(serde_json::from_value::<ProjectionView>(wire).expect("parses"), v);
        }
        assert_eq!(ProjectionView::default(), ProjectionView::Summary, "the cheap view is the default");
    }

    #[test]
    fn strip_bookkeeping_is_a_no_op_on_an_object_without_it() {
        let mut v = json!({ "metadata": { "name": "x" }, "spec": {} });
        let before = v.clone();
        strip_bookkeeping(&mut v);
        assert_eq!(v, before);
        // …and on a value with no metadata at all.
        let mut none = json!({ "spec": {} });
        strip_bookkeeping(&mut none);
        assert_eq!(none, json!({ "spec": {} }));
    }

    #[test]
    fn an_annotations_map_emptied_by_the_strip_is_removed_outright() {
        let mut v = json!({ "metadata": { "name": "x", "annotations": { LAST_APPLIED_ANNOTATION: "{...}" } } });
        strip_bookkeeping(&mut v);
        assert_eq!(v["metadata"].get("annotations"), None, "an empty annotations map is noise, not information");
    }

    // ── pools + postures ──────────────────────────────────────────────────────

    #[test]
    fn the_object_projection_strips_metadata_and_keeps_spec_and_status_verbatim() {
        let cr = json!({
            "kind": "BreatheNodePool",
            "metadata": {
                "name": "rio", "uid": "u", "resourceVersion": "9",
                "labels": { "a": "b" },
                "managedFields": [ { "manager": "kustomize-controller", "fieldsV1": { "f:spec": {} } } ],
                "ownerReferences": [ { "kind": "Kustomization", "name": "k" } ],
                "annotations": { LAST_APPLIED_ANNOTATION: "{...}", "keep": "me" },
            },
            "spec": { "nodeName": "rio", "writeEnabled": true },
            "status": { "phase": "Ready", "managedUnits": 4 },
        });
        let out = project_object(&cr, ProjectionView::Summary);
        assert_eq!(out["spec"], cr["spec"], "a pool spec is small and authored — keep it verbatim");
        assert_eq!(out["status"], cr["status"]);
        assert_eq!(out["name"], json!("rio"));
        assert_eq!(out["labels"], json!({ "a": "b" }));
        let s = serde_json::to_string(&out).expect("serializes");
        assert!(!s.contains("managedFields"));
        assert!(!s.contains("ownerReferences"));
        assert!(!s.contains(LAST_APPLIED_ANNOTATION));

        // full keeps the non-bookkeeping annotation the compact view drops.
        let full = project_object(&cr, ProjectionView::Full);
        assert_eq!(full["metadata"]["annotations"]["keep"], json!("me"));
        assert_eq!(full["metadata"].get("managedFields"), None);
        // raw is untouched.
        assert_eq!(project_object(&cr, ProjectionView::Raw), cr);
    }

    #[test]
    fn the_object_list_projection_counts_and_strips() {
        let list = json!([
            { "kind": "BreathePosture", "metadata": { "name": "platform-default", "managedFields": [ { "manager": "m" } ] }, "spec": { "setpoint": 0.8 } },
            { "kind": "BreathePosture", "metadata": { "name": "aggressive", "ownerReferences": [ { "kind": "K" } ] }, "spec": { "setpoint": 0.9 } },
        ]);
        let out = project_object_list(&list, ProjectionView::Summary);
        assert_eq!(out["count"], json!(2));
        assert_eq!(out["items"][0]["name"], json!("platform-default"));
        assert_eq!(out["items"][1]["spec"]["setpoint"], json!(0.9));
        let s = payload_bytes(&out, "items");
        assert!(!s.contains("managedFields"));
        assert!(!s.contains("ownerReferences"));
    }
}
