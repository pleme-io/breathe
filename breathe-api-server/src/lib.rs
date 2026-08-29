//! `breathe-api-server` — the REST surface over the [`breathe_facade`] core.
//!
//! Every route is a thin call into the same `BreatheStore` the MCP drives, so the
//! two surfaces can never diverge. The path/verb set mirrors
//! `spec/breathe.openapi.yaml` (the source of truth); the handlers are kube-rs
//! facade calls — the one place breathe legitimately hand-serves (a generic
//! `kube::Api<T>` dispatch that forge-gen's HTTP-client model fits poorly).

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, patch},
};
use breathe_facade::{BreatheStore, DimensionId, StoreError};
use serde::Deserialize;
use serde_json::{Value, json};

pub type SharedStore = Arc<dyn BreatheStore>;

/// The full HTTP router: REST + a `/graphql` endpoint, both over the shared
/// facade. `store` is the real `KubeStore` or a mock.
#[must_use]
pub fn router(store: SharedStore) -> Router {
    let schema = graphql::schema(store.clone());
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/api/v1/catalog", get(catalog))
        .route("/api/v1/bands/:kind", get(list_bands))
        .route(
            "/api/v1/bands/:kind/:namespace/:name",
            get(get_band).patch(patch_band),
        )
        .route(
            "/api/v1/bands/:kind/:namespace/:name/dry-run",
            patch(set_dry_run),
        )
        .route(
            "/api/v1/bands/:kind/:namespace/:name/write-intent",
            patch(set_write_intent),
        )
        .route(
            "/api/v1/bands/:kind/:namespace/:name/confirm",
            patch(confirm_band),
        )
        .route("/api/v1/nodepools", get(list_pools))
        .route("/api/v1/nodepools/:name", get(get_pool))
        .route(
            "/api/v1/nodepools/:name/write-enabled",
            patch(set_write_enabled),
        )
        .route_service("/graphql", async_graphql_axum::GraphQL::new(schema))
        .with_state(store)
}

/// Map a facade result to an HTTP response.
fn respond(r: Result<Value, StoreError>) -> Response {
    match r {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(StoreError::BadRequest(m)) => {
            (StatusCode::BAD_REQUEST, Json(json!({ "error": m }))).into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

fn kind_or_400(s: &str) -> Result<DimensionId, Response> {
    DimensionId::parse(s).ok_or_else(|| {
        let known: Vec<&str> = DimensionId::ALL.iter().map(|d| d.as_str()).collect();
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("unknown band kind '{s}'"), "known": known })),
        )
            .into_response()
    })
}

#[derive(Deserialize)]
struct NsQuery {
    namespace: Option<String>,
}

async fn catalog(State(store): State<SharedStore>) -> Response {
    (StatusCode::OK, Json(store.catalog())).into_response()
}

async fn list_bands(
    State(store): State<SharedStore>,
    Path(kind): Path<String>,
    Query(q): Query<NsQuery>,
) -> Response {
    match kind_or_400(&kind) {
        Ok(k) => respond(store.list_bands(k, q.namespace).await),
        Err(r) => r,
    }
}

async fn get_band(
    State(store): State<SharedStore>,
    Path((kind, ns, name)): Path<(String, String, String)>,
) -> Response {
    match kind_or_400(&kind) {
        Ok(k) => respond(store.get_band(k, ns, name).await),
        Err(r) => r,
    }
}

async fn patch_band(
    State(store): State<SharedStore>,
    Path((kind, ns, name)): Path<(String, String, String)>,
    Json(spec): Json<Value>,
) -> Response {
    match kind_or_400(&kind) {
        Ok(k) => respond(store.patch_band_spec(k, ns, name, spec).await),
        Err(r) => r,
    }
}

#[derive(Deserialize)]
struct DryRunBody {
    #[serde(rename = "dryRun")]
    dry_run: bool,
}

/// RETIRED for eight of the ten kinds.
///
/// This route used to patch `spec.dryRun` on any kind and return 200. On every
/// kind but `host-param` and `kube-param` that patch has changed nothing since
/// 2026-06-19, so the 200 was a report of a safety gate that had not been
/// applied. It now returns 400 there, names the retirement, and points at
/// `writeIntent`; on the two kinds that DO read the field it still applies it,
/// because a blanket refusal would be the opposite lie.
async fn set_dry_run(
    State(store): State<SharedStore>,
    Path((kind, ns, name)): Path<(String, String, String)>,
    Json(b): Json<DryRunBody>,
) -> Response {
    match kind_or_400(&kind) {
        Ok(k) if !k.dry_run_is_honored() => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "spec.dryRun has no effect on this band kind",
                "kind": k.as_str(),
                "retiredSince": "breathe@76924b0 (2026-06-19)",
                "useInstead": "PATCH /api/v1/bands/{kind}/{namespace}/{name}/write-intent",
                "honoredBy": ["host-param", "kube-param"],
                "wroteNothing": true,
            })),
        )
            .into_response(),
        Ok(k) => respond(
            store
                .patch_band_spec(k, ns, name, json!({ "dryRun": b.dry_run }))
                .await,
        ),
        Err(r) => r,
    }
}

#[derive(Deserialize)]
struct WriteIntentBody {
    intent: String,
    #[serde(rename = "confirmAfterSeconds", default)]
    confirm_after_seconds: Option<u64>,
    #[serde(rename = "authorizedBy", default)]
    authorized_by: Option<String>,
}

/// Author `spec.writeIntent` — the authorization gate that actually holds.
async fn set_write_intent(
    State(store): State<SharedStore>,
    Path((kind, ns, name)): Path<(String, String, String)>,
    Json(b): Json<WriteIntentBody>,
) -> Response {
    let k = match kind_or_400(&kind) {
        Ok(k) => k,
        Err(r) => return r,
    };
    // Refuse an unattributed go-live here rather than writing a CR the controller
    // would only ever resolve to a fail-safe shadow. Same verdict, one round-trip
    // earlier, and the operator learns why while still holding the decision.
    if b.intent == "write"
        && b.authorized_by
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "intent=write requires authorizedBy",
                "why": "a live carve must name who authorized it; the witness is carried into \
                        status.effectiveGate so 'why is this band writing?' is answerable from the CR alone",
                "wroteNothing": true,
            })),
        )
            .into_response();
    }
    let mut intent = json!({ "intent": b.intent });
    if let Some(secs) = b.confirm_after_seconds {
        intent["confirmAfterSeconds"] = json!(secs);
    }
    if let Some(by) = b.authorized_by {
        intent["authorizedBy"] = json!(by);
    }
    respond(
        store
            .patch_band_spec(k, ns, name, json!({ "writeIntent": intent }))
            .await,
    )
}

#[derive(Deserialize)]
struct ConfirmBody {
    confirmed: bool,
}

/// Set or clear `breathe.pleme.io/confirmed` — the operator fast-path that
/// promotes a calibrating band without waiting out its confirm window.
async fn confirm_band(
    State(store): State<SharedStore>,
    Path((kind, ns, name)): Path<(String, String, String)>,
    Json(b): Json<ConfirmBody>,
) -> Response {
    match kind_or_400(&kind) {
        // JSON null in a merge-patch REMOVES the key — the honest "un-confirm".
        Ok(k) => {
            let v = if b.confirmed {
                json!("true")
            } else {
                Value::Null
            };
            respond(
                store
                    .annotate_band(
                        k,
                        ns,
                        name,
                        json!({ breathe_provider::CONFIRMED_ANNOTATION: v }),
                    )
                    .await,
            )
        }
        Err(r) => r,
    }
}

async fn list_pools(State(store): State<SharedStore>) -> Response {
    respond(store.list_pools().await)
}

async fn get_pool(State(store): State<SharedStore>, Path(name): Path<String>) -> Response {
    respond(store.get_pool(name).await)
}

#[derive(Deserialize)]
struct WriteEnabledBody {
    #[serde(rename = "writeEnabled")]
    write_enabled: bool,
}

async fn set_write_enabled(
    State(store): State<SharedStore>,
    Path(name): Path<String>,
    Json(b): Json<WriteEnabledBody>,
) -> Response {
    respond(
        store
            .patch_pool_spec(name, json!({ "writeEnabled": b.write_enabled }))
            .await,
    )
}

// ───────────────────────────── GraphQL ─────────────────────────────

/// The GraphQL surface (async-graphql) over the same facade. Resolvers return
/// the CRD JSON via the `Json` scalar so there is one typed source of truth.
pub mod graphql {
    use super::{DimensionId, SharedStore, StoreError};
    use async_graphql::{Context, EmptySubscription, Json, Object, Schema};
    use serde_json::Value;

    fn gql(e: StoreError) -> async_graphql::Error {
        async_graphql::Error::new(e.to_string())
    }
    fn parse_kind(s: &str) -> async_graphql::Result<DimensionId> {
        DimensionId::parse(s).ok_or_else(|| {
            let known: Vec<&str> = DimensionId::ALL.iter().map(|d| d.as_str()).collect();
            async_graphql::Error::new(format!(
                "unknown band kind '{s}' (known: {})",
                known.join(", ")
            ))
        })
    }
    fn store<'a>(ctx: &Context<'a>) -> async_graphql::Result<&'a SharedStore> {
        ctx.data::<SharedStore>()
    }

    pub struct Query;
    #[Object]
    impl Query {
        /// The self-describing dimension catalog.
        async fn catalog(&self, ctx: &Context<'_>) -> async_graphql::Result<Json<Value>> {
            Ok(Json(store(ctx)?.catalog()))
        }
        /// List bands of a dimension, optionally namespace-scoped.
        async fn bands(
            &self,
            ctx: &Context<'_>,
            kind: String,
            namespace: Option<String>,
        ) -> async_graphql::Result<Json<Value>> {
            Ok(Json(
                store(ctx)?
                    .list_bands(parse_kind(&kind)?, namespace)
                    .await
                    .map_err(gql)?,
            ))
        }
        /// One band CR.
        async fn band(
            &self,
            ctx: &Context<'_>,
            kind: String,
            namespace: String,
            name: String,
        ) -> async_graphql::Result<Json<Value>> {
            Ok(Json(
                store(ctx)?
                    .get_band(parse_kind(&kind)?, namespace, name)
                    .await
                    .map_err(gql)?,
            ))
        }
        /// All node pools.
        async fn nodepools(&self, ctx: &Context<'_>) -> async_graphql::Result<Json<Value>> {
            Ok(Json(store(ctx)?.list_pools().await.map_err(gql)?))
        }
        async fn nodepool(
            &self,
            ctx: &Context<'_>,
            name: String,
        ) -> async_graphql::Result<Json<Value>> {
            Ok(Json(store(ctx)?.get_pool(name).await.map_err(gql)?))
        }
    }

    pub struct Mutation;
    #[Object]
    impl Mutation {
        /// Merge-patch a band's spec.
        async fn patch_band(
            &self,
            ctx: &Context<'_>,
            kind: String,
            namespace: String,
            name: String,
            spec: Json<Value>,
        ) -> async_graphql::Result<Json<Value>> {
            Ok(Json(
                store(ctx)?
                    .patch_band_spec(parse_kind(&kind)?, namespace, name, spec.0)
                    .await
                    .map_err(gql)?,
            ))
        }
        /// Author a band's `writeIntent` — THE authorization gate on every kind.
        /// `intent` is observe | calibrateThenWrite | write | frozen;
        /// `authorizedBy` is required when `intent` is `write`.
        async fn set_write_intent(
            &self,
            ctx: &Context<'_>,
            kind: String,
            namespace: String,
            name: String,
            intent: String,
            confirm_after_seconds: Option<u64>,
            authorized_by: Option<String>,
        ) -> async_graphql::Result<Json<Value>> {
            if intent == "write"
                && authorized_by
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default()
                    .is_empty()
            {
                return Err(async_graphql::Error::new(
                    "intent=write requires authorizedBy — a live carve must name who authorized it; nothing was written",
                ));
            }
            let mut body = serde_json::json!({ "intent": intent });
            if let Some(secs) = confirm_after_seconds {
                body["confirmAfterSeconds"] = serde_json::json!(secs);
            }
            if let Some(by) = authorized_by {
                body["authorizedBy"] = serde_json::json!(by);
            }
            let spec = serde_json::json!({ "writeIntent": body });
            Ok(Json(
                store(ctx)?
                    .patch_band_spec(parse_kind(&kind)?, namespace, name, spec)
                    .await
                    .map_err(gql)?,
            ))
        }
        /// Set or clear `breathe.pleme.io/confirmed` — promote a calibrating band
        /// to writing now instead of waiting out its confirm window.
        async fn confirm_band(
            &self,
            ctx: &Context<'_>,
            kind: String,
            namespace: String,
            name: String,
            confirmed: bool,
        ) -> async_graphql::Result<Json<Value>> {
            let v = if confirmed {
                serde_json::json!("true")
            } else {
                Value::Null
            };
            let ann = serde_json::json!({ breathe_provider::CONFIRMED_ANNOTATION: v });
            Ok(Json(
                store(ctx)?
                    .annotate_band(parse_kind(&kind)?, namespace, name, ann)
                    .await
                    .map_err(gql)?,
            ))
        }
        /// Write a band's `dryRun`. RETIRED for eight of the ten kinds — only
        /// `host-param` and `kube-param` read it; elsewhere this errors rather
        /// than reporting success for a patch that changes nothing. Use
        /// `setWriteIntent`.
        async fn set_dry_run(
            &self,
            ctx: &Context<'_>,
            kind: String,
            namespace: String,
            name: String,
            dry_run: bool,
        ) -> async_graphql::Result<Json<Value>> {
            let k = parse_kind(&kind)?;
            if !k.dry_run_is_honored() {
                return Err(async_graphql::Error::new(format!(
                    "spec.dryRun has no effect on {kind} bands (retired breathe@76924b0, 2026-06-19); \
                     use setWriteIntent. Only host-param and kube-param read it. Nothing was written."
                )));
            }
            Ok(Json(
                store(ctx)?
                    .patch_band_spec(k, namespace, name, serde_json::json!({ "dryRun": dry_run }))
                    .await
                    .map_err(gql)?,
            ))
        }
        /// Flip a BreatheNodePool's writeEnabled master switch. Bounds HOST-plane
        /// writes for bands enrolled on that pool; not a cluster-wide kill switch.
        async fn set_write_enabled(
            &self,
            ctx: &Context<'_>,
            name: String,
            write_enabled: bool,
        ) -> async_graphql::Result<Json<Value>> {
            Ok(Json(
                store(ctx)?
                    .patch_pool_spec(name, serde_json::json!({ "writeEnabled": write_enabled }))
                    .await
                    .map_err(gql)?,
            ))
        }
    }

    pub type BreatheSchema = Schema<Query, Mutation, EmptySubscription>;

    #[must_use]
    pub fn schema(store: SharedStore) -> BreatheSchema {
        Schema::build(Query, Mutation, EmptySubscription)
            .data(store)
            .finish()
    }
}

// ─────────────────────────────── gRPC ──────────────────────────────

/// The gRPC surface (tonic) over the same facade — now with TYPED proto messages
/// (no JSON-string envelope). `proto/breathe.proto` is generated by grpc-forge
/// from `spec/breathe.openapi.yaml`; pbjson makes the messages serde-capable, so
/// the facade's `serde_json::Value` bridges straight into the typed responses via
/// [`typed`]. The handler is the one place breathe legitimately hand-serves (a
/// generic `kube::Api<T>` dispatch) — it just maps facade JSON ↔ typed proto.
pub mod grpc {
    use super::{DimensionId, SharedStore, StoreError};
    use serde_json::Value;
    use tonic::{Request, Response, Status};

    /// The generated proto messages, enums, and the service definition, plus the
    /// pbjson serde impls. A submodule so the proto `BandKind` enum doesn't clash
    /// with the facade's [`super::DimensionId`].
    pub mod pb {
        tonic::include_proto!("breathe.v1");
        include!(concat!(env!("OUT_DIR"), "/breathe.v1.serde.rs"));
    }

    fn st(e: StoreError) -> Status {
        match e {
            StoreError::BadRequest(m) => Status::invalid_argument(m),
            other => Status::internal(other.to_string()),
        }
    }

    /// Map a facade JSON `Value` into a typed proto response. pbjson is STRICT:
    /// a parse failure means the live CRD JSON drifted from
    /// `spec/breathe.openapi.yaml` — surfaced as a typed error, never a silent
    /// wrong answer (the spec-first standard keeps the two in sync).
    fn typed<T: serde::de::DeserializeOwned>(v: Value) -> Result<T, Status> {
        serde_json::from_value(v).map_err(|e| {
            Status::internal(format!(
                "response did not match the typed schema (spec drift?): {e}"
            ))
        })
    }

    fn opt_ns(s: String) -> Option<String> {
        if s.is_empty() { None } else { Some(s) }
    }

    /// proto `BandKind` (an `i32` enum on the wire) → the canonical
    /// [`DimensionId`].
    ///
    /// The match over `P` is exhaustive apart from `Unspecified`, so adding a
    /// dimension to the proto without mapping it here is `E0004` rather than a
    /// gRPC method that silently rejects a kind the rest of the substrate
    /// supports — which is exactly how five kinds stayed unreachable.
    pub fn kind_of(k: i32) -> Result<DimensionId, Status> {
        use pb::BandKind as P;
        let p = P::try_from(k).map_err(|_| Status::invalid_argument("unknown band kind"))?;
        match p {
            P::Memory => Ok(DimensionId::Memory),
            P::Cpu => Ok(DimensionId::Cpu),
            P::Storage => Ok(DimensionId::Storage),
            P::Replica => Ok(DimensionId::Replica),
            P::Arc => Ok(DimensionId::Arc),
            P::Cgroup => Ok(DimensionId::Cgroup),
            P::CgroupCpu => Ok(DimensionId::CgroupCpu),
            P::HostParam => Ok(DimensionId::HostParam),
            P::KubeParam => Ok(DimensionId::KubeParam),
            P::AppParam => Ok(DimensionId::AppParam),
            P::Request => Ok(DimensionId::Request),
            P::Unspecified => Err(Status::invalid_argument("band kind unspecified")),
        }
    }

    pub struct GrpcService {
        pub store: SharedStore,
    }

    #[tonic::async_trait]
    impl pb::breathe_server::Breathe for GrpcService {
        async fn band_list(
            &self,
            req: Request<pb::BandListRequest>,
        ) -> Result<Response<pb::BandListResponse>, Status> {
            let r = req.into_inner();
            let v = self
                .store
                .list_bands(kind_of(r.kind)?, opt_ns(r.namespace))
                .await
                .map_err(st)?;
            Ok(Response::new(pb::BandListResponse { items: typed(v)? }))
        }
        async fn band_get(
            &self,
            req: Request<pb::BandGetRequest>,
        ) -> Result<Response<pb::Band>, Status> {
            let r = req.into_inner();
            let v = self
                .store
                .get_band(kind_of(r.kind)?, r.namespace, r.name)
                .await
                .map_err(st)?;
            Ok(Response::new(typed(v)?))
        }
        async fn band_patch(
            &self,
            req: Request<pb::BandPatchRequest>,
        ) -> Result<Response<pb::Band>, Status> {
            let r = req.into_inner();
            // BandSpec scalars are proto3 `optional` (field presence): pbjson emits
            // the fields the client SET (Some — incl. a zero like dryRun=false) and
            // omits the rest (None) → correct RFC-7386 merge: present writes, absent
            // leaves unchanged. (Without presence, proto3 would drop zero values.)
            let spec = serde_json::to_value(r.body.unwrap_or_default())
                .map_err(|e| Status::internal(e.to_string()))?;
            // Same border check as the REST and MCP surfaces: a go-live must name
            // its author. Refusing here keeps all four surfaces answering
            // identically, instead of gRPC being the one that writes a CR the
            // controller will only ever resolve to a fail-safe shadow.
            if spec["writeIntent"]["intent"] == "write"
                && spec["writeIntent"]["authorizedBy"]
                    .as_str()
                    .map(str::trim)
                    .unwrap_or_default()
                    .is_empty()
            {
                return Err(Status::invalid_argument(
                    "writeIntent.intent=write requires authorizedBy — a live carve must name who \
                     authorized it. Nothing was written.",
                ));
            }
            let v = self
                .store
                .patch_band_spec(kind_of(r.kind)?, r.namespace, r.name, spec)
                .await
                .map_err(st)?;
            Ok(Response::new(typed(v)?))
        }
        /// RETIRED for eight of the ten kinds — see [`super::set_dry_run`]. Only
        /// `host-param` and `kube-param` read `spec.dryRun`; elsewhere this
        /// returns `FAILED_PRECONDITION` rather than an OK for a write that
        /// changes nothing.
        async fn band_set_dry_run(
            &self,
            req: Request<pb::BandSetDryRunRequest>,
        ) -> Result<Response<pb::Band>, Status> {
            let r = req.into_inner();
            let k = kind_of(r.kind)?;
            if !k.dry_run_is_honored() {
                return Err(Status::failed_precondition(
                    "spec.dryRun has no effect on this band kind (retired breathe@76924b0, 2026-06-19); \
                     set BandSpec.write_intent via BandPatch instead. Only host-param and kube-param \
                     read dryRun. Nothing was written.",
                ));
            }
            let v = self
                .store
                .patch_band_spec(
                    k,
                    r.namespace,
                    r.name,
                    serde_json::json!({ "dryRun": r.dry_run }),
                )
                .await
                .map_err(st)?;
            Ok(Response::new(typed(v)?))
        }
        async fn catalog_list(
            &self,
            _req: Request<pb::CatalogListRequest>,
        ) -> Result<Response<pb::Catalog>, Status> {
            Ok(Response::new(typed(self.store.catalog())?))
        }
        async fn nodepool_list(
            &self,
            _req: Request<pb::NodepoolListRequest>,
        ) -> Result<Response<pb::NodepoolListResponse>, Status> {
            let v = self.store.list_pools().await.map_err(st)?;
            Ok(Response::new(pb::NodepoolListResponse { items: typed(v)? }))
        }
        async fn nodepool_get(
            &self,
            req: Request<pb::NodepoolGetRequest>,
        ) -> Result<Response<pb::NodePool>, Status> {
            let v = self
                .store
                .get_pool(req.into_inner().name)
                .await
                .map_err(st)?;
            Ok(Response::new(typed(v)?))
        }
        async fn nodepool_set_write_enabled(
            &self,
            req: Request<pb::NodepoolSetWriteEnabledRequest>,
        ) -> Result<Response<pb::NodePool>, Status> {
            let r = req.into_inner();
            let v = self
                .store
                .patch_pool_spec(
                    r.name,
                    serde_json::json!({ "writeEnabled": r.write_enabled }),
                )
                .await
                .map_err(st)?;
            Ok(Response::new(typed(v)?))
        }
        async fn healthz(
            &self,
            _req: Request<pb::HealthzRequest>,
        ) -> Result<Response<::pbjson_types::Empty>, Status> {
            Ok(Response::new(::pbjson_types::Empty {}))
        }
    }

    #[must_use]
    pub fn server(store: SharedStore) -> pb::breathe_server::BreatheServer<GrpcService> {
        pb::breathe_server::BreatheServer::new(GrpcService { store })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use std::sync::Mutex;
    use tower::ServiceExt;

    #[derive(Default)]
    struct MockStore {
        patches: Mutex<Vec<(String, Value)>>,
    }
    #[async_trait]
    impl BreatheStore for MockStore {
        async fn list_bands(
            &self,
            kind: DimensionId,
            _ns: Option<String>,
        ) -> Result<Value, StoreError> {
            Ok(json!([{ "kind": kind.as_str() }]))
        }
        async fn get_band(
            &self,
            _kind: DimensionId,
            ns: String,
            name: String,
        ) -> Result<Value, StoreError> {
            // schema-faithful Band JSON (what the real KubeStore serializes): the
            // typed gRPC surface deserializes this strictly into pb::Band.
            Ok(json!({
                "apiVersion": "breathe.pleme.io/v1",
                "kind": "ArcBand",
                "metadata": { "name": name, "namespace": ns, "resourceVersion": "42" },
                "spec": { "setpoint": 0.8, "dryRun": false },
                "status": { "phase": "Holding" }
            }))
        }
        async fn patch_band_spec(
            &self,
            _k: DimensionId,
            _ns: String,
            name: String,
            spec: Value,
        ) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, spec.clone()));
            Ok(json!({ "spec": spec }))
        }
        async fn annotate_band(
            &self,
            _k: DimensionId,
            _ns: String,
            name: String,
            ann: Value,
        ) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, ann.clone()));
            Ok(json!({ "metadata": { "annotations": ann } }))
        }
        async fn list_pools(&self) -> Result<Value, StoreError> {
            Ok(
                json!([{ "metadata": { "name": "rio" }, "spec": { "nodeName": "rio", "arcMaxGiB": 6 } }]),
            )
        }
        async fn get_pool(&self, name: String) -> Result<Value, StoreError> {
            Ok(json!({
                "apiVersion": "breathe.pleme.io/v1",
                "kind": "BreatheNodePool",
                "metadata": { "name": name },
                "spec": { "nodeName": name, "arcMaxGiB": 6, "writeEnabled": true },
                "status": { "phase": "Active" }
            }))
        }
        async fn patch_pool_spec(&self, name: String, spec: Value) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, spec.clone()));
            Ok(json!({ "spec": spec }))
        }
        async fn list_postures(&self) -> Result<Value, StoreError> {
            Ok(json!([{ "metadata": { "name": "platform-default" }, "spec": { "setpoint": 0.8 } }]))
        }
        async fn get_posture(&self, name: String) -> Result<Value, StoreError> {
            Ok(json!({
                "apiVersion": "breathe.pleme.io/v1",
                "kind": "BreathePosture",
                "metadata": { "name": name },
                "spec": { "setpoint": 0.8, "growAbove": 0.85, "growFactor": 1.25, "shrinkBelow": 0.7, "shrinkFactor": 0.9, "cooldownSeconds": 600, "maxStalenessSeconds": 120, "disruptionPolicy": "restartFreeOnly" }
            }))
        }
        async fn patch_posture_spec(&self, name: String, spec: Value) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, spec.clone()));
            Ok(json!({ "spec": spec }))
        }
        fn catalog(&self) -> Value {
            breathe_facade::catalog_json()
        }
    }

    async fn body_json(resp: Response) -> Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// The REST half of the D3 fix: an `arc` band does not read `dryRun`, so the
    /// route refuses instead of returning 200 for a patch that changes nothing.
    /// (It used to return 200 and land the patch — the test that asserted this
    /// encoded the false model and stayed green through the whole defect.)
    #[tokio::test]
    async fn set_dry_run_route_refuses_where_the_field_is_inert() {
        let mock = Arc::new(MockStore::default());
        let app = router(mock.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/bands/arc/pangea-system/rio-arc/dry-run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"dryRun":false}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(
            mock.patches.lock().unwrap().is_empty(),
            "a refused call must not reach the store"
        );
    }

    /// …and still applies on the two kinds that genuinely read it.
    #[tokio::test]
    async fn set_dry_run_route_applies_on_a_param_kind() {
        let mock = Arc::new(MockStore::default());
        let app = router(mock.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/bands/host-param/isolated/vm-dirty/dry-run")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"dryRun":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "dryRun": true }));
    }

    /// Every shipped dimension is routable. Five of these ten returned 400
    /// "unknown band kind" before this stage, despite shipping as CRDs.
    #[tokio::test]
    async fn every_shipped_dimension_is_routable() {
        for kind in DimensionId::ALL {
            let app = router(Arc::new(MockStore::default()));
            let uri = ["/api/v1/bands/", kind.as_str()].concat();
            let resp = app
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "{kind} must be routable");
        }
    }

    #[tokio::test]
    async fn write_intent_route_writes_the_intent_and_refuses_an_unattributed_go_live() {
        let mock = Arc::new(MockStore::default());
        let app = router(mock.clone());
        let patch_intent = |app: Router, body: &'static str| async move {
            app.oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/bands/cpu/isolated/coredns/write-intent")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap()
        };
        let resp = patch_intent(app.clone(), r#"{"intent":"write"}"#).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "an unattributed go-live is refused"
        );
        assert!(mock.patches.lock().unwrap().is_empty());

        let resp = patch_intent(
            app,
            r#"{"intent":"write","authorizedBy":"drzzln 2026-07-26"}"#,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            mock.patches.lock().unwrap()[0].1,
            json!({ "writeIntent": { "intent": "write", "authorizedBy": "drzzln 2026-07-26" } })
        );
    }

    #[tokio::test]
    async fn confirm_route_sets_the_operator_annotation() {
        let mock = Arc::new(MockStore::default());
        let app = router(mock.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/bands/memory/isolated/b/confirm")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"confirmed":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            mock.patches.lock().unwrap()[0].1,
            json!({ breathe_provider::CONFIRMED_ANNOTATION: "true" })
        );
    }

    #[tokio::test]
    async fn unknown_band_kind_is_400() {
        let app = router(Arc::new(MockStore::default()));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/bands/bogus")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn catalog_route_returns_all_dimensions() {
        let app = router(Arc::new(MockStore::default()));
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/catalog")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v = body_json(resp).await;
        // every catalogued dimension is exposed on the REST surface (not a literal —
        // tracks breathe_catalog::ALL_DIMENSIONS so it never goes stale).
        assert_eq!(
            v["dimensions"].as_array().unwrap().len(),
            breathe_catalog::ALL_DIMENSIONS.len()
        );
    }

    #[tokio::test]
    async fn write_enabled_route_patches_the_pool() {
        let mock = Arc::new(MockStore::default());
        let app = router(mock.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/nodepools/rio/write-enabled")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"writeEnabled":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            mock.patches.lock().unwrap()[0].1,
            json!({ "writeEnabled": true })
        );
    }

    #[tokio::test]
    async fn graphql_set_dry_run_errors_where_the_field_is_inert() {
        let mock = Arc::new(MockStore::default());
        let schema = graphql::schema(mock.clone());
        let resp = schema
            .execute(r#"mutation { setDryRun(kind:"arc", namespace:"pangea-system", name:"rio-arc", dryRun:false) }"#)
            .await;
        assert!(
            !resp.errors.is_empty(),
            "an inert dryRun write must not report success"
        );
        assert!(mock.patches.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn graphql_set_write_intent_mutation_patches_the_band() {
        let mock = Arc::new(MockStore::default());
        let schema = graphql::schema(mock.clone());
        let resp = schema
            .execute(r#"mutation { setWriteIntent(kind:"cpu", namespace:"isolated", name:"coredns", intent:"observe") }"#)
            .await;
        assert!(resp.errors.is_empty(), "{:?}", resp.errors);
        assert_eq!(
            mock.patches.lock().unwrap()[0].1,
            json!({ "writeIntent": { "intent": "observe" } })
        );
    }

    /// GraphQL reaches the five dimensions it could not name before.
    #[tokio::test]
    async fn graphql_reaches_the_previously_invisible_kinds() {
        let schema = graphql::schema(Arc::new(MockStore::default()));
        for kind in [
            "cgroup-cpu",
            "host-param",
            "kube-param",
            "app-param",
            "replica",
        ] {
            let q = ["{ bands(kind:\"", kind, "\") }"].concat();
            let resp = schema.execute(q).await;
            assert!(resp.errors.is_empty(), "{kind}: {:?}", resp.errors);
        }
    }

    #[tokio::test]
    async fn graphql_catalog_query_returns_dimensions() {
        let schema = graphql::schema(Arc::new(MockStore::default()));
        let resp = schema.execute("{ catalog }").await;
        assert!(resp.errors.is_empty(), "{:?}", resp.errors);
        assert!(resp.data.to_string().contains("dimensions"));
    }

    #[tokio::test]
    async fn grpc_set_write_enabled_returns_typed_nodepool() {
        use grpc::pb::breathe_server::Breathe;
        let mock = Arc::new(MockStore::default());
        let svc = grpc::GrpcService {
            store: mock.clone(),
        };
        let resp = svc
            .nodepool_set_write_enabled(tonic::Request::new(
                grpc::pb::NodepoolSetWriteEnabledRequest {
                    name: "rio".into(),
                    write_enabled: true,
                },
            ))
            .await
            .unwrap();
        // the response is a TYPED NodePool, not a JSON string envelope.
        assert!(resp.into_inner().spec.unwrap().write_enabled);
        assert_eq!(
            mock.patches.lock().unwrap()[0].1,
            json!({ "writeEnabled": true })
        );
    }

    #[tokio::test]
    async fn grpc_band_get_returns_typed_band() {
        use grpc::pb::breathe_server::Breathe;
        let svc = grpc::GrpcService {
            store: Arc::new(MockStore::default()),
        };
        let resp = svc
            .band_get(tonic::Request::new(grpc::pb::BandGetRequest {
                kind: grpc::pb::BandKind::Arc as i32,
                namespace: "pangea-system".into(),
                name: "rio-arc".into(),
            }))
            .await
            .unwrap();
        let band = resp.into_inner();
        assert_eq!(band.api_version, "breathe.pleme.io/v1");
        // BandSpec scalars are `optional` (field presence) → Option<f64> here.
        assert!((band.spec.unwrap().setpoint.unwrap() - 0.8).abs() < 1e-9);
        assert_eq!(band.status.unwrap().phase, "Holding");
    }

    #[tokio::test]
    async fn grpc_band_patch_transmits_zero_values_via_field_presence() {
        // the merge-patch fix: a client setting dryRun=false (a zero value) over
        // a typed BandSpec must actually transmit it — proto3 `optional` presence
        // makes Some(false) serialize as `{"dryRun": false}`, not get dropped.
        use grpc::pb::breathe_server::Breathe;
        let mock = Arc::new(MockStore::default());
        let svc = grpc::GrpcService {
            store: mock.clone(),
        };
        let body = grpc::pb::BandSpec {
            dry_run: Some(false),
            ..Default::default()
        };
        svc.band_patch(tonic::Request::new(grpc::pb::BandPatchRequest {
            kind: grpc::pb::BandKind::Arc as i32,
            namespace: "pangea-system".into(),
            name: "rio-arc".into(),
            body: Some(body),
        }))
        .await
        .unwrap();
        // exactly the one set field reached the facade — no other scalars leaked in.
        assert_eq!(
            mock.patches.lock().unwrap()[0].1,
            json!({ "dryRun": false })
        );
    }

    #[tokio::test]
    async fn grpc_band_list_returns_typed_items() {
        use grpc::pb::breathe_server::Breathe;
        let svc = grpc::GrpcService {
            store: Arc::new(MockStore::default()),
        };
        let resp = svc
            .band_list(tonic::Request::new(grpc::pb::BandListRequest {
                kind: grpc::pb::BandKind::Arc as i32,
                namespace: String::new(),
            }))
            .await
            .unwrap();
        assert_eq!(resp.into_inner().items.len(), 1);
    }

    #[tokio::test]
    async fn grpc_catalog_list_returns_typed_catalog() {
        // exercises the full bridge incl. nullable `upstreamMirror` (proto3 string).
        use grpc::pb::breathe_server::Breathe;
        let svc = grpc::GrpcService {
            store: Arc::new(MockStore::default()),
        };
        let resp = svc
            .catalog_list(tonic::Request::new(grpc::pb::CatalogListRequest {}))
            .await
            .unwrap();
        let cat = resp.into_inner();
        // the gRPC bridge preserves every catalogued dimension (tracks the canonical
        // count, never a literal — see breathe_catalog::ALL_DIMENSIONS).
        assert_eq!(cat.dimensions.len(), breathe_catalog::ALL_DIMENSIONS.len());
        assert!(cat.dimensions.iter().any(|d| d.id == "arc" && d.is_host));
        assert!(
            cat.dimensions
                .iter()
                .any(|d| d.id == "cgroup-cpu" && d.is_host)
        );
        assert!(
            cat.dimensions
                .iter()
                .any(|d| d.id == "memory" && !d.is_host)
        );
    }

    #[tokio::test]
    async fn grpc_set_dry_run_refuses_where_inert_and_applies_where_honored() {
        use grpc::pb::breathe_server::Breathe;
        let mock = Arc::new(MockStore::default());
        let svc = grpc::GrpcService {
            store: mock.clone(),
        };
        let call = |kind: grpc::pb::BandKind| {
            tonic::Request::new(grpc::pb::BandSetDryRunRequest {
                kind: kind as i32,
                namespace: "pangea-system".into(),
                name: "b".into(),
                dry_run: false,
            })
        };
        let err = svc
            .band_set_dry_run(call(grpc::pb::BandKind::Arc))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(mock.patches.lock().unwrap().is_empty());

        let resp = svc
            .band_set_dry_run(call(grpc::pb::BandKind::HostParam))
            .await
            .unwrap();
        assert_eq!(resp.into_inner().spec.unwrap().dry_run, Some(false));
        assert_eq!(
            mock.patches.lock().unwrap()[0].1,
            json!({ "dryRun": false })
        );
    }

    /// gRPC can finally author `writeIntent` at all — its `BandSpec` carried only
    /// `dry_run`, which is inert on eight kinds, so this surface had no way to
    /// hold or release a band. And it applies the same unattributed-go-live
    /// refusal as REST and MCP, so all four surfaces answer identically.
    #[tokio::test]
    async fn grpc_band_patch_carries_write_intent_and_refuses_an_unattributed_go_live() {
        use grpc::pb::breathe_server::Breathe;
        let mock = Arc::new(MockStore::default());
        let svc = grpc::GrpcService {
            store: mock.clone(),
        };
        let call = |wi: grpc::pb::WriteIntent| {
            tonic::Request::new(grpc::pb::BandPatchRequest {
                kind: grpc::pb::BandKind::Cpu as i32,
                namespace: "isolated".into(),
                name: "coredns".into(),
                body: Some(grpc::pb::BandSpec {
                    write_intent: Some(wi),
                    ..Default::default()
                }),
            })
        };
        let unattributed = grpc::pb::WriteIntent {
            intent: "write".into(),
            confirm_after_seconds: None,
            authorized_by: None,
        };
        let err = svc.band_patch(call(unattributed)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            mock.patches.lock().unwrap().is_empty(),
            "a refused go-live must not reach the store"
        );

        let observe = grpc::pb::WriteIntent {
            intent: "observe".into(),
            confirm_after_seconds: None,
            authorized_by: None,
        };
        svc.band_patch(call(observe)).await.unwrap();
        assert_eq!(
            mock.patches.lock().unwrap()[0].1["writeIntent"]["intent"],
            "observe"
        );
    }

    /// **The spec-drift gate.** `spec/breathe.openapi.yaml` calls itself the
    /// single source of truth and forge-gen emits SDKs and docs from it, so a
    /// stale `BandKind` enum there ships into every generated artifact — which is
    /// how the five-arm list survived five new dimensions. This parses the real
    /// file (which also proves it is still valid YAML) and pins its enum, and its
    /// two `/bands/{kind}/…` gate routes, to the code.
    ///
    /// Tier: **CI forcing-function**. It pins the enum and the route set, not the
    /// prose around them — a `summary:` can still go stale without failing here.
    #[test]
    fn openapi_spec_band_kinds_match_the_code() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../spec/breathe.openapi.yaml");
        let raw =
            std::fs::read_to_string(path).expect("the spec is where the crate doc says it is");
        let spec: serde_yaml::Value =
            serde_yaml::from_str(&raw).expect("spec/breathe.openapi.yaml must be valid YAML");

        let declared: Vec<String> = spec["components"]["schemas"]["BandKind"]["enum"]
            .as_sequence()
            .expect("BandKind.enum is a sequence")
            .iter()
            .map(|v| v.as_str().expect("each enum value is a string").to_owned())
            .collect();
        let canonical: Vec<String> = DimensionId::ALL
            .iter()
            .map(|d| d.as_str().to_owned())
            .collect();
        assert_eq!(
            declared, canonical,
            "the OpenAPI BandKind enum has drifted from DimensionId::ALL"
        );

        // The authorization routes the surface actually serves must exist in the
        // spec, or a generated SDK cannot reach the gate that replaced dryRun.
        let paths = spec["paths"].as_mapping().expect("paths is a mapping");
        for route in [
            "/api/v1/bands/{kind}/{namespace}/{name}/write-intent",
            "/api/v1/bands/{kind}/{namespace}/{name}/confirm",
        ] {
            assert!(
                paths.contains_key(serde_yaml::Value::from(route)),
                "{route} is served by the router but absent from the spec"
            );
        }
    }

    /// The five proto enum values added this stage map to the five dimensions
    /// gRPC could not previously address — and the pre-existing numbers 1-5 are
    /// unchanged, so old clients keep working.
    #[test]
    fn grpc_band_kind_covers_every_dimension_without_renumbering() {
        use grpc::pb::BandKind as P;
        for (p, d) in [
            (P::Memory, DimensionId::Memory),
            (P::Cpu, DimensionId::Cpu),
            (P::Storage, DimensionId::Storage),
            (P::Replica, DimensionId::Replica),
            (P::Arc, DimensionId::Arc),
            (P::Cgroup, DimensionId::Cgroup),
            (P::CgroupCpu, DimensionId::CgroupCpu),
            (P::HostParam, DimensionId::HostParam),
            (P::KubeParam, DimensionId::KubeParam),
            (P::AppParam, DimensionId::AppParam),
            (P::Request, DimensionId::Request),
        ] {
            assert_eq!(grpc::kind_of(p as i32).unwrap(), d);
        }
        // the frozen wire numbers — renumbering these silently breaks every client
        assert_eq!(P::Memory as i32, 1);
        assert_eq!(P::Cpu as i32, 2);
        assert_eq!(P::Storage as i32, 3);
        assert_eq!(P::Arc as i32, 4);
        assert_eq!(P::Cgroup as i32, 5);
        // The eleventh kind took the next free number rather than reusing one.
        assert_eq!(P::Request as i32, 11);
    }

    /// The wire enum must stay a BIJECTION with `DimensionId::ALL`. The loop
    /// above proves every proto arm maps somewhere; this proves no dimension is
    /// missing from the proto — the direction that actually goes wrong, and the
    /// one that left five kinds unreachable over gRPC before.
    #[test]
    fn every_dimension_has_a_wire_number() {
        for d in DimensionId::ALL {
            let found = (1..=64).any(|n| grpc::kind_of(n).ok() == Some(d));
            assert!(
                found,
                "dimension {d} has no BandKind wire number — add one, never renumber"
            );
        }
    }
}
