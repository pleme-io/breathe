//! `breathe-api-server` — the REST surface over the [`breathe_facade`] core.
//!
//! Every route is a thin call into the same `BreatheStore` the MCP drives, so the
//! two surfaces can never diverge. The path/verb set mirrors
//! `spec/breathe.openapi.yaml` (the source of truth); the handlers are kube-rs
//! facade calls — the one place breathe legitimately hand-serves (a generic
//! `kube::Api<T>` dispatch that forge-gen's HTTP-client model fits poorly).

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, patch},
    Json, Router,
};
use breathe_facade::{BandKind, BreatheStore, StoreError};
use serde::Deserialize;
use serde_json::{json, Value};

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
        .route("/api/v1/bands/:kind/:namespace/:name", get(get_band).patch(patch_band))
        .route("/api/v1/bands/:kind/:namespace/:name/dry-run", patch(set_dry_run))
        .route("/api/v1/nodepools", get(list_pools))
        .route("/api/v1/nodepools/:name", get(get_pool))
        .route("/api/v1/nodepools/:name/write-enabled", patch(set_write_enabled))
        .route_service("/graphql", async_graphql_axum::GraphQL::new(schema))
        .with_state(store)
}

/// Map a facade result to an HTTP response.
fn respond(r: Result<Value, StoreError>) -> Response {
    match r {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(StoreError::BadRequest(m)) => (StatusCode::BAD_REQUEST, Json(json!({ "error": m }))).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({ "error": e.to_string() }))).into_response(),
    }
}

fn kind_or_400(s: &str) -> Result<BandKind, Response> {
    BandKind::parse(s).ok_or_else(|| {
        (StatusCode::BAD_REQUEST, Json(json!({ "error": format!("unknown band kind '{s}'") }))).into_response()
    })
}

#[derive(Deserialize)]
struct NsQuery {
    namespace: Option<String>,
}

async fn catalog(State(store): State<SharedStore>) -> Response {
    (StatusCode::OK, Json(store.catalog())).into_response()
}

async fn list_bands(State(store): State<SharedStore>, Path(kind): Path<String>, Query(q): Query<NsQuery>) -> Response {
    match kind_or_400(&kind) {
        Ok(k) => respond(store.list_bands(k, q.namespace).await),
        Err(r) => r,
    }
}

async fn get_band(State(store): State<SharedStore>, Path((kind, ns, name)): Path<(String, String, String)>) -> Response {
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

async fn set_dry_run(
    State(store): State<SharedStore>,
    Path((kind, ns, name)): Path<(String, String, String)>,
    Json(b): Json<DryRunBody>,
) -> Response {
    match kind_or_400(&kind) {
        Ok(k) => respond(store.patch_band_spec(k, ns, name, json!({ "dryRun": b.dry_run })).await),
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
    respond(store.patch_pool_spec(name, json!({ "writeEnabled": b.write_enabled })).await)
}

// ───────────────────────────── GraphQL ─────────────────────────────

/// The GraphQL surface (async-graphql) over the same facade. Resolvers return
/// the CRD JSON via the `Json` scalar so there is one typed source of truth.
pub mod graphql {
    use super::{BandKind, SharedStore, StoreError};
    use async_graphql::{Context, EmptySubscription, Json, Object, Schema};
    use serde_json::Value;

    fn gql(e: StoreError) -> async_graphql::Error {
        async_graphql::Error::new(e.to_string())
    }
    fn parse_kind(s: &str) -> async_graphql::Result<BandKind> {
        BandKind::parse(s).ok_or_else(|| async_graphql::Error::new(format!("unknown band kind '{s}'")))
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
        async fn bands(&self, ctx: &Context<'_>, kind: String, namespace: Option<String>) -> async_graphql::Result<Json<Value>> {
            Ok(Json(store(ctx)?.list_bands(parse_kind(&kind)?, namespace).await.map_err(gql)?))
        }
        /// One band CR.
        async fn band(&self, ctx: &Context<'_>, kind: String, namespace: String, name: String) -> async_graphql::Result<Json<Value>> {
            Ok(Json(store(ctx)?.get_band(parse_kind(&kind)?, namespace, name).await.map_err(gql)?))
        }
        /// All node pools.
        async fn nodepools(&self, ctx: &Context<'_>) -> async_graphql::Result<Json<Value>> {
            Ok(Json(store(ctx)?.list_pools().await.map_err(gql)?))
        }
        async fn nodepool(&self, ctx: &Context<'_>, name: String) -> async_graphql::Result<Json<Value>> {
            Ok(Json(store(ctx)?.get_pool(name).await.map_err(gql)?))
        }
    }

    pub struct Mutation;
    #[Object]
    impl Mutation {
        /// Merge-patch a band's spec.
        async fn patch_band(&self, ctx: &Context<'_>, kind: String, namespace: String, name: String, spec: Json<Value>) -> async_graphql::Result<Json<Value>> {
            Ok(Json(store(ctx)?.patch_band_spec(parse_kind(&kind)?, namespace, name, spec.0).await.map_err(gql)?))
        }
        /// Flip a band's dryRun (one of the two safety gates).
        async fn set_dry_run(&self, ctx: &Context<'_>, kind: String, namespace: String, name: String, dry_run: bool) -> async_graphql::Result<Json<Value>> {
            Ok(Json(store(ctx)?.patch_band_spec(parse_kind(&kind)?, namespace, name, serde_json::json!({ "dryRun": dry_run })).await.map_err(gql)?))
        }
        /// Flip a node's writeEnabled master switch (the node-level safety gate).
        async fn set_write_enabled(&self, ctx: &Context<'_>, name: String, write_enabled: bool) -> async_graphql::Result<Json<Value>> {
            Ok(Json(store(ctx)?.patch_pool_spec(name, serde_json::json!({ "writeEnabled": write_enabled })).await.map_err(gql)?))
        }
    }

    pub type BreatheSchema = Schema<Query, Mutation, EmptySubscription>;

    #[must_use]
    pub fn schema(store: SharedStore) -> BreatheSchema {
        Schema::build(Query, Mutation, EmptySubscription).data(store).finish()
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
    use super::{BandKind, SharedStore, StoreError};
    use serde_json::Value;
    use tonic::{Request, Response, Status};

    /// The generated proto messages, enums, and the service definition, plus the
    /// pbjson serde impls. A submodule so the proto `BandKind` enum doesn't clash
    /// with the facade's [`super::BandKind`].
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
        serde_json::from_value(v)
            .map_err(|e| Status::internal(format!("response did not match the typed schema (spec drift?): {e}")))
    }

    fn opt_ns(s: String) -> Option<String> {
        if s.is_empty() { None } else { Some(s) }
    }

    /// proto `BandKind` (an `i32` enum on the wire) → the facade's [`BandKind`].
    fn kind_of(k: i32) -> Result<BandKind, Status> {
        use pb::BandKind as P;
        match P::try_from(k) {
            Ok(P::Memory) => Ok(BandKind::Memory),
            Ok(P::Cpu) => Ok(BandKind::Cpu),
            Ok(P::Storage) => Ok(BandKind::Storage),
            Ok(P::Arc) => Ok(BandKind::Arc),
            Ok(P::Cgroup) => Ok(BandKind::Cgroup),
            _ => Err(Status::invalid_argument("band kind unspecified")),
        }
    }

    pub struct GrpcService {
        pub store: SharedStore,
    }

    #[tonic::async_trait]
    impl pb::breathe_server::Breathe for GrpcService {
        async fn band_list(&self, req: Request<pb::BandListRequest>) -> Result<Response<pb::BandListResponse>, Status> {
            let r = req.into_inner();
            let v = self.store.list_bands(kind_of(r.kind)?, opt_ns(r.namespace)).await.map_err(st)?;
            Ok(Response::new(pb::BandListResponse { items: typed(v)? }))
        }
        async fn band_get(&self, req: Request<pb::BandGetRequest>) -> Result<Response<pb::Band>, Status> {
            let r = req.into_inner();
            let v = self.store.get_band(kind_of(r.kind)?, r.namespace, r.name).await.map_err(st)?;
            Ok(Response::new(typed(v)?))
        }
        async fn band_patch(&self, req: Request<pb::BandPatchRequest>) -> Result<Response<pb::Band>, Status> {
            let r = req.into_inner();
            // BandSpec scalars are proto3 `optional` (field presence): pbjson emits
            // the fields the client SET (Some — incl. a zero like dryRun=false) and
            // omits the rest (None) → correct RFC-7386 merge: present writes, absent
            // leaves unchanged. (Without presence, proto3 would drop zero values.)
            let spec = serde_json::to_value(r.body.unwrap_or_default()).map_err(|e| Status::internal(e.to_string()))?;
            let v = self.store.patch_band_spec(kind_of(r.kind)?, r.namespace, r.name, spec).await.map_err(st)?;
            Ok(Response::new(typed(v)?))
        }
        async fn band_set_dry_run(&self, req: Request<pb::BandSetDryRunRequest>) -> Result<Response<pb::Band>, Status> {
            let r = req.into_inner();
            let v = self.store.patch_band_spec(kind_of(r.kind)?, r.namespace, r.name, serde_json::json!({ "dryRun": r.dry_run })).await.map_err(st)?;
            Ok(Response::new(typed(v)?))
        }
        async fn catalog_list(&self, _req: Request<pb::CatalogListRequest>) -> Result<Response<pb::Catalog>, Status> {
            Ok(Response::new(typed(self.store.catalog())?))
        }
        async fn nodepool_list(&self, _req: Request<pb::NodepoolListRequest>) -> Result<Response<pb::NodepoolListResponse>, Status> {
            let v = self.store.list_pools().await.map_err(st)?;
            Ok(Response::new(pb::NodepoolListResponse { items: typed(v)? }))
        }
        async fn nodepool_get(&self, req: Request<pb::NodepoolGetRequest>) -> Result<Response<pb::NodePool>, Status> {
            let v = self.store.get_pool(req.into_inner().name).await.map_err(st)?;
            Ok(Response::new(typed(v)?))
        }
        async fn nodepool_set_write_enabled(&self, req: Request<pb::NodepoolSetWriteEnabledRequest>) -> Result<Response<pb::NodePool>, Status> {
            let r = req.into_inner();
            let v = self.store.patch_pool_spec(r.name, serde_json::json!({ "writeEnabled": r.write_enabled })).await.map_err(st)?;
            Ok(Response::new(typed(v)?))
        }
        async fn healthz(&self, _req: Request<pb::HealthzRequest>) -> Result<Response<::pbjson_types::Empty>, Status> {
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
        async fn list_bands(&self, kind: BandKind, _ns: Option<String>) -> Result<Value, StoreError> {
            Ok(json!([{ "kind": kind.as_str() }]))
        }
        async fn get_band(&self, _kind: BandKind, ns: String, name: String) -> Result<Value, StoreError> {
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
        async fn patch_band_spec(&self, _k: BandKind, _ns: String, name: String, spec: Value) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, spec.clone()));
            Ok(json!({ "spec": spec }))
        }
        async fn list_pools(&self) -> Result<Value, StoreError> {
            Ok(json!([{ "metadata": { "name": "rio" }, "spec": { "nodeName": "rio", "arcMaxGiB": 6 } }]))
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
        fn catalog(&self) -> Value {
            breathe_facade::catalog_json()
        }
    }

    async fn body_json(resp: Response) -> Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn set_dry_run_route_patches_the_spec() {
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
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "dryRun": false }));
    }

    #[tokio::test]
    async fn unknown_band_kind_is_400() {
        let app = router(Arc::new(MockStore::default()));
        let resp = app
            .oneshot(Request::builder().uri("/api/v1/bands/bogus").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn catalog_route_returns_all_dimensions() {
        let app = router(Arc::new(MockStore::default()));
        let resp = app
            .oneshot(Request::builder().uri("/api/v1/catalog").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v = body_json(resp).await;
        assert_eq!(v["dimensions"].as_array().unwrap().len(), 8);
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
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "writeEnabled": true }));
    }

    #[tokio::test]
    async fn graphql_set_dry_run_mutation_patches_the_band() {
        let mock = Arc::new(MockStore::default());
        let schema = graphql::schema(mock.clone());
        let resp = schema
            .execute(r#"mutation { setDryRun(kind:"arc", namespace:"pangea-system", name:"rio-arc", dryRun:false) }"#)
            .await;
        assert!(resp.errors.is_empty(), "{:?}", resp.errors);
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "dryRun": false }));
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
        let svc = grpc::GrpcService { store: mock.clone() };
        let resp = svc
            .nodepool_set_write_enabled(tonic::Request::new(grpc::pb::NodepoolSetWriteEnabledRequest { name: "rio".into(), write_enabled: true }))
            .await
            .unwrap();
        // the response is a TYPED NodePool, not a JSON string envelope.
        assert!(resp.into_inner().spec.unwrap().write_enabled);
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "writeEnabled": true }));
    }

    #[tokio::test]
    async fn grpc_band_get_returns_typed_band() {
        use grpc::pb::breathe_server::Breathe;
        let svc = grpc::GrpcService { store: Arc::new(MockStore::default()) };
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
        let svc = grpc::GrpcService { store: mock.clone() };
        let body = grpc::pb::BandSpec { dry_run: Some(false), ..Default::default() };
        svc.band_patch(tonic::Request::new(grpc::pb::BandPatchRequest {
            kind: grpc::pb::BandKind::Arc as i32,
            namespace: "pangea-system".into(),
            name: "rio-arc".into(),
            body: Some(body),
        }))
        .await
        .unwrap();
        // exactly the one set field reached the facade — no other scalars leaked in.
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "dryRun": false }));
    }

    #[tokio::test]
    async fn grpc_band_list_returns_typed_items() {
        use grpc::pb::breathe_server::Breathe;
        let svc = grpc::GrpcService { store: Arc::new(MockStore::default()) };
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
        let svc = grpc::GrpcService { store: Arc::new(MockStore::default()) };
        let resp = svc.catalog_list(tonic::Request::new(grpc::pb::CatalogListRequest {})).await.unwrap();
        let cat = resp.into_inner();
        assert_eq!(cat.dimensions.len(), 8);
        assert!(cat.dimensions.iter().any(|d| d.id == "arc" && d.is_host));
        assert!(cat.dimensions.iter().any(|d| d.id == "cgroup-cpu" && d.is_host));
        assert!(cat.dimensions.iter().any(|d| d.id == "memory" && !d.is_host));
    }

    #[tokio::test]
    async fn grpc_set_dry_run_returns_typed_band_and_patches() {
        use grpc::pb::breathe_server::Breathe;
        let mock = Arc::new(MockStore::default());
        let svc = grpc::GrpcService { store: mock.clone() };
        let resp = svc
            .band_set_dry_run(tonic::Request::new(grpc::pb::BandSetDryRunRequest {
                kind: grpc::pb::BandKind::Arc as i32,
                namespace: "pangea-system".into(),
                name: "rio-arc".into(),
                dry_run: false,
            }))
            .await
            .unwrap();
        // patched band comes back typed; the facade recorded the dryRun merge-patch.
        assert_eq!(resp.into_inner().spec.unwrap().dry_run, Some(false));
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "dryRun": false }));
    }
}
