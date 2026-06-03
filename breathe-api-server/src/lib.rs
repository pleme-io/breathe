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

/// The gRPC surface (tonic) over the same facade. Replies carry the CRD JSON.
pub mod grpc {
    use super::{BandKind, SharedStore, StoreError};
    use tonic::{Request, Response, Status};

    tonic::include_proto!("breathe.v1");

    fn st(e: StoreError) -> Status {
        match e {
            StoreError::BadRequest(m) => Status::invalid_argument(m),
            other => Status::internal(other.to_string()),
        }
    }
    fn kind(s: &str) -> Result<BandKind, Status> {
        BandKind::parse(s).ok_or_else(|| Status::invalid_argument(format!("unknown band kind '{s}'")))
    }
    fn opt_ns(s: String) -> Option<String> {
        if s.is_empty() { None } else { Some(s) }
    }
    fn reply(v: serde_json::Value) -> Response<JsonReply> {
        Response::new(JsonReply { json: v.to_string() })
    }

    pub struct GrpcService {
        pub store: SharedStore,
    }

    #[tonic::async_trait]
    impl breathe_server::Breathe for GrpcService {
        async fn list_bands(&self, req: Request<ListBandsRequest>) -> Result<Response<JsonReply>, Status> {
            let r = req.into_inner();
            Ok(reply(self.store.list_bands(kind(&r.kind)?, opt_ns(r.namespace)).await.map_err(st)?))
        }
        async fn get_band(&self, req: Request<BandRefRequest>) -> Result<Response<JsonReply>, Status> {
            let r = req.into_inner();
            Ok(reply(self.store.get_band(kind(&r.kind)?, r.namespace, r.name).await.map_err(st)?))
        }
        async fn patch_band(&self, req: Request<PatchBandRequest>) -> Result<Response<JsonReply>, Status> {
            let r = req.into_inner();
            let spec: serde_json::Value = serde_json::from_str(&r.spec_json).map_err(|e| Status::invalid_argument(format!("spec_json: {e}")))?;
            Ok(reply(self.store.patch_band_spec(kind(&r.kind)?, r.namespace, r.name, spec).await.map_err(st)?))
        }
        async fn set_dry_run(&self, req: Request<SetDryRunRequest>) -> Result<Response<JsonReply>, Status> {
            let r = req.into_inner();
            Ok(reply(self.store.patch_band_spec(kind(&r.kind)?, r.namespace, r.name, serde_json::json!({ "dryRun": r.dry_run })).await.map_err(st)?))
        }
        async fn list_node_pools(&self, _req: Request<Empty>) -> Result<Response<JsonReply>, Status> {
            Ok(reply(self.store.list_pools().await.map_err(st)?))
        }
        async fn get_node_pool(&self, req: Request<PoolRefRequest>) -> Result<Response<JsonReply>, Status> {
            Ok(reply(self.store.get_pool(req.into_inner().name).await.map_err(st)?))
        }
        async fn set_write_enabled(&self, req: Request<SetWriteEnabledRequest>) -> Result<Response<JsonReply>, Status> {
            let r = req.into_inner();
            Ok(reply(self.store.patch_pool_spec(r.name, serde_json::json!({ "writeEnabled": r.write_enabled })).await.map_err(st)?))
        }
        async fn catalog(&self, _req: Request<Empty>) -> Result<Response<JsonReply>, Status> {
            Ok(reply(self.store.catalog()))
        }
    }

    #[must_use]
    pub fn server(store: SharedStore) -> breathe_server::BreatheServer<GrpcService> {
        breathe_server::BreatheServer::new(GrpcService { store })
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
        async fn get_band(&self, kind: BandKind, _ns: String, name: String) -> Result<Value, StoreError> {
            Ok(json!({ "kind": kind.as_str(), "name": name }))
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
    async fn catalog_route_returns_six_dimensions() {
        let app = router(Arc::new(MockStore::default()));
        let resp = app
            .oneshot(Request::builder().uri("/api/v1/catalog").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let v = body_json(resp).await;
        assert_eq!(v["dimensions"].as_array().unwrap().len(), 6);
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
    async fn grpc_set_write_enabled_patches_the_pool() {
        use grpc::breathe_server::Breathe;
        let mock = Arc::new(MockStore::default());
        let svc = grpc::GrpcService { store: mock.clone() };
        let resp = svc
            .set_write_enabled(tonic::Request::new(grpc::SetWriteEnabledRequest { name: "rio".into(), write_enabled: true }))
            .await
            .unwrap();
        assert!(resp.into_inner().json.contains("writeEnabled"));
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "writeEnabled": true }));
    }
}
