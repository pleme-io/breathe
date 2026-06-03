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

/// The full router. `store` is the shared facade (real `KubeStore` or a mock).
#[must_use]
pub fn router(store: SharedStore) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/api/v1/catalog", get(catalog))
        .route("/api/v1/bands/:kind", get(list_bands))
        .route("/api/v1/bands/:kind/:namespace/:name", get(get_band).patch(patch_band))
        .route("/api/v1/bands/:kind/:namespace/:name/dry-run", patch(set_dry_run))
        .route("/api/v1/nodepools", get(list_pools))
        .route("/api/v1/nodepools/:name", get(get_pool))
        .route("/api/v1/nodepools/:name/write-enabled", patch(set_write_enabled))
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
}
