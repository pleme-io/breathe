//! breathe-api-server entrypoint. Serves the REST surface over the shared facade
//! (real `KubeStore`). Bind from `BREATHE_API_BIND` (default `0.0.0.0:8080`).

use std::sync::Arc;

use breathe_facade::KubeStore;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,breathe_api_server=info")))
        .init();

    let bind = std::env::var("BREATHE_API_BIND").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let store = Arc::new(KubeStore::from_env().await.map_err(|e| format!("kube client: {e}"))?);
    let app = breathe_api_server::router(store);

    tracing::info!(%bind, "breathe-api-server listening");
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
