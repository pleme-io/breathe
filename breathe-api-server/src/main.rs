//! breathe-api-server entrypoint. One process, three surfaces over one facade:
//! REST + GraphQL (`/graphql`) on `BREATHE_API_BIND` (default 0.0.0.0:8080), and
//! gRPC on `BREATHE_GRPC_BIND` (default 0.0.0.0:8081). All drive the real
//! `KubeStore`.

use std::sync::Arc;

use breathe_facade::KubeStore;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // See breathe-controller/src/main.rs for the full story -- same
    // workspace-wide ambiguous-CryptoProvider panic risk, same fix,
    // applied here too before this binary's own first TLS use
    // (`KubeStore::from_env()` below).
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install_default() should only be called once, at startup");

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,breathe_api_server=info")))
        .init();

    let http_bind = std::env::var("BREATHE_API_BIND").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let grpc_bind = std::env::var("BREATHE_GRPC_BIND").unwrap_or_else(|_| "0.0.0.0:8081".to_string());

    let store = Arc::new(KubeStore::from_env().await.map_err(|e| format!("kube client: {e}"))?);
    let app = breathe_api_server::router(store.clone());
    let grpc = breathe_api_server::grpc::server(store);

    let grpc_addr: std::net::SocketAddr = grpc_bind.parse()?;
    let listener = tokio::net::TcpListener::bind(&http_bind).await?;
    tracing::info!(http = %http_bind, grpc = %grpc_bind, "breathe-api-server listening (REST + GraphQL + gRPC)");

    let http_task = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "http server exited");
        }
    });
    let grpc_task = tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder().add_service(grpc).serve(grpc_addr).await {
            tracing::error!(error = %e, "grpc server exited");
        }
    });

    let _ = tokio::join!(http_task, grpc_task);
    Ok(())
}
