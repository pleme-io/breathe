//! breathe-mcp stdio entrypoint. stdout owns the MCP JSON-RPC protocol, so all
//! tracing goes to stderr. The client is the in-cluster/default kube client —
//! run it as a sidecar in the breathe chart or locally against a kubeconfig.

use std::sync::Arc;

use anyhow::Result;
use breathe_mcp::{BreatheMcp, KubeStore};
use rmcp::{transport::stdio, ServiceExt};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("breathe_mcp=info")))
        .with_writer(std::io::stderr)
        .init();

    tracing::info!("breathe-mcp starting (stdio transport)");

    let store = KubeStore::from_env()
        .await
        .map_err(|e| anyhow::anyhow!("failed to initialise breathe store (kube client): {e}"))?;
    let server = BreatheMcp::new(Arc::new(store));

    let service = server.serve(stdio()).await.map_err(|e| anyhow::anyhow!("serve: {e}"))?;
    service.waiting().await.map_err(|e| anyhow::anyhow!("waiting: {e}"))?;

    tracing::info!("breathe-mcp exiting");
    Ok(())
}
