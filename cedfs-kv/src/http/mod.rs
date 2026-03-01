use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

use crate::Shared;

pub mod controller;
mod router;

pub use controller::{InferRequest, InferResponse};

/// 默认 HTTP 服务端口（与 Python 客户端对接）
const DEFAULT_HTTP_PORT: u16 = 8080;

/// 启动 HTTP 服务，处理 POST JSON：{ model, prompt, max_tokens }
/// 客户端可请求 POST /infer，Content-Type: application/json，超时建议 300s
pub async fn serve(shared: Arc<Shared>, port: Option<u16>) {
    let port = port.unwrap_or(DEFAULT_HTTP_PORT);
    let app = router::build(shared);
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await.unwrap();
    info!("HTTP server listening on {}", addr);
    axum::serve(listener, app).await.unwrap();
}
