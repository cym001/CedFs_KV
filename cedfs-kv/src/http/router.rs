use axum::extract::DefaultBodyLimit;
use axum::routing::post;
use axum::Router;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

use super::controller;
use super::controller::ControllerState;

/// 构建 HTTP 路由：POST 接收 { model, prompt, max_tokens }，返回 JSON
pub fn build(state: Arc<ControllerState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/infer", post(controller::infer))
        .route("/performance", post(controller::performance))
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024)) // 16MB
        .layer(cors)
        .with_state(state)
}