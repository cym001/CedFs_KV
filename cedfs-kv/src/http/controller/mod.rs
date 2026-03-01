//! HTTP 控制器：处理 model/prompt/max_tokens 类型的推理请求

mod scheduler;
pub mod inference_load_tracker;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use inference_load_tracker::InferenceLoadTracker;
use crate::Shared;

/// 请求结束时自动从该实例减去本次 prompt 长度，避免遗漏
struct InferenceLoadGuard {
    tracker: Arc<InferenceLoadTracker>,
    server_key: u32,
    prompt_len: usize,
}

impl Drop for InferenceLoadGuard {
    fn drop(&mut self) {
        self.tracker.sub_load(&self.server_key, self.prompt_len);
    }
}

/// 客户端 POST 的 JSON 请求体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferRequest {
    /// 模型路径或名称
    pub model: String,
    /// 输入 prompt
    pub prompt: String,
    /// 最大生成 token 数
    pub max_tokens: u32,
}

/// 返回给客户端的 JSON 响应（与 Python 端 response.json() 对应）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferResponse {
    /// 是否成功
    pub success: bool,
    /// 生成结果或错误信息
    pub result: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// 下游 /v1/completions 等返回的 OpenAI 风格 completion 响应（仅解析所需字段）
#[derive(Debug, Deserialize)]
struct OpenAICompletionChoice {
    text: String,
}

#[derive(Debug, Deserialize)]
struct OpenAICompletionResponse {
    choices: Vec<OpenAICompletionChoice>,
}

/// 将下游响应体解析为 InferResponse：先尝试本协议，再尝试 OpenAI completion 格式
fn parse_infer_response(body: &str) -> Result<InferResponse, serde_json::Error> {
    if let Ok(r) = serde_json::from_str::<InferResponse>(body) {
        return Ok(r);
    }
    let openai: OpenAICompletionResponse = serde_json::from_str(body)?;
    let text = openai
        .choices
        .first()
        .map(|c| c.text.clone())
        .unwrap_or_default();
    Ok(InferResponse {
        success: true,
        result: Some(text),
        error: None,
    })
}

/// POST /infer：接收 { model, prompt, max_tokens }，按各实例当前未完成请求长度总和最小做负载均衡，转发并返回 JSON
pub async fn infer(
    State(shared): State<Arc<Shared>>,
    Json(payload): Json<InferRequest>,
) -> Result<Json<InferResponse>, (StatusCode, Json<InferResponse>)> {
    let prompt_len = payload.prompt.len();
    info!(
        "infer request: model={}, prompt_len={}, max_tokens={}",
        payload.model, prompt_len, payload.max_tokens
    );

    // 选取当前未完成推理请求长度总和最小的实例
    let server = match scheduler::select_server(
        &shared.local_data_server_collect,
        &shared.inference_load_tracker,
        Some(payload.model.as_str()),
    )
    .await
    {
        Some(s) => s,
        None => {
            warn!(
                "no data server available for model={}, prompt_len={}",
                payload.model, prompt_len
            );
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(InferResponse {
                    success: false,
                    result: None,
                    error: Some("no data server available for this model".to_string()),
                }),
            ));
        }
    };

    // 记录本次请求长度，返回时由 InferenceLoadGuard 自动扣减
    shared
        .inference_load_tracker
        .add_load(server.id, prompt_len);
    let _load_guard = InferenceLoadGuard {
        tracker: shared.inference_load_tracker.clone(),
        server_key: server.id,
        prompt_len,
    };

    //todo()错误处理
    let url = server.url;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .map_err(|e| {
            warn!("reqwest client build failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(InferResponse {
                    success: false,
                    result: None,
                    error: Some(format!("client build failed: {}", e)),
                }),
            )
        })?;

    let response = client
        .post(&url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| {
            warn!("infer forward to {} failed: {}", url, e);
            (
                StatusCode::BAD_GATEWAY,
                Json(InferResponse {
                    success: false,
                    result: None,
                    error: Some(format!("forward failed: {}", e)),
                }),
            )
        })?;

    let body = response.text().await.map_err(|e| {
        warn!("infer response body read from {} failed: {}", url, e);
        (
            StatusCode::BAD_GATEWAY,
            Json(InferResponse {
                success: false,
                result: None,
                error: Some(format!("response read failed: {}", e)),
            }),
        )
    })?;

    let result: InferResponse = parse_infer_response(&body).map_err(|e| {
        warn!(
            "infer response parse from {} failed: {}, response body: {}",
            url, e, body
        );
        (
            StatusCode::BAD_GATEWAY,
            Json(InferResponse {
                success: false,
                result: None,
                error: Some(format!("response parse failed: {}", e)),
            }),
        )
    })?;

    Ok(Json(result))
}
