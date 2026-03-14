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
use async_openai::{
    config::OpenAIConfig,
    types::completions::CreateCompletionRequestArgs,
    Client,
};
use futures_util::StreamExt;
use std::time::Instant;

use inference_load_tracker::InferenceLoadTracker;
use scheduler::Scheduler;
use crate::Shared;

#[derive(Clone)]
pub struct ControllerState {
    pub shared: Arc<Shared>,
    pub scheduler: Arc<Scheduler>,
}

impl ControllerState {
    pub fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            scheduler: Arc::new(Scheduler::new()),
        }
    }
}

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

/// 请求结束时自动对本次涉及的 token_hashes 做 concurrency_counter 扣减
struct TokenConcurrencyGuard {
    shared: Arc<Shared>,
    token_hashes: Vec<[u8; 32]>,
}

impl Drop for TokenConcurrencyGuard {
    fn drop(&mut self) {
        for &h in &self.token_hashes {
            self.shared.concurrency_counter.decrement(h);
        }
    }
}

/// 客户端 POST 的 JSON 请求体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferRequest {
    /// 模型名称
    pub model_name: String,
    /// 模型路径
    pub model_path: String,
    /// 输入 prompt
    pub prompt: String,
    /// 最大生成 token 数
    pub max_tokens: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInferRequest {
    /// 模型路径
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

/// Performance 接口请求体：使用 async-openai 直接测量 LLM 性能
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceRequest {
    /// 模型名称
    pub model_name: String,
    /// 模型路径
    pub model_path: String,
    /// 输入 prompt
    pub prompt: String,
    /// 最大生成 token 数
    pub max_tokens: u16,
}

/// Performance 接口响应体：在 infer 功能基础上增加性能指标
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceResponse {
    /// 是否成功
    pub success: bool,
    /// 生成结果
    pub result: Option<String>,
    /// 被调度实例的 URL
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_url: Option<String>,
    /// 首 token 延迟（Time To First Token），单位秒
    pub ttft: f64,
    /// 生成时长（从首 token 到末 token），单位秒
    pub generation_time: f64,
    /// 总耗时（从请求发出到最后一个 token），单位秒
    pub total_time: f64,
    /// 提示词 token 数
    pub prompt_tokens: u32,
    /// 生成 token 数
    pub completion_tokens: u32,
    /// 错误信息
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// POST /infer：接收 { model, prompt, max_tokens }，按各实例当前未完成请求长度总和最小做负载均衡，转发并返回 JSON
pub async fn infer(
    State(state): State<Arc<ControllerState>>,
    Json(payload): Json<InferRequest>,
) -> Result<Json<InferResponse>, (StatusCode, Json<InferResponse>)> {
    let shared = &state.shared;
    let prompt_len = payload.prompt.len();
    info!(
        "infer request: model_name={}, model_path={},prompt_len={}, max_tokens={}",
        payload.model_name, payload.model_path, prompt_len, payload.max_tokens
    );

    // 选取当前未完成推理请求长度总和最小的实例
    // let server = match scheduler::select_server_by_workload(&shared, payload.model_name.as_str()).await
    // {
    //     Some(s) => {
    //         info!(
    //             "infer schedule selected server_id={} url={} model={}",
    //             s.id, s.url, s.model_name
    //         );
    //         s
    //     }
    //     None => {
    //         warn!(
    //             "no data server available for model={}, prompt_len={}",
    //             payload.model_name, prompt_len
    //         );
    //         return Err((
    //             StatusCode::SERVICE_UNAVAILABLE,
    //             Json(InferResponse {
    //                 success: false,
    //                 result: None,
    //                 error: Some("no data server available for this model".to_string()),
    //             }),
    //         ));
    //     }
    // };
    let (server, token_hashes) = match state.scheduler.select_server_by_kvcache(shared, payload.model_name.as_str(), payload.prompt.as_str()).await
    {
        Some((s, hashes)) => {
            info!(
                "infer schedule selected server_id={} url={} model={}",
                s.id, s.url, s.model_name
            );
            (s, hashes)
        }
        None => {
            warn!(
                "no data server available for model={}, prompt_len={}",
                payload.model_name, prompt_len
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

    // 使用调度阶段已计算的 token_hashes，请求开始 +1，请求结束由 guard 扣减
    let _token_guard = if let Some(ref hashes) = token_hashes {
        if !hashes.is_empty() {
            let replica_counts = shared.get_replica_counts(hashes.clone());
            let items: Vec<([u8; 32], u32)> = hashes
                .iter()
                .zip(replica_counts.iter())
                .map(|(&h, &r)| (h, r))
                .collect();
            shared.increment_concurrency_and_maybe_migrate(&items);
            Some(TokenConcurrencyGuard {
                shared: shared.clone(),
                token_hashes: hashes.clone(),
            })
        } else {
            None
        }
    } else {
        None
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

    let instance_payload = InstanceInferRequest {
        model: payload.model_path.clone(),
        prompt: payload.prompt.clone(),
        max_tokens: payload.max_tokens,
    };
    let response = client
        .post(&url)
        .json(&instance_payload)
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

pub async fn performance(
    State(state): State<Arc<ControllerState>>,
    Json(payload): Json<PerformanceRequest>,
) -> Result<Json<PerformanceResponse>, (StatusCode, Json<PerformanceResponse>)> {
    let shared = &state.shared;
    let prompt_len = payload.prompt.len();
    info!(
        "infer request: model_name={}, model_path={},prompt_len={}, max_tokens={}",
        payload.model_name, payload.model_path, prompt_len, payload.max_tokens
    );
    let (server, token_hashes) = match state.scheduler.select_server_hybrid(shared, payload.model_name.as_str(), payload.prompt.as_str()).await
    {
        Some((s, hashes)) => {
            info!(
                "infer schedule selected server_id={} url={} model={}",
                s.id, s.url, s.model_name
            );
            (s, hashes)
        }
        None => {
            warn!(
                "no data server available for model={}, prompt_len={}",
                payload.model_name, prompt_len
            );
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(PerformanceResponse {
                    success: false,
                    result: None,
                    server_url: None,
                    ttft: 0.0,
                    generation_time: 0.0,
                    total_time: 0.0,
                    prompt_tokens: 0,
                    completion_tokens: 0,
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

    // 使用调度阶段已计算的 token_hashes，请求开始 +1，请求结束由 guard 扣减
    let _token_guard = if let Some(ref hashes) = token_hashes {
        if !hashes.is_empty() {
            let replica_counts = shared.get_replica_counts(hashes.clone());
            let items: Vec<([u8; 32], u32)> = hashes
                .iter()
                .zip(replica_counts.iter())
                .map(|(&h, &r)| (h, r))
                .collect();
            shared.increment_concurrency_and_maybe_migrate(&items);
            Some(TokenConcurrencyGuard {
                shared: shared.clone(),
                token_hashes: hashes.clone(),
            })
        } else {
            None
        }
    } else {
        None
    };

    // 创建 async-openai client
    let config = OpenAIConfig::new()
        .with_api_base(server.url.clone());

    let client = Client::with_config(config);

    // ===============================
    // 构建 completion 请求
    // ===============================

    let request = CreateCompletionRequestArgs::default()
        .model(payload.model_path.clone())
        .prompt(payload.prompt.clone())
        .max_tokens(payload.max_tokens)
        .stream(true)
        .build()
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(PerformanceResponse {
                    success: false,
                    result: None,
                    server_url: Some(server.url.clone()),
                    ttft: 0.0,
                    generation_time: 0.0,
                    total_time: 0.0,
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    error: Some(format!("request build failed {}", e)),
                }),
            )
        })?;

    // ===============================
    // 时间统计
    // ===============================

    let start_time = Instant::now();

    // first_token_time：仅在收到第一个非空 token 时赋值一次
    // last_token_time：仅在循环结束后根据最后一次记录的时刻赋值，不在循环内反复写
    let mut first_token_time: Option<Instant> = None;
    let mut last_text_token_time: Option<Instant> = None;

    let mut response_text = String::new();

    let mut prompt_tokens = 0;
    let mut completion_tokens = 0;

    // ===============================
    // 创建 streaming
    // ===============================

    let mut stream = client
        .completions()
        .create_stream(request)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                Json(PerformanceResponse {
                    success: false,
                    result: None,
                    server_url: Some(server.url.clone()),
                    ttft: 0.0,
                    generation_time: 0.0,
                    total_time: 0.0,
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    error: Some(format!("stream create failed {}", e)),
                }),
            )
        })?;

    // ===============================
    // 接收 token stream
    // ===============================

    while let Some(chunk) = stream.next().await {

        let chunk = chunk.map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                Json(PerformanceResponse {
                    success: false,
                    result: None,
                    server_url: Some(server.url.clone()),
                    ttft: 0.0,
                    generation_time: 0.0,
                    total_time: 0.0,
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    error: Some(format!("stream chunk error {}", e)),
                }),
            )
        })?;

        // usage 信息只做 token 计数，不影响时间统计
        if let Some(usage) = chunk.usage {
            prompt_tokens = usage.prompt_tokens;
            completion_tokens = usage.completion_tokens;
        }

        if chunk.choices.is_empty() {
            continue;
        }

        let text = &chunk.choices[0].text;

        if !text.is_empty() {
            let now = Instant::now();
            // 仅第一个非空 token 时赋值
            if first_token_time.is_none() {
                first_token_time = Some(now);
            }
            // 持续更新，循环结束后保留最后一次
            last_text_token_time = Some(now);
            response_text.push_str(text);
        }
    }

    // ===============================
    // 计算性能指标
    // ===============================

    // ttft：从发出请求到收到第一个 token 的时间
    let ttft = first_token_time
        .map(|t| t.duration_since(start_time).as_secs_f64())
        .unwrap_or(0.0);

    // generation_time：从第一个 token 到最后一个 token 的时间（纯生成阶段）
    let generation_time = match (first_token_time, last_text_token_time) {
        (Some(first), Some(last)) => last.duration_since(first).as_secs_f64(),
        _ => 0.0,
    };

    // total_time：从发出请求到最后一个 token 的总时间
    let total_time = last_text_token_time
        .unwrap_or_else(Instant::now)
        .duration_since(start_time)
        .as_secs_f64();

    Ok(Json(PerformanceResponse {
        success: true,
        result: Some(response_text),
        server_url: Some(server.url.clone()),
        ttft,
        generation_time,
        total_time,
        prompt_tokens,
        completion_tokens,
        error: None,
    }))
}