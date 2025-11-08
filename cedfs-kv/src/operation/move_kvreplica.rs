use reqwest;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct MoveRequest {
    old_position: Vec<String>,
    new_position: Vec<String>,
    tokens: Vec<i64>,
}

pub struct MoveKVReplicaOp {
    client: reqwest::Client,
    base_url: String,
}

impl MoveKVReplicaOp {
    // 构造函数
    pub fn new(base_url: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.to_string(),
        }
    }

    // 发送 move 请求
    pub async fn send_move_request(
        &self,
        old_position: Vec<String>,
        new_position: Vec<String>,
        tokens: Vec<i64>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let request_body = MoveRequest {
            old_position,
            new_position,
            tokens,
        };

        let url = format!("{}/move", self.base_url);
        
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await?;

        Ok(response)
    }

    // 获取响应文本
    pub async fn get_response_text(response: reqwest::Response) -> Result<String, reqwest::Error> {
        response.text().await
    }
}
