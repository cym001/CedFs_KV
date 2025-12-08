use reqwest;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TransferRequest {
    hashes: Vec<[u8;32]>,
    old_position: (String, String),
    offsets: Vec<i64>,
    peer_ip: String,
    peer_init_port: i32,
    do_copy: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransferResponse {
    pub num_tokens: i32,
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

    // 发送 transfer 请求
    pub async fn send_transfer_request(
        &self,
        hashes: Vec<[u8;32]>,
        old_position: (String, String),
        offsets: Vec<i64>,
        peer_ip: String,
        peer_init_port: i32,
        do_copy: bool,
    ) -> Result<TransferResponse, reqwest::Error> {
        let request_body = TransferRequest {
            hashes,
            old_position,
            offsets,
            peer_ip,
            peer_init_port,
            do_copy,
        };

        let url = format!("{}/Transfer", self.base_url);
        
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await?;

        let transfer_response = response.json::<TransferResponse>().await?;
        Ok(transfer_response)
    }
}
