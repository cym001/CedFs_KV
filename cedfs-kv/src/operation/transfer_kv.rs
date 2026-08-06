use cedfs_proto::lmcache::lmcache_server_client::LmcacheServerClient;
use cedfs_proto::lmcache::{TransferKvRequest, TransferKvResponse};
use cedfs_proto::lmcache_v2::lmcache_server_v2_client::LmcacheServerV2Client;
use cedfs_proto::lmcache_v2::{TransferKvV2Request, TransferKvV2Response};
use std::time::Duration;
use tonic::Request;

pub const KV_TRANSFER_NOT_FOUND: i32 = -1;
pub const KV_TRANSFER_FAILED: i32 = -2;
pub const KV_TRANSFER_ALREADY_SATISFIED: i32 = 2_147_483_647;

/// TransferKv 状态码语义：
/// - `KV_TRANSFER_NOT_FOUND`: source worker 找不到 requested chunks。
/// - `KV_TRANSFER_FAILED`: read 和目标端 existing 检查后仍然没有任何 satisfied chunk。
/// - `KV_TRANSFER_ALREADY_SATISFIED`: 目标端已经拥有所有 requested chunks。
/// - 其它正数：satisfied chunks 数量。
///
/// source worker 按 `num_read_chunks + num_existing_chunks` 计算 satisfied chunks。

pub struct TransferKvOp {
    base_url: String,
}

#[derive(Debug, Clone, Copy)]
pub struct TransferV2Limits {
    pub max_blocks: usize,
    pub max_tokens: u64,
    pub max_bytes: u64,
    pub estimated_bytes_per_token: u64,
    pub timeout: Duration,
}

impl TransferKvOp {
    /// 构造函数
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.to_string(),
        }
    }

    /// 发送 transfer_kv 请求 (使用 gRPC)
    pub async fn send_transfer_request(
        &self,
        hash: Vec<u8>,
        position: String,
        offsets: Vec<u32>,
        token_ids: Vec<u32>,
        target_ip: String,
        target_port: i32,
        do_copy: bool,
    ) -> Result<TransferKvResponse, anyhow::Error> {
        let request_body = TransferKvRequest {
            hash,
            position,
            offset: offsets,
            tokens: token_ids,
            target_ip,
            target_port,
            do_copy,
        };

        // 连接到 gRPC 服务器
        let mut client = LmcacheServerClient::connect(self.base_url.clone()).await?;

        // 发送请求
        let response = client.transfer_kv(request_body).await?;

        Ok(response.into_inner())
    }

    pub async fn send_transfer_request_v2(
        &self,
        request_body: TransferKvV2Request,
        timeout: Duration,
    ) -> Result<TransferKvV2Response, anyhow::Error> {
        let mut client = LmcacheServerV2Client::connect(self.base_url.clone()).await?;
        let mut request = Request::new(request_body);
        request.set_timeout(timeout);
        Ok(client.transfer_kv_v2(request).await?.into_inner())
    }

    /// Split a logical transfer without changing block order or response cardinality.
    pub async fn send_transfer_requests_v2(
        &self,
        request: TransferKvV2Request,
        limits: TransferV2Limits,
    ) -> Result<TransferKvV2Response, anyhow::Error> {
        if limits.max_blocks == 0
            || limits.max_tokens == 0
            || limits.max_bytes == 0
            || limits.estimated_bytes_per_token == 0
        {
            anyhow::bail!("V2 transfer limits must be non-zero");
        }
        let transfer_id = request.transfer_id.clone();
        let mut all_results = Vec::with_capacity(request.blocks.len());
        let mut start = 0;
        while start < request.blocks.len() {
            let mut end = start;
            let mut tokens = 0_u64;
            let mut bytes = 0_u64;
            while end < request.blocks.len() && end - start < limits.max_blocks {
                let next_tokens = u64::from(request.blocks[end].offset);
                let next_bytes = next_tokens
                    .checked_mul(limits.estimated_bytes_per_token)
                    .ok_or_else(|| anyhow::anyhow!("V2 transfer byte estimate overflow"))?;
                if end > start
                    && (tokens + next_tokens > limits.max_tokens
                        || bytes + next_bytes > limits.max_bytes)
                {
                    break;
                }
                if next_tokens > limits.max_tokens || next_bytes > limits.max_bytes {
                    anyhow::bail!("one V2 block exceeds transfer limits");
                }
                tokens += next_tokens;
                bytes += next_bytes;
                end += 1;
            }
            let mut page = request.clone();
            page.blocks = request.blocks[start..end].to_vec();
            let response = self
                .send_transfer_request_v2(page, limits.timeout)
                .await?;
            if response.transfer_id != transfer_id || response.results.len() != end - start {
                anyhow::bail!("invalid V2 transfer response identity/cardinality");
            }
            all_results.extend(response.results);
            start = end;
        }
        Ok(TransferKvV2Response {
            transfer_id,
            results: all_results,
        })
    }
}
