use cedfs_proto::lmcache::lmcache_server_client::LmcacheServerClient;
use cedfs_proto::lmcache::{TransferKvRequest, TransferKvResponse};

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
}
