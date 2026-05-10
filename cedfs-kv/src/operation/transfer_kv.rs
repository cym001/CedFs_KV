use cedfs_proto::lmcache::lmcache_server_client::LmcacheServerClient;
use cedfs_proto::lmcache::{TransferKvRequest, TransferKvResponse};

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
