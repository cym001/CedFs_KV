use tonic::{Request, Response, Status};

use cedfs_proto::kvcache::{
    kv_meta2_meta_server::KvMeta2Meta, SearchKvBlockByPromptsRequest,
    SearchKvBlockByPromptsResponse, SearchKvBlockRequest, SearchKvBlockResponse,
};

use crate::operation::search_kv::{SearchKvByPromptsOp, SearchKvOp};
use crate::Shared;

pub struct KvCacheMetaService {
    pub(crate) shared: Shared,
}

#[tonic::async_trait]
impl KvMeta2Meta for KvCacheMetaService {
    /// 搜索KV块
    async fn search_kv_block(
        &self,
        request: Request<SearchKvBlockRequest>,
    ) -> Result<Response<SearchKvBlockResponse>, Status> {
        tracing::info!("search_kv_block request received");
        let _req = request.into_inner();

        // 将 proto 的 Token_lists 转换为 Vec<Vec<Vec<u32>>>
        let query_lists: Vec<Vec<u32>> = _req
            .query_lists
            .into_iter()
            .map(|token_list| token_list.tokens)
            .collect();

        let resp = SearchKvOp {
            shared: self.shared.clone(),
            query_lists,
        }
        .run()
        .await;

        match resp {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    /// 通过prompts搜索kv块
    async fn search_kv_block_by_prompts(
        &self,
        request: Request<SearchKvBlockByPromptsRequest>,
    ) -> Result<Response<SearchKvBlockByPromptsResponse>, Status> {
        tracing::info!("search_kv_block_by_prompts request received");
        let _req = request.into_inner();

        let resp = SearchKvByPromptsOp {
            shared: self.shared.clone(),
            model_names: _req.model_name,
            prompts: _req.prompts,
        }
        .run()
        .await;

        match resp {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}
