
use tonic::{Request, Response, Status};

use cedfs_proto::kvcache::{
    kv_meta2_meta_server::KvMeta2Meta,
    GetKvMetaRequest, GetKvMetaResponse,
    UpdateKvMetaRequest, UpdateKvMetaResponse, 
};

use crate::Shared;
use crate::operation::get_kvmeta::GetKvMetaOp;
use crate::operation::update_kvmeta::UpdateKvMetaOp;

pub struct KvCacheMetaService {
    pub(crate) shared: Shared,
}


#[tonic::async_trait]
impl KvMeta2Meta for KvCacheMetaService{
    /// 获取KV元数据
    async fn get_kv_meta(
        &self,
        request: Request<GetKvMetaRequest>,
    ) -> Result<Response<GetKvMetaResponse>, Status> {
        tracing::info!("get_kv_meta request received");
        let _req = request.into_inner();
    
        let resp = GetKvMetaOp {
            shared: self.shared.clone(),
            new_meta_server: _req.meta_server.unwrap().into(),
            new_data_server: _req.data_server.unwrap().into(),
        }.run().await;
        
        match resp {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }


    /// 更新KV元数据
    async fn update_kv_meta(
        &self,
        request: Request<UpdateKvMetaRequest>,
    ) -> Result<Response<UpdateKvMetaResponse>, Status> {
        tracing::info!("update_kv_meta request received");
        let _req = request.into_inner();
        let resp = UpdateKvMetaOp {
            kv_meta: _req.meta.into_iter().map(|m| m.into()).collect(),
            kv_ref: _req.local_counts.into_iter().map(|lc| (lc.block_id, lc.count)).collect(),
            shared: self.shared.clone(),
        }.run();
        match resp {
            Ok(_) => Ok(Response::new(UpdateKvMetaResponse {meta_server: (*self.shared.meta_server_collect).read().await.clone().into_iter().map(|m| m.into()).collect()
                , data_server: (*self.shared.data_server_collect).read().await.clone().into_iter().map(|d| d.into()).collect()})),
            Err(e) => Err(Status::internal(e.to_string())), 
        }    
    }
}