use tonic::{Request, Response, Status};

use cedfs_proto::kvcache::kv_meta2_data_server::{KvMeta2Data};
use cedfs_proto::kvcache::{
    GetLocalKvMetaRequest, GetLocalKvMetaResponse,
    IncrRefCountSyncRequest, IncrRefCountSyncResponse,
    UploadKvMetaRequest, UploadKvMetaResponse,
};
use crate::Shared;
use crate::operation::{incr_local_ref::IncrLocalRefOp, upload_kvmeta::UploadKvMetaOp, get_local_kvmeta::GetLocalKvMetaOp};

pub struct KvCacheDataService {
    pub(crate) shared: Shared,
}


#[tonic::async_trait]
impl KvMeta2Data for KvCacheDataService{
    /// 获取本地KV元数据
    async fn get_local_kv_meta(
        &self,
        request: Request<GetLocalKvMetaRequest>,
    ) -> Result<Response<GetLocalKvMetaResponse>, Status> {
        tracing::info!("get_local_kv_meta request received");
        let _req = request.into_inner();
        let resp = GetLocalKvMetaOp {
            kv_meta: _req.kvmeta.into_iter().map(|m| m.into()).collect(),
            kv_ref: _req.kv_ref,
            shared: self.shared.clone(),
        }.run();
        match resp {
            Ok(_) => Ok(Response::new(GetLocalKvMetaResponse {success: true,})),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    /// 增加引用计数
    async fn incr_ref_count(
        &self,
        request: Request<IncrRefCountSyncRequest>,
    ) -> Result<Response<IncrRefCountSyncResponse>, Status> {
        tracing::info!("incr_ref_count request received");
        let _req = request.into_inner();
        let resp = IncrLocalRefOp {
            incr: _req.kv_incr,
            shared: self.shared.clone(),
        }.run();
        match resp {
            Ok(_) => Ok(Response::new(IncrRefCountSyncResponse {success: true,})),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    /// 上传KV元数据
    async fn upload_kv_meta(
        &self,
        request: Request<UploadKvMetaRequest>,
    ) -> Result<Response<UploadKvMetaResponse>, Status> {
        tracing::info!("upload_kv_meta request received");
        let _req = request.into_inner();
        let resp = UploadKvMetaOp {
            kv_meta: _req.kvmeta.into_iter().map(|m| m.into()).collect(),
            kv_ref: _req.kv_ref,
            shared: self.shared.clone(),
        }.run();
        match resp {
            Ok(_) => Ok(Response::new(UploadKvMetaResponse {success: true,})),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}