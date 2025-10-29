use tonic::{Request, Response, Status};

use cedfs_proto::kvcache::kv_meta2_data_server::{KvMeta2Data};
use cedfs_proto::kvcache::{
    GetLocalKvMetaRequest, GetLocalKvMetaResponse,
    UploadKvMetaRequest, UploadKvMetaResponse,
};
use crate::Shared;
use crate::operation::{upload_kvmeta::UploadKvMetaOp, get_local_kvmeta::GetLocalKvMetaOp};

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
        let req = request.into_inner();
        let resp = GetLocalKvMetaOp {
            kv_meta: req.kvmeta,
            shared: self.shared.clone(),
        }.run();
        match resp {
            Ok(_) => Ok(Response::new(GetLocalKvMetaResponse {success: true,})),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    /// 上传KV元数据
    async fn upload_kv_meta(
        &self,
        request: Request<UploadKvMetaRequest>,
    ) -> Result<Response<UploadKvMetaResponse>, Status> {
        tracing::info!("upload_kv_meta request received");
        let req = request.into_inner();
        let resp = UploadKvMetaOp {
            kv_meta: req.kvmeta,
            shared: self.shared.clone(),
        }.run();
        match resp {
            Ok(_) => Ok(Response::new(UploadKvMetaResponse {success: true,})),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}