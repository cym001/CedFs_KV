use tonic::{Request, Response, Status};

use cedfs_proto::kvcache::kv_meta2_data_server::{KvMeta2Data};
use cedfs_proto::kvcache::{
    UploadKvMetaRequest, UploadKvMetaResponse,
    RegisterInstanceRequest, RegisterInstanceResponse,
    RemoveKvMetaRequest, RemoveKvMetaResponse,
};
use crate::Shared;
use crate::operation::{upload_kvmeta::UploadKvMetaOp,remove_kvmeta::RemoveKvMetaOp};


pub struct KvCacheDataService {
    pub(crate) shared: Shared,
}


#[tonic::async_trait]
impl KvMeta2Data for KvCacheDataService{
    /// 上传KV元数据
    async fn upload_kv_meta(
        &self,
        request: Request<UploadKvMetaRequest>,
    ) -> Result<Response<UploadKvMetaResponse>, Status> {
        tracing::info!("upload_kv_meta request received");
        let req = request.into_inner();
        let resp = UploadKvMetaOp {
            server_id: req.server_id,
            tokens: req.tokens,
            shared: self.shared.clone(),
        }.run().await;
        match resp {
            Ok(_) => Ok(Response::new(UploadKvMetaResponse {success: true,})),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    /// 注册推理实例信息
    async fn register_instance(&self,
        request: Request<RegisterInstanceRequest>,
    ) -> Result<Response<RegisterInstanceResponse>, Status>{
        tracing::info!("register_instance request received");
        let _req = request.into_inner();
        let _op = crate::operation::register_instance::RegisterInstanceOp{
            data_server: _req.data_server.unwrap().into(),
            shared: self.shared.clone(),
        };
        let resp = _op.run().await;
        match resp {
            Ok(_) => Ok(Response::new(RegisterInstanceResponse {success: true,})),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    ///删除KV元数据
    async fn remove_kv_meta(
        &self,
        request: Request<RemoveKvMetaRequest>,
    ) -> Result<Response<RemoveKvMetaResponse>, Status> {
        tracing::info!("upload_kv_meta request received");
        let req = request.into_inner();
        let resp = RemoveKvMetaOp {
            remove_nums: req.remove_nums,
            tokens_hash: req.tokens_hash,
            shared: self.shared.clone(),
        }.run().await;
        match resp {
            Ok(_) => Ok(Response::new(RemoveKvMetaResponse {success: true,})),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}