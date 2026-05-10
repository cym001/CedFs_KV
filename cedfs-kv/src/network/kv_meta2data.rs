use tonic::{Request, Response, Status};

use crate::operation::{
    new_request::NewRequestOp, remove_kvmeta::RemoveKvMetaOp, request_end::RequestEndOp,
    upload_kvmeta::UploadKvMetaOp,
};
use crate::Shared;
use cedfs_proto::kvcache::kv_meta2_data_server::KvMeta2Data;
use cedfs_proto::kvcache::{
    NewRequestRequest, NewRequestResponse, RegisterInstanceRequest, RegisterInstanceResponse,
    RemoveKvMetaRequest, RemoveKvMetaResponse, RequestEndRequest, RequestEndResponse,
    UploadKvMetaRequest, UploadKvMetaResponse,
};

pub struct KvCacheDataService {
    pub(crate) shared: Shared,
}

#[tonic::async_trait]
impl KvMeta2Data for KvCacheDataService {
    /// 上传KV元数据
    async fn upload_kv_meta(
        &self,
        request: Request<UploadKvMetaRequest>,
    ) -> Result<Response<UploadKvMetaResponse>, Status> {
        tracing::debug!("upload_kv_meta request received");
        let req = request.into_inner();
        let resp = UploadKvMetaOp {
            server_id: req.server_id,
            tokens: req.tokens,
            shared: self.shared.clone(),
        }
        .run()
        .await;
        match resp {
            Ok(_) => Ok(Response::new(UploadKvMetaResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    /// 注册推理实例信息
    async fn register_instance(
        &self,
        request: Request<RegisterInstanceRequest>,
    ) -> Result<Response<RegisterInstanceResponse>, Status> {
        tracing::debug!("register_instance request received");
        let _req = request.into_inner();
        let _op = crate::operation::register_instance::RegisterInstanceOp {
            data_server: _req.data_server.unwrap().into(),
            shared: self.shared.clone(),
        };
        let resp = _op.run().await;
        match resp {
            Ok(_) => Ok(Response::new(RegisterInstanceResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    ///删除KV元数据
    async fn remove_kv_meta(
        &self,
        request: Request<RemoveKvMetaRequest>,
    ) -> Result<Response<RemoveKvMetaResponse>, Status> {
        tracing::debug!("upload_kv_meta request received");
        let req = request.into_inner();
        let resp = RemoveKvMetaOp {
            server_id: req.id,
            remove_nums: req.remove_nums,
            tokens_hash: req.tokens_hash,
            shared: self.shared.clone(),
        }
        .run()
        .await;
        match resp {
            Ok(_) => Ok(Response::new(RemoveKvMetaResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    /// 新请求开始
    async fn new_request(
        &self,
        request: Request<NewRequestRequest>,
    ) -> Result<Response<NewRequestResponse>, Status> {
        tracing::debug!("new_request request received");
        let req = request.into_inner();
        let op = NewRequestOp {
            request_id: req.request_id,
            server_id: req.server_id,
            tokens: req.tokens,
            shared: self.shared.clone(),
        };
        match op.run().await {
            Ok(_) => Ok(Response::new(NewRequestResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    /// 请求结束
    async fn request_end(
        &self,
        request: Request<RequestEndRequest>,
    ) -> Result<Response<RequestEndResponse>, Status> {
        tracing::debug!("request_end request received");
        let req = request.into_inner();
        let op = RequestEndOp {
            request_id: req.request_id,
            shared: self.shared.clone(),
        };
        match op.run().await {
            Ok(_) => Ok(Response::new(RequestEndResponse { success: true })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}
