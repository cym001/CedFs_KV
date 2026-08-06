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
        let request = request.into_inner();
        let data_server = request
            .data_server
            .ok_or_else(|| Status::invalid_argument("missing data_server"))?;
        let ip = data_server
            .ip
            .parse::<std::net::IpAddr>()
            .map_err(|_| Status::invalid_argument("data_server.ip must be an IP address"))?;
        let http_port = u16::try_from(data_server.http_port)
            .ok()
            .filter(|port| *port > 0)
            .ok_or_else(|| Status::invalid_argument("invalid data_server.http_port"))?;
        let init_port = u16::try_from(data_server.init_port)
            .ok()
            .filter(|port| *port > 0)
            .ok_or_else(|| Status::invalid_argument("invalid data_server.init_port"))?;
        let rpc_port = u16::try_from(data_server.rpc_port)
            .ok()
            .filter(|port| *port > 0)
            .ok_or_else(|| Status::invalid_argument("invalid data_server.rpc_port"))?;
        let url = reqwest::Url::parse(&data_server.url)
            .map_err(|_| Status::invalid_argument("data_server.url must be absolute"))?;
        let url_host = url
            .host_str()
            .unwrap_or_default()
            .trim_start_matches('[')
            .trim_end_matches(']');
        if !matches!(url.scheme(), "http" | "https")
            || url_host != data_server.ip.as_str()
            || url.port_or_known_default() != Some(http_port)
            || data_server.model_name.is_empty()
        {
            return Err(Status::invalid_argument(
                "data_server endpoint/model fields are inconsistent",
            ));
        }
        let op = crate::operation::register_instance::RegisterInstanceOp {
            data_server: crate::types::DataServer {
                id: data_server.id,
                ip,
                http_port,
                init_port,
                rpc_port,
                model_name: data_server.model_name,
                url: data_server.url,
            },
            shared: self.shared.clone(),
        };
        let resp = op.run().await;
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
