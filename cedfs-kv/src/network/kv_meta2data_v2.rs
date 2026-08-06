use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};

use cedfs_proto::kvcache_v2::kv_meta2_data_v2_server::KvMeta2DataV2;
use cedfs_proto::kvcache_v2::{
    AbortInventorySyncV2Request, AbortInventorySyncV2Response, BeginInventorySyncV2Request,
    BeginInventorySyncV2Response, CommitInventorySyncV2Request, CommitInventorySyncV2Response,
    GetCapabilitiesRequestV2, GetCapabilitiesResponseV2, HeartbeatV2Request,
    HeartbeatV2Response, RegisterInstanceV2Request, RegisterInstanceV2Response,
    ReportCacheMutationsV2Request, ReportCacheMutationsV2Response, ReportRequestEndV2Request,
    ReportRequestStartV2Request, ReportRequestV2Response, UnregisterInstanceV2Request,
    UnregisterInstanceV2Response, UploadInventoryPageV2Request, UploadInventoryPageV2Response,
};

use crate::Shared;

const PROTOCOL_MAJOR: u32 = 2;
const PROTOCOL_MINOR: u32 = 0;

pub struct KvCacheDataServiceV2 {
    pub(crate) shared: Shared,
}

fn phase_a_unimplemented<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented(
        "V2 state handling is disabled in the phase A protocol skeleton",
    ))
}

#[tonic::async_trait]
impl KvMeta2DataV2 for KvCacheDataServiceV2 {
    async fn get_capabilities(
        &self,
        _request: Request<GetCapabilitiesRequestV2>,
    ) -> Result<Response<GetCapabilitiesResponseV2>, Status> {
        Ok(Response::new(GetCapabilitiesResponseV2 {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            capabilities: vec![
                "capability_handshake".to_string(),
                "instance_registration".to_string(),
                "cache_mutation_shadow".to_string(),
            ],
            transfer_enabled: self.shared.config.enable_v2_transfer,
            descriptor_sha256: Sha256::digest(cedfs_proto::V2_DESCRIPTOR_SET).to_vec(),
        }))
    }

    async fn register_instance(
        &self,
        request: Request<RegisterInstanceV2Request>,
    ) -> Result<Response<RegisterInstanceV2Response>, Status> {
        let state = self
            .shared
            .v2_state
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("V2 state is disabled"))?;
        Ok(Response::new(state.register(request.into_inner())))
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatV2Request>,
    ) -> Result<Response<HeartbeatV2Response>, Status> {
        phase_a_unimplemented()
    }

    async fn unregister_instance(
        &self,
        _request: Request<UnregisterInstanceV2Request>,
    ) -> Result<Response<UnregisterInstanceV2Response>, Status> {
        phase_a_unimplemented()
    }

    async fn report_cache_mutations(
        &self,
        request: Request<ReportCacheMutationsV2Request>,
    ) -> Result<Response<ReportCacheMutationsV2Response>, Status> {
        let state = self
            .shared
            .v2_state
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("V2 state is disabled"))?;
        Ok(Response::new(state.report_mutations(request.into_inner())))
    }

    async fn begin_inventory_sync(
        &self,
        _request: Request<BeginInventorySyncV2Request>,
    ) -> Result<Response<BeginInventorySyncV2Response>, Status> {
        phase_a_unimplemented()
    }

    async fn upload_inventory_page(
        &self,
        _request: Request<UploadInventoryPageV2Request>,
    ) -> Result<Response<UploadInventoryPageV2Response>, Status> {
        phase_a_unimplemented()
    }

    async fn commit_inventory_sync(
        &self,
        _request: Request<CommitInventorySyncV2Request>,
    ) -> Result<Response<CommitInventorySyncV2Response>, Status> {
        phase_a_unimplemented()
    }

    async fn abort_inventory_sync(
        &self,
        _request: Request<AbortInventorySyncV2Request>,
    ) -> Result<Response<AbortInventorySyncV2Response>, Status> {
        phase_a_unimplemented()
    }

    async fn report_request_start(
        &self,
        _request: Request<ReportRequestStartV2Request>,
    ) -> Result<Response<ReportRequestV2Response>, Status> {
        phase_a_unimplemented()
    }

    async fn report_request_end(
        &self,
        _request: Request<ReportRequestEndV2Request>,
    ) -> Result<Response<ReportRequestV2Response>, Status> {
        phase_a_unimplemented()
    }
}
