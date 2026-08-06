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
                "lease_heartbeat".to_string(),
                "inventory_sync".to_string(),
                "request_lifecycle".to_string(),
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
        request: Request<HeartbeatV2Request>,
    ) -> Result<Response<HeartbeatV2Response>, Status> {
        let state = self.shared.v2_state.as_ref().ok_or_else(|| {
            Status::failed_precondition("V2 state is disabled")
        })?;
        Ok(Response::new(state.heartbeat(request.into_inner())))
    }

    async fn unregister_instance(
        &self,
        request: Request<UnregisterInstanceV2Request>,
    ) -> Result<Response<UnregisterInstanceV2Response>, Status> {
        let state = self.shared.v2_state.as_ref().ok_or_else(|| {
            Status::failed_precondition("V2 state is disabled")
        })?;
        Ok(Response::new(state.unregister(request.into_inner())))
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
        request: Request<BeginInventorySyncV2Request>,
    ) -> Result<Response<BeginInventorySyncV2Response>, Status> {
        let state = self.shared.v2_state.as_ref().ok_or_else(|| {
            Status::failed_precondition("V2 state is disabled")
        })?;
        Ok(Response::new(state.begin_inventory_sync(request.into_inner())))
    }

    async fn upload_inventory_page(
        &self,
        request: Request<UploadInventoryPageV2Request>,
    ) -> Result<Response<UploadInventoryPageV2Response>, Status> {
        let state = self.shared.v2_state.as_ref().ok_or_else(|| {
            Status::failed_precondition("V2 state is disabled")
        })?;
        Ok(Response::new(state.upload_inventory_page(request.into_inner())))
    }

    async fn commit_inventory_sync(
        &self,
        request: Request<CommitInventorySyncV2Request>,
    ) -> Result<Response<CommitInventorySyncV2Response>, Status> {
        let state = self.shared.v2_state.as_ref().ok_or_else(|| {
            Status::failed_precondition("V2 state is disabled")
        })?;
        Ok(Response::new(state.commit_inventory_sync(request.into_inner())))
    }

    async fn abort_inventory_sync(
        &self,
        request: Request<AbortInventorySyncV2Request>,
    ) -> Result<Response<AbortInventorySyncV2Response>, Status> {
        let state = self.shared.v2_state.as_ref().ok_or_else(|| {
            Status::failed_precondition("V2 state is disabled")
        })?;
        Ok(Response::new(state.abort_inventory_sync(request.into_inner())))
    }

    async fn report_request_start(
        &self,
        request: Request<ReportRequestStartV2Request>,
    ) -> Result<Response<ReportRequestV2Response>, Status> {
        let state = self.shared.v2_state.as_ref().ok_or_else(|| {
            Status::failed_precondition("V2 state is disabled")
        })?;
        let request = request.into_inner();
        let active_id = composite_request_id(request.request.as_ref());
        let hashes: Vec<[u8; 32]> = request
            .blocks
            .iter()
            .filter_map(|block| block.seq_hash.as_slice().try_into().ok())
            .collect();
        let response = state.report_request_start(request);
        if response.accepted && !response.duplicate {
            if let Some(active_id) = active_id {
                self.shared
                    .active_squence
                    .add_request(active_id, Some(hashes));
            }
        }
        Ok(Response::new(response))
    }

    async fn report_request_end(
        &self,
        request: Request<ReportRequestEndV2Request>,
    ) -> Result<Response<ReportRequestV2Response>, Status> {
        let state = self.shared.v2_state.as_ref().ok_or_else(|| {
            Status::failed_precondition("V2 state is disabled")
        })?;
        let request = request.into_inner();
        let active_id = composite_request_id(request.request.as_ref());
        let response = state.report_request_end(request);
        if response.accepted {
            if let Some(active_id) = active_id {
                self.shared.active_squence.free(&active_id);
            }
        }
        Ok(Response::new(response))
    }
}

fn composite_request_id(
    request: Option<&cedfs_proto::kvcache_v2::RequestIdentityV2>,
) -> Option<String> {
    let request = request?;
    let instance = request.instance.as_ref()?;
    let key = instance.key.as_ref()?;
    Some(format!(
        "{}:{}:{}:{}",
        key.lmcache_instance_id, key.worker_id, instance.epoch, request.request_id
    ))
}
