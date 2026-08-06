use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::{mapref::entry::Entry, DashMap};
use serde::Serialize;
use sha2::{Digest, Sha256};

use cedfs_proto::kvcache_v2::cache_mutation_event_v2::Payload;
use cedfs_proto::kvcache_v2::{
    AbortInventorySyncV2Request, AbortInventorySyncV2Response, BeginInventorySyncV2Request,
    BeginInventorySyncV2Response, BlockDescriptorV2, CapacitySnapshotV2,
    CommitInventorySyncV2Request, CommitInventorySyncV2Response, CompatibilityFingerprintV2,
    HeartbeatV2Request, HeartbeatV2Response, InstanceEndpointsV2, InstanceIdentityV2,
    InstanceKeyV2, InstanceSessionV2, MutationStatusV2, RegisterInstanceV2Request,
    RegisterInstanceV2Response, RegisterStatusV2, ReportCacheMutationsV2Request,
    ReportCacheMutationsV2Response, ReportRequestEndV2Request, ReportRequestStartV2Request,
    ReportRequestV2Response, UnregisterInstanceV2Request, UnregisterInstanceV2Response,
    UploadInventoryPageV2Request, UploadInventoryPageV2Response,
};
use cedfs_proto::lmcache_v2::{
    BlockTransferResultV2, BlockTransferStatusV2, TransferKvV2Request,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InstanceKey {
    pub lmcache_instance_id: String,
    pub worker_id: u32,
}

#[derive(Debug, Clone)]
pub struct InstanceRecord {
    pub key: InstanceKey,
    pub handle: u64,
    pub epoch: String,
    pub group_id: Vec<u8>,
    pub chunk_size: u32,
    pub committed_event_seq: u64,
    pub lease_id: String,
    pub lease_deadline: Instant,
    pub endpoints: InstanceEndpointsV2,
    pub capacity: CapacitySnapshotV2,
    pub eviction_rate: f64,
    pub capacity_updated_at: Instant,
    pub inventory_ready: bool,
}

#[derive(Debug, Clone)]
pub struct ShadowBlock {
    pub descriptor: BlockDescriptorV2,
    pub replicas: HashMap<u64, u64>,
    pub last_versions: HashMap<u64, u64>,
}

#[derive(Debug, Default)]
pub struct GroupState {
    pub blocks: Mutex<HashMap<Vec<u8>, ShadowBlock>>,
    demand: Mutex<HashMap<Vec<u8>, DemandWindow>>,
}

#[derive(Debug, Clone)]
struct DemandWindow {
    current: u64,
    previous: u64,
    started_at: Instant,
}

#[derive(Debug, Clone)]
pub struct RebalanceCandidate {
    pub group_id: Vec<u8>,
    pub source: InstanceIdentityV2,
    pub source_endpoints: InstanceEndpointsV2,
    pub target: InstanceIdentityV2,
    pub target_endpoints: InstanceEndpointsV2,
    pub blocks: Vec<BlockDescriptorV2>,
    pub demand_score: f64,
    pub estimated_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct GroupStatusSnapshot {
    pub compatibility_group_id: String,
    pub registered_instances: usize,
    pub ready_instances: usize,
    pub blocks: usize,
    pub replicas: usize,
    pub ready: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct V2StatusSnapshot {
    pub meta_generation: String,
    pub ready: bool,
    pub pending_inventory_syncs: usize,
    pub active_requests: usize,
    pub lease_expired_total: u64,
    pub mutation_sequence_gap_total: u64,
    pub inventory_sync_success_total: u64,
    pub inventory_sync_failure_total: u64,
    pub inventory_sync_blocks_total: u64,
    pub inventory_sync_duration_ms_total: u64,
    pub reconcile_mismatch_total: u64,
    pub groups: Vec<GroupStatusSnapshot>,
}

#[derive(Debug)]
pub struct V2State {
    next_instance_handle: AtomicU64,
    instances: DashMap<InstanceKey, Arc<Mutex<InstanceRecord>>>,
    groups: DashMap<Vec<u8>, Arc<GroupState>>,
    inventory_syncs: Mutex<HashMap<String, InventorySync>>,
    requests: DashMap<RequestKey, Instant>,
    lease_ttl: Duration,
    request_ttl: Duration,
    inventory_page_limit: u32,
    meta_generation: String,
    demand_window: Duration,
    lease_expired_total: AtomicU64,
    mutation_sequence_gap_total: AtomicU64,
    inventory_sync_success_total: AtomicU64,
    inventory_sync_failure_total: AtomicU64,
    inventory_sync_blocks_total: AtomicU64,
    inventory_sync_duration_ms_total: AtomicU64,
    reconcile_mismatch_total: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RequestKey {
    instance: InstanceKey,
    epoch: String,
    request_id: String,
}

#[derive(Debug)]
struct InventorySync {
    instance_key: InstanceKey,
    instance_handle: u64,
    group_id: Vec<u8>,
    base_event_seq: u64,
    total_blocks: u64,
    total_pages: u32,
    inventory_checksum: Vec<u8>,
    pages: HashMap<u32, Vec<BlockDescriptorV2>>,
    started_at: Instant,
}

impl Default for V2State {
    fn default() -> Self {
        Self::new(Duration::from_secs(30), Duration::from_secs(300), 256)
    }
}

impl V2State {
    pub fn new(
        lease_ttl: Duration,
        request_ttl: Duration,
        inventory_page_limit: u32,
    ) -> Self {
        Self::new_with_demand_window(
            lease_ttl,
            request_ttl,
            inventory_page_limit,
            Duration::from_secs(300),
        )
    }

    pub fn new_with_demand_window(
        lease_ttl: Duration,
        request_ttl: Duration,
        inventory_page_limit: u32,
        demand_window: Duration,
    ) -> Self {
        let generation = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            next_instance_handle: AtomicU64::new(0),
            instances: DashMap::new(),
            groups: DashMap::new(),
            inventory_syncs: Mutex::new(HashMap::new()),
            requests: DashMap::new(),
            lease_ttl,
            request_ttl,
            inventory_page_limit,
            meta_generation: format!("{}-{generation}", std::process::id()),
            demand_window,
            lease_expired_total: AtomicU64::new(0),
            mutation_sequence_gap_total: AtomicU64::new(0),
            inventory_sync_success_total: AtomicU64::new(0),
            inventory_sync_failure_total: AtomicU64::new(0),
            inventory_sync_blocks_total: AtomicU64::new(0),
            inventory_sync_duration_ms_total: AtomicU64::new(0),
            reconcile_mismatch_total: AtomicU64::new(0),
        }
    }

    pub fn register(&self, request: RegisterInstanceV2Request) -> RegisterInstanceV2Response {
        self.cleanup_expired();
        if request.protocol_major != 2 {
            return register_error(
                RegisterStatusV2::UnsupportedProtocol,
                "protocol_major must be 2",
            );
        }
        let Some(identity) = request.instance else {
            return register_error(RegisterStatusV2::InvalidArgument, "missing instance identity");
        };
        let Some(key) = parse_instance_key(&identity) else {
            return register_error(RegisterStatusV2::InvalidArgument, "invalid instance identity");
        };
        let Some(endpoints) = request.endpoints.clone() else {
            return register_error(RegisterStatusV2::InvalidArgument, "missing endpoints");
        };
        if endpoints.host.is_empty()
            || matches!(endpoints.host.as_str(), "0.0.0.0" | "::")
            || endpoints.http_port == 0
            || endpoints.nixl_init_port == 0
            || endpoints.transfer_rpc_port == 0
            || endpoints.http_port > u32::from(u16::MAX)
            || endpoints.nixl_init_port > u32::from(u16::MAX)
            || endpoints.transfer_rpc_port > u32::from(u16::MAX)
            || !endpoints.api_path.starts_with('/')
        {
            return register_error(RegisterStatusV2::InvalidArgument, "invalid endpoints");
        }
        let Some(fingerprint) = request.fingerprint else {
            return register_error(RegisterStatusV2::InvalidArgument, "missing fingerprint");
        };
        if fingerprint.model_name.is_empty()
            || fingerprint.hash_algorithm.is_empty()
            || fingerprint.kv_dtype.is_empty()
            || fingerprint.kv_layout.is_empty()
            || fingerprint.chunk_size == 0
            || fingerprint.world_size == 0
            || fingerprint.tensor_parallel_size == 0
            || fingerprint.pipeline_parallel_size == 0
            || u64::from(fingerprint.tensor_parallel_size)
                * u64::from(fingerprint.pipeline_parallel_size)
                != u64::from(fingerprint.world_size)
            || fingerprint.worker_id >= fingerprint.world_size
            || fingerprint.worker_id != key.worker_id
            || fingerprint.tensor_parallel_rank
                != fingerprint.worker_id % fingerprint.tensor_parallel_size
            || fingerprint.pipeline_parallel_rank
                != fingerprint.worker_id / fingerprint.tensor_parallel_size
        {
            return register_error(
                RegisterStatusV2::Incompatible,
                "fingerprint chunk_size/worker_id is incompatible",
            );
        }

        let group_id = fingerprint_group_id(&fingerprint);
        if let Some(existing) = self.instances.get(&key) {
            let mut existing = existing.lock().unwrap();
            if existing.epoch == identity.epoch {
                if existing.group_id != group_id {
                    return register_error(
                        RegisterStatusV2::Incompatible,
                        "same instance epoch registered with a different fingerprint",
                    );
                }
                existing.lease_deadline = Instant::now() + self.lease_ttl;
                existing.endpoints = endpoints;
                existing.capacity = CapacitySnapshotV2 {
                    capacity_bytes: request.capacity_bytes,
                    used_bytes: request.used_bytes,
                    eviction_count: 0,
                };
                existing.eviction_rate = 0.0;
                existing.capacity_updated_at = Instant::now();
                let generation_mismatch =
                    request.known_meta_generation != self.meta_generation;
                if generation_mismatch {
                    existing.inventory_ready = false;
                }
                return register_success(
                    &existing,
                    self.lease_ttl,
                    &self.meta_generation,
                    generation_mismatch || !existing.inventory_ready,
                );
            }
        }
        let handle = self.next_instance_handle.fetch_add(1, Ordering::Relaxed) + 1;
        let lease_id = format!("{}:{handle}", identity.epoch);
        let record = InstanceRecord {
            key: key.clone(),
            handle,
            epoch: identity.epoch,
            group_id: group_id.clone(),
            chunk_size: fingerprint.chunk_size,
            committed_event_seq: 0,
            lease_id: lease_id.clone(),
            lease_deadline: Instant::now() + self.lease_ttl,
            endpoints,
            capacity: CapacitySnapshotV2 {
                capacity_bytes: request.capacity_bytes,
                used_bytes: request.used_bytes,
                eviction_count: 0,
            },
            eviction_rate: 0.0,
            capacity_updated_at: Instant::now(),
            inventory_ready: false,
        };

        if let Some(previous) = self.instances.insert(key, Arc::new(Mutex::new(record))) {
            let previous = previous.lock().unwrap();
            if let Some(group) = self.groups.get(&previous.group_id) {
                remove_instance_replicas(group.value().as_ref(), previous.handle);
            }
        }
        self.groups.entry(group_id.clone()).or_default();

        RegisterInstanceV2Response {
            status: RegisterStatusV2::Accepted as i32,
            error_detail: String::new(),
            compatibility_group_id: group_id,
            instance_handle: handle,
            lease_id,
            lease_ttl_ms: self.lease_ttl.as_millis() as u64,
            meta_generation: self.meta_generation.clone(),
            require_inventory_sync: true,
            protocol_minor: 0,
            capabilities: vec![
                "cache_mutation_shadow".to_string(),
                "lease_heartbeat".to_string(),
                "inventory_sync".to_string(),
                "request_lifecycle".to_string(),
            ],
        }
    }

    pub fn heartbeat(&self, request: HeartbeatV2Request) -> HeartbeatV2Response {
        self.cleanup_expired();
        let Ok((_key, record)) = self.resolve_session(request.session.as_ref()) else {
            return HeartbeatV2Response {
                require_registration: true,
                meta_generation: self.meta_generation.clone(),
                lease_ttl_ms: self.lease_ttl.as_millis() as u64,
                ..Default::default()
            };
        };
        let mut record = record.lock().unwrap();
        record.lease_deadline = Instant::now() + self.lease_ttl;
        if let Some(capacity) = request.capacity {
            let elapsed = record.capacity_updated_at.elapsed().as_secs_f64();
            let evictions = capacity
                .eviction_count
                .saturating_sub(record.capacity.eviction_count);
            record.eviction_rate = if elapsed > 0.0 {
                evictions as f64 / elapsed
            } else {
                0.0
            };
            record.capacity = capacity;
            record.capacity_updated_at = Instant::now();
        }
        let generation_mismatch = request.known_meta_generation != self.meta_generation;
        if generation_mismatch {
            record.inventory_ready = false;
        }
        HeartbeatV2Response {
            accepted: true,
            require_registration: false,
            require_inventory_sync: generation_mismatch || !record.inventory_ready,
            meta_generation: self.meta_generation.clone(),
            lease_ttl_ms: self.lease_ttl.as_millis() as u64,
        }
    }

    pub fn unregister(
        &self,
        request: UnregisterInstanceV2Request,
    ) -> UnregisterInstanceV2Response {
        let Ok((key, record)) = self.resolve_session(request.session.as_ref()) else {
            return UnregisterInstanceV2Response { accepted: false };
        };
        let record = record.lock().unwrap().clone();
        let removed = self
            .instances
            .remove_if(&key, |_, current| {
                current.lock().unwrap().handle == record.handle
            })
            .is_some();
        if !removed {
            return UnregisterInstanceV2Response { accepted: false };
        }
        if let Some(group) = self.groups.get(&record.group_id) {
            remove_instance_replicas(group.value(), record.handle);
        }
        self.inventory_syncs
            .lock()
            .unwrap()
            .retain(|_, sync| sync.instance_handle != record.handle);
        self.requests.retain(|request, _| request.instance != key);
        UnregisterInstanceV2Response { accepted: true }
    }

    pub fn begin_inventory_sync(
        &self,
        request: BeginInventorySyncV2Request,
    ) -> BeginInventorySyncV2Response {
        let Ok((key, record)) = self.resolve_session(request.session.as_ref()) else {
            return self.inventory_failure(inventory_begin_error(
                "invalid or expired instance session",
            ));
        };
        let record = record.lock().unwrap();
        if record.group_id != request.compatibility_group_id
            || request.total_pages == 0
            || request.inventory_checksum.len() != 32
        {
            return self.inventory_failure(inventory_begin_error(
                "invalid inventory declaration",
            ));
        }
        let sync_id = format!("{}:{}", record.handle, request.base_event_seq);
        self.inventory_syncs.lock().unwrap().insert(
            sync_id.clone(),
            InventorySync {
                instance_key: key,
                instance_handle: record.handle,
                group_id: record.group_id.clone(),
                base_event_seq: request.base_event_seq,
                total_blocks: request.total_blocks,
                total_pages: request.total_pages,
                inventory_checksum: request.inventory_checksum,
                pages: HashMap::new(),
                started_at: Instant::now(),
            },
        );
        BeginInventorySyncV2Response {
            accepted: true,
            sync_id,
            page_size_limit: self.inventory_page_limit,
            error_detail: String::new(),
        }
    }

    pub fn upload_inventory_page(
        &self,
        request: UploadInventoryPageV2Request,
    ) -> UploadInventoryPageV2Response {
        let Ok((session_key, _record)) = self.resolve_session(request.session.as_ref()) else {
            return self.inventory_failure(inventory_page_error(
                request.page_id,
                "invalid or expired instance session",
            ));
        };
        let mut syncs = self.inventory_syncs.lock().unwrap();
        let Some(sync) = syncs.get_mut(&request.sync_id) else {
            return self.inventory_failure(inventory_page_error(
                request.page_id,
                "unknown inventory sync",
            ));
        };
        if sync.instance_key != session_key
            || request.page_id >= sync.total_pages
            || request.blocks.len() > self.inventory_page_limit as usize
            || inventory_checksum(&request.blocks) != request.page_checksum
        {
            return self.inventory_failure(inventory_page_error(
                request.page_id,
                "invalid inventory page",
            ));
        }
        if let Some(existing) = sync.pages.get(&request.page_id) {
            if existing != &request.blocks {
                self.inventory_sync_failure_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            return UploadInventoryPageV2Response {
                accepted: existing == &request.blocks,
                page_id: request.page_id,
                error_detail: if existing == &request.blocks {
                    String::new()
                } else {
                    "page id already contains different data".to_string()
                },
            };
        }
        sync.pages.insert(request.page_id, request.blocks);
        UploadInventoryPageV2Response {
            accepted: true,
            page_id: request.page_id,
            error_detail: String::new(),
        }
    }

    pub fn commit_inventory_sync(
        &self,
        request: CommitInventorySyncV2Request,
    ) -> CommitInventorySyncV2Response {
        let Ok((key, record)) = self.resolve_session(request.session.as_ref()) else {
            return self.inventory_failure(inventory_commit_error(
                "invalid or expired instance session",
            ));
        };
        let mut syncs = self.inventory_syncs.lock().unwrap();
        let Some(sync) = syncs.remove(&request.sync_id) else {
            return self.inventory_failure(inventory_commit_error("unknown inventory sync"));
        };
        if sync.instance_key != key || sync.pages.len() != sync.total_pages as usize {
            return self.inventory_failure(inventory_commit_error(
                "inventory pages are incomplete",
            ));
        }
        let mut descriptors = Vec::with_capacity(sync.total_blocks as usize);
        for page_id in 0..sync.total_pages {
            let Some(page) = sync.pages.get(&page_id) else {
                return self.inventory_failure(inventory_commit_error(
                    "inventory page is missing",
                ));
            };
            descriptors.extend(page.iter().cloned());
        }
        if descriptors.len() != sync.total_blocks as usize
            || inventory_checksum(&descriptors) != sync.inventory_checksum
        {
            self.reconcile_mismatch_total
                .fetch_add(1, Ordering::Relaxed);
            return self.inventory_failure(inventory_commit_error(
                "inventory checksum/count mismatch",
            ));
        }
        let mut record = record.lock().unwrap();
        if record.handle != sync.instance_handle
            || record.committed_event_seq > sync.base_event_seq
        {
            return self.inventory_failure(inventory_commit_error(
                "inventory base event sequence is stale",
            ));
        }
        let group = self.groups.entry(sync.group_id).or_default().clone();
        let mut live_blocks = group.blocks.lock().unwrap();
        let mut staged = live_blocks.clone();
        remove_instance_replicas_from_map(&mut staged, record.handle);
        let mut available = HashSet::new();
        for descriptor in descriptors {
            if let Err(detail) = validate_descriptor(&descriptor, record.chunk_size, &available) {
                return self.inventory_failure(inventory_commit_error(&detail));
            }
            if let Some(existing) = staged.get(&descriptor.seq_hash) {
                if !same_descriptor(&existing.descriptor, &descriptor) {
                    self.reconcile_mismatch_total
                        .fetch_add(1, Ordering::Relaxed);
                    return self.inventory_failure(inventory_commit_error(
                        "inventory descriptor conflict",
                    ));
                }
            }
            let seq_hash = descriptor.seq_hash.clone();
            available.insert(seq_hash.clone());
            let block = staged.entry(seq_hash).or_insert_with(|| ShadowBlock {
                descriptor,
                replicas: HashMap::new(),
                last_versions: HashMap::new(),
            });
            block
                .last_versions
                .insert(record.handle, sync.base_event_seq);
            block.replicas.insert(record.handle, sync.base_event_seq);
        }
        *live_blocks = staged;
        record.committed_event_seq = sync.base_event_seq;
        record.inventory_ready = true;
        self.inventory_sync_success_total
            .fetch_add(1, Ordering::Relaxed);
        self.inventory_sync_blocks_total
            .fetch_add(sync.total_blocks, Ordering::Relaxed);
        self.inventory_sync_duration_ms_total.fetch_add(
            sync.started_at
                .elapsed()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        CommitInventorySyncV2Response {
            committed: true,
            committed_event_seq: record.committed_event_seq,
            error_detail: String::new(),
        }
    }

    pub fn abort_inventory_sync(
        &self,
        request: AbortInventorySyncV2Request,
    ) -> AbortInventorySyncV2Response {
        let Ok((session_key, _record)) = self.resolve_session(request.session.as_ref()) else {
            return AbortInventorySyncV2Response { aborted: false };
        };
        let mut syncs = self.inventory_syncs.lock().unwrap();
        let owned = syncs
            .get(&request.sync_id)
            .is_some_and(|sync| sync.instance_key == session_key);
        AbortInventorySyncV2Response {
            aborted: owned && syncs.remove(&request.sync_id).is_some(),
        }
    }

    pub fn report_request_start(
        &self,
        request: ReportRequestStartV2Request,
    ) -> ReportRequestV2Response {
        self.cleanup_expired_requests();
        let Some(request_identity) = request.request else {
            return ReportRequestV2Response::default();
        };
        let Some(identity) = request_identity.instance else {
            return ReportRequestV2Response::default();
        };
        let Some(instance) = parse_instance_key(&identity) else {
            return ReportRequestV2Response::default();
        };
        let Some(record) = self.instances.get(&instance) else {
            return ReportRequestV2Response::default();
        };
        let group_id = {
            let record = record.lock().unwrap();
            if record.epoch != identity.epoch || record.lease_deadline <= Instant::now() {
                return ReportRequestV2Response::default();
            }
            record.group_id.clone()
        };
        if request_identity.request_id.is_empty() {
            return ReportRequestV2Response::default();
        }
        let key = RequestKey {
            instance,
            epoch: identity.epoch,
            request_id: request_identity.request_id,
        };
        match self.requests.entry(key) {
            Entry::Occupied(_) => {
                return ReportRequestV2Response {
                    accepted: true,
                    duplicate: true,
                };
            }
            Entry::Vacant(entry) => {
                entry.insert(Instant::now() + self.request_ttl);
            }
        }
        if let Some(group) = self.groups.get(&group_id) {
            let known_blocks = group.blocks.lock().unwrap();
            let mut demand = group.demand.lock().unwrap();
            for block in request.blocks {
                if known_blocks.contains_key(&block.seq_hash) {
                    let window = demand
                        .entry(block.seq_hash)
                        .or_insert_with(|| DemandWindow {
                            current: 0,
                            previous: 0,
                            started_at: Instant::now(),
                        });
                    rotate_demand_window(window, self.demand_window);
                    window.current = window.current.saturating_add(1);
                }
            }
        }
        ReportRequestV2Response {
            accepted: true,
            duplicate: false,
        }
    }

    pub fn report_request_end(
        &self,
        request: ReportRequestEndV2Request,
    ) -> ReportRequestV2Response {
        let Some(request_identity) = request.request else {
            return ReportRequestV2Response::default();
        };
        let Some(identity) = request_identity.instance else {
            return ReportRequestV2Response::default();
        };
        let Some(instance) = parse_instance_key(&identity) else {
            return ReportRequestV2Response::default();
        };
        let key = RequestKey {
            instance,
            epoch: identity.epoch,
            request_id: request_identity.request_id,
        };
        ReportRequestV2Response {
            accepted: self.requests.remove(&key).is_some(),
            duplicate: false,
        }
    }

    pub fn cleanup_expired(&self) -> usize {
        let now = Instant::now();
        let expired: Vec<_> = self
            .instances
            .iter()
            .filter_map(|entry| {
                let record = entry.value().lock().unwrap();
                (record.lease_deadline <= now).then(|| (entry.key().clone(), record.clone()))
            })
            .collect();
        let mut removed_count = 0;
        for (key, record) in &expired {
            let removed = self
                .instances
                .remove_if(key, |_, current| {
                    current.lock().unwrap().handle == record.handle
                })
                .is_some();
            if removed {
                removed_count += 1;
                if let Some(group) = self.groups.get(&record.group_id) {
                    remove_instance_replicas(group.value(), record.handle);
                }
                self.inventory_syncs
                    .lock()
                    .unwrap()
                    .retain(|_, sync| sync.instance_handle != record.handle);
                self.requests
                    .retain(|request, _| &request.instance != key);
            }
        }
        self.cleanup_expired_requests();
        for group in self.groups.iter() {
            cleanup_expired_demand(group.value(), self.demand_window);
        }
        self.lease_expired_total
            .fetch_add(removed_count as u64, Ordering::Relaxed);
        removed_count
    }

    fn inventory_failure<T>(&self, response: T) -> T {
        self.inventory_sync_failure_total
            .fetch_add(1, Ordering::Relaxed);
        response
    }

    fn cleanup_expired_requests(&self) {
        let now = Instant::now();
        self.requests.retain(|_, deadline| *deadline > now);
    }

    fn resolve_session(
        &self,
        session: Option<&InstanceSessionV2>,
    ) -> Result<(InstanceKey, Arc<Mutex<InstanceRecord>>), ()> {
        let session = session.ok_or(())?;
        let identity = session.instance.as_ref().ok_or(())?;
        let key = parse_instance_key(identity).ok_or(())?;
        let record = self.instances.get(&key).ok_or(())?.value().clone();
        {
            let locked = record.lock().unwrap();
            if locked.epoch != identity.epoch
                || locked.lease_id != session.lease_id
                || locked.lease_deadline <= Instant::now()
            {
                return Err(());
            }
        }
        Ok((key, record))
    }

    pub fn group_ids(&self) -> Vec<Vec<u8>> {
        self.groups.iter().map(|entry| entry.key().clone()).collect()
    }

    pub fn status_snapshot(&self) -> V2StatusSnapshot {
        let now = Instant::now();
        let mut instance_counts: HashMap<Vec<u8>, (usize, usize)> = HashMap::new();
        for instance in self.instances.iter() {
            let instance = instance.value().lock().unwrap();
            if instance.lease_deadline <= now {
                continue;
            }
            let counts = instance_counts.entry(instance.group_id.clone()).or_default();
            counts.0 += 1;
            if instance.inventory_ready {
                counts.1 += 1;
            }
        }
        let mut groups = Vec::new();
        for group in self.groups.iter() {
            let group_id = group.key();
            let Some(&(registered_instances, ready_instances)) =
                instance_counts.get(group_id.as_slice())
            else {
                continue;
            };
            let blocks = group.blocks.lock().unwrap();
            groups.push(GroupStatusSnapshot {
                compatibility_group_id: hex_bytes(group_id),
                registered_instances,
                ready_instances,
                blocks: blocks.len(),
                replicas: blocks.values().map(|block| block.replicas.len()).sum(),
                ready: ready_instances == registered_instances,
            });
        }
        groups.sort_by(|left, right| {
            left.compatibility_group_id
                .cmp(&right.compatibility_group_id)
        });
        V2StatusSnapshot {
            meta_generation: self.meta_generation.clone(),
            ready: !groups.is_empty() && groups.iter().all(|group| group.ready),
            pending_inventory_syncs: self.inventory_syncs.lock().unwrap().len(),
            active_requests: self.requests.len(),
            lease_expired_total: self.lease_expired_total.load(Ordering::Relaxed),
            mutation_sequence_gap_total: self
                .mutation_sequence_gap_total
                .load(Ordering::Relaxed),
            inventory_sync_success_total: self
                .inventory_sync_success_total
                .load(Ordering::Relaxed),
            inventory_sync_failure_total: self
                .inventory_sync_failure_total
                .load(Ordering::Relaxed),
            inventory_sync_blocks_total: self
                .inventory_sync_blocks_total
                .load(Ordering::Relaxed),
            inventory_sync_duration_ms_total: self
                .inventory_sync_duration_ms_total
                .load(Ordering::Relaxed),
            reconcile_mismatch_total: self
                .reconcile_mismatch_total
                .load(Ordering::Relaxed),
            groups,
        }
    }

    pub fn select_rebalance_candidates(
        &self,
        group_id: &[u8],
        reserve_bytes: u64,
        bytes_per_token: u64,
        max_replicas: usize,
        min_benefit: f64,
        max_evictions_per_second: f64,
        target_max_usage_ratio: f64,
        max_blocks: usize,
    ) -> Vec<RebalanceCandidate> {
        let now = Instant::now();
        let instances: Vec<_> = self
            .instances
            .iter()
            .filter_map(|entry| {
                let record = entry.value().lock().unwrap();
                (record.group_id == group_id
                    && record.inventory_ready
                    && record.lease_deadline > now)
                    .then(|| record.clone())
            })
            .collect();
        let Some(group) = self.groups.get(group_id) else {
            return Vec::new();
        };
        let blocks = group.blocks.lock().unwrap();
        let mut demand = group.demand.lock().unwrap();
        let mut candidates = Vec::new();
        for (seq_hash, block) in blocks.iter() {
            if block.replicas.is_empty() || block.replicas.len() >= max_replicas {
                continue;
            }
            let Some(window) = demand.get_mut(seq_hash) else {
                continue;
            };
            rotate_demand_window(window, self.demand_window);
            let score = window.current as f64 + 0.5 * window.previous as f64;
            let replicas = block.replicas.len() as f64;
            let benefit = score / replicas - score / (replicas + 1.0);
            if benefit <= min_benefit {
                continue;
            }
            let mut chain = Vec::new();
            let mut current = block.descriptor.clone();
            loop {
                chain.push(current.clone());
                if current.parent_hash.is_empty() {
                    break;
                }
                let Some(parent) = blocks.get(&current.parent_hash) else {
                    chain.clear();
                    break;
                };
                current = parent.descriptor.clone();
            }
            if chain.is_empty() || chain.len() > max_blocks {
                continue;
            }
            chain.reverse();
            let Some(source) = instances.iter().find(|instance| {
                chain.iter().all(|descriptor| {
                    blocks.get(&descriptor.seq_hash).is_some_and(|entry| {
                        entry.replicas.contains_key(&instance.handle)
                    })
                })
            }) else {
                continue;
            };
            let target = instances
                .iter()
                .filter(|instance| {
                    !block.replicas.contains_key(&instance.handle)
                        && instance.capacity.capacity_bytes > 0
                        && instance.eviction_rate <= max_evictions_per_second
                        && instance.capacity.used_bytes as f64
                            / instance.capacity.capacity_bytes as f64
                            <= target_max_usage_ratio
                        && chain.iter().all(|descriptor| {
                            blocks.get(&descriptor.seq_hash).is_some_and(|entry| {
                                entry.replicas.contains_key(&instance.handle)
                                    || entry.replicas.len() < max_replicas
                            })
                        })
                })
                .filter_map(|instance| {
                    let missing_tokens: u64 = chain
                        .iter()
                        .filter(|descriptor| {
                            blocks
                                .get(&descriptor.seq_hash)
                                .map_or(true, |entry| {
                                    !entry.replicas.contains_key(&instance.handle)
                                })
                        })
                        .map(|descriptor| u64::from(descriptor.offset))
                        .sum();
                    let estimated = missing_tokens.saturating_mul(bytes_per_token);
                    let free = instance
                        .capacity
                        .capacity_bytes
                        .saturating_sub(instance.capacity.used_bytes);
                    (free >= estimated.saturating_add(reserve_bytes))
                        .then_some((instance, estimated))
                })
                .max_by_key(|(instance, _)| {
                    instance
                        .capacity
                        .capacity_bytes
                        .saturating_sub(instance.capacity.used_bytes)
                });
            let Some((target, estimated_bytes)) = target else {
                continue;
            };
            candidates.push(RebalanceCandidate {
                group_id: group_id.to_vec(),
                source: instance_identity(source),
                source_endpoints: source.endpoints.clone(),
                target: instance_identity(target),
                target_endpoints: target.endpoints.clone(),
                blocks: chain,
                demand_score: benefit,
                estimated_bytes,
            });
        }
        candidates.sort_by(|left, right| {
            let left_efficiency =
                left.demand_score / left.estimated_bytes.max(1) as f64;
            let right_efficiency =
                right.demand_score / right.estimated_bytes.max(1) as f64;
            right_efficiency
                .total_cmp(&left_efficiency)
                .then_with(|| left.estimated_bytes.cmp(&right.estimated_bytes))
        });
        candidates
    }

    pub fn report_mutations(
        &self,
        request: ReportCacheMutationsV2Request,
    ) -> ReportCacheMutationsV2Response {
        self.cleanup_expired();
        let Ok((_key, instance)) = self.resolve_session(request.session.as_ref()) else {
            return mutation_error(
                MutationStatusV2::StaleLease,
                0,
                1,
                0,
                "invalid or expired instance session",
            );
        };
        let mut instance = instance.lock().unwrap();
        if instance.group_id != request.compatibility_group_id {
            return mutation_error(
                MutationStatusV2::WrongGroup,
                instance.committed_event_seq,
                instance.committed_event_seq + 1,
                0,
                "compatibility group does not match registration",
            );
        }

        let group = self
            .groups
            .entry(instance.group_id.clone())
            .or_default()
            .clone();
        let mut live_blocks = group.blocks.lock().unwrap();
        let mut staged_blocks = live_blocks.clone();
        let mut next_seq = instance.committed_event_seq + 1;
        let mut committed_seq = instance.committed_event_seq;
        let mut applied = false;

        for event in request.events {
            if event.event_seq <= instance.committed_event_seq {
                continue;
            }
            if event.event_seq != next_seq {
                instance.inventory_ready = false;
                self.mutation_sequence_gap_total
                    .fetch_add(1, Ordering::Relaxed);
                return mutation_error(
                    MutationStatusV2::SequenceGap,
                    instance.committed_event_seq,
                    next_seq,
                    event.event_seq,
                    "mutation sequence gap",
                );
            }
            if let Err(detail) = apply_event(
                &mut staged_blocks,
                instance.handle,
                instance.chunk_size,
                event.event_seq,
                event.payload,
            ) {
                instance.inventory_ready = false;
                return mutation_error(
                    MutationStatusV2::InvalidDescriptor,
                    instance.committed_event_seq,
                    next_seq,
                    event.event_seq,
                    &detail,
                );
            }
            committed_seq = event.event_seq;
            next_seq += 1;
            applied = true;
        }

        if applied {
            *live_blocks = staged_blocks;
            instance.committed_event_seq = committed_seq;
        }
        ReportCacheMutationsV2Response {
            status: if applied {
                MutationStatusV2::Committed as i32
            } else {
                MutationStatusV2::Duplicate as i32
            },
            committed_through_seq: instance.committed_event_seq,
            expected_next_seq: instance.committed_event_seq + 1,
            require_inventory_sync: false,
            failed_event_seq: 0,
            error_detail: String::new(),
        }
    }

    /// Atomically validate a V2 response and commit only authoritative block results.
    pub fn commit_transfer_results(
        &self,
        request: &TransferKvV2Request,
        results: &[BlockTransferResultV2],
    ) -> Result<usize, String> {
        if request.blocks.len() != results.len() {
            return Err("transfer response cardinality mismatch".to_string());
        }
        if request
            .blocks
            .iter()
            .zip(results)
            .any(|(block, result)| block.seq_hash != result.seq_hash)
        {
            return Err("transfer response order/hash mismatch".to_string());
        }
        let source = self.resolve_transfer_instance(request.source.as_ref())?;
        let target = self.resolve_transfer_instance(request.target.as_ref())?;
        if source.group_id != request.compatibility_group_id
            || target.group_id != request.compatibility_group_id
        {
            return Err("transfer instance is outside compatibility group".to_string());
        }
        let group = self
            .groups
            .get(&request.compatibility_group_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| "compatibility group is unknown".to_string())?;
        let mut blocks = group.blocks.lock().unwrap();
        let mut committed = 0;
        for (descriptor, result) in request.blocks.iter().zip(results) {
            let status = BlockTransferStatusV2::try_from(result.status)
                .map_err(|_| "unknown block transfer status".to_string())?;
            match status {
                BlockTransferStatusV2::Copied
                | BlockTransferStatusV2::AlreadyPresent => {
                    if result.target_replica_version == 0 {
                        continue;
                    }
                    let Some(block) = blocks.get_mut(&descriptor.seq_hash) else {
                        continue;
                    };
                    if !same_descriptor(&block.descriptor, descriptor) {
                        return Err("transfer descriptor conflicts with shadow block".to_string());
                    }
                    let last_version = block
                        .last_versions
                        .get(&target.handle)
                        .copied()
                        .unwrap_or(0);
                    if result.target_replica_version < last_version {
                        continue;
                    }
                    block
                        .last_versions
                        .insert(target.handle, result.target_replica_version);
                    block
                        .replicas
                        .insert(target.handle, result.target_replica_version);
                    committed += 1;
                },
                BlockTransferStatusV2::SourceMissing => {
                    if let Some(block) = blocks.get_mut(&descriptor.seq_hash) {
                        block.replicas.remove(&source.handle);
                    }
                },
                _ => {},
            }
        }
        Ok(committed)
    }

    fn resolve_transfer_instance(
        &self,
        identity: Option<&InstanceIdentityV2>,
    ) -> Result<InstanceRecord, String> {
        let identity = identity.ok_or_else(|| "missing transfer identity".to_string())?;
        let key = parse_instance_key(identity)
            .ok_or_else(|| "invalid transfer identity".to_string())?;
        let record = self
            .instances
            .get(&key)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| "transfer instance is not registered".to_string())?;
        let record = record.lock().unwrap();
        if record.epoch != identity.epoch {
            return Err("stale transfer instance epoch".to_string());
        }
        if record.lease_deadline <= Instant::now() || !record.inventory_ready {
            return Err("transfer instance is not ready".to_string());
        }
        Ok(record.clone())
    }
}

fn apply_event(
    blocks: &mut HashMap<Vec<u8>, ShadowBlock>,
    instance_handle: u64,
    chunk_size: u32,
    event_seq: u64,
    payload: Option<Payload>,
) -> Result<(), String> {
    match payload {
        Some(Payload::Store(store)) => {
            let mut available: HashSet<Vec<u8>> = blocks
                .iter()
                .filter(|(_, block)| block.replicas.contains_key(&instance_handle))
                .map(|(hash, _)| hash.clone())
                .collect();
            for descriptor in store.blocks {
                validate_descriptor(&descriptor, chunk_size, &available)?;
                if let Some(existing) = blocks.get(&descriptor.seq_hash) {
                    if !same_descriptor(&existing.descriptor, &descriptor) {
                        return Err("descriptor conflicts with existing group block".to_string());
                    }
                }
                let seq_hash = descriptor.seq_hash.clone();
                available.insert(seq_hash.clone());
                let block = blocks
                    .entry(seq_hash)
                    .or_insert_with(|| ShadowBlock {
                        descriptor,
                        replicas: HashMap::new(),
                        last_versions: HashMap::new(),
                    });
                block.last_versions.insert(instance_handle, event_seq);
                block.replicas.insert(instance_handle, event_seq);
            }
        },
        Some(Payload::Remove(remove)) => {
            for seq_hash in remove.seq_hashes {
                if seq_hash.len() != 32 {
                    return Err("removed seq_hash must be exactly 32 bytes".to_string());
                }
                if let Some(block) = blocks.get_mut(&seq_hash) {
                    block.replicas.remove(&instance_handle);
                    block.last_versions.insert(instance_handle, event_seq);
                }
            }
        },
        None => return Err("mutation event payload is missing".to_string()),
    }
    Ok(())
}

fn rotate_demand_window(window: &mut DemandWindow, duration: Duration) {
    let elapsed = window.started_at.elapsed();
    if elapsed >= duration.saturating_mul(2) {
        window.previous = 0;
        window.current = 0;
        window.started_at = Instant::now();
    } else if elapsed >= duration {
        window.previous = window.current;
        window.current = 0;
        window.started_at += duration;
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn instance_identity(record: &InstanceRecord) -> InstanceIdentityV2 {
    InstanceIdentityV2 {
        key: Some(InstanceKeyV2 {
            lmcache_instance_id: record.key.lmcache_instance_id.clone(),
            worker_id: record.key.worker_id,
        }),
        epoch: record.epoch.clone(),
    }
}

fn validate_descriptor(
    descriptor: &BlockDescriptorV2,
    chunk_size: u32,
    available: &HashSet<Vec<u8>>,
) -> Result<(), String> {
    if descriptor.seq_hash.len() != 32 {
        return Err("seq_hash must be exactly 32 bytes".to_string());
    }
    if descriptor.offset == 0 || descriptor.offset > chunk_size {
        return Err("offset is outside the registered chunk size".to_string());
    }
    if descriptor.token_ids.len() != descriptor.offset as usize {
        return Err("token_ids length does not match offset".to_string());
    }
    if descriptor.position == 0 {
        if !descriptor.parent_hash.is_empty() {
            return Err("root descriptor must not contain parent_hash".to_string());
        }
    } else if descriptor.parent_hash.len() != 32 {
        return Err("non-root descriptor must contain a 32-byte parent_hash".to_string());
    } else if !available.contains(&descriptor.parent_hash) {
        return Err("parent block is not present on the reporting instance".to_string());
    }
    Ok(())
}

fn same_descriptor(left: &BlockDescriptorV2, right: &BlockDescriptorV2) -> bool {
    left.seq_hash == right.seq_hash
        && left.parent_hash == right.parent_hash
        && left.position == right.position
        && left.offset == right.offset
        && left.token_ids == right.token_ids
}

fn parse_instance_key(identity: &InstanceIdentityV2) -> Option<InstanceKey> {
    let InstanceKeyV2 {
        lmcache_instance_id,
        worker_id,
    } = identity.key.as_ref()?;
    if lmcache_instance_id.is_empty() || identity.epoch.is_empty() {
        return None;
    }
    Some(InstanceKey {
        lmcache_instance_id: lmcache_instance_id.clone(),
        worker_id: *worker_id,
    })
}

fn fingerprint_group_id(fingerprint: &CompatibilityFingerprintV2) -> Vec<u8> {
    let mut hasher = Sha256::new();
    for value in [
        &fingerprint.model_name,
        &fingerprint.model_revision,
        &fingerprint.tokenizer_name,
        &fingerprint.tokenizer_revision,
        &fingerprint.hash_algorithm,
        &fingerprint.python_hash_seed,
        &fingerprint.kv_dtype,
        &fingerprint.kv_layout,
    ] {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    for value in [
        fingerprint.hash_seed,
        fingerprint.chunk_size as u64,
        fingerprint.save_unfull_chunk as u64,
        fingerprint.tensor_parallel_size as u64,
        fingerprint.pipeline_parallel_size as u64,
        fingerprint.world_size as u64,
    ] {
        hasher.update(value.to_be_bytes());
    }
    let mut tags = fingerprint.cache_key_tags.clone();
    tags.sort();
    for tag in &tags {
        hasher.update((tag.len() as u64).to_be_bytes());
        hasher.update(tag.as_bytes());
    }
    hasher.finalize().to_vec()
}

fn remove_instance_replicas(group: &GroupState, instance_handle: u64) {
    let mut blocks = group.blocks.lock().unwrap();
    remove_instance_replicas_from_map(&mut blocks, instance_handle);
}

fn cleanup_expired_demand(group: &GroupState, duration: Duration) {
    let retention = duration.saturating_mul(2);
    group
        .demand
        .lock()
        .unwrap()
        .retain(|_, window| window.started_at.elapsed() < retention);
}

fn remove_instance_replicas_from_map(
    blocks: &mut HashMap<Vec<u8>, ShadowBlock>,
    instance_handle: u64,
) {
    blocks.retain(|_, block| {
        block.replicas.remove(&instance_handle);
        block.last_versions.remove(&instance_handle);
        !block.replicas.is_empty()
    });
}

fn inventory_checksum(blocks: &[BlockDescriptorV2]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    for block in blocks {
        for bytes in [&block.seq_hash, &block.parent_hash] {
            hasher.update((bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
        }
        hasher.update(block.position.to_be_bytes());
        hasher.update(block.offset.to_be_bytes());
        hasher.update((block.token_ids.len() as u64).to_be_bytes());
        for token in &block.token_ids {
            hasher.update(token.to_be_bytes());
        }
    }
    hasher.finalize().to_vec()
}

fn register_error(status: RegisterStatusV2, detail: &str) -> RegisterInstanceV2Response {
    RegisterInstanceV2Response {
        status: status as i32,
        error_detail: detail.to_string(),
        ..Default::default()
    }
}

fn register_success(
    record: &InstanceRecord,
    lease_ttl: Duration,
    meta_generation: &str,
    require_inventory_sync: bool,
) -> RegisterInstanceV2Response {
    RegisterInstanceV2Response {
        status: RegisterStatusV2::Accepted as i32,
        compatibility_group_id: record.group_id.clone(),
        instance_handle: record.handle,
        lease_id: record.lease_id.clone(),
        lease_ttl_ms: lease_ttl.as_millis() as u64,
        meta_generation: meta_generation.to_string(),
        require_inventory_sync,
        protocol_minor: 0,
        capabilities: vec![
            "cache_mutation_shadow".to_string(),
            "lease_heartbeat".to_string(),
            "inventory_sync".to_string(),
            "request_lifecycle".to_string(),
        ],
        ..Default::default()
    }
}

fn inventory_begin_error(detail: &str) -> BeginInventorySyncV2Response {
    BeginInventorySyncV2Response {
        error_detail: detail.to_string(),
        ..Default::default()
    }
}

fn inventory_page_error(page_id: u32, detail: &str) -> UploadInventoryPageV2Response {
    UploadInventoryPageV2Response {
        page_id,
        error_detail: detail.to_string(),
        ..Default::default()
    }
}

fn inventory_commit_error(detail: &str) -> CommitInventorySyncV2Response {
    CommitInventorySyncV2Response {
        error_detail: detail.to_string(),
        ..Default::default()
    }
}

fn mutation_error(
    status: MutationStatusV2,
    committed: u64,
    expected: u64,
    failed: u64,
    detail: &str,
) -> ReportCacheMutationsV2Response {
    ReportCacheMutationsV2Response {
        status: status as i32,
        committed_through_seq: committed,
        expected_next_seq: expected,
        require_inventory_sync: status == MutationStatusV2::SequenceGap,
        failed_event_seq: failed,
        error_detail: detail.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cedfs_proto::kvcache_v2::{
        CacheMutationEventV2, InstanceEndpointsV2, InstanceSessionV2, RemoveBlocksV2,
        RequestIdentityV2, StoreBlocksV2,
    };
    use cedfs_proto::lmcache_v2::{BlockTransferResultV2, TransferKvV2Request};

    fn identity(instance_id: &str, worker_id: u32, epoch: &str) -> InstanceIdentityV2 {
        InstanceIdentityV2 {
            key: Some(InstanceKeyV2 {
                lmcache_instance_id: instance_id.to_string(),
                worker_id,
            }),
            epoch: epoch.to_string(),
        }
    }

    fn register_request(instance_id: &str, model: &str) -> RegisterInstanceV2Request {
        RegisterInstanceV2Request {
            protocol_major: 2,
            instance: Some(identity(instance_id, 0, "epoch-1")),
            endpoints: Some(InstanceEndpointsV2 {
                host: "127.0.0.1".to_string(),
                http_port: 8000,
                nixl_init_port: 8001,
                transfer_rpc_port: 8002,
                api_path: "/v1".to_string(),
            }),
            fingerprint: Some(CompatibilityFingerprintV2 {
                model_name: model.to_string(),
                hash_algorithm: "sha256_cbor".to_string(),
                chunk_size: 4,
                kv_dtype: "float16".to_string(),
                kv_layout: "kv".to_string(),
                tensor_parallel_size: 1,
                pipeline_parallel_size: 1,
                world_size: 1,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn store_request(
        instance_id: &str,
        group_id: Vec<u8>,
        event_seq: u64,
        descriptor: BlockDescriptorV2,
        lease_id: &str,
    ) -> ReportCacheMutationsV2Request {
        ReportCacheMutationsV2Request {
            session: Some(InstanceSessionV2 {
                instance: Some(identity(instance_id, 0, "epoch-1")),
                lease_id: lease_id.to_string(),
            }),
            compatibility_group_id: group_id,
            events: vec![CacheMutationEventV2 {
                event_seq,
                payload: Some(Payload::Store(StoreBlocksV2 {
                    blocks: vec![descriptor],
                })),
            }],
        }
    }

    fn root_descriptor(hash_byte: u8) -> BlockDescriptorV2 {
        BlockDescriptorV2 {
            seq_hash: vec![hash_byte; 32],
            position: 0,
            offset: 4,
            token_ids: vec![1, 2, 3, 4],
            ..Default::default()
        }
    }

    fn mark_ready(state: &V2State, instance_id: &str) {
        state
            .instances
            .get(&InstanceKey {
                lmcache_instance_id: instance_id.to_string(),
                worker_id: 0,
            })
            .unwrap()
            .lock()
            .unwrap()
            .inventory_ready = true;
    }

    fn session(instance_id: &str, lease_id: &str) -> InstanceSessionV2 {
        InstanceSessionV2 {
            instance: Some(identity(instance_id, 0, "epoch-1")),
            lease_id: lease_id.to_string(),
        }
    }

    fn remove_request(
        instance_id: &str,
        group_id: Vec<u8>,
        event_seq: u64,
        seq_hash: Vec<u8>,
        lease_id: &str,
    ) -> ReportCacheMutationsV2Request {
        ReportCacheMutationsV2Request {
            session: Some(InstanceSessionV2 {
                instance: Some(identity(instance_id, 0, "epoch-1")),
                lease_id: lease_id.to_string(),
            }),
            compatibility_group_id: group_id,
            events: vec![CacheMutationEventV2 {
                event_seq,
                payload: Some(Payload::Remove(RemoveBlocksV2 {
                    seq_hashes: vec![seq_hash],
                })),
            }],
        }
    }

    #[test]
    fn mutation_sequence_gap_does_not_modify_shadow_blocks() {
        let state = V2State::default();
        let registered = state.register(register_request("instance-a", "model-a"));
        let response = state.report_mutations(store_request(
            "instance-a",
            registered.compatibility_group_id.clone(),
            2,
            root_descriptor(1),
            &registered.lease_id,
        ));

        assert_eq!(response.status, MutationStatusV2::SequenceGap as i32);
        let group = state.groups.get(&registered.compatibility_group_id).unwrap();
        assert!(group.value().blocks.lock().unwrap().is_empty());
    }

    #[test]
    fn same_hash_is_isolated_between_compatibility_groups() {
        let state = V2State::default();
        let group_a = state.register(register_request("instance-a", "model-a"));
        let group_b = state.register(register_request("instance-b", "model-b"));

        assert_eq!(
            state
                .report_mutations(store_request(
                    "instance-a",
                    group_a.compatibility_group_id.clone(),
                    1,
                    root_descriptor(7),
                    &group_a.lease_id,
                ))
                .status,
            MutationStatusV2::Committed as i32
        );
        assert_eq!(
            state
                .report_mutations(store_request(
                    "instance-b",
                    group_b.compatibility_group_id.clone(),
                    1,
                    root_descriptor(7),
                    &group_b.lease_id,
                ))
                .status,
            MutationStatusV2::Committed as i32
        );
        assert_ne!(group_a.compatibility_group_id, group_b.compatibility_group_id);
        assert_eq!(state.groups.len(), 2);
    }

    #[test]
    fn non_root_descriptor_requires_parent_on_same_instance() {
        let state = V2State::default();
        let registered = state.register(register_request("instance-a", "model-a"));
        let descriptor = BlockDescriptorV2 {
            seq_hash: vec![2; 32],
            parent_hash: vec![1; 32],
            position: 1,
            offset: 4,
            token_ids: vec![5, 6, 7, 8],
        };

        let response = state.report_mutations(store_request(
            "instance-a",
            registered.compatibility_group_id,
            1,
            descriptor,
            &registered.lease_id,
        ));
        assert_eq!(response.status, MutationStatusV2::InvalidDescriptor as i32);
    }

    #[test]
    fn transfer_commits_only_successful_blocks() {
        let state = V2State::default();
        let source_registration = state.register(register_request("source", "model-a"));
        let target_registration = state.register(register_request("target", "model-a"));
        mark_ready(&state, "source");
        mark_ready(&state, "target");
        assert_eq!(
            source_registration.compatibility_group_id,
            target_registration.compatibility_group_id
        );
        let descriptors: Vec<_> = (1..=5).map(root_descriptor).collect();
        let mut source_store = store_request(
            "source",
            source_registration.compatibility_group_id.clone(),
            1,
            descriptors[0].clone(),
            &source_registration.lease_id,
        );
        source_store.events[0].payload = Some(Payload::Store(StoreBlocksV2 {
            blocks: descriptors.clone(),
        }));
        assert_eq!(
            state.report_mutations(source_store).status,
            MutationStatusV2::Committed as i32
        );
        let request = TransferKvV2Request {
            transfer_id: "partial".to_string(),
            compatibility_group_id: source_registration.compatibility_group_id.clone(),
            source: Some(identity("source", 0, "epoch-1")),
            target: Some(identity("target", 0, "epoch-1")),
            blocks: descriptors.clone(),
            do_copy: true,
            ..Default::default()
        };
        let statuses = [
            BlockTransferStatusV2::Copied,
            BlockTransferStatusV2::AlreadyPresent,
            BlockTransferStatusV2::TargetNoCapacity,
            BlockTransferStatusV2::ReadFailed,
            BlockTransferStatusV2::NotAttempted,
        ];
        let results: Vec<_> = descriptors
            .iter()
            .zip(statuses)
            .map(|(block, status)| BlockTransferResultV2 {
                seq_hash: block.seq_hash.clone(),
                status: status as i32,
                target_replica_version: 1,
                ..Default::default()
            })
            .collect();

        assert_eq!(state.commit_transfer_results(&request, &results), Ok(2));
        let target_handle = state
            .instances
            .get(&InstanceKey {
                lmcache_instance_id: "target".to_string(),
                worker_id: 0,
            })
            .unwrap()
            .lock()
            .unwrap()
            .handle;
        let group = state.groups.get(&request.compatibility_group_id).unwrap();
        let blocks = group.blocks.lock().unwrap();
        assert!(blocks[&descriptors[0].seq_hash]
            .replicas
            .contains_key(&target_handle));
        assert!(blocks[&descriptors[1].seq_hash]
            .replicas
            .contains_key(&target_handle));
        assert!(!blocks[&descriptors[2].seq_hash]
            .replicas
            .contains_key(&target_handle));
    }

    #[test]
    fn newer_target_remove_rejects_stale_transfer_result() {
        let state = V2State::default();
        let source = state.register(register_request("source", "model-a"));
        let target = state.register(register_request("target", "model-a"));
        mark_ready(&state, "source");
        mark_ready(&state, "target");
        let descriptor = root_descriptor(9);
        state.report_mutations(store_request(
            "source",
            source.compatibility_group_id.clone(),
            1,
            descriptor.clone(),
            &source.lease_id,
        ));
        state.report_mutations(store_request(
            "target",
            source.compatibility_group_id.clone(),
            1,
            descriptor.clone(),
            &target.lease_id,
        ));
        state.report_mutations(remove_request(
            "target",
            source.compatibility_group_id.clone(),
            2,
            descriptor.seq_hash.clone(),
            &target.lease_id,
        ));
        let request = TransferKvV2Request {
            compatibility_group_id: source.compatibility_group_id.clone(),
            source: Some(identity("source", 0, "epoch-1")),
            target: Some(identity("target", 0, "epoch-1")),
            blocks: vec![descriptor.clone()],
            do_copy: true,
            ..Default::default()
        };
        let result = BlockTransferResultV2 {
            seq_hash: descriptor.seq_hash.clone(),
            status: BlockTransferStatusV2::Copied as i32,
            target_replica_version: 1,
            ..Default::default()
        };

        assert_eq!(state.commit_transfer_results(&request, &[result]), Ok(0));
    }

    #[test]
    fn incomplete_inventory_never_replaces_live_index() {
        let state = V2State::default();
        let registered = state.register(register_request("instance-a", "model-a"));
        let descriptor = root_descriptor(3);
        let begin = state.begin_inventory_sync(BeginInventorySyncV2Request {
            session: Some(session("instance-a", &registered.lease_id)),
            compatibility_group_id: registered.compatibility_group_id.clone(),
            base_event_seq: 0,
            total_blocks: 1,
            total_pages: 2,
            inventory_checksum: inventory_checksum(&[descriptor.clone()]),
        });
        assert!(begin.accepted);
        assert!(state
            .upload_inventory_page(UploadInventoryPageV2Request {
                session: Some(session("instance-a", &registered.lease_id)),
                sync_id: begin.sync_id.clone(),
                page_id: 0,
                blocks: vec![descriptor.clone()],
                page_checksum: inventory_checksum(&[descriptor]),
            })
            .accepted);
        assert!(!state
            .commit_inventory_sync(CommitInventorySyncV2Request {
                session: Some(session("instance-a", &registered.lease_id)),
                sync_id: begin.sync_id,
            })
            .committed);
        let group = state.groups.get(&registered.compatibility_group_id).unwrap();
        assert!(group.blocks.lock().unwrap().is_empty());
    }

    #[test]
    fn request_lifecycle_is_idempotent_and_composite() {
        let state = V2State::default();
        state.register(register_request("instance-a", "model-a"));
        let start = ReportRequestStartV2Request {
            request: Some(RequestIdentityV2 {
                instance: Some(identity("instance-a", 0, "epoch-1")),
                request_id: "request-1".to_string(),
            }),
            blocks: Vec::new(),
        };
        assert!(!state.report_request_start(start.clone()).duplicate);
        assert!(state.report_request_start(start).duplicate);
        assert!(state
            .report_request_end(ReportRequestEndV2Request {
                request: Some(RequestIdentityV2 {
                    instance: Some(identity("instance-a", 0, "epoch-1")),
                    request_id: "request-1".to_string(),
                }),
            })
            .accepted);
    }

    #[test]
    fn rebalance_selects_ready_empty_target_with_capacity() {
        let state = V2State::default();
        let source = state.register(register_request("source", "model-a"));
        let target = state.register(register_request("target", "model-a"));
        mark_ready(&state, "source");
        mark_ready(&state, "target");
        let descriptor = root_descriptor(4);
        state.report_mutations(store_request(
            "source",
            source.compatibility_group_id.clone(),
            1,
            descriptor.clone(),
            &source.lease_id,
        ));
        assert!(state
            .heartbeat(HeartbeatV2Request {
                session: Some(session("target", &target.lease_id)),
                capacity: Some(CapacitySnapshotV2 {
                    capacity_bytes: 1_000_000,
                    used_bytes: 0,
                    eviction_count: 0,
                }),
                known_meta_generation: target.meta_generation.clone(),
                ..Default::default()
            })
            .accepted);
        state.report_request_start(ReportRequestStartV2Request {
            request: Some(RequestIdentityV2 {
                instance: Some(identity("source", 0, "epoch-1")),
                request_id: "hot-request".to_string(),
            }),
            blocks: vec![descriptor],
        });

        let candidates = state.select_rebalance_candidates(
            &source.compatibility_group_id,
            0,
            1,
            2,
            0.1,
            10.0,
            0.85,
            16,
        );
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0]
                .target
                .key
                .as_ref()
                .unwrap()
                .lmcache_instance_id,
            "target"
        );

        assert!(state
            .heartbeat(HeartbeatV2Request {
                session: Some(session("target", &target.lease_id)),
                capacity: Some(CapacitySnapshotV2 {
                    capacity_bytes: 1_000_000,
                    used_bytes: 900_000,
                    eviction_count: 0,
                }),
                known_meta_generation: target.meta_generation,
                ..Default::default()
            })
            .accepted);
        assert!(state
            .select_rebalance_candidates(
                &source.compatibility_group_id,
                0,
                1,
                2,
                0.1,
                10.0,
                0.85,
                16,
            )
            .is_empty());
    }

    #[test]
    fn demand_window_decays_and_expires_old_heat() {
        let duration = Duration::from_secs(10);
        let mut window = DemandWindow {
            current: 4,
            previous: 2,
            started_at: Instant::now() - duration,
        };
        rotate_demand_window(&mut window, duration);
        assert_eq!(window.current, 0);
        assert_eq!(window.previous, 4);

        window.started_at = Instant::now() - duration.saturating_mul(2);
        rotate_demand_window(&mut window, duration);
        assert_eq!(window.current, 0);
        assert_eq!(window.previous, 0);
    }

    #[test]
    fn status_distinguishes_registered_from_inventory_ready() {
        let state = V2State::default();
        state.register(register_request("instance-a", "model-a"));
        let recovering = state.status_snapshot();
        assert!(!recovering.ready);
        assert_eq!(recovering.groups.len(), 1);
        assert_eq!(recovering.groups[0].ready_instances, 0);

        mark_ready(&state, "instance-a");
        let ready = state.status_snapshot();
        assert!(ready.ready);
        assert_eq!(ready.groups[0].ready_instances, 1);
        assert_eq!(ready.inventory_sync_success_total, 1);
    }

    #[test]
    fn registration_rejects_wildcard_advertised_endpoint() {
        let state = V2State::default();
        let mut request = register_request("instance-a", "model-a");
        request.endpoints.as_mut().unwrap().host = "0.0.0.0".to_string();
        assert_eq!(
            state.register(request).status,
            RegisterStatusV2::InvalidArgument as i32
        );
    }
}
