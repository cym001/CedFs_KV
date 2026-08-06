use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use sha2::{Digest, Sha256};

use cedfs_proto::kvcache_v2::cache_mutation_event_v2::Payload;
use cedfs_proto::kvcache_v2::{
    BlockDescriptorV2, CompatibilityFingerprintV2, InstanceIdentityV2, InstanceKeyV2,
    MutationStatusV2, RegisterInstanceV2Request, RegisterInstanceV2Response, RegisterStatusV2,
    ReportCacheMutationsV2Request, ReportCacheMutationsV2Response,
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
    pub handle: u64,
    pub epoch: String,
    pub group_id: Vec<u8>,
    pub chunk_size: u32,
    pub committed_event_seq: u64,
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
}

#[derive(Debug, Default)]
pub struct V2State {
    next_instance_handle: AtomicU64,
    instances: DashMap<InstanceKey, Arc<Mutex<InstanceRecord>>>,
    groups: DashMap<Vec<u8>, Arc<GroupState>>,
}

impl V2State {
    pub fn register(&self, request: RegisterInstanceV2Request) -> RegisterInstanceV2Response {
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
        let Some(endpoints) = request.endpoints else {
            return register_error(RegisterStatusV2::InvalidArgument, "missing endpoints");
        };
        if endpoints.host.is_empty()
            || endpoints.nixl_init_port == 0
            || endpoints.transfer_rpc_port == 0
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
            let existing = existing.lock().unwrap();
            if existing.epoch == identity.epoch {
                if existing.group_id != group_id {
                    return register_error(
                        RegisterStatusV2::Incompatible,
                        "same instance epoch registered with a different fingerprint",
                    );
                }
                return register_success(&existing);
            }
        }
        let handle = self.next_instance_handle.fetch_add(1, Ordering::Relaxed) + 1;
        let record = InstanceRecord {
            handle,
            epoch: identity.epoch,
            group_id: group_id.clone(),
            chunk_size: fingerprint.chunk_size,
            committed_event_seq: 0,
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
            lease_id: String::new(),
            lease_ttl_ms: 0,
            meta_generation: String::new(),
            require_inventory_sync: false,
            protocol_minor: 0,
            capabilities: vec!["cache_mutation_shadow".to_string()],
        }
    }

    pub fn report_mutations(
        &self,
        request: ReportCacheMutationsV2Request,
    ) -> ReportCacheMutationsV2Response {
        let Some(session) = request.session else {
            return mutation_error(MutationStatusV2::StaleEpoch, 0, 1, 0, "missing session");
        };
        let Some(identity) = session.instance else {
            return mutation_error(
                MutationStatusV2::StaleEpoch,
                0,
                1,
                0,
                "missing instance identity",
            );
        };
        let Some(key) = parse_instance_key(&identity) else {
            return mutation_error(
                MutationStatusV2::StaleEpoch,
                0,
                1,
                0,
                "invalid instance identity",
            );
        };
        let Some(instance) = self
            .instances
            .get(&key)
            .map(|entry| Arc::clone(entry.value()))
        else {
            return mutation_error(
                MutationStatusV2::StaleEpoch,
                0,
                1,
                0,
                "instance is not registered",
            );
        };
        let mut instance = instance.lock().unwrap();
        if instance.epoch != identity.epoch {
            return mutation_error(
                MutationStatusV2::StaleEpoch,
                instance.committed_event_seq,
                instance.committed_event_seq + 1,
                0,
                "instance epoch does not match registration",
            );
        }
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
    blocks.retain(|_, block| {
        block.replicas.remove(&instance_handle);
        block.last_versions.remove(&instance_handle);
        !block.replicas.is_empty()
    });
}

fn register_error(status: RegisterStatusV2, detail: &str) -> RegisterInstanceV2Response {
    RegisterInstanceV2Response {
        status: status as i32,
        error_detail: detail.to_string(),
        ..Default::default()
    }
}

fn register_success(record: &InstanceRecord) -> RegisterInstanceV2Response {
    RegisterInstanceV2Response {
        status: RegisterStatusV2::Accepted as i32,
        compatibility_group_id: record.group_id.clone(),
        instance_handle: record.handle,
        protocol_minor: 0,
        capabilities: vec!["cache_mutation_shadow".to_string()],
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
        StoreBlocksV2,
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
                nixl_init_port: 8001,
                transfer_rpc_port: 8002,
                ..Default::default()
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
    ) -> ReportCacheMutationsV2Request {
        ReportCacheMutationsV2Request {
            session: Some(InstanceSessionV2 {
                instance: Some(identity(instance_id, 0, "epoch-1")),
                lease_id: String::new(),
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

    fn remove_request(
        instance_id: &str,
        group_id: Vec<u8>,
        event_seq: u64,
        seq_hash: Vec<u8>,
    ) -> ReportCacheMutationsV2Request {
        ReportCacheMutationsV2Request {
            session: Some(InstanceSessionV2 {
                instance: Some(identity(instance_id, 0, "epoch-1")),
                lease_id: String::new(),
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
        ));
        assert_eq!(response.status, MutationStatusV2::InvalidDescriptor as i32);
    }

    #[test]
    fn transfer_commits_only_successful_blocks() {
        let state = V2State::default();
        let source_registration = state.register(register_request("source", "model-a"));
        let target_registration = state.register(register_request("target", "model-a"));
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
        state.register(register_request("target", "model-a"));
        let descriptor = root_descriptor(9);
        state.report_mutations(store_request(
            "source",
            source.compatibility_group_id.clone(),
            1,
            descriptor.clone(),
        ));
        state.report_mutations(store_request(
            "target",
            source.compatibility_group_id.clone(),
            1,
            descriptor.clone(),
        ));
        state.report_mutations(remove_request(
            "target",
            source.compatibility_group_id.clone(),
            2,
            descriptor.seq_hash.clone(),
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
}
