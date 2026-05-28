use dashmap::DashMap;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, Weak};
use std::time::Instant;

use crate::types::{BlockHash, BlockHashInfo, ServerId};

type SharedBlock = Arc<RwLock<RadixBlock>>;

#[derive(Debug)]
pub struct RadixBlock {
    children: HashMap<BlockHash, SharedBlock>,
    parent: Weak<RwLock<RadixBlock>>,
    seq_hash: Option<BlockHash>,
    local_hash: Option<BlockHash>,
    position: usize,
    offset: u32,
    tokens: Vec<u32>,
    servers: HashSet<ServerId>,
    heat: u64,
    last_access: Option<Instant>,
}

impl RadixBlock {
    fn root() -> Self {
        Self {
            children: HashMap::new(),
            parent: Weak::new(),
            seq_hash: None,
            local_hash: None,
            position: 0,
            offset: 0,
            tokens: Vec::new(),
            servers: HashSet::new(),
            heat: 0,
            last_access: None,
        }
    }

    fn from_info(info: BlockHashInfo, parent: &SharedBlock) -> Self {
        Self {
            children: HashMap::new(),
            parent: Arc::downgrade(parent),
            seq_hash: Some(info.seq_hash),
            local_hash: Some(info.local_hash),
            position: info.position,
            offset: info.offset,
            tokens: info.tokens,
            servers: HashSet::new(),
            heat: 0,
            last_access: None,
        }
    }

    fn covers_info(&self, info: &BlockHashInfo) -> bool {
        self.seq_hash == Some(info.seq_hash)
            && self.local_hash == Some(info.local_hash)
            && self.position == info.position
    }
}

#[derive(Debug, Clone)]
pub struct BlockSnapshot {
    pub seq_hash: BlockHash,
    pub local_hash: BlockHash,
    pub position: usize,
    pub offset: u32,
    pub tokens: Vec<u32>,
    pub servers: Vec<ServerId>,
    pub heat: u64,
    pub parent_seq_hash: Option<BlockHash>,
}

#[derive(Debug, Clone)]
pub struct StoreReport {
    pub seq_hash: BlockHash,
    pub replica_count: u32,
    pub server_added: bool,
}

#[derive(Debug, Clone, Default)]
pub struct InstanceMetricsSnapshot {
    pub server_id: ServerId,
    pub total_heat: u64,
    pub kv_block_count: usize,
    pub total_replica_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct PressureExtremes {
    pub max_server: Option<ServerId>,
    pub max_pressure: f64,
    pub min_server: Option<ServerId>,
    pub min_pressure: f64,
}

#[derive(Debug, Clone)]
pub struct MigrationCandidate {
    pub seq_hash: BlockHash,
    pub src_server: ServerId,
    pub dst_server: ServerId,
    pub projected_pressure_drop: f64,
    pub projected_target_increase: f64,
}

#[derive(Debug, Clone)]
pub struct EvictionReport {
    pub seq_hash: BlockHash,
    pub server_id: ServerId,
    pub heat_before: u64,
    pub heat_after: u64,
    pub replica_count_before: u32,
    pub replica_count_after: u32,
    pub removed: bool,
}

#[derive(Debug)]
struct CandidateInternal {
    seq_hash: BlockHash,
    parent_seq_hash: Option<BlockHash>,
    position: usize,
    pressure_drop: f64,
    target_increase: f64,
}

#[derive(Debug)]
pub struct KvRadixTree {
    root: SharedBlock,
    lookup: DashMap<ServerId, RwLock<HashMap<BlockHash, SharedBlock>>>,
    block_index: DashMap<BlockHash, SharedBlock>,
}

impl Default for KvRadixTree {
    fn default() -> Self {
        Self::new()
    }
}

impl KvRadixTree {
    pub fn new() -> Self {
        Self {
            root: Arc::new(RwLock::new(RadixBlock::root())),
            lookup: DashMap::new(),
            block_index: DashMap::new(),
        }
    }

    pub fn instance_metrics_snapshots(&self) -> Vec<InstanceMetricsSnapshot> {
        let mut snapshots = Vec::new();

        for entry in self.lookup.iter() {
            let server_id = *entry.key();
            let server_map = entry.value().read().expect("server lookup poisoned");
            let mut snapshot = InstanceMetricsSnapshot {
                server_id,
                kv_block_count: server_map.len(),
                ..Default::default()
            };

            for node in server_map.values() {
                let block = node.read().expect("radix block poisoned");
                snapshot.total_heat = snapshot.total_heat.saturating_add(block.heat);
                snapshot.total_replica_count = snapshot
                    .total_replica_count
                    .saturating_add(block.servers.len() as u64);
            }

            snapshots.push(snapshot);
        }

        snapshots.sort_by_key(|snapshot| snapshot.server_id);
        snapshots
    }

    pub fn store_blocks(&self, server_id: ServerId, blocks: &[BlockHashInfo]) -> Vec<StoreReport> {
        if blocks.is_empty() {
            return Vec::new();
        }

        self.lookup
            .entry(server_id)
            .or_insert_with(|| RwLock::new(HashMap::new()));

        let matched_len = self.match_len_for_server(server_id, blocks);
        let mut reports = Vec::with_capacity(blocks.len().saturating_sub(matched_len));
        let mut current = if matched_len == 0 {
            self.root.clone()
        } else {
            self.lookup_node_for_server(server_id, blocks[matched_len - 1].seq_hash)
                .unwrap_or_else(|| self.root.clone())
        };

        for info in blocks.iter().skip(matched_len).cloned() {
            let seq_hash = info.seq_hash;
            let child = self.child_or_insert(&current, info);
            let (server_added, replica_count) = {
                let mut node = child.write().expect("radix block poisoned");
                let server_added = node.servers.insert(server_id);
                (server_added, node.servers.len() as u32)
            };

            self.block_index.insert(seq_hash, child.clone());
            self.insert_server_lookup(server_id, seq_hash, child.clone());
            reports.push(StoreReport {
                seq_hash,
                replica_count,
                server_added,
            });
            current = child;
        }

        reports
    }

    pub fn add_server(&self, seq_hash: BlockHash, server_id: ServerId) -> bool {
        let Some(node) = self.node_for_hash(seq_hash) else {
            return false;
        };
        let server_added = {
            let mut block = node.write().expect("radix block poisoned");
            block.servers.insert(server_id)
        };
        if server_added {
            self.insert_server_lookup(server_id, seq_hash, node);
        }
        server_added
    }

    pub fn remove_server(&self, seq_hash: BlockHash, server_id: ServerId) -> bool {
        let Some(node) = self.node_for_hash(seq_hash) else {
            return false;
        };

        if let Some(server_map) = self.lookup.get(&server_id) {
            server_map
                .write()
                .expect("server lookup poisoned")
                .remove(&seq_hash);
        }

        let (removed, empty_after_remove) = {
            let mut block = node.write().expect("radix block poisoned");
            let removed = block.servers.remove(&server_id);
            let empty_after_remove = block.servers.is_empty();
            if empty_after_remove {
                self.block_index.remove(&seq_hash);
            }
            (removed, empty_after_remove)
        };

        if empty_after_remove {
            self.prune_empty_leaf_chain(node);
        }

        removed
    }

    pub fn increment_heat(&self, seq_hash: BlockHash) -> Option<u64> {
        let node = self.node_for_hash(seq_hash)?;
        let mut block = node.write().expect("radix block poisoned");
        block.heat = block.heat.saturating_add(1);
        block.last_access = Some(Instant::now());
        Some(block.heat)
    }

    pub fn block_snapshot(&self, seq_hash: BlockHash) -> Option<BlockSnapshot> {
        let node = self.node_for_hash(seq_hash)?;
        let block = node.read().expect("radix block poisoned");
        let parent_seq_hash = block
            .parent
            .upgrade()
            .and_then(|parent| parent.read().expect("radix block poisoned").seq_hash);

        Some(BlockSnapshot {
            seq_hash: block.seq_hash?,
            local_hash: block.local_hash?,
            position: block.position,
            offset: block.offset,
            tokens: block.tokens.clone(),
            servers: block.servers.iter().copied().collect(),
            heat: block.heat,
            parent_seq_hash,
        })
    }

    pub fn contains_server(&self, seq_hash: BlockHash, server_id: ServerId) -> bool {
        self.node_for_hash(seq_hash)
            .map(|node| {
                node.read()
                    .expect("radix block poisoned")
                    .servers
                    .contains(&server_id)
            })
            .unwrap_or(false)
    }

    pub fn replica_count(&self, seq_hash: BlockHash) -> u32 {
        self.node_for_hash(seq_hash)
            .map(|node| node.read().expect("radix block poisoned").servers.len() as u32)
            .unwrap_or(0)
    }

    pub fn find_matches(&self, blocks: &[BlockHashInfo]) -> Vec<(ServerId, u32)> {
        if blocks.is_empty() {
            return Vec::new();
        }

        let mut current = self.root.clone();
        let mut active_servers: Option<HashSet<ServerId>> = None;
        let mut matched_offsets: HashMap<ServerId, u32> = HashMap::new();

        for info in blocks.iter() {
            let Some(child) = Self::child_for_local(&current, info.local_hash) else {
                break;
            };

            let state_servers = {
                let block = child.read().expect("radix block poisoned");
                if !block.covers_info(info) {
                    break;
                }
                block.servers.clone()
            };

            if let Some(active) = &mut active_servers {
                active.retain(|server_id| state_servers.contains(server_id));
                if active.is_empty() {
                    break;
                }
                for server_id in active.iter() {
                    matched_offsets
                        .entry(*server_id)
                        .and_modify(|count| *count += info.offset)
                        .or_insert(info.offset);
                }
            } else {
                for server_id in &state_servers {
                    matched_offsets.insert(*server_id, info.offset);
                }
                active_servers = Some(state_servers);
            }

            current = child;
        }

        let mut result: Vec<_> = matched_offsets.into_iter().collect();
        result.sort_by_key(|(server_id, _)| *server_id);
        result
    }

    pub fn match_len_for_server(&self, server_id: ServerId, blocks: &[BlockHashInfo]) -> usize {
        let Some(server_map) = self.lookup.get(&server_id) else {
            return 0;
        };
        let server_map = server_map.read().expect("server lookup poisoned");
        let mut matched = 0;
        for info in blocks {
            let Some(node) = server_map.get(&info.seq_hash) else {
                break;
            };
            let block = node.read().expect("radix block poisoned");
            if !block.covers_info(info) || !block.servers.contains(&server_id) {
                break;
            }
            matched += 1;
        }
        matched
    }

    pub fn instance_pressure(&self, server_id: ServerId) -> f64 {
        let Some(server_map) = self.lookup.get(&server_id) else {
            return 0.0;
        };
        let server_map = server_map.read().expect("server lookup poisoned");
        server_map
            .values()
            .map(|node| {
                let block = node.read().expect("radix block poisoned");
                let replicas = block.servers.len();
                if replicas == 0 {
                    0.0
                } else {
                    block.heat as f64 / replicas as f64
                }
            })
            .sum()
    }

    pub fn pressure_extremes(&self) -> PressureExtremes {
        let mut extremes = PressureExtremes::default();

        for entry in self.lookup.iter() {
            let server_id = *entry.key();
            let pressure = self.instance_pressure(server_id);

            if extremes
                .max_server
                .map(|_| pressure > extremes.max_pressure)
                .unwrap_or(true)
            {
                extremes.max_server = Some(server_id);
                extremes.max_pressure = pressure;
            }

            if extremes
                .min_server
                .map(|_| pressure < extremes.min_pressure)
                .unwrap_or(true)
            {
                extremes.min_server = Some(server_id);
                extremes.min_pressure = pressure;
            }
        }

        extremes
    }

    pub fn select_replication_candidates(
        &self,
        src_server: ServerId,
        dst_server: ServerId,
    ) -> Vec<MigrationCandidate> {
        let mut candidates = self.replication_candidate_pool(src_server, dst_server);

        candidates.sort_by(|left, right| {
            right
                .pressure_drop
                .partial_cmp(&left.pressure_drop)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.parent_seq_hash.cmp(&right.parent_seq_hash))
                .then_with(|| left.position.cmp(&right.position))
        });
        candidates = Self::prefer_contiguous_candidates(candidates);

        let Some(start) = candidates.iter().position(|candidate| {
            candidate.pressure_drop > 0.0
                && candidate
                    .parent_seq_hash
                    .map(|parent_hash| self.contains_server(parent_hash, dst_server))
                    .unwrap_or(true)
        }) else {
            return Vec::new();
        };

        let mut selected = Vec::new();
        let mut current_hash = candidates[start].seq_hash;
        let first = candidates.remove(start);
        selected.push(MigrationCandidate {
            seq_hash: first.seq_hash,
            src_server,
            dst_server,
            projected_pressure_drop: first.pressure_drop,
            projected_target_increase: first.target_increase,
        });

        while let Some(next_idx) = candidates
            .iter()
            .position(|candidate| candidate.parent_seq_hash == Some(current_hash))
        {
            let next = candidates.remove(next_idx);
            if next.pressure_drop <= 0.0 {
                break;
            }
            current_hash = next.seq_hash;
            selected.push(MigrationCandidate {
                seq_hash: next.seq_hash,
                src_server,
                dst_server,
                projected_pressure_drop: next.pressure_drop,
                projected_target_increase: next.target_increase,
            });
        }

        selected
    }

    pub fn select_eviction_target(&self, server_id: ServerId) -> Option<BlockHash> {
        let server_map = self.lookup.get(&server_id)?;
        let server_map = server_map.read().expect("server lookup poisoned");

        server_map
            .iter()
            .filter_map(|(seq_hash, node)| {
                if self.has_child_for_server(node, server_id) {
                    return None;
                }
                let block = node.read().expect("radix block poisoned");
                Some((*seq_hash, block.last_access))
            })
            .min_by(
                |(_, left_access), (_, right_access)| match (left_access, right_access) {
                    (None, None) => Ordering::Equal,
                    (None, Some(_)) => Ordering::Less,
                    (Some(_), None) => Ordering::Greater,
                    (Some(left), Some(right)) => left.cmp(right),
                },
            )
            .map(|(seq_hash, _)| seq_hash)
    }

    pub fn apply_eviction(
        &self,
        server_id: ServerId,
        seq_hash: BlockHash,
    ) -> Option<EvictionReport> {
        let node = self.node_for_hash(seq_hash)?;
        let (heat_before, heat_after, replicas_before) = {
            let mut block = node.write().expect("radix block poisoned");
            if !block.servers.contains(&server_id) {
                return None;
            }
            let replicas_before = block.servers.len() as u32;
            let heat_before = block.heat;
            let shared_heat = if replicas_before == 0 {
                0
            } else {
                heat_before / replicas_before as u64
            };
            block.heat = block.heat.saturating_sub(shared_heat);
            (heat_before, block.heat, replicas_before)
        };

        let removed = self.remove_server(seq_hash, server_id);
        Some(EvictionReport {
            seq_hash,
            server_id,
            heat_before,
            heat_after,
            replica_count_before: replicas_before,
            replica_count_after: self.replica_count(seq_hash),
            removed,
        })
    }

    fn child_or_insert(&self, parent: &SharedBlock, info: BlockHashInfo) -> SharedBlock {
        {
            let parent_read = parent.read().expect("radix block poisoned");
            if let Some(child) = parent_read.children.get(&info.local_hash) {
                return child.clone();
            }
        }

        let mut parent_write = parent.write().expect("radix block poisoned");
        parent_write
            .children
            .entry(info.local_hash)
            .or_insert_with(|| Arc::new(RwLock::new(RadixBlock::from_info(info, parent))))
            .clone()
    }

    fn child_for_local(parent: &SharedBlock, local_hash: BlockHash) -> Option<SharedBlock> {
        parent
            .read()
            .expect("radix block poisoned")
            .children
            .get(&local_hash)
            .cloned()
    }

    fn insert_server_lookup(&self, server_id: ServerId, seq_hash: BlockHash, node: SharedBlock) {
        self.lookup
            .entry(server_id)
            .or_insert_with(|| RwLock::new(HashMap::new()));
        if let Some(server_map) = self.lookup.get(&server_id) {
            server_map
                .write()
                .expect("server lookup poisoned")
                .insert(seq_hash, node);
        }
    }

    fn lookup_node_for_server(
        &self,
        server_id: ServerId,
        seq_hash: BlockHash,
    ) -> Option<SharedBlock> {
        let server_map = self.lookup.get(&server_id)?;
        let node = server_map
            .read()
            .expect("server lookup poisoned")
            .get(&seq_hash)
            .cloned();
        node
    }

    fn node_for_hash(&self, seq_hash: BlockHash) -> Option<SharedBlock> {
        self.block_index
            .get(&seq_hash)
            .map(|entry| entry.value().clone())
    }

    fn prune_empty_leaf_chain(&self, mut node: SharedBlock) {
        loop {
            let (seq_hash, local_hash, parent, should_prune) = {
                let block = node.read().expect("radix block poisoned");
                (
                    block.seq_hash,
                    block.local_hash,
                    block.parent.upgrade(),
                    block.servers.is_empty() && block.children.is_empty(),
                )
            };

            if !should_prune {
                break;
            }

            if let Some(seq_hash) = seq_hash {
                self.block_index.remove(&seq_hash);
            }

            let Some(parent) = parent else {
                break;
            };
            let Some(local_hash) = local_hash else {
                break;
            };

            {
                let mut parent_write = parent.write().expect("radix block poisoned");
                let remove_child = parent_write
                    .children
                    .get(&local_hash)
                    .map(|child| Arc::ptr_eq(child, &node))
                    .unwrap_or(false);
                if remove_child {
                    parent_write.children.remove(&local_hash);
                }
            }

            node = parent;
        }
    }

    fn replication_candidate_pool(
        &self,
        src_server: ServerId,
        dst_server: ServerId,
    ) -> Vec<CandidateInternal> {
        let Some(src_map) = self.lookup.get(&src_server) else {
            return Vec::new();
        };
        let src_map = src_map.read().expect("server lookup poisoned");
        let dst_hashes: HashSet<BlockHash> = self
            .lookup
            .get(&dst_server)
            .map(|dst_map| {
                dst_map
                    .read()
                    .expect("server lookup poisoned")
                    .keys()
                    .copied()
                    .collect()
            })
            .unwrap_or_default();

        src_map
            .iter()
            .filter_map(|(seq_hash, node)| {
                if dst_hashes.contains(seq_hash) {
                    return None;
                }

                let block = node.read().expect("radix block poisoned");
                if !block.servers.contains(&src_server) {
                    return None;
                }

                let replicas = block.servers.len();
                if replicas == 0 {
                    return None;
                }

                let parent_seq_hash = block.parent.upgrade().and_then(|parent| {
                    let parent_read = parent.read().expect("radix block poisoned");
                    Some(parent_read.seq_hash)
                })?;

                let heat = block.heat as f64;
                let before = heat / replicas as f64;
                let after = heat / (replicas + 1) as f64;
                Some(CandidateInternal {
                    seq_hash: *seq_hash,
                    parent_seq_hash,
                    position: block.position,
                    pressure_drop: before - after,
                    target_increase: after,
                })
            })
            .collect()
    }

    fn prefer_contiguous_candidates(
        mut candidates: Vec<CandidateInternal>,
    ) -> Vec<CandidateInternal> {
        let mut ordered = Vec::with_capacity(candidates.len());
        while !candidates.is_empty() {
            let first = candidates.remove(0);
            let mut current_hash = first.seq_hash;
            ordered.push(first);

            while let Some(next_idx) = candidates
                .iter()
                .position(|candidate| candidate.parent_seq_hash == Some(current_hash))
            {
                let next = candidates.remove(next_idx);
                current_hash = next.seq_hash;
                ordered.push(next);
            }
        }
        ordered
    }

    fn has_child_for_server(&self, node: &SharedBlock, server_id: ServerId) -> bool {
        let children: Vec<_> = node
            .read()
            .expect("radix block poisoned")
            .children
            .values()
            .cloned()
            .collect();
        children.iter().any(|child| {
            child
                .read()
                .expect("radix block poisoned")
                .servers
                .contains(&server_id)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn hash(byte: u8) -> BlockHash {
        let mut value = [0u8; 32];
        value[0] = byte;
        value
    }

    fn info(position: usize, local: u8, seq: u8, offset: u32) -> BlockHashInfo {
        let start = position as u32 * offset;
        BlockHashInfo {
            position,
            local_hash: hash(local),
            seq_hash: hash(seq),
            offset,
            tokens: (start..start + offset).collect(),
        }
    }

    #[test]
    fn store_and_find_prefix_matches() {
        let index = KvRadixTree::new();
        let blocks = vec![info(0, 10, 20, 4), info(1, 11, 21, 4)];

        let stored = index.store_blocks(7, &blocks);
        assert_eq!(stored.len(), 2);
        assert_eq!(index.match_len_for_server(7, &blocks), 2);
        assert_eq!(index.find_matches(&blocks), vec![(7, 8)]);
    }

    #[test]
    fn stores_parent_chain_and_snapshots() {
        let index = KvRadixTree::new();
        let blocks = vec![info(0, 10, 20, 4), info(1, 11, 21, 4)];
        index.store_blocks(1, &blocks);

        let first = index.block_snapshot(blocks[0].seq_hash).unwrap();
        let second = index.block_snapshot(blocks[1].seq_hash).unwrap();
        assert_eq!(first.parent_seq_hash, None);
        assert_eq!(second.parent_seq_hash, Some(blocks[0].seq_hash));
        assert_eq!(second.position, 1);
        assert_eq!(second.offset, 4);
        assert_eq!(first.tokens, blocks[0].tokens);
        assert_eq!(second.tokens, blocks[1].tokens);
    }

    #[test]
    fn supports_multiple_servers_for_same_path() {
        let index = KvRadixTree::new();
        let blocks = vec![info(0, 10, 20, 4), info(1, 11, 21, 4)];
        index.store_blocks(1, &blocks);
        index.store_blocks(2, &blocks);

        assert_eq!(index.replica_count(blocks[0].seq_hash), 2);
        assert!(index.contains_server(blocks[1].seq_hash, 1));
        assert!(index.contains_server(blocks[1].seq_hash, 2));
        assert_eq!(index.find_matches(&blocks), vec![(1, 8), (2, 8)]);
    }

    #[test]
    fn remove_server_cleans_empty_leaf() {
        let index = KvRadixTree::new();
        let blocks = vec![info(0, 10, 20, 4), info(1, 11, 21, 4)];
        index.store_blocks(1, &blocks);
        index.store_blocks(2, &blocks);

        assert!(index.remove_server(blocks[1].seq_hash, 1));
        assert!(!index.contains_server(blocks[1].seq_hash, 1));
        assert!(index.contains_server(blocks[1].seq_hash, 2));

        assert!(index.remove_server(blocks[1].seq_hash, 2));
        assert!(index.block_snapshot(blocks[1].seq_hash).is_none());
        assert!(index.block_snapshot(blocks[0].seq_hash).is_some());
    }

    #[test]
    fn concurrent_store_and_heat_updates() {
        let index = Arc::new(KvRadixTree::new());
        let blocks = vec![info(0, 10, 20, 4), info(1, 11, 21, 4)];

        let mut handles = Vec::new();
        for server_id in 0..8 {
            let index = Arc::clone(&index);
            let blocks = blocks.clone();
            handles.push(thread::spawn(move || {
                index.store_blocks(server_id, &blocks);
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(index.replica_count(blocks[0].seq_hash), 8);

        let mut handles = Vec::new();
        for _ in 0..16 {
            let index = Arc::clone(&index);
            let seq_hash = blocks[0].seq_hash;
            handles.push(thread::spawn(move || {
                index.increment_heat(seq_hash);
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        let snapshot = index.block_snapshot(blocks[0].seq_hash).unwrap();
        assert_eq!(snapshot.heat, 16);
    }

    #[test]
    fn pressure_extremes_match_manual_values() {
        let index = KvRadixTree::new();
        let blocks = vec![info(0, 10, 20, 4), info(1, 11, 21, 4)];
        index.store_blocks(1, &blocks);
        index.store_blocks(2, &blocks[..1]);
        for _ in 0..6 {
            index.increment_heat(blocks[0].seq_hash);
        }
        for _ in 0..4 {
            index.increment_heat(blocks[1].seq_hash);
        }

        assert_eq!(index.instance_pressure(1), 7.0);
        assert_eq!(index.instance_pressure(2), 3.0);
        let extremes = index.pressure_extremes();
        assert_eq!(extremes.max_server, Some(1));
        assert_eq!(extremes.min_server, Some(2));
        let gap = extremes.max_pressure - extremes.min_pressure;
        assert!(gap > 3.0);
        assert!(!(gap > 4.0));
    }

    #[test]
    fn select_replication_requires_parent_on_target() {
        let index = KvRadixTree::new();
        let blocks = vec![info(0, 10, 20, 4), info(1, 11, 21, 4), info(2, 12, 22, 4)];
        index.store_blocks(1, &blocks);
        index.store_blocks(2, &blocks[..1]);
        for _ in 0..6 {
            index.increment_heat(blocks[1].seq_hash);
        }
        for _ in 0..6 {
            index.increment_heat(blocks[2].seq_hash);
        }

        let candidates = index.select_replication_candidates(1, 2);
        assert_eq!(candidates[0].seq_hash, blocks[1].seq_hash);
        assert_eq!(candidates[1].seq_hash, blocks[2].seq_hash);
        assert!(candidates.iter().all(|candidate| candidate.src_server == 1));
        assert!(candidates.iter().all(|candidate| candidate.dst_server == 2));
    }

    #[test]
    fn select_replication_returns_complete_contiguous_sequence() {
        let index = KvRadixTree::new();
        let blocks = vec![info(0, 10, 20, 4), info(1, 11, 21, 4), info(2, 12, 22, 4)];
        index.store_blocks(1, &blocks);
        index.store_blocks(2, &blocks[..1]);
        for _ in 0..8 {
            index.increment_heat(blocks[1].seq_hash);
        }
        for _ in 0..8 {
            index.increment_heat(blocks[2].seq_hash);
        }

        let candidates = index.select_replication_candidates(1, 2);
        let selected_hashes: Vec<_> = candidates
            .iter()
            .map(|candidate| candidate.seq_hash)
            .collect();
        assert_eq!(
            selected_hashes,
            vec![blocks[1].seq_hash, blocks[2].seq_hash]
        );
    }

    #[test]
    fn select_eviction_target_prefers_sequence_tail() {
        let index = KvRadixTree::new();
        let blocks = vec![info(0, 10, 20, 4), info(1, 11, 21, 4)];
        index.store_blocks(1, &blocks);
        index.increment_heat(blocks[0].seq_hash);
        index.increment_heat(blocks[1].seq_hash);

        assert_eq!(index.select_eviction_target(1), Some(blocks[1].seq_hash));
    }

    #[test]
    fn apply_eviction_subtracts_shared_heat() {
        let index = KvRadixTree::new();
        let block = info(0, 10, 20, 4);
        index.store_blocks(1, std::slice::from_ref(&block));
        index.store_blocks(2, std::slice::from_ref(&block));
        for _ in 0..6 {
            index.increment_heat(block.seq_hash);
        }

        let report = index.apply_eviction(1, block.seq_hash).unwrap();
        assert!(report.removed);
        assert_eq!(report.heat_before, 6);
        assert_eq!(report.heat_after, 3);
        assert_eq!(report.replica_count_before, 2);
        assert_eq!(report.replica_count_after, 1);
    }
}
