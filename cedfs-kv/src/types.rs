use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::RwLock;

pub type BlockHash = [u8; 32];
pub type ServerId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockHashInfo {
    pub position: usize,
    pub local_hash: BlockHash,
    pub seq_hash: BlockHash,
    pub offset: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct KvBlockMeta {
    pub seq_hash: BlockHash,
    pub local_hash: BlockHash,
    pub position: usize,
    pub offset: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockLocation {
    pub position: usize,
    pub local_hash: BlockHash,
}

#[derive(Debug, Clone)]
pub struct KvBlockState {
    pub meta: KvBlockMeta,
    pub servers: HashSet<ServerId>,
    pub ref_count: u64,
}

#[derive(Debug, Clone)]
pub struct KvBlockSnapshot {
    pub meta: KvBlockMeta,
    pub servers: Vec<ServerId>,
    pub ref_count: u64,
}

#[derive(Debug, Clone)]
pub struct StoreBlockResult {
    pub seq_hash: BlockHash,
    pub replica_count: u32,
    pub server_added: bool,
}

#[derive(Debug, Clone)]
enum SeqEntry {
    Single(BlockHash, KvBlockState),
    Multi(HashMap<BlockHash, KvBlockState>),
}

#[derive(Debug, Clone, Copy, Default)]
struct RemoveState {
    seq_removed: bool,
    entry_empty: bool,
}

impl SeqEntry {
    fn new(info: BlockHashInfo, server_id: ServerId) -> Self {
        let mut servers = HashSet::new();
        servers.insert(server_id);
        Self::Single(
            info.seq_hash,
            KvBlockState {
                meta: KvBlockMeta::from(info),
                servers,
                ref_count: 0,
            },
        )
    }

    fn insert(&mut self, info: BlockHashInfo, server_id: ServerId) -> StoreBlockResult {
        match self {
            Self::Single(existing_hash, state) if *existing_hash == info.seq_hash => {
                let server_added = state.servers.insert(server_id);
                StoreBlockResult {
                    seq_hash: info.seq_hash,
                    replica_count: state.servers.len() as u32,
                    server_added,
                }
            },
            Self::Single(existing_hash, existing_state) => {
                let mut map = HashMap::with_capacity(2);
                map.insert(*existing_hash, existing_state.clone());
                let mut servers = HashSet::new();
                servers.insert(server_id);
                map.insert(
                    info.seq_hash,
                    KvBlockState {
                        meta: KvBlockMeta::from(info),
                        servers,
                        ref_count: 0,
                    },
                );
                *self = Self::Multi(map);
                StoreBlockResult {
                    seq_hash: info.seq_hash,
                    replica_count: 1,
                    server_added: true,
                }
            },
            Self::Multi(map) => {
                let state = map.entry(info.seq_hash).or_insert_with(|| {
                    let mut servers = HashSet::new();
                    servers.insert(server_id);
                    KvBlockState {
                        meta: KvBlockMeta::from(info),
                        servers,
                        ref_count: 0,
                    }
                });
                let server_added = state.servers.insert(server_id);
                StoreBlockResult {
                    seq_hash: info.seq_hash,
                    replica_count: state.servers.len() as u32,
                    server_added,
                }
            },
        }
    }

    fn get(&self, seq_hash: BlockHash) -> Option<&KvBlockState> {
        match self {
            Self::Single(existing_hash, state) if *existing_hash == seq_hash => Some(state),
            Self::Single(_, _) => None,
            Self::Multi(map) => map.get(&seq_hash),
        }
    }

    fn get_mut(&mut self, seq_hash: BlockHash) -> Option<&mut KvBlockState> {
        match self {
            Self::Single(existing_hash, state) if *existing_hash == seq_hash => Some(state),
            Self::Single(_, _) => None,
            Self::Multi(map) => map.get_mut(&seq_hash),
        }
    }

    fn remove_server(&mut self, seq_hash: BlockHash, server_id: ServerId) -> RemoveState {
        match self {
            Self::Single(existing_hash, state) if *existing_hash == seq_hash => {
                state.servers.remove(&server_id);
                let empty = state.servers.is_empty();
                RemoveState {
                    seq_removed: empty,
                    entry_empty: empty,
                }
            },
            Self::Single(_, _) => RemoveState::default(),
            Self::Multi(map) => {
                let mut seq_removed = false;
                if let Some(state) = map.get_mut(&seq_hash) {
                    state.servers.remove(&server_id);
                    seq_removed = state.servers.is_empty();
                }
                if seq_removed {
                    map.remove(&seq_hash);
                }
                RemoveState {
                    seq_removed,
                    entry_empty: map.is_empty(),
                }
            },
        }
    }
}

#[derive(Debug)]
pub struct KvMetaIndex {
    index: DashMap<(usize, BlockHash), SeqEntry>,
    server_blocks: DashMap<ServerId, RwLock<HashMap<BlockHash, BlockLocation>>>,
    block_locations: DashMap<BlockHash, BlockLocation>,
}

impl KvMetaIndex {
    pub fn new() -> Self {
        Self {
            index: DashMap::new(),
            server_blocks: DashMap::new(),
            block_locations: DashMap::new(),
        }
    }

    pub fn store_blocks(
        &self,
        server_id: ServerId,
        blocks: &[BlockHashInfo],
    ) -> Vec<StoreBlockResult> {
        let matched_len = self.match_len_for_server(server_id, blocks);
        let mut results = Vec::with_capacity(blocks.len().saturating_sub(matched_len));

        self.server_blocks
            .entry(server_id)
            .or_insert_with(|| RwLock::new(HashMap::new()));

        for info in blocks.iter().copied().skip(matched_len) {
            let key = (info.position, info.local_hash);
            let entry = self
                .index
                .entry(key)
                .and_modify(|entry| {
                    results.push(entry.insert(info, server_id));
                })
                .or_insert_with(|| {
                    results.push(StoreBlockResult {
                        seq_hash: info.seq_hash,
                        replica_count: 1,
                        server_added: true,
                    });
                    SeqEntry::new(info, server_id)
                });
            drop(entry);

            let location = BlockLocation {
                position: info.position,
                local_hash: info.local_hash,
            };
            self.block_locations.insert(info.seq_hash, location);

            if let Some(server_map) = self.server_blocks.get(&server_id) {
                server_map
                    .write()
                    .expect("server block map poisoned")
                    .insert(info.seq_hash, location);
            }
        }

        results
    }

    pub fn add_server(&self, seq_hash: BlockHash, server_id: ServerId) -> bool {
        let Some(location) = self.location(seq_hash) else {
            return false;
        };
        let key = (location.position, location.local_hash);
        let Some(mut entry) = self.index.get_mut(&key) else {
            return false;
        };
        let Some(state) = entry.get_mut(seq_hash) else {
            return false;
        };

        let server_added = state.servers.insert(server_id);
        drop(entry);

        if server_added {
            self.server_blocks
                .entry(server_id)
                .or_insert_with(|| RwLock::new(HashMap::new()));
            if let Some(server_map) = self.server_blocks.get(&server_id) {
                server_map
                    .write()
                    .expect("server block map poisoned")
                    .insert(seq_hash, location);
            }
        }

        server_added
    }

    pub fn remove_server(&self, seq_hash: BlockHash, server_id: ServerId) -> bool {
        let Some(location) = self.location(seq_hash) else {
            return false;
        };

        if let Some(server_map) = self.server_blocks.get(&server_id) {
            server_map
                .write()
                .expect("server block map poisoned")
                .remove(&seq_hash);
        }

        let key = (location.position, location.local_hash);
        let Some(mut entry) = self.index.get_mut(&key) else {
            return false;
        };
        let removed = entry.remove_server(seq_hash, server_id);
        drop(entry);

        if removed.seq_removed {
            self.block_locations.remove(&seq_hash);
        }
        if removed.entry_empty {
            self.index.remove(&key);
        }

        removed.seq_removed || self.location(seq_hash).is_some()
    }

    pub fn remove_block(&self, seq_hash: BlockHash) -> Option<KvBlockSnapshot> {
        let snapshot = self.get_block(seq_hash)?;
        for server_id in &snapshot.servers {
            if let Some(server_map) = self.server_blocks.get(server_id) {
                server_map
                    .write()
                    .expect("server block map poisoned")
                    .remove(&seq_hash);
            }
        }

        let location = self.location(seq_hash)?;
        let key = (location.position, location.local_hash);
        if let Some(mut entry) = self.index.get_mut(&key) {
            match &mut *entry {
                SeqEntry::Single(existing_hash, _) if *existing_hash == seq_hash => {
                    drop(entry);
                    self.index.remove(&key);
                },
                SeqEntry::Multi(map) => {
                    map.remove(&seq_hash);
                    let empty = map.is_empty();
                    drop(entry);
                    if empty {
                        self.index.remove(&key);
                    }
                },
                _ => {},
            }
        }
        self.block_locations.remove(&seq_hash);
        Some(snapshot)
    }

    pub fn increment_ref_count(&self, seq_hash: BlockHash) -> Option<u64> {
        let location = self.location(seq_hash)?;
        let key = (location.position, location.local_hash);
        let mut entry = self.index.get_mut(&key)?;
        let state = entry.get_mut(seq_hash)?;
        state.ref_count = state.ref_count.saturating_add(1);
        Some(state.ref_count)
    }

    pub fn get_block(&self, seq_hash: BlockHash) -> Option<KvBlockSnapshot> {
        let location = self.location(seq_hash)?;
        self.get_block_at(seq_hash, location)
    }

    pub fn contains_server(&self, seq_hash: BlockHash, server_id: ServerId) -> bool {
        self.get_block(seq_hash)
            .map(|snapshot| snapshot.servers.contains(&server_id))
            .unwrap_or(false)
    }

    pub fn replica_count(&self, seq_hash: BlockHash) -> u32 {
        self.get_block(seq_hash)
            .map(|snapshot| snapshot.servers.len() as u32)
            .unwrap_or(0)
    }

    pub fn find_matches(&self, blocks: &[BlockHashInfo]) -> Vec<(ServerId, u32)> {
        if blocks.is_empty() {
            return Vec::new();
        }

        let mut active_servers: Option<HashSet<ServerId>> = None;
        let mut matched_offsets: HashMap<ServerId, u32> = HashMap::new();

        for info in blocks {
            let Some(state) = self.get_state_for_info(*info) else {
                break;
            };
            let state_servers: HashSet<ServerId> = state.servers.into_iter().collect();

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
        }

        matched_offsets.into_iter().collect()
    }

    pub fn match_len_for_server(&self, server_id: ServerId, blocks: &[BlockHashInfo]) -> usize {
        let Some(server_map) = self.server_blocks.get(&server_id) else {
            return 0;
        };
        let server_map = server_map.read().expect("server block map poisoned");
        let mut matched = 0;
        for info in blocks {
            let Some(location) = server_map.get(&info.seq_hash) else {
                break;
            };
            if location.position != info.position || location.local_hash != info.local_hash {
                break;
            }
            matched += 1;
        }
        matched
    }

    fn get_state_for_info(&self, info: BlockHashInfo) -> Option<KvBlockSnapshot> {
        let entry = self.index.get(&(info.position, info.local_hash))?;
        let state = entry.get(info.seq_hash)?;
        Some(KvBlockSnapshot {
            meta: state.meta,
            servers: state.servers.iter().copied().collect(),
            ref_count: state.ref_count,
        })
    }

    fn get_block_at(
        &self,
        seq_hash: BlockHash,
        location: BlockLocation,
    ) -> Option<KvBlockSnapshot> {
        let entry = self.index.get(&(location.position, location.local_hash))?;
        let state = entry.get(seq_hash)?;
        Some(KvBlockSnapshot {
            meta: state.meta,
            servers: state.servers.iter().copied().collect(),
            ref_count: state.ref_count,
        })
    }

    fn location(&self, seq_hash: BlockHash) -> Option<BlockLocation> {
        self.block_locations
            .get(&seq_hash)
            .map(|location| *location)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataServer {
    pub id: u32,
    pub ip: IpAddr,
    pub http_port: u16,
    pub init_port: u16,
    pub rpc_port: u16,
    pub model_name: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaServer {
    pub id: u32,
    pub ip: IpAddr,
    pub port: u16,
    pub layer: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateKvOp {
    pub token_hash: BlockHash,
    pub operation: u32,
    pub server_id: u32,
}

impl From<BlockHashInfo> for KvBlockMeta {
    fn from(info: BlockHashInfo) -> Self {
        Self {
            seq_hash: info.seq_hash,
            local_hash: info.local_hash,
            position: info.position,
            offset: info.offset,
        }
    }
}

impl Default for DataServer {
    fn default() -> Self {
        Self {
            id: 0,
            ip: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            http_port: 0,
            init_port: 0,
            rpc_port: 0,
            model_name: "default_model_name".to_string(),
            url: "default_url".to_string(),
        }
    }
}

impl Default for MetaServer {
    fn default() -> Self {
        Self {
            id: 0,
            ip: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            port: 0,
            layer: 0,
        }
    }
}

impl MetaServer {
    pub fn hash_id(&self) -> u32 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.ip.hash(&mut hasher);
        self.port.hash(&mut hasher);
        self.layer.hash(&mut hasher);
        (hasher.finish() & 0xFFFF_FFFF) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn hash(byte: u8) -> BlockHash {
        let mut value = [0u8; 32];
        value[0] = byte;
        value
    }

    fn info(position: usize, local: u8, seq: u8, offset: u32) -> BlockHashInfo {
        BlockHashInfo {
            position,
            local_hash: hash(local),
            seq_hash: hash(seq),
            offset,
        }
    }

    #[test]
    fn store_and_find_prefix_matches() {
        let index = KvMetaIndex::new();
        let blocks = vec![info(0, 10, 20, 4), info(1, 11, 21, 4)];

        let stored = index.store_blocks(7, &blocks);
        assert_eq!(stored.len(), 2);
        assert_eq!(index.match_len_for_server(7, &blocks), 2);

        let matches = index.find_matches(&blocks);
        assert_eq!(matches, vec![(7, 8)]);
    }

    #[test]
    fn supports_multiple_seq_hashes_at_same_position_and_local_hash() {
        let index = KvMetaIndex::new();
        let first = info(0, 10, 20, 4);
        let second = info(0, 10, 21, 4);

        index.store_blocks(1, &[first]);
        index.store_blocks(2, &[second]);

        assert_eq!(index.find_matches(&[first]), vec![(1, 4)]);
        assert_eq!(index.find_matches(&[second]), vec![(2, 4)]);
    }

    #[test]
    fn remove_server_cleans_empty_blocks() {
        let index = KvMetaIndex::new();
        let block = info(0, 10, 20, 4);

        index.store_blocks(1, &[block]);
        index.store_blocks(2, &[block]);
        assert_eq!(index.replica_count(block.seq_hash), 2);

        assert!(index.remove_server(block.seq_hash, 1));
        assert!(!index.contains_server(block.seq_hash, 1));
        assert!(index.contains_server(block.seq_hash, 2));
        assert_eq!(index.replica_count(block.seq_hash), 1);

        assert!(index.remove_server(block.seq_hash, 2));
        assert!(index.get_block(block.seq_hash).is_none());
        assert_eq!(index.replica_count(block.seq_hash), 0);
    }

    #[test]
    fn concurrent_store_and_ref_count_updates() {
        let index = Arc::new(KvMetaIndex::new());
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
                index.increment_ref_count(seq_hash);
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        let snapshot = index.get_block(blocks[0].seq_hash).unwrap();
        assert_eq!(snapshot.ref_count, 16);
    }
}
