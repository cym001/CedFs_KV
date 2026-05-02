use dashmap::DashMap;
use std::sync::Arc;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use tokio::time;
use serde::{Serialize, Deserialize};

/// 元数据
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KvBlockMeta {
             
    // 块哈希值
    pub token_hash: [u8; 32],        

    // token数量
    pub offset: u32,

    // 前驱块的哈希值（根块为全零）
    pub pre_token: [u8; 32],

    // 后继块的哈希值列表
    pub next_tokens: Vec<[u8; 32]>,                                              

    // 副本信息
    pub server_id: Vec<u32>,      

}


/// 引用计数
#[derive(Debug)]
pub struct RefCount {
    /// 本地增量计数 key: token_hash, value: incremental_count
    pub local_incremental_count: DashMap<[u8; 32], u64>,
    /// 本地完整引用计数 key: token_hash, value: full_count
    pub local_ref_counts: DashMap<[u8; 32], u64>,
    /// 全局引用计数 key: token_hash, value: count
    pub global_ref_counts: DashMap<[u8; 32], u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataServer {
    /// 推理实例的id
    pub id: u32,

    /// ip
    pub ip: IpAddr,

    /// http端口
    pub http_port: u16,

    /// zmq端口
    pub init_port: u16,

    /// rpc端口
    pub rpc_port: u16,

    /// 实例部署的模型名称
    pub model_name: String,

    /// 实例服务URL
    pub url: String,

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaServer {
    /// 元数据管理器id
    pub id: u32,

    /// 存储机器的ip
    pub ip: IpAddr,

    /// kvcache数据服务器的端口
    pub port: u16,

    /// 网络层级（云、边）
    pub layer: u32,

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateKvOp{

    pub token_hash: [u8; 32],

    pub operation: u32,

    pub server_id: u32, 

}

/// KV块的唯一标识键
// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
// pub struct KvBlockKey {
//     pub model_hash: i64,
//     pub token_hash: i64,
// }
/// 全局块ID生成器
// pub struct BlockIdGenerator {
//     counter: AtomicU64,
//     node_id: u32,
// }


impl Default for DataServer {
    fn default() -> Self {
        Self {
            id: 0,
            // 默认0.0.0.0
            ip: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 
            // 默认端口0
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
            // 默认0.0.0.0
            ip: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 
            // 默认端口0
            port: 0,
            // 默认云侧         
            layer: 0,     
        }
    }
}

impl MetaServer {
    /// 生成一个稳定的 u32 哈希 ID
    pub fn hash_id(&self) -> u32 {
        let mut hasher = DefaultHasher::new();
        self.ip.hash(&mut hasher);
        self.port.hash(&mut hasher);
        self.layer.hash(&mut hasher);
        (hasher.finish() & 0xFFFF_FFFF) as u32
    }
}

impl RefCount {
    pub fn new() -> Self {
        RefCount {
            local_incremental_count: DashMap::new(),
            local_ref_counts: DashMap::new(),
            global_ref_counts: DashMap::new(),
        }
    }

    /// 启动定时清空任务
    /// 
    /// # 参数
    /// - `interval`: 清空间隔时间
    /// 
    /// # 示例
    /// ```
    /// let ref_count = Arc::new(RefCount::new());
    /// RefCount::start_periodic_clear(ref_count.clone(), Duration::from_secs(60));
    /// ```
    pub fn start_periodic_clear(ref_count: Arc<RefCount>, interval: Duration) {
        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval);
            loop {
                interval_timer.tick().await;
                ref_count.clear_and_consolidate_incremental_counts();
            }
        });
    }

    /// 清空增量计数并合并到完整计数
    /// 
    /// 该方法会遍历所有增量计数，将其合并到对应的完整计数中，然后清空增量计数
    pub fn clear_and_consolidate_incremental_counts(&self) {
        // 遍历所有增量计数
        for entry in self.local_incremental_count.iter() {
            let token_hash = *entry.key();
            let incremental = *entry.value();
            
            if incremental > 0 {
                // 更新本地完整计数
                self.local_ref_counts
                    .entry(token_hash)
                    .and_modify(|c| *c += incremental)
                    .or_insert(incremental);
            }
        }
        
        // 清空增量计数
        self.local_incremental_count.clear();
    }

    /// 增加本地增量计数
    /// 
    /// # 参数
    /// - `token_hash`: 块 ID
    /// - `increment`: 增量值
    pub fn increment_local_incremental_count(&self, token_hash: [u8; 32], increment: u64) {
        self.local_incremental_count
            .entry(token_hash)
            .and_modify(|c| *c += increment)
            .or_insert(increment);
    }

    /// 批量增加本地增量计数
    /// 
    /// # 参数
    /// - `token_hashes`: 块 ID 列表
    /// - `increment`: 每个块的增量值
    pub fn batch_increment_local_incremental_count(&self, token_hashes: &[[u8; 32]], increment: u64) {
        for &token_hash in token_hashes {
            self.local_incremental_count
                .entry(token_hash)
                .and_modify(|c| *c += increment)
                .or_insert(increment);
            self.global_ref_counts
                .entry(token_hash)
                .and_modify(|c| *c += increment)
                .or_insert(increment);
        }
    }

    /// 插入或更新本地完整引用计数
    /// 
    /// # 参数
    /// - `token_hash`: 块 ID
    /// - `count`: 计数值
    /// 
    /// # 返回
    /// - `Some(old_count)`: 更新已存在的块，返回旧值
    /// - `None`: 插入新块
    pub fn insert_or_update_local_ref_count(&self, token_hash: [u8; 32], count: u64) -> Option<u64> {
        self.local_ref_counts.insert(token_hash, count)
    }

    /// 增加本地完整引用计数
    /// 
    /// # 参数
    /// - `token_hash`: 块 ID
    /// - `increment`: 增量值
    pub fn increment_local_ref_count(&self, token_hash: [u8; 32], increment: u64) -> u64 {
        self.local_ref_counts
            .entry(token_hash)
            .and_modify(|c| *c += increment)
            .or_insert(increment)
            .clone()
    }



    /// 合并指定块的增量计数到完整计数
    /// 
    /// # 参数
    /// - `token_hash`: 块 ID
    /// 
    /// # 返回
    /// - `Some(total_count)`: 合并后的总计数
    /// - `None`: 该块不存在增量计数
    pub fn consolidate_local_ref_count(&self, token_hash: [u8; 32]) -> Option<u64> {
        if let Some((_, incremental)) = self.local_incremental_count.remove(&token_hash) {
            if incremental > 0 {
                let total = self.local_ref_counts
                    .entry(token_hash)
                    .and_modify(|c| *c += incremental)
                    .or_insert(incremental)
                    .clone();
                return Some(total);
            }
        }
        self.local_ref_counts.get(&token_hash).map(|v| *v)
    }

    /// 获取本地块的总引用计数(完整计数 + 增量计数)
    /// 
    /// # 参数
    /// - `token_hash`: 块 ID
    /// 
    /// # 返回
    /// - `Some(total)`: 总计数
    /// - `None`: 该块不存在任何计数
    pub fn get_local_total_count(&self, token_hash: [u8; 32]) -> Option<u64> {
        let full_count = self.local_ref_counts.get(&token_hash).map(|v| *v).unwrap_or(0);
        let incremental_count = self.local_incremental_count.get(&token_hash).map(|v| *v).unwrap_or(0);
        
        if full_count == 0 && incremental_count == 0 {
            None
        } else {
            Some(full_count + incremental_count)
        }
    }

    /// 插入或更新全局引用计数
    /// 
    /// # 参数
    /// - `token_hash`: 块 ID
    /// - `count`: 新的计数值
    /// 
    /// # 返回
    /// - `Some(old_count)`: 更新已存在的块，返回旧值
    /// - `None`: 插入新块
    pub fn insert_or_update_global_ref_count(&self, token_hash: [u8; 32], count: u64) -> Option<u64> {
        self.global_ref_counts.insert(token_hash, count)
    }

    /// 增加全局引用计数
    /// 
    /// # 参数
    /// - `token_hash`: 块 ID
    /// - `increment`: 增量值
    pub fn increment_global_ref_count(&self, token_hash: [u8; 32], increment: u64) -> u64 {
        // 判断id是否在本地存在，存在则增加本地计数
        if self.local_ref_counts.contains_key(&token_hash) {
            self.increment_local_ref_count(token_hash, increment)
        }else{
            self.global_ref_counts
            .entry(token_hash)
            .and_modify(|c| *c += increment)
            .or_insert(increment)
            .clone()
        }
        
    }

    /// 减少全局引用计数
    /// 
    /// # 参数
    /// - `token_hash`: 块 ID
    /// - `decrement`: 减量值
    pub fn decrement_global_ref_count(&self, token_hash: [u8; 32], decrement: u64) -> Option<u64> {
        self.global_ref_counts.get_mut(&token_hash).map(|mut entry| {
            *entry = entry.saturating_sub(decrement);
            *entry
        })
    }

    /// 删除本地引用计数（包括增量和完整计数）
    /// 
    /// # 参数
    /// - `token_hash`: 块 ID
    /// 
    /// # 返回
    /// - `(full_count, incremental_count)`: 删除的计数值
    pub fn remove_local_ref_count(&self, token_hash: [u8; 32]) -> (Option<u64>, Option<u64>) {
        let full = self.local_ref_counts.remove(&token_hash).map(|(_, v)| v);
        let incremental = self.local_incremental_count.remove(&token_hash).map(|(_, v)| v);
        (full, incremental)
    }

    /// 删除全局引用计数
    /// 
    /// # 参数
    /// - `token_hash`: 块 ID
    pub fn remove_global_ref_count(&self, token_hash: [u8; 32]) -> Option<u64> {
        self.global_ref_counts.remove(&token_hash).map(|(_, v)| v)
    }

    /// 批量更新全局引用计数
    /// 
    /// # 参数
    /// - `updates`: (token_hash, count) 元组的向量
    pub fn batch_update_global_ref_counts(&self, updates: Vec<([u8; 32], u64)>) {
        for (token_hash, count) in updates {
            self.insert_or_update_global_ref_count(token_hash, count);
        }
    }

    /// 清除本地引用计数（包括增量和完整计数）
    pub fn clear_local_ref_counts(&self) {
        self.local_ref_counts.clear();
        self.local_incremental_count.clear();
    }

    /// 获取所有本地块的总引用计数(完整计数 + 增量计数)
    /// 
    /// # 返回
    /// - `Vec<(token_hash, total_count)>`: 所有块的 ID 和总计数
    pub fn get_all_local_total_counts(&self) -> Vec<([u8; 32], u64)> {
        use std::collections::HashMap;
        
        let mut counts: HashMap<[u8; 32], u64> = HashMap::new();
        
        // 收集所有完整计数
        for entry in self.local_ref_counts.iter() {
            counts.insert(*entry.key(), *entry.value());
        }
        
        // 累加增量计数
        for entry in self.local_incremental_count.iter() {
            counts.entry(*entry.key())
                .and_modify(|c| *c += *entry.value())
                .or_insert(*entry.value());
        }
        
        // 转换为 Vec 并返回
        counts.into_iter().collect()
    }
}


// impl KvBlockKey {
//     pub fn new(model_hash: i64, token_hash: i64) -> Self {
//         Self {
//             model_hash,
//             token_hash,
//         }
//     }
// }

// impl BlockIdGenerator {
//     pub fn new(node_id: u32) -> Self {
//         Self {
//             counter: AtomicU64::new(0),
//             node_id,
//         }
//     }

//     /// 生成新的token_hash（前32位node_id，后32位自增ID）
//     pub fn next_id(&self) -> u64 {
//         let local_id = self.counter.fetch_add(1, Ordering::SeqCst);
//         ((self.node_id as u64) << 32) | (local_id & 0xFFFFFFFF)
//     }

//     /// 从token_hash提取node_id
//     pub fn extract_node_id(token_hash: u64) -> u32 {
//         (token_hash >> 32) as u32
//     }

//     /// 从token_hash提取本地ID
//     pub fn extract_local_id(token_hash: u64) -> u32 {
//         (token_hash & 0xFFFFFFFF) as u32
//     }
// }

impl KvBlockMeta {
    /// 检查tokens是否完全匹配
    pub fn tokens_match(&self, token_hash: [u8; 32]) -> bool {
        self.token_hash == token_hash
    }

    /// 添加副本服务器
    pub fn add_replica(&mut self, server_id: u32) {
        if !self.server_id.contains(&server_id) {
            self.server_id.push(server_id);
        }
    }

    /// 移除副本服务器
    pub fn remove_replica(&mut self, server_id: u32) {
        self.server_id.retain(|&id| id != server_id);
    }
}