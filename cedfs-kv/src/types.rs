use dashmap::DashMap;
use std::sync::Arc;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use tokio::time;
use serde::{Serialize, Deserialize};

use cedfs_proto::kvcache::KvBlockMeta as ProtoKvBlockMeta;
use cedfs_proto::kvcache::ServerSocket as ProtoServerSocket;
use cedfs_proto::kvcache::MetaServer as ProtoMetaServer;

/// 元数据
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KvBlockMeta {
    // 块 ID(前32位node_id， 后32位node内块id)
    pub block_id: u64,              

    // 块哈希值
    pub block_hash: u64,            

    // 模型哈希值: 
    pub model_hash: u64,      

    // token ids
    pub tokens: Vec<i32>,                                

    // 物理大小
    pub phy_size: usize,                

    // 副本信息
    pub server_socket: Vec<ServerSocket>,      

}


/// 引用计数
#[derive(Debug)]
pub struct RefCount {
    /// 本地增量计数 key: block_id, value: incremental_count
    pub local_incremental_count: DashMap<u64, u64>,
    /// 本地完整引用计数 key: block_id, value: full_count
    pub local_ref_counts: DashMap<u64, u64>,
    /// 全局引用计数 key: block_id, value: count
    pub global_ref_counts: DashMap<u64, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataServer {
    /// 存储机器的ip
    pub ip: IpAddr,

    /// http端口
    pub http_port: u16,

    /// rpc端口
    pub rpc_port: u16,

    /// 存储机器的网络层级（云、边）  0为云侧， 1为边侧
    pub layer: u32,

    ///实例类型
    pub instance: Vec<String>,

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaServer {
    /// 存储机器的ip
    pub ip: IpAddr,

    /// kvcache数据服务器的端口
    pub port: u16,

    /// 网络层级（云、边）
    pub layer: u32,

}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerSocket {
    pub ip: IpAddr,
    pub http_port: u16,
    pub rpc_port: u16,
}

impl Default for DataServer {
    fn default() -> Self {
        Self {
            // 默认0.0.0.0
            ip: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 
            // 默认端口0
            http_port: 0,
            rpc_port: 0,
            // 默认云侧         
            layer: 0,       
            instance: Vec::new(),
        }
    }
}

impl Default for MetaServer {
    fn default() -> Self {
        Self {
            // 默认0.0.0.0
            ip: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 
            // 默认端口0
            port: 0,
            // 默认云侧         
            layer: 0,     
        }
    }
}

impl Default for ServerSocket {
    fn default() -> Self {
        Self {
            // 默认0.0.0.0
            ip: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 
            // 默认端口0
            http_port: 0,
            rpc_port: 0, 
        }
    }
}

// Proto -> Internal 转换
impl From<ProtoKvBlockMeta> for KvBlockMeta {
    fn from(proto: ProtoKvBlockMeta) -> Self {
        KvBlockMeta {
            block_id: proto.block_id,
            block_hash: proto.block_hash,
            model_hash: proto.model_hash,
            tokens: proto.tokens,
            phy_size: proto.phy_size as usize,
            server_socket: proto.server_socket.into_iter().map(|s| s.into()).collect(),
        }
    }
}

// Internal -> Proto 转换
impl From<KvBlockMeta> for ProtoKvBlockMeta {
    fn from(internal: KvBlockMeta) -> Self {
        ProtoKvBlockMeta {
            block_id: internal.block_id,
            block_hash: internal.block_hash,
            model_hash: internal.model_hash,
            tokens: internal.tokens,
            phy_size: internal.phy_size as u64,
            server_socket: internal.server_socket.into_iter().map(|s| s.into()).collect(),
        }
    }
}

// ServerSocket 转换
impl From<ProtoServerSocket> for ServerSocket {
    fn from(proto: ProtoServerSocket) -> Self {
        ServerSocket {
            ip: proto.ip.parse().unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            http_port: proto.http_port as u16,
            rpc_port: proto.rpc_port as u16,
        }
    }
}

impl From<ServerSocket> for ProtoServerSocket {
    fn from(internal: ServerSocket) -> Self {
        ProtoServerSocket {
            ip: internal.ip.to_string(),
            http_port: internal.http_port as u32,
            rpc_port: internal.rpc_port as u32,
        }
    }
}

// MetaServer 转换
impl From<ProtoMetaServer> for MetaServer {
    fn from(proto: ProtoMetaServer) -> Self {
        MetaServer {
            ip: proto.ip.parse().unwrap_or(IpAddr::V4(std::net::    Ipv4Addr::LOCALHOST)),
            port: proto.port as u16,
            layer: proto.layer,
        }
    }
}

impl From<MetaServer> for ProtoMetaServer {
    fn from(internal: MetaServer) -> Self {
        ProtoMetaServer {
            ip: internal.ip.to_string(),
            port: internal.port as u32,
            layer: internal.layer,
        }
    }
}

// DataServer 转换
impl From<cedfs_proto::kvcache::DataServer> for DataServer {
    fn from(proto: cedfs_proto::kvcache::DataServer) -> Self {
        DataServer {
            ip: proto.ip.parse().unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            http_port: proto.http_port as u16,  
            rpc_port: proto.rpc_port as u16,
            layer: proto.layer,
            instance: proto.instance,
        }
    }
}   
impl From<DataServer> for cedfs_proto::kvcache::DataServer {
    fn from(internal: DataServer) -> Self {
        cedfs_proto::kvcache::DataServer {
            ip: internal.ip.to_string(),
            http_port: internal.http_port as u32,
            rpc_port: internal.rpc_port as u32,
            layer: internal.layer,
            instance: internal.instance,
        }
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
            let block_id = *entry.key();
            let incremental = *entry.value();
            
            if incremental > 0 {
                // 更新本地完整计数
                self.local_ref_counts
                    .entry(block_id)
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
    /// - `block_id`: 块 ID
    /// - `increment`: 增量值
    pub fn increment_local_incremental_count(&self, block_id: u64, increment: u64) {
        self.local_incremental_count
            .entry(block_id)
            .and_modify(|c| *c += increment)
            .or_insert(increment);
    }

    /// 插入或更新本地完整引用计数
    /// 
    /// # 参数
    /// - `block_id`: 块 ID
    /// - `count`: 计数值
    /// 
    /// # 返回
    /// - `Some(old_count)`: 更新已存在的块，返回旧值
    /// - `None`: 插入新块
    pub fn insert_or_update_local_ref_count(&self, block_id: u64, count: u64) -> Option<u64> {
        self.local_ref_counts.insert(block_id, count)
    }

    /// 增加本地完整引用计数
    /// 
    /// # 参数
    /// - `block_id`: 块 ID
    /// - `increment`: 增量值
    pub fn increment_local_ref_count(&self, block_id: u64, increment: u64) -> u64 {
        self.local_ref_counts
            .entry(block_id)
            .and_modify(|c| *c += increment)
            .or_insert(increment)
            .clone()
    }

    /// 合并指定块的增量计数到完整计数
    /// 
    /// # 参数
    /// - `block_id`: 块 ID
    /// 
    /// # 返回
    /// - `Some(total_count)`: 合并后的总计数
    /// - `None`: 该块不存在增量计数
    pub fn consolidate_local_ref_count(&self, block_id: u64) -> Option<u64> {
        if let Some((_, incremental)) = self.local_incremental_count.remove(&block_id) {
            if incremental > 0 {
                let total = self.local_ref_counts
                    .entry(block_id)
                    .and_modify(|c| *c += incremental)
                    .or_insert(incremental)
                    .clone();
                return Some(total);
            }
        }
        self.local_ref_counts.get(&block_id).map(|v| *v)
    }

    /// 获取本地块的总引用计数(完整计数 + 增量计数)
    /// 
    /// # 参数
    /// - `block_id`: 块 ID
    /// 
    /// # 返回
    /// - `Some(total)`: 总计数
    /// - `None`: 该块不存在任何计数
    pub fn get_local_total_count(&self, block_id: u64) -> Option<u64> {
        let full_count = self.local_ref_counts.get(&block_id).map(|v| *v).unwrap_or(0);
        let incremental_count = self.local_incremental_count.get(&block_id).map(|v| *v).unwrap_or(0);
        
        if full_count == 0 && incremental_count == 0 {
            None
        } else {
            Some(full_count + incremental_count)
        }
    }

    /// 插入或更新全局引用计数
    /// 
    /// # 参数
    /// - `block_id`: 块 ID
    /// - `count`: 新的计数值
    /// 
    /// # 返回
    /// - `Some(old_count)`: 更新已存在的块，返回旧值
    /// - `None`: 插入新块
    pub fn insert_or_update_global_ref_count(&self, block_id: u64, count: u64) -> Option<u64> {
        self.global_ref_counts.insert(block_id, count)
    }

    /// 增加全局引用计数
    /// 
    /// # 参数
    /// - `block_id`: 块 ID
    /// - `increment`: 增量值
    pub fn increment_global_ref_count(&self, block_id: u64, increment: u64) -> u64 {
        self.global_ref_counts
            .entry(block_id)
            .and_modify(|c| *c += increment)
            .or_insert(increment)
            .clone()
    }

    /// 减少全局引用计数
    /// 
    /// # 参数
    /// - `block_id`: 块 ID
    /// - `decrement`: 减量值
    pub fn decrement_global_ref_count(&self, block_id: u64, decrement: u64) -> Option<u64> {
        self.global_ref_counts.get_mut(&block_id).map(|mut entry| {
            *entry = entry.saturating_sub(decrement);
            *entry
        })
    }

    /// 删除本地引用计数（包括增量和完整计数）
    /// 
    /// # 参数
    /// - `block_id`: 块 ID
    /// 
    /// # 返回
    /// - `(full_count, incremental_count)`: 删除的计数值
    pub fn remove_local_ref_count(&self, block_id: u64) -> (Option<u64>, Option<u64>) {
        let full = self.local_ref_counts.remove(&block_id).map(|(_, v)| v);
        let incremental = self.local_incremental_count.remove(&block_id).map(|(_, v)| v);
        (full, incremental)
    }

    /// 删除全局引用计数
    /// 
    /// # 参数
    /// - `block_id`: 块 ID
    pub fn remove_global_ref_count(&self, block_id: u64) -> Option<u64> {
        self.global_ref_counts.remove(&block_id).map(|(_, v)| v)
    }

    /// 批量更新全局引用计数
    /// 
    /// # 参数
    /// - `updates`: (block_id, count) 元组的向量
    pub fn batch_update_global_ref_counts(&self, updates: Vec<(u64, u64)>) {
        for (block_id, count) in updates {
            self.insert_or_update_global_ref_count(block_id, count);
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
    /// - `Vec<(block_id, total_count)>`: 所有块的 ID 和总计数
    pub fn get_all_local_total_counts(&self) -> Vec<(u64, u64)> {
        use std::collections::HashMap;
        
        let mut counts: HashMap<u64, u64> = HashMap::new();
        
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