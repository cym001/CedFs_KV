use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr};

pub type BlockHash = [u8; 32];
pub type ServerId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockHashInfo {
    pub position: usize,
    pub local_hash: BlockHash,
    pub seq_hash: BlockHash,
    pub offset: u32,
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
