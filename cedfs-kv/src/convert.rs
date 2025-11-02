use std::net::{IpAddr};

use cedfs_proto::kvcache::KvBlockMeta as ProtoKvBlockMeta;
use cedfs_proto::kvcache::MetaServer as ProtoMetaServer;

use crate::types::{KvBlockMeta, MetaServer, DataServer, UpdateKvOp};

// Proto -> Internal 转换
impl From<ProtoKvBlockMeta> for KvBlockMeta {
    fn from(proto: ProtoKvBlockMeta) -> Self {
        KvBlockMeta {
            block_id: proto.block_id,
            token_hash: proto.token_hash,
            tokens: proto.tokens,
            server_id: proto.server_id,
        }
    }
}

// Internal -> Proto 转换
impl From<KvBlockMeta> for ProtoKvBlockMeta {
    fn from(internal: KvBlockMeta) -> Self {
        ProtoKvBlockMeta {
            block_id: internal.block_id,
            token_hash: internal.token_hash,
            tokens: internal.tokens,
            server_id: internal.server_id,
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
            id: proto.id,
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
            id: internal.id,
            ip: internal.ip.to_string(),
            http_port: internal.http_port as u32,
            rpc_port: internal.rpc_port as u32,
            layer: internal.layer,
            instance: internal.instance,
        }
    }
}


//UpdateKvOp转换
impl From<cedfs_proto::kvcache::UpdateKvOp> for UpdateKvOp {
    fn from(proto: cedfs_proto::kvcache::UpdateKvOp) -> Self {
        UpdateKvOp {
            block_id: proto.block_id,
            operation: proto.operation,
            server_id: proto.server_id,
        }
    }
}

impl From<UpdateKvOp> for cedfs_proto::kvcache::UpdateKvOp {
    fn from(internal: UpdateKvOp) -> Self {
        cedfs_proto::kvcache::UpdateKvOp {
            block_id: internal.block_id,
            operation: internal.operation,
            server_id: internal.server_id,
        }
    }
    
}
