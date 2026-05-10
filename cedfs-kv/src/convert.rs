use std::net::IpAddr;

// use cedfs_proto::kvcache::KvBlockMeta as ProtoKvBlockMeta;
// use cedfs_proto::kvcache::MetaServer as ProtoMetaServer;

use crate::types::DataServer;

// Proto -> Internal 转换
// impl From<ProtoKvBlockMeta> for KvBlockMeta {
//     fn from(proto: ProtoKvBlockMeta) -> Self {
//         KvBlockMeta {
//             token_hash: bytes2hash(proto.token_hash),
//             offset: proto.offset,
//             pre_token: bytes2hash(proto.pre_token),
//             next_tokens: vecbytes2vechash(proto.next_tokens),
//             server_id: proto.server_id,
//         }
//     }
// }

// // Internal -> Proto 转换
// impl From<KvBlockMeta> for ProtoKvBlockMeta {
//     fn from(internal: KvBlockMeta) -> Self {
//         ProtoKvBlockMeta {
//             token_hash: hash2bytes(internal.token_hash),
//             offset: internal.offset,
//             pre_token: hash2bytes(internal.pre_token),
//             next_tokens: vechash2vecbytes(internal.next_tokens),
//             server_id: internal.server_id,
//         }
//     }
// }

// MetaServer 转换
// impl From<ProtoMetaServer> for MetaServer {
//     fn from(proto: ProtoMetaServer) -> Self {
//         MetaServer {
//             id: proto.id,
//             ip: proto.ip.parse().unwrap_or(IpAddr::V4(std::net::    Ipv4Addr::LOCALHOST)),
//             port: proto.port as u16,
//             layer: proto.layer,
//         }
//     }
// }

// impl From<MetaServer> for ProtoMetaServer {
//     fn from(internal: MetaServer) -> Self {
//         ProtoMetaServer {
//             id: internal.id,
//             ip: internal.ip.to_string(),
//             port: internal.port as u32,
//             layer: internal.layer,
//         }
//     }
// }

// DataServer 转换
impl From<cedfs_proto::kvcache::DataServer> for DataServer {
    fn from(proto: cedfs_proto::kvcache::DataServer) -> Self {
        DataServer {
            id: proto.id,
            ip: proto
                .ip
                .parse()
                .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            http_port: proto.http_port as u16,
            init_port: proto.init_port as u16,
            rpc_port: proto.rpc_port as u16,
            model_name: proto.model_name,
            url: proto.url,
        }
    }
}
impl From<DataServer> for cedfs_proto::kvcache::DataServer {
    fn from(internal: DataServer) -> Self {
        cedfs_proto::kvcache::DataServer {
            id: internal.id,
            ip: internal.ip.to_string(),
            http_port: internal.http_port as u32,
            init_port: internal.init_port as u32,
            rpc_port: internal.rpc_port as u32,
            model_name: internal.model_name,
            url: internal.url,
        }
    }
}

pub fn hash2bytes(a: [u8; 32]) -> Vec<u8> {
    a.to_vec()
}

pub fn bytes2hash(v: Vec<u8>) -> [u8; 32] {
    if v.len() != 32 {
        tracing::error!("bytes2hash: input length must be 32, got {}", v.len());
        return [0u8; 32];
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&v);
    arr
}

pub fn vechash2vecbytes(v: Vec<[u8; 32]>) -> Vec<Vec<u8>> {
    v.into_iter().map(|a| a.to_vec()).collect()
}

pub fn vecbytes2vechash(v: Vec<Vec<u8>>) -> Vec<[u8; 32]> {
    let mut out = Vec::with_capacity(v.len());
    for item in v {
        if item.len() != 32 {
            tracing::error!(
                "vecbytes2vechash: item length must be 32, got {}",
                item.len()
            );
            continue;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&item);
        out.push(arr);
    }
    out
}
