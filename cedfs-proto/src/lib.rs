pub mod kvcache {
    include!("kvcache.rs");

    pub mod v2 {
        tonic::include_proto!("kvcache.v2");
    }
}

pub mod lmcache {
    include!("lmcache.rs");

    pub mod v2 {
        tonic::include_proto!("lmcache.v2");
    }
}

pub use kvcache::v2 as kvcache_v2;
pub use lmcache::v2 as lmcache_v2;

pub const V2_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("cedfs_kv_v2_descriptor");
