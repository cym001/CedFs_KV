use chrono::{DateTime, Duration, Timelike, Utc};
use serde::{Deserialize, Serialize};

use crate::client2meta;
use crate::constant::{
    NO_ATIME, STRICT_ATIME, TYPE_LOCAL_DIRECTORY, TYPE_REMOTE_DIRECTORY, TYPE_REMOTE_FILE,
};

pub type Ino = u64;

#[derive(Debug, Clone, PartialEq)]
pub struct AccessContext {
    pub pid: u32,
    pub uid: u32,
    pub gid: Vec<u32>,
}

/// Attr represents attributes of a node.
#[derive(Debug, Default, Clone, Serialize, Deserialize, derive_builder::Builder)]
pub struct Attr {
    // Flags
    #[builder(default = 0)]
    pub flags: u8,
    // Type of node
    #[builder(default = TYPE_LOCAL_DIRECTORY)]
    pub _type: u8,
    // Permission mode
    // TODO: default value is temporary 0o777
    #[builder(default = 0o777)]
    pub mode: u16,
    // Owner ID
    #[builder(default = 0)]
    pub uid: u32,
    // Group ID of owner
    #[builder(default = 0)]
    pub gid: u32,
    // Device number
    #[builder(default = 0)]
    pub rdev: u32,
    // Last access time
    #[builder(default = 0)]
    pub atime: i64,
    // Last modified time
    #[builder(default = 0)]
    pub mtime: i64,
    // Last change time for metadata
    #[builder(default = 0)]
    pub ctime: i64,
    // Nanosecond part of atime
    #[builder(default = 0)]
    pub atime_nsec: u32,
    // Nanosecond part of mtime
    #[builder(default = 0)]
    pub mtime_nsec: u32,
    // Nanosecond part of ctime
    #[builder(default = 0)]
    pub ctime_nsec: u32,
    // Number of links (subdirectories or hardlinks)
    #[builder(default = 1)]
    pub nlink: u32,
    // Length of regular file
    #[builder(default = 0)]
    pub length: u64,

    // Inode of parent; 0 means tracked by parent_key (for hardlinks)
    #[builder(default = 0)]
    pub parent: Ino,
    // The attributes are completed or not
    #[builder(default = false)]
    pub full: bool,
    // Whether to keep the cached page or not
    #[builder(default = false)]
    pub keep_cache: bool,

    // Access ACL ID (identical ACL rules share the same access ACL ID.)
    #[builder(default = 0)]
    pub access_acl: u32,
    // Default ACL ID (default ACL and the access ACL share the same cache and store)
    #[builder(default = 0)]
    pub default_acl: u32,
}

impl Attr {
    pub fn decode(v: &[u8]) -> Self {
        bincode::deserialize(v).unwrap()
    }

    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn access_mode(&self, uid: u32, gids: &[u32]) -> u8 {
        todo!()
    }

    pub fn clean_sugid(&mut self, ctx: &AccessContext) {
        if self._type != TYPE_LOCAL_DIRECTORY && self._type != TYPE_REMOTE_DIRECTORY {
            if ctx.uid != 0 || (self.mode >> 3) & 1 != 0 {
                self.mode &= 0o1777;
            } else {
                self.mode &= 0o3777;
            }
        }
    }

    pub fn atime(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.atime, self.atime_nsec).unwrap()
    }

    pub fn mtime(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.mtime, self.mtime_nsec).unwrap()
    }

    pub fn ctime(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.ctime, self.ctime_nsec).unwrap()
    }

    // this will be called when create() or mknod() is called to ensure
    // atime, mtime and ctime are set as the same time.
    pub fn init_time(&mut self) {
        let now = Utc::now();
        self.atime = now.timestamp();
        self.atime_nsec = now.nanosecond();
        self.ctime = now.timestamp();
        self.ctime_nsec = now.nanosecond();
        self.mtime = now.timestamp();
        self.mtime_nsec = now.nanosecond();
    }

    pub fn update_atime(&mut self) {
        let now = Utc::now();
        self.atime = now.timestamp();
        self.atime_nsec = now.nanosecond();
    }

    pub fn update_mtime(&mut self) {
        let now = Utc::now();
        self.mtime = now.timestamp();
        self.mtime_nsec = now.nanosecond();
    }

    pub fn update_ctime(&mut self) {
        let now = Utc::now();
        self.ctime = now.timestamp();
        self.ctime_nsec = now.nanosecond();
    }

    pub fn atime_need_update(&self, atime_mode: u8) -> bool {
        // judge xxx
        if atime_mode != NO_ATIME && self.relative_atime_need_update() {
            return true;
        }

        // judge
        if atime_mode == STRICT_ATIME
            && (Utc::now() - DateTime::from_timestamp(self.atime, self.atime_nsec).unwrap()
                > Duration::seconds(1))
        {
            return true;
        }

        false
    }

    pub fn relative_atime_need_update(&self) -> bool {
        let atime = self.atime();
        let ctime = self.ctime();
        let mtime = self.mtime();

        mtime > atime || ctime > atime || Utc::now() - atime > Duration::hours(24)
    }

    pub fn type_into_remote(mut self) -> Self {
        match self._type {
            TYPE_LOCAL_DIRECTORY => self._type = TYPE_REMOTE_DIRECTORY,
            TYPE_LOCAL_FILE => self._type = TYPE_REMOTE_FILE,
            _ => {},
        };

        self
    }

    pub fn type_into_local(mut self) -> Self {
        match self._type {
            TYPE_REMOTE_DIRECTORY => self._type = TYPE_LOCAL_DIRECTORY,
            TYPE_REMOTE_FILE => self._type = TYPE_LOCAL_DIRECTORY,
            _ => {},
        }

        self
    }

    pub fn is_dir(&self) -> bool {
        matches!(self._type, TYPE_LOCAL_DIRECTORY | TYPE_REMOTE_DIRECTORY)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlocksPosition {
    pub blocks_pos: Vec<BlockPosition>,
    pub replica_blocks_pos: Vec<Vec<BlockPosition>>,
}

impl BlocksPosition {
    pub fn has_replica_with_ip(&self, target_ip: &str) -> bool {
        self.replica_blocks_pos.iter().any(|replicas| {
            replicas
                .iter()
                .any(|replica| replica.datanode_ip == target_ip)
        })
    }
    pub fn convert_to_proto_blocks_pos(&self) -> client2meta::BlocksPos {
        let blocks_pos = self
            .blocks_pos
            .iter()
            .map(|block| client2meta::BlockPos {
                datanode_ip: block.datanode_ip.clone(),
                block_id: block.block_id,
            })
            .collect();

        let replica_blocks_pos = self
            .replica_blocks_pos
            .iter()
            .map(|replica_blocks| {
                let replica_blocks = replica_blocks
                    .iter()
                    .map(|replica| client2meta::BlockPos {
                        datanode_ip: replica.datanode_ip.clone(),
                        block_id: replica.block_id,
                    })
                    .collect();
                client2meta::ReplicaBlocks { replica_blocks }
            })
            .collect();

        client2meta::BlocksPos {
            blocks_pos,
            replica_blocks_pos,
        }
    }
}

impl From<client2meta::BlocksPos> for BlocksPosition {
    fn from(protoc_blocks_pos: client2meta::BlocksPos) -> Self {
        let blocks_pos = protoc_blocks_pos
            .blocks_pos
            .into_iter()
            .map(|proto_block| BlockPosition {
                datanode_ip: proto_block.datanode_ip,
                block_id: proto_block.block_id,
            })
            .collect();

        let replica_blocks_pos = protoc_blocks_pos
            .replica_blocks_pos
            .into_iter()
            .map(|replica_block| {
                replica_block
                    .replica_blocks
                    .into_iter()
                    .map(|replica| BlockPosition {
                        datanode_ip: replica.datanode_ip,
                        block_id: replica.block_id,
                    })
                    .collect()
            })
            .collect();

        Self {
            blocks_pos,
            replica_blocks_pos,
        }
    }
}

/// BlockPos is the information of a block stored in meta server,
/// and BlockEntry is information of a block to write or read data, which is used in data server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockPosition {
    /// The ip of data server where the block is located
    pub datanode_ip: String,
    /// The block id in data server
    pub block_id: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetAttr {
    pub mode: Option<u16>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub length: Option<u64>,
    pub atime: Option<i64>,
    pub mtime: Option<i64>,
    pub ctime: Option<i64>,
    pub atime_nsec: Option<u32>,
    pub mtime_nsec: Option<u32>,
    pub ctime_nsec: Option<u32>,
}

impl SetAttr {
    pub fn decode(v: &[u8]) -> Self {
        bincode::deserialize(v).unwrap()
    }

    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}
