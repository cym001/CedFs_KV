use iota::iota;

pub const ROOT_INODE: u64 = 1;
pub const KEY_ACL_COUNTER: &str = "KEY_ACL_COUNTER";
pub const KEY_PARENT_PATH: &str = "KEY_PARENT_PATH";

iota!(
    // The first file type, reserved for special purposes
    pub const TYPE_RESERVED: u8 = iota;,
    // Represents a local file
    TYPE_LOCAL_FILE,
    // Represents a local directory
    TYPE_LOCAL_DIRECTORY,
    // Represents a remote file
    TYPE_REMOTE_FILE,
    // Represents a remote directory
    TYPE_REMOTE_DIRECTORY
);

iota!(
    // Set file mode
    pub const SET_ATTR_MODE: u16 = 1 << iota;,
    // Set user ID
    SET_ATTR_UID,
    // Set group ID
    SET_ATTR_GID,
    // Set file size
    SET_ATTR_SIZE,
    // Set access time
    SET_ATTR_ATIME,
    // Set modification time
    SET_ATTR_MTIME,
    // Set creation time
    SET_ATTR_CTIME,
    // Set access time
    SET_ATTR_ATIME_NOW,
    // Set modification time
    SET_ATTR_MTIME_NOW,
    // Set file flags
    SET_ATTR_FLAG
);

iota!(
    // Mask for read permission
    pub const MODE_MASK_R: u8 = 1 << iota;,
    // Mask for write permission
    MODE_MASK_W,
    // Mask for execute permission
    MODE_MASK_X
);

iota!(
    // Flag for immutable files
    pub const FLAG_IMMUTABLE: u8 = 1 << iota;,
    // Flag for append-only files
    FLAG_APPEND
);

iota!(
    // Operation to set quota
    pub const QUOTA_SET: u8 = 1 << iota;,
    // Operation to get quota
    QUOTA_GET,
    // Operation to delete quota
    QUOTA_DEL,
    // Operation to list quotas
    QUOTA_LIST,
    // Operation to check quota
    QUOTA_CHECK
);

iota!(
    // Rename without replacing existing files
    const RENAME_NO_REPLACE: u16 = 1 << iota;,
    // Rename by exchanging file positions
    RENAME_EXCHANGE,
    // Rename using whiteout for deleted files
    RENAME_WHITEOUT,
    // Rename to restore a file
    RENAME_RESTORE
);

iota!(
    // No ACL specified
    pub const ACL_NONE: u32 = 1 << iota;,
    // ACL for access permissions
    ACL_ACCESS,
    // ACL for default permissions
    ACL_DEFAULT
);

iota!(
    // Flag to disable access time updates
    pub const NO_ATIME: u8 = 1 << iota;,
    // Flag for relative access time updates
    REL_ATIME,
    // Flag for strict access time updates
    STRICT_ATIME
);

pub(crate) const KB: u64 = 1024;

pub(crate) const MB: u64 = 1024 * KB;

/// Enum representing the type of QUIC operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum QuinnOperationType {
    /// Read operation
    Read = 0,
    /// Write operation
    Write = 1,
}

/// Enum representing the response status of QUIC operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum QuinnResponseStatus {
    /// Operation succeeded
    Success = 0,
    /// Operation failed
    Fail = 1,
}
