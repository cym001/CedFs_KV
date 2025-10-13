use serde::{Deserialize, Serialize};

use crate::constant::QuinnResponseStatus;

/// Error type for QUIC message operations
#[derive(Debug)]
pub enum QuicMessageError {
    /// Serialization/Deserialization error
    BincodeError(bincode::Error),
    /// Invalid format error
    InvalidFormat(String),
}

impl std::fmt::Display for QuicMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuicMessageError::BincodeError(e) => write!(f, "Bincode error: {}", e),
            QuicMessageError::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
        }
    }
}

impl std::error::Error for QuicMessageError {}

impl From<bincode::Error> for QuicMessageError {
    fn from(err: bincode::Error) -> Self {
        QuicMessageError::BincodeError(err)
    }
}

/// Result type for QUIC message operations
pub type QuicResult<T> = Result<T, QuicMessageError>;

/// Represents a QUIC read request message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicReadRequest {
    /// Block ID to read from
    pub bid: i64,
    /// Offset within the block
    pub offset: u64,
    /// Size of data to read
    pub size: u64,
}

impl QuicReadRequest {
    /// Create a new QuicReadRequest
    pub fn new(bid: i64, offset: u64, size: u64) -> Self {
        Self { bid, offset, size }
    }
}

/// Represents a QUIC write request message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicWriteRequest {
    /// Block ID to write to
    pub bid: i64,
    /// Offset within the block
    pub offset: u64,
    /// Data to write
    pub data: Vec<u8>,
}

impl QuicWriteRequest {
    /// Create a new QuicWriteRequest
    pub fn new(bid: i64, offset: u64, data: Vec<u8>) -> Self {
        Self { bid, offset, data }
    }
}

/// Represents a QUIC response message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicResponse {
    /// Status of the response
    pub status: QuinnResponseStatus,
    /// Response data (if any)
    pub data: Vec<u8>,
}

impl QuicResponse {
    /// Create a new successful QuicResponse with data
    pub fn success(data: Vec<u8>) -> Self {
        Self {
            status: QuinnResponseStatus::Success,
            data,
        }
    }

    /// Create a new failed QuicResponse
    pub fn fail() -> Self {
        Self {
            status: QuinnResponseStatus::Fail,
            data: Vec::new(),
        }
    }
}

/// Enum representing all possible QUIC messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuicMessage {
    /// Read request
    ReadRequest(QuicReadRequest),
    /// Write request
    WriteRequest(QuicWriteRequest),
    /// Response
    Response(QuicResponse),
}

impl QuicMessage {
    /// Serialize the message into bytes using bincode
    pub fn serialize(&self) -> QuicResult<Vec<u8>> {
        let bytes = bincode::serialize(self)?;
        Ok(bytes)
    }

    /// Deserialize bytes into a QuicMessage using bincode
    pub fn deserialize(buf: &[u8]) -> QuicResult<Self> {
        if buf.is_empty() {
            return Err(QuicMessageError::InvalidFormat(
                "Empty message buffer".to_string(),
            ));
        }

        let message = bincode::deserialize(buf)?;
        Ok(message)
    }
}
