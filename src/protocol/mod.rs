//! Kafka 协议：定义与编解码
//!
//! 采用手工实现的编解码器，与 Kafka 官方二进制协议完全一致。
//! 支持核心请求/响应：
//! - ApiVersions
//! - Metadata
//! - Produce
//! - Fetch
//! - 以及消费者组 / Offset 管理相关请求

pub mod primitive;
pub mod messages;
pub mod recordbatch;

pub use messages::*;
pub use primitive::{Decoder, Encoder, Error as ProtocolError, Result};
pub use recordbatch::{
    Attribute as RecordBatchAttribute, Compression, Record, RecordBatch, RecordBatchReader, Records,
};

/// Kafka API Key 常量
pub mod apikey {
    pub const PRODUCE: i16 = 0;
    pub const FETCH: i16 = 1;
    pub const LIST_OFFSETS: i16 = 2;
    pub const METADATA: i16 = 3;
    pub const OFFSET_COMMIT: i16 = 8;
    pub const OFFSET_FETCH: i16 = 9;
    pub const FIND_COORDINATOR: i16 = 10;
    pub const JOIN_GROUP: i16 = 11;
    pub const HEARTBEAT: i16 = 12;
    pub const LEAVE_GROUP: i16 = 13;
    pub const SYNC_GROUP: i16 = 14;
    pub const API_VERSIONS: i16 = 18;
    pub const CREATE_TOPICS: i16 = 19;
    pub const DELETE_TOPICS: i16 = 20;
    pub const INIT_PRODUCER_ID: i16 = 22;
    pub const DESCRIBE_GROUPS: i16 = 15;
    pub const LIST_GROUPS: i16 = 16;
}

/// Kafka 错误码
pub mod error_codes {
    pub const NONE: i16 = 0;
    pub const UNKNOWN_SERVER_ERROR: i16 = -1;
    pub const OFFSET_OUT_OF_RANGE: i16 = 1;
    pub const CORRUPT_MESSAGE: i16 = 2;
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    pub const INVALID_MESSAGE_SIZE: i16 = 10;
    pub const LEADER_NOT_AVAILABLE: i16 = 5;
    pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    pub const REQUEST_TIMED_OUT: i16 = 7;
    pub const REPLICA_NOT_AVAILABLE: i16 = 9;
    pub const MESSAGE_TOO_LARGE: i16 = 10;
    pub const NETWORK_EXCEPTION: i16 = 13;
    pub const GROUP_LOAD_IN_PROGRESS: i16 = 14;
    pub const GROUP_COORDINATOR_NOT_AVAILABLE: i16 = 15;
    pub const NOT_COORDINATOR: i16 = 16;
    pub const INVALID_TOPIC_EXCEPTION: i16 = 17;
    pub const RECORD_LIST_TOO_LARGE: i16 = 18;
    pub const NOT_ENOUGH_REPLICAS: i16 = 19;
    pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
    pub const INVALID_REQUIRED_ACKS: i16 = 21;
    pub const ILLEGAL_GENERATION: i16 = 22;
    pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
    pub const INVALID_GROUP_ID: i16 = 24;
    pub const UNKNOWN_MEMBER_ID: i16 = 25;
    pub const INVALID_SESSION_TIMEOUT: i16 = 26;
    pub const REBALANCE_IN_PROGRESS: i16 = 27;
    pub const INVALID_COMMIT_OFFSET_SIZE: i16 = 28;
    pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
    pub const GROUP_AUTHORIZATION_FAILED: i16 = 30;
    pub const OFFSET_METADATA_TOO_LARGE: i16 = 12;
    pub const UNSUPPORTED_VERSION: i16 = 35;
    pub const TOPIC_ALREADY_EXISTS: i16 = 36;
    pub const INVALID_PARTITIONS: i16 = 37;
    pub const INVALID_REPLICATION_FACTOR: i16 = 38;
    pub const UNKNOWN_MEMBER_ID_NEW: i16 = 82;
    pub const FETCH_SESSION_ID_NOT_FOUND: i16 = 89;
    pub const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;
}

/// 请求头
#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    // v2 起有 tagged fields，本实现使用 0 长度标记
}

impl RequestHeader {
    pub fn decode(buf: &mut Decoder, _api_version: i16) -> Result<Self> {
        let api_key = buf.get_i16()?;
        let api_version = buf.get_i16()?;
        let correlation_id = buf.get_i32()?;
        // 请求头本身不包含 header_version 字段，API key 决定 header_version。
        let client_id = buf.get_nullable_string()?;
        // 若该 API 的该版本使用 flexible（紧凑）请求头，则需要消费 tagged fields。
        // 请求头是否 flexible 由该 API 的 flexibleVersions 决定（KIP-482）。
        if request_header_is_flexible(api_key, api_version) {
            let _ = read_tagged_fields(buf)?;
        }
        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }
}

/// 该 API 在指定版本下是否使用 flexible（紧凑）请求头。
///
/// Kafka 中请求头是否 flexible 由各 API 的 `flexibleVersions` 决定：
/// 版本达到该阈值后，请求头才带 tagged fields（header_version 2），
/// 之前的版本为传统请求头（header_version 1，不带 tagged fields）。
fn request_header_is_flexible(api_key: i16, api_version: i16) -> bool {
    // ApiVersions 的请求头永远为 v1（KIP-482 例外，协商版本前无法使用 v2 请求头）。
    if api_key == apikey::API_VERSIONS {
        return false;
    }
    // 每个 API 引入 flexible（紧凑）格式的最低版本
    let flexible_since: i16 = match api_key {
        apikey::PRODUCE => 9,
        apikey::FETCH => 12,
        apikey::LIST_OFFSETS => 6,
        apikey::METADATA => 9,
        apikey::OFFSET_COMMIT => 8,
        apikey::OFFSET_FETCH => 6,
        apikey::FIND_COORDINATOR => 3,
        apikey::JOIN_GROUP => 6,
        apikey::HEARTBEAT => 4,
        apikey::LEAVE_GROUP => 4,
        apikey::SYNC_GROUP => 4,
        apikey::API_VERSIONS => 3,
        apikey::DESCRIBE_GROUPS => 5,
        apikey::LIST_GROUPS => 4,
        apikey::CREATE_TOPICS => 5,
        apikey::DELETE_TOPICS => 4,
        // 未知 API 默认不消费 tagged fields
        _ => i16::MAX,
    };
    api_version >= flexible_since
}

/// 读取 tagged fields（紧凑协议），返回字段数量并跳过内容。
pub fn read_tagged_fields(buf: &mut Decoder) -> Result<u32> {
    let n = buf.get_unsigned_varint()? as u32;
    for _ in 0..n {
        let _tag = buf.get_unsigned_varint()?;
        let size = buf.get_unsigned_varint()? as usize;
        let _ = buf.get_bytes(size)?;
    }
    Ok(n)
}

/// 写入 tagged fields（紧凑协议），写入 0 个字段。
pub fn write_tagged_fields(e: &mut Encoder) {
    e.put_unsigned_varint(0);
}
