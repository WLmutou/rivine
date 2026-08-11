//! rivine — 用 Rust 重写 Apache Kafka
//!
//!
//! 模块结构：
//! - protocol : Kafka 协议定义与编解码
//! - storage  : 日志段存储格式
//! - config   : 配置管理
//! - server   : 网络层与请求处理
//! - cluster  : 副本协议与 Controller
//! - group    : 消费者组协议
//! - internals: 内部主题
//! - metrics  : 监控

pub mod protocol;
pub mod storage;
pub mod config;
pub mod server;
pub mod cluster;
pub mod group;
pub mod internals;
pub mod metrics;

pub use config::BrokerConfig;
pub use server::Broker;

/// 当前 Unix 时间戳（毫秒）。
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 本 Broker 支持的 API 版本范围（Kafka 协议中的 ApiVersions 响应）
pub const SUPPORTED_API_KEYS: &[(i16, i16, i16)] = &[
    (0, 0, 3),   // Produce
    (1, 0, 15),  // Fetch
    (2, 0, 13),  // ListOffsets
    (3, 0, 13),  // Metadata
    (8, 0, 3),   // OffsetCommit
    (9, 0, 9),   // OffsetFetch
    (10, 0, 6),  // FindCoordinator
    (11, 0, 9),  // JoinGroup
    (12, 0, 7),  // Heartbeat
    (13, 0, 7),  // LeaveGroup
    (14, 0, 5),  // SyncGroup
    (18, 0, 4),  // ApiVersions
    (19, 0, 2),  // CreateTopics
    (20, 0, 7),  // DeleteTopics
    (32, 0, 1),  // SASLHandshake (最小占位)
];
