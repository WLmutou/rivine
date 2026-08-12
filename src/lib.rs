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

/// 本 Broker 支持的 API 版本范围（Kafka 协议中的 ApiVersions 响应）。
///
/// 说明：Broker 的请求/响应编解码目前主要实现的是**传统（非紧凑）格式**，
/// 因此这里声明的 max_version 都被限制在各 API 引入紧凑格式（flexible）之前的
/// 旧版本。这样 kafka-python 等标准客户端在 ApiVersions 协商后会选用传统格式，
/// 与 Broker 的编解码保持一致；同时 Produce 仍使用 v3（RecordBatch magic=2），
/// 与存储层的消息格式匹配。
pub const SUPPORTED_API_KEYS: &[(i16, i16, i16)] = &[
    (0, 0, 3),   // Produce (v3: 传统格式 + RecordBatch magic=2)
    (1, 0, 5),   // Fetch (flexible 自 v12 起，限到 v5 传统格式)
    (2, 0, 2),   // ListOffsets (flexible 自 v6 起，限到 v2 传统格式)
    (3, 0, 4),   // Metadata (flexible 自 v9 起，限到 v4 传统格式)
    (8, 0, 3),   // OffsetCommit (flexible 自 v8 起，限到 v3 传统格式)
    (9, 0, 3),   // OffsetFetch (flexible 自 v6 起，限到 v3 传统格式)
    (10, 0, 1),  // FindCoordinator (flexible 自 v4 起，限到 v1 传统格式)
    (11, 0, 2),  // JoinGroup (flexible 自 v6 起，限到 v2 传统格式)
    (12, 0, 1),  // Heartbeat (flexible 自 v4 起，限到 v1 传统格式)
    (13, 0, 1),  // LeaveGroup (flexible 自 v5 起，限到 v1 传统格式)
    (14, 0, 1),  // SyncGroup (flexible 自 v5 起，限到 v1 传统格式)
    (18, 0, 4),  // ApiVersions (v0-v2 传统，v3+ 紧凑；按请求版本动态编码)
    (15, 0, 2),  // DescribeGroups (flexible 自 v5 起，限到 v2 传统格式)
    (16, 0, 2),  // ListGroups (flexible 自 v4 起，限到 v2 传统格式)
    (19, 0, 2),  // CreateTopics (flexible 自 v5 起，限到 v2 传统格式)
    (20, 0, 1),  // DeleteTopics (flexible 自 v4 起，限到 v1 传统格式)
    (22, 0, 1),  // InitProducerId (flexible 自 v2 起，限到 v1 传统格式，支持幂等生产者)
];
