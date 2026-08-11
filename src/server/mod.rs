//! 服务端：网络层与请求处理
//!
//! - 2.1 网络层：tokio 异步 TCP，连接管理，请求读取循环
//! - 2.2 核心请求处理器：ApiVersions / Metadata / Produce / Fetch
//! - 2.3 日志管理层：LogManager
//! - 2.4 启动恢复

pub mod metadata;
pub mod network;
pub mod handler;

pub use metadata::MetadataManager;
pub use network::Broker;
