
//! 支持配置文件（TOML）、环境变量和默认值。使用 `config` crate 分层加载。

use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Broker 配置
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BrokerConfig {
    /// 监听地址
    pub host: String,
    /// 监听端口
    pub port: i32,
    /// Broker ID
    pub broker_id: i32,
    /// 日志目录（逗号分隔）
    pub log_dirs: Vec<String>,
    /// 默认分区数
    pub num_partitions: i32,
    /// 默认副本因子
    pub default_replication_factor: i16,
    /// 集群 ID
    pub cluster_id: String,
    /// 段大小阈值（字节）
    pub log_segment_bytes: i64,
    /// 段滚动时间（毫秒）
    pub log_roll_ms: i64,
    /// 索引间隔字节
    pub index_interval_bytes: i32,
    /// 基于时间的保留（小时）
    pub log_retention_hours: i64,
    /// 基于大小的保留（字节）
    pub log_retention_bytes: i64,
    /// 清理策略：delete 或 compact
    pub cleanup_policy: String,
    /// 刷盘间隔消息数
    pub log_flush_interval_messages: i64,
    /// 刷盘间隔毫秒
    pub log_flush_interval_ms: i64,
    /// min.insync.replicas
    pub min_insync_replicas: i32,
    /// 默认 acks 需要的副本确认
    pub default_replication_ack: i16,
    /// 消费者组超时
    pub group_initial_rebalance_delay_ms: i32,
    /// 是否单机模式（不启动 Raft）
    pub single_node: bool,
    /// Raft 对等节点地址（multi-node 模式）
    pub raft_peers: Vec<String>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 9092,
            broker_id: 0,
            log_dirs: vec!["/tmp/rivine-logs".to_string()],
            num_partitions: 1,
            default_replication_factor: 1,
            cluster_id: "rivine-cluster".to_string(),
            log_segment_bytes: 1024 * 1024 * 1024,
            log_roll_ms: 7 * 24 * 3600 * 1000,
            index_interval_bytes: 4096,
            log_retention_hours: 168,
            log_retention_bytes: -1,
            cleanup_policy: "delete".to_string(),
            log_flush_interval_messages: 10000,
            log_flush_interval_ms: 1000,
            min_insync_replicas: 1,
            default_replication_ack: 1,
            group_initial_rebalance_delay_ms: 3000,
            single_node: true,
            raft_peers: vec![],
        }
    }
}

impl BrokerConfig {
    /// 从文件 + 环境变量加载配置。文件可不存在（使用默认值）。
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let mut cfg = config::Config::builder();
        if let Some(p) = path {
            if p.exists() {
                cfg = cfg.add_source(config::File::from(p));
            }
        }
        // 环境变量：RIVINE_BROKER_ID 等
        cfg = cfg.add_source(
            config::Environment::with_prefix("RIVINE")
                .try_parsing(true)
                .separator("_"),
        );
        let settings = cfg.build()?;
        let conf: BrokerConfig = settings.try_deserialize()?;
        // 确保日志目录存在
        for dir in &conf.log_dirs {
            std::fs::create_dir_all(dir)?;
        }
        Ok(conf)
    }

    /// 获取第一个日志目录（单机模式使用）。
    pub fn primary_log_dir(&self) -> PathBuf {
        PathBuf::from(&self.log_dirs[0])
    }
}
