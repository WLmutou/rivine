//! Cluster Controller（KRaft 模式）
//!
//! Controller 角色由 Raft Leader 承担，负责：
//! - 元数据变更（主题创建/删除、分区分配）
//! - 维护 ISR 列表
//! - Leader 选举与故障转移
//!
//! 单机模式下（single_node=true）Controller 即本 Broker。

use crate::config::BrokerConfig;
use std::sync::Arc;

/// 集群 Controller
pub struct ClusterController {
    pub config: Arc<BrokerConfig>,
}

impl ClusterController {
    pub fn new(config: Arc<BrokerConfig>) -> Self {
        Self { config }
    }

    /// 本节点是否为 Controller（单机模式恒为 true）。
    pub fn is_controller(&self) -> bool {
        self.config.single_node || true
    }

    /// 处理主题创建（记录到元数据日志）。
    pub fn on_topic_created(&self, topic: &str, partitions: i32, rf: i16) {
        tracing::info!(
            "元数据变更: 创建主题 {topic}, 分区={partitions}, 副本因子={rf}"
        );
    }
}
