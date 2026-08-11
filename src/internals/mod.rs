//! 内部主题管理
//!
//! - __consumer_offsets：消费者组偏移量
//! - __cluster_metadata：元数据变更日志（KRaft 模式）

use crate::server::metadata::MetadataManager;
use std::sync::Arc;

pub const CONSUMER_OFFSETS_TOPIC: &str = "__consumer_offsets";
pub const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

/// 内部主题管理器
pub struct InternalTopics {
    pub metadata: Arc<MetadataManager>,
}

impl InternalTopics {
    pub fn new(metadata: Arc<MetadataManager>) -> Self {
        Self { metadata }
    }

    /// 确保内部主题在启动时创建。
    pub fn ensure_created(&self) {
        if let Err(e) = self
            .metadata
            .create_internal_topic(CONSUMER_OFFSETS_TOPIC, 50)
        {
            tracing::warn!("创建 {CONSUMER_OFFSETS_TOPIC} 失败: {e}");
        }
        if let Err(e) = self.metadata.create_internal_topic(CLUSTER_METADATA_TOPIC, 1) {
            tracing::warn!("创建 {CLUSTER_METADATA_TOPIC} 失败: {e}");
        }
    }
}
