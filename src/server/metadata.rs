//! 元数据管理（LogManager + 主题/分区元数据缓存）
//!
//! 维护主题/分区元数据缓存，负责创建/加载/恢复每个分区的日志（PartitionLog）。
//! 使用 dashmap 提升并发读性能。

use crate::config::BrokerConfig;
use crate::storage::PartitionLog;
use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 分区元数据（单机模式：leader 与副本都为本 broker）
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub topic: String,
    pub partition: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct TopicInfo {
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<PartitionInfo>,
}

/// LogManager：管理所有分区的日志实例。
pub struct MetadataManager {
    config: Arc<BrokerConfig>,
    /// topic -> (partition -> 分区日志)
    logs: DashMap<String, DashMap<i32, Arc<RwLock<PartitionLog>>>>,
    /// 主题元数据缓存
    topics: DashMap<String, TopicInfo>,
}

impl MetadataManager {
    pub fn new(config: Arc<BrokerConfig>) -> Self {
        Self {
            config,
            logs: DashMap::new(),
            topics: DashMap::new(),
        }
    }

    /// 启动：扫描 log.dirs 加载所有已有主题/分区。
    pub fn init(&self) -> Result<()> {
        for dir in &self.config.log_dirs {
            let root = PathBuf::from(dir);
            if !root.exists() {
                std::fs::create_dir_all(&root)?;
                continue;
            }
            for entry in std::fs::read_dir(&root)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if let Some((topic, partition)) = parse_topic_partition(&name) {
                    self.open_partition(&topic, partition)?;
                }
            }
        }
        Ok(())
    }

    /// 打开（或创建）一个分区的日志，并注册元数据。
    pub fn open_partition(&self, topic: &str, partition: i32) -> Result<()> {
        let dir = self.partition_dir(topic, partition);
        let log = PartitionLog::open(topic, partition, &dir, self.config.clone())?;
        self.logs
            .entry(topic.to_string())
            .or_insert_with(DashMap::new)
            .insert(partition, Arc::new(RwLock::new(log)));

        let info = PartitionInfo {
            topic: topic.to_string(),
            partition,
            leader: self.config.broker_id,
            replicas: vec![self.config.broker_id],
            isr: vec![self.config.broker_id],
        };
        self.topics
            .entry(topic.to_string())
            .or_insert_with(|| TopicInfo {
                name: topic.to_string(),
                is_internal: false,
                partitions: vec![],
            })
            .partitions
            .push(info);
        Ok(())
    }

    /// 计算分区目录路径：<log_dir>/<topic>-<partition>
    pub fn partition_dir(&self, topic: &str, partition: i32) -> PathBuf {
        self.config
            .primary_log_dir()
            .join(format!("{topic}-{partition}"))
    }

    /// 创建主题（num_partitions 个分区）。
    pub fn create_topic(&self, topic: &str, num_partitions: i32, is_internal: bool) -> Result<()> {
        self.topics.entry(topic.to_string()).or_insert_with(|| TopicInfo {
            name: topic.to_string(),
            is_internal,
            partitions: vec![],
        });
        for p in 0..num_partitions {
            if self.partition_exists(topic, p) {
                continue;
            }
            self.open_partition(topic, p)?;
        }
        Ok(())
    }

    /// 删除主题（删除所有分区日志）。
    pub fn delete_topic(&self, topic: &str) -> Result<()> {
        if let Some(logs) = self.logs.remove(topic) {
            for entry in logs.1.iter() {
                let dir = self.partition_dir(topic, *entry.key());
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
        self.topics.remove(topic);
        Ok(())
    }

    /// 追加消息到指定分区。返回 base_offset 与 last_offset。
    pub async fn append_records(
        &self,
        topic: &str,
        partition: i32,
        batches: &[Bytes],
    ) -> Result<Vec<(i64, i64)>> {
        let log = self.get_log_arc(topic, partition)
            .ok_or_else(|| anyhow::anyhow!("分区 {topic}-{partition} 不存在"))?;
        let mut guard = log.write().await;
        let results = guard.append(batches)?;
        Ok(results.iter().map(|r| (r.base_offset, r.last_offset)).collect())
    }

    /// 从分区读取消息。返回 (数据, 高水位)。
    pub async fn read_records(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_bytes: usize,
    ) -> Option<(Bytes, i64)> {
        let log = self.get_log_arc(topic, partition)?;
        let guard = log.read().await;
        let hw = guard.high_watermark();
        let data = guard.read(start_offset, max_bytes);
        Some((data, hw))
    }

    /// 获取分区 LEO。
    pub async fn partition_leo(&self, topic: &str, partition: i32) -> Option<i64> {
        let log = self.get_log_arc(topic, partition)?;
        Some(log.read().await.leo())
    }

    /// 同步获取分区 LEO（用于同步处理路径）。
    pub fn partition_leo_sync(&self, topic: &str, partition: i32) -> Option<i64> {
        let log = self.get_log_arc(topic, partition)?;
        log.try_read().ok().map(|l| l.leo())
    }

    /// 同步获取分区日志起始偏移量。
    pub fn partition_log_start_sync(&self, topic: &str, partition: i32) -> Option<i64> {
        let log = self.get_log_arc(topic, partition)?;
        log.try_read().ok().map(|l| l.log_start())
    }

    /// 获取分区日志的 Arc 引用。
    pub fn get_log_arc(&self, topic: &str, partition: i32) -> Option<Arc<RwLock<PartitionLog>>> {
        self.logs.get(topic)?.get(&partition).map(|l| l.clone())
    }

    /// 主题列表。
    pub fn topic_list(&self) -> Vec<TopicInfo> {
        self.topics.iter().map(|t| t.value().clone()).collect()
    }

    /// 获取主题信息。
    pub fn get_topic(&self, topic: &str) -> Option<TopicInfo> {
        self.topics.get(topic).map(|t| t.value().clone())
    }

    /// 分区是否已存在。
    pub fn partition_exists(&self, topic: &str, partition: i32) -> bool {
        self.logs
            .get(topic)
            .map(|l| l.contains_key(&partition))
            .unwrap_or(false)
    }

    /// 所有主题数量（监控用）。
    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }

    /// 创建主题时的默认分区数（auto.create.topics 时使用）。
    pub fn default_partitions(&self) -> i32 {
        self.config.num_partitions.max(1)
    }

    /// 主题内分区数。
    pub fn partitions_of(&self, topic: &str) -> i32 {
        self.topics
            .get(topic)
            .map(|t| t.value().partitions.len() as i32)
            .unwrap_or(0)
    }

    /// 创建内部主题（如 __consumer_offsets）。
    pub fn create_internal_topic(&self, name: &str, partitions: i32) -> Result<()> {
        self.create_topic(name, partitions, true)
    }
}

/// 从目录名 "topic-123" 解析出 (topic, partition)。
fn parse_topic_partition(name: &str) -> Option<(String, i32)> {
    let idx = name.rfind('-')?;
    let (topic, part) = name.split_at(idx);
    let part = part.trim_start_matches('-').parse::<i32>().ok()?;
    Some((topic.to_string(), part))
}
