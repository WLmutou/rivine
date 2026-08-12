//! Offset 持久化存储
//!
//! 将已提交的消费偏移量写入内部主题 `__consumer_offsets`，并在启动时恢复。
//!
//! 编码格式（每个 offset 记录为一条 RecordBatch，key=group_id）：
//! ```text
//! value => topic(string) partition(int32) offset(int64) metadata(nullable string)
//! ```
//!
//! 分区选择：按 group_id 的稳定哈希映射到 `__consumer_offsets` 的某个分区。

use crate::protocol::recordbatch::{Compression, Record, RecordBatch};
use crate::server::metadata::MetadataManager;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

pub const CONSUMER_OFFSETS_TOPIC: &str = "__consumer_offsets";
/// __consumer_offsets 默认分区数（与 Kafka 默认一致）。
pub const DEFAULT_OFFSETS_PARTITIONS: i32 = 50;

/// 持久化 Offset 存储。
pub struct OffsetStore {
    metadata: Arc<MetadataManager>,
    offsets_topic_partitions: i32,
    /// 内存缓存：group_id -> topic -> partition -> (offset, metadata)
    cache: HashMap<String, HashMap<String, HashMap<i32, (i64, Option<String>)>>>,
}

impl OffsetStore {
    pub fn new(metadata: Arc<MetadataManager>) -> Self {
        Self {
            metadata,
            offsets_topic_partitions: DEFAULT_OFFSETS_PARTITIONS,
            cache: HashMap::new(),
        }
    }

    /// 计算 group_id 对应的 __consumer_offsets 分区。
    pub fn coordinator_partition(&self, group_id: &str) -> i32 {
        let h = stable_hash(group_id);
        (h % (self.offsets_topic_partitions as u64)) as i32
    }

    /// 确保内部主题存在。
    pub fn ensure_created(&self) {
        if let Err(e) = self
            .metadata
            .create_internal_topic(CONSUMER_OFFSETS_TOPIC, self.offsets_topic_partitions)
        {
            tracing::warn!("创建 {CONSUMER_OFFSETS_TOPIC} 失败: {e}");
        }
    }

    /// 启动时从 __consumer_offsets 扫描并恢复所有已提交的偏移量。
    pub fn recover(&mut self) {
        let mut recovered: HashMap<String, HashMap<String, HashMap<i32, (i64, Option<String>)>>> =
            HashMap::new();
        for partition in 0..self.offsets_topic_partitions {
            // 读取整个分区的数据。
            let (data, _) = self
                .metadata
                .read_records_sync(CONSUMER_OFFSETS_TOPIC, partition, 0, usize::MAX)
                .unwrap_or((Bytes::new(), 0));
            if data.is_empty() {
                continue;
            }
            // 解析 RecordBatch，恢复每条 offset 记录。
            let mut reader = crate::protocol::recordbatch::RecordBatchReader::new(data);
            while let Ok(Some((batch, _))) = reader.next_batch() {
                for record in &batch.records {
                    let Some(group_id) = record.key.as_ref().and_then(|k| String::from_utf8(k.to_vec()).ok()) else {
                        continue;
                    };
                    let Some(value) = record.value.as_ref() else {
                        continue;
                    };
                    if let Some((topic, partition, offset, metadata)) = decode_offset(value) {
                        recovered
                            .entry(group_id)
                            .or_insert_with(HashMap::new)
                            .entry(topic)
                            .or_insert_with(HashMap::new)
                            .insert(partition, (offset, metadata));
                    }
                }
            }
        }
        tracing::info!(
            "OffsetStore 恢复完成: {} 个组的偏移量",
            recovered.len()
        );
        self.cache = recovered;
    }

    /// 提交偏移量：更新内存缓存并写入 __consumer_offsets。
    pub fn commit(&mut self, group_id: &str, topic: &str, partition: i32, offset: i64, metadata: Option<String>) {
        // 更新内存缓存。
        self.cache
            .entry(group_id.to_string())
            .or_insert_with(HashMap::new)
            .entry(topic.to_string())
            .or_insert_with(HashMap::new)
            .insert(partition, (offset, metadata.clone()));

        // 持久化到 __consumer_offsets。
        let target_partition = self.coordinator_partition(group_id);
        let value = encode_offset(topic, partition, offset, metadata);
        let record = Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: Some(Bytes::from(group_id.to_string())),
            value: Some(Bytes::from(value)),
            headers: vec![],
        };
        let batch = RecordBatch::serialize(
            0,
            vec![record],
            Compression::None,
            crate::now_ms(),
            0,
            0,
            0,
            0,
        );
        // 追加到 __consumer_offsets 对应分区。
        if let Err(e) = self.metadata.append_records_sync(CONSUMER_OFFSETS_TOPIC, target_partition, &[batch]) {
            tracing::warn!(
                "写入 {CONSUMER_OFFSETS_TOPIC} 分区 {target_partition} 失败: {e}"
            );
        }
    }

    /// 查询偏移量及元数据。
    pub fn fetch_offset_with_meta(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> Option<(i64, Option<String>)> {
        self.cache.get(group_id)?.get(topic)?.get(&partition).cloned()
    }

    /// 查询偏移量。
    pub fn fetch_offset(&self, group_id: &str, topic: &str, partition: i32) -> Option<i64> {
        self.fetch_offset_with_meta(group_id, topic, partition)
            .map(|(offset, _)| offset)
    }

    /// 列出有已提交偏移量的组（用于 ListGroups）。
    pub fn groups_with_offsets(&self) -> Vec<String> {
        self.cache.keys().cloned().collect()
    }
}

/// 编码一条 offset 记录（不包含 key，key 为 group_id）。
fn encode_offset(topic: &str, partition: i32, offset: i64, metadata: Option<String>) -> Vec<u8> {
    let mut e = crate::protocol::Encoder::new();
    e.put_string(topic);
    e.put_i32(partition);
    e.put_i64(offset);
    e.put_nullable_string(metadata.as_deref());
    e.into_bytes().to_vec()
}

/// 解码一条 offset 记录。
fn decode_offset(data: &[u8]) -> Option<(String, i32, i64, Option<String>)> {
    let mut d = crate::protocol::Decoder::new(Bytes::copy_from_slice(data));
    let topic = d.get_string().ok()?;
    let partition = d.get_i32().ok()?;
    let offset = d.get_i64().ok()?;
    let metadata = d.get_nullable_string().ok()?;
    Some((topic, partition, offset, metadata))
}

/// 稳定字符串哈希（用于 Coordinator 分区分配）。
fn stable_hash(s: &str) -> u64 {
    let mut h: u64 = 0;
    for b in s.bytes() {
        h = h.wrapping_mul(31).wrapping_add(b as u64);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BrokerConfig;

    fn make_metadata(tag: &str) -> Arc<MetadataManager> {
        let dir = std::env::temp_dir().join(format!("rivine-offset-test-{tag}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut cfg = BrokerConfig::default();
        cfg.log_dirs = vec![dir.to_string_lossy().to_string()];
        let metadata = Arc::new(MetadataManager::new(Arc::new(cfg)));
        metadata.init().unwrap();
        metadata
    }

    #[test]
    fn test_offset_commit_and_recover() {
        let metadata = make_metadata("basic");
        let mut store = OffsetStore::new(metadata.clone());
        store.ensure_created();

        // 提交几个偏移量。
        store.commit("group-a", "topic-1", 0, 42, None);
        store.commit("group-a", "topic-1", 1, 100, Some("meta".into()));
        store.commit("group-b", "topic-2", 3, 7, None);

        // 查询应命中。
        assert_eq!(store.fetch_offset("group-a", "topic-1", 0), Some(42));
        assert_eq!(
            store.fetch_offset_with_meta("group-a", "topic-1", 1),
            Some((100, Some("meta".to_string())))
        );
        assert_eq!(store.fetch_offset("group-b", "topic-2", 3), Some(7));
        // 未提交的应返回 None。
        assert_eq!(store.fetch_offset("group-a", "topic-2", 0), None);

        // 模拟重启：新建 store，从 __consumer_offsets 恢复。
        let mut store2 = OffsetStore::new(metadata.clone());
        store2.recover();

        assert_eq!(store2.fetch_offset("group-a", "topic-1", 0), Some(42));
        assert_eq!(
            store2.fetch_offset_with_meta("group-a", "topic-1", 1),
            Some((100, Some("meta".to_string())))
        );
        assert_eq!(store2.fetch_offset("group-b", "topic-2", 3), Some(7));
    }
}
