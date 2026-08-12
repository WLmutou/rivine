//! PartitionLog：一个分区的日志管理
//!
//! 管理该分区的所有 Segment（活跃段 + 只读段），提供 append() 与 read()，
//! 维护 hw（高水位）与 leo（日志末尾偏移量），并实现 Segment 滚动与保留清理。

use super::segment::LogSegment;
use crate::config::BrokerConfig;
use crate::protocol::{RecordBatch, RecordBatchReader};
use anyhow::Result;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

/// 追加操作的结果
#[derive(Debug, Clone)]
pub struct AppendResult {
    pub base_offset: i64,
    pub last_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
}

/// 一个分区的日志
pub struct PartitionLog {
    /// 主题名
    pub topic: String,
    /// 分区号
    pub partition: i32,
    pub dir: PathBuf,
    /// 所有 Segment，按 base_offset 排序（BTreeMap 按键排序）
    segments: BTreeMap<i64, LogSegment>,
    /// 高水位（消费者可见的最大偏移量，即 LEO）
    pub high_watermark: i64,
    /// 日志起始偏移量（最旧可用偏移量）
    pub log_start_offset: i64,
    config: Arc<BrokerConfig>,
}

impl PartitionLog {
    /// 在给定目录下打开或创建分区日志。
    pub fn open(topic: &str, partition: i32, dir: &PathBuf, config: Arc<BrokerConfig>) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut segments = BTreeMap::new();
        // 扫描目录，找到所有 .log 文件并按其 base_offset 排序加载
        let mut base_offsets: Vec<i64> = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".log") {
                let base = name.trim_end_matches(".log").parse::<i64>().unwrap_or(0);
                base_offsets.push(base);
            }
        }
        base_offsets.sort_unstable();
        let mut log_start_offset = i64::MAX;
        for base in &base_offsets {
            let seg = LogSegment::open(
                dir,
                *base,
                config.index_interval_bytes,
                config.log_segment_bytes,
                config.log_roll_ms,
            )?;
            if *base < log_start_offset {
                log_start_offset = *base;
            }
            segments.insert(*base, seg);
        }
        if segments.is_empty() {
            // 创建第一个段，base_offset 从 0 开始
            let seg = LogSegment::open(
                dir,
                0,
                config.index_interval_bytes,
                config.log_segment_bytes,
                config.log_roll_ms,
            )?;
            log_start_offset = 0;
            segments.insert(0, seg);
        }

        // LEO = 最后一个段的最大偏移量 + 1
        let leo = segments
            .iter()
            .next_back()
            .map(|(_, s)| s.max_offset + 1)
            .unwrap_or(0);

        let log = Self {
            topic: topic.to_string(),
            partition,
            dir: dir.clone(),
            segments,
            high_watermark: leo,
            log_start_offset: if log_start_offset == i64::MAX { 0 } else { log_start_offset },
            config,
        };
        log.recover()?;
        Ok(log)
    }

    /// 启动恢复：重建索引（已在 segment open 中完成），重放未刷盘数据。
    fn recover(&self) -> Result<()> {
        // 本实现：段打开时即加载索引与日志长度，data 全部在文件。
        // 若有未刷盘数据，可在段 open 时通过日志末尾验证。此处留空。
        Ok(())
    }

    /// 追加消息批次，自动处理 Segment 滚动。
    pub fn append(&mut self, batches: &[Bytes]) -> Result<Vec<AppendResult>> {
        let mut results = Vec::new();
        for raw in batches {
            // 可能包含多个 RecordBatch，逐个解析
            let mut reader = RecordBatchReader::new(raw.clone());
            while let Some((batch, _)) = reader.next_batch()? {
                // 检查是否需要滚动（先取 offset，再取 active segment）
                let offset = self.next_offset();
                {
                    let active = self.active_segment();
                    if !active.closed && active.should_roll() {
                        active.close()?;
                    }
                }
                // 重设 batch 的 base_offset 为当前 LEO
                let rebased = rebase_batch(&batch, offset);
                let active = self.active_segment();
                let r = active.append(&rebased)?;
                // 更新 LEO（max_offset + 1）
                self.high_watermark = active.max_offset + 1;
                results.push(r);
            }
        }
        Ok(results)
    }

    /// 读取从指定偏移量开始的批次，受 max_bytes 限制。
    pub fn read(&self, start_offset: i64, max_bytes: usize) -> Bytes {
        if start_offset < self.log_start_offset {
            return Bytes::new();
        }
        // 找到包含 start_offset 的 segment
        if let Some(seg) = self.find_segment(start_offset) {
            seg.read(start_offset, max_bytes).unwrap_or_default()
        } else {
            Bytes::new()
        }
    }

    /// 当前活跃段（最后一个）。
    fn active_segment(&mut self) -> &mut LogSegment {
        // BTreeMap 的 last 值
        let last_key = self.segments.iter().next_back().map(|(k, _)| *k).unwrap_or(0);
        if let Some(seg) = self.segments.get_mut(&last_key) {
            seg
        } else {
            // 不可能
            unreachable!()
        }
    }

    fn find_segment(&self, offset: i64) -> Option<&LogSegment> {
        // 找 base_offset <= offset 的最大段
        self.segments
            .range(..=offset)
            .next_back()
            .map(|(_, s)| s)
    }

    /// 当前日志末尾偏移量（LEO）。
    pub fn next_offset(&self) -> i64 {
        self.high_watermark
    }

    pub fn leo(&self) -> i64 {
        self.high_watermark
    }

    /// 日志起始偏移量（最旧可用偏移量，受保留清理影响）。
    pub fn log_start(&self) -> i64 {
        self.log_start_offset
    }

    /// 高水位（当前实现 LEO 即 HW，单机模式）。
    pub fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    /// 刷盘。
    pub fn flush(&mut self) -> Result<()> {
        for seg in self.segments.values_mut() {
            seg.flush_log()?;
        }
        Ok(())
    }

    /// 执行保留清理：删除过期段与超出大小限制的旧段。
    pub fn cleanup_retention(&mut self) -> Result<()> {
        if self.config.cleanup_policy != "delete" {
            return Ok(());
        }
        let retention_hours = self.config.log_retention_hours;
        let retention_bytes = self.config.log_retention_bytes;
        let total: u64 = self.segments.values().map(|s| s.size_in_bytes).sum();

        let keys: Vec<i64> = self.segments.keys().copied().collect();
        for base in &keys {
            if *base == *self.segments.iter().next_back().map(|(k, _)| k).unwrap_or(&0) {
                break; // 保留活跃段
            }
            let seg = self.segments.get(base).unwrap();
            let expired_by_time = seg.is_expired(retention_hours);
            let expired_by_size = retention_bytes >= 0 && total > retention_bytes as u64;
            if expired_by_time || expired_by_size {
                seg.delete_files()?;
                self.segments.remove(base);
                if let Some(first) = self.segments.iter().next().map(|(k, _)| *k) {
                    self.log_start_offset = first;
                }
            }
        }
        Ok(())
    }

    /// 段数量（用于测试/监控）。
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

/// 将批次的基础偏移量重设为给定 offset，并重算内部 last_offset_delta 相关的 base。
/// 这里采用重新序列化（记录偏移量不变）。
fn rebase_batch(batch: &RecordBatch, new_base_offset: i64) -> RecordBatch {
    // 重新序列化以更新 base_offset。由于 serialize 需要 Vec<Record>，直接构造。
    let mut records = batch.records.clone();
    // 调整 offset_delta 使绝对偏移量连续
    for (i, r) in records.iter_mut().enumerate() {
        r.offset_delta = i as i32;
    }
    let raw = crate::protocol::recordbatch::RecordBatch::serialize(
        new_base_offset,
        records,
        batch.compression(),
        batch.base_timestamp,
        batch.producer_id,
        batch.producer_epoch,
        batch.base_sequence,
        batch.partition_leader_epoch,
    );
    RecordBatch::parse(raw).expect("重写后批次必然合法")
}
