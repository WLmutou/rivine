//! 日志段（LogSegment）实现
//!
//! 每个 Segment 包含：
//! - .log      消息批次（RecordBatch）顺序存储
//! - .index    偏移量索引（稀疏索引）
//! - .timeindex 时间戳索引
//!
//! 生命周期：活跃段（可写）→ 滚动（达到大小/时间阈值）→ 只读段

use super::index::{OffsetIndex, TimeIndex};
use crate::protocol::RecordBatch;
use crate::storage::log::AppendResult;
use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct LogSegment {
    /// Segment 基础偏移量（第一条消息的偏移量）
    pub base_offset: i64,
    pub dir: PathBuf,
    pub log_path: PathBuf,
    pub index_path: PathBuf,
    pub timeindex_path: PathBuf,
    log_file: File,
    index: OffsetIndex,
    time_index: TimeIndex,
    /// 已写字节数（= 当前日志末尾物理位置）
    pub size_in_bytes: u64,
    /// 已刷盘字节数
    flushed: u64,
    /// 距离上一次索引条目的已写字节数
    bytes_since_last_index: u32,
    /// 该段包含的最大偏移量（最后一条消息的偏移量）
    pub max_offset: i64,
    /// 第一条消息时间戳
    pub first_timestamp: i64,
    /// 段是否已关闭（只读）
    pub closed: bool,
    /// 段创建时间（毫秒）
    created_ms: i64,
    index_interval_bytes: i32,
    log_segment_bytes: i64,
    log_roll_ms: i64,
}

impl LogSegment {
    /// 打开或创建新段。base_offset 决定文件名。
    pub fn open(
        dir: &Path,
        base_offset: i64,
        index_interval_bytes: i32,
        log_segment_bytes: i64,
        log_roll_ms: i64,
    ) -> Result<Self> {
        let name = format!("{base_offset:020}");
        let log_path = dir.join(format!("{name}.log"));
        let index_path = dir.join(format!("{name}.index"));
        let timeindex_path = dir.join(format!("{name}.timeindex"));

        let log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&log_path)?;
        let size_in_bytes = log_file.metadata()?.len();
        let index = OffsetIndex::open(&index_path)?;
        let time_index = TimeIndex::open(&timeindex_path)?;
        // 从 .log 文件末尾读取最后一批的末偏移量恢复 max_offset
        let max_offset = if index.is_empty() && size_in_bytes == 0 {
            base_offset - 1
        } else {
            read_last_batch_offset(&log_file, size_in_bytes).unwrap_or(base_offset - 1)
        };

        Ok(Self {
            base_offset,
            dir: dir.to_path_buf(),
            log_path,
            index_path,
            timeindex_path,
            log_file,
            index,
            time_index,
            size_in_bytes,
            flushed: size_in_bytes,
            bytes_since_last_index: 0,
            max_offset,
            first_timestamp: 0,
            closed: false,
            created_ms: current_ms(),
            index_interval_bytes,
            log_segment_bytes,
            log_roll_ms,
        })
    }

    /// 追加一个 RecordBatch，返回追加后的信息。
    pub fn append(&mut self, batch: &RecordBatch) -> Result<AppendResult> {
        if self.closed {
            anyhow::bail!("不能向已关闭的 segment 追加");
        }
        let append_position = self.size_in_bytes;
        let raw = &batch.raw;
        self.log_file.seek(SeekFrom::End(0))?;
        self.log_file.write_all(raw)?;
        self.size_in_bytes = append_position + raw.len() as u64;

        let last_offset = batch.last_offset();
        if last_offset > self.max_offset {
            self.max_offset = last_offset;
        }
        if self.first_timestamp == 0 {
            self.first_timestamp = batch.base_timestamp;
        }

        // 稀疏索引：每 index_interval_bytes 字节新增一个条目
        if self.index.is_empty()
            || self.bytes_since_last_index + raw.len() as u32 >= self.index_interval_bytes as u32
        {
            let rel = (batch.base_offset - self.base_offset) as i32;
            self.index.append(rel, append_position as u32)?;
            self.time_index.append(batch.base_timestamp, rel)?;
            self.bytes_since_last_index = 0;
        } else {
            self.bytes_since_last_index += raw.len() as u32;
        }

        Ok(AppendResult {
            base_offset: batch.base_offset,
            last_offset,
            log_append_time_ms: current_ms(),
            log_start_offset: self.base_offset,
        })
    }

    /// 从指定偏移量读取消息批次，受 max_bytes 限制。
    pub fn read(&self, start_offset: i64, max_bytes: usize) -> Result<Bytes> {
        if start_offset > self.max_offset {
            return Ok(Bytes::new());
        }
        let target_rel = (start_offset - self.base_offset) as i32;
        let position = self.index.lookup(target_rel).unwrap_or(0) as u64;

        let mut file = File::open(&self.log_path)?;
        file.seek(SeekFrom::Start(position))?;
        let mut buf = Vec::with_capacity(64 * 1024);
        file.read_to_end(&mut buf)?;

        let mut out = BytesMut::new();
        let mut p = 0usize;
        let mut started = false;
        while p + 12 <= buf.len() && out.len() + 12 <= max_bytes {
            let batch_start = p;
            let base_offset = i64::from_be_bytes(buf[p..p + 8].try_into().unwrap());
            let batch_length = i32::from_be_bytes(buf[p + 8..p + 12].try_into().unwrap());
            if batch_length < 0 {
                break;
            }
            let total = 12 + batch_length as usize;
            if p + total > buf.len() {
                break;
            }
            let batch_last = base_offset + batch_length as i64; // 粗略估算
            let _ = batch_last;
            if base_offset + 0 >= start_offset {
                started = true;
            }
            if started {
                if out.len() + total > max_bytes {
                    break;
                }
                out.put_slice(&buf[batch_start..batch_start + total]);
            }
            p += total;
            if out.len() >= max_bytes {
                break;
            }
        }
        Ok(out.freeze())
    }

    /// 根据时间戳查找偏移量。
    pub fn offset_for_timestamp(&self, timestamp: i64) -> Option<i64> {
        let rel = self.time_index.lookup(timestamp)?;
        Some(self.base_offset + rel as i64)
    }

    /// 判断是否需要滚动（达到大小或时间阈值）。
    pub fn should_roll(&self) -> bool {
        self.size_in_bytes >= self.log_segment_bytes as u64
            || (current_ms() - self.created_ms) >= self.log_roll_ms
    }

    /// 关闭段（刷新索引与日志）。
    pub fn close(&mut self) -> Result<()> {
        self.closed = true;
        self.log_file.flush()?;
        self.index.flush()?;
        self.time_index.flush()?;
        Ok(())
    }

    /// 仅刷盘日志（不关闭）。
    pub fn flush_log(&mut self) -> Result<()> {
        self.log_file.sync_all()?;
        self.flushed = self.size_in_bytes;
        Ok(())
    }

    /// 判断段是否在时间保留期限内。
    pub fn is_expired(&self, retention_hours: i64) -> bool {
        let mtime = std::fs::metadata(&self.log_path)
            .and_then(|m| m.modified())
            .map(|t| {
                t.duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        (current_ms() - mtime) > retention_hours * 3600 * 1000
    }

    /// 删除段文件。
    pub fn delete_files(&self) -> Result<()> {
        std::fs::remove_file(&self.log_path)?;
        let _ = std::fs::remove_file(&self.index_path);
        let _ = std::fs::remove_file(&self.timeindex_path);
        Ok(())
    }
}

fn current_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 从日志文件末尾读取最后一批的末偏移量，用于恢复 max_offset。
fn read_last_batch_offset(file: &File, size: u64) -> Option<i64> {
    if size == 0 {
        return None;
    }
    let mut f = file;
    let _ = f.seek(SeekFrom::Start(0));
    let mut last_base: Option<i64> = None;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).ok()?;
    let mut p = 0usize;
    while p + 12 <= buf.len() {
        let base = i64::from_be_bytes(buf[p..p + 8].try_into().unwrap());
        let len = i32::from_be_bytes(buf[p + 8..p + 12].try_into().unwrap());
        if len < 0 || p + 12 + len as usize > buf.len() {
            break;
        }
        last_base = Some(base);
        p += 12 + len as usize;
    }
    last_base
}

// 移除 index_entry_rel，改用内联逻辑
