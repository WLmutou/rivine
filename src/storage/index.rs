//! 偏移量索引（.index）与时间戳索引（.timeindex）实现
//!
//! - 偏移量索引是稀疏索引：条目为 <相对偏移量, 物理位置>，相对偏移量是相对于
//!   Segment 基础偏移量的差值。查找时先二分定位最近的条目，再在 .log 精确定位。
//! - 时间索引条目为 <时间戳, 相对偏移量>。

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// 单个偏移量索引条目
#[derive(Debug, Clone, Copy)]
pub struct OffsetIndexEntry {
    /// 相对偏移量（相对 Segment base offset）
    pub relative_offset: i32,
    /// 物理位置（在 .log 文件中的字节偏移）
    pub position: u32,
}

/// 偏移量索引（.index 文件），条目定长 8 字节。
pub struct OffsetIndex {
    file: File,
    entries: Vec<OffsetIndexEntry>,
}

impl OffsetIndex {
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut entries = Vec::new();
        for chunk in buf.chunks_exact(8) {
            let rel = i32::from_be_bytes(chunk[0..4].try_into().unwrap());
            let pos = u32::from_be_bytes(chunk[4..8].try_into().unwrap());
            entries.push(OffsetIndexEntry {
                relative_offset: rel,
                position: pos,
            });
        }
        Ok(Self { file, entries })
    }

    /// 追加一个索引条目。
    pub fn append(&mut self, relative_offset: i32, position: u32) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&relative_offset.to_be_bytes())?;
        self.file.write_all(&position.to_be_bytes())?;
        self.file.flush()?;
        self.entries.push(OffsetIndexEntry {
            relative_offset,
            position,
        });
        Ok(())
    }

    /// 二分查找小于等于目标偏移量的最近条目（含小于目标的最大条目）。
    /// 返回该条目的物理位置。
    pub fn lookup(&self, target_relative: i32) -> Option<u32> {
        if self.entries.is_empty() {
            return None;
        }
        // 二分查找最大的 <= target
        let mut lo = 0usize;
        let mut hi = self.entries.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.entries[mid].relative_offset <= target_relative {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            return None;
        }
        Some(self.entries[lo - 1].position)
    }

    /// 返回最后一个条目的位置（用于确定日志末尾物理位置）。
    pub fn last_position(&self) -> u32 {
        self.entries.last().map(|e| e.position).unwrap_or(0)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    /// 截断索引，保留物理位置小于给定值的条目。
    pub fn truncate_to(&mut self, max_position: u32) -> io::Result<()> {
        let keep: Vec<OffsetIndexEntry> = self
            .entries
            .iter()
            .copied()
            .filter(|e| e.position < max_position)
            .collect();
        self.entries = keep;
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        for e in &self.entries {
            self.file.write_all(&e.relative_offset.to_be_bytes())?;
            self.file.write_all(&e.position.to_be_bytes())?;
        }
        self.file.flush()?;
        Ok(())
    }
}

/// 时间戳索引条目
#[derive(Debug, Clone, Copy)]
pub struct TimeIndexEntry {
    pub timestamp: i64,
    /// 相对偏移量
    pub relative_offset: i32,
}

/// 时间戳索引（.timeindex 文件），条目定长 12 字节。
pub struct TimeIndex {
    file: File,
    entries: Vec<TimeIndexEntry>,
}

impl TimeIndex {
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut entries = Vec::new();
        for chunk in buf.chunks_exact(12) {
            let ts = i64::from_be_bytes(chunk[0..8].try_into().unwrap());
            let rel = i32::from_be_bytes(chunk[8..12].try_into().unwrap());
            entries.push(TimeIndexEntry {
                timestamp: ts,
                relative_offset: rel,
            });
        }
        Ok(Self { file, entries })
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    pub fn append(&mut self, timestamp: i64, relative_offset: i32) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&timestamp.to_be_bytes())?;
        self.file.write_all(&relative_offset.to_be_bytes())?;
        self.file.flush()?;
        self.entries.push(TimeIndexEntry {
            timestamp,
            relative_offset,
        });
        Ok(())
    }

    /// 查找时间戳 >= 目标的最早条目，返回其相对偏移量。
    pub fn lookup(&self, target_ts: i64) -> Option<i32> {
        if self.entries.is_empty() {
            return None;
        }
        let mut lo = 0usize;
        let mut hi = self.entries.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.entries[mid].timestamp < target_ts {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == self.entries.len() {
            None
        } else {
            Some(self.entries[lo].relative_offset)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_index_binary_search() {
        let dir = std::env::temp_dir().join("rivine-index-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("00000000000000000000.index");
        let mut idx = OffsetIndex::open(&path).unwrap();
        for (rel, pos) in [(0u32, 0u32), (10, 100), (20, 200), (30, 300)] {
            idx.append(rel as i32, pos).unwrap();
        }
        assert_eq!(idx.lookup(5).unwrap(), 0);
        assert_eq!(idx.lookup(15).unwrap(), 100);
        assert_eq!(idx.lookup(30).unwrap(), 300);
        assert_eq!(idx.lookup(35).unwrap(), 300);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
