//! 日志压缩（Log Compaction）实现
//!
//! cleanup.policy=compact 时，后台线程扫描日志段，只保留每个 Key 的最新值，
//! 删除旧值。用于事件溯源 / Key-Value 状态存储场景。

use crate::protocol::RecordBatchReader;
use anyhow::Result;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

/// 压缩器：接收一个分区的所有批次，输出压缩后的记录。
pub struct LogCompactor;

impl LogCompactor {
    /// 对一段原始批次数据进行压缩：保留每个 key 的最后一条记录。
    ///
    /// 返回压缩后的记录列表（不含 key 的记录默认保留）。
    pub fn compact_batches(data: Bytes) -> Result<Vec<crate::protocol::Record>> {
        let mut reader = RecordBatchReader::new(data);
        // key -> 最新 Record
        let mut latest: HashMap<Bytes, (i64, crate::protocol::Record)> = HashMap::new();
        let mut no_key_records: Vec<crate::protocol::Record> = Vec::new();
        let mut next_offset = 0i64;

        while let Some((batch, _)) = reader.next_batch()? {
            for record in batch.records {
                if let Some(key) = &record.key {
                    latest.insert(key.clone(), (next_offset, record));
                } else {
                    no_key_records.push(record);
                }
                next_offset += 1;
            }
        }

        // 按偏移量排序 key 记录，得到顺序输出
        let mut key_records: Vec<(i64, crate::protocol::Record)> =
            latest.into_values().collect();
        key_records.sort_by_key(|(off, _)| *off);

        let mut out: Vec<crate::protocol::Record> = Vec::new();
        for (_, r) in key_records {
            out.push(r);
        }
        out.extend(no_key_records);
        Ok(out)
    }
}

/// 后台压缩任务的状态（供 LogManager 驱动）。
pub struct CompactWorker {
    _inner: Arc<Mutex<()>>,
}

impl CompactWorker {
    pub fn new() -> Self {
        Self {
            _inner: Arc::new(Mutex::new(())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::recordbatch::{Compression, Record, RecordBatch};
    use bytes::Bytes;

    #[test]
    fn test_compaction_keeps_latest() {
        let records = vec![
            Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: Some(Bytes::from("a")),
                value: Some(Bytes::from("1")),
                headers: vec![],
            },
            Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 1,
                key: Some(Bytes::from("b")),
                value: Some(Bytes::from("2")),
                headers: vec![],
            },
            Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 2,
                key: Some(Bytes::from("a")),
                value: Some(Bytes::from("3")),
                headers: vec![],
            },
        ];
        let raw = RecordBatch::serialize(0, records, Compression::None, 100, 0, 0, 0, 0);
        let compacted = LogCompactor::compact_batches(raw).unwrap();
        // a -> 3, b -> 2
        let vals: Vec<Option<Bytes>> = compacted
            .iter()
            .map(|r| r.value.clone())
            .collect();
        assert!(vals.contains(&Some(Bytes::from("3"))));
        assert!(vals.contains(&Some(Bytes::from("2"))));
        assert!(!vals.contains(&Some(Bytes::from("1"))));
    }
}
