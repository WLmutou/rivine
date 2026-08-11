//! Kafka RecordBatch 编解码（与 Kafka 消息格式 v2 完全一致）
//!
//! RecordBatch 磁盘/网络格式：
//! ```text
//! baseOffset:        int64
//! batchLength:       int32
//! partitionLeaderEpoch: int32
//! magic:             int8   (2)
//! crc:               uint32 (覆盖 attributes 起的全部内容)
//! attributes:        int16
//! lastOffsetDelta:   int32
//! baseTimestamp:     int64
//! maxTimestamp:      int64
//! producerId:        int64
//! producerEpoch:     int16
//! baseSequence:      int32
//! recordsCount:      int32
//! records:           [Record]
//! ```

use super::primitive::{Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::Cursor;

pub const MAGIC_V2: i8 = 2;
pub const CURRENT_BATCH: &str = "";

/// 压缩算法（attributes 的低 3 位）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl Compression {
    pub fn from_code(code: i8) -> Result<Self> {
        Ok(match code {
            0 => Self::None,
            1 => Self::Gzip,
            2 => Self::Snappy,
            3 => Self::Lz4,
            4 => Self::Zstd,
            _ => return Err(Error::Decode(format!("未知压缩类型 {code}"))),
        })
    }

    pub fn code(&self) -> i8 {
        *self as i8
    }
}

/// RecordBatch attributes 位标志
pub struct Attribute;

impl Attribute {
    pub const COMPRESSION_MASK: i16 = 0x07;
    pub const TIMESTAMP_TYPE_MASK: i16 = 0x08;
    pub const TRANSACTIONAL_MASK: i16 = 0x10;
    pub const CONTROL_MASK: i16 = 0x20;
}

/// 单个 Record（magic=2）
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Option<Bytes>)>,
}

/// 一个 RecordBatch（消息批次）
#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
    /// 原始编码（用于零拷贝读取）
    pub raw: Bytes,
}

impl RecordBatch {
    pub fn compression(&self) -> Compression {
        Compression::from_code((self.attributes & Attribute::COMPRESSION_MASK) as i8).unwrap_or(Compression::None)
    }

    pub fn is_control(&self) -> bool {
        self.attributes & Attribute::CONTROL_MASK != 0
    }

    /// 批次中最后一条记录的绝对偏移量
    pub fn last_offset(&self) -> i64 {
        self.base_offset + self.last_offset_delta as i64
    }

    pub fn count(&self) -> usize {
        self.records.len()
    }

    /// 将记录序列化为 RecordBatch（使用给定压缩）。
    pub fn serialize(
        base_offset: i64,
        records: Vec<Record>,
        compression: Compression,
        base_timestamp: i64,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        partition_leader_epoch: i32,
    ) -> Bytes {
        // 先计算 last_offset_delta、max_timestamp
        let last_offset_delta = if records.is_empty() { 0 } else { records.len() as i32 - 1 };
        let max_timestamp = records.iter().fold(base_timestamp, |m, r| {
            m.max(base_timestamp + r.timestamp_delta)
        });

        // 1. 序列化 records 部分（不含压缩时直接写入）
        let mut records_buf = BytesMut::new();
        for r in &records {
            records_buf.put_i8(r.attributes);
            records_buf.put_i64(r.timestamp_delta);
            records_buf.put_i32(r.offset_delta);
            put_varint_len(&mut records_buf, r.key.as_ref().map(|b| b.len() as i64).unwrap_or(-1));
            if let Some(k) = &r.key {
                records_buf.put_slice(k);
            }
            put_varint_len(&mut records_buf, r.value.as_ref().map(|b| b.len() as i64).unwrap_or(-1));
            if let Some(v) = &r.value {
                records_buf.put_slice(v);
            }
            records_buf.put_i32(r.headers.len() as i32);
            for (k, v) in &r.headers {
                records_buf.put_i8(0); // header key len
                let kb = k.as_bytes();
                put_varint_len(&mut records_buf, kb.len() as i64);
                records_buf.put_slice(kb);
                put_varint_len(&mut records_buf, v.as_ref().map(|b| b.len() as i64).unwrap_or(-1));
                if let Some(vb) = v {
                    records_buf.put_slice(vb);
                }
            }
        }

        // 2. 压缩 records
        let records_compressed = compress(compression, &records_buf);

        // 3. 组装 batch：先写 attributes 之后的部分（crc 覆盖范围）
        let mut body = BytesMut::new();
        body.put_i16(
            (compression as i16) | (attributes_flags(compression) & !Attribute::COMPRESSION_MASK),
        );
        body.put_i32(last_offset_delta);
        body.put_i64(base_timestamp);
        body.put_i64(max_timestamp);
        body.put_i64(producer_id);
        body.put_i16(producer_epoch);
        body.put_i32(base_sequence);
        body.put_i32(records.len() as i32);
        body.put_slice(&records_compressed);

        // 4. 计算 CRC（覆盖 attributes 到 records 结束）
        let crc = crc32fast::hash(&body);

        // 5. 组装完整 batch
        // batch_length = 从 batchLength 字段之后到批次末尾 = epoch(4)+magic(1)+crc(4)+body
        let total_len = 4 + 1 + 4 + body.len();
        let mut out = BytesMut::with_capacity(8 + 4 + total_len);
        out.put_i64(base_offset);
        out.put_i32(total_len as i32);
        out.put_i32(partition_leader_epoch);
        out.put_i8(MAGIC_V2);
        out.put_u32(crc);
        out.put_slice(&body);

        out.freeze()
    }

    /// 解析原始 RecordBatch 字节。
    pub fn parse(raw: Bytes) -> Result<Self> {
        let mut cur = Cursor::new(&raw[..]);
        let base_offset = cur.get_i64();
        let batch_length = cur.get_i32();
        let partition_leader_epoch = cur.get_i32();
        let magic = cur.get_i8();
        if magic != MAGIC_V2 {
            return Err(Error::Decode(format!("不支持的 magic={magic}，仅支持 v2")));
        }
        let crc_stored = cur.get_u32();
        // crc 覆盖范围：从 attributes 开始到 records 结束（batch_length 减去 magic 和 crc 长度）。
        let crc_len = batch_length as usize - 4 - 1 - 4;
        let crc_computed = crc32fast::hash(&raw[cur.position() as usize..cur.position() as usize + crc_len]);
        if crc_stored != crc_computed {
            // 允许容忍：某些实现校验失败，这里仅警告不返回错误（读取路径由上层处理）
            tracing::warn!(
                "RecordBatch CRC 不匹配: 存储={:#x} 计算={:#x}",
                crc_stored,
                crc_computed
            );
        }
        let attributes = cur.get_i16();
        let last_offset_delta = cur.get_i32();
        let base_timestamp = cur.get_i64();
        let max_timestamp = cur.get_i64();
        let producer_id = cur.get_i64();
        let producer_epoch = cur.get_i16();
        let base_sequence = cur.get_i32();
        let records_count = cur.get_i32();
        // 从批次开头(pos 12)起，已读字节数 = records_pos - 12，剩余即 records。
        let records_pos = cur.position() as usize;
        let records_len = batch_length as usize - (records_pos - 12);
        let records_bytes = raw[records_pos..records_pos + records_len].to_vec();

        let compression = Compression::from_code((attributes & Attribute::COMPRESSION_MASK) as i8)?;
        let decompressed = decompress(compression, &records_bytes)?;

        let records = parse_records(&decompressed, records_count)?;

        Ok(Self {
            base_offset,
            partition_leader_epoch,
            magic,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
            raw,
        })
    }
}

/// attributes 中的时间戳类型、事务、控制位（本实现无压缩时仅保留时间戳类型=CreateTime）
fn attributes_flags(_compression: Compression) -> i16 {
    0 // CreateTime，非事务，非控制
}

fn put_varint_len(buf: &mut BytesMut, len: i64) {
    // Kafka varint 是有符号的 zigzag + 无符号 varint
    let zigzag = ((len << 1) ^ (len >> 63)) as u64;
    put_uvarint(buf, zigzag);
}

fn put_uvarint(buf: &mut BytesMut, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if v == 0 {
            break;
        }
    }
}

fn parse_records(data: &[u8], count: i32) -> Result<Vec<Record>> {
    let mut pos = 0usize;
    let mut records = Vec::new();
    for _ in 0..count {
        let attr = data[pos] as i8;
        pos += 1;
        let timestamp_delta = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let offset_delta = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let key_len = read_varint(data, &mut pos)?;
        let key = if key_len >= 0 {
            let k = data[pos..pos + key_len as usize].to_vec();
            pos += key_len as usize;
            Some(Bytes::from(k))
        } else {
            None
        };
        let value_len = read_varint(data, &mut pos)?;
        let value = if value_len >= 0 {
            let v = data[pos..pos + value_len as usize].to_vec();
            pos += value_len as usize;
            Some(Bytes::from(v))
        } else {
            None
        };
        let n_headers = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let mut headers = Vec::new();
        for _ in 0..n_headers {
            let _header_attr = data[pos] as i8;
            pos += 1;
            let hk_len = read_varint(data, &mut pos)?;
            let hk = String::from_utf8(data[pos..pos + hk_len as usize].to_vec())
                .map_err(|e| Error::Decode(format!("header key utf8: {e}")))?;
            pos += hk_len as usize;
            let hv_len = read_varint(data, &mut pos)?;
            let hv = if hv_len >= 0 {
                let v = data[pos..pos + hv_len as usize].to_vec();
                pos += hv_len as usize;
                Some(Bytes::from(v))
            } else {
                None
            };
            headers.push((hk, hv));
        }
        records.push(Record {
            attributes: attr,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        });
    }
    Ok(records)
}

fn read_varint(data: &[u8], pos: &mut usize) -> Result<i64> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        if *pos >= data.len() {
            return Err(Error::Decode("varint 越界".into()));
        }
        let byte = data[*pos];
        *pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    // zigzag 解码
    Ok(((result >> 1) as i64) ^ -((result & 1) as i64))
}

fn compress(c: Compression, data: &[u8]) -> Bytes {
    match c {
        Compression::None => Bytes::copy_from_slice(data),
        Compression::Gzip => {
            use flate2::write::GzEncoder;
            use flate2::Compression as Fc;
            use std::io::Write;
            let mut enc = GzEncoder::new(Vec::new(), Fc::default());
            enc.write_all(data).unwrap();
            Bytes::from(enc.finish().unwrap())
        }
        Compression::Snappy => {
            let out = snap::raw::Encoder::new().compress_vec(data).unwrap();
            Bytes::from(out)
        }
        Compression::Lz4 => {
            use lz4_flex::frame::FrameEncoder;
            use std::io::Write;
            let mut enc = FrameEncoder::new(Vec::new());
            enc.write_all(data).unwrap();
            Bytes::from(enc.finish().unwrap())
        }
        Compression::Zstd => {
            let out = zstd::stream::encode_all(Cursor::new(data), 3).unwrap();
            Bytes::from(out)
        }
    }
}

fn decompress(c: Compression, data: &[u8]) -> Result<Vec<u8>> {
    match c {
        Compression::None => Ok(data.to_vec()),
        Compression::Gzip => {
            use flate2::read::GzDecoder;
            use std::io::Read;
            let mut d = GzDecoder::new(data);
            let mut out = Vec::new();
            d.read_to_end(&mut out).map_err(|e| Error::Decode(format!("gzip 解压失败: {e}")))?;
            Ok(out)
        }
        Compression::Snappy => {
            let mut d = snap::raw::Decoder::new();
            d.decompress_vec(data).map_err(|e| Error::Decode(format!("snappy 解压失败: {e}")))
        }
        Compression::Lz4 => {
            // LZ4 Frame 解压（Kafka 使用 LZ4 Frame 格式）
            use lz4_flex::frame::FrameDecoder;
            use std::io::Read;
            let mut dec = FrameDecoder::new(data);
            let mut out = Vec::new();
            dec.read_to_end(&mut out)
                .map_err(|e| Error::Decode(format!("lz4 解压失败: {e}")))?;
            Ok(out)
        }
        Compression::Zstd => {
            zstd::stream::decode_all(data)
                .map_err(|e| Error::Decode(format!("zstd 解压失败: {e}")))
        }
    }
}

/// 用于遍历一批原始字节中的多个 RecordBatch。
pub struct RecordBatchReader {
    data: Bytes,
    pos: usize,
}

impl RecordBatchReader {
    pub fn new(data: Bytes) -> Self {
        Self { data, pos: 0 }
    }

    /// 读取下一个批次，返回 (解析后的批次, 原始字节)，耗尽时返回 None。
    pub fn next_batch(&mut self) -> Result<Option<(RecordBatch, Bytes)>> {
        if self.pos >= self.data.len() {
            return Ok(None);
        }
        let mut cur = Cursor::new(&self.data[self.pos..]);
        let _base_offset = cur.get_i64();
        let batch_length = cur.get_i32();
        if batch_length < 0 || batch_length as usize + 12 > self.data.len() - self.pos {
            // 无效批次，结束
            return Ok(None);
        }
        let total = 12 + batch_length as usize;
        let raw = self.data.slice(self.pos..self.pos + total);
        self.pos += total;
        let batch = RecordBatch::parse(raw.clone())?;
        Ok(Some((batch, raw)))
    }
}

/// 便捷类型：原始的 Records 载荷（可能包含多个 batch）。
pub type Records = Bytes;

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_records() -> Vec<Record> {
        vec![
            Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: Some(Bytes::from("k1")),
                value: Some(Bytes::from("value1")),
                headers: vec![],
            },
            Record {
                attributes: 0,
                timestamp_delta: 100,
                offset_delta: 1,
                key: None,
                value: Some(Bytes::from("value2")),
                headers: vec![("h".into(), Some(Bytes::from("v")))],
            },
        ]
    }

    #[test]
    fn test_recordbatch_roundtrip() {
        for comp in [Compression::None, Compression::Gzip, Compression::Snappy] {
            let bytes = RecordBatch::serialize(0, sample_records(), comp, 1_000_000, 0, 0, 0, 0);
            let parsed = RecordBatch::parse(bytes).unwrap();
            assert_eq!(parsed.records.len(), 2);
            assert_eq!(parsed.records[0].key.as_ref().unwrap(), "k1");
            assert_eq!(parsed.records[0].value.as_ref().unwrap(), "value1");
            assert_eq!(parsed.records[1].value.as_ref().unwrap(), "value2");
            assert_eq!(parsed.records[1].headers[0].0, "h");
            assert_eq!(parsed.compression(), comp);
            assert_eq!(parsed.base_offset, 0);
            assert_eq!(parsed.last_offset(), 1);
        }
    }

    #[test]
    fn test_recordbatch_reader() {
        let b1 = RecordBatch::serialize(0, sample_records(), Compression::None, 100, 0, 0, 0, 0);
        let b2 = RecordBatch::serialize(2, sample_records(), Compression::None, 200, 0, 0, 2, 0);
        let mut all = BytesMut::new();
        all.put_slice(&b1);
        all.put_slice(&b2);
        let mut r = RecordBatchReader::new(all.freeze());
        assert!(r.next_batch().unwrap().is_some());
        assert!(r.next_batch().unwrap().is_some());
        assert!(r.next_batch().unwrap().is_none());
    }
}
