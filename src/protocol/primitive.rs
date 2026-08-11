//! Kafka 协议原语编解码
//!
//! Kafka 使用自定义的二进制格式。本模块提供与官方一致的编解码器，使用
//! `bytes::BytesMut` 实现零拷贝的读写。

use bytes::{BufMut, Bytes, BytesMut};

/// 统一的协议编解码错误
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("协议解码错误: {0}")]
    Decode(String),
    #[error("协议编码错误: {0}")]
    Encode(String),
    #[error("不支持的协议版本: api_key={api_key} version={version}")]
    UnsupportedVersion { api_key: i16, version: i16 },
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// 编码器包装，提供 Kafka 原语的序列化接口。
#[derive(Default)]
pub struct Encoder {
    buf: BytesMut,
}

impl Encoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(cap),
        }
    }

    pub fn into_bytes(self) -> Bytes {
        self.buf.freeze()
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn put_i8(&mut self, v: i8) {
        self.buf.put_i8(v);
    }
    pub fn put_i16(&mut self, v: i16) {
        self.buf.put_i16(v);
    }
    pub fn put_i32(&mut self, v: i32) {
        self.buf.put_i32(v);
    }
    pub fn put_i64(&mut self, v: i64) {
        self.buf.put_i64(v);
    }
    pub fn put_u32(&mut self, v: u32) {
        self.buf.put_u32(v);
    }
    pub fn put_u64(&mut self, v: u64) {
        self.buf.put_u64(v);
    }
    pub fn put_bytes(&mut self, b: &[u8]) {
        self.buf.put_slice(b);
    }

    /// 紧凑型或传统型字符串。Kafka 中 compact string 用 uvarint 长度（-1 为 null）。
    pub fn put_string(&mut self, s: &str) {
        self.put_i16(s.len() as i16);
        self.put_bytes(s.as_bytes());
    }

    pub fn put_nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => {
                self.put_i16(s.len() as i16);
                self.put_bytes(s.as_bytes());
            }
            None => self.put_i16(-1),
        }
    }

    pub fn put_compact_string(&mut self, s: &str) {
        let n = s.len() as u64;
        self.put_unsigned_varint(n + 1);
        self.put_bytes(s.as_bytes());
    }

    pub fn put_nullable_compact_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.put_compact_string(s),
            None => self.put_unsigned_varint(0),
        }
    }

    pub fn put_compact_nullable_bytes(&mut self, b: Option<&[u8]>) {
        match b {
            Some(b) => {
                self.put_unsigned_varint(b.len() as u64 + 1);
                self.put_bytes(b);
            }
            None => self.put_unsigned_varint(0),
        }
    }

    pub fn put_uvarint(&mut self, mut v: u64) {
        loop {
            let mut byte = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                byte |= 0x80;
            }
            self.put_u8(byte);
            if v == 0 {
                break;
            }
        }
    }

    pub fn put_unsigned_varint(&mut self, v: u64) {
        self.put_uvarint(v);
    }

    pub fn put_u8(&mut self, v: u8) {
        self.buf.put_u8(v);
    }

    pub fn put_array_len(&mut self, n: i32) {
        self.put_i32(n);
    }
}

/// 解码器包装，提供 Kafka 原语的解析接口。
pub struct Decoder {
    buf: Bytes,
    pos: usize,
}

impl Decoder {
    pub fn new(buf: Bytes) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&[u8]> {
        if self.remaining() < n {
            return Err(Error::Decode(format!(
                "剩余字节不足: 需要 {} 字节，实际剩余 {}",
                n,
                self.remaining()
            )));
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    pub fn get_i8(&mut self) -> Result<i8> {
        Ok(i8::from_be_bytes([self.take(1)?[0]]))
    }
    pub fn get_i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn get_i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn get_i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn get_u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn get_u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn get_bytes(&mut self, n: usize) -> Result<Bytes> {
        let slice = self.take(n)?;
        Ok(Bytes::copy_from_slice(slice))
    }

    /// 读取传统字符串（2 字节长度前缀，-1 表示 null）。
    pub fn get_string(&mut self) -> Result<String> {
        let len = self.get_i16()?;
        if len < 0 {
            return Err(Error::Decode("非法字符串长度".into()));
        }
        let s = String::from_utf8(self.take(len as usize)?.to_vec())
            .map_err(|e| Error::Decode(format!("UTF-8 错误: {e}")))?;
        Ok(s)
    }

    pub fn get_nullable_string(&mut self) -> Result<Option<String>> {
        let len = self.get_i16()?;
        if len < 0 {
            return Ok(None);
        }
        let s = String::from_utf8(self.take(len as usize)?.to_vec())
            .map_err(|e| Error::Decode(format!("UTF-8 错误: {e}")))?;
        Ok(Some(s))
    }

    pub fn get_varint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift = 0u32;
        loop {
            let byte = self.take(1)?[0];
            result |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        Ok(result)
    }

    pub fn get_unsigned_varint(&mut self) -> Result<u64> {
        self.get_varint()
    }

    pub fn get_compact_string(&mut self) -> Result<String> {
        let n = self.get_unsigned_varint()?;
        if n == 0 {
            return Err(Error::Decode("非法 compact string 长度".into()));
        }
        let len = (n - 1) as usize;
        let s = String::from_utf8(self.take(len)?.to_vec())
            .map_err(|e| Error::Decode(format!("UTF-8 错误: {e}")))?;
        Ok(s)
    }

    pub fn get_nullable_compact_string(&mut self) -> Result<Option<String>> {
        let n = self.get_unsigned_varint()?;
        if n == 0 {
            return Ok(None);
        }
        let len = (n - 1) as usize;
        let s = String::from_utf8(self.take(len)?.to_vec())
            .map_err(|e| Error::Decode(format!("UTF-8 错误: {e}")))?;
        Ok(Some(s))
    }

    pub fn get_compact_nullable_bytes(&mut self) -> Result<Option<Bytes>> {
        let n = self.get_unsigned_varint()?;
        if n == 0 {
            return Ok(None);
        }
        let len = (n - 1) as usize;
        Ok(Some(self.get_bytes(len)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_roundtrip() {
        let mut e = Encoder::new();
        e.put_string("hello");
        let bytes = e.into_bytes();
        let mut d = Decoder::new(bytes);
        assert_eq!(d.get_string().unwrap(), "hello");
    }

    #[test]
    fn test_compact_string_roundtrip() {
        let mut e = Encoder::new();
        e.put_compact_string("world");
        let bytes = e.into_bytes();
        let mut d = Decoder::new(bytes);
        assert_eq!(d.get_compact_string().unwrap(), "world");
    }

    #[test]
    fn test_nullable_string() {
        let mut e = Encoder::new();
        e.put_nullable_string(None);
        e.put_nullable_string(Some("abc"));
        let bytes = e.into_bytes();
        let mut d = Decoder::new(bytes);
        assert_eq!(d.get_nullable_string().unwrap(), None);
        assert_eq!(d.get_nullable_string().unwrap(), Some("abc".into()));
    }
}
