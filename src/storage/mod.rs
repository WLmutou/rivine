//! 存储引擎
//!
//! 实现了分段（Segment）存储、稀疏索引（.index）、时间戳索引（.timeindex）、
//! 数据保留与清理策略，以及顺序写 / 零拷贝读优化。

pub mod segment;
pub mod log;
pub mod index;
pub mod compaction;

pub use log::PartitionLog;
pub use segment::LogSegment;
