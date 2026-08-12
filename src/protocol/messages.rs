//! 具体请求/响应消息结构体与编解码（与 Kafka 官方协议一致）

use super::primitive::{Decoder, Encoder};
use super::{read_tagged_fields, write_tagged_fields, Result};
use bytes::Bytes;

// ============================ ApiVersions ============================

#[derive(Debug, Clone)]
pub struct ApiVersionsRequest {
    pub client_software_name: Option<String>,
    pub client_software_version: Option<String>,
}

impl ApiVersionsRequest {
    pub fn decode(buf: &mut Decoder, version: i16) -> Result<Self> {
        let client_software_name = if version >= 3 {
            buf.get_nullable_compact_string()?
        } else {
            None
        };
        let client_software_version = if version >= 3 {
            buf.get_nullable_compact_string()?
        } else {
            None
        };
        Ok(Self {
            client_software_name,
            client_software_version,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug, Clone)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiVersion>,
    pub throttle_time_ms: i32,
}

impl ApiVersionsResponse {
    pub fn encode(&self, e: &mut Encoder, version: i16) {
        e.put_i16(self.error_code);
        if version >= 3 {
            e.put_unsigned_varint(self.api_keys.len() as u64 + 1);
            for k in &self.api_keys {
                e.put_i16(k.api_key);
                e.put_i16(k.min_version);
                e.put_i16(k.max_version);
                write_tagged_fields(e);
            }
        } else {
            e.put_i32(self.api_keys.len() as i32);
            for k in &self.api_keys {
                e.put_i16(k.api_key);
                e.put_i16(k.min_version);
                e.put_i16(k.max_version);
            }
        }
        e.put_i32(self.throttle_time_ms);
        if version >= 3 {
            write_tagged_fields(e);
        }
    }
}

// ============================ Metadata ============================

#[derive(Debug, Clone)]
pub struct MetadataRequest {
    pub topics: Vec<String>,
    /// 主题不存在时是否允许 broker 自动创建（v4+）
    pub allow_auto_topic_creation: bool,
}

impl MetadataRequest {
    pub fn decode(buf: &mut Decoder, version: i16) -> Result<Self> {
        let topics = if version >= 9 {
            let n = buf.get_unsigned_varint()? as usize;
            let mut topics = Vec::new();
            for _ in 0..n {
                // 每个 topic 是 compact nullable string + tagged fields
                if let Some(t) = buf.get_nullable_compact_string()? {
                    topics.push(t);
                }
                let _ = read_tagged_fields(buf)?;
            }
            topics
        } else {
            let n = buf.get_i32()?;
            if n < 0 {
                Vec::new() // null 表示所有主题
            } else {
                let mut topics = Vec::new();
                for _ in 0..n {
                    topics.push(buf.get_string()?);
                }
                topics
            }
        };
        // v4 起有 allow_auto_topic_creation（bool），v8 起另有 include_cluster/topic
        // authorized_operations（bool）。仅解析 v4-v7 的 allow_auto_topic_creation。
        let allow_auto_topic_creation = if (4..=7).contains(&version) {
            buf.get_i8()? != 0
        } else {
            false
        };
        Ok(Self {
            topics,
            allow_auto_topic_creation,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone)]
pub struct MetadataResponse {
    pub brokers: Vec<BrokerMetadata>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
}

impl MetadataResponse {
    pub fn encode(&self, e: &mut Encoder, version: i16) {
        // throttle_time_ms (v3+)
        if version >= 3 {
            e.put_i32(0);
        }
        // brokers
        e.put_i32(self.brokers.len() as i32);
        for b in &self.brokers {
            e.put_i32(b.node_id);
            e.put_string(&b.host);
            e.put_i32(b.port);
            if version >= 1 {
                // rack
                e.put_nullable_string(None);
            }
        }
        // cluster_id (v2+)
        if version >= 2 {
            e.put_nullable_string(self.cluster_id.as_deref());
        }
        // controller_id (v1+)
        if version >= 1 {
            e.put_i32(self.controller_id);
        }
        // topics
        e.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            e.put_i16(t.error_code);
            e.put_string(&t.name);
            if version >= 1 {
                e.put_i8(t.is_internal as i8);
            }
            e.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                e.put_i16(p.error_code);
                e.put_i32(p.partition_index);
                e.put_i32(p.leader_id);
                e.put_i32(p.replica_nodes.len() as i32);
                for r in &p.replica_nodes {
                    e.put_i32(*r);
                }
                e.put_i32(p.isr_nodes.len() as i32);
                for r in &p.isr_nodes {
                    e.put_i32(*r);
                }
                if version >= 5 {
                    e.put_i32(0); // offline_replicas
                }
            }
        }
    }
}

// ============================ Produce ============================

#[derive(Debug, Clone)]
pub struct ProducePartitionData {
    pub partition_index: i32,
    pub records: Bytes, // 原始 RecordBatch 数据
}

#[derive(Debug, Clone)]
pub struct ProduceTopicData {
    pub topic: String,
    pub partitions: Vec<ProducePartitionData>,
}

#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: Vec<ProduceTopicData>,
}

impl ProduceRequest {
    pub fn decode(buf: &mut Decoder, version: i16) -> Result<Self> {
        // transactional_id 字段：v3-v8 为传统 nullable_string，v9+（flexible）为 compact。
        // 本 Broker 支持到 v3，按传统格式解析。
        let transactional_id = if version >= 9 {
            buf.get_nullable_compact_string()?
        } else if version >= 3 {
            buf.get_nullable_string()?
        } else {
            None
        };
        let acks = buf.get_i16()?;
        let timeout_ms = buf.get_i32()?;
        // 注意：幂等生产者所需 producer_id/producer_epoch/base_sequence 位于
        // RecordBatch 内部（消息格式 v2），不在 Produce 请求顶层。
        let n_topics = if version >= 9 {
            buf.get_unsigned_varint()? as usize
        } else {
            buf.get_i32()? as usize
        };
        let mut topic_data = Vec::with_capacity(n_topics);
        for _ in 0..n_topics {
            let topic = if version >= 9 {
                buf.get_compact_string()?
            } else {
                buf.get_string()?
            };
            let n_part = if version >= 9 {
                buf.get_unsigned_varint()? as usize
            } else {
                buf.get_i32()? as usize
            };
            let mut partitions = Vec::with_capacity(n_part);
            for _ in 0..n_part {
                let partition_index = buf.get_i32()?;
                let records = if version >= 9 {
                    // compact bytes
                    let len = buf.get_unsigned_varint()? as usize;
                    buf.get_bytes(len.saturating_sub(1))?
                } else {
                    let len = buf.get_i32()?;
                    buf.get_bytes(len as usize)?
                };
                partitions.push(ProducePartitionData {
                    partition_index,
                    records,
                });
            }
            topic_data.push(ProduceTopicData { topic, partitions });
        }
        Ok(Self {
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ProduceResponsePartition {
    pub partition_index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
}

#[derive(Debug, Clone)]
pub struct ProduceResponseTopic {
    pub topic: String,
    pub partitions: Vec<ProduceResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct ProduceResponse {
    pub topics: Vec<ProduceResponseTopic>,
    pub throttle_time_ms: i32,
}

impl ProduceResponse {
    pub fn encode(&self, e: &mut Encoder, version: i16) {
        e.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            e.put_string(&t.topic);
            e.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                e.put_i32(p.partition_index);
                e.put_i16(p.error_code);
                e.put_i64(p.base_offset);
                if version >= 2 {
                    e.put_i64(p.log_append_time_ms);
                }
                if version >= 5 {
                    e.put_i64(p.log_start_offset);
                }
            }
        }
        if version >= 1 {
            e.put_i32(self.throttle_time_ms);
        }
    }
}

// ============================ Fetch ============================

#[derive(Debug, Clone)]
pub struct FetchPartition {
    pub partition_index: i32,
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    pub log_start_offset: i64,
    pub partition_max_bytes: i32,
}

#[derive(Debug, Clone)]
pub struct FetchTopic {
    pub topic: String,
    pub partitions: Vec<FetchPartition>,
}

#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub topics: Vec<FetchTopic>,
}

impl FetchRequest {
    pub fn decode(buf: &mut Decoder, version: i16) -> Result<Self> {
        let replica_id = if version >= 15 {
            // 紧凑格式 i32
            let _n = buf.get_unsigned_varint()?;
            i32::from_be_bytes(buf.get_bytes(4)?.to_vec().try_into().unwrap())
        } else {
            buf.get_i32()?
        };
        let _ = &replica_id;
        let max_wait_ms = buf.get_i32()?;
        let min_bytes = buf.get_i32()?;
        let max_bytes = if version >= 3 { buf.get_i32()? } else { 0 };
        let isolation_level = if version >= 4 { buf.get_i8()? } else { 0 };
        if version >= 7 {
            let _session_id = buf.get_i32()?;
            let _session_epoch = buf.get_i32()?;
        }
        let n_topics = if version >= 15 {
            buf.get_unsigned_varint()? as usize
        } else {
            buf.get_i32()? as usize
        };
        let mut topics = Vec::with_capacity(n_topics);
        for _ in 0..n_topics {
            let topic = if version >= 15 {
                buf.get_compact_string()?
            } else {
                buf.get_string()?
            };
            let n_part = if version >= 15 {
                buf.get_unsigned_varint()? as usize
            } else {
                buf.get_i32()? as usize
            };
            let mut partitions = Vec::with_capacity(n_part);
            for _ in 0..n_part {
                let partition_index = buf.get_i32()?;
                let current_leader_epoch = if version >= 9 { buf.get_i32()? } else { -1 };
                let fetch_offset = buf.get_i64()?;
                let log_start_offset = if version >= 5 { buf.get_i64()? } else { -1 };
                let partition_max_bytes = buf.get_i32()?;
                partitions.push(FetchPartition {
                    partition_index,
                    current_leader_epoch,
                    fetch_offset,
                    log_start_offset,
                    partition_max_bytes,
                });
            }
            topics.push(FetchTopic { topic, partitions });
        }
        Ok(Self {
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            topics,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
}

#[derive(Debug, Clone)]
pub struct FetchResponsePartition {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub aborted_transactions: Vec<AbortedTransaction>,
    pub records: Bytes,
}

#[derive(Debug, Clone)]
pub struct FetchResponseTopic {
    pub topic: String,
    pub partitions: Vec<FetchResponsePartition>,
}

#[derive(Debug, Clone)]
pub struct FetchResponse {
    pub throttle_time_ms: i32,
    pub topics: Vec<FetchResponseTopic>,
}

impl FetchResponse {
    pub fn encode(&self, e: &mut Encoder, version: i16) {
        e.put_i32(self.throttle_time_ms);
        e.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            e.put_string(&t.topic);
            e.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                e.put_i32(p.partition_index);
                e.put_i16(p.error_code);
                e.put_i64(p.high_watermark);
                if version >= 4 {
                    e.put_i64(p.last_stable_offset);
                }
                if version >= 5 {
                    e.put_i64(p.log_start_offset);
                }
                if version >= 4 {
                    e.put_i32(p.aborted_transactions.len() as i32);
                    for a in &p.aborted_transactions {
                        e.put_i64(a.producer_id);
                        e.put_i64(a.first_offset);
                    }
                }
                e.put_i32(p.records.len() as i32);
                e.put_bytes(&p.records);
            }
        }
    }
}

// ============================ ListOffsets ============================

#[derive(Debug, Clone)]
pub struct ListOffsetsRequest {
    pub replica_id: i32,
    pub topics: Vec<(String, Vec<(i32, i8, i64, i32)>)>, // topic -> (partition, timestamp, max_num)
}

impl ListOffsetsRequest {
    pub fn decode(buf: &mut Decoder, version: i16) -> Result<Self> {
        let replica_id = buf.get_i32()?;
        // v2+ 引入 isolation_level(int8)
        if version >= 2 {
            let _isolation = buf.get_i8()?;
        }
        let n_topics = buf.get_i32()?;
        let mut topics = Vec::new();
        for _ in 0..n_topics {
            let topic = buf.get_string()?;
            let n_part = buf.get_i32()?;
            let mut parts = Vec::new();
            for _ in 0..n_part {
                let partition = buf.get_i32()?;
                // v0：partition_index + timestamp + max_num_offsets
                // v1-v3：partition_index + timestamp
                // v4+：partition_index + current_leader_epoch + timestamp
                if version == 0 {
                    let timestamp = buf.get_i64()?;
                    let max_num = buf.get_i32()?;
                    parts.push((partition, 0i8, timestamp, max_num));
                } else {
                    if version >= 4 {
                        let _current_leader_epoch = buf.get_i32()?;
                    }
                    let timestamp = buf.get_i64()?;
                    parts.push((partition, 0i8, timestamp, 1));
                }
            }
            topics.push((topic, parts));
        }
        Ok(Self {
            replica_id,
            topics,
        })
    }
}

// ============================ 消费者组 / Offset ============================

#[derive(Debug, Clone)]
pub struct FindCoordinatorRequest {
    pub key: String,
}

impl FindCoordinatorRequest {
    pub fn decode(buf: &mut Decoder, _version: i16) -> Result<Self> {
        let key = buf.get_string()?;
        Ok(Self { key })
    }
}

#[derive(Debug, Clone)]
pub struct FindCoordinatorResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

impl FindCoordinatorResponse {
    pub fn encode(&self, e: &mut Encoder, version: i16) {
        e.put_i32(self.throttle_time_ms);
        if version >= 1 {
            e.put_i16(self.error_code);
            e.put_nullable_string(self.error_message.as_deref());
        }
        e.put_i32(self.node_id);
        e.put_string(&self.host);
        e.put_i32(self.port);
    }
}

#[derive(Debug, Clone)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: String,
    pub protocol_type: String,
    pub group_protocols: Vec<(String, Bytes)>, // name -> metadata
}

impl JoinGroupRequest {
    pub fn decode(buf: &mut Decoder, version: i16) -> Result<Self> {
        let group_id = buf.get_string()?;
        let session_timeout_ms = buf.get_i32()?;
        let rebalance_timeout_ms = if version >= 1 { buf.get_i32()? } else { session_timeout_ms };
        let member_id = buf.get_string()?;
        if version >= 5 {
            let _group_instance_id = buf.get_nullable_string()?;
        }
        let protocol_type = buf.get_string()?;
        let n_prot = buf.get_i32()?;
        let mut group_protocols = Vec::new();
        for _ in 0..n_prot {
            let name = buf.get_string()?;
            let len = buf.get_i32()?;
            let meta = buf.get_bytes(len as usize)?;
            group_protocols.push((name, meta));
        }
        Ok(Self {
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            protocol_type,
            group_protocols,
        })
    }
}

#[derive(Debug, Clone)]
pub struct JoinGroupResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<(String, Bytes)>, // member_id -> metadata
}

impl JoinGroupResponse {
    pub fn encode(&self, e: &mut Encoder, version: i16) {
        e.put_i32(self.throttle_time_ms);
        e.put_i16(self.error_code);
        e.put_i32(self.generation_id);
        // ProtocolType 字段 v7+ 才存在；v0-6 响应没有该字段。
        if version >= 7 {
            e.put_nullable_compact_string(self.protocol_type.as_deref());
            e.put_nullable_compact_string(self.protocol_name.as_deref());
        } else {
            // ProtocolName 字段 v0+ 存在（传统格式）
            e.put_nullable_string(self.protocol_name.as_deref());
        }
        e.put_string(&self.leader);
        if version >= 9 {
            e.put_i8(0); // skip_assignment = false
        }
        e.put_string(&self.member_id);
        e.put_i32(self.members.len() as i32);
        for (mid, meta) in &self.members {
            e.put_string(mid);
            if version >= 5 {
                e.put_nullable_string(None); // group_instance_id
            }
            e.put_i32(meta.len() as i32);
            e.put_bytes(meta);
        }
    }
}

#[derive(Debug, Clone)]
pub struct SyncGroupRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub assignments: Vec<(String, Bytes)>,
}

impl SyncGroupRequest {
    pub fn decode(buf: &mut Decoder, _version: i16) -> Result<Self> {
        let group_id = buf.get_string()?;
        let generation_id = buf.get_i32()?;
        let member_id = buf.get_string()?;
        let n_assign = buf.get_i32()?;
        let mut assignments = Vec::new();
        for _ in 0..n_assign {
            let mid = buf.get_string()?;
            let len = buf.get_i32()?;
            let meta = buf.get_bytes(len as usize)?;
            assignments.push((mid, meta));
        }
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            assignments,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SyncGroupResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub assignment: Bytes,
}

impl SyncGroupResponse {
    pub fn encode(&self, e: &mut Encoder, version: i16) {
        e.put_i32(self.throttle_time_ms);
        e.put_i16(self.error_code);
        if version >= 5 {
            // compact bytes
            e.put_unsigned_varint(self.assignment.len() as u64 + 1);
            e.put_bytes(&self.assignment);
        } else {
            e.put_i32(self.assignment.len() as i32);
            e.put_bytes(&self.assignment);
        }
    }
}

#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
}

impl HeartbeatRequest {
    pub fn decode(buf: &mut Decoder, _version: i16) -> Result<Self> {
        let group_id = buf.get_string()?;
        let generation_id = buf.get_i32()?;
        let member_id = buf.get_string()?;
        Ok(Self {
            group_id,
            generation_id,
            member_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    pub member_id: String,
}

impl LeaveGroupRequest {
    pub fn decode(buf: &mut Decoder, _version: i16) -> Result<Self> {
        let group_id = buf.get_string()?;
        let member_id = buf.get_string()?;
        Ok(Self {
            group_id,
            member_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct OffsetCommitRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub topics: Vec<(String, Vec<(i32, i64, Option<String>, i64)>)>, // topic -> (partition, offset, metadata, commit_ts)
}

impl OffsetCommitRequest {
    pub fn decode(buf: &mut Decoder, version: i16) -> Result<Self> {
        let group_id = buf.get_string()?;
        let generation_id = if version >= 1 { buf.get_i32()? } else { -1 };
        let member_id = if version >= 1 { buf.get_string()? } else { String::new() };
        if version >= 7 {
            let _group_instance_id = buf.get_nullable_string()?;
        }
        if version >= 2 {
            let _retention_time = buf.get_i64()?;
        }
        let n_topics = buf.get_i32()?;
        let mut topics = Vec::new();
        for _ in 0..n_topics {
            let topic = buf.get_string()?;
            let n_part = buf.get_i32()?;
            let mut parts = Vec::new();
            for _ in 0..n_part {
                let partition = buf.get_i32()?;
                let offset = buf.get_i64()?;
                // CommitTimestamp 仅 v1 存在；v0/v2+ 无该字段。
                let ts = if version == 1 { buf.get_i64()? } else { -1 };
                // CommittedLeaderEpoch 仅 v6-v8 存在（非 flexible 时的显式字段）
                if version >= 6 {
                    let _epoch = buf.get_i32()?;
                }
                let metadata = buf.get_nullable_string()?;
                parts.push((partition, offset, metadata, ts));
            }
            topics.push((topic, parts));
        }
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            topics,
        })
    }
}

#[derive(Debug, Clone)]
pub struct OffsetFetchRequest {
    pub group_id: String,
    pub topics: Vec<(String, Vec<i32>)>,
}

impl OffsetFetchRequest {
    pub fn decode(buf: &mut Decoder, _version: i16) -> Result<Self> {
        let group_id = buf.get_string()?;
        let n_topics = buf.get_i32()?;
        let mut topics = Vec::new();
        for _ in 0..n_topics {
            let topic = buf.get_string()?;
            let n_part = buf.get_i32()?;
            let mut parts = Vec::new();
            for _ in 0..n_part {
                parts.push(buf.get_i32()?);
            }
            topics.push((topic, parts));
        }
        Ok(Self {
            group_id,
            topics,
        })
    }
}

#[derive(Debug, Clone)]
pub struct OffsetFetchResponse {
    pub throttle_time_ms: i32,
    pub topics: Vec<(String, Vec<(i32, i64, Option<String>, i16)>)>,
    pub error_code: i16,
}

impl OffsetFetchResponse {
    pub fn encode(&self, e: &mut Encoder, version: i16) {
        e.put_i32(self.throttle_time_ms);
        e.put_i32(self.topics.len() as i32);
        for (topic, parts) in &self.topics {
            e.put_string(topic);
            e.put_i32(parts.len() as i32);
            for (partition, offset, metadata, err) in parts {
                e.put_i32(*partition);
                e.put_i64(*offset);
                e.put_nullable_string(metadata.as_deref());
                e.put_i16(*err);
            }
        }
        if version >= 2 {
            e.put_i16(self.error_code);
        }
    }
}

// ============================ CreateTopics / DeleteTopics ============================

#[derive(Debug, Clone)]
pub struct CreateTopicsRequest {
    pub topics: Vec<(String, i32, i16, Vec<(String, String)>)>, // name, num_partitions, rf, configs
    pub timeout_ms: i32,
    pub validate_only: bool,
}

impl CreateTopicsRequest {
    pub fn decode(buf: &mut Decoder, _version: i16) -> Result<Self> {
        let n_topics = buf.get_i32()?;
        let mut topics = Vec::new();
        for _ in 0..n_topics {
            let name = buf.get_string()?;
            let num_partitions = buf.get_i32()?;
            let rf = buf.get_i16()?;
            let n_configs = buf.get_i32()?;
            let mut configs = Vec::new();
            for _ in 0..n_configs {
                let k = buf.get_string()?;
                let v = buf.get_nullable_string()?.unwrap_or_default();
                configs.push((k, v));
            }
            topics.push((name, num_partitions, rf, configs));
        }
        let timeout_ms = buf.get_i32()?;
        let validate_only = if buf.remaining() > 0 { buf.get_i8()? != 0 } else { false };
        Ok(Self {
            topics,
            timeout_ms,
            validate_only,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DeleteTopicsRequest {
    pub topic_names: Vec<String>,
    pub timeout_ms: i32,
}

impl DeleteTopicsRequest {
    pub fn decode(buf: &mut Decoder, _version: i16) -> Result<Self> {
        let n = buf.get_i32()?;
        let mut names = Vec::new();
        for _ in 0..n {
            names.push(buf.get_string()?);
        }
        let timeout_ms = buf.get_i32()?;
        Ok(Self {
            topic_names: names,
            timeout_ms,
        })
    }
}
