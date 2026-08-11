//! 请求处理器
//!
//! 解析请求头，根据 API Key 路由到对应的处理器，序列化响应。
//! 覆盖：ApiVersions / Metadata / Produce / Fetch / ListOffsets /
//! CreateTopics / DeleteTopics / 消费者组与 Offset 相关。

use super::metadata::{MetadataManager, TopicInfo};
use crate::group::GroupCoordinator;
use crate::metrics::Metrics;
use crate::protocol::*;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Instant;

/// 请求处理器：为每个连接创建一份（持有共享状态）。
pub struct RequestHandler {
    metadata: Arc<MetadataManager>,
    groups: Arc<GroupCoordinator>,
    metrics: Arc<Metrics>,
    broker_host: String,
    broker_port: i32,
    broker_id: i32,
}

impl RequestHandler {
    pub fn new(
        metadata: Arc<MetadataManager>,
        groups: Arc<GroupCoordinator>,
        metrics: Arc<Metrics>,
    ) -> Self {
        // broker 信息从 metadata 内部 config 获取（简化：使用默认）
        Self {
            metadata,
            groups,
            metrics,
            broker_host: "127.0.0.1".to_string(),
            broker_port: 9092,
            broker_id: 0,
        }
    }

    /// 处理一个完整请求体（不含 4 字节长度前缀），返回响应体（不含长度前缀）。
    pub async fn process(&self, body: Bytes) -> Bytes {
        let start = Instant::now();
        let mut decoder = Decoder::new(body);
        self.dispatch(&mut decoder, start).await
    }

    async fn dispatch(&self, decoder: &mut Decoder, start: Instant) -> Bytes {
        // 解析请求头（会推进 decoder 到 body 开始处）
        let header = match RequestHeader::decode(decoder, 0) {
            Ok(h) => h,
            Err(e) => {
                tracing::error!("请求头解析失败: {e}");
                return Bytes::new();
            }
        };
        self.metrics
            .requests_total
            .with_label_values(&[&header.api_key.to_string()])
            .inc();

        // 构造带 header 的响应：correlation_id + response
        let mut out = Encoder::new();
        out.put_i32(header.correlation_id);

        let resp_body = match header.api_key {
            apikey::API_VERSIONS => self.handle_api_versions(&header),
            apikey::METADATA => self.handle_metadata(&header, decoder),
            apikey::PRODUCE => self.handle_produce(&header, decoder).await,
            apikey::FETCH => self.handle_fetch(&header, decoder).await,
            apikey::LIST_OFFSETS => self.handle_list_offsets(&header, decoder),
            apikey::CREATE_TOPICS => self.handle_create_topics(&header, decoder),
            apikey::DELETE_TOPICS => self.handle_delete_topics(&header, decoder),
            apikey::FIND_COORDINATOR => self.handle_find_coordinator(&header, decoder),
            apikey::JOIN_GROUP => self.handle_join_group(&header, decoder),
            apikey::SYNC_GROUP => self.handle_sync_group(&header, decoder),
            apikey::HEARTBEAT => self.handle_heartbeat(&header, decoder),
            apikey::LEAVE_GROUP => self.handle_leave_group(&header, decoder),
            apikey::OFFSET_COMMIT => self.handle_offset_commit(&header, decoder),
            apikey::OFFSET_FETCH => self.handle_offset_fetch(&header, decoder),
            _ => {
                tracing::warn!("未支持的 API key: {}", header.api_key);
                // 返回 unsupported version
                let mut e = Encoder::new();
                e.put_i16(error_codes::UNSUPPORTED_VERSION);
                e.into_bytes()
            }
        };
        out.put_bytes(&resp_body);

        let latency = start.elapsed().as_secs_f64();
        self.metrics
            .request_latency
            .with_label_values(&[&header.api_key.to_string()])
            .observe(latency);
        out.into_bytes()
    }

    // ---------------- ApiVersions ----------------
    fn handle_api_versions(&self, _header: &RequestHeader) -> Bytes {
        let resp = ApiVersionsResponse {
            error_code: error_codes::NONE,
            api_keys: crate::SUPPORTED_API_KEYS
                .iter()
                .map(|(k, min, max)| ApiVersion {
                    api_key: *k,
                    min_version: *min,
                    max_version: *max,
                })
                .collect(),
            throttle_time_ms: 0,
        };
        let mut e = Encoder::new();
        resp.encode(&mut e, 3);
        e.into_bytes()
    }

    // ---------------- Metadata ----------------
    fn handle_metadata(&self, _header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match MetadataRequest::decode(decoder, 13) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Metadata 解码失败: {e}");
                return Bytes::new();
            }
        };
        let topics = if req.topics.is_empty() {
            self.metadata.topic_list()
        } else {
            req.topics
                .iter()
                .map(|t| {
                    self.metadata
                        .get_topic(t)
                        .unwrap_or(TopicInfo {
                            name: t.clone(),
                            is_internal: false,
                            partitions: vec![],
                        })
                })
                .collect()
        };

        let broker_meta = BrokerMetadata {
            node_id: self.broker_id,
            host: self.broker_host.clone(),
            port: self.broker_port,
        };
        let mut topics_meta = Vec::new();
        for t in topics {
            let partitions = if t.partitions.is_empty() {
                // 未知主题
                topics_meta.push(TopicMetadata {
                    error_code: error_codes::UNKNOWN_TOPIC_OR_PARTITION,
                    name: t.name,
                    is_internal: t.is_internal,
                    partitions: vec![],
                });
                continue;
            } else {
                t.partitions
                    .iter()
                    .map(|p| PartitionMetadata {
                        error_code: error_codes::NONE,
                        partition_index: p.partition,
                        leader_id: p.leader,
                        replica_nodes: p.replicas.clone(),
                        isr_nodes: p.isr.clone(),
                    })
                    .collect()
            };
            topics_meta.push(TopicMetadata {
                error_code: error_codes::NONE,
                name: t.name,
                is_internal: t.is_internal,
                partitions,
            });
        }

        let resp = MetadataResponse {
            brokers: vec![broker_meta],
            cluster_id: Some("rivine-cluster".to_string()),
            controller_id: self.broker_id,
            topics: topics_meta,
        };
        let mut e = Encoder::new();
        resp.encode(&mut e, 13);
        e.into_bytes()
    }

    // ---------------- Produce ----------------
    async fn handle_produce(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match ProduceRequest::decode(decoder, header.api_version) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Produce 解码失败: {e}");
                return Bytes::new();
            }
        };
        let mut resp_topics = Vec::new();
        for t in &req.topic_data {
            let mut partitions = Vec::new();
            for p in &t.partitions {
                let result = self
                    .metadata
                    .append_records(&t.topic, p.partition_index, &[p.records.clone()])
                    .await;
                match result {
                    Ok(offsets) => {
                        let (base, last) = offsets.last().copied().unwrap_or((0, 0));
                        partitions.push(ProduceResponsePartition {
                            partition_index: p.partition_index,
                            error_code: error_codes::NONE,
                            base_offset: base,
                            log_append_time_ms: crate::now_ms(),
                            log_start_offset: 0,
                        });
                        let _ = last;
                    }
                    Err(e) => {
                        tracing::warn!("Produce 追加失败: {e}");
                        partitions.push(ProduceResponsePartition {
                            partition_index: p.partition_index,
                            error_code: error_codes::UNKNOWN_TOPIC_OR_PARTITION,
                            base_offset: -1,
                            log_append_time_ms: -1,
                            log_start_offset: -1,
                        });
                    }
                }
            }
            resp_topics.push(ProduceResponseTopic {
                topic: t.topic.clone(),
                partitions,
            });
        }
        let resp = ProduceResponse {
            topics: resp_topics,
            throttle_time_ms: 0,
        };
        let mut e = Encoder::new();
        resp.encode(&mut e, header.api_version);
        e.into_bytes()
    }

    // ---------------- Fetch ----------------
    async fn handle_fetch(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match FetchRequest::decode(decoder, header.api_version) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Fetch 解码失败: {e}");
                return Bytes::new();
            }
        };
        let mut topics = Vec::new();
        for t in &req.topics {
            let mut partitions = Vec::new();
            for p in &t.partitions {
                let (data, hw) = self
                    .metadata
                    .read_records(&t.topic, p.partition_index, p.fetch_offset, p.partition_max_bytes.max(1) as usize)
                    .await
                    .unwrap_or((Bytes::new(), -1));
                partitions.push(FetchResponsePartition {
                    partition_index: p.partition_index,
                    error_code: error_codes::NONE,
                    high_watermark: hw,
                    last_stable_offset: hw,
                    log_start_offset: 0,
                    aborted_transactions: vec![],
                    records: data,
                });
            }
            topics.push(FetchResponseTopic {
                topic: t.topic.clone(),
                partitions,
            });
        }
        let resp = FetchResponse {
            throttle_time_ms: 0,
            topics,
        };
        let mut e = Encoder::new();
        resp.encode(&mut e, header.api_version);
        e.into_bytes()
    }

    // ---------------- ListOffsets ----------------
    fn handle_list_offsets(&self, _header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match ListOffsetsRequest::decode(decoder, 2) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("ListOffsets 解码失败: {e}");
                return Bytes::new();
            }
        };
        // 简化响应：返回每个分区的 LEO
        let mut e = Encoder::new();
        e.put_i32(req.topics.len() as i32);
        for (topic, parts) in &req.topics {
            e.put_string(topic);
            e.put_i32(parts.len() as i32);
            for (partition, _ts_type, ts, _max) in parts {
                let leo = self.metadata.partition_leo_sync(topic, *partition).unwrap_or(0);
                e.put_i32(*partition);
                e.put_i16(error_codes::NONE);
                if *ts < 0 {
                    e.put_i64(leo);
                } else {
                    e.put_i64(leo);
                    e.put_i64(*ts);
                }
            }
        }
        e.into_bytes()
    }

    // ---------------- CreateTopics / DeleteTopics ----------------
    fn handle_create_topics(&self, _header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match CreateTopicsRequest::decode(decoder, 4) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("CreateTopics 解码失败: {e}");
                return Bytes::new();
            }
        };
        let mut e = Encoder::new();
        e.put_i32(req.topics.len() as i32);
        for (name, num_partitions, _rf, _configs) in &req.topics {
            let err = match self.metadata.create_topic(name, *num_partitions, false) {
                Ok(_) => error_codes::NONE,
                Err(_) => error_codes::UNKNOWN_SERVER_ERROR,
            };
            e.put_string(name);
            e.put_i16(err);
            if 4 >= 1 {
                e.put_i16(-1); // error_message
            }
            if 4 >= 2 {
                e.put_i32(1); // num_partitions
            }
            if 4 >= 2 {
                e.put_i16(1); // replication_factor
            }
            if 4 >= 5 {
                e.put_i32(0); // configs count
            }
        }
        e.put_i32(req.timeout_ms);
        e.into_bytes()
    }

    fn handle_delete_topics(&self, _header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match DeleteTopicsRequest::decode(decoder, 4) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("DeleteTopics 解码失败: {e}");
                return Bytes::new();
            }
        };
        let mut e = Encoder::new();
        e.put_i32(req.topic_names.len() as i32);
        for name in &req.topic_names {
            let err = match self.metadata.delete_topic(name) {
                Ok(_) => error_codes::NONE,
                Err(_) => error_codes::UNKNOWN_TOPIC_OR_PARTITION,
            };
            e.put_string(name);
            e.put_i16(err);
        }
        e.put_i32(req.timeout_ms);
        e.into_bytes()
    }

    // ---------------- 消费者组 ----------------
    fn handle_find_coordinator(&self, _header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match FindCoordinatorRequest::decode(decoder, 3) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("FindCoordinator 解码失败: {e}");
                return Bytes::new();
            }
        };
        let _ = self.groups.coordinator_partition(&req.key);
        let resp = FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_code: error_codes::NONE,
            error_message: None,
            node_id: self.broker_id,
            host: self.broker_host.clone(),
            port: self.broker_port,
        };
        let mut e = Encoder::new();
        resp.encode(&mut e, 3);
        e.into_bytes()
    }

    fn handle_join_group(&self, _header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match JoinGroupRequest::decode(decoder, 6) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("JoinGroup 解码失败: {e}");
                return Bytes::new();
            }
        };
        let (err, generation, leader, member_id, protocol, members) = self.groups.join_group(
            &req.group_id,
            req.session_timeout_ms,
            req.rebalance_timeout_ms,
            &req.member_id,
            &req.protocol_type,
            req.group_protocols,
        );
        let resp = JoinGroupResponse {
            throttle_time_ms: 0,
            error_code: err,
            generation_id: generation,
            protocol_type: Some(req.protocol_type),
            protocol_name: protocol,
            leader,
            member_id,
            members,
        };
        let mut e = Encoder::new();
        resp.encode(&mut e, 6);
        e.into_bytes()
    }

    fn handle_sync_group(&self, _header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match SyncGroupRequest::decode(decoder, 4) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("SyncGroup 解码失败: {e}");
                return Bytes::new();
            }
        };
        let (err, assignment) =
            self.groups
                .sync_group(&req.group_id, req.generation_id, &req.member_id, req.assignments);
        let resp = SyncGroupResponse {
            throttle_time_ms: 0,
            error_code: err,
            assignment,
        };
        let mut e = Encoder::new();
        resp.encode(&mut e, 4);
        e.into_bytes()
    }

    fn handle_heartbeat(&self, _header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match HeartbeatRequest::decode(decoder, 4) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Heartbeat 解码失败: {e}");
                return Bytes::new();
            }
        };
        let err = self.groups.heartbeat(&req.group_id, req.generation_id, &req.member_id);
        let mut e = Encoder::new();
        e.put_i32(0); // throttle_time_ms
        e.put_i16(err);
        e.into_bytes()
    }

    fn handle_leave_group(&self, _header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match LeaveGroupRequest::decode(decoder, 3) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("LeaveGroup 解码失败: {e}");
                return Bytes::new();
            }
        };
        let err = self.groups.leave_group(&req.group_id, &req.member_id);
        let mut e = Encoder::new();
        e.put_i32(0);
        e.put_i16(err);
        e.into_bytes()
    }

    fn handle_offset_commit(&self, _header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match OffsetCommitRequest::decode(decoder, 8) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("OffsetCommit 解码失败: {e}");
                return Bytes::new();
            }
        };
        // 单机实现：写内存（简化为空操作，返回成功）
        let mut e = Encoder::new();
        e.put_i32(0);
        e.put_i32(req.topics.len() as i32);
        for (topic, parts) in &req.topics {
            e.put_string(topic);
            e.put_i32(parts.len() as i32);
            for (partition, _offset, _meta, _ts) in parts {
                e.put_i32(*partition);
                e.put_i16(error_codes::NONE);
            }
        }
        e.into_bytes()
    }

    fn handle_offset_fetch(&self, _header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match OffsetFetchRequest::decode(decoder, 5) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("OffsetFetch 解码失败: {e}");
                return Bytes::new();
            }
        };
        let mut topics = Vec::new();
        for (topic, parts) in &req.topics {
            let part_resp: Vec<(i32, i64, Option<String>, i16)> = parts
                .iter()
                .map(|p| (*p, -1, None, error_codes::NONE))
                .collect();
            topics.push((topic.clone(), part_resp));
        }
        let resp = OffsetFetchResponse {
            throttle_time_ms: 0,
            topics,
            error_code: error_codes::NONE,
        };
        let mut e = Encoder::new();
        resp.encode(&mut e, 5);
        e.into_bytes()
    }
}


