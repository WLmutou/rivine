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
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// 幂等生产者 ID 分配器（单机递增，>=0 为有效 producer_id）。
static NEXT_PRODUCER_ID: AtomicI64 = AtomicI64::new(0);

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

    /// 处理一个完整请求体（不含 4 字节长度前缀）。
    /// 返回响应体（不含长度前缀）；`None` 表示该请求按协议约定不返回响应（如 Produce acks=0）。
    pub async fn process(&self, body: Bytes) -> Option<Bytes> {
        let start = Instant::now();
        let mut decoder = Decoder::new(body);
        self.dispatch(&mut decoder, start).await
    }

    async fn dispatch(&self, decoder: &mut Decoder, start: Instant) -> Option<Bytes> {
        // 解析请求头（会推进 decoder 到 body 开始处）
        let header = match RequestHeader::decode(decoder, 0) {
            Ok(h) => h,
            Err(e) => {
                tracing::error!("请求头解析失败: {e}");
                return None;
            }
        };
        self.metrics
            .requests_total
            .with_label_values(&[&header.api_key.to_string()])
            .inc();

        tracing::debug!(
            "请求: api_key={} api_version={} correlation_id={}",
            header.api_key,
            header.api_version,
            header.correlation_id
        );

        let resp_body = match header.api_key {
            apikey::API_VERSIONS => Some(self.handle_api_versions(&header)),
            apikey::METADATA => Some(self.handle_metadata(&header, decoder)),
            apikey::PRODUCE => self.handle_produce(&header, decoder).await,
            apikey::FETCH => Some(self.handle_fetch(&header, decoder).await),
            apikey::LIST_OFFSETS => Some(self.handle_list_offsets(&header, decoder)),
            apikey::CREATE_TOPICS => Some(self.handle_create_topics(&header, decoder)),
            apikey::DELETE_TOPICS => Some(self.handle_delete_topics(&header, decoder)),
            apikey::INIT_PRODUCER_ID => Some(self.handle_init_producer_id(&header, decoder)),
            apikey::FIND_COORDINATOR => Some(self.handle_find_coordinator(&header, decoder)),
            apikey::JOIN_GROUP => Some(self.handle_join_group(&header, decoder)),
            apikey::SYNC_GROUP => Some(self.handle_sync_group(&header, decoder)),
            apikey::HEARTBEAT => Some(self.handle_heartbeat(&header, decoder)),
            apikey::LEAVE_GROUP => Some(self.handle_leave_group(&header, decoder)),
            apikey::OFFSET_COMMIT => Some(self.handle_offset_commit(&header, decoder)),
            apikey::OFFSET_FETCH => Some(self.handle_offset_fetch(&header, decoder)),
            apikey::LIST_GROUPS => Some(self.handle_list_groups(&header, decoder)),
            apikey::DESCRIBE_GROUPS => Some(self.handle_describe_groups(&header, decoder)),
            _ => {
                tracing::warn!("未支持的 API key: {}", header.api_key);
                // 返回 unsupported version
                let mut e = Encoder::new();
                e.put_i16(error_codes::UNSUPPORTED_VERSION);
                Some(e.into_bytes())
            }
        };

        // 若需要返回响应，则构造带 correlation_id 的响应帧。
        let resp = resp_body.map(|body| {
            let mut out = Encoder::new();
            out.put_i32(header.correlation_id);
            out.put_bytes(&body);
            let latency = start.elapsed().as_secs_f64();
            self.metrics
                .request_latency
                .with_label_values(&[&header.api_key.to_string()])
                .observe(latency);
            out.into_bytes()
        });
        resp
    }

    // ---------------- ApiVersions ----------------
    fn handle_api_versions(&self, header: &RequestHeader) -> Bytes {
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
        // 响应格式随客户端请求的 api_version 变化（v0-v2 传统，v3+ 紧凑）
        let mut e = Encoder::new();
        resp.encode(&mut e, header.api_version);
        e.into_bytes()
    }

    // ---------------- Metadata ----------------
    fn handle_metadata(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match MetadataRequest::decode(decoder, header.api_version) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Metadata 解码失败: {e}");
                return Bytes::new();
            }
        };
        // 若客户端允许自动创建主题，则对不存在的非内部主题自动创建。
        if req.allow_auto_topic_creation {
            for t in &req.topics {
                if self.metadata.get_topic(t).is_none() {
                    let _ = self
                        .metadata
                        .create_topic(t, self.metadata.default_partitions(), false);
                }
            }
        }

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
        resp.encode(&mut e, header.api_version);
        e.into_bytes()
    }

    // ---------------- Produce ----------------
    /// 处理 Produce。当 acks=0 时按协议约定不返回响应（返回 `None`）。
    async fn handle_produce(&self, header: &RequestHeader, decoder: &mut Decoder) -> Option<Bytes> {
        let req = match ProduceRequest::decode(decoder, header.api_version) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Produce 解码失败: {e}");
                return None;
            }
        };

        // 校验 RequiredAcks（仅 0、1、-1 合法；其他值返回 INVALID_REQUIRED_ACKS）。
        let invalid_acks = !matches!(req.acks, 0 | 1 | -1);
        // acks=0：fire-and-forget，不等待任何确认，也不返回响应。
        let no_response = req.acks == 0;

        let mut resp_topics = Vec::new();
        for t in &req.topic_data {
            let mut partitions = Vec::new();
            for p in &t.partitions {
                if invalid_acks {
                    partitions.push(ProduceResponsePartition {
                        partition_index: p.partition_index,
                        error_code: error_codes::INVALID_REQUIRED_ACKS,
                        base_offset: -1,
                        log_append_time_ms: -1,
                        log_start_offset: -1,
                    });
                    continue;
                }
                // 主题或分区不存在时自动创建（模拟 auto.create.topics.enable=true）
                if !self.metadata.partition_exists(&t.topic, p.partition_index) {
                    let _ = self
                        .metadata
                        .create_topic(&t.topic, self.metadata.default_partitions(), false);
                }
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
        // acks=0 时不构造响应体，直接返回 None。
        if no_response {
            return None;
        }
        let resp = ProduceResponse {
            topics: resp_topics,
            throttle_time_ms: 0,
        };
        let mut e = Encoder::new();
        resp.encode(&mut e, header.api_version);
        Some(e.into_bytes())
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
        tracing::debug!(
            "Fetch v{} max_wait={} min_bytes={} topics={:?}",
            header.api_version,
            req.max_wait_ms,
            req.min_bytes,
            req.topics.iter().map(|t| (t.topic.clone(), t.partitions.iter().map(|p| (p.partition_index, p.fetch_offset)).collect::<Vec<_>>())).collect::<Vec<_>>()
        );

        // 实现 Fetch 的 long-poll 语义：
        // 若当前没有足够数据（min_bytes），则最多等待 max_wait_ms。
        let min_bytes = req.min_bytes.max(0) as usize;
        let max_wait = std::time::Duration::from_millis(req.max_wait_ms.max(0) as u64);
        let deadline = std::time::Instant::now() + max_wait;

        // 首次尝试读取；若不足且未超时，则轮询等待直到数据满足或超时。
        loop {
            let mut total_bytes = 0usize;
            let mut topics = Vec::new();
            let mut has_error = false;
            for t in &req.topics {
                let mut partitions = Vec::new();
                for p in &t.partitions {
                    let log_start = self.metadata.partition_leo_sync(&t.topic, p.partition_index).unwrap_or(-1);
                    let fetch_result = self
                        .metadata
                        .read_records(&t.topic, p.partition_index, p.fetch_offset, p.partition_max_bytes.max(1) as usize)
                        .await;
                    match fetch_result {
                        Some((data, hw)) => {
                            // 若请求的 offset 超出当前日志末尾，视为 offset 越界。
                            if p.fetch_offset > hw {
                                partitions.push(FetchResponsePartition {
                                    partition_index: p.partition_index,
                                    error_code: error_codes::OFFSET_OUT_OF_RANGE,
                                    high_watermark: hw,
                                    last_stable_offset: hw,
                                    log_start_offset: log_start,
                                    aborted_transactions: vec![],
                                    records: Bytes::new(),
                                });
                                has_error = true;
                                continue;
                            }
                            total_bytes += data.len();
                            partitions.push(FetchResponsePartition {
                                partition_index: p.partition_index,
                                error_code: error_codes::NONE,
                                high_watermark: hw,
                                last_stable_offset: hw,
                                log_start_offset: log_start,
                                aborted_transactions: vec![],
                                records: data,
                            });
                        }
                        None => {
                            // 分区不存在。
                            partitions.push(FetchResponsePartition {
                                partition_index: p.partition_index,
                                error_code: error_codes::UNKNOWN_TOPIC_OR_PARTITION,
                                high_watermark: -1,
                                last_stable_offset: -1,
                                log_start_offset: -1,
                                aborted_transactions: vec![],
                                records: Bytes::new(),
                            });
                            has_error = true;
                        }
                    }
                }
                topics.push(FetchResponseTopic {
                    topic: t.topic.clone(),
                    partitions,
                });
            }

            // 数据足够或已出错，或等待超时，则返回。
            if total_bytes >= min_bytes || has_error || std::time::Instant::now() >= deadline {
                let resp = FetchResponse {
                    throttle_time_ms: 0,
                    topics,
                };
                let mut e = Encoder::new();
                resp.encode(&mut e, header.api_version);
                return e.into_bytes();
            }

            // 数据不足且未超时：短暂休眠后重试。
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    // ---------------- ListOffsets ----------------
    fn handle_list_offsets(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match ListOffsetsRequest::decode(decoder, header.api_version) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("ListOffsets 解码失败: {e}");
                return Bytes::new();
            }
        };
        // 响应：每个分区返回请求的 offset。
        let mut e = Encoder::new();
        // ListOffsets 的 ThrottleTimeMs 仅 v2+ 存在（v0-v1 响应无 throttle）
        if header.api_version >= 2 {
            e.put_i32(0); // throttle_time_ms
        }
        e.put_i32(req.topics.len() as i32);
        for (topic, parts) in &req.topics {
            e.put_string(topic);
            e.put_i32(parts.len() as i32);
            for (partition, _ts_type, ts, max_num) in parts {
                // 分区不存在时返回错误码。
                let (leo, log_start) = match (
                    self.metadata.partition_leo_sync(topic, *partition),
                    self.metadata.partition_log_start_sync(topic, *partition),
                ) {
                    (Some(leo), Some(log_start)) => (leo, log_start),
                    _ => {
                        e.put_i32(*partition);
                        e.put_i16(error_codes::UNKNOWN_TOPIC_OR_PARTITION);
                        if header.api_version >= 1 {
                            e.put_i64(-1);
                            e.put_i64(-1);
                        } else {
                            e.put_i32(0); // old_style_offsets 空数组
                        }
                        continue;
                    }
                };
                e.put_i32(*partition);
                e.put_i16(error_codes::NONE);
                if header.api_version >= 1 {
                    // v1+ 响应：timestamp + offset 两个 int64
                    match *ts {
                        -2 => {
                            // earliest：日志起始偏移
                            e.put_i64(-1);
                            e.put_i64(log_start);
                        }
                        -1 => {
                            // latest：LEO（下一条消息的偏移）
                            e.put_i64(-1);
                            e.put_i64(leo);
                        }
                        _ => {
                            // 按时间戳查找：本实现返回 LEO（简化，不精确定位时间戳）。
                            e.put_i64(*ts);
                            e.put_i64(leo);
                        }
                    }
                } else {
                    // v0 响应：old_style_offsets (int32 array)，offset 按时间倒序返回。
                    let n = (*max_num).max(1) as i32;
                    let mut offsets: Vec<i32> = Vec::new();
                    match *ts {
                        -2 => offsets.push(log_start as i32),
                        _ => {
                            // latest 或按时间：从 LEO 向前返回 n 个 offset。
                            let mut o = leo - 1;
                            for _ in 0..n {
                                if o < log_start {
                                    break;
                                }
                                offsets.push(o as i32);
                                o -= 1;
                            }
                        }
                    }
                    e.put_i32(offsets.len() as i32);
                    for o in offsets {
                        e.put_i32(o);
                    }
                }
            }
        }
        e.into_bytes()
    }

    // ---------------- CreateTopics / DeleteTopics ----------------
    fn handle_create_topics(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match CreateTopicsRequest::decode(decoder, header.api_version) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("CreateTopics 解码失败: {e}");
                return Bytes::new();
            }
        };
        let ver = header.api_version;
        let mut e = Encoder::new();
        e.put_i32(req.topics.len() as i32);
        for (name, num_partitions, _rf, _configs) in &req.topics {
            let err = match self.metadata.create_topic(name, *num_partitions, false) {
                Ok(_) => error_codes::NONE,
                Err(_) => error_codes::UNKNOWN_SERVER_ERROR,
            };
            e.put_string(name);
            e.put_i16(err);
            if ver >= 1 {
                e.put_i16(-1); // error_message (null)
            }
            if ver >= 2 {
                e.put_i32(*num_partitions); // num_partitions
            }
            if ver >= 2 {
                e.put_i16(1); // replication_factor
            }
            if ver >= 5 {
                e.put_i32(0); // configs count
            }
        }
        if ver >= 2 {
            e.put_i32(0); // throttle_time_ms
        }
        e.into_bytes()
    }

    fn handle_delete_topics(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match DeleteTopicsRequest::decode(decoder, header.api_version) {
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
        if header.api_version >= 1 {
            e.put_i32(0); // throttle_time_ms
        }
        e.into_bytes()
    }

    // ---------------- InitProducerId（幂等生产者） ----------------
    fn handle_init_producer_id(&self, _header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        // v0-v1 请求体仅含 transactional_id（nullable string）。
        let _ = match decoder.get_nullable_string() {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("InitProducerId 解码失败: {e}");
                return Bytes::new();
            }
        };
        let producer_id = NEXT_PRODUCER_ID.fetch_add(1, Ordering::Relaxed);
        let mut e = Encoder::new();
        e.put_i32(0); // throttle_time_ms
        e.put_i16(error_codes::NONE);
        e.put_i64(producer_id);
        e.put_i16(0); // producer_epoch
        e.into_bytes()
    }

    // ---------------- 消费者组 ----------------
    fn handle_find_coordinator(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match FindCoordinatorRequest::decode(decoder, header.api_version) {
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
        resp.encode(&mut e, header.api_version);
        e.into_bytes()
    }

    fn handle_join_group(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match JoinGroupRequest::decode(decoder, header.api_version) {
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
        resp.encode(&mut e, header.api_version);
        e.into_bytes()
    }

    fn handle_sync_group(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match SyncGroupRequest::decode(decoder, header.api_version) {
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
        resp.encode(&mut e, header.api_version);
        e.into_bytes()
    }

    fn handle_heartbeat(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match HeartbeatRequest::decode(decoder, header.api_version) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Heartbeat 解码失败: {e}");
                return Bytes::new();
            }
        };
        let err = self.groups.heartbeat(&req.group_id, req.generation_id, &req.member_id);
        let mut e = Encoder::new();
        // Heartbeat v0 无 throttle_time_ms；v1+ 才有
        if header.api_version >= 1 {
            e.put_i32(0); // throttle_time_ms
        }
        e.put_i16(err);
        e.into_bytes()
    }

    fn handle_leave_group(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match LeaveGroupRequest::decode(decoder, header.api_version) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("LeaveGroup 解码失败: {e}");
                return Bytes::new();
            }
        };
        let err = self.groups.leave_group(&req.group_id, &req.member_id);
        let mut e = Encoder::new();
        // LeaveGroup v0 无 throttle_time_ms；v1+ 才有
        if header.api_version >= 1 {
            e.put_i32(0);
        }
        e.put_i16(err);
        e.into_bytes()
    }

    fn handle_offset_commit(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match OffsetCommitRequest::decode(decoder, header.api_version) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("OffsetCommit 解码失败: {e}");
                return Bytes::new();
            }
        };

        tracing::debug!(
            "OffsetCommit: group={} generation={} member='{}' v{} topics={:?}",
            req.group_id,
            req.generation_id,
            req.member_id,
            header.api_version,
            req.topics.iter().map(|(t, p)| (t, p.iter().map(|(p, o, _, _)| (*p, *o)).collect::<Vec<_>>())).collect::<Vec<_>>()
        );

        // 若该 group 属于一个活跃的消费者组（generation >= 0 且 member 非空），
        // 校验成员身份与 generation；simple consumer（generation=-1、空 member）跳过校验。
        let is_simple_consumer = req.generation_id == -1 && req.member_id.is_empty();
        if !is_simple_consumer {
            let gid = &req.group_id;
            if let Some(group) = self.groups.groups.get(gid) {
                if group.generation_id != req.generation_id {
                    tracing::debug!("OffsetCommit 拒绝: ILLEGAL_GENERATION group_gen={} req_gen={}", group.generation_id, req.generation_id);
                    // ILLEGAL_GENERATION
                    return self.encode_offset_commit_err_response(header.api_version, &req.topics, error_codes::ILLEGAL_GENERATION);
                }
                if !group.members.contains_key(&req.member_id) {
                    tracing::debug!("OffsetCommit 拒绝: UNKNOWN_MEMBER_ID member='{}'", req.member_id);
                    // UNKNOWN_MEMBER_ID
                    return self.encode_offset_commit_err_response(header.api_version, &req.topics, error_codes::UNKNOWN_MEMBER_ID);
                }
                if group.state != crate::group::GroupState::Stable {
                    tracing::debug!("OffsetCommit 拒绝: REBALANCE_IN_PROGRESS state={:?}", group.state);
                    // REBALANCE_IN_PROGRESS
                    return self.encode_offset_commit_err_response(header.api_version, &req.topics, error_codes::REBALANCE_IN_PROGRESS);
                }
            }
        }

        let mut e = Encoder::new();
        // OffsetCommit 的 ThrottleTimeMs 仅 v3+ 存在（v0-v2 响应无 throttle）
        if header.api_version >= 3 {
            e.put_i32(0);
        }
        e.put_i32(req.topics.len() as i32);
        for (topic, parts) in &req.topics {
            e.put_string(topic);
            e.put_i32(parts.len() as i32);
            for (partition, offset, metadata, _ts) in parts {
                // 真正持久化提交的偏移量到内存存储。
                self.groups
                    .commit_offset(&req.group_id, topic, *partition, *offset, metadata.clone());
                e.put_i32(*partition);
                e.put_i16(error_codes::NONE);
            }
        }
        e.into_bytes()
    }

    /// 构造 OffsetCommit 全分区统一的错误响应。
    fn encode_offset_commit_err_response(
        &self,
        version: i16,
        topics: &[(String, Vec<(i32, i64, Option<String>, i64)>)],
        err: i16,
    ) -> Bytes {
        let mut e = Encoder::new();
        if version >= 3 {
            e.put_i32(0); // throttle_time_ms
        }
        e.put_i32(topics.len() as i32);
        for (topic, parts) in topics {
            e.put_string(topic);
            e.put_i32(parts.len() as i32);
            for (partition, _, _, _) in parts {
                e.put_i32(*partition);
                e.put_i16(err);
            }
        }
        e.into_bytes()
    }

    fn handle_offset_fetch(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match OffsetFetchRequest::decode(decoder, header.api_version) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("OffsetFetch 解码失败: {e}");
                return Bytes::new();
            }
        };
        tracing::debug!(
            "OffsetFetch: group='{}' v{} topics={:?}",
            req.group_id,
            header.api_version,
            req.topics
        );
        let mut topics = Vec::new();
        for (topic, parts) in &req.topics {
            let part_resp: Vec<(i32, i64, Option<String>, i16)> = parts
                .iter()
                .map(|p| {
                    match self.groups.fetch_offset_with_meta(&req.group_id, topic, *p) {
                        Some((offset, meta)) => (*p, offset, meta, error_codes::NONE),
                        // 没有已提交的 offset：按文档约定返回 offset=-1、空 metadata、无错误码。
                        None => (*p, -1, None, error_codes::NONE),
                    }
                })
                .collect();
            topics.push((topic.clone(), part_resp));
        }
        let resp = OffsetFetchResponse {
            throttle_time_ms: 0,
            topics,
            error_code: error_codes::NONE,
        };
        let mut e = Encoder::new();
        resp.encode(&mut e, header.api_version);
        e.into_bytes()
    }

    // ---------------- ListGroups ----------------
    fn handle_list_groups(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match ListGroupsRequest::decode(decoder, header.api_version) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("ListGroups 解码失败: {e}");
                return Bytes::new();
            }
        };
        let mut groups: Vec<ListedGroup> = self
            .groups
            .list_groups()
            .into_iter()
            .map(|(group_id, protocol_type)| {
                let group_state = self
                    .groups
                    .group_state_str(&group_id)
                    .unwrap_or_else(|| "Empty".to_string());
                ListedGroup {
                    group_id,
                    protocol_type,
                    group_state,
                }
            })
            .collect();
        // 若提供了状态过滤器，则仅返回匹配状态的组。
        if !req.states_filter.is_empty() {
            groups.retain(|g| req.states_filter.contains(&g.group_state));
        }
        groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        let resp = ListGroupsResponse {
            throttle_time_ms: 0,
            error_code: error_codes::NONE,
            groups,
        };
        let mut e = Encoder::new();
        resp.encode(&mut e, header.api_version);
        e.into_bytes()
    }

    // ---------------- DescribeGroups ----------------
    fn handle_describe_groups(&self, header: &RequestHeader, decoder: &mut Decoder) -> Bytes {
        let req = match DescribeGroupsRequest::decode(decoder, header.api_version) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("DescribeGroups 解码失败: {e}");
                return Bytes::new();
            }
        };
        let mut groups = Vec::new();
        for group_id in &req.groups {
            // 协议元数据与分配信息的解析。
            let members = self.groups.describe_members(group_id);
            let group_state = self
                .groups
                .group_state_str(group_id)
                .unwrap_or_else(|| "Dead".to_string());
            let protocol_type = self
                .groups
                .group_protocol_type(group_id)
                .unwrap_or_else(|| String::new());
            let error_code = if members.is_empty() && group_state == "Dead" {
                error_codes::NONE // 不存在的组返回空描述（无错误）
            } else {
                error_codes::NONE
            };
            let desc_members = members
                .into_iter()
                .map(|(mid, meta, assign)| DescribedGroupMember {
                    member_id: mid,
                    group_instance_id: None,
                    client_id: String::new(),
                    client_host: "".to_string(),
                    member_metadata: meta,
                    member_assignment: assign,
                })
                .collect();
            groups.push(DescribedGroup {
                error_code,
                group_id: group_id.clone(),
                group_state,
                protocol_type,
                protocol_data: String::new(),
                members: desc_members,
                authorized_operations: -2147483648, // 0x80000000（ALL），与 Kafka 一致
            });
        }
        let resp = DescribeGroupsResponse {
            throttle_time_ms: 0,
            groups,
        };
        let mut e = Encoder::new();
        resp.encode(&mut e, header.api_version);
        e.into_bytes()
    }
}


