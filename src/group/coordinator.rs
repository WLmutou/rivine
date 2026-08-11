//! Group Coordinator 实现
//!
//! 维护每个消费者组的状态，处理 JoinGroup/SyncGroup/Heartbeat/LeaveGroup，
//! 并管理 Offset 提交（写入 __consumer_offsets）。

use crate::server::metadata::MetadataManager;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Rebalance 状态机状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

/// 组成员
#[derive(Debug, Clone)]
pub struct Member {
    pub member_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocols: Vec<(String, Bytes)>,
    pub last_heartbeat: Instant,
    pub assignment: Bytes,
    pub metadata: Bytes,
}

/// 消费者组
#[derive(Debug)]
pub struct Group {
    pub group_id: String,
    pub state: GroupState,
    pub generation_id: i32,
    pub leader_id: Option<String>,
    pub protocol: Option<String>,
    pub members: HashMap<String, Member>,
    pub protocol_type: String,
}

impl Group {
    pub fn new(group_id: &str) -> Self {
        Self {
            group_id: group_id.to_string(),
            state: GroupState::Empty,
            generation_id: 0,
            leader_id: None,
            protocol: None,
            members: HashMap::new(),
            protocol_type: String::new(),
        }
    }
}

/// Group Coordinator
pub struct GroupCoordinator {
    metadata: Arc<MetadataManager>,
    /// group_id -> Group
    pub groups: DashMap<String, Group>,
    /// __consumer_offsets 的分区数
    pub offsets_topic_partitions: i32,
}

impl GroupCoordinator {
    pub fn new(metadata: Arc<MetadataManager>) -> Self {
        Self {
            metadata,
            groups: DashMap::new(),
            offsets_topic_partitions: 50,
        }
    }

    /// 启动：确保 __consumer_offsets 内部主题存在。
    pub fn init(&self) {
        if let Err(e) = self
            .metadata
            .create_internal_topic("__consumer_offsets", self.offsets_topic_partitions)
        {
            tracing::warn!("创建 __consumer_offsets 失败: {e}");
        }
    }

    /// 计算 group.id 对应的 Coordinator 分区：hash(group) % num_partitions
    pub fn coordinator_partition(&self, group_id: &str) -> i32 {
        // 与 Java String.hashCode 兼容（简化：简单 hash）
        let hash = stable_hash(group_id);
        (hash % (self.offsets_topic_partitions as u64)) as i32
    }

    /// 处理 JoinGroup：加入成员，必要时触发 Rebalance。
    /// 返回 (error_code, generation_id, leader_id, member_id, protocol, members)。
    pub fn join_group(
        &self,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        member_id: &str,
        protocol_type: &str,
        protocols: Vec<(String, Bytes)>,
    ) -> (i16, i32, String, String, Option<String>, Vec<(String, Bytes)>) {
        let mut group = self
            .groups
            .entry(group_id.to_string())
            .or_insert_with(|| Group::new(group_id));

        let member_id = if member_id.is_empty() {
            format!("rivine-{}-{}", group_id, group.members.len() + 1)
        } else {
            member_id.to_string()
        };

        // 新成员加入或 rebalance
        if !group.members.contains_key(&member_id) {
            group.state = GroupState::PreparingRebalance;
            group.leader_id = None;
            group.protocol = None;
        }

        let member = Member {
            member_id: member_id.clone(),
            session_timeout_ms,
            rebalance_timeout_ms,
            protocols,
            last_heartbeat: Instant::now(),
            assignment: Bytes::new(),
            metadata: Bytes::new(),
        };
        group.members.insert(member_id.clone(), member);
        group.protocol_type = protocol_type.to_string();

        // 如果没有 leader，第一个成员成为 leader
        if group.leader_id.is_none() {
            group.leader_id = Some(member_id.clone());
        }

        // 选择协议（取第一个成员提供的第一个协议，简化为标准协议）
        let protocol = group
            .members
            .values()
            .next()
            .and_then(|m| m.protocols.first().map(|(n, _)| n.clone()));

        // 完成 Rebalance
        let members: Vec<(String, Bytes)> = group
            .members
            .values()
            .map(|m| (m.member_id.clone(), m.metadata.clone()))
            .collect();

        group.state = GroupState::CompletingRebalance;
        group.generation_id += 1;
        let generation = group.generation_id;
        let leader = group.leader_id.clone().unwrap_or_default();

        (0, generation, leader, member_id, protocol, members)
    }

    /// 处理 SyncGroup：leader 提交分配方案，广播给所有成员。
    /// 返回 (error_code, assignment)。
    pub fn sync_group(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        assignments: Vec<(String, Bytes)>,
    ) -> (i16, Bytes) {
        let Some(mut entry) = self.groups.get_mut(group_id) else {
            return (16, Bytes::new()); // NOT_COORDINATOR
        };
        let group = entry.value_mut();
        if group.generation_id != generation_id {
            return (22, Bytes::new()); // ILLEGAL_GENERATION
        }
        if !group.members.contains_key(member_id) {
            return (25, Bytes::new()); // UNKNOWN_MEMBER_ID
        }

        // 应用 leader 的分配方案
        if let Some(leader) = &group.leader_id {
            if leader == member_id {
                let map: HashMap<String, Bytes> = assignments.into_iter().collect();
                for (mid, assign) in &map {
                    if let Some(m) = group.members.get_mut(mid) {
                        m.assignment = assign.clone();
                    }
                }
            }
        }

        group.state = GroupState::Stable;
        let assignment = group.members.get(member_id).map(|m| m.assignment.clone()).unwrap_or_default();
        (0, assignment)
    }

    /// 处理 Heartbeat。
    pub fn heartbeat(&self, group_id: &str, generation_id: i32, member_id: &str) -> i16 {
        let Some(mut entry) = self.groups.get_mut(group_id) else {
            return 16; // NOT_COORDINATOR
        };
        let group = entry.value_mut();
        if group.generation_id != generation_id {
            return 22; // ILLEGAL_GENERATION
        }
        if let Some(member) = group.members.get_mut(member_id) {
            member.last_heartbeat = Instant::now();
            0
        } else {
            25 // UNKNOWN_MEMBER_ID
        }
    }

    /// 处理 LeaveGroup。
    pub fn leave_group(&self, group_id: &str, member_id: &str) -> i16 {
        let Some(mut entry) = self.groups.get_mut(group_id) else {
            return 16;
        };
        let group = entry.value_mut();
        group.members.remove(member_id);
        if group.members.is_empty() {
            group.state = GroupState::Empty;
        } else {
            group.state = GroupState::PreparingRebalance;
        }
        0
    }

    /// 提交偏移量（写入 __consumer_offsets 或内存表）。
    pub fn commit_offset(&self, group_id: &str, topic: &str, partition: i32, offset: i64) {
        // 单机实现：直接写入 __consumer_offsets 分区日志（由调用方处理）
        let _ = (group_id, topic, partition, offset);
    }

    /// 查询偏移量。
    pub fn fetch_offset(&self, group_id: &str, topic: &str, partition: i32) -> Option<i64> {
        let _ = (group_id, topic, partition);
        None
    }
}

/// 稳定字符串哈希（用于 Coordinator 分区分配）。
fn stable_hash(s: &str) -> u64 {
    let mut h: u64 = 0;
    for b in s.bytes() {
        h = h.wrapping_mul(31).wrapping_add(b as u64);
    }
    h
}
