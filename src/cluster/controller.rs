//! Cluster Controller（KRaft 模式）
//!
//! Controller 角色由 Raft Leader 承担，负责：
//! - 元数据变更（主题创建/删除、分区分配）
//! - 维护 ISR 列表
//! - Leader 选举与故障转移
//!
//! 单机模式下（single_node=true）Controller 即本 Broker。
//! 多机模式下（single_node=false）通过 Raft 维护集群一致性，
//! 由共享的 `RaftState` 反映当前 leader 与任期。

use super::driver::{DriverState, RaftDriver};
use super::transport::MemTransport;
use crate::config::BrokerConfig;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// 共享的 Raft 状态，由驱动任务更新、上层读取。
pub struct RaftState {
    /// 当前集群 leader 的节点 id（0 表示未知/选举中）。
    pub leader_id: AtomicU64,
    /// 当前任期。
    pub current_term: AtomicU64,
    /// 驱动是否已启动。
    pub started: AtomicBool,
}

impl RaftState {
    pub fn new() -> Self {
        Self {
            leader_id: AtomicU64::new(0),
            current_term: AtomicU64::new(0),
            started: AtomicBool::new(false),
        }
    }
}

impl Default for RaftState {
    fn default() -> Self {
        Self::new()
    }
}

/// 集群 Controller
pub struct ClusterController {
    pub config: Arc<BrokerConfig>,
    /// 共享 Raft 状态（多机模式使用）。
    pub raft_state: Arc<RaftState>,
    /// 驱动共享状态（多机模式，is_controller 直接读取）。
    driver_state: std::sync::Mutex<Option<Arc<DriverState>>>,
}

impl ClusterController {
    pub fn new(config: Arc<BrokerConfig>) -> Self {
        Self {
            config,
            raft_state: Arc::new(RaftState::new()),
            driver_state: std::sync::Mutex::new(None),
        }
    }

    /// 本节点是否为 Controller（单机模式恒为 true；多机模式为 Raft leader）。
    pub fn is_controller(&self) -> bool {
        if self.config.single_node {
            return true;
        }
        if let Ok(guard) = self.driver_state.lock() {
            if let Some(ds) = guard.as_ref() {
                return ds.is_leader.load(Ordering::SeqCst);
            }
        }
        let leader = self.raft_state.leader_id.load(Ordering::SeqCst);
        leader == self.config.broker_id as u64 && leader != 0
    }

    /// 处理主题创建（记录到元数据日志）。
    pub fn on_topic_created(&self, topic: &str, partitions: i32, rf: i16) {
        tracing::info!(
            "元数据变更: 创建主题 {topic}, 分区={partitions}, 副本因子={rf}"
        );
    }

    /// 在多机模式下启动本节点的 Raft 驱动。
    /// 返回是否成功启动（单机模式不启动）。
    pub fn start_raft(
        &self,
        transport: MemTransport,
        all_nodes: Vec<u64>,
    ) -> bool {
        if self.config.single_node {
            return false;
        }
        if self
            .raft_state
            .started
            .swap(true, Ordering::SeqCst)
        {
            return false;
        }
        let id = self.config.broker_id as u64;
        let peers: Vec<u64> = all_nodes.iter().copied().filter(|p| *p != id).collect();
        let mut driver = RaftDriver::new(id, peers, transport);
        let state = driver.state.clone();
        // 保存驱动状态供 is_controller 读取。
        *self.driver_state.lock().unwrap() = Some(state.clone());
        let raft_state = self.raft_state.clone();
        driver.start(Arc::new(move |_entries: &[crate::cluster::raft::LogEntry]| {
            // 将驱动状态同步到共享 RaftState（供外部诊断使用）。
            raft_state
                .leader_id
                .store(if state.is_leader.load(Ordering::SeqCst) {
                    id
                } else {
                    0
                }, Ordering::SeqCst);
            raft_state
                .current_term
                .store(state.current_term.load(Ordering::SeqCst), Ordering::SeqCst);
        }));
        // 保存驱动（仅用于持有，避免被 drop）。start 已在后台启动任务。
        std::mem::forget(driver);
        true
    }
}
