//! Raft 驱动
//!
//! 在后台运行一个 Raft 节点：参与 Leader 选举、接收/复制日志、推进 commit_index。
//! 通过 `MemTransport` 与同进程的其他节点通信（用于测试与单进程多 broker）。
//!
//! 选举采用完全异步的事件循环：在等待自己发起的投票应答时，仍能处理其他节点
//! 发来的请求（避免进程内同步阻塞导致的死锁）。

use super::raft::{AppendEntries, LogEntry, NodeRole, RaftNode, RequestVote};
use super::transport::{MemTransport, Rpc, RpcResult};
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// 提交后的应用回调：参数为已提交的日志条目。
pub type ApplyFn = Arc<dyn Fn(&[LogEntry]) + Send + Sync>;

/// 共享的驱动状态（供外部读取 leader/任期信息）。
pub struct DriverState {
    pub is_leader: AtomicBool,
    pub current_term: AtomicU64,
}

impl DriverState {
    pub fn new() -> Self {
        Self {
            is_leader: AtomicBool::new(false),
            current_term: AtomicU64::new(0),
        }
    }
}

/// Raft 驱动：持有节点状态、事件通道与对外接口。
pub struct RaftDriver {
    id: u64,
    peers: Vec<u64>,
    transport: MemTransport,
    rpc_rx: tokio::sync::mpsc::UnboundedReceiver<Rpc>,
    /// 提交数据的通道（由 leader 应用并复制）。
    propose_tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    /// 接收端（启动时移入后台任务）。
    propose_rx: Option<tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>>,
    /// 共享状态（启动后由后台任务更新）。
    pub state: Arc<DriverState>,
    /// 后台任务句柄。
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl RaftDriver {
    /// 创建驱动并注册到传输。需调用 `start` 启动后台循环。
    pub fn new(id: u64, peers: Vec<u64>, transport: MemTransport) -> Self {
        let rpc_rx = transport.register(id);
        let (propose_tx, propose_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            id,
            peers,
            transport,
            rpc_rx,
            propose_tx,
            propose_rx: Some(propose_rx),
            state: Arc::new(DriverState::new()),
            handle: None,
        }
    }

    /// 向 Leader 提交一条数据（仅 Leader 会应用并复制）。
    /// 若非 Leader 或未启动，返回 false。
    pub fn propose(&self, data: Vec<u8>) -> bool {
        self.propose_tx.send(data).is_ok()
    }

    /// 当前节点是否为 Leader。
    pub fn is_leader(&self) -> bool {
        self.state.is_leader.load(Ordering::SeqCst)
    }

    /// 当前节点 id。
    pub fn id(&self) -> u64 {
        self.id
    }

    /// 启动后台 Raft 循环。可调用多次，仅首次生效。
    pub fn start(&mut self, apply: ApplyFn) {
        if self.handle.is_some() {
            return;
        }
        let mut node = RaftNode::new(self.id, self.peers.clone());
        let state = self.state.clone();
        let transport = self.transport.clone();
        let mut rpc_rx = std::mem::replace(
            &mut self.rpc_rx,
            tokio::sync::mpsc::unbounded_channel().1,
        );
        let mut propose_rx = self.propose_rx.take().expect("propose_rx 已被启动");
        let mut tick = tokio::time::interval(Duration::from_millis(80));

        let handle = tokio::spawn(async move {
            // 进行中的 RPC 应答。
            let mut pending: FuturesUnordered<
                std::pin::Pin<Box<tokio::sync::oneshot::Receiver<RpcResult>>>,
            > = FuturesUnordered::new();
            // 当前选举已获得的票数（含自身一票）。
            let mut granted_votes: usize = 0;
            let mut election_term: u64 = 0;

            loop {
                tokio::select! {
                    rpc = rpc_rx.recv() => {
                        if let Some(rpc) = rpc {
                            Self::handle_rpc(&mut node, rpc);
                        }
                    }
                    // 收到待复制的数据：仅 Leader 追加到日志。
                    data = propose_rx.recv() => {
                        if let Some(data) = data {
                            if node.role == NodeRole::Leader {
                                node.append_log(data);
                            }
                        }
                    }
                    _ = tick.tick() => {
                        Self::run_tick(
                            &mut node, &transport, &apply, &state,
                            &mut pending, &mut election_term, &mut granted_votes,
                        );
                    }
                    // 有新的待应答 RPC 完成。
                    res = pending.next(), if !pending.is_empty() => {
                        if let Some(Ok(result)) = res {
                            Self::process_reply(&mut node, &mut election_term, &mut granted_votes, result);
                        }
                    }
                }
            }
        });
        self.handle = Some(handle);
    }

    /// 停止驱动。
    pub fn shutdown(&self) {
        if let Some(h) = &self.handle {
            h.abort();
        }
    }

    fn handle_rpc(node: &mut RaftNode, rpc: Rpc) {
        if let Some(req) = rpc.vote {
            let result = node.handle_request_vote(&req);
            let _ = rpc.reply.send(RpcResult::Vote(result));
            return;
        }
        if let Some(req) = rpc.append {
            let ok = node.handle_append_entries(&req);
            let _ = rpc.reply.send(RpcResult::Append(ok));
        }
    }

    fn run_tick(
        node: &mut RaftNode,
        transport: &MemTransport,
        apply: &ApplyFn,
        state: &Arc<DriverState>,
        pending: &mut FuturesUnordered<std::pin::Pin<Box<tokio::sync::oneshot::Receiver<RpcResult>>>>,
        election_term: &mut u64,
        granted_votes: &mut usize,
    ) {
        if node.role == NodeRole::Leader {
            // 心跳 + 日志复制。
            for peer in &node.peers.clone() {
                let req = AppendEntries {
                    term: node.current_term,
                    leader_id: node.id,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: node.log.clone(),
                    leader_commit: node.commit_index,
                };
                let _ = transport.send_append(*peer, req);
            }
            state.is_leader.store(true, Ordering::SeqCst);
            state.current_term.store(node.current_term, Ordering::SeqCst);
            // 简化：Leader 将 commit_index 推进到日志末尾（假设副本均已收到）。
            if let Some(last) = node.log.last() {
                node.commit_index = node.commit_index.max(last.index);
            }
        } else {
            // 非 Leader 时清除 leader 标记（避免旧 leader 残留导致多 leader 判定）。
            state.is_leader.store(false, Ordering::SeqCst);
            if node.election_timeout_expired() {
            // 发起选举：发送投票请求，将应答加入 pending。
            node.role = NodeRole::Candidate;
            node.current_term += 1;
            node.voted_for = Some(node.id);
            *election_term = node.current_term;
            *granted_votes = 1; // 自身一票。
            let last_log_index = node.log.last().map(|e| e.index).unwrap_or(0);
            let last_log_term = node.log.last().map(|e| e.term).unwrap_or(0);
            let req = RequestVote {
                term: node.current_term,
                candidate_id: node.id,
                last_log_index,
                last_log_term,
            };
            for peer in &node.peers.clone() {
                if let Some(rx) = transport.send_vote(*peer, req.clone()) {
                    pending.push(Box::pin(rx));
                }
            }
            node.reset_election_timeout();
            // 无 peer（单节点）时立即当选。
            Self::try_become_leader(node, granted_votes, *election_term);
            }
        }

        // 推进 last_applied 并触发业务回调。
        let mut i = node.last_applied + 1;
        let mut to_apply = Vec::new();
        while i <= node.commit_index {
            if let Some(e) = node.log.iter().find(|e| e.index == i) {
                to_apply.push(e.clone());
            }
            i += 1;
        }
        if !to_apply.is_empty() {
            node.last_applied = to_apply.last().map(|e| e.index).unwrap_or(node.last_applied);
            apply(&to_apply);
        }
    }

    /// 判断是否获得多数票并成为 leader。
    fn try_become_leader(node: &mut RaftNode, granted_votes: &usize, term: u64) {
        if node.role != NodeRole::Candidate || term != node.current_term {
            return;
        }
        let majority = (node.peers.len() + 1) / 2 + 1;
        if *granted_votes >= majority {
            node.role = NodeRole::Leader;
            node.voted_for = None;
            tracing::debug!(
                "节点 {} 在任期 {} 当选 leader (得票 {}/{})",
                node.id,
                node.current_term,
                granted_votes,
                node.peers.len() + 1
            );
        }
    }

    fn process_reply(
        node: &mut RaftNode,
        election_term: &mut u64,
        granted_votes: &mut usize,
        result: RpcResult,
    ) {
        match result {
            RpcResult::Vote(v) => {
                if *election_term == v.term && v.granted && node.role == NodeRole::Candidate {
                    *granted_votes += 1;
                    Self::try_become_leader(node, granted_votes, *election_term);
                }
            }
            RpcResult::Append(_) => {}
        }
        node.reset_election_timeout();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 构建一个 N 节点内存集群。
    fn make_cluster(nodes: &[u64]) -> (MemTransport, Vec<RaftDriver>) {
        let transport = MemTransport::new();
        let mut drivers = Vec::new();
        for &id in nodes {
            let peers: Vec<u64> = nodes.iter().copied().filter(|p| *p != id).collect();
            drivers.push(RaftDriver::new(id, peers, transport.clone()));
        }
        (transport, drivers)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_leader_election_runs() {
        let ids = [1u64, 2, 3];
        let (_transport, mut drivers) = make_cluster(&ids);
        for d in &mut drivers {
            d.start(Arc::new(|_: &[LogEntry]| {}));
        }
        tokio::time::sleep(Duration::from_millis(3500)).await;
        let leaders = drivers.iter().filter(|d| d.is_leader()).count();
        assert_eq!(leaders, 1, "3 节点集群应恰好选举出 1 个 leader，实际 {leaders}");
        for d in &drivers {
            d.shutdown();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_single_node_is_leader() {
        let ids = [1u64];
        let (_transport, mut drivers) = make_cluster(&ids);
        let d = drivers.get_mut(0).unwrap();
        d.start(Arc::new(|_: &[LogEntry]| {}));
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(d.is_leader(), "单节点应成为 leader");
        d.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_leader_log_replicated_to_followers() {
        // 3 节点集群：Leader propose 的数据应复制到所有 follower 的日志。
        let ids = [1u64, 2, 3];
        let (_transport, mut drivers) = make_cluster(&ids);

        // 每个节点记录其应用过的日志数据。
        let applied: Vec<Arc<std::sync::Mutex<Vec<String>>>> = (0..3)
            .map(|_| Arc::new(std::sync::Mutex::new(Vec::new())))
            .collect();

        for (i, d) in drivers.iter_mut().enumerate() {
            let sink = applied[i].clone();
            d.start(Arc::new(move |entries: &[LogEntry]| {
                for e in entries {
                    sink.lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&e.data).to_string());
                }
            }));
        }

        // 等待选举完成。
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let leader_idx = drivers.iter().position(|d| d.is_leader()).unwrap();

        // Leader propose 一条数据。
        let proposed = format!("replicated-msg-{}", 1);
        assert!(
            drivers[leader_idx].propose(proposed.as_bytes().to_vec()),
            "Leader propose 应成功"
        );

        // 等待日志复制。
        tokio::time::sleep(Duration::from_millis(1200)).await;

        // 所有节点（含 follower）都应应用了该数据。
        for (i, sink) in applied.iter().enumerate() {
            let has = sink.lock().unwrap().iter().any(|s| s == &proposed);
            let node_id = ids[i];
            assert!(has, "节点 {node_id} 应复制并应用数据: {proposed}");
        }

        for d in &drivers {
            d.shutdown();
        }
    }
}
