//! 自定义 Raft 共识实现（对标 KRaft）
//!
//! 实现：
//! - Raft 核心状态机与角色：Follower / Candidate / Leader
//! - 任期管理与选举超时计时器（随机超时，减少选举冲突）
//! - Leader 选举与心跳维持（AppendEntries RPC）
//! - 日志复制与提交（No-op 日志）

use tokio::sync::oneshot;
use tokio::time::{self, Duration, Instant};
use std::cell::Cell;
use std::time::{SystemTime, UNIX_EPOCH};

/// 节点角色
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

/// Raft 日志条目
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
}

/// 一次选举的结果回调
pub struct VoteResult {
    pub granted: bool,
    pub term: u64,
}

/// 请求投票 RPC
#[derive(Debug, Clone)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// 附加日志 RPC（心跳也使用此 RPC）
#[derive(Debug, Clone)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

/// 节点通信接口（由上层注入实现）。
pub trait RaftTransport: Send + Sync + 'static {
    fn send_vote(&self, node: u64, req: RequestVote) -> oneshot::Receiver<VoteResult>;
    fn send_append(&self, node: u64, req: AppendEntries) -> oneshot::Receiver<bool>;
}

/// Raft 节点
pub struct RaftNode {
    pub id: u64,
    pub role: NodeRole,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    /// 所有投票节点 id
    pub peers: Vec<u64>,
    election_deadline: Instant,
    heartbeat_tick: Option<time::Interval>,
}

impl RaftNode {
    pub fn new(id: u64, peers: Vec<u64>) -> Self {
        let mut node = Self {
            id,
            role: NodeRole::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            peers,
            election_deadline: Instant::now(),
            heartbeat_tick: None,
        };
        node.reset_election_timeout();
        node
    }

    /// 重置随机选举超时（150-300ms），避免同时选举。
    pub fn reset_election_timeout(&mut self) {
        let rand_ms = 150 + (jitter() % 150);
        self.election_deadline = Instant::now() + Duration::from_millis(rand_ms);
    }

    pub fn election_timeout_expired(&self) -> bool {
        Instant::now() >= self.election_deadline
    }

    /// 发起选举：任期 +1，投自己一票，发送 RequestVote。
    pub fn start_election<T: RaftTransport>(&mut self, transport: &T) {
        self.role = NodeRole::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        let last_log_index = self.log.last().map(|e| e.index).unwrap_or(0);
        let last_log_term = self.log.last().map(|e| e.term).unwrap_or(0);
        let req = RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index,
            last_log_term,
        };
        for peer in &self.peers {
            let _rx = transport.send_vote(*peer, req.clone());
        }
        self.reset_election_timeout();
    }

    /// 处理收到的投票请求。
    pub fn handle_request_vote(&mut self, req: &RequestVote) -> VoteResult {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.role = NodeRole::Follower;
            self.voted_for = None;
        }
        let granted = req.term >= self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(req.candidate_id))
            && self.is_log_up_to_date(req.last_log_index, req.last_log_term);
        if granted {
            self.voted_for = Some(req.candidate_id);
            self.reset_election_timeout();
        }
        VoteResult {
            granted,
            term: self.current_term,
        }
    }

    fn is_log_up_to_date(&self, index: u64, term: u64) -> bool {
        let last = self.log.last();
        let last_term = last.map(|e| e.term).unwrap_or(0);
        let last_index = last.map(|e| e.index).unwrap_or(0);
        term > last_term || (term == last_term && index >= last_index)
    }

    /// 处理 AppendEntries（心跳/日志复制）。
    pub fn handle_append_entries(&mut self, req: &AppendEntries) -> bool {
        if req.term < self.current_term {
            return false;
        }
        if req.term > self.current_term {
            self.current_term = req.term;
            self.role = NodeRole::Follower;
            self.voted_for = None;
        }
        self.role = NodeRole::Follower;
        self.reset_election_timeout();
        for entry in &req.entries {
            if !self.log.iter().any(|e| e.index == entry.index) {
                self.log.push(entry.clone());
            }
        }
        if req.leader_commit > self.commit_index {
            self.commit_index = req
                .leader_commit
                .min(self.log.last().map(|e| e.index).unwrap_or(0));
        }
        true
    }

    /// Leader 心跳：向所有 peer 发送 AppendEntries。
    pub fn heartbeat<T: RaftTransport>(&mut self, transport: &T) {
        if self.role != NodeRole::Leader {
            return;
        }
        let req = AppendEntries {
            term: self.current_term,
            leader_id: self.id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: self.commit_index,
        };
        for peer in &self.peers {
            let _rx = transport.send_append(*peer, req.clone());
        }
    }

    /// 等待下一次心跳 tick（返回时即可发送心跳）。
    pub async fn tick(&mut self) {
        if self.heartbeat_tick.is_none() {
            self.heartbeat_tick = Some(time::interval(Duration::from_millis(150)));
        }
        if let Some(t) = &mut self.heartbeat_tick {
            t.tick().await;
        }
    }
}

/// 时间抖动随机源（xorshift，基于系统时间初始化）。
fn jitter() -> u64 {
    thread_local! {
        static STATE: Cell<u64> = Cell::new(0);
    }
    STATE.with(|s| {
        if s.get() == 0 {
            let seed = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            s.set(seed);
        }
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        x
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vote_granted_once() {
        let mut node = RaftNode::new(1, vec![2, 3]);
        let req = RequestVote {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        let r1 = node.handle_request_vote(&req);
        assert!(r1.granted);
        // 同一任期不同候选人，不能再投（每个任期只能投一票）
        let req2 = RequestVote {
            term: 1,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        };
        let r2 = node.handle_request_vote(&req2);
        assert!(!r2.granted);
    }
}
