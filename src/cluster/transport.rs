//! 集群节点间通信
//!
//! `MemTransport`：进程内共享内存通道，用于单进程内启动多个 broker 的测试，
//! 也作为真实 TCP 传输的抽象基础。

use crate::cluster::raft::{AppendEntries, RequestVote, VoteResult};
use tokio::sync::{mpsc, oneshot};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// 一次 RPC 调用。
pub struct Rpc {
    pub vote: Option<RequestVote>,
    pub append: Option<AppendEntries>,
    pub reply: oneshot::Sender<RpcResult>,
}

/// RPC 结果。
pub enum RpcResult {
    Vote(VoteResult),
    Append(bool),
}

/// 内存传输：管理节点间通道，用于进程内多 broker 测试。
#[derive(Clone)]
pub struct MemTransport {
    /// 节点 id -> 发送通道。
    nodes: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<Rpc>>>>,
}

impl Default for MemTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTransport {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 注册一个节点，返回该节点的接收队列（驱动任务消费）。
    pub fn register(&self, node_id: u64) -> mpsc::UnboundedReceiver<Rpc> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.nodes.lock().unwrap().insert(node_id, tx);
        rx
    }

    /// 向指定节点发送 RequestVote RPC，返回应答接收器。
    pub fn send_vote(&self, target: u64, req: RequestVote) -> Option<oneshot::Receiver<RpcResult>> {
        let tx = self.nodes.lock().unwrap().get(&target).cloned()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(Rpc {
            vote: Some(req),
            append: None,
            reply: reply_tx,
        })
        .ok()?;
        Some(reply_rx)
    }

    /// 向指定节点发送 AppendEntries RPC，返回应答接收器。
    pub fn send_append(&self, target: u64, req: AppendEntries) -> Option<oneshot::Receiver<RpcResult>> {
        let tx = self.nodes.lock().unwrap().get(&target).cloned()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(Rpc {
            vote: None,
            append: Some(req),
            reply: reply_tx,
        })
        .ok()?;
        Some(reply_rx)
    }
}
