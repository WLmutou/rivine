//! 副本协议与集群元数据
//!
//! - 3.1 Controller 与元数据集群：KRaft（自定义 Raft）
//! - 3.2 副本同步协议：ISR、Follower 拉取
//! - 3.3 Leader 选举与故障转移

pub mod raft;
pub mod controller;

pub use controller::ClusterController;
pub use raft::{NodeRole, RaftNode};
