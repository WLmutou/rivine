//! 消费者组协议
//!
//! - 4.1 Group Coordinator：__consumer_offsets 内部主题
//! - 4.2 消费者组成员管理：JoinGroup / SyncGroup / Heartbeat / LeaveGroup
//! - 4.3 Offset 管理：OffsetCommit / OffsetFetch
//! - 4.4 Rebalance 协议：状态机 Empty → PreparingRebalance → CompletingRebalance → Stable

pub mod coordinator;

pub use coordinator::GroupCoordinator;
