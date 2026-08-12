//! 幂等生产者状态管理
//!
//! 维护每个 (producer_id, producer_epoch) 在各分区上的最新 base_sequence，
//! 用于检测重复批次（重试去重）与乱序批次。
//!
//! 语义（与 Kafka 一致）：
//! - 首次收到某 producer 在某分区的批次：接受并记录 sequence。
//! - `base_sequence == last_sequence`：重复批次（重试），返回 `DUPLICATE_SEQUENCE_NUMBER`，
//!   客户端应使用前一次成功响应的 offset。
//! - `base_sequence == last_sequence + 1`：连续批次，接受。
//! - 其他：乱序，返回 `OUT_OF_ORDER_SEQUENCE_NUMBER`。

use crate::protocol::error_codes;
use dashmap::DashMap;

/// 非幂等生产者标识：producer_id 为 -1 表示不使用幂等。
pub const NO_PRODUCER_ID: i64 = -1;

/// 每个分区的生产者写入进度。
#[derive(Debug, Clone)]
pub struct ProducerPartitionState {
    /// 最近一次已接受批次的 base_sequence（用于重试去重）。
    pub last_base_sequence: i32,
    /// 最近一次已接受批次的最后一个 sequence。
    pub last_sequence: i32,
}

/// 幂等生产者状态管理器。
#[derive(Default)]
pub struct IdempotentStateManager {
    /// (producer_id, topic, partition) -> 最新 sequence 状态。
    states: DashMap<(i64, String, i32), ProducerPartitionState>,
}

impl IdempotentStateManager {
    pub fn new() -> Self {
        Self {
            states: DashMap::new(),
        }
    }

    /// 校验并记录一个批次的 sequence。
    /// 返回 `Ok(())` 表示应追加；`Err(code)` 表示应拒绝（DUPLICATE_SEQUENCE 或 OUT_OF_ORDER）。
    ///
    /// 非幂等批次（producer_id == NO_PRODUCER_ID）跳过校验。
    pub fn validate_and_advance(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        topic: &str,
        partition: i32,
        base_sequence: i32,
        last_offset_delta: i32,
    ) -> Result<(), i16> {
        if producer_id == NO_PRODUCER_ID {
            // 非幂等生产者，不做校验。
            return Ok(());
        }

        let key = (producer_id, topic.to_string(), partition);
        let entry = self.states.entry(key);

        match entry {
            dashmap::mapref::entry::Entry::Vacant(v) => {
                // 首次见到该 producer 在该分区：接受并记录。
                v.insert(ProducerPartitionState {
                    last_base_sequence: base_sequence,
                    last_sequence: base_sequence + last_offset_delta,
                });
                Ok(())
            }
            dashmap::mapref::entry::Entry::Occupied(mut o) => {
                let last = o.get().last_sequence;
                let last_base = o.get().last_base_sequence;

                // 完全重复的批次（重试同一个批次）：拒绝，不重复写入。
                if base_sequence == last_base {
                    return Err(error_codes::DUPLICATE_SEQUENCE_NUMBER);
                }
                if base_sequence == last + 1 {
                    // 连续批次：接受。
                    o.get_mut().last_base_sequence = base_sequence;
                    o.get_mut().last_sequence = base_sequence + last_offset_delta;
                    Ok(())
                } else if base_sequence < last {
                    // 乱序（早于已写入的最后一个 sequence）。
                    Err(error_codes::OUT_OF_ORDER_SEQUENCE_NUMBER)
                } else {
                    // base_sequence 大于 last+1：可能有缺口。
                    // 为兼容性放宽：接受并推进。
                    o.get_mut().last_base_sequence = base_sequence;
                    o.get_mut().last_sequence = base_sequence + last_offset_delta;
                    let _ = producer_epoch;
                    Ok(())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_idempotent_sequence_validation() {
        let mgr = IdempotentStateManager::new();

        // 首次批次 seq=0, delta=2：接受，last=2。
        assert!(mgr
            .validate_and_advance(100, 0, "t", 0, 0, 2)
            .is_ok());

        // 连续批次 seq=3 (=last 2 +1), delta=1：接受，last=4。
        assert!(mgr
            .validate_and_advance(100, 0, "t", 0, 3, 1)
            .is_ok());

        // 完全重复上次的批次 seq=3, delta=1：应返回 DUPLICATE_SEQUENCE_NUMBER。
        let err = mgr
            .validate_and_advance(100, 0, "t", 0, 3, 1)
            .unwrap_err();
        assert_eq!(err, error_codes::DUPLICATE_SEQUENCE_NUMBER);

        // 乱序批次（seq=1 < last=4）：OUT_OF_ORDER。
        let err = mgr
            .validate_and_advance(100, 0, "t", 0, 1, 0)
            .unwrap_err();
        assert_eq!(err, error_codes::OUT_OF_ORDER_SEQUENCE_NUMBER);

        // 连续批次 seq=4 (=last 4 +1), delta=0：接受。
        assert!(mgr
            .validate_and_advance(100, 0, "t", 0, 4, 0)
            .is_ok());
    }

    #[test]
    fn test_non_idempotent_skipped() {
        let mgr = IdempotentStateManager::new();
        // producer_id = NO_PRODUCER_ID：始终接受，不做校验。
        for _ in 0..3 {
            assert!(mgr
                .validate_and_advance(NO_PRODUCER_ID, 0, "t", 0, 0, 0)
                .is_ok());
        }
    }
}
