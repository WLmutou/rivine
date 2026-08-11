//! 监控与 Metrics
//!
//! 使用 prometheus crate 暴露指标，提供 /metrics、/health、/ready 端点。
//! 全局指标注册表（进程内共享一份，避免重复注册冲突）。

use prometheus::{
    register_histogram_vec, register_int_counter, register_int_counter_vec, register_int_gauge,
    HistogramVec, IntCounter, IntCounterVec, IntGauge,
};
use std::sync::OnceLock;

/// 全局指标注册表
#[derive(Clone)]
pub struct Metrics {
    /// 收到的消息总数
    pub messages_in: IntCounter,
    /// 收到的字节数
    pub bytes_in: IntCounter,
    /// 发送的消息总数
    pub messages_out: IntCounter,
    /// 发送的字节数
    pub bytes_out: IntCounter,
    /// 请求总数（按 api）
    pub requests_total: IntCounterVec,
    /// 请求延迟直方图（按 api）
    pub request_latency: HistogramVec,
    /// ISR 变化次数
    pub isr_changes: IntCounter,
    /// Leader 选举次数
    pub leader_elections: IntCounter,
    /// 当前分区日志总大小（字节）
    pub log_size_bytes: IntGauge,
    /// 当前活跃连接数
    pub active_connections: IntGauge,
    /// 请求错误数
    pub request_errors: IntCounterVec,
}

impl Metrics {
    /// 获取进程级共享的 Metrics 实例（OnceLock 保证只注册一次）。
    pub fn global() -> &'static Metrics {
        static GLOBAL: OnceLock<Metrics> = OnceLock::new();
        GLOBAL.get_or_init(Self::create)
    }

    fn create() -> Self {
        Self {
            messages_in: register_int_counter!("rivine_messages_in_total", "接收消息总数").unwrap(),
            bytes_in: register_int_counter!("rivine_bytes_in_total", "接收字节数").unwrap(),
            messages_out: register_int_counter!("rivine_messages_out_total", "发送消息总数").unwrap(),
            bytes_out: register_int_counter!("rivine_bytes_out_total", "发送字节数").unwrap(),
            requests_total: register_int_counter_vec!("rivine_requests_total", "请求总数", &["api"])
                .unwrap(),
            request_latency: register_histogram_vec!(
                "rivine_request_latency_seconds",
                "请求延迟",
                &["api"]
            )
            .unwrap(),
            isr_changes: register_int_counter!("rivine_isr_changes_total", "ISR 变化次数").unwrap(),
            leader_elections: register_int_counter!("rivine_leader_elections_total", "Leader 选举次数")
                .unwrap(),
            log_size_bytes: register_int_gauge!("rivine_log_size_bytes", "日志总大小").unwrap(),
            active_connections: register_int_gauge!("rivine_active_connections", "活跃连接数").unwrap(),
            request_errors: register_int_counter_vec!(
                "rivine_request_errors_total",
                "请求错误数",
                &["api"]
            )
            .unwrap(),
        }
    }
}

/// 健康检查状态
#[derive(Debug, Clone, serde::Serialize)]
pub struct HealthStatus {
    pub status: &'static str,
    pub version: &'static str,
}

impl Default for HealthStatus {
    fn default() -> Self {
        Self {
            status: "ok",
            version: env!("CARGO_PKG_VERSION"),
        }
    }
}
