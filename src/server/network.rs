//! 网络层与 Broker 主循环
//!
//! - tokio 异步 TCP Listener
//! - 连接管理：每个客户端连接生成一个 Handler 任务
//! - 请求读取循环：读取 4 字节长度前缀 → 完整请求体 → 解析请求头 → 路由
//! - 使用 tokio::io::BufReader 减少系统调用

use super::handler::RequestHandler;
use super::metadata::MetadataManager;
use crate::cluster::ClusterController;
use crate::config::BrokerConfig;
use crate::group::GroupCoordinator;
use crate::internals::InternalTopics;
use crate::metrics::Metrics;
use bytes::Bytes;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

/// Kafka Broker
#[derive(Clone)]
pub struct Broker {
    pub config: Arc<BrokerConfig>,
    pub metadata: Arc<MetadataManager>,
    pub groups: Arc<GroupCoordinator>,
    pub controller: Arc<ClusterController>,
    pub metrics: Arc<Metrics>,
}

impl Broker {
    pub fn new(config: BrokerConfig) -> Self {
        let config = Arc::new(config);
        let metadata = Arc::new(MetadataManager::new(config.clone()));
        let groups = Arc::new(GroupCoordinator::new(metadata.clone()));
        let controller = Arc::new(ClusterController::new(config.clone()));
        // Metrics 为进程级共享实例
        let metrics: Arc<Metrics> = Arc::new(Metrics::global().clone());
        Self {
            config,
            metadata,
            groups,
            controller,
            metrics,
        }
    }

    /// 启动 Broker：初始化内部主题、恢复日志，然后监听端口。
    pub async fn run(&self) -> anyhow::Result<()> {
        // 2.4 启动恢复：扫描日志目录加载所有 Segment
        self.metadata.init()?;

        // 第 5 节：创建内部主题
        let internals = InternalTopics::new(self.metadata.clone());
        internals.ensure_created();
        self.groups.init();
        // 启动组成员过期清理后台任务。
        self.groups.spawn_expiry_cleanup();

        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        tracing::info!("rivine Broker 已监听 {addr} (broker_id={})", self.config.broker_id);

        // 并发限制信号量（第 6.2 节，防止 OOM）
        let semaphore = Arc::new(Semaphore::new(1024));

        let metadata = self.metadata.clone();
        let groups = self.groups.clone();
        let metrics = self.metrics.clone();

        loop {
            let (socket, peer) = listener.accept().await?;
            tracing::debug!("新连接: {peer}");
            let _permit = semaphore.clone().acquire_owned().await?;
            metrics.active_connections.inc();
            let metadata = metadata.clone();
            let groups = groups.clone();
            let metrics_conn = metrics.clone();
            let metrics_dec = metrics.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(metadata, groups, metrics_conn, socket).await {
                    tracing::warn!("连接处理结束/出错: {e}");
                }
                metrics_dec.active_connections.dec();
            });
        }
    }
}

/// 处理单个连接：读取请求，路由到 handler，写回响应。
async fn handle_connection(
    metadata: Arc<MetadataManager>,
    groups: Arc<GroupCoordinator>,
    metrics: Arc<Metrics>,
    socket: TcpStream,
) -> anyhow::Result<()> {
    let (read_half, write_half) = socket.into_split();
    let mut reader = BufReader::new(read_half);
    let mut writer = write_half;
    let handler = RequestHandler::new(metadata, groups, metrics);

    let mut buf = Vec::new();
    loop {
        // 1. 读取 4 字节长度前缀
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break Ok(()); // 客户端关闭
            }
            Err(e) => return Err(e.into()),
        }
        let body_len = u32::from_be_bytes(len_buf) as usize;
        if body_len == 0 || body_len > 100 * 1024 * 1024 {
            return Err(anyhow::anyhow!("非法的请求长度 {body_len}"));
        }
        // 2. 读取完整请求体
        buf.resize(body_len, 0);
        reader.read_exact(&mut buf).await?;
        let body = Bytes::copy_from_slice(&buf);

        // 3. 处理请求并获取响应。
        //    `None` 表示该请求按协议约定不返回响应（如 Produce acks=0，fire-and-forget）。
        let response = handler.process(body).await;

        // 4. 写回响应（4 字节长度前缀 + 响应体）。
        let Some(response) = response else {
            continue;
        };
        let resp_len = response.len() as u32;
        writer.write_all(&resp_len.to_be_bytes()).await?;
        writer.write_all(&response).await?;
        writer.flush().await?;
    }
}
