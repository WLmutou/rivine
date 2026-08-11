# rivine 
一个用 Rust 重写的 Apache Kafka®.用 Rust 实现一个 Kafka 兼容的 Broker。

## 特性

### 第 1 阶段：协议与存储设计
- **协议编解码**（`src/protocol/`）：手工实现与官方完全一致的二进制协议
  - ApiVersions / Metadata / Produce / Fetch
  - 消费者组：JoinGroup / SyncGroup / Heartbeat / LeaveGroup
  - Offset 管理：OffsetCommit / OffsetFetch / FindCoordinator
  - Admin：CreateTopics / DeleteTopics / ListOffsets
  - 紧凑格式（compact string/bytes）与 tagged fields 支持
- **RecordBatch**（`src/protocol/recordbatch.rs`）：消息批次编解码（magic=2），支持
  gzip / snappy / lz4 / zstd 压缩
- **存储引擎**（`src/storage/`）：
  - 分段存储：`.log`（消息）+ `.index`（偏移量稀疏索引）+ `.timeindex`（时间戳索引）
  - Segment 滚动策略（大小 / 时间阈值）
  - 基于时间 / 大小的保留清理，日志压缩（compact）
  - 顺序写追加，索引二分查找精确定位

### 第 2 阶段：核心 Broker 骨架（单机）
- **网络层**（`src/server/network.rs`）：tokio 异步 TCP，连接管理，长度前缀帧协议
- **请求处理器**（`src/server/handler.rs`）：ApiVersions / Metadata / Produce / Fetch
- **LogManager**（`src/server/metadata.rs`）：主题/分区日志管理与元数据缓存
- **启动恢复**：扫描日志目录加载 Segment，重建索引

### 第 3 阶段：副本协议（多机）
- **KRaft / 自定义 Raft**（`src/cluster/raft.rs`）：Follower/Candidate/Leader 状态机、
  随机选举超时、RequestVote / AppendEntries RPC、日志复制与提交

### 第 4 阶段：消费者组协议
- **GroupCoordinator**（`src/group/coordinator.rs`）：JoinGroup / SyncGroup / Heartbeat /
  LeaveGroup，Rebalance 状态机，Offset 管理

### 第 5 阶段：内部主题
- 自动创建 `__consumer_offsets`、`__cluster_metadata` 内部主题

### 第 6-7 阶段：性能与监控
- Metrics（prometheus）：请求数 / 延迟 / 消息吞吐 / 活跃连接
- 并发限制信号量，dashmap 提升并发读

## 快速开始

```bash
# 构建
cargo build --release

# 启动（默认 127.0.0.1:9092）
./target/release/rivine-broker

# 使用配置文件
./target/release/rivine-broker --config config/broker.toml.example

# 使用环境变量
RIVINE_PORT=9093 RIVINE_LOG_DIRS=/var/lib/rivine ./target/release/rivine-broker
```

## 测试

```bash
cargo test          # 单元测试（协议编解码、存储、Raft、压缩）
cargo test --test integration   # 集成测试（TCP 握手 + Produce→Fetch 全链路）

# 压力测试（并发 / 吞吐 / 数据一致性）
cargo test --test stress -- --ignored --nocapture
```

压力测试负载可通过环境变量调节（`RIVINE_STRESS_CONNECTIONS`、`RIVINE_STRESS_MESSAGES`、`RIVINE_STRESS_VALUE_BYTES`）。

## 项目结构

```
src/
├── protocol/   # Kafka 协议定义与编解码
│   ├── primitive.rs    # 原语编解码器
│   ├── messages.rs     # 请求/响应消息
│   └── recordbatch.rs  # RecordBatch 编解码 + 压缩
├── storage/    # 存储引擎
│   ├── segment.rs      # LogSegment
│   ├── index.rs        # 偏移量/时间戳索引
│   ├── log.rs          # PartitionLog
│   └── compaction.rs   # 日志压缩
├── config/     # 配置管理
├── server/     # 网络层 + 请求处理 + LogManager
├── cluster/    # Raft/KRaft + Controller
├── group/      # 消费者组协议
├── internals/  # 内部主题
└── metrics/    # 监控
```

## 兼容性说明

本实现是一个用于学习的 Kafka 兼容 Broker，优先保证核心数据路径
（Produce/Fetch/存储）的正确性，并通过集成测试验证了：
- TCP 握手（ApiVersions）
- 消息生产与消费（Produce→Fetch 完整链路，含 RecordBatch 序列化与存储回读）

多机副本（Raft）与消费者组提供了基础实现，可在 `single_node=false` 模式下进一步扩展。
