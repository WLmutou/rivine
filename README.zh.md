# rivine

一个用 Rust 重写的 Apache Kafka®。用 Rust 实现一个 Kafka 兼容的 Broker。

## 特性

### 第 1 阶段：协议与存储设计
- **协议编解码**（`src/protocol/`）：手工实现与官方完全一致的二进制协议
  - ApiVersions / Metadata / Produce / Fetch
  - 消费者组：JoinGroup / SyncGroup / Heartbeat / LeaveGroup
  - Offset 管理：OffsetCommit / OffsetFetch / FindCoordinator
  - Admin：CreateTopics / DeleteTopics / ListOffsets / ListGroups / DescribeGroups
  - 紧凑格式（compact string/bytes）与 tagged fields 支持
- **RecordBatch**（`src/protocol/recordbatch.rs`）：消息批次编解码（magic=2），支持
  gzip / snappy / lz4 / zstd 压缩，含多 header 编解码
- **存储引擎**（`src/storage/`）：
  - 分段存储：`.log`（消息）+ `.index`（偏移量稀疏索引）+ `.timeindex`（时间戳索引）
  - Segment 滚动策略（大小 / 时间阈值）
  - 基于时间 / 大小的保留清理，日志压缩（compact）
  - 顺序写追加，索引二分查找精确定位

### 第 2 阶段：核心 Broker 骨架（单机）
- **网络层**（`src/server/network.rs`）：tokio 异步 TCP，连接管理，长度前缀帧协议
- **请求处理器**（`src/server/handler.rs`）：完整 API 分发与请求/响应编解码
- **LogManager**（`src/server/metadata.rs`）：主题/分区日志管理与元数据缓存
- **启动恢复**：扫描日志目录加载 Segment，重建索引

### 第 3 阶段：副本协议（多机）
- **Raft**（`src/cluster/`）：Follower/Candidate/Leader 状态机、随机选举超时、
  RequestVote / AppendEntries RPC、日志复制与提交
- **Raft 驱动**（`src/cluster/driver.rs`）：后台异步事件循环，异步选举投票（避免进程内死锁），
  Leader 日志提交与复制
- **内存传输**（`src/cluster/transport.rs`）：进程内共享内存通道，支持单进程内启动多 broker
- **集群集成**（`tests/cluster_multi.rs`）：多 broker 集群选举唯一 leader、日志跨节点复制

### 第 4 阶段：消费者组协议
- **GroupCoordinator**（`src/group/coordinator.rs`）：JoinGroup / SyncGroup / Heartbeat /
  LeaveGroup，Rebalance 状态机，Offset 管理与成员过期清理

### 第 5 阶段：内部主题
- 自动创建 `__consumer_offsets`、`__cluster_metadata` 内部主题
- **Offset 持久化**（`src/group/offset_store.rs`）：将已提交偏移量写入 `__consumer_offsets`，
  启动时扫描恢复，broker 重启后偏移量不丢失

### 第 6-7 阶段：性能与监控
- Metrics（prometheus）：请求数 / 延迟 / 消息吞吐 / 活跃连接
- 并发限制信号量，dashmap 提升并发读

### 生产一致性
- **幂等生产者**（`src/internals/idempotence.rs`）：基于 producer_id / sequence 去重，
  支持重复批次（重试）与乱序检测，返回 `DUPLICATE_SEQUENCE_NUMBER` /
  `OUT_OF_ORDER_SEQUENCE_NUMBER`
- **Produce acks 语义**：支持 acks=0（fire-and-forget，不返回响应）与 acks 校验
- **Fetch long-poll**：实现 max_wait_ms / min_bytes 语义，分区错误码处理

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
cargo test          # 单元测试（协议编解码、存储、Raft、压缩、幂等、Offset 持久化）
cargo test --test integration    # 集成测试（TCP 握手 + Produce→Fetch 全链路）
cargo test --test cluster_multi  # 多 broker 集群测试（Raft 选举 + 数据复制）

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
├── cluster/    # Raft 驱动 + 传输 + Controller
│   ├── raft.rs         # Raft 状态机
│   ├── driver.rs       # Raft 后台驱动
│   ├── transport.rs    # 内存/网络传输
│   └── controller.rs   # 集群 Controller
├── group/      # 消费者组协议 + Offset 持久化
│   ├── coordinator.rs  # 消费者组协调器
│   └── offset_store.rs # Offset 持久化存储
├── internals/  # 内部主题 + 幂等生产者状态
└── metrics/    # 监控
```

## 兼容性验证

本实现通过 **Python（kafka-python）、Go（segmentio/kafka-go）、Rust（rdkafka）** 三种标准
客户端进行了真实协议互操作验证，覆盖：

- **Producer**：三种客户端均可生产消息，包含消息 header 编解码、RecordBatch 序列化
- **Consumer**：三种客户端均可消费消息，包含消费者组 JoinGroup/SyncGroup/Fetch/OffsetCommit
- **幂等生产者**：rdkafka（默认启用幂等）连续发送无 `DUPLICATE_SEQUENCE_NUMBER` / `OUT_OF_ORDER`
- **Offset 持久化**：提交的偏移量在 broker 重启后可恢复

已实现/完善的协议细节：
- ListGroups / DescribeGroups 管理 API
- ApiVersions v4、Produce acks=0（fire-and-forget）、Fetch long-poll（max_wait_ms/min_bytes）
- ListOffsets 的 earliest/latest 与分区错误码
- Offset Commit/Fetch 的身份校验（ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID / REBALANCE_IN_PROGRESS）
