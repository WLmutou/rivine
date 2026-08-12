# rivine

Apache Kafka® rewritten in Rust. A Kafka-compatible broker implemented from scratch in Rust.

## Features

### Phase 1: Protocol & Storage Design
- **Protocol codecs** (`src/protocol/`): hand-crafted binary protocol fully consistent with the official implementation
  - ApiVersions / Metadata / Produce / Fetch
  - Consumer groups: JoinGroup / SyncGroup / Heartbeat / LeaveGroup
  - Offset management: OffsetCommit / OffsetFetch / FindCoordinator
  - Admin: CreateTopics / DeleteTopics / ListOffsets / ListGroups / DescribeGroups
  - Compact string/bytes format and tagged fields support
- **RecordBatch** (`src/protocol/recordbatch.rs`): message batch codec (magic=2) with
  gzip / snappy / lz4 / zstd compression support, including multi-header encoding/decoding
- **Storage engine** (`src/storage/`):
  - Segmented storage: `.log` (messages) + `.index` (sparse offset index) + `.timeindex` (timestamp index)
  - Segment rolling policy (size / time thresholds)
  - Time/size-based retention cleanup and log compaction
  - Sequential write append with binary-search-based precise index lookup

### Phase 2: Core Broker Skeleton (Single Node)
- **Network layer** (`src/server/network.rs`): tokio async TCP, connection management, length-prefixed framing protocol
- **Request handler** (`src/server/handler.rs`): full API dispatch with request/response codecs
- **LogManager** (`src/server/metadata.rs`): topic/partition log management and metadata cache
- **Startup recovery**: scans the log directory to load segments and rebuild indexes

### Phase 3: Replication Protocol (Multi Node)
- **Raft** (`src/cluster/`): Follower/Candidate/Leader state machine, randomized election timeouts,
  RequestVote / AppendEntries RPCs, log replication and commit
- **Raft driver** (`src/cluster/driver.rs`): background async event loop, async vote collection
  (avoids in-process deadlock), Leader log commit and replication
- **In-memory transport** (`src/cluster/transport.rs`): shared-memory channel supporting multiple
  brokers within a single process
- **Cluster integration** (`tests/cluster_multi.rs`): multi-broker cluster elects a single leader,
  log replicated across nodes

### Phase 4: Consumer Group Protocol
- **GroupCoordinator** (`src/group/coordinator.rs`): JoinGroup / SyncGroup / Heartbeat /
  LeaveGroup, Rebalance state machine, Offset management, and member expiry cleanup

### Phase 5: Internal Topics
- Automatic creation of `__consumer_offsets` and `__cluster_metadata` internal topics
- **Offset persistence** (`src/group/offset_store.rs`): committed offsets written to
  `__consumer_offsets`, recovered on startup — offsets survive broker restarts

### Phase 6-7: Performance & Monitoring
- Metrics (Prometheus): request count / latency / message throughput / active connections
- Concurrency-limiting semaphore and dashmap for higher concurrent reads

### Producer Consistency
- **Idempotent producer** (`src/internals/idempotence.rs`): deduplication based on
  producer_id / sequence, detecting duplicate batches (retries) and out-of-order sequences,
  returning `DUPLICATE_SEQUENCE_NUMBER` / `OUT_OF_ORDER_SEQUENCE_NUMBER`
- **Produce acks semantics**: supports acks=0 (fire-and-forget, no response) and acks validation
- **Fetch long-poll**: implements max_wait_ms / min_bytes semantics and partition error codes

## Quick Start

```bash
# Build
cargo build --release

# Start (defaults to 127.0.0.1:9092)
./target/release/rivine-broker

# Start with a config file
./target/release/rivine-broker --config config/broker.toml.example

# Start with environment variables
RIVINE_PORT=9093 RIVINE_LOG_DIRS=/var/lib/rivine ./target/release/rivine-broker
```

## Testing

```bash
cargo test                       # Unit tests (protocol codec, storage, Raft, compression, idempotence, offset persistence)
cargo test --test integration    # Integration tests (TCP handshake + full Produce→Fetch flow)
cargo test --test cluster_multi  # Multi-broker cluster tests (Raft election + data replication)

# Stress tests (concurrency / throughput / data consistency)
cargo test --test stress -- --ignored --nocapture
```

Stress test loads can be tuned via environment variables
(`RIVINE_STRESS_CONNECTIONS`, `RIVINE_STRESS_MESSAGES`, `RIVINE_STRESS_VALUE_BYTES`).

## Project Structure

```
src/
├── protocol/   # Kafka protocol definitions and codecs
│   ├── primitive.rs    # Primitive codecs
│   ├── messages.rs     # Request/response messages
│   └── recordbatch.rs  # RecordBatch codec + compression
├── storage/    # Storage engine
│   ├── segment.rs      # LogSegment
│   ├── index.rs        # Offset/timestamp index
│   ├── log.rs          # PartitionLog
│   └── compaction.rs   # Log compaction
├── config/     # Configuration management
├── server/     # Network layer + request handling + LogManager
├── cluster/    # Raft driver + transport + Controller
│   ├── raft.rs         # Raft state machine
│   ├── driver.rs       # Raft background driver
│   ├── transport.rs    # In-memory / network transport
│   └── controller.rs   # Cluster Controller
├── group/      # Consumer group protocol + Offset persistence
│   ├── coordinator.rs  # Consumer group coordinator
│   └── offset_store.rs # Offset persistent store
├── internals/  # Internal topics + idempotent producer state
└── metrics/    # Monitoring
```

## Compatibility Verification

This implementation has been verified through real protocol interop with **Python
(kafka-python), Go (segmentio/kafka-go), and Rust (rdkafka)** standard clients, covering:

- **Producer**: all three clients can produce messages, including message header codec
  and RecordBatch serialization
- **Consumer**: all three clients can consume messages, including consumer group
  JoinGroup/SyncGroup/Fetch/OffsetCommit flows
- **Idempotent producer**: rdkafka (idempotence on by default) sends sequentially without
  `DUPLICATE_SEQUENCE_NUMBER` / `OUT_OF_ORDER`
- **Offset persistence**: committed offsets survive broker restarts

Implemented / refined protocol details:
- ListGroups / DescribeGroups administrative APIs
- ApiVersions v4, Produce acks=0 (fire-and-forget), Fetch long-poll (max_wait_ms/min_bytes)
- ListOffsets earliest/latest handling and partition error codes
- Offset Commit/Fetch identity validation (ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID / REBALANCE_IN_PROGRESS)
