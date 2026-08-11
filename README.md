# rivine

Apache Kafka® rewritten in Rust. A Kafka-compatible broker implemented from scratch in Rust.

## Features

### Phase 1: Protocol & Storage Design
- **Protocol codecs** (`src/protocol/`): hand-crafted binary protocol fully consistent with the official implementation
  - ApiVersions / Metadata / Produce / Fetch
  - Consumer groups: JoinGroup / SyncGroup / Heartbeat / LeaveGroup
  - Offset management: OffsetCommit / OffsetFetch / FindCoordinator
  - Admin: CreateTopics / DeleteTopics / ListOffsets
  - Compact string/bytes format and tagged fields support
- **RecordBatch** (`src/protocol/recordbatch.rs`): message batch codec (magic=2) with
  gzip / snappy / lz4 / zstd compression support
- **Storage engine** (`src/storage/`):
  - Segmented storage: `.log` (messages) + `.index` (sparse offset index) + `.timeindex` (timestamp index)
  - Segment rolling policy (size / time thresholds)
  - Time/size-based retention cleanup and log compaction
  - Sequential write append with binary-search-based precise index lookup

### Phase 2: Core Broker Skeleton (Single Node)
- **Network layer** (`src/server/network.rs`): tokio async TCP, connection management, length-prefixed framing protocol
- **Request handler** (`src/server/handler.rs`): ApiVersions / Metadata / Produce / Fetch
- **LogManager** (`src/server/metadata.rs`): topic/partition log management and metadata cache
- **Startup recovery**: scans the log directory to load segments and rebuild indexes

### Phase 3: Replication Protocol (Multi Node)
- **KRaft / custom Raft** (`src/cluster/raft.rs`): Follower/Candidate/Leader state machine,
  randomized election timeouts, RequestVote / AppendEntries RPCs, log replication and commit

### Phase 4: Consumer Group Protocol
- **GroupCoordinator** (`src/group/coordinator.rs`): JoinGroup / SyncGroup / Heartbeat /
  LeaveGroup, Rebalance state machine, and Offset management

### Phase 5: Internal Topics
- Automatic creation of `__consumer_offsets` and `__cluster_metadata` internal topics

### Phase 6-7: Performance & Monitoring
- Metrics (Prometheus): request count / latency / message throughput / active connections
- Concurrency-limiting semaphore and dashmap for higher concurrent reads

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
cargo test                       # Unit tests (protocol codec, storage, Raft, compression)
cargo test --test integration    # Integration tests (TCP handshake + full Produce→Fetch flow)

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
├── cluster/    # Raft/KRaft + Controller
├── group/      # Consumer group protocol 
├── internals/  # Internal topics 
└── metrics/    # Monitoring
```

## Compatibility Notes

This implementation is a Kafka-compatible broker for learning purposes. It prioritizes
correctness of the core data path (Produce/Fetch/storage), verified through integration tests covering:
- TCP handshake (ApiVersions)
- Message production and consumption (full Produce→Fetch flow, including RecordBatch serialization and storage read-back)

Multi-node replication (Raft) and consumer groups provide a foundational implementation that
can be further extended in `single_node=false` mode.
