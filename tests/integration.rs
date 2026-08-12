//! 集成测试：启动 Broker，使用裸 TCP 客户端模拟 Kafka 协议交互。
//! 验证 ApiVersions 握手、Produce→Fetch 完整读写链路（存储引擎）。

use bytes::{BufMut, Bytes, BytesMut};
use rivine::protocol::recordbatch::{Compression, Record, RecordBatch};
use rivine::protocol::{Decoder, Encoder};
use rivine::Broker;
use rivine::BrokerConfig;
use std::io::{Read, Write};
use std::net::TcpStream;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("debug"))
        .try_init();
}

/// 通过 TCP 发送一个完整请求（body 不带长度前缀），返回完整响应字节。
fn send_raw(port: i32, body: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).expect("连接 broker");
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let mut frame = BytesMut::with_capacity(4 + body.len());
    frame.put_i32(body.len() as i32);
    frame.put_slice(body);
    stream.write_all(&frame).unwrap();
    stream.flush().unwrap();

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut resp = vec![0u8; len];
    stream.read_exact(&mut resp).unwrap();
    resp
}

/// 构建请求头（传统格式，无 tagged fields），返回 Encoder（header 已写入）。
fn request_header(e: &mut Encoder, api_key: i16, api_version: i16, correlation_id: i32) {
    e.put_i16(api_key);
    e.put_i16(api_version);
    e.put_i32(correlation_id);
    e.put_i16(-1); // client_id = null
}

/// ApiVersions v3 请求
fn build_api_versions_request(correlation_id: i32) -> Vec<u8> {
    let mut e = Encoder::new();
    request_header(&mut e, 18, 3, correlation_id);
    let name = "test-client";
    e.put_compact_string(name);
    e.put_compact_string(name);
    e.put_unsigned_varint(0); // tagged fields
    e.into_bytes().to_vec()
}

/// 构建 Produce v3 请求，写入一个 RecordBatch。
fn build_produce_request(correlation_id: i32, topic: &str, partition: i32, value: &str) -> Vec<u8> {
    let mut e = Encoder::new();
    // Produce v3 为非 flexible，请求头为 header v1（不带 tagged fields）
    e.put_i16(0); // Produce
    e.put_i16(3);
    e.put_i32(correlation_id);
    e.put_i16(-1); // client_id null

    // body v3（非紧凑格式）：transactional_id 为传统 nullable string
    e.put_nullable_string(None); // transactional_id
    e.put_i16(1); // acks
    e.put_i32(1000); // timeout_ms
    // topic_data 数组（v3 非紧凑）
    e.put_i32(1); // 1 个 topic
    e.put_string(topic);
    e.put_i32(1); // 1 个 partition
    e.put_i32(partition);
    // records: 构造 RecordBatch
    let record = Record {
        attributes: 0,
        timestamp_delta: 0,
        offset_delta: 0,
        key: Some(Bytes::from("k")),
        value: Some(Bytes::from(value.as_bytes().to_vec())),
        headers: vec![],
    };
    let batch = RecordBatch::serialize(0, vec![record], Compression::None, 1_700_000_000_000, 0, 0, 0, 0);
    e.put_i32(batch.len() as i32); // records 长度（v3 非紧凑）
    e.put_bytes(&batch);
    e.into_bytes().to_vec()
}

/// 构建 Fetch v4 请求。
fn build_fetch_request(correlation_id: i32, topic: &str, partition: i32, offset: i64) -> Vec<u8> {
    let mut e = Encoder::new();
    e.put_i16(1); // Fetch
    e.put_i16(4);
    e.put_i32(correlation_id);
    e.put_i16(-1); // client_id null
    // Fetch v4 为非 flexible，请求头为 header v1（不带 tagged fields）

    // body v4
    e.put_i32(-1); // replica_id
    e.put_i32(100); // max_wait_ms
    e.put_i32(1); // min_bytes
    e.put_i32(100 * 1024 * 1024); // max_bytes
    e.put_i8(0); // isolation_level
    e.put_i32(1); // topic 数
    e.put_string(topic);
    e.put_i32(1); // partition 数
    e.put_i32(partition);
    e.put_i64(offset);
    e.put_i32(1024 * 1024); // partition_max_bytes
    e.into_bytes().to_vec()
}

/// 启动一个测试 broker（使用指定端口）。
async fn start_test_broker(port: i32, tag: &str) -> Broker {
    let mut cfg = BrokerConfig::default();
    cfg.port = port;
    cfg.log_dirs = vec![
        std::env::temp_dir()
            .join(format!("rivine-itest-{tag}"))
            .to_string_lossy()
            .to_string(),
    ];
    let _ = std::fs::remove_dir_all(std::env::temp_dir().join(format!("rivine-itest-{tag}")));
    let broker = Broker::new(cfg);
    let handle = broker.clone();
    tokio::spawn(async move {
        let _ = handle.run().await;
    });
    let addr = format!("127.0.0.1:{port}");
    let mut ready = false;
    for _ in 0..50 {
        if TcpStream::connect(&addr).is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    if !ready {
        panic!("broker 在 {addr} 未就绪");
    }
    broker
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_api_versions_over_tcp() {
    let _broker = start_test_broker(19192, "tcp").await;
    let req = build_api_versions_request(42);
    let resp = send_raw(19192, &req);
    assert!(resp.len() > 6, "响应太短: {}", resp.len());
    let correlation_id = i32::from_be_bytes(resp[0..4].try_into().unwrap());
    assert_eq!(correlation_id, 42);
    let error_code = i16::from_be_bytes(resp[4..6].try_into().unwrap());
    assert_eq!(error_code, 0, "ApiVersions 应返回成功");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_starts_and_creates_internal_topics() {
    let broker = start_test_broker(19193, "internal").await;
    let topics = broker.metadata.topic_list();
    let names: Vec<String> = topics.iter().map(|t| t.name.clone()).collect();
    assert!(
        names.contains(&"__consumer_offsets".to_string()),
        "应自动创建 __consumer_offsets，实际: {names:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_produce_then_fetch() {
    init_tracing();
    let broker = start_test_broker(19194, "pf").await;
    // 先创建主题 test-topic（1 分区）
    broker
        .metadata
        .create_topic("test-topic", 1, false)
        .expect("创建主题失败");

    // Produce
    let prod_req = build_produce_request(1, "test-topic", 0, "hello-rivine");
    let prod_resp = send_raw(19194, &prod_req);
    // 响应: corr(4) + topics array
    let corr = i32::from_be_bytes(prod_resp[0..4].try_into().unwrap());
    assert_eq!(corr, 1);
    // 解析 topic -> partition -> error_code(2), base_offset(8)
    let mut d = Decoder::new(Bytes::from(prod_resp[4..].to_vec()));
    let n_topics = d.get_i32().unwrap();
    assert_eq!(n_topics, 1);
    let topic = d.get_string().unwrap();
    assert_eq!(topic, "test-topic");
    let n_part = d.get_i32().unwrap();
    assert_eq!(n_part, 1);
    let _partition = d.get_i32().unwrap();
    let error_code = d.get_i16().unwrap();
    assert_eq!(error_code, 0, "Produce 应成功");
    let base_offset = d.get_i64().unwrap();
    assert_eq!(base_offset, 0, "首条消息偏移量应为 0");

    // Fetch
    let fetch_req = build_fetch_request(2, "test-topic", 0, 0);
    let fetch_resp = send_raw(19194, &fetch_req);
    let corr_fetch = i32::from_be_bytes(fetch_resp[0..4].try_into().unwrap());
    assert_eq!(corr_fetch, 2);
    // 跳过 correlation_id，解析响应体
    let mut d2 = Decoder::new(Bytes::from(fetch_resp[4..].to_vec()));
    let _throttle = d2.get_i32().unwrap();
    let n_topics = d2.get_i32().unwrap();
    assert_eq!(n_topics, 1);
    let topic = d2.get_string().unwrap();
    assert_eq!(topic, "test-topic");
    let n_part = d2.get_i32().unwrap();
    assert_eq!(n_part, 1);
    let _partition = d2.get_i32().unwrap();
    let err = d2.get_i16().unwrap();
    assert_eq!(err, 0, "Fetch 应成功");
    let _hw = d2.get_i64().unwrap();
    let _lso = d2.get_i64().unwrap();
    // Fetch v4 不包含 log_start_offset（v5+ 才有），但包含 aborted_transactions(v4+)
    let n_aborted = d2.get_i32().unwrap();
    assert_eq!(n_aborted, 0, "无事务，应无 aborted_transactions");
    let records_len = d2.get_i32().unwrap();
    assert!(records_len > 0, "应取回消息数据，records_len={records_len}");
    let records = d2.get_bytes(records_len as usize).unwrap();

    // 解析 RecordBatch，验证值
    let batch = RecordBatch::parse(records).unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].value.as_ref().unwrap(), "hello-rivine");
    assert_eq!(batch.records[0].key.as_ref().unwrap(), "k");
}
