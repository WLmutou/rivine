//! 压力测试：验证 Broker 在高并发、高吞吐下的稳定性与数据一致性。
//!
//! 由于压力测试较耗时，默认被标记为 `#[ignore]`，需要显式运行：
//!
//! ```bash
//! cargo test --test stress -- --ignored --nocapture
//! ```
//!
//! 可通过环境变量调节负载（默认值对普通机器友好）：
//! - RIVINE_STRESS_CONNECTIONS：并发 TCP 连接数（默认 16）
//! - RIVINE_STRESS_MESSAGES：每个连接发送的消息数（默认 500）
//! - RIVINE_STRESS_VALUE_BYTES：每条消息 value 的大小（默认 128）

use bytes::{BufMut, Bytes, BytesMut};
use rivine::protocol::recordbatch::{Compression, Record, RecordBatch};
use rivine::protocol::{Decoder, Encoder};
use rivine::Broker;
use rivine::BrokerConfig;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// 从环境变量读取整数配置，缺失时使用默认值。
fn env_int(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// 通过 TCP 发送一个完整请求（body 不带长度前缀），返回完整响应字节。
fn send_raw(port: i32, body: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).expect("连接 broker");
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(15)))
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

/// 构建 Produce v3 请求，写入一个 RecordBatch，value 为 `{prefix}-{seq}`。
fn build_produce_request(correlation_id: i32, topic: &str, partition: i32, value: &str) -> Vec<u8> {
    let mut e = Encoder::new();
    e.put_i16(0); // Produce
    e.put_i16(3);
    e.put_i32(correlation_id);
    e.put_i16(-1); // client_id null
    e.put_unsigned_varint(0); // header v2 tagged fields

    e.put_nullable_compact_string(None); // transactional_id
    e.put_i16(1); // acks
    e.put_i32(5000); // timeout_ms
    e.put_i32(1); // topic 数
    e.put_string(topic);
    e.put_i32(1); // partition 数
    e.put_i32(partition);
    let record = Record {
        attributes: 0,
        timestamp_delta: 0,
        offset_delta: 0,
        key: Some(Bytes::from("k")),
        value: Some(Bytes::from(value.as_bytes().to_vec())),
        headers: vec![],
    };
    let batch = RecordBatch::serialize(0, vec![record], Compression::None, 1_700_000_000_000, 0, 0, 0, 0);
    e.put_i32(batch.len() as i32);
    e.put_bytes(&batch);
    e.into_bytes().to_vec()
}

/// 构建 Fetch v4 请求，从指定 offset 取回消息。
fn build_fetch_request(correlation_id: i32, topic: &str, partition: i32, offset: i64) -> Vec<u8> {
    let mut e = Encoder::new();
    e.put_i16(1); // Fetch
    e.put_i16(4);
    e.put_i32(correlation_id);
    e.put_i16(-1); // client_id null
    e.put_unsigned_varint(0); // header v2 tagged

    e.put_i32(-1); // replica_id
    e.put_i32(5000); // max_wait_ms
    e.put_i32(1); // min_bytes
    e.put_i32(64 * 1024 * 1024); // max_bytes
    e.put_i8(0); // isolation_level
    e.put_i32(1); // topic 数
    e.put_string(topic);
    e.put_i32(1); // partition 数
    e.put_i32(partition);
    e.put_i64(offset);
    e.put_i32(64 * 1024 * 1024); // partition_max_bytes
    e.into_bytes().to_vec()
}

/// 从 Produce 响应解析 error_code 和 base_offset。
fn parse_produce_response(resp: &[u8]) -> (i16, i64) {
    assert!(resp.len() >= 4, "Produce 响应过短");
    let mut d = Decoder::new(Bytes::from(resp[4..].to_vec()));
    let _n_topics = d.get_i32().unwrap();
    let _topic = d.get_string().unwrap();
    let _n_part = d.get_i32().unwrap();
    let _partition = d.get_i32().unwrap();
    let error_code = d.get_i16().unwrap();
    let base_offset = d.get_i64().unwrap();
    (error_code, base_offset)
}

/// 从 Fetch 响应中累计取回的 record value 集合。
fn collect_fetch_values(resp: &[u8]) -> Vec<String> {
    let mut values = Vec::new();
    let mut d = Decoder::new(Bytes::from(resp[4..].to_vec()));
    let _throttle = d.get_i32().unwrap();
    let n_topics = d.get_i32().unwrap();
    for _ in 0..n_topics {
        let _topic = d.get_string().unwrap();
        let n_part = d.get_i32().unwrap();
        for _ in 0..n_part {
            let _partition = d.get_i32().unwrap();
            let _err = d.get_i16().unwrap();
            let _hw = d.get_i64().unwrap();
            let _lso = d.get_i64().unwrap();
            let records_len = d.get_i32().unwrap();
            if records_len == 0 {
                continue;
            }
            let records = d.get_bytes(records_len as usize).unwrap();
            let mut reader = rivine::protocol::recordbatch::RecordBatchReader::new(Bytes::from(records.to_vec()));
            while let Some((batch, _raw)) = reader.next_batch().unwrap() {
                for r in batch.records {
                    if let Some(v) = r.value {
                        values.push(String::from_utf8(v.to_vec()).unwrap());
                    }
                }
            }
        }
    }
    values
}

/// 启动测试 broker，返回 (broker, port)。
async fn start_test_broker(port: i32, tag: &str) -> Broker {
    let mut cfg = BrokerConfig::default();
    cfg.port = port;
    cfg.log_dirs = vec![
        std::env::temp_dir()
            .join(format!("rivine-stress-{tag}"))
            .to_string_lossy()
            .to_string(),
    ];
    let _ = std::fs::remove_dir_all(std::env::temp_dir().join(format!("rivine-stress-{tag}")));
    let broker = Broker::new(cfg);
    let handle = broker.clone();
    tokio::spawn(async move {
        let _ = handle.run().await;
    });
    let addr = format!("127.0.0.1:{port}");
    let mut ready = false;
    for _ in 0..100 {
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

/// 并发 Produce：`connections` 个线程各发送 `messages` 条，统计吞吐与成功率。
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "压力测试，需显式运行"]
async fn stress_concurrent_produce() {
    let connections = env_int("RIVINE_STRESS_CONNECTIONS", 16);
    let messages = env_int("RIVINE_STRESS_MESSAGES", 500);
    let value_bytes = env_int("RIVINE_STRESS_VALUE_BYTES", 128);

    let port = 19500;
    let _broker = start_test_broker(port, "produce").await;
    let topic = "stress-produce";
    _broker.metadata.create_topic(topic, 1, false).expect("创建主题失败");

    let value_payload = "x".repeat(value_bytes);
    let total_expected = connections * messages;
    let success_count = Arc::new(AtomicUsize::new(0));
    // 记录所有连接拿到的 base_offset，用于校验全局无重复、无丢失
    let offsets = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));

    let start = Instant::now();
    let mut handles = Vec::new();
    for c in 0..connections {
        let payload = value_payload.clone();
        let success = success_count.clone();
        let offsets = offsets.clone();
        let topic = topic.to_string();
        handles.push(std::thread::spawn(move || {
            for m in 0..messages {
                let corr = (c * 1_000_000 + m) as i32;
                let value = format!("{payload}-{c}-{m}");
                let req = build_produce_request(corr, &topic, 0, &value);
                let resp = send_raw(port, &req);
                let (err, base_offset) = parse_produce_response(&resp);
                assert_eq!(err, 0, "Produce 返回错误: {err}");
                offsets.lock().unwrap().push(base_offset);
                success.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();

    let succeeded = success_count.load(Ordering::Relaxed);
    assert_eq!(succeeded, total_expected, "Produce 应全部成功");
    // 并发写同一分区：base_offset 全局应恰好覆盖 [0, total_expected)，无重复无空洞
    let mut collected = offsets.lock().unwrap().clone();
    collected.sort();
    let expected_offsets: Vec<i64> = (0..total_expected as i64).collect();
    assert_eq!(collected, expected_offsets, "分区偏移量应无重复、无丢失、严格连续");
    let msgs_per_sec = succeeded as f64 / elapsed.as_secs_f64();
    let reqs_per_sec = msgs_per_sec; // 每条 Produce 承载 1 条消息
    println!(
        "并发 Produce 完成: {succeeded} 条 / {elapsed:.2?}s, 吞吐 ~{:.0} msg/s ({:.0} req/s)",
        msgs_per_sec, reqs_per_sec
    );
}

/// 混合读写：生产者并发写入，随后校验全部消息可完整 Fetch 回来。
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "压力测试，需显式运行"]
async fn stress_produce_then_consume_all() {
    let connections = env_int("RIVINE_STRESS_CONNECTIONS", 16);
    let messages = env_int("RIVINE_STRESS_MESSAGES", 300);
    let value_bytes = env_int("RIVINE_STRESS_VALUE_BYTES", 128);

    let port = 19501;
    let _broker = start_test_broker(port, "consume").await;
    let topic = "stress-consume";
    _broker.metadata.create_topic(topic, 1, false).expect("创建主题失败");

    let value_payload = "y".repeat(value_bytes);
    let total_expected = connections * messages;
    let success_count = Arc::new(AtomicUsize::new(0));
    let produced_values = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    // 并发生产
    let start = Instant::now();
    let mut handles = Vec::new();
    for c in 0..connections {
        let payload = value_payload.clone();
        let success = success_count.clone();
        let values = produced_values.clone();
        let topic = topic.to_string();
        handles.push(std::thread::spawn(move || {
            let mut local = Vec::new();
            for m in 0..messages {
                let corr = (c * 1_000_000 + m) as i32;
                let value = format!("{payload}-{c}-{m}");
                let req = build_produce_request(corr, &topic, 0, &value);
                let resp = send_raw(port, &req);
                let (err, _) = parse_produce_response(&resp);
                assert_eq!(err, 0, "Produce 返回错误: {err}");
                local.push(value);
                success.fetch_add(1, Ordering::Relaxed);
            }
            values.lock().unwrap().extend(local);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let produce_elapsed = start.elapsed();
    let succeeded = success_count.load(Ordering::Relaxed);
    assert_eq!(succeeded, total_expected, "Produce 应全部成功");
    println!("生产阶段: {succeeded} 条 / {produce_elapsed:.2?}s");

    // 消费全部消息并校验集合一致
    let mut fetched: Vec<String> = Vec::new();
    let mut offset = 0i64;
    let mut loops = 0;
    loop {
        loops += 1;
        assert!(loops < 1000, "消费循环过多，疑似死循环");
        let req = build_fetch_request(-1, &topic, 0, offset);
        let resp = send_raw(port, &req);
        let batch = collect_fetch_values(&resp);
        if batch.is_empty() {
            break;
        }
        fetched.extend(batch);
        offset = fetched.len() as i64;
    }

    assert_eq!(fetched.len(), total_expected, "Fetch 回的消息数量应与写入一致");
    let mut expected: Vec<String> = produced_values.lock().unwrap().clone();
    let mut actual = fetched.clone();
    expected.sort();
    actual.sort();
    assert_eq!(expected, actual, "消息内容应一一对应");
    println!("消费校验: 取回 {} 条, 全部一致, 共 {loops} 次 Fetch 循环", fetched.len());
}

/// 压缩批次的并发吞吐（验证压缩路径在高负载下稳定）。
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "压力测试，需显式运行"]
async fn stress_produce_compressed() {
    let connections = env_int("RIVINE_STRESS_CONNECTIONS", 8);
    let messages = env_int("RIVINE_STRESS_MESSAGES", 400);
    let value_bytes = env_int("RIVINE_STRESS_VALUE_BYTES", 256);

    let port = 19502;
    let _broker = start_test_broker(port, "compressed").await;
    let topic = "stress-compressed";
    _broker.metadata.create_topic(topic, 1, false).expect("创建主题失败");

    let value_payload = "z".repeat(value_bytes);
    let total_expected = connections * messages;
    let success_count = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();
    let mut handles = Vec::new();
    for c in 0..connections {
        let payload = value_payload.clone();
        let success = success_count.clone();
        let topic = topic.to_string();
        handles.push(std::thread::spawn(move || {
            for m in 0..messages {
                let corr = (c * 1_000_000 + m) as i32;
                let value = format!("{payload}-{c}-{m}");
                // 使用 gzip 压缩的 RecordBatch 发送
                let mut e = Encoder::new();
                e.put_i16(0);
                e.put_i16(3);
                e.put_i32(corr);
                e.put_i16(-1);
                e.put_unsigned_varint(0);
                e.put_nullable_compact_string(None);
                e.put_i16(1);
                e.put_i32(5000);
                e.put_i32(1);
                e.put_string(&topic);
                e.put_i32(1);
                e.put_i32(0);
                let record = Record {
                    attributes: 0,
                    timestamp_delta: 0,
                    offset_delta: 0,
                    key: Some(Bytes::from("k")),
                    value: Some(Bytes::from(value.as_bytes().to_vec())),
                    headers: vec![],
                };
                let batch = RecordBatch::serialize(0, vec![record], Compression::Gzip, 1_700_000_000_000, 0, 0, 0, 0);
                e.put_i32(batch.len() as i32);
                e.put_bytes(&batch);
                let req = e.into_bytes().to_vec();
                let resp = send_raw(port, &req);
                let (err, _) = parse_produce_response(&resp);
                assert_eq!(err, 0, "压缩 Produce 返回错误: {err}");
                success.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();
    let succeeded = success_count.load(Ordering::Relaxed);
    assert_eq!(succeeded, total_expected, "压缩 Produce 应全部成功");
    let msgs_per_sec = succeeded as f64 / elapsed.as_secs_f64();
    println!(
        "压缩(gzip)并发 Produce 完成: {succeeded} 条 / {elapsed:.2?}s, 吞吐 ~{:.0} msg/s",
        msgs_per_sec
    );
}
