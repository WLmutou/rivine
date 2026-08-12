use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 配置生产者，指定 Kafka 集群地址
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()?;

    // 2. 构建并发送消息，等待发送结果
    let delivery_status = producer
        .send(
            FutureRecord::to("my-topic-rust")
                .key("key-1")
                .payload("Hello, Kafka from rust-rdkafka!")
                .headers(
                    rdkafka::message::OwnedHeaders::new().insert(rdkafka::message::Header {
                        key: "my-header",
                        value: Some("value"),
                    }),
                ),
            Timeout::Never, // 永不过期，一直等待
        )
        .await;

    // 3. 检查发送结果
    match delivery_status {
        Ok((partition, offset)) => {
            println!("消息发送成功! 分区: {}, 偏移量: {}", partition, offset);
        }
        Err((e, _)) => eprintln!("消息发送失败: {}", e),
    }

    Ok(())
}