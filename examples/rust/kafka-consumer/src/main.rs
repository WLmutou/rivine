use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 配置消费者，指定消费者组ID
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "my-rust-group")
        .set("auto.offset.reset", "earliest") // 从头开始消费
        .create()?;

    // 2. 订阅主题
    consumer.subscribe(&["my-topic-rust"])?;

    println!("开始消费消息...");

    // 3. 从消息流中拉取并处理消息
    loop {
        match consumer.recv().await {
            Ok(msg) => {
                // 处理消息
                let key = msg.key_view::<str>().map(Result::ok).flatten().unwrap_or("");
                let value = msg.payload_view::<str>().map(Result::ok).flatten().unwrap_or("");
                println!("收到消息: key={:?}, value={:?}", key, value);
                // 注意：在 `rust-rdkafka` 中，消息提交是自动管理的。
                // 你也可以通过 `consumer.store_offset(msg)` 进行手动控制以实现更精确的语义。
            }
            Err(e) => eprintln!("消费消息出错: {}", e),
        }
    }
}
