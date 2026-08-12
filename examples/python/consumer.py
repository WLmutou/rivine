from kafka import KafkaConsumer
import json

print("start consumer....")
# 创建消费者
consumer = KafkaConsumer(
    'test-topic',  # 主题
    bootstrap_servers=['localhost:9092'],
    group_id='my-group',  # 消费者组
    auto_offset_reset='earliest',  # 从最早的消息开始消费
    enable_auto_commit=True,  # 自动提交偏移量
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),  # 反序列化
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

# 方式一：循环拉取消息
print("开始消费消息...")
for msg in consumer:
    print(f"收到消息: topic={msg.topic}, partition={msg.partition}, offset={msg.offset}")
    print(f"  key={msg.key}, value={msg.value}")
    # 处理消息逻辑...

# 方式二：手动拉取（poll模式）
while True:
    records = consumer.poll(timeout_ms=1000)  # 1秒超时
    for topic_partition, messages in records.items():
        for msg in messages:
            print(f"收到: {msg.value}")
    # 可以自定义提交偏移量
    # consumer.commit()
