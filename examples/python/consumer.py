from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092'],
    group_id='python-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

print("开始消费消息...")
for msg in consumer:
    print(f"收到消息: topic={msg.topic}, partition={msg.partition}, offset={msg.offset}")
    print(f"  key={msg.key}, value={msg.value}")
    # 处理消息逻辑...
    # 如果处理失败，可以捕获异常，避免程序退出



