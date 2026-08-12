from kafka import KafkaProducer
import json

# 创建生产者
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Kafka服务器地址
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # 序列化
    key_serializer=lambda k: k.encode('utf-8') if k else None
)
print("start producer...")
# 发送消息（同步）
future = producer.send(
    topic='test-topic',
    key='user-001',
    value={'name': '张三', 'age': 25}
)
result = future.get(timeout=10)  # 等待发送结果
print(f"消息发送成功: {result.topic}, partition={result.partition}, offset={result.offset}")

# 发送消息（异步回调）
def on_send_success(record_metadata):
    print(f"成功: {record_metadata.topic} [{record_metadata.partition}] offset={record_metadata.offset}")

def on_send_error(excp):
    print(f"失败: {excp}")

producer.send('test-topic', value={'msg': '异步发送'}).add_callback(on_send_success).add_errback(on_send_error)

# 批量发送
messages = [{'id': i, 'data': f'message_{i}'} for i in range(10)]
for msg in messages:
    producer.send('test-topic', value=msg)

# 关闭连接
producer.close()
