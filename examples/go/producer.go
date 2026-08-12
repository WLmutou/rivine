package main

import (
    "context"
    "log"
    "github.com/segmentio/kafka-go"
)

func main() {
    // 创建一个写入指定Topic的生产者
    w := &kafka.Writer{
        Addr:                  kafka.TCP("localhost:9092"), // Kafka 集群地址
        Topic:                 "my-topic-go",                  // 主题名称
        Balancer:              &kafka.LeastBytes{},         // 分区均衡策略
        AllowAutoTopicCreation: true,                       // 允许自动创建不存在的主题
    }
    defer w.Close()

    // 发送一条消息
    err := w.WriteMessages(context.Background(),
        kafka.Message{
            Key:   []byte("key-1"),
            Value: []byte("Hello, Kafka from Go!"),
        },
    )
    if err != nil {
        log.Fatal("failed to write messages:", err)
    }
}
