package main

import (
    "context"
    "log"
    "github.com/segmentio/kafka-go"
)

func main() {
    // 创建一个消费者组模式的 Reader
    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers:  []string{"localhost:9092"},
        GroupID:  "my-group",      // 消费者组ID，用于协同消费
        Topic:    "my-topic",
        MinBytes: 10e3,            // 10KB
        MaxBytes: 10e6,            // 10MB
    })
    defer r.Close()

    for {
        // 从 Kafka 拉取消息，阻塞直到有消息或context取消
        msg, err := r.ReadMessage(context.Background())
        if err != nil {
            log.Println("read error:", err)
            break
        }
        log.Printf("收到消息: key=%s, value=%s\n", string(msg.Key), string(msg.Value))
        // 消息提交由 Reader 内部自动管理
    }
}
