## KClient

### KClient提供了如下功能

1. 简单易用：进一步简化了可Kafka客户端API的使用方法, 使用者并不需要了解Kafka客户端API的细节, 只需实现MessageHandler接口并处理消息即可
2. 集成：与Spring无缝集成，使用Spring环境的项目可以直接从环境中导入并启动Kafka客户端
3. 效率：提供两种线程模型，消息消费线程中处理业务和异步线程池处理业务
4. 稳定性：优雅关机和优雅重启

简单易用：
高性能：两种线程模型，轻量级服务和IO型服务
稳定性：异常处理，错误数据恢复，优雅关机
集成：



### Kafka基础知识

生产者，消费者

## 分区和流

Partition, Stream, Group等

## 分组

4. Support group, without group, it is a queue, with group, it is publish/subscribe model

### 线程模型

消息消费线程中处理业务和异步线程池处理业务

### 异常处理

1. 如果某个线程中处理某条消息失败怎么办？如果大量失败怎么办？
2. 并发的处理器中，如果断电了，怎么回复

### 优雅关机

3. 研究关机的时候哪些信号会杀死线程，deamon和非deamon线程的区别

### 性能压测

### TODO

Add a backend startup app, like servlet, listner etc.

### 使用文档