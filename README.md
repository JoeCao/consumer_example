# IoT平台服务端订阅的消费者示例

这是一个简单的IoT平台服务端订阅功能的AMQP消费者示例，用于订阅指定的队列并接收消息。

## 环境要求

- JDK 1.8+
- Maven 3.0+

## 配置说明

MQ连接信息已在代码中配置：

- 服务器: xxx.xxx.xxx.xxx
- 端口: 5672
- 用户名: ZHiGPqEVwC
- 密码: JyAfWnXoXR
- 队列名: queue_NUWiXpCwlE

## 功能特点

- 自动连接RabbitMQ服务器并订阅指定队列
- 接收并显示原始消息
- 自动格式化JSON消息，提高可读性
- 支持嵌套JSON结构的格式化显示
- 详细的日志记录

## 如何构建

```bash
mvn clean package
```

此命令将创建一个包含所有依赖的JAR文件。

## 如何运行

执行以下命令：

```bash
java -jar target/demo-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```

或者在IDE中直接运行`com.aliyun.iotx.demo.RabbitMQConsumer`类。

## 日志

日志将输出到控制台和`logs/rabbitmq-consumer.log`文件中。

## 消息格式示例

原始消息格式：
```
"{\"content\":\"{\\\"clientIp\\\":\\\"172.16.116.61\\\",\\\"time\\\":\\\"2025-04-28 15:54:51\\\",\\\"productKey\\\":\\\"kdlxqvXX\\\",\\\"deviceName\\\":\\\"qLeIdlTiUI\\\",\\\"status\\\":\\\"online\\\"}\",\"generateTime\":1745826891,\"messageId\":\"1745826891727\",\"topic\":\"/as/mqtt/status/kdlxqvXX/qLeIdlTiUI\"}"
```

格式化后：
```json
{
  "content" : "{\"clientIp\":\"172.16.116.61\",\"time\":\"2025-04-28 15:54:51\",\"productKey\":\"kdlxqvXX\",\"deviceName\":\"qLeIdlTiUI\",\"status\":\"online\"}",
  "generateTime" : 1745826891,
  "messageId" : "1745826891727",
  "topic" : "/as/mqtt/status/kdlxqvXX/qLeIdlTiUI"
}

内层content内容:
{
  "clientIp" : "172.16.116.61",
  "time" : "2025-04-28 15:54:51",
  "productKey" : "kdlxqvXX",
  "deviceName" : "qLeIdlTiUI",
  "status" : "online"
}
``` 