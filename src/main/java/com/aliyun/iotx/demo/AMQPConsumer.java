package com.aliyun.iotx.demo;

import com.rabbitmq.client.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * AMQPMQ消费者示例
 */
public class AMQPConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AMQPConsumer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // MQ连接信息
    private static final String HOST = "xxx.xxx.xxx.xxx";
    private static final int PORT = 5672;
    private static final String USERNAME = "ZHiGPqEVwC";
    private static final String PASSWORD = "JyAfWnXoXR";
    private static final String QUEUE_NAME = "queue_NUWiXpCwlE";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(PORT);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            logger.info("等待接收消息...");

            // 设置消费者
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String originalMessage = new String(delivery.getBody(), StandardCharsets.UTF_8);
                
                
                logger.info("------ 消息内容 ------");
                logger.info("接收到原始消息: '{}'", originalMessage);
                
                // 处理消息内容
                processMessage(originalMessage);
            };

            CancelCallback cancelCallback = consumerTag -> {
                logger.warn("消费者取消: {}", consumerTag);
            };

            // 开始消费消息
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, cancelCallback);

            logger.info("按回车键退出程序...");
            System.in.read();
            
            // 关闭资源
            channel.close();
            connection.close();
            
        } catch (IOException | TimeoutException e) {
            logger.error("连接MQ失败: ", e);
        }
    }
    
    /**
     * 处理接收到的消息，尝试多种方式解析
     * @param message 原始消息
     */
    private static void processMessage(String message) {
        // 处理可能的额外引号
        String processedMessage = message;
        if (message.startsWith("\"") && message.endsWith("\"")) {
            // 去掉开头和结尾的引号，并处理转义
            try {
                processedMessage = objectMapper.readValue(message, String.class);
                logger.info("处理后的消息 (去掉额外引号): '{}'", processedMessage);
            } catch (Exception e) {
                // 如果解析失败，保留原始消息
                processedMessage = message;
            }
        }
        
        // 尝试解析JSON
        try {
            JsonNode rootNode = objectMapper.readTree(processedMessage);
            // 格式化并输出
            String prettyJson = objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(rootNode);
            logger.info("格式化的JSON:\n{}", prettyJson);
            
            // 提取并显示topic字段
            if (rootNode.has("topic")) {
                String topic = rootNode.get("topic").asText();
                logger.info("消息内容中的Topic字段: {}", topic);
            }
            
            // 检查是否有content字段，并尝试解析
            if (rootNode.has("content")) {
                try {
                    String content = rootNode.get("content").asText();
                    
                    // 尝试解析为JSON
                    JsonNode contentNode = objectMapper.readTree(content);
                    String prettyContent = objectMapper.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(contentNode);
                    logger.info("内层content字段 (格式化):\n{}", prettyContent);
                    
                    // 检查内层JSON中是否也有topic字段
                    if (contentNode.has("topic")) {
                        String innerTopic = contentNode.get("topic").asText();
                        logger.info("内层消息中的Topic字段: {}", innerTopic);
                    }
                } catch (Exception e) {
                    // content字段不是有效的JSON，显示原始内容
                    logger.info("content字段 (非JSON): {}", rootNode.get("content").asText());
                }
            }
        } catch (Exception e) {
            // 不是有效的JSON，显示为普通文本
            logger.info("消息不是有效的JSON格式，以纯文本显示:\n{}", processedMessage);
        }
    }
} 