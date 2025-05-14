package com.aliyun.iotx.demo;

import com.rabbitmq.client.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
    private static final String USERNAME = "OEADFoxsxT";
    private static final String PASSWORD = "qVAmrV***";
    private static final String QUEUE_NAME = "queue_tHJFOvFkhR";

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
                byte[] messageBytes = delivery.getBody();
                String contentType = null;
                
                // 提取header信息
                AMQP.BasicProperties props = delivery.getProperties();
                
                // 安全地提取header值
                String messageId = null;
                String topic = null;
                String generateTime = null;
                if (props.getHeaders() != null) {
                    Object messageIdObj = props.getHeaders().get("messageId");
                    Object topicObj = props.getHeaders().get("topic");
                    Object generateTimeObj = props.getHeaders().get("generateTime");
                    Object contentTypeObj = props.getHeaders().get("contentType");
                    
                    // 处理不同类型的header值
                    messageId = messageIdObj != null ? messageIdObj.toString() : null;
                    topic = topicObj != null ? topicObj.toString() : null;
                    generateTime = generateTimeObj != null ? generateTimeObj.toString() : null;
                    contentType = contentTypeObj != null ? contentTypeObj.toString() : null;
                }
                
                // 如果消息属性中有Content-Type，优先使用它
                if (props.getContentType() != null) {
                    contentType = props.getContentType();
                }
                
                logger.info("------ 消息元数据 ------");
                logger.info("MessageId: {}", messageId);
                logger.info("Topic: {}", topic);
                logger.info("GenerateTime: {}", generateTime);
                logger.info("ContentType: {}", contentType);
                
                // 判断是否为二进制数据
                boolean isBinary;
                
                if (contentType != null) {
                    // 根据ContentType判断
                    if (contentType.startsWith("text/")) {
                        // 文本类型
                        logger.info("ContentType: {} (文本类型)", contentType);
                        isBinary = false;
                    } else if (contentType.startsWith("application/octet-stream")) {
                        // application/octet-stream需要特别处理，可能是文本也可能是二进制
                        logger.info("ContentType: {} (需要进一步分析内容)", contentType);
                        // 使用内容分析
                        isBinary = !isTextContent(messageBytes);
                    } else if (contentType.startsWith("application/binary") ||
                              contentType.startsWith("image/") || 
                              contentType.startsWith("audio/") || 
                              contentType.startsWith("video/")) {
                        // 明确的二进制类型
                        logger.info("ContentType: {} (明确的二进制类型)", contentType);
                        isBinary = true;
                    } else {
                        // 其他类型，使用内容检测
                        logger.info("ContentType: {} (未知类型，将分析内容)", contentType);
                        isBinary = !isTextContent(messageBytes);
                    }
                } else {
                    // ContentType为空，使用内容检测
                    logger.info("ContentType为空，将分析内容");
                    isBinary = !isTextContent(messageBytes);
                }
                
                // 根据内容类型处理消息
                if (isBinary) {
                    // 二进制内容处理
                    processBinaryMessage(messageBytes, messageId, topic, generateTime);
                } else {
                    // 文本内容处理
                    String originalMessage = new String(messageBytes, StandardCharsets.UTF_8);
                    logger.info("接收到文本消息: '{}'", originalMessage);
                    processTextMessage(originalMessage, messageId, topic, generateTime);
                }
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
     * 判断内容是否为二进制数据（基于contentType）
     */
    private static boolean isBinaryContent(String contentType) {
        if (contentType == null) {
            logger.info("contentType为空");
            return false;
        }
        
        // 常见二进制内容类型
        return contentType.startsWith("application/octet-stream") || 
               contentType.startsWith("application/binary") ||
               contentType.startsWith("image/") || 
               contentType.startsWith("audio/") || 
               contentType.startsWith("video/") ||
               contentType.contains("binary");
    }
    
    /**
     * 自动检测数据是否为二进制内容（当contentType不可用时）
     * 
     * @param data 待检测的字节数组
     * @return 是否可能是二进制内容
     */
    private static boolean detectBinaryContent(byte[] data) {
        // 如果数据为空，当作文本处理
        if (data == null || data.length == 0) {
            return false;
        }
        
        // 检查是否有常见二进制文件的特征头部
        if (data.length >= 4) {
            // 检查常见二进制文件格式的魔数
            if ((data[0] == (byte)0xFF && data[1] == (byte)0xD8) || // JPEG
                (data[0] == (byte)0x89 && data[1] == 'P' && data[2] == 'N' && data[3] == 'G') || // PNG
                (data[0] == 'G' && data[1] == 'I' && data[2] == 'F') || // GIF
                (data[0] == 'P' && data[1] == 'K') || // ZIP/JAR
                (data[0] == (byte)0x25 && data[1] == (byte)0x50 && data[2] == (byte)0x44 && data[3] == (byte)0x46)) { // PDF
                return true;
            }
        }
        
        // 检查数据中是否包含大量非ASCII字符或控制字符
        int binaryCount = 0;
        int sampleSize = Math.min(data.length, 100); // 抽样检查前100字节
        
        for (int i = 0; i < sampleSize; i++) {
            byte b = data[i];
            // 非ASCII可打印字符和非常见控制字符（如 \n \r \t）
            if ((b < 32 && b != 9 && b != 10 && b != 13) || b > 126) {
                binaryCount++;
            }
        }
        
        // 如果有超过15%的字节是非ASCII字符，可能是二进制数据
        double binaryRatio = (double) binaryCount / sampleSize;
        return binaryRatio > 0.15;
    }
    
    /**
     * 尝试将数据作为JSON解析
     * 如果能成功解析，则说明是文本数据
     */
    private static boolean isValidJson(byte[] data) {
        try {
            String jsonStr = new String(data, StandardCharsets.UTF_8);
            objectMapper.readTree(jsonStr);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * 处理二进制消息
     */
    private static void processBinaryMessage(byte[] messageBytes, String messageId, String topic, String generateTime) {
        // 记录二进制数据大小
        logger.info("接收到二进制数据, 大小: {} 字节", messageBytes.length);
        
        // 尝试检测常见的二进制格式
        detectBinaryFormat(messageBytes);
        
        // 可以直接处理原始的二进制数据，不需要Base64编码
        // 例如：
        // 1. 保存为文件
        // 2. 解析特定的二进制协议
        // 3. 转发到其他处理单元
        
        // 如需调试预览二进制内容，可以显示前几个字节的十六进制值
        StringBuilder hexPreview = new StringBuilder();
        int previewSize = Math.min(messageBytes.length, 20); // 只显示前20个字节
        for (int i = 0; i < previewSize; i++) {
            hexPreview.append(String.format("%02X ", messageBytes[i]));
        }
        if (messageBytes.length > 20) {
            hexPreview.append("...");
        }
        logger.info("二进制数据预览(十六进制): {}", hexPreview.toString());
    }
    
    /**
     * 检测二进制数据的格式
     */
    private static void detectBinaryFormat(byte[] data) {
        if (data.length < 4) {
            logger.info("二进制数据太短，无法判断格式");
            return;
        }
        
        // 检查文件头部特征来判断格式
        if (data[0] == (byte)0xFF && data[1] == (byte)0xD8) {
            logger.info("检测到可能是JPEG图像数据");
        } else if (data[0] == (byte)0x89 && data[1] == 'P' && data[2] == 'N' && data[3] == 'G') {
            logger.info("检测到可能是PNG图像数据");
        } else if (data[0] == 'G' && data[1] == 'I' && data[2] == 'F') {
            logger.info("检测到可能是GIF图像数据");
        } else if (data[0] == 'P' && data[1] == 'K') {
            logger.info("检测到可能是ZIP/JAR等压缩格式");
        } else if (data[0] == (byte)0x25 && data[1] == (byte)0x50 && data[2] == (byte)0x44 && data[3] == (byte)0x46) {
            logger.info("检测到可能是PDF文档");
        } else if (data.length >= 2 && ((data[0] == (byte)0xFE && data[1] == (byte)0xFF) || (data[0] == (byte)0xFF && data[1] == (byte)0xFE))) {
            logger.info("检测到可能是带BOM的Unicode文本");
            // 尝试解码并显示文本预览
            String encoding = (data[0] == (byte)0xFE && data[1] == (byte)0xFF) ? "UTF-16BE" : "UTF-16LE";
            try {
                String text = new String(data, encoding);
                String preview = text.length() > 100 ? text.substring(0, 100) + "..." : text;
                logger.info("Unicode文本预览 ({}): {}", encoding, preview);
            } catch (Exception e) {
                logger.warn("Unicode解码失败", e);
            }
        } else {
            logger.info("未能识别的二进制格式");
        }
    }
    
    /**
     * 处理接收到的文本消息，尝试多种方式解析
     */
    private static void processTextMessage(String message, String messageId, String topic, String generateTime) {
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
                String contentTopic = rootNode.get("topic").asText();
                logger.info("消息内容中的Topic字段: {}", contentTopic);
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

    /**
     * 判断内容是否为文本数据
     * @param data 待检测的字节数组
     * @return 是否可能是文本内容
     */
    private static boolean isTextContent(byte[] data) {
        // 如果数据为空，默认当作文本处理
        if (data == null || data.length == 0) {
            return true;
        }
        
        // 尝试作为JSON解析
        if (isValidJson(data)) {
            logger.info("检测到有效的JSON内容");
            return true;
        }
        
        // 检查是否全部为可打印ASCII字符或常见控制字符
        int textCharCount = 0;
        for (byte b : data) {
            // 可打印ASCII字符(32-126)或常见控制字符(\n \r \t)
            if ((b >= 32 && b <= 126) || b == 9 || b == 10 || b == 13) {
                textCharCount++;
            }
        }
        
        // 如果85%以上的字符都是可打印字符或常见控制字符，可能是文本
        double textRatio = (double) textCharCount / data.length;
        boolean isText = textRatio > 0.85;
        
        if (isText) {
            logger.info("基于字符分析，判断可能是文本内容 (文本字符比例: {}%)", String.format("%.2f", textRatio * 100));
        } else {
            logger.info("基于字符分析，判断可能是二进制内容 (非文本字符比例: {}%)", String.format("%.2f", (1 - textRatio) * 100));
        }
        
        return isText;
    }
} 