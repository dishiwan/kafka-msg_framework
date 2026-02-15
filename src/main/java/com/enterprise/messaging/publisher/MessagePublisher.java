package com.enterprise.messaging.publisher;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessagePublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${application.kafka.topics.outgoing}")
    private String outgoingTopic;

    public void publishToChannel(String channel, String message, String correlationId) {
        String topic = determineTopicForChannel(channel);
        
        CompletableFuture.runAsync(() -> {
            try {
                kafkaTemplate.send(topic, correlationId, message)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Message published successfully: topic={}, correlationId={}", topic, correlationId);
                        } else {
                            log.error("Failed to publish message: topic={}, correlationId={}", topic, correlationId, ex);
                        }
                    });
            } catch (Exception e) {
                log.error("Error publishing message", e);
            }
        });
    }

    private String determineTopicForChannel(String channel) {
        return switch (channel.toUpperCase()) {
            case "EMAIL" -> outgoingTopic + ".email";
            case "SMS" -> outgoingTopic + ".sms";
            case "VOICE" -> outgoingTopic + ".voice";
            default -> outgoingTopic;
        };
    }
}
