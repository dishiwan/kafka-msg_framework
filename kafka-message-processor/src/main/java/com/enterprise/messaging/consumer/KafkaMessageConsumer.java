package com.enterprise.messaging.consumer;

import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.service.DuplicateDetectionService;
import com.enterprise.messaging.service.MessagePersistenceService;
import com.enterprise.messaging.service.MessageProcessingOrchestrator;
import com.enterprise.messaging.util.MDCUtil;
import com.enterprise.messaging.util.PriorityCalculator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaMessageConsumer {

    private final MessagePersistenceService persistenceService;
    private final DuplicateDetectionService duplicateDetectionService;
    private final MessageProcessingOrchestrator orchestrator;
    private final PriorityCalculator priorityCalculator;
    private final ObjectMapper objectMapper;

    @KafkaListener(
        topics = "${application.kafka.topics.incoming}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeMessages(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        long startTime = System.currentTimeMillis();
        
        log.info("Received batch of {} messages from Kafka", records.size());
        
        try {
            // Process each message in the batch
            for (ConsumerRecord<String, String> record : records) {
                processMessage(record);
            }
            
            // Acknowledge the batch to Kafka
            acknowledgment.acknowledge();
            
            long processingTime = System.currentTimeMillis() - startTime;
            log.info("Batch processing completed: size={}, time={}ms", records.size(), processingTime);
            
        } catch (Exception e) {
            log.error("Error processing message batch", e);
            // Do not acknowledge - will be reprocessed
        } finally {
            MDCUtil.clear();
        }
    }

    private void processMessage(ConsumerRecord<String, String> record) {
        try {
            String message = record.value();
            String key = record.key();
            
            // Parse message to extract metadata
            JsonNode jsonNode = objectMapper.readTree(message);
            String source = jsonNode.has("source") ? jsonNode.get("source").asText() : "UNKNOWN";
            String correlationId = jsonNode.has("x-correlation-id") ? 
                    jsonNode.get("x-correlation-id").asText() : generateCorrelationId();
            String eventType = jsonNode.has("event_type") ? jsonNode.get("event_type").asText() : null;
            
            MDCUtil.putCorrelationId(correlationId);
            MDCUtil.put("source", source);
            MDCUtil.put("eventType", eventType);
            
            log.debug("Processing message: correlationId={}, source={}, eventType={}", 
                     correlationId, source, eventType);
            
            // Check for duplicates
            if (duplicateDetectionService.isIncomingMessageDuplicate(message, correlationId, source)) {
                log.warn("Duplicate message detected, skipping: correlationId={}", correlationId);
                return;
            }
            
            // Calculate priority
            Integer priority = priorityCalculator.calculatePriority(source, eventType);
            
            // Persist message and create IncomingTask
            IncomingTask task = persistenceService.persistIncomingMessage(
                    message, source, correlationId, eventType, priority);
            
            // Send ACK to Kafka immediately after persistence (already acknowledged in batch)
            log.debug("Message persisted with msgId={}, processing asynchronously", task.getMsgId());
            
            // Process asynchronously
            CompletableFuture.runAsync(() -> orchestrator.processMessage(task))
                    .exceptionally(ex -> {
                        log.error("Error in async processing for msgId={}", task.getMsgId(), ex);
                        return null;
                    });
            
        } catch (Exception e) {
            log.error("Error processing individual message", e);
        }
    }

    private String generateCorrelationId() {
        return "GEN-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId();
    }
}
