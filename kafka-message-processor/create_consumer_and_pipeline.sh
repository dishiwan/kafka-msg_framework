#!/bin/bash

echo "Creating Kafka Consumer..."

cat > src/main/java/com/enterprise/messaging/consumer/KafkaMessageConsumer.java << 'EOF'
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
EOF

cat > src/main/java/com/enterprise/messaging/service/MessageProcessingOrchestrator.java << 'EOF'
package com.enterprise.messaging.service;

import com.enterprise.messaging.exception.MessageProcessingException;
import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.performer.MessagePerformer;
import com.enterprise.messaging.processor.MessageProcessor;
import com.enterprise.messaging.util.MDCUtil;
import com.enterprise.messaging.validator.MessageValidator;
import com.enterprise.messaging.worker.MessageWorker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProcessingOrchestrator {

    private final MessageValidator messageValidator;
    private final MessagePerformer messagePerformer;
    private final MessageProcessor messageProcessor;
    private final MessageWorker messageWorker;
    private final MessagePersistenceService persistenceService;
    private final RetryService retryService;

    @Async("messageProcessorExecutor")
    @Retryable(
        retryFor = {MessageProcessingException.class},
        maxAttemptsExpression = "${application.processing.retry.max-attempts:3}",
        backoff = @Backoff(
            delayExpression = "${application.processing.retry.initial-delay-ms:1000}",
            multiplierExpression = "${application.processing.retry.multiplier:2.0}",
            maxDelayExpression = "${application.processing.retry.max-delay-ms:10000}"
        )
    )
    public void processMessage(IncomingTask task) {
        MDCUtil.putCorrelationId(task.getXCorrelationId());
        MDCUtil.put("msgId", String.valueOf(task.getMsgId()));
        
        try {
            log.info("Starting message processing: msgId={}, source={}, correlationId={}", 
                     task.getMsgId(), task.getSource(), task.getXCorrelationId());
            
            // Validation Phase
            messageValidator.validate(task);
            persistenceService.updateInternalStatus(task.getMsgId(), "MessageValidator", "validate");
            
            // Performer Phase
            messagePerformer.perform(task);
            persistenceService.updateInternalStatus(task.getMsgId(), "MessagePerformer", "perform");
            
            // Processor Phase
            messageProcessor.process(task);
            persistenceService.updateInternalStatus(task.getMsgId(), "MessageProcessor", "process");
            
            // Worker Phase (Final Publishing)
            messageWorker.publish(task);
            persistenceService.updateInternalStatus(task.getMsgId(), "MessageWorker", "publish");
            
            // Mark as complete
            persistenceService.updateInternalStatus(task.getMsgId(), "ProcessComplete", "success");
            persistenceService.updateFinalStatus(task.getMsgId(), "PUBLISHED");
            
            log.info("Message processing completed successfully: msgId={}", task.getMsgId());
            
        } catch (Exception e) {
            log.error("Error processing message: msgId={}", task.getMsgId(), e);
            handleProcessingError(task, e);
        } finally {
            MDCUtil.clear();
        }
    }

    private void handleProcessingError(IncomingTask task, Exception e) {
        try {
            String errorCode = determineErrorCode(e);
            String errorMessage = e.getMessage();
            
            persistenceService.updateInternalStatus(task.getMsgId(), errorCode, "failed");
            
            // Check if error is retriable
            if (retryService.isRetriable(errorCode)) {
                log.warn("Retriable error for msgId={}, will retry later", task.getMsgId());
                persistenceService.updateFinalStatus(task.getMsgId(), "RETRY_PENDING");
            } else {
                log.error("Non-retriable error for msgId={}, marking as failed", task.getMsgId());
                persistenceService.updateFinalStatus(task.getMsgId(), "FAILED");
            }
            
        } catch (Exception ex) {
            log.error("Error handling processing error", ex);
        }
    }

    private String determineErrorCode(Exception e) {
        if (e instanceof MessageProcessingException) {
            return ((MessageProcessingException) e).getErrorCode();
        }
        return "GENERAL_ERROR";
    }
}
EOF

cat > src/main/java/com/enterprise/messaging/validator/MessageValidator.java << 'EOF'
package com.enterprise.messaging.validator;

import com.enterprise.messaging.exception.ValidationException;
import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.service.MessagePersistenceService;
import com.enterprise.messaging.util.MDCUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageValidator {

    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;
    private final XmlMapper xmlMapper;
    private final MessagePersistenceService persistenceService;

    /**
     * Validates incoming message for:
     * 1. Duplicate check using Redis
     * 2. Schema validation (JSON or XML)
     * 3. Business rules validation
     */
    public void validate(IncomingTask task) throws ValidationException {
        MDCUtil.putCorrelationId(task.getXCorrelationId());
        
        try {
            log.debug("Validating message: msgId={}, correlationId={}", 
                     task.getMsgId(), task.getXCorrelationId());
            
            // Check Redis for duplicate based on composite key
            if (isDuplicateInRedis(task)) {
                throw new ValidationException("DUPLICATE_MESSAGE", 
                        "Duplicate message found in Redis cache");
            }
            
            // Validate message format and schema
            validateSchema(task);
            
            // Store in Redis for future duplicate checks (24 hour TTL)
            storeInRedis(task);
            
            task.addValidationResult("validated", true);
            task.addValidationResult("validatedAt", System.currentTimeMillis());
            
            log.info("Message validation successful: msgId={}", task.getMsgId());
            
        } catch (ValidationException e) {
            log.error("Validation failed for msgId={}: {}", task.getMsgId(), e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error during validation for msgId={}", task.getMsgId(), e);
            throw new ValidationException("VALIDATION_ERROR", "Validation failed: " + e.getMessage());
        } finally {
            MDCUtil.clear();
        }
    }

    private boolean isDuplicateInRedis(IncomingTask task) {
        String redisKey = buildRedisKey(task.getXCorrelationId(), task.getSource());
        Boolean exists = redisTemplate.hasKey(redisKey);
        
        if (Boolean.TRUE.equals(exists)) {
            log.warn("Duplicate found in Redis: correlationId={}, source={}", 
                    task.getXCorrelationId(), task.getSource());
            return true;
        }
        
        return false;
    }

    private void storeInRedis(IncomingTask task) {
        String redisKey = buildRedisKey(task.getXCorrelationId(), task.getSource());
        redisTemplate.opsForValue().set(redisKey, task.getMsgId(), 24, TimeUnit.HOURS);
        log.debug("Stored message in Redis: key={}, msgId={}", redisKey, task.getMsgId());
    }

    private String buildRedisKey(String correlationId, String source) {
        return "msg:duplicate:" + correlationId + ":" + source;
    }

    private void validateSchema(IncomingTask task) throws ValidationException {
        String message = task.getOriginalMessage();
        
        try {
            // Determine if message is JSON or XML
            if (message.trim().startsWith("{") || message.trim().startsWith("[")) {
                validateJsonSchema(message);
            } else if (message.trim().startsWith("<")) {
                validateXmlSchema(message);
            } else {
                throw new ValidationException("INVALID_FORMAT", "Message format not recognized");
            }
            
        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new ValidationException("SCHEMA_INVALID", "Schema validation failed: " + e.getMessage());
        }
    }

    private void validateJsonSchema(String jsonMessage) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(jsonMessage);
        
        // Basic JSON validation - in production, load schema from configuration
        if (!jsonNode.has("source")) {
            throw new ValidationException("SCHEMA_INVALID", "Missing required field: source");
        }
        
        log.debug("JSON schema validation passed");
    }

    private void validateXmlSchema(String xmlMessage) throws Exception {
        // Basic XML validation - in production, use XSD schema
        xmlMapper.readTree(xmlMessage);
        log.debug("XML schema validation passed");
    }
}
EOF

echo "Kafka Consumer and Pipeline created!"

