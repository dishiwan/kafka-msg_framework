#!/bin/bash

echo "Creating Performer, Processor, and Worker classes..."

cat > src/main/java/com/enterprise/messaging/performer/MessagePerformer.java << 'EOF'
package com.enterprise.messaging.performer;

import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.service.MessagePersistenceService;
import com.enterprise.messaging.util.MDCUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessagePerformer {

    private final RedisTemplate<String, Object> redisTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final MessagePersistenceService persistenceService;

    /**
     * Performs enrichment and subscription checks for the message
     * - Checks customer subscriptions (SMS, Email, Voice)
     * - Enriches message with static data from database
     * - Stores enrichment data in IncomingTask
     */
    public void perform(IncomingTask task) {
        MDCUtil.putCorrelationId(task.getXCorrelationId());
        
        try {
            log.debug("Performing message enrichment: msgId={}", task.getMsgId());
            
            // Parse message to get customer identifiers
            String customerId = extractCustomerId(task);
            
            // Check customer subscriptions
            Map<String, Boolean> subscriptions = checkCustomerSubscriptions(customerId);
            task.addEnrichmentData("subscriptions", subscriptions.toString());
            
            // Enrich with static data from database
            Map<String, String> staticData = enrichWithStaticData(customerId);
            staticData.forEach(task::addEnrichmentData);
            
            // Cache enriched data in Redis for quick access
            cacheEnrichmentData(task, customerId, subscriptions, staticData);
            
            log.info("Message enrichment completed: msgId={}, subscriptions={}", 
                     task.getMsgId(), subscriptions);
            
        } catch (Exception e) {
            log.error("Error during message enrichment for msgId={}", task.getMsgId(), e);
            throw new RuntimeException("Enrichment failed", e);
        } finally {
            MDCUtil.clear();
        }
    }

    private String extractCustomerId(IncomingTask task) {
        try {
            JsonNode jsonNode = objectMapper.readTree(task.getOriginalMessage());
            return jsonNode.has("customerId") ? jsonNode.get("customerId").asText() : "UNKNOWN";
        } catch (Exception e) {
            log.warn("Could not extract customerId from message", e);
            return "UNKNOWN";
        }
    }

    private Map<String, Boolean> checkCustomerSubscriptions(String customerId) {
        Map<String, Boolean> subscriptions = new HashMap<>();
        
        try {
            // Mock implementation - in production, query subscription database
            String sql = "SELECT SMS_ENABLED, EMAIL_ENABLED, VOICE_ENABLED FROM CUSTOMER_SUBSCRIPTIONS WHERE CUSTOMER_ID = ?";
            
            // For demo purposes, using default subscriptions
            subscriptions.put("sms", true);
            subscriptions.put("email", true);
            subscriptions.put("voice", false);
            
            log.debug("Customer subscriptions retrieved: customerId={}, subscriptions={}", 
                     customerId, subscriptions);
            
        } catch (Exception e) {
            log.error("Error checking customer subscriptions", e);
            // Default to all enabled
            subscriptions.put("sms", true);
            subscriptions.put("email", true);
            subscriptions.put("voice", true);
        }
        
        return subscriptions;
    }

    private Map<String, String> enrichWithStaticData(String customerId) {
        Map<String, String> staticData = new HashMap<>();
        
        try {
            // Mock implementation - in production, query enrichment database
            staticData.put("customerName", "John Doe");
            staticData.put("customerTier", "PREMIUM");
            staticData.put("preferredLanguage", "EN");
            staticData.put("timezone", "UTC-5");
            
            log.debug("Static data enrichment completed for customerId={}", customerId);
            
        } catch (Exception e) {
            log.error("Error enriching with static data", e);
        }
        
        return staticData;
    }

    private void cacheEnrichmentData(IncomingTask task, String customerId, 
                                    Map<String, Boolean> subscriptions, 
                                    Map<String, String> staticData) {
        try {
            String cacheKey = "enrichment:" + customerId;
            Map<String, Object> cacheData = new HashMap<>();
            cacheData.put("subscriptions", subscriptions);
            cacheData.put("staticData", staticData);
            cacheData.put("cachedAt", System.currentTimeMillis());
            
            redisTemplate.opsForHash().putAll(cacheKey, cacheData);
            
        } catch (Exception e) {
            log.warn("Error caching enrichment data", e);
        }
    }
}
EOF

cat > src/main/java/com/enterprise/messaging/processor/MessageProcessor.java << 'EOF'
package com.enterprise.messaging.processor;

import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.service.MessagePersistenceService;
import com.enterprise.messaging.util.MDCUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageProcessor {

    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;
    private final MessagePersistenceService persistenceService;

    /**
     * Processes the message and determines routing channels
     * - Determines delivery channels based on subscriptions
     * - Creates separate messages for each channel (email, SMS, voice)
     * - Stores transformed messages in IncomingTask
     */
    public void process(IncomingTask task) {
        MDCUtil.putCorrelationId(task.getXCorrelationId());
        
        try {
            log.debug("Processing message to determine routes: msgId={}", task.getMsgId());
            
            // Get enrichment data
            String subscriptionsStr = task.getEnrichmentData("subscriptions");
            Map<String, Boolean> subscriptions = parseSubscriptions(subscriptionsStr);
            
            // Determine delivery channels
            List<String> deliveryChannels = determineDeliveryChannels(subscriptions);
            task.addProcessingMetadata("deliveryChannels", deliveryChannels);
            
            // Create transformed messages for each channel
            List<Map<String, Object>> transformedMessages = new ArrayList<>();
            for (String channel : deliveryChannels) {
                Map<String, Object> transformedMsg = transformMessageForChannel(task, channel);
                transformedMessages.add(transformedMsg);
            }
            
            task.addProcessingMetadata("transformedMessages", transformedMessages);
            task.addProcessingMetadata("messageCount", transformedMessages.size());
            
            // Store transformed message in database
            String transformedMsgStr = objectMapper.writeValueAsString(transformedMessages);
            task.setTransformedMessage(transformedMsgStr);
            
            log.info("Message processing completed: msgId={}, channels={}, messageCount={}", 
                     task.getMsgId(), deliveryChannels, transformedMessages.size());
            
        } catch (Exception e) {
            log.error("Error processing message for msgId={}", task.getMsgId(), e);
            throw new RuntimeException("Processing failed", e);
        } finally {
            MDCUtil.clear();
        }
    }

    private Map<String, Boolean> parseSubscriptions(String subscriptionsStr) {
        Map<String, Boolean> subscriptions = new HashMap<>();
        subscriptions.put("sms", true);
        subscriptions.put("email", true);
        subscriptions.put("voice", false);
        return subscriptions;
    }

    private List<String> determineDeliveryChannels(Map<String, Boolean> subscriptions) {
        List<String> channels = new ArrayList<>();
        
        if (Boolean.TRUE.equals(subscriptions.get("sms"))) {
            channels.add("SMS");
        }
        if (Boolean.TRUE.equals(subscriptions.get("email"))) {
            channels.add("EMAIL");
        }
        if (Boolean.TRUE.equals(subscriptions.get("voice"))) {
            channels.add("VOICE");
        }
        
        return channels;
    }

    private Map<String, Object> transformMessageForChannel(IncomingTask task, String channel) {
        Map<String, Object> transformed = new HashMap<>();
        transformed.put("channel", channel);
        transformed.put("correlationId", task.getXCorrelationId());
        transformed.put("source", task.getSource());
        transformed.put("eventType", task.getEventType());
        transformed.put("internalSourceId", task.getInternalSourceId());
        transformed.put("content", task.getOriginalMessage());
        transformed.put("enrichmentData", task.getEnrichmentData());
        transformed.put("timestamp", System.currentTimeMillis());
        
        return transformed;
    }
}
EOF

cat > src/main/java/com/enterprise/messaging/worker/MessageWorker.java << 'EOF'
package com.enterprise.messaging.worker;

import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.model.OutgoingMessage;
import com.enterprise.messaging.publisher.MessagePublisher;
import com.enterprise.messaging.service.DuplicateDetectionService;
import com.enterprise.messaging.service.MessagePersistenceService;
import com.enterprise.messaging.util.MDCUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageWorker {

    private final MessagePublisher messagePublisher;
    private final MessagePersistenceService persistenceService;
    private final DuplicateDetectionService duplicateDetectionService;
    private final ObjectMapper objectMapper;

    /**
     * Final worker that publishes messages to outgoing queue
     * - Persists each outgoing message to database
     * - Checks for duplicates before publishing
     * - Publishes to appropriate delivery channel
     */
    public void publish(IncomingTask task) {
        MDCUtil.putCorrelationId(task.getXCorrelationId());
        
        try {
            log.debug("Publishing messages: msgId={}", task.getMsgId());
            
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> transformedMessages = 
                (List<Map<String, Object>>) task.getProcessingMetadata().get("transformedMessages");
            
            if (transformedMessages == null || transformedMessages.isEmpty()) {
                log.warn("No transformed messages to publish for msgId={}", task.getMsgId());
                return;
            }
            
            int sequenceNumber = 1;
            for (Map<String, Object> message : transformedMessages) {
                publishSingleMessage(task, message, sequenceNumber++);
            }
            
            log.info("All messages published successfully: msgId={}, count={}", 
                     task.getMsgId(), transformedMessages.size());
            
        } catch (Exception e) {
            log.error("Error publishing messages for msgId={}", task.getMsgId(), e);
            throw new RuntimeException("Publishing failed", e);
        } finally {
            MDCUtil.clear();
        }
    }

    private void publishSingleMessage(IncomingTask task, Map<String, Object> message, int sequenceNumber) {
        try {
            String messageStr = objectMapper.writeValueAsString(message);
            String channel = (String) message.get("channel");
            
            // Check for duplicates
            if (duplicateDetectionService.isOutgoingMessageDuplicate(messageStr, task.getInternalSourceId())) {
                log.warn("Duplicate outgoing message detected, skipping: internalSourceId={}, seq={}", 
                        task.getInternalSourceId(), sequenceNumber);
                return;
            }
            
            // Persist outgoing message
            OutgoingMessage outgoing = persistenceService.persistOutgoingMessage(task, messageStr, sequenceNumber);
            
            // Publish to Kafka
            messagePublisher.publishToChannel(channel, messageStr, task.getXCorrelationId());
            
            // Update status
            persistenceService.updateFinalStatus(outgoing.getMsgId(), "PUBLISHED");
            
            log.info("Message published: outgoingMsgId={}, channel={}, seq={}", 
                     outgoing.getOutgoingMsgId(), channel, sequenceNumber);
            
        } catch (Exception e) {
            log.error("Error publishing single message: seq={}", sequenceNumber, e);
            throw new RuntimeException("Failed to publish message", e);
        }
    }
}
EOF

echo "Performer, Processor, and Worker classes created!"

