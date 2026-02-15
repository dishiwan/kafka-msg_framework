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
