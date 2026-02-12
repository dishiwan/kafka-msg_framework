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
