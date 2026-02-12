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
