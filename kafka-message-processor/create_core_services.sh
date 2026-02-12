#!/bin/bash

echo "Creating Core Service classes..."

cat > src/main/java/com/enterprise/messaging/service/MessagePersistenceService.java << 'EOF'
package com.enterprise.messaging.service;

import com.enterprise.messaging.cache.SequenceCacheService;
import com.enterprise.messaging.model.IncomingMessage;
import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.model.OutgoingMessage;
import com.enterprise.messaging.repository.IncomingMessageRepository;
import com.enterprise.messaging.repository.OutgoingMessageRepository;
import com.enterprise.messaging.util.HashCodeGenerator;
import com.enterprise.messaging.util.MDCUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessagePersistenceService {

    private final IncomingMessageRepository incomingMessageRepository;
    private final OutgoingMessageRepository outgoingMessageRepository;
    private final SequenceCacheService sequenceCacheService;
    private final HashCodeGenerator hashCodeGenerator;

    @Value("${application.name:messaging_app}")
    private String applicationName;

    // Static HashMap to store correlation mapping
    private static final Map<String, BigDecimal> CORRELATION_MSG_ID_MAP = new ConcurrentHashMap<>();

    @Transactional
    public IncomingTask persistIncomingMessage(String message, String source, String correlationId, 
                                               String eventType, Integer priority) {
        MDCUtil.putCorrelationId(correlationId);
        
        try {
            // Get sequence for Internal Source ID
            BigDecimal internalSourceId = sequenceCacheService.getNextSequence("INTERNAL_SOURCE_SEQUENCE");
            
            // Generate hashcode for duplicate detection
            String hashCode = hashCodeGenerator.generateHashCode(message);
            
            // Build incoming message entity
            IncomingMessage incomingMessage = IncomingMessage.builder()
                    .source(source)
                    .xCorrelationId(correlationId)
                    .internalSourceId(internalSourceId)
                    .eventType(eventType)
                    .priority(priority != null ? priority : 5)
                    .internalStatus("IN_PROGRESS")
                    .phase("RECEIVED")
                    .cycleDate(new Date())
                    .attributeKey4(hashCode)  // Store hashcode
                    .originalMsg(message)
                    .insertedBy("KAFKA_CONSUMER")
                    .build();
            
            // Persist to database
            IncomingMessage savedMessage = incomingMessageRepository.save(incomingMessage);
            
            log.info("Persisted incoming message: msgId={}, internalSourceId={}, correlationId={}", 
                     savedMessage.getMsgId(), internalSourceId, correlationId);
            
            // Store in correlation map
            String mapKey = correlationId + "_" + source;
            CORRELATION_MSG_ID_MAP.put(mapKey, savedMessage.getMsgId());
            
            // Create IncomingTask for processing pipeline
            IncomingTask task = IncomingTask.builder()
                    .msgId(savedMessage.getMsgId())
                    .source(source)
                    .xCorrelationId(correlationId)
                    .internalSourceId(internalSourceId)
                    .eventType(eventType)
                    .priority(priority)
                    .originalMessage(message)
                    .build();
            
            task.addProcessingMetadata("persistedAt", LocalDateTime.now());
            task.addProcessingMetadata("hashCode", hashCode);
            
            return task;
            
        } catch (Exception e) {
            log.error("Error persisting incoming message: correlationId={}", correlationId, e);
            throw new RuntimeException("Failed to persist incoming message", e);
        } finally {
            MDCUtil.clear();
        }
    }

    @Transactional
    public void updateInternalStatus(BigDecimal msgId, String className, String methodName) {
        String status = className + "." + methodName;
        incomingMessageRepository.updateInternalStatus(msgId, status, LocalDateTime.now(), "SYSTEM");
        log.debug("Updated internal status for msgId={}: {}", msgId, status);
    }

    @Transactional
    public void updateFinalStatus(BigDecimal msgId, String status) {
        incomingMessageRepository.updateFinalStatus(msgId, status, LocalDateTime.now());
        log.info("Updated final status for msgId={}: {}", msgId, status);
    }

    @Transactional
    public OutgoingMessage persistOutgoingMessage(IncomingTask task, String outgoingMessage, 
                                                  int sequenceNumber) {
        try {
            // Generate outgoing message ID
            String outgoingMsgId = task.getInternalSourceId() + "-" + sequenceNumber;
            
            // Generate hashcode for duplicate detection
            String hashCode = hashCodeGenerator.generateHashCode(outgoingMessage);
            
            OutgoingMessage outgoing = OutgoingMessage.builder()
                    .source(task.getSource())
                    .xCorrelationId(task.getXCorrelationId())
                    .internalSourceId(task.getInternalSourceId())
                    .outgoingMsgId(outgoingMsgId)
                    .eventType(task.getEventType())
                    .finalStatus("PENDING")
                    .phase("CREATED")
                    .cycleDate(new Date())
                    .attributeKey4(hashCode)  // Store hashcode
                    .originalMsg(outgoingMessage)
                    .insertedBy("MESSAGE_PROCESSOR")
                    .build();
            
            OutgoingMessage saved = outgoingMessageRepository.save(outgoing);
            
            log.info("Persisted outgoing message: msgId={}, outgoingMsgId={}, internalSourceId={}", 
                     saved.getMsgId(), outgoingMsgId, task.getInternalSourceId());
            
            return saved;
            
        } catch (Exception e) {
            log.error("Error persisting outgoing message for internalSourceId={}", 
                     task.getInternalSourceId(), e);
            throw new RuntimeException("Failed to persist outgoing message", e);
        }
    }

    public static BigDecimal getMsgIdFromCorrelation(String correlationId, String source) {
        String mapKey = correlationId + "_" + source;
        return CORRELATION_MSG_ID_MAP.get(mapKey);
    }

    public static void clearCorrelationMap(String correlationId, String source) {
        String mapKey = correlationId + "_" + source;
        CORRELATION_MSG_ID_MAP.remove(mapKey);
    }
}
EOF

cat > src/main/java/com/enterprise/messaging/service/DuplicateDetectionService.java << 'EOF'
package com.enterprise.messaging.service;

import com.enterprise.messaging.model.DuplicateMessage;
import com.enterprise.messaging.repository.DuplicateMessageRepository;
import com.enterprise.messaging.repository.IncomingMessageRepository;
import com.enterprise.messaging.repository.OutgoingMessageRepository;
import com.enterprise.messaging.util.HashCodeGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class DuplicateDetectionService {

    private final IncomingMessageRepository incomingMessageRepository;
    private final OutgoingMessageRepository outgoingMessageRepository;
    private final DuplicateMessageRepository duplicateMessageRepository;
    private final HashCodeGenerator hashCodeGenerator;

    @Value("${application.duplicate.detection.check-window-hours:24}")
    private int checkWindowHours;

    @Value("${application.duplicate.detection.enabled:true}")
    private boolean duplicateDetectionEnabled;

    /**
     * Check if incoming message is duplicate
     */
    public boolean isIncomingMessageDuplicate(String message, String correlationId, String source) {
        if (!duplicateDetectionEnabled) {
            return false;
        }

        try {
            String hashCode = hashCodeGenerator.generateHashCode(message);
            LocalDateTime threshold = LocalDateTime.now().minusHours(checkWindowHours);
            
            boolean isDuplicate = incomingMessageRepository.existsByAttributeKey4AndInsertTimestampAfter(
                    hashCode, threshold);
            
            if (isDuplicate) {
                log.warn("Duplicate incoming message detected: correlationId={}, source={}, hashCode={}", 
                         correlationId, source, hashCode);
                
                recordDuplicate(source, correlationId, hashCode, "INCOMING", null, null);
            }
            
            return isDuplicate;
            
        } catch (Exception e) {
            log.error("Error checking for duplicate incoming message", e);
            return false;
        }
    }

    /**
     * Check if outgoing message is duplicate
     */
    public boolean isOutgoingMessageDuplicate(String message, BigDecimal internalSourceId) {
        if (!duplicateDetectionEnabled) {
            return false;
        }

        try {
            String hashCode = hashCodeGenerator.generateHashCode(message);
            LocalDateTime threshold = LocalDateTime.now().minusHours(checkWindowHours);
            
            boolean isDuplicate = outgoingMessageRepository.existsByAttributeKey4AndInsertTimestampAfter(
                    hashCode, threshold);
            
            if (isDuplicate) {
                log.warn("Duplicate outgoing message detected: internalSourceId={}, hashCode={}", 
                         internalSourceId, hashCode);
                
                recordDuplicate(null, null, hashCode, "OUTGOING", null, null);
            }
            
            return isDuplicate;
            
        } catch (Exception e) {
            log.error("Error checking for duplicate outgoing message", e);
            return false;
        }
    }

    @Transactional
    private void recordDuplicate(String source, String correlationId, String hashCode, 
                                 String type, BigDecimal originalMsgId, BigDecimal duplicateMsgId) {
        try {
            DuplicateMessage duplicate = DuplicateMessage.builder()
                    .source(source)
                    .xCorrelationId(correlationId)
                    .messageHashcode(hashCode)
                    .duplicateType(type)
                    .originalMsgId(originalMsgId)
                    .duplicateMsgId(duplicateMsgId)
                    .details("Duplicate detected within " + checkWindowHours + " hours window")
                    .detectedBy("DUPLICATE_DETECTION_SERVICE")
                    .build();
            
            duplicateMessageRepository.save(duplicate);
            
        } catch (Exception e) {
            log.error("Error recording duplicate message", e);
        }
    }
}
EOF

echo "Core Service classes created!"

