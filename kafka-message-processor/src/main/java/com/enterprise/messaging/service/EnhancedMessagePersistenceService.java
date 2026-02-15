package com.enterprise.messaging.service;

import com.enterprise.messaging.cache.SequenceCacheService;
import com.enterprise.messaging.model.IncomingMessage;
import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.repository.IncomingMessageRepository;
import com.enterprise.messaging.util.HashCodeGenerator;
import com.enterprise.messaging.util.MDCUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class EnhancedMessagePersistenceService {

    private final IncomingMessageRepository incomingMessageRepository;
    private final SequenceCacheService sequenceCacheService;
    private final HashCodeGenerator hashCodeGenerator;
    private final LeaseRenewalService leaseRenewalService;

    private static final Map<String, BigDecimal> CORRELATION_MSG_ID_MAP = new ConcurrentHashMap<>();

    @Transactional
    public IncomingTask persistIncomingMessage(String message, String source, String correlationId, 
                                               String eventType, Integer priority) {
        MDCUtil.putCorrelationId(correlationId);
        
        try {
            BigDecimal internalSourceId = sequenceCacheService.getNextSequence("INTERNAL_SOURCE_SEQUENCE");
            String hashCode = hashCodeGenerator.generateHashCode(message);
            
            IncomingMessage incomingMessage = IncomingMessage.builder()
                    .source(source)
                    .xCorrelationId(correlationId)
                    .internalSourceId(internalSourceId)
                    .eventType(eventType)
                    .priority(priority != null ? priority : 5)
                    .internalStatus("IN_PROGRESS")
                    .phase("RECEIVED")
                    .cycleDate(new Date())
                    .attributeKey4(hashCode)
                    .originalMsg(message)
                    .insertedBy("KAFKA_CONSUMER")
                    .retryCount(0)
                    .build();
            
            IncomingMessage savedMessage = incomingMessageRepository.save(incomingMessage);
            
            // Acquire lease immediately after persistence
            leaseRenewalService.acquireLease(savedMessage.getMsgId());
            
            log.info("Persisted incoming message with lease: msgId={}, internalSourceId={}, correlationId={}", 
                     savedMessage.getMsgId(), internalSourceId, correlationId);
            
            String mapKey = correlationId + "_" + source;
            CORRELATION_MSG_ID_MAP.put(mapKey, savedMessage.getMsgId());
            
            IncomingTask task = IncomingTask.builder()
                    .msgId(savedMessage.getMsgId())
                    .source(source)
                    .xCorrelationId(correlationId)
                    .internalSourceId(internalSourceId)
                    .eventType(eventType)
                    .priority(priority)
                    .originalMessage(message)
                    .build();
            
            return task;
            
        } catch (Exception e) {
            log.error("Error persisting incoming message: correlationId={}", correlationId, e);
            throw new RuntimeException("Failed to persist incoming message", e);
        } finally {
            MDCUtil.clear();
        }
    }

    public static BigDecimal getMsgIdFromCorrelation(String correlationId, String source) {
        String mapKey = correlationId + "_" + source;
        return CORRELATION_MSG_ID_MAP.get(mapKey);
    }
}
