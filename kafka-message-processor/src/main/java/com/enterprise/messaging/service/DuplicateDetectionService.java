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
