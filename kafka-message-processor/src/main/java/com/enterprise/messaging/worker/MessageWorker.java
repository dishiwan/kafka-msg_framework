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
