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
