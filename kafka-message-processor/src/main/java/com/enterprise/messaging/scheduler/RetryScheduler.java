package com.enterprise.messaging.scheduler;

import com.enterprise.messaging.model.IncomingMessage;
import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.model.OutgoingMessage;
import com.enterprise.messaging.publisher.MessagePublisher;
import com.enterprise.messaging.repository.IncomingMessageRepository;
import com.enterprise.messaging.repository.OutgoingMessageRepository;
import com.enterprise.messaging.service.MessageProcessingOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class RetryScheduler {

    private final IncomingMessageRepository incomingMessageRepository;
    private final OutgoingMessageRepository outgoingMessageRepository;
    private final MessageProcessingOrchestrator orchestrator;
    private final MessagePublisher messagePublisher;

    @Value("${application.processing.hourly-retry.enabled:true}")
    private boolean hourlyRetryEnabled;

    @Value("${application.processing.hourly-retry.batch-size:100}")
    private int batchSize;

    @Value("#{'${application.processing.hourly-retry.retriable-error-codes}'.split(',')}")
    private List<String> retriableErrorCodes;

    @Scheduled(cron = "${application.processing.hourly-retry.cron:0 0 * * * *}")
    public void retryFailedMessages() {
        if (!hourlyRetryEnabled) {
            return;
        }

        log.info("Starting hourly retry job");

        retryIncomingMessages();
        retryOutgoingMessages();

        log.info("Hourly retry job completed");
    }

    private void retryIncomingMessages() {
        try {
            List<IncomingMessage> failedMessages = incomingMessageRepository
                    .findRetriableFailedMessages(retriableErrorCodes, 3);

            log.info("Found {} incoming messages to retry", failedMessages.size());

            for (IncomingMessage message : failedMessages) {
                try {
                    retryIncomingMessage(message);
                } catch (Exception e) {
                    log.error("Error retrying incoming message: msgId={}", message.getMsgId(), e);
                }
            }
        } catch (Exception e) {
            log.error("Error in retryIncomingMessages", e);
        }
    }

    private void retryIncomingMessage(IncomingMessage message) {
        IncomingTask task = IncomingTask.builder()
                .msgId(message.getMsgId())
                .source(message.getSource())
                .xCorrelationId(message.getXCorrelationId())
                .internalSourceId(message.getInternalSourceId())
                .eventType(message.getEventType())
                .priority(message.getPriority())
                .originalMessage(message.getOriginalMsg())
                .build();

        log.info("Retrying incoming message: msgId={}", message.getMsgId());
        orchestrator.processMessage(task);
    }

    private void retryOutgoingMessages() {
        try {
            List<OutgoingMessage> failedMessages = outgoingMessageRepository
                    .findRetriableFailedMessages(retriableErrorCodes, 3);

            log.info("Found {} outgoing messages to retry", failedMessages.size());

            for (OutgoingMessage message : failedMessages) {
                try {
                    retryOutgoingMessage(message);
                } catch (Exception e) {
                    log.error("Error retrying outgoing message: msgId={}", message.getMsgId(), e);
                }
            }
        } catch (Exception e) {
            log.error("Error in retryOutgoingMessages", e);
        }
    }

    private void retryOutgoingMessage(OutgoingMessage message) {
        log.info("Retrying outgoing message: msgId={}", message.getMsgId());
        String channel = determineChannel(message.getEventType());
        messagePublisher.publishToChannel(channel, message.getOriginalMsg(), message.getXCorrelationId());
    }

    private String determineChannel(String eventType) {
        return "EMAIL";
    }
}
