package com.enterprise.messaging.service;

import com.enterprise.messaging.model.IncomingMessage;
import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.model.ProcessLock;
import com.enterprise.messaging.repository.IncomingMessageRepository;
import com.enterprise.messaging.repository.ProcessLockRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AutoloaderService {

    private final IncomingMessageRepository incomingMessageRepository;
    private final ProcessLockRepository processLockRepository;
    private final MessageProcessingOrchestrator orchestrator;

    @Value("${application.autoloader.enabled:true}")
    private boolean autoloaderEnabled;

    @Value("${application.autoloader.process-on-startup:true}")
    private boolean processOnStartup;

    @Value("${application.autoloader.lock-timeout-minutes:15}")
    private int lockTimeoutMinutes;

    private static final String LOCK_NAME = "AUTOLOADER_STARTUP_LOCK";

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (!autoloaderEnabled || !processOnStartup) {
            log.info("Autoloader disabled or process-on-startup is false");
            return;
        }

        log.info("=================================================================");
        log.info("Autoloader Service Started - Processing in-progress messages");
        log.info("=================================================================");

        try {
            if (acquireLock()) {
                processInProgressMessages();
                releaseLock();
            } else {
                log.info("Another instance is already processing in-progress messages");
            }
        } catch (Exception e) {
            log.error("Error in autoloader service", e);
            releaseLockOnError();
        }
    }

    @Transactional
    private boolean acquireLock() {
        try {
            String instanceId = getInstanceId();
            LocalDateTime expiryTime = LocalDateTime.now().plusMinutes(lockTimeoutMinutes);

            var existingLock = processLockRepository.findByLockNameAndLockStatus(LOCK_NAME, "ACTIVE");
            
            if (existingLock.isPresent()) {
                ProcessLock lock = existingLock.get();
                if (lock.isExpired()) {
                    log.warn("Expired lock found, taking over: lockedBy={}", lock.getLockedBy());
                    lock.setLockedBy(instanceId);
                    lock.setLockStatus("ACTIVE");
                    lock.setLockExpiryTime(expiryTime);
                    processLockRepository.save(lock);
                    return true;
                } else {
                    log.info("Active lock held by another instance: {}", lock.getLockedBy());
                    return false;
                }
            }

            ProcessLock newLock = ProcessLock.builder()
                    .lockName(LOCK_NAME)
                    .lockedBy(instanceId)
                    .lockStatus("ACTIVE")
                    .lockExpiryTime(expiryTime)
                    .build();
            
            processLockRepository.save(newLock);
            log.info("Lock acquired successfully by instance: {}", instanceId);
            return true;

        } catch (Exception e) {
            log.error("Error acquiring lock", e);
            return false;
        }
    }

    @Transactional
    private void releaseLock() {
        try {
            String instanceId = getInstanceId();
            processLockRepository.releaseLock(LOCK_NAME, instanceId, LocalDateTime.now());
            log.info("Lock released successfully");
        } catch (Exception e) {
            log.error("Error releasing lock", e);
        }
    }

    private void releaseLockOnError() {
        try {
            releaseLock();
        } catch (Exception e) {
            log.error("Error releasing lock after error", e);
        }
    }

    private void processInProgressMessages() {
        try {
            LocalDateTime threshold = LocalDateTime.now().minusMinutes(lockTimeoutMinutes);
            List<IncomingMessage> inProgressMessages = incomingMessageRepository.findInProgressMessages("IN_PROGRESS", threshold);

            log.info("Found {} in-progress messages to process", inProgressMessages.size());

            for (IncomingMessage message : inProgressMessages) {
                try {
                    processInProgressMessage(message);
                } catch (Exception e) {
                    log.error("Error processing in-progress message: msgId={}", message.getMsgId(), e);
                }
            }

            log.info("Completed processing all in-progress messages");

        } catch (Exception e) {
            log.error("Error in processInProgressMessages", e);
        }
    }

    private void processInProgressMessage(IncomingMessage message) {
        IncomingTask task = IncomingTask.builder()
                .msgId(message.getMsgId())
                .source(message.getSource())
                .xCorrelationId(message.getXCorrelationId())
                .internalSourceId(message.getInternalSourceId())
                .eventType(message.getEventType())
                .priority(message.getPriority())
                .originalMessage(message.getOriginalMsg())
                .build();

        log.info("Reprocessing in-progress message: msgId={}, correlationId={}", message.getMsgId(), message.getXCorrelationId());
        orchestrator.processMessage(task);
    }

    private String getInstanceId() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            return hostname + "-" + UUID.randomUUID().toString().substring(0, 8);
        } catch (Exception e) {
            return "UNKNOWN-" + UUID.randomUUID().toString().substring(0, 8);
        }
    }
}
