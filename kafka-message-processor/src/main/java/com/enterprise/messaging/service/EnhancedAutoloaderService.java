package com.enterprise.messaging.service;

import com.enterprise.messaging.model.IncomingMessage;
import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.model.ProcessLock;
import com.enterprise.messaging.repository.IncomingMessageRepository;
import com.enterprise.messaging.repository.ProcessLockRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class EnhancedAutoloaderService {

    private final IncomingMessageRepository incomingMessageRepository;
    private final ProcessLockRepository processLockRepository;
    private final MessageProcessingOrchestrator orchestrator;
    private final InstanceRegistryService instanceRegistryService;
    private final JdbcTemplate jdbcTemplate;

    @Value("${application.autoloader.enabled:true}")
    private boolean autoloaderEnabled;

    @Value("${application.autoloader.process-on-startup:true}")
    private boolean processOnStartup;

    @Value("${application.autoloader.lock-timeout-minutes:15}")
    private int lockTimeoutMinutes;

    @Value("${application.autoloader.max-retry-count:3}")
    private int maxRetryCount;

    @Value("${application.instance.heartbeat-timeout-minutes:5}")
    private int instanceHeartbeatTimeoutMinutes;

    private static final String LOCK_NAME = "AUTOLOADER_STARTUP_LOCK";

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (!autoloaderEnabled || !processOnStartup) {
            log.info("Autoloader disabled or process-on-startup is false");
            return;
        }

        log.info("=================================================================");
        log.info("ENHANCED Autoloader Service Started - Processing orphaned messages");
        log.info("=================================================================");

        try {
            if (acquireLock()) {
                processOrphanedMessages();
                releaseLock();
            } else {
                log.info("Another instance is already processing orphaned messages");
            }
        } catch (Exception e) {
            log.error("Error in enhanced autoloader service", e);
            releaseLockOnError();
        }
    }

    /**
     * Processes messages that are truly orphaned:
     * 1. Lease has expired AND/OR
     * 2. Processing instance is dead
     * 3. Retry count has not exceeded maximum
     */
    @Transactional
    private void processOrphanedMessages() {
        try {
            List<Map<String, Object>> orphanedMessages = findOrphanedMessages();
            
            log.info("Found {} orphaned messages to process", orphanedMessages.size());

            for (Map<String, Object> messageRow : orphanedMessages) {
                try {
                    processOrphanedMessage(messageRow);
                } catch (Exception e) {
                    BigDecimal msgId = (BigDecimal) messageRow.get("MSG_ID");
                    log.error("Error processing orphaned message: msgId={}", msgId, e);
                }
            }

            log.info("Completed processing all orphaned messages");

        } catch (Exception e) {
            log.error("Error in processOrphanedMessages", e);
        }
    }

    /**
     * Finds orphaned messages using SQL query
     * A message is orphaned if:
     * - INTERNAL_STATUS = 'IN_PROGRESS'
     * - AND (LEASE_EXPIRY_TIMESTAMP < SYSDATE 
     *        OR PROCESSING_INSTANCE_ID is from a dead instance)
     * - AND RETRY_COUNT < max_retry_count
     */
    private List<Map<String, Object>> findOrphanedMessages() {
        LocalDateTime leaseThreshold = LocalDateTime.now();
        LocalDateTime heartbeatThreshold = LocalDateTime.now().minusMinutes(instanceHeartbeatTimeoutMinutes);

        String sql = "SELECT m.* FROM INCOMING_MESSAGE_TABLE m " +
                    "WHERE m.INTERNAL_STATUS = 'IN_PROGRESS' " +
                    "AND m.RETRY_COUNT < ? " +
                    "AND ( " +
                    "  m.LEASE_EXPIRY_TIMESTAMP < ? " +
                    "  OR m.PROCESSING_INSTANCE_ID IS NULL " +
                    "  OR m.PROCESSING_INSTANCE_ID IN ( " +
                    "    SELECT i.INSTANCE_ID FROM INSTANCE_REGISTRY_TABLE i " +
                    "    WHERE i.LAST_HEARTBEAT < ? " +
                    "    OR i.INSTANCE_STATUS = 'DEAD' " +
                    "  ) " +
                    ") " +
                    "ORDER BY m.PRIORITY ASC, m.INSERT_TIMESTAMP ASC";

        return jdbcTemplate.queryForList(sql, maxRetryCount, leaseThreshold, heartbeatThreshold);
    }

    private void processOrphanedMessage(Map<String, Object> messageRow) {
        BigDecimal msgId = (BigDecimal) messageRow.get("MSG_ID");
        Integer retryCount = ((Number) messageRow.get("RETRY_COUNT")).intValue();

        // Check retry limit
        if (retryCount >= maxRetryCount) {
            log.warn("Message exceeded max retry count: msgId={}, retryCount={}", msgId, retryCount);
            markAsFailedPermanently(msgId);
            return;
        }

        // Increment retry count
        incrementRetryCount(msgId);

        // Create IncomingTask for reprocessing
        IncomingTask task = IncomingTask.builder()
                .msgId(msgId)
                .source((String) messageRow.get("SOURCE"))
                .xCorrelationId((String) messageRow.get("X_CORRELATION_ID"))
                .internalSourceId((BigDecimal) messageRow.get("INTERNAL_SOURCE_ID"))
                .eventType((String) messageRow.get("EVENT_TYPE"))
                .priority(((Number) messageRow.get("PRIORITY")).intValue())
                .originalMessage((String) messageRow.get("ORIGINAL_MSG"))
                .build();

        log.info("Reprocessing orphaned message: msgId={}, correlationId={}, retryAttempt={}", 
                msgId, task.getXCorrelationId(), retryCount + 1);

        orchestrator.processMessage(task);
    }

    @Transactional
    private void incrementRetryCount(BigDecimal msgId) {
        String sql = "UPDATE INCOMING_MESSAGE_TABLE SET RETRY_COUNT = RETRY_COUNT + 1 WHERE MSG_ID = ?";
        jdbcTemplate.update(sql, msgId);
    }

    @Transactional
    private void markAsFailedPermanently(BigDecimal msgId) {
        String sql = "UPDATE INCOMING_MESSAGE_TABLE SET " +
                    "FINAL_STATUS = 'FAILED_PERMANENTLY', " +
                    "INTERNAL_STATUS = 'MAX_RETRY_EXCEEDED', " +
                    "ERROR_MESSAGE = 'Maximum retry count exceeded', " +
                    "UPDATE_TIMESTAMP = SYSTIMESTAMP " +
                    "WHERE MSG_ID = ?";
        jdbcTemplate.update(sql, msgId);
        log.warn("Marked message as FAILED_PERMANENTLY: msgId={}", msgId);
    }

    @Transactional
    private boolean acquireLock() {
        try {
            String instanceId = instanceRegistryService.getCurrentInstanceId();
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
            String instanceId = instanceRegistryService.getCurrentInstanceId();
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
}
