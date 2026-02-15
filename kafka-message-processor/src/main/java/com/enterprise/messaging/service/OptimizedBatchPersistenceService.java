package com.enterprise.messaging.service;

import com.enterprise.messaging.cache.SequenceCacheService;
import com.enterprise.messaging.model.IncomingMessage;
import com.enterprise.messaging.repository.OptimizedIncomingMessageRepository;
import com.enterprise.messaging.util.HashCodeGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * OPTIMIZED: Batch persistence service
 * Reduces 500 individual DB calls to 1-2 batch calls
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OptimizedBatchPersistenceService {

    private final OptimizedIncomingMessageRepository repository;
    private final SequenceCacheService sequenceCacheService;
    private final HashCodeGenerator hashCodeGenerator;
    private final InstanceRegistryService instanceRegistryService;

    /**
     * OPTIMIZED: Batch persist messages
     * Single transaction, single batch insert
     * 
     * Before: 500 messages = 500 INSERT statements = 500 DB round trips
     * After:  500 messages = 1 batch INSERT = 1 DB round trip
     */
    @Transactional
    public List<IncomingMessage> batchPersistMessages(
            List<String> messages,
            List<String> sources,
            List<String> correlationIds,
            List<String> eventTypes,
            List<Integer> priorities
    ) {
        long startTime = System.currentTimeMillis();
        
        try {
            List<IncomingMessage> entities = new ArrayList<>();
            
            // Prepare all entities
            for (int i = 0; i < messages.size(); i++) {
                BigDecimal internalSourceId = sequenceCacheService.getNextSequence("INTERNAL_SOURCE_SEQUENCE");
                String hashCode = hashCodeGenerator.generateHashCode(messages.get(i));
                
                IncomingMessage entity = IncomingMessage.builder()
                        .source(sources.get(i))
                        .xCorrelationId(correlationIds.get(i))
                        .internalSourceId(internalSourceId)
                        .eventType(eventTypes.get(i))
                        .priority(priorities.get(i) != null ? priorities.get(i) : 5)
                        .internalStatus("IN_PROGRESS")
                        .phase("RECEIVED")
                        .cycleDate(new Date())
                        .attributeKey4(hashCode)
                        .originalMsg(messages.get(i))
                        .insertedBy("KAFKA_CONSUMER")
                        .retryCount(0)
                        .build();
                
                entities.add(entity);
            }
            
            // OPTIMIZED: Single batch save
            List<IncomingMessage> saved = repository.saveAll(entities);
            
            // OPTIMIZED: Batch acquire leases
            LocalDateTime leaseExpiry = LocalDateTime.now().plusMinutes(15);
            LocalDateTime startedTime = LocalDateTime.now();
            String instanceId = instanceRegistryService.getCurrentInstanceId();
            
            List<BigDecimal> msgIds = saved.stream()
                    .map(IncomingMessage::getMsgId)
                    .collect(Collectors.toList());
            
            repository.batchAcquireLease(msgIds, instanceId, leaseExpiry, startedTime);
            
            long duration = System.currentTimeMillis() - startTime;
            log.info("Batch persisted {} messages in {}ms ({} msg/sec)", 
                     saved.size(), duration, (saved.size() * 1000) / Math.max(duration, 1));
            
            return saved;
            
        } catch (Exception e) {
            log.error("Error batch persisting messages", e);
            throw new RuntimeException("Batch persistence failed", e);
        }
    }

    /**
     * OPTIMIZED: Async batch status update
     * Non-blocking status updates
     */
    public CompletableFuture<Void> batchUpdateStatusAsync(
            List<BigDecimal> msgIds,
            String status
    ) {
        return CompletableFuture.runAsync(() -> {
            try {
                repository.batchUpdateInternalStatus(
                        msgIds,
                        status,
                        LocalDateTime.now(),
                        "SYSTEM"
                );
            } catch (Exception e) {
                log.error("Error in async batch status update", e);
            }
        });
    }

    /**
     * OPTIMIZED: Find orphaned messages with pagination
     * Uses optimized query with covering index
     */
    @Transactional(readOnly = true)
    public List<IncomingMessage> findOrphanedMessages(int batchSize, int maxRetryCount) {
        LocalDateTime leaseThreshold = LocalDateTime.now();
        LocalDateTime heartbeatThreshold = LocalDateTime.now().minusMinutes(5);
        
        return repository.findOrphanedMessagesBatch(
                maxRetryCount,
                leaseThreshold,
                heartbeatThreshold,
                batchSize
        );
    }
}
