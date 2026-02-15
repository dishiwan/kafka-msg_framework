package com.enterprise.messaging.repository;

import com.enterprise.messaging.model.IncomingMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.QueryHint;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * OPTIMIZED Repository for high-volume message processing
 * Implements batch operations, query hints, and optimized queries
 */
@Repository
public interface OptimizedIncomingMessageRepository extends JpaRepository<IncomingMessage, BigDecimal> {

    /**
     * OPTIMIZED: Batch save with JDBC batch processing
     * Saves 500 messages in single batch instead of 500 individual inserts
     */
    @Override
    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    <S extends IncomingMessage> List<S> saveAll(Iterable<S> entities);

    /**
     * OPTIMIZED: Single query with join to avoid N+1 problem
     * Uses covering index IDX_INCOMING_ORPHAN_COVER
     */
    @Query(value = "SELECT /*+ INDEX(m IDX_INCOMING_ORPHAN_COVER) */ m.* " +
                   "FROM INCOMING_MESSAGE_TABLE m " +
                   "WHERE m.INTERNAL_STATUS = 'IN_PROGRESS' " +
                   "AND m.RETRY_COUNT < :maxRetryCount " +
                   "AND (m.LEASE_EXPIRY_TIMESTAMP < :leaseThreshold " +
                   "     OR m.PROCESSING_INSTANCE_ID IN (" +
                   "         SELECT i.INSTANCE_ID FROM INSTANCE_REGISTRY_TABLE i " +
                   "         WHERE i.LAST_HEARTBEAT < :heartbeatThreshold " +
                   "         OR i.INSTANCE_STATUS = 'DEAD'" +
                   "     )) " +
                   "ORDER BY m.PRIORITY ASC, m.INSERT_TIMESTAMP ASC " +
                   "FETCH FIRST :batchSize ROWS ONLY",
           nativeQuery = true)
    @QueryHints({
        @QueryHint(name = "org.hibernate.fetchSize", value = "100"),
        @QueryHint(name = "org.hibernate.readOnly", value = "false"),
        @QueryHint(name = "org.hibernate.cacheable", value = "false")
    })
    List<IncomingMessage> findOrphanedMessagesBatch(
        @Param("maxRetryCount") int maxRetryCount,
        @Param("leaseThreshold") LocalDateTime leaseThreshold,
        @Param("heartbeatThreshold") LocalDateTime heartbeatThreshold,
        @Param("batchSize") int batchSize
    );

    /**
     * OPTIMIZED: Batch update status
     */
    @Modifying
    @Query(value = "UPDATE INCOMING_MESSAGE_TABLE " +
                   "SET INTERNAL_STATUS = :status, " +
                   "    UPDATE_TIMESTAMP = :timestamp, " +
                   "    UPDATED_BY = :updatedBy " +
                   "WHERE MSG_ID IN :msgIds",
           nativeQuery = true)
    @QueryHints(@QueryHint(name = "org.hibernate.flushMode", value = "COMMIT"))
    int batchUpdateInternalStatus(
        @Param("msgIds") List<BigDecimal> msgIds,
        @Param("status") String status,
        @Param("timestamp") LocalDateTime timestamp,
        @Param("updatedBy") String updatedBy
    );

    /**
     * OPTIMIZED: Batch update with lease
     */
    @Modifying
    @Query(value = "UPDATE /*+ INDEX(INCOMING_MESSAGE_TABLE IDX_INCOMING_MSG_ID_HASH) */ " +
                   "INCOMING_MESSAGE_TABLE " +
                   "SET PROCESSING_INSTANCE_ID = :instanceId, " +
                   "    LEASE_EXPIRY_TIMESTAMP = :leaseExpiry, " +
                   "    PROCESSING_STARTED_TIMESTAMP = :startedTime, " +
                   "    LAST_LEASE_RENEWAL = SYSTIMESTAMP " +
                   "WHERE MSG_ID IN :msgIds",
           nativeQuery = true)
    int batchAcquireLease(
        @Param("msgIds") List<BigDecimal> msgIds,
        @Param("instanceId") String instanceId,
        @Param("leaseExpiry") LocalDateTime leaseExpiry,
        @Param("startedTime") LocalDateTime startedTime
    );

    /**
     * OPTIMIZED: Uses bitmap index for status
     */
    @Query("SELECT COUNT(m) FROM IncomingMessage m WHERE m.finalStatus = :status")
    @QueryHints(@QueryHint(name = "org.hibernate.cacheable", value = "true"))
    long countByStatus(@Param("status") String status);

    /**
     * OPTIMIZED: Read-only query with result caching
     */
    @Query("SELECT m FROM IncomingMessage m WHERE m.xCorrelationId = :correlationId AND m.source = :source")
    @QueryHints({
        @QueryHint(name = "org.hibernate.cacheable", value = "true"),
        @QueryHint(name = "org.hibernate.cacheRegion", value = "messages"),
        @QueryHint(name = "org.hibernate.readOnly", value = "true")
    })
    IncomingMessage findByCorrelationAndSource(
        @Param("correlationId") String correlationId,
        @Param("source") String source
    );
}
