package com.enterprise.messaging.integration;

import com.enterprise.messaging.model.InstanceRegistry;
import com.enterprise.messaging.repository.InstanceRegistryRepository;
import com.enterprise.messaging.service.EnhancedAutoloaderService;
import com.enterprise.messaging.service.InstanceRegistryService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Orphan Detection scenarios
 * Tests multiple instances, crashes, lease expiry, etc.
 */
@SpringBootTest
@ActiveProfiles("test")
@DisplayName("Orphan Detection Integration Tests")
class OrphanDetectionIntegrationTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private InstanceRegistryRepository instanceRegistryRepository;

    @Autowired
    private InstanceRegistryService instanceRegistryService;

    @Test
    @Transactional
    @DisplayName("Scenario 1: Normal Processing - Message with valid lease should NOT be picked up")
    void testNormalProcessing_ValidLease() {
        // Arrange - Create alive instance
        InstanceRegistry aliveInstance = createInstance("instance-alive", "ACTIVE", LocalDateTime.now());
        instanceRegistryRepository.save(aliveInstance);

        // Create message with valid lease
        insertTestMessage("1001", "instance-alive", LocalDateTime.now().plusMinutes(10));

        // Act - Query for orphaned messages
        List<Map<String, Object>> orphans = findOrphanedMessages();

        // Assert - Should be empty (message is being actively processed)
        assertThat(orphans).isEmpty();
    }

    @Test
    @Transactional
    @DisplayName("Scenario 2: Instance Crash - Message with expired lease should be picked up")
    void testInstanceCrash_ExpiredLease() {
        // Arrange - Create dead instance
        InstanceRegistry deadInstance = createInstance("instance-dead", "DEAD", LocalDateTime.now().minusMinutes(10));
        instanceRegistryRepository.save(deadInstance);

        // Create message with expired lease
        insertTestMessage("1002", "instance-dead", LocalDateTime.now().minusMinutes(5));

        // Act - Query for orphaned messages
        List<Map<String, Object>> orphans = findOrphanedMessages();

        // Assert - Should find the orphaned message
        assertThat(orphans).hasSize(1);
        assertThat(orphans.get(0).get("MSG_ID")).isEqualTo(new BigDecimal("1002"));
    }

    @Test
    @Transactional
    @DisplayName("Scenario 3: Network Partition - Instance with old heartbeat should be detected")
    void testNetworkPartition_StaleHeartbeat() {
        // Arrange - Create instance with stale heartbeat
        InstanceRegistry partitionedInstance = createInstance("instance-partitioned", "ACTIVE", 
                LocalDateTime.now().minusMinutes(10));
        instanceRegistryRepository.save(partitionedInstance);

        // Create message owned by partitioned instance
        insertTestMessage("1003", "instance-partitioned", LocalDateTime.now().plusMinutes(5));

        // Act - Query for orphaned messages
        List<Map<String, Object>> orphans = findOrphanedMessages();

        // Assert - Should find the message (instance is considered dead due to stale heartbeat)
        assertThat(orphans).hasSize(1);
    }

    @Test
    @Transactional
    @DisplayName("Scenario 4: Slow Processing - Message with renewed lease should NOT be picked up")
    void testSlowProcessing_RenewedLease() {
        // Arrange - Create alive instance
        InstanceRegistry aliveInstance = createInstance("instance-slow", "ACTIVE", LocalDateTime.now());
        instanceRegistryRepository.save(aliveInstance);

        // Create message with renewed lease (simulating slow processing with renewals)
        insertTestMessage("1004", "instance-slow", LocalDateTime.now().plusMinutes(20));

        // Act - Query for orphaned messages
        List<Map<String, Object>> orphans = findOrphanedMessages();

        // Assert - Should be empty (lease is still valid)
        assertThat(orphans).isEmpty();
    }

    @Test
    @Transactional
    @DisplayName("Scenario 5: Max Retry Exceeded - Message should not be reprocessed")
    void testMaxRetryExceeded() {
        // Arrange
        InstanceRegistry deadInstance = createInstance("instance-retry", "DEAD", LocalDateTime.now().minusMinutes(10));
        instanceRegistryRepository.save(deadInstance);

        // Create message with retry count at maximum
        insertTestMessageWithRetry("1005", "instance-retry", LocalDateTime.now().minusMinutes(5), 3);

        // Act - Query for orphaned messages (max retry = 3)
        List<Map<String, Object>> orphans = findOrphanedMessages();

        // Assert - Should be empty (max retries exceeded)
        assertThat(orphans).isEmpty();
    }

    // Helper methods
    private InstanceRegistry createInstance(String id, String status, LocalDateTime heartbeat) {
        return InstanceRegistry.builder()
                .instanceId(id)
                .instanceHostname("test-host")
                .instanceIp("127.0.0.1")
                .instanceStatus(status)
                .lastHeartbeat(heartbeat)
                .startedTimestamp(LocalDateTime.now())
                .build();
    }

    private void insertTestMessage(String msgId, String instanceId, LocalDateTime leaseExpiry) {
        insertTestMessageWithRetry(msgId, instanceId, leaseExpiry, 0);
    }

    private void insertTestMessageWithRetry(String msgId, String instanceId, LocalDateTime leaseExpiry, int retryCount) {
        String sql = "INSERT INTO INCOMING_MESSAGE_TABLE (" +
                    "MSG_ID, SOURCE, X_CORRELATION_ID, INTERNAL_SOURCE_ID, " +
                    "INTERNAL_STATUS, PROCESSING_INSTANCE_ID, LEASE_EXPIRY_TIMESTAMP, " +
                    "RETRY_COUNT, PRIORITY, CYCLE_DATE, ORIGINAL_MSG, INSERTED_BY" +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, SYSDATE, ?, ?)";
        
        jdbcTemplate.update(sql,
                new BigDecimal(msgId),
                "TEST_SOURCE",
                "CORR-" + msgId,
                new BigDecimal("1000000"),
                "IN_PROGRESS",
                instanceId,
                leaseExpiry,
                retryCount,
                5,
                "{}",
                "TEST"
        );
    }

    private List<Map<String, Object>> findOrphanedMessages() {
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
                    ")";
        
        return jdbcTemplate.queryForList(sql, 
                3, // max retry count
                LocalDateTime.now(),
                LocalDateTime.now().minusMinutes(5)
        );
    }
}
