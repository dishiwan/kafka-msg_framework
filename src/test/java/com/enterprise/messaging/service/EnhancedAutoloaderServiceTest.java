package com.enterprise.messaging.service;

import com.enterprise.messaging.model.ProcessLock;
import com.enterprise.messaging.repository.IncomingMessageRepository;
import com.enterprise.messaging.repository.ProcessLockRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Enhanced Autoloader Service Tests")
class EnhancedAutoloaderServiceTest {

    @Mock
    private IncomingMessageRepository incomingMessageRepository;

    @Mock
    private ProcessLockRepository processLockRepository;

    @Mock
    private MessageProcessingOrchestrator orchestrator;

    @Mock
    private InstanceRegistryService instanceRegistryService;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @InjectMocks
    private EnhancedAutoloaderService autoloaderService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(autoloaderService, "autoloaderEnabled", true);
        ReflectionTestUtils.setField(autoloaderService, "processOnStartup", true);
        ReflectionTestUtils.setField(autoloaderService, "lockTimeoutMinutes", 15);
        ReflectionTestUtils.setField(autoloaderService, "maxRetryCount", 3);
        ReflectionTestUtils.setField(autoloaderService, "instanceHeartbeatTimeoutMinutes", 5);
        
        when(instanceRegistryService.getCurrentInstanceId()).thenReturn("test-instance");
    }

    @Test
    @DisplayName("Should process orphaned messages with expired lease")
    void testProcessOrphanedMessages_ExpiredLease() {
        // Arrange
        Map<String, Object> orphanedMessage = new HashMap<>();
        orphanedMessage.put("MSG_ID", new BigDecimal("1001"));
        orphanedMessage.put("SOURCE", "TEST_SOURCE");
        orphanedMessage.put("X_CORRELATION_ID", "CORR-123");
        orphanedMessage.put("INTERNAL_SOURCE_ID", new BigDecimal("1000001"));
        orphanedMessage.put("EVENT_TYPE", "TEST_EVENT");
        orphanedMessage.put("PRIORITY", 5);
        orphanedMessage.put("ORIGINAL_MSG", "{}");
        orphanedMessage.put("RETRY_COUNT", 0);

        when(processLockRepository.findByLockNameAndLockStatus(anyString(), anyString()))
                .thenReturn(Optional.empty());
        when(processLockRepository.save(any(ProcessLock.class)))
                .thenReturn(new ProcessLock());
        when(jdbcTemplate.queryForList(anyString(), any(), any(), any()))
                .thenReturn(Arrays.asList(orphanedMessage));
        when(jdbcTemplate.update(anyString(), any())).thenReturn(1);

        // Act
        autoloaderService.onApplicationReady();

        // Assert
        verify(orchestrator, times(1)).processMessage(any());
        verify(jdbcTemplate, times(1)).update(contains("RETRY_COUNT"), any());
    }

    @Test
    @DisplayName("Should mark message as FAILED_PERMANENTLY when max retries exceeded")
    void testProcessOrphanedMessages_MaxRetriesExceeded() {
        // Arrange
        Map<String, Object> orphanedMessage = new HashMap<>();
        orphanedMessage.put("MSG_ID", new BigDecimal("1002"));
        orphanedMessage.put("RETRY_COUNT", 3); // At max
        orphanedMessage.put("SOURCE", "TEST");
        orphanedMessage.put("X_CORRELATION_ID", "CORR-456");
        orphanedMessage.put("INTERNAL_SOURCE_ID", new BigDecimal("1000002"));
        orphanedMessage.put("EVENT_TYPE", "TEST");
        orphanedMessage.put("PRIORITY", 5);
        orphanedMessage.put("ORIGINAL_MSG", "{}");

        when(processLockRepository.findByLockNameAndLockStatus(anyString(), anyString()))
                .thenReturn(Optional.empty());
        when(processLockRepository.save(any(ProcessLock.class)))
                .thenReturn(new ProcessLock());
        when(jdbcTemplate.queryForList(anyString(), any(), any(), any()))
                .thenReturn(Arrays.asList(orphanedMessage));
        when(jdbcTemplate.update(anyString(), any())).thenReturn(1);

        // Act
        autoloaderService.onApplicationReady();

        // Assert
        verify(orchestrator, never()).processMessage(any()); // Should not retry
        verify(jdbcTemplate, times(1)).update(contains("FAILED_PERMANENTLY"), any());
    }

    @Test
    @DisplayName("Should not process when another instance holds lock")
    void testAcquireLock_AlreadyLocked() {
        // Arrange
        ProcessLock existingLock = ProcessLock.builder()
                .lockName("AUTOLOADER_STARTUP_LOCK")
                .lockedBy("other-instance")
                .lockStatus("ACTIVE")
                .lockExpiryTime(LocalDateTime.now().plusMinutes(10))
                .build();

        when(processLockRepository.findByLockNameAndLockStatus(anyString(), anyString()))
                .thenReturn(Optional.of(existingLock));

        // Act
        autoloaderService.onApplicationReady();

        // Assert
        verify(orchestrator, never()).processMessage(any());
    }

    @Test
    @DisplayName("Should take over expired lock")
    void testAcquireLock_ExpiredLock() {
        // Arrange
        ProcessLock expiredLock = ProcessLock.builder()
                .lockName("AUTOLOADER_STARTUP_LOCK")
                .lockedBy("dead-instance")
                .lockStatus("ACTIVE")
                .lockExpiryTime(LocalDateTime.now().minusMinutes(5)) // Expired
                .build();

        when(processLockRepository.findByLockNameAndLockStatus(anyString(), anyString()))
                .thenReturn(Optional.of(expiredLock));
        when(processLockRepository.save(any(ProcessLock.class)))
                .thenReturn(expiredLock);
        when(jdbcTemplate.queryForList(anyString(), any(), any(), any()))
                .thenReturn(Collections.emptyList());

        // Act
        autoloaderService.onApplicationReady();

        // Assert
        verify(processLockRepository, times(1)).save(any(ProcessLock.class));
    }
}
