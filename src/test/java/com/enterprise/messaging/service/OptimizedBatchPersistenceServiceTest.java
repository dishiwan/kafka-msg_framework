package com.enterprise.messaging.service;

import com.enterprise.messaging.cache.SequenceCacheService;
import com.enterprise.messaging.model.IncomingMessage;
import com.enterprise.messaging.repository.OptimizedIncomingMessageRepository;
import com.enterprise.messaging.util.HashCodeGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Optimized Batch Persistence Service Tests - HIGH COVERAGE")
class OptimizedBatchPersistenceServiceTest {

    @Mock
    private OptimizedIncomingMessageRepository repository;

    @Mock
    private SequenceCacheService sequenceCacheService;

    @Mock
    private HashCodeGenerator hashCodeGenerator;

    @Mock
    private InstanceRegistryService instanceRegistryService;

    @InjectMocks
    private OptimizedBatchPersistenceService service;

    @Test
    @DisplayName("Should batch persist 500 messages in single transaction")
    void testBatchPersist500Messages() {
        // Arrange
        List<String> messages = generateMessages(500);
        List<String> sources = generateSources(500);
        List<String> correlationIds = generateCorrelationIds(500);
        List<String> eventTypes = generateEventTypes(500);
        List<Integer> priorities = generatePriorities(500);

        when(sequenceCacheService.getNextSequence(anyString()))
                .thenAnswer(inv -> new BigDecimal(Math.random() * 1000000));
        when(hashCodeGenerator.generateHashCode(anyString()))
                .thenReturn("hash123");
        when(instanceRegistryService.getCurrentInstanceId())
                .thenReturn("instance-1");
        when(repository.saveAll(any()))
                .thenAnswer(inv -> {
                    List<IncomingMessage> msgs = inv.getArgument(0);
                    msgs.forEach(m -> m.setMsgId(new BigDecimal(Math.random() * 1000000)));
                    return msgs;
                });
        when(repository.batchAcquireLease(any(), anyString(), any(), any()))
                .thenReturn(500);

        // Act
        List<IncomingMessage> result = service.batchPersistMessages(
                messages, sources, correlationIds, eventTypes, priorities
        );

        // Assert
        assertThat(result).hasSize(500);
        verify(repository, times(1)).saveAll(any()); // Single batch call
        verify(repository, times(1)).batchAcquireLease(any(), anyString(), any(), any());
    }

    @Test
    @DisplayName("Should handle batch update status asynchronously")
    void testBatchUpdateStatusAsync() {
        // Arrange
        List<BigDecimal> msgIds = Arrays.asList(
                new BigDecimal("1001"),
                new BigDecimal("1002"),
                new BigDecimal("1003")
        );
        when(repository.batchUpdateInternalStatus(any(), anyString(), any(), anyString()))
                .thenReturn(3);

        // Act
        service.batchUpdateStatusAsync(msgIds, "PROCESSING");

        // Assert - async operation completes eventually
        verify(repository, timeout(1000).times(1))
                .batchUpdateInternalStatus(eq(msgIds), eq("PROCESSING"), any(), eq("SYSTEM"));
    }

    @Test
    @DisplayName("Should find orphaned messages with optimized query")
    void testFindOrphanedMessages() {
        // Arrange
        List<IncomingMessage> orphanedMessages = Arrays.asList(
                IncomingMessage.builder().msgId(new BigDecimal("1001")).build(),
                IncomingMessage.builder().msgId(new BigDecimal("1002")).build()
        );
        when(repository.findOrphanedMessagesBatch(anyInt(), any(), any(), anyInt()))
                .thenReturn(orphanedMessages);

        // Act
        List<IncomingMessage> result = service.findOrphanedMessages(100, 3);

        // Assert
        assertThat(result).hasSize(2);
        verify(repository, times(1)).findOrphanedMessagesBatch(
                eq(3),
                any(LocalDateTime.class),
                any(LocalDateTime.class),
                eq(100)
        );
    }

    private List<String> generateMessages(int count) {
        return Arrays.asList(new String[count]).stream()
                .map(s -> "{\"test\":\"message\"}")
                .toList();
    }

    private List<String> generateSources(int count) {
        return Arrays.asList(new String[count]).stream()
                .map(s -> "SOURCE_A")
                .toList();
    }

    private List<String> generateCorrelationIds(int count) {
        List<String> ids = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            ids.add("CORR-" + i);
        }
        return ids;
    }

    private List<String> generateEventTypes(int count) {
        return Arrays.asList(new String[count]).stream()
                .map(s -> "TEST_EVENT")
                .toList();
    }

    private List<Integer> generatePriorities(int count) {
        return Arrays.asList(new Integer[count]).stream()
                .map(s -> 5)
                .toList();
    }
}
