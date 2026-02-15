package com.enterprise.messaging.service;

import com.enterprise.messaging.cache.SequenceCacheService;
import com.enterprise.messaging.model.IncomingMessage;
import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.repository.IncomingMessageRepository;
import com.enterprise.messaging.util.HashCodeGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Message Persistence Service - Complete Coverage")
class MessagePersistenceServiceTestComplete {

    @Mock
    private IncomingMessageRepository repository;

    @Mock
    private SequenceCacheService sequenceCacheService;

    @Mock
    private HashCodeGenerator hashCodeGenerator;

    @InjectMocks
    private MessagePersistenceService service;

    private String testMessage;
    private String testSource;
    private String testCorrelationId;
    private String testEventType;

    @BeforeEach
    void setUp() {
        testMessage = "{\"test\":\"data\"}";
        testSource = "TEST_SOURCE";
        testCorrelationId = "CORR-12345";
        testEventType = "TEST_EVENT";
    }

    @Test
    @DisplayName("Should persist message successfully")
    void testPersistIncomingMessage() {
        // Arrange
        BigDecimal internalSourceId = new BigDecimal("1000001");
        String hashCode = "hash123";
        
        when(sequenceCacheService.getNextSequence(anyString())).thenReturn(internalSourceId);
        when(hashCodeGenerator.generateHashCode(anyString())).thenReturn(hashCode);
        when(repository.save(any(IncomingMessage.class))).thenAnswer(invocation -> {
            IncomingMessage msg = invocation.getArgument(0);
            msg.setMsgId(new BigDecimal("1001"));
            return msg;
        });

        // Act
        IncomingTask result = service.persistIncomingMessage(
                testMessage, testSource, testCorrelationId, testEventType, 5
        );

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.getMsgId()).isNotNull();
        assertThat(result.getInternalSourceId()).isEqualTo(internalSourceId);
        assertThat(result.getSource()).isEqualTo(testSource);
        assertThat(result.getXCorrelationId()).isEqualTo(testCorrelationId);
        
        verify(sequenceCacheService, times(1)).getNextSequence("INTERNAL_SOURCE_SEQUENCE");
        verify(hashCodeGenerator, times(1)).generateHashCode(testMessage);
        verify(repository, times(1)).save(any(IncomingMessage.class));
    }

    @Test
    @DisplayName("Should handle null priority with default value")
    void testPersistWithNullPriority() {
        // Arrange
        when(sequenceCacheService.getNextSequence(anyString())).thenReturn(new BigDecimal("1000001"));
        when(hashCodeGenerator.generateHashCode(anyString())).thenReturn("hash");
        when(repository.save(any(IncomingMessage.class))).thenAnswer(inv -> {
            IncomingMessage msg = inv.getArgument(0);
            msg.setMsgId(new BigDecimal("1001"));
            return msg;
        });

        // Act
        IncomingTask result = service.persistIncomingMessage(
                testMessage, testSource, testCorrelationId, testEventType, null
        );

        // Assert
        assertThat(result.getPriority()).isEqualTo(5); // Default priority
    }

    @Test
    @DisplayName("Should throw exception when sequence generation fails")
    void testPersistFailsOnSequenceGeneration() {
        // Arrange
        when(sequenceCacheService.getNextSequence(anyString()))
                .thenThrow(new RuntimeException("Sequence generation failed"));

        // Act & Assert
        assertThatThrownBy(() -> service.persistIncomingMessage(
                testMessage, testSource, testCorrelationId, testEventType, 5
        )).isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Failed to persist incoming message");
    }

    @Test
    @DisplayName("Should throw exception when hash generation fails")
    void testPersistFailsOnHashGeneration() {
        // Arrange
        when(sequenceCacheService.getNextSequence(anyString())).thenReturn(new BigDecimal("1000001"));
        when(hashCodeGenerator.generateHashCode(anyString()))
                .thenThrow(new RuntimeException("Hash generation failed"));

        // Act & Assert
        assertThatThrownBy(() -> service.persistIncomingMessage(
                testMessage, testSource, testCorrelationId, testEventType, 5
        )).isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("Should throw exception when repository save fails")
    void testPersistFailsOnRepositorySave() {
        // Arrange
        when(sequenceCacheService.getNextSequence(anyString())).thenReturn(new BigDecimal("1000001"));
        when(hashCodeGenerator.generateHashCode(anyString())).thenReturn("hash");
        when(repository.save(any(IncomingMessage.class)))
                .thenThrow(new RuntimeException("Database error"));

        // Act & Assert
        assertThatThrownBy(() -> service.persistIncomingMessage(
                testMessage, testSource, testCorrelationId, testEventType, 5
        )).isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Failed to persist incoming message");
    }

    @Test
    @DisplayName("Should handle empty message")
    void testPersistEmptyMessage() {
        // Arrange
        when(sequenceCacheService.getNextSequence(anyString())).thenReturn(new BigDecimal("1000001"));
        when(hashCodeGenerator.generateHashCode(anyString())).thenReturn("hash");
        when(repository.save(any(IncomingMessage.class))).thenAnswer(inv -> {
            IncomingMessage msg = inv.getArgument(0);
            msg.setMsgId(new BigDecimal("1001"));
            return msg;
        });

        // Act
        IncomingTask result = service.persistIncomingMessage(
                "", testSource, testCorrelationId, testEventType, 5
        );

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.getOriginalMessage()).isEmpty();
    }

    @Test
    @DisplayName("Should set correct initial status")
    void testInitialStatusIsInProgress() {
        // Arrange
        when(sequenceCacheService.getNextSequence(anyString())).thenReturn(new BigDecimal("1000001"));
        when(hashCodeGenerator.generateHashCode(anyString())).thenReturn("hash");
        when(repository.save(any(IncomingMessage.class))).thenAnswer(inv -> {
            IncomingMessage msg = inv.getArgument(0);
            assertThat(msg.getInternalStatus()).isEqualTo("IN_PROGRESS");
            assertThat(msg.getPhase()).isEqualTo("RECEIVED");
            assertThat(msg.getRetryCount()).isEqualTo(0);
            msg.setMsgId(new BigDecimal("1001"));
            return msg;
        });

        // Act
        service.persistIncomingMessage(testMessage, testSource, testCorrelationId, testEventType, 5);

        // Assert
        verify(repository, times(1)).save(any(IncomingMessage.class));
    }

    @Test
    @DisplayName("Should retrieve msgId from correlation map")
    void testGetMsgIdFromCorrelation() {
        // Arrange
        when(sequenceCacheService.getNextSequence(anyString())).thenReturn(new BigDecimal("1000001"));
        when(hashCodeGenerator.generateHashCode(anyString())).thenReturn("hash");
        when(repository.save(any(IncomingMessage.class))).thenAnswer(inv -> {
            IncomingMessage msg = inv.getArgument(0);
            msg.setMsgId(new BigDecimal("1001"));
            return msg;
        });

        // Act
        service.persistIncomingMessage(testMessage, testSource, testCorrelationId, testEventType, 5);
        BigDecimal retrievedMsgId = MessagePersistenceService.getMsgIdFromCorrelation(
                testCorrelationId, testSource
        );

        // Assert
        assertThat(retrievedMsgId).isEqualTo(new BigDecimal("1001"));
    }

    @Test
    @DisplayName("Should return null for non-existent correlation")
    void testGetMsgIdFromNonExistentCorrelation() {
        // Act
        BigDecimal result = MessagePersistenceService.getMsgIdFromCorrelation(
                "NON_EXISTENT", "SOURCE"
        );

        // Assert
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Should handle different priority values")
    void testDifferentPriorities() {
        // Arrange
        when(sequenceCacheService.getNextSequence(anyString())).thenReturn(new BigDecimal("1000001"));
        when(hashCodeGenerator.generateHashCode(anyString())).thenReturn("hash");
        when(repository.save(any(IncomingMessage.class))).thenAnswer(inv -> {
            IncomingMessage msg = inv.getArgument(0);
            msg.setMsgId(new BigDecimal("1001"));
            return msg;
        });

        // Test priority 1 (highest)
        IncomingTask result1 = service.persistIncomingMessage(
                testMessage, testSource, "CORR-1", testEventType, 1
        );
        assertThat(result1.getPriority()).isEqualTo(1);

        // Test priority 10 (lowest)
        IncomingTask result2 = service.persistIncomingMessage(
                testMessage, testSource, "CORR-2", testEventType, 10
        );
        assertThat(result2.getPriority()).isEqualTo(10);
    }
}
