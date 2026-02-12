package com.enterprise.messaging.service;

import com.enterprise.messaging.cache.SequenceCacheService;
import com.enterprise.messaging.model.IncomingMessage;
import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.repository.IncomingMessageRepository;
import com.enterprise.messaging.util.HashCodeGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessagePersistenceServiceTest {

    @Mock
    private IncomingMessageRepository incomingMessageRepository;

    @Mock
    private SequenceCacheService sequenceCacheService;

    @Mock
    private HashCodeGenerator hashCodeGenerator;

    @InjectMocks
    private MessagePersistenceService messagePersistenceService;

    private String testMessage;
    private String testCorrelationId;

    @BeforeEach
    void setUp() {
        testMessage = "{\"source\":\"TEST\",\"data\":\"test data\"}";
        testCorrelationId = "TEST-CORR-123";
    }

    @Test
    void testPersistIncomingMessage_Success() {
        // Arrange
        BigDecimal internalSourceId = new BigDecimal("1000001");
        BigDecimal msgId = new BigDecimal("1");
        String hashCode = "test-hash-code";

        when(sequenceCacheService.getNextSequence("INTERNAL_SOURCE_SEQUENCE"))
                .thenReturn(internalSourceId);
        when(hashCodeGenerator.generateHashCode(testMessage)).thenReturn(hashCode);
        
        IncomingMessage savedMessage = IncomingMessage.builder()
                .msgId(msgId)
                .source("TEST")
                .xCorrelationId(testCorrelationId)
                .internalSourceId(internalSourceId)
                .build();
        
        when(incomingMessageRepository.save(any(IncomingMessage.class)))
                .thenReturn(savedMessage);

        // Act
        IncomingTask result = messagePersistenceService.persistIncomingMessage(
                testMessage, "TEST", testCorrelationId, "TEST_EVENT", 5);

        // Assert
        assertThat(result).isNotNull();
        assertThat(result.getMsgId()).isEqualTo(msgId);
        assertThat(result.getInternalSourceId()).isEqualTo(internalSourceId);
        assertThat(result.getXCorrelationId()).isEqualTo(testCorrelationId);
    }

    @Test
    void testPersistIncomingMessage_WithDefaultPriority() {
        // Arrange
        BigDecimal internalSourceId = new BigDecimal("1000002");
        BigDecimal msgId = new BigDecimal("2");

        when(sequenceCacheService.getNextSequence("INTERNAL_SOURCE_SEQUENCE"))
                .thenReturn(internalSourceId);
        when(hashCodeGenerator.generateHashCode(any())).thenReturn("hash");
        
        IncomingMessage savedMessage = IncomingMessage.builder()
                .msgId(msgId)
                .priority(5)
                .internalSourceId(internalSourceId)
                .build();
        
        when(incomingMessageRepository.save(any())).thenReturn(savedMessage);

        // Act
        IncomingTask result = messagePersistenceService.persistIncomingMessage(
                testMessage, "TEST", testCorrelationId, "EVENT", null);

        // Assert
        assertThat(result.getPriority()).isEqualTo(5);
    }
}
