package com.enterprise.messaging.validator;

import com.enterprise.messaging.exception.ValidationException;
import com.enterprise.messaging.model.IncomingTask;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessageValidatorTest {

    @Mock
    private RedisTemplate<String, Object> redisTemplate;

    @Mock
    private ValueOperations<String, Object> valueOperations;

    @InjectMocks
    private MessageValidator messageValidator;

    private IncomingTask testTask;

    @BeforeEach
    void setUp() {
        messageValidator = new MessageValidator(redisTemplate, new ObjectMapper(), null, null);
        
        testTask = IncomingTask.builder()
                .msgId(new BigDecimal("1"))
                .source("TEST")
                .xCorrelationId("CORR-123")
                .originalMessage("{\"source\":\"TEST\",\"data\":\"test\"}")
                .build();

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void testValidate_Success_NoDuplicate() {
        // Arrange
        when(redisTemplate.hasKey(anyString())).thenReturn(false);

        // Act & Assert
        assertDoesNotThrow(() -> messageValidator.validate(testTask));
    }

    @Test
    void testValidate_ThrowsException_WhenDuplicate() {
        // Arrange
        when(redisTemplate.hasKey(anyString())).thenReturn(true);

        // Act & Assert
        assertThatThrownBy(() -> messageValidator.validate(testTask))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Duplicate");
    }

    @Test
    void testValidate_ThrowsException_InvalidFormat() {
        // Arrange
        testTask.setOriginalMessage("INVALID");
        when(redisTemplate.hasKey(anyString())).thenReturn(false);

        // Act & Assert
        assertThatThrownBy(() -> messageValidator.validate(testTask))
                .isInstanceOf(ValidationException.class);
    }
}
