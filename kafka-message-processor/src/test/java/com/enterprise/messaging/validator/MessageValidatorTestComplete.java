package com.enterprise.messaging.validator;

import com.enterprise.messaging.exception.ValidationException;
import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.service.DuplicateDetectionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.math.BigDecimal;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Message Validator - Complete Coverage")
class MessageValidatorTestComplete {
    @Mock
    private DuplicateDetectionService duplicateDetectionService;
    
    @InjectMocks
    private MessageValidator validator;
    
    private IncomingTask task;

    @BeforeEach
    void setUp() {
        task = IncomingTask.builder()
                .msgId(new BigDecimal("1001"))
                .source("SOURCE")
                .xCorrelationId("CORR-123")
                .originalMessage("{"valid":"json"}")
                .build();
    }

    @Test
    void testValidateSuccess() {
        when(duplicateDetectionService.isDuplicate(anyString(), anyString(), anyString()))
                .thenReturn(false);
        
        assertThatCode(() -> validator.validate(task))
                .doesNotThrowAnyException();
    }

    @Test
    void testValidateDuplicate() {
        when(duplicateDetectionService.isDuplicate(anyString(), anyString(), anyString()))
                .thenReturn(true);
        
        assertThatThrownBy(() -> validator.validate(task))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Duplicate");
    }

    @Test
    void testValidateNullMessage() {
        task.setOriginalMessage(null);
        
        assertThatThrownBy(() -> validator.validate(task))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testValidateEmptyMessage() {
        task.setOriginalMessage("");
        
        assertThatThrownBy(() -> validator.validate(task))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testValidateInvalidJson() {
        task.setOriginalMessage("{invalid json}");
        
        assertThatThrownBy(() -> validator.validate(task))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testValidateNullCorrelationId() {
        task.setXCorrelationId(null);
        when(duplicateDetectionService.isDuplicate(anyString(), anyString(), anyString()))
                .thenReturn(false);
        
        assertThatThrownBy(() -> validator.validate(task))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testValidateNullSource() {
        task.setSource(null);
        when(duplicateDetectionService.isDuplicate(anyString(), anyString(), anyString()))
                .thenReturn(false);
        
        assertThatThrownBy(() -> validator.validate(task))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testValidateXMLMessage() {
        task.setOriginalMessage("<root><valid>xml</valid></root>");
        when(duplicateDetectionService.isDuplicate(anyString(), anyString(), anyString()))
                .thenReturn(false);
        
        assertThatCode(() -> validator.validate(task))
                .doesNotThrowAnyException();
    }
}
