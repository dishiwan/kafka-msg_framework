package com.enterprise.messaging.controller;

import com.enterprise.messaging.model.IncomingMessage;
import com.enterprise.messaging.repository.IncomingMessageRepository;
import com.enterprise.messaging.service.MessageProcessingOrchestrator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import java.math.BigDecimal;
import java.util.Optional;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReprocessControllerTest {
    @Mock
    private IncomingMessageRepository repository;
    @Mock
    private MessageProcessingOrchestrator orchestrator;
    
    @InjectMocks
    private ReprocessController controller;

    @Test
    void testReprocessSuccess() {
        IncomingMessage msg = new IncomingMessage();
        when(repository.findById(any())).thenReturn(Optional.of(msg));
        
        ResponseEntity<String> response = controller.reprocessIncoming(new BigDecimal("1"));
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        verify(orchestrator, times(1)).processMessage(any());
    }

    @Test
    void testReprocessNotFound() {
        when(repository.findById(any())).thenReturn(Optional.empty());
        
        ResponseEntity<String> response = controller.reprocessIncoming(new BigDecimal("1"));
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void testReprocessException() {
        IncomingMessage msg = new IncomingMessage();
        when(repository.findById(any())).thenReturn(Optional.of(msg));
        doThrow(new RuntimeException("Error")).when(orchestrator).processMessage(any());
        
        ResponseEntity<String> response = controller.reprocessIncoming(new BigDecimal("1"));
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
