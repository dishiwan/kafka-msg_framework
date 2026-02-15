package com.enterprise.messaging.service;

import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.processor.MessageProcessor;
import com.enterprise.messaging.validator.MessageValidator;
import com.enterprise.messaging.performer.MessagePerformer;
import com.enterprise.messaging.worker.MessageWorker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.math.BigDecimal;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageProcessingOrchestratorTestComplete {
    @Mock private MessageValidator validator;
    @Mock private MessageProcessor processor;
    @Mock private MessagePerformer performer;
    @Mock private MessageWorker worker;
    
    @InjectMocks
    private MessageProcessingOrchestrator orchestrator;

    @Test
    void testProcessMessageSuccess() {
        IncomingTask task = IncomingTask.builder()
                .msgId(new BigDecimal("1"))
                .originalMessage("{}")
                .build();
        
        doNothing().when(validator).validate(any());
        doNothing().when(processor).process(any());
        doNothing().when(performer).perform(any());
        doNothing().when(worker).work(any());
        
        orchestrator.processMessage(task);
        
        verify(validator).validate(task);
        verify(processor).process(task);
        verify(performer).perform(task);
        verify(worker).work(task);
    }

    @Test
    void testProcessMessageValidationFailure() {
        IncomingTask task = IncomingTask.builder().msgId(new BigDecimal("1")).build();
        doThrow(new RuntimeException("Validation failed")).when(validator).validate(any());
        
        try {
            orchestrator.processMessage(task);
        } catch (Exception e) {
            // Expected
        }
        
        verify(validator).validate(task);
        verify(processor, never()).process(any());
    }

    @Test
    void testProcessMessageProcessorFailure() {
        IncomingTask task = IncomingTask.builder().msgId(new BigDecimal("1")).build();
        doNothing().when(validator).validate(any());
        doThrow(new RuntimeException("Process failed")).when(processor).process(any());
        
        try {
            orchestrator.processMessage(task);
        } catch (Exception e) {
            // Expected
        }
        
        verify(processor).process(task);
        verify(performer, never()).perform(any());
    }
}
