package com.enterprise.messaging.consumer;

import com.enterprise.messaging.service.MessagePersistenceService;
import com.enterprise.messaging.service.MessageProcessingOrchestrator;
import com.enterprise.messaging.util.PriorityCalculator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;
import java.util.Arrays;
import java.util.List;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaMessageConsumerTest {
    @Mock
    private MessagePersistenceService persistenceService;
    @Mock
    private MessageProcessingOrchestrator orchestrator;
    @Mock
    private PriorityCalculator priorityCalculator;
    @Mock
    private Acknowledgment acknowledgment;
    
    @InjectMocks
    private KafkaMessageConsumer consumer;

    @Test
    void testConsumeMessages() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0, "key", "value");
        List<ConsumerRecord<String, String>> records = Arrays.asList(record);
        
        when(priorityCalculator.calculatePriority(anyString(), anyString())).thenReturn(5);
        
        consumer.consumeMessages(records, acknowledgment);
        
        verify(acknowledgment, times(1)).acknowledge();
    }

    @Test
    void testConsumeEmptyBatch() {
        consumer.consumeMessages(Arrays.asList(), acknowledgment);
        verify(acknowledgment, times(1)).acknowledge();
    }
}
