package com.enterprise.messaging.worker;

import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.publisher.MessagePublisher;
import org.junit.jupiter.api.BeforeEach;
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
class MessageWorkerTest {
    @Mock
    private MessagePublisher publisher;
    
    @InjectMocks
    private MessageWorker worker;
    
    private IncomingTask task;

    @BeforeEach
    void setUp() {
        task = IncomingTask.builder()
                .msgId(new BigDecimal("1"))
                .originalMessage("{"test":"data"}")
                .build();
    }

    @Test
    void testWork() {
        assertThatCode(() -> worker.work(task))
                .doesNotThrowAnyException();
    }
}
