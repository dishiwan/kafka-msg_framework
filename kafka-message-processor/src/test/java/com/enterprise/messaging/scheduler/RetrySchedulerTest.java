package com.enterprise.messaging.scheduler;

import com.enterprise.messaging.repository.IncomingMessageRepository;
import com.enterprise.messaging.service.RetryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.Collections;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RetrySchedulerTest {
    @Mock
    private IncomingMessageRepository repository;
    @Mock
    private RetryService retryService;
    
    @InjectMocks
    private RetryScheduler scheduler;

    @Test
    void testRetryFailedMessages() {
        when(repository.findByFinalStatus(anyString())).thenReturn(Collections.emptyList());
        assertThatCode(() -> scheduler.retryFailedMessages())
                .doesNotThrowAnyException();
    }
}
