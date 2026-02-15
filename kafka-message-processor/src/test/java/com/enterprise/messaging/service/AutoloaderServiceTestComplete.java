package com.enterprise.messaging.service;

import com.enterprise.messaging.model.IncomingMessage;
import com.enterprise.messaging.repository.IncomingMessageRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AutoloaderServiceTestComplete {
    @Mock private IncomingMessageRepository repository;
    @InjectMocks private AutoloaderService service;

    @Test
    void testLoadFailedMessagesFound() {
        ReflectionTestUtils.setField(service, "maxRetryCount", 3);
        
        IncomingMessage msg = new IncomingMessage();
        msg.setMsgId(new BigDecimal("1"));
        msg.setRetryCount(1);
        
        when(repository.findByInternalStatusAndRetryCountLessThan(anyString(), anyInt()))
                .thenReturn(Arrays.asList(msg));
        
        service.loadFailedMessages();
        
        verify(repository).findByInternalStatusAndRetryCountLessThan("IN_PROGRESS", 3);
    }

    @Test
    void testLoadFailedMessagesEmpty() {
        ReflectionTestUtils.setField(service, "maxRetryCount", 3);
        when(repository.findByInternalStatusAndRetryCountLessThan(anyString(), anyInt()))
                .thenReturn(Collections.emptyList());
        
        service.loadFailedMessages();
        
        verify(repository).findByInternalStatusAndRetryCountLessThan("IN_PROGRESS", 3);
    }
}
