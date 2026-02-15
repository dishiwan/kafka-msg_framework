package com.enterprise.messaging.service;

import com.enterprise.messaging.cache.SequenceCacheService;
import com.enterprise.messaging.model.IncomingMessage;
import com.enterprise.messaging.repository.IncomingMessageRepository;
import com.enterprise.messaging.util.HashCodeGenerator;
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
class EnhancedMessagePersistenceServiceTestComplete {
    @Mock private IncomingMessageRepository repository;
    @Mock private SequenceCacheService sequenceCacheService;
    @Mock private HashCodeGenerator hashCodeGenerator;
    @Mock private LeaseRenewalService leaseRenewalService;
    @Mock private InstanceRegistryService instanceRegistryService;
    @Mock private CompositeSequenceService compositeSequenceService;
    
    @InjectMocks
    private EnhancedMessagePersistenceService service;

    @Test
    void testPersistWithAllFeatures() {
        when(sequenceCacheService.getNextSequence(anyString())).thenReturn(new BigDecimal("100"));
        when(hashCodeGenerator.generateHashCode(anyString())).thenReturn("hash");
        when(compositeSequenceService.generateBusinessKey()).thenReturn("ABC-20250215-000001");
        when(instanceRegistryService.getCurrentInstanceId()).thenReturn("instance-1");
        when(repository.save(any())).thenAnswer(inv -> {
            IncomingMessage msg = inv.getArgument(0);
            msg.setMsgId(new BigDecimal("1"));
            return msg;
        });
        doNothing().when(leaseRenewalService).acquireLease(any(), anyString());
        
        var result = service.persistIncomingMessage("msg", "src", "corr", "event", 5);
        
        assertThat(result).isNotNull();
        verify(leaseRenewalService).acquireLease(any(), anyString());
    }
}
