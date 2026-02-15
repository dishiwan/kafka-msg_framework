package com.enterprise.messaging.service;

import com.enterprise.messaging.model.DuplicateMessage;
import com.enterprise.messaging.repository.DuplicateMessageRepository;
import com.enterprise.messaging.util.HashCodeGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DuplicateDetectionServiceTest {
    @Mock
    private DuplicateMessageRepository repository;
    @Mock
    private HashCodeGenerator hashCodeGenerator;
    @Mock
    private RedisTemplate<String, String> redisTemplate;
    @Mock
    private ValueOperations<String, String> valueOperations;
    
    @InjectMocks
    private DuplicateDetectionService service;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void testIsDuplicateInRedis() {
        when(valueOperations.get(anyString())).thenReturn("EXISTS");
        boolean result = service.isDuplicate("CORR", "SRC", "MSG");
        assertThat(result).isTrue();
    }

    @Test
    void testIsNotDuplicate() {
        when(valueOperations.get(anyString())).thenReturn(null);
        when(repository.findByMessageHashcode(anyString(), any())).thenReturn(Optional.empty());
        boolean result = service.isDuplicate("CORR", "SRC", "MSG");
        assertThat(result).isFalse();
    }

    @Test
    void testRecordDuplicate() {
        when(repository.save(any())).thenReturn(new DuplicateMessage());
        assertThatCode(() -> service.recordDuplicate(new BigDecimal("1"), "CORR", "SRC", "TYPE"))
                .doesNotThrowAnyException();
    }
}
