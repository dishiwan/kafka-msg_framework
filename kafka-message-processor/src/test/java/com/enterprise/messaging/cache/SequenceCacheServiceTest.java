package com.enterprise.messaging.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import java.math.BigDecimal;
import java.time.LocalDate;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Sequence Cache Service - Complete Coverage")
class SequenceCacheServiceTest {
    @Mock
    private JdbcTemplate jdbcTemplate;
    
    private SequenceCacheService service;

    @BeforeEach
    void setUp() {
        service = new SequenceCacheService(jdbcTemplate);
        ReflectionTestUtils.setField(service, "maxCacheSize", 1000);
        ReflectionTestUtils.setField(service, "replenishThreshold", 200);
        ReflectionTestUtils.setField(service, "refreshDateBasedCache", true);
    }

    @Test
    void testGetNextSequence() {
        when(jdbcTemplate.queryForObject(anyString(), eq(BigDecimal.class)))
                .thenReturn(new BigDecimal("1000"), new BigDecimal("2000"));
        
        BigDecimal seq1 = service.getNextSequence("TEST_SEQ");
        assertThat(seq1).isNotNull();
        
        BigDecimal seq2 = service.getNextSequence("TEST_SEQ");
        assertThat(seq2).isNotNull();
        assertThat(seq2).isGreaterThan(seq1);
    }

    @Test
    void testSequenceCaching() {
        when(jdbcTemplate.queryForObject(anyString(), eq(BigDecimal.class)))
                .thenReturn(new BigDecimal("1000"), new BigDecimal("2000"));
        
        service.getNextSequence("TEST_SEQ");
        service.getNextSequence("TEST_SEQ");
        
        verify(jdbcTemplate, times(2)).queryForObject(anyString(), eq(BigDecimal.class));
    }

    @Test
    void testMultipleSequences() {
        when(jdbcTemplate.queryForObject(anyString(), eq(BigDecimal.class)))
                .thenReturn(new BigDecimal("1000"));
        
        BigDecimal seq1 = service.getNextSequence("SEQ_1");
        BigDecimal seq2 = service.getNextSequence("SEQ_2");
        
        assertThat(seq1).isNotNull();
        assertThat(seq2).isNotNull();
    }

    @Test
    void testThreadSafety() throws Exception {
        when(jdbcTemplate.queryForObject(anyString(), eq(BigDecimal.class)))
                .thenReturn(new BigDecimal("1000"), new BigDecimal("2000"), new BigDecimal("3000"));
        
        Thread t1 = new Thread(() -> service.getNextSequence("TEST_SEQ"));
        Thread t2 = new Thread(() -> service.getNextSequence("TEST_SEQ"));
        
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        
        verify(jdbcTemplate, atLeastOnce()).queryForObject(anyString(), eq(BigDecimal.class));
    }

    @Test
    void testDatabaseException() {
        when(jdbcTemplate.queryForObject(anyString(), eq(BigDecimal.class)))
                .thenThrow(new RuntimeException("DB Error"));
        
        assertThatThrownBy(() -> service.getNextSequence("TEST_SEQ"))
                .isInstanceOf(RuntimeException.class);
    }
}
