package com.enterprise.messaging.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Composite Sequence Service Tests - COMPREHENSIVE COVERAGE")
class CompositeSequenceServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @InjectMocks
    private CompositeSequenceService service;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "keyPrefix", "ABC");
        ReflectionTestUtils.setField(service, "sequenceLength", 6);
        ReflectionTestUtils.setField(service, "separator", "-");
        ReflectionTestUtils.setField(service, "resetStrategy", "DAILY");
        ReflectionTestUtils.setField(service, "enabled", true);
    }

    @Test
    @DisplayName("Should generate business key with correct format")
    void testGenerateBusinessKey() {
        // Arrange
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), anyString()))
                .thenReturn(1); // Sequence exists
        when(jdbcTemplate.queryForObject(contains("NEXTVAL"), eq(Long.class)))
                .thenReturn(123L);

        // Act
        String businessKey = service.generateBusinessKey();

        // Assert
        assertThat(businessKey).isNotNull();
        assertThat(businessKey).matches("ABC-\\d{8}-\\d{6}");
        
        String expectedDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        assertThat(businessKey).contains(expectedDate);
        assertThat(businessKey).endsWith("000123");
    }

    @Test
    @DisplayName("Should handle different prefix codes")
    void testDifferentPrefixes() {
        // Test DEV prefix
        ReflectionTestUtils.setField(service, "keyPrefix", "DEV");
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), anyString()))
                .thenReturn(1);
        when(jdbcTemplate.queryForObject(contains("NEXTVAL"), eq(Long.class)))
                .thenReturn(1L);

        String devKey = service.generateBusinessKey();
        assertThat(devKey).startsWith("DEV-");

        // Test PRD prefix
        ReflectionTestUtils.setField(service, "keyPrefix", "PRD");
        String prdKey = service.generateBusinessKey();
        assertThat(prdKey).startsWith("PRD-");
    }

    @Test
    @DisplayName("Should create daily sequence if not exists")
    void testDailySequenceCreation() {
        // Arrange
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), anyString()))
                .thenReturn(0); // Sequence does not exist
        when(jdbcTemplate.execute(anyString()))
                .thenReturn(true);
        when(jdbcTemplate.queryForObject(contains("NEXTVAL"), eq(Long.class)))
                .thenReturn(1L);

        // Act
        String businessKey = service.generateBusinessKey();

        // Assert
        verify(jdbcTemplate, times(1)).execute(contains("CREATE SEQUENCE"));
        assertThat(businessKey).isNotNull();
    }

    @Test
    @DisplayName("Should use continuous sequence strategy")
    void testContinuousSequence() {
        // Arrange
        ReflectionTestUtils.setField(service, "resetStrategy", "CONTINUOUS");
        when(jdbcTemplate.queryForObject(contains("BUSINESS_KEY_CONTINUOUS_SEQ"), eq(Long.class)))
                .thenReturn(456789L);

        // Act
        String businessKey = service.generateBusinessKey();

        // Assert
        assertThat(businessKey).endsWith("456789");
        verify(jdbcTemplate, never()).execute(contains("CREATE SEQUENCE"));
    }

    @Test
    @DisplayName("Should parse business key correctly")
    void testParseBusinessKey() {
        // Arrange
        String businessKey = "ABC-20250215-000123";

        // Act
        var components = service.parseBusinessKey(businessKey);

        // Assert
        assertThat(components).isNotNull();
        assertThat(components.getPrefix()).isEqualTo("ABC");
        assertThat(components.getDate()).isEqualTo(LocalDate.of(2025, 2, 15));
        assertThat(components.getSequence()).isEqualTo(123L);
    }

    @Test
    @DisplayName("Should handle null business key in parse")
    void testParseNullBusinessKey() {
        // Act
        var components = service.parseBusinessKey(null);

        // Assert
        assertThat(components).isNull();
    }

    @Test
    @DisplayName("Should format sequence with leading zeros")
    void testSequenceFormatting() {
        // Test various sequence numbers
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), anyString()))
                .thenReturn(1);

        // Sequence 1 -> 000001
        when(jdbcTemplate.queryForObject(contains("NEXTVAL"), eq(Long.class)))
                .thenReturn(1L);
        String key1 = service.generateBusinessKey();
        assertThat(key1).endsWith("000001");

        // Sequence 999 -> 000999
        when(jdbcTemplate.queryForObject(contains("NEXTVAL"), eq(Long.class)))
                .thenReturn(999L);
        String key999 = service.generateBusinessKey();
        assertThat(key999).endsWith("000999");

        // Sequence 123456 -> 123456
        when(jdbcTemplate.queryForObject(contains("NEXTVAL"), eq(Long.class)))
                .thenReturn(123456L);
        String keyMax = service.generateBusinessKey();
        assertThat(keyMax).endsWith("123456");
    }

    @Test
    @DisplayName("Should return null when disabled")
    void testDisabledService() {
        // Arrange
        ReflectionTestUtils.setField(service, "enabled", false);

        // Act
        String businessKey = service.generateBusinessKey();

        // Assert
        assertThat(businessKey).isNull();
        verify(jdbcTemplate, never()).queryForObject(anyString(), any());
    }

    @Test
    @DisplayName("Should handle concurrent sequence creation gracefully")
    void testConcurrentSequenceCreation() {
        // Arrange
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), anyString()))
                .thenReturn(0); // Sequence does not exist
        when(jdbcTemplate.execute(anyString()))
                .thenThrow(new RuntimeException("Sequence already exists"));
        when(jdbcTemplate.queryForObject(contains("NEXTVAL"), eq(Long.class)))
                .thenReturn(1L);

        // Act & Assert - should not throw exception
        String businessKey = service.generateBusinessKey();
        assertThat(businessKey).isNotNull();
    }
}
