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

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Lease Renewal Service Tests")
class LeaseRenewalServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private InstanceRegistryService instanceRegistryService;

    @InjectMocks
    private LeaseRenewalService leaseRenewalService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(leaseRenewalService, "leaseDurationMinutes", 15);
        when(instanceRegistryService.getCurrentInstanceId()).thenReturn("test-instance");
    }

    @Test
    @DisplayName("Should acquire lease for message")
    void testAcquireLease() {
        // Arrange
        BigDecimal msgId = new BigDecimal("1001");
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any())).thenReturn(1);

        // Act
        leaseRenewalService.acquireLease(msgId);

        // Assert
        verify(jdbcTemplate, times(1)).update(
                anyString(), 
                eq("test-instance"), 
                any(LocalDateTime.class), 
                any(LocalDateTime.class), 
                eq(msgId)
        );
    }

    @Test
    @DisplayName("Should renew lease for message")
    void testRenewLease() {
        // Arrange
        BigDecimal msgId = new BigDecimal("1002");
        when(jdbcTemplate.update(anyString(), any(), any(), any())).thenReturn(1);

        // Act
        leaseRenewalService.renewLease(msgId);

        // Assert
        verify(jdbcTemplate, times(1)).update(
                anyString(),
                any(LocalDateTime.class),
                eq("test-instance"),
                eq(msgId)
        );
    }

    @Test
    @DisplayName("Should release lease for message")
    void testReleaseLease() {
        // Arrange
        BigDecimal msgId = new BigDecimal("1003");
        when(jdbcTemplate.update(anyString(), any())).thenReturn(1);

        // Act
        leaseRenewalService.releaseLease(msgId);

        // Assert
        verify(jdbcTemplate, times(1)).update(anyString(), eq(msgId));
    }

    @Test
    @DisplayName("Should handle errors gracefully during lease renewal")
    void testRenewLease_WithException() {
        // Arrange
        BigDecimal msgId = new BigDecimal("1004");
        when(jdbcTemplate.update(anyString(), any(), any(), any()))
                .thenThrow(new RuntimeException("Database error"));

        // Act - should not throw exception
        leaseRenewalService.renewLease(msgId);

        // Assert - verify error was handled
        verify(jdbcTemplate, times(1)).update(anyString(), any(), any(), any());
    }
}
