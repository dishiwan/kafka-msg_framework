package com.enterprise.messaging.service;

import com.enterprise.messaging.model.InstanceRegistry;
import com.enterprise.messaging.repository.InstanceRegistryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Instance Registry Service Tests")
class InstanceRegistryServiceTest {

    @Mock
    private InstanceRegistryRepository instanceRegistryRepository;

    @InjectMocks
    private InstanceRegistryService instanceRegistryService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(instanceRegistryService, "heartbeatIntervalSeconds", 30);
        ReflectionTestUtils.setField(instanceRegistryService, "heartbeatTimeoutMinutes", 5);
    }

    @Test
    @DisplayName("Should register instance on startup")
    void testRegisterInstance() {
        // Arrange
        when(instanceRegistryRepository.save(any(InstanceRegistry.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        // Act
        instanceRegistryService.registerInstance();

        // Assert
        verify(instanceRegistryRepository, times(1)).save(any(InstanceRegistry.class));
        assertThat(instanceRegistryService.getCurrentInstanceId()).isNotNull();
    }

    @Test
    @DisplayName("Should send heartbeat successfully")
    void testSendHeartbeat() {
        // Arrange
        ReflectionTestUtils.setField(instanceRegistryService, "currentInstanceId", "test-instance-123");
        when(instanceRegistryRepository.updateHeartbeat(anyString(), any(LocalDateTime.class)))
                .thenReturn(1);

        // Act
        instanceRegistryService.sendHeartbeat();

        // Assert
        verify(instanceRegistryRepository, times(1))
                .updateHeartbeat(eq("test-instance-123"), any(LocalDateTime.class));
    }

    @Test
    @DisplayName("Should cleanup dead instances")
    void testCleanupDeadInstances() {
        // Arrange
        InstanceRegistry deadInstance1 = InstanceRegistry.builder()
                .instanceId("dead-1")
                .instanceStatus("ACTIVE")
                .lastHeartbeat(LocalDateTime.now().minusMinutes(10))
                .build();

        InstanceRegistry deadInstance2 = InstanceRegistry.builder()
                .instanceId("dead-2")
                .instanceStatus("ACTIVE")
                .lastHeartbeat(LocalDateTime.now().minusMinutes(15))
                .build();

        when(instanceRegistryRepository.findDeadInstances(any(LocalDateTime.class)))
                .thenReturn(Arrays.asList(deadInstance1, deadInstance2));
        when(instanceRegistryRepository.markInstanceAsDead(anyString(), any(LocalDateTime.class)))
                .thenReturn(1);

        // Act
        instanceRegistryService.cleanupDeadInstances();

        // Assert
        verify(instanceRegistryRepository, times(2))
                .markInstanceAsDead(anyString(), any(LocalDateTime.class));
    }

    @Test
    @DisplayName("Should get alive instances")
    void testGetAliveInstances() {
        // Arrange
        List<InstanceRegistry> aliveInstances = Arrays.asList(
                InstanceRegistry.builder().instanceId("instance-1").instanceStatus("ACTIVE").build(),
                InstanceRegistry.builder().instanceId("instance-2").instanceStatus("ACTIVE").build()
        );

        when(instanceRegistryRepository.findByInstanceStatus("ACTIVE"))
                .thenReturn(aliveInstances);

        // Act
        List<InstanceRegistry> result = instanceRegistryService.getAliveInstances();

        // Assert
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getInstanceId()).isEqualTo("instance-1");
    }

    @Test
    @DisplayName("Should identify dead instance based on heartbeat timeout")
    void testIsInstanceAlive() {
        // Arrange
        InstanceRegistry aliveInstance = InstanceRegistry.builder()
                .instanceId("alive")
                .instanceStatus("ACTIVE")
                .lastHeartbeat(LocalDateTime.now().minusMinutes(1))
                .build();

        InstanceRegistry deadInstance = InstanceRegistry.builder()
                .instanceId("dead")
                .instanceStatus("ACTIVE")
                .lastHeartbeat(LocalDateTime.now().minusMinutes(10))
                .build();

        // Act & Assert
        assertThat(aliveInstance.isAlive(5)).isTrue();
        assertThat(deadInstance.isAlive(5)).isFalse();
    }
}
