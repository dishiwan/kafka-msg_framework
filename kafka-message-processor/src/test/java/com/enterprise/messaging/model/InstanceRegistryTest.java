package com.enterprise.messaging.model;

import org.junit.jupiter.api.Test;
import java.time.LocalDateTime;
import static org.assertj.core.api.Assertions.*;

class InstanceRegistryTest {
    @Test
    void testIsAlive() {
        InstanceRegistry registry = new InstanceRegistry();
        registry.setLastHeartbeat(LocalDateTime.now());
        
        assertThat(registry.isAlive()).isTrue();
    }

    @Test
    void testIsNotAlive() {
        InstanceRegistry registry = new InstanceRegistry();
        registry.setLastHeartbeat(LocalDateTime.now().minusMinutes(10));
        
        assertThat(registry.isAlive()).isFalse();
    }

    @Test
    void testAllFields() {
        InstanceRegistry registry = new InstanceRegistry();
        registry.setInstanceId("inst-1");
        registry.setHostname("host1");
        registry.setIpAddress("192.168.1.1");
        registry.setInstanceStatus("ACTIVE");
        registry.setStartedTimestamp(LocalDateTime.now());
        
        assertThat(registry.getInstanceId()).isEqualTo("inst-1");
        assertThat(registry.getHostname()).isEqualTo("host1");
    }
}
