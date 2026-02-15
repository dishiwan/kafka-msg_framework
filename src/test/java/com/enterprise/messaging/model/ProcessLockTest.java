package com.enterprise.messaging.model;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import static org.assertj.core.api.Assertions.*;

class ProcessLockTest {
    @Test
    void testAllFields() {
        ProcessLock lock = new ProcessLock();
        lock.setId(new BigDecimal("1"));
        lock.setLockKey("KEY");
        lock.setLockedBy("INSTANCE");
        lock.setLockedAt(LocalDateTime.now());
        lock.setExpiresAt(LocalDateTime.now().plusMinutes(5));
        
        assertThat(lock.getLockKey()).isEqualTo("KEY");
        assertThat(lock.getLockedBy()).isEqualTo("INSTANCE");
    }
}
