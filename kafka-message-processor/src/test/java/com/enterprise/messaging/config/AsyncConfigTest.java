package com.enterprise.messaging.config;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AsyncConfigTest {
    @Test
    void testConfigurationExists() {
        AsyncConfig config = new AsyncConfig();
        assertThat(config).isNotNull();
    }
}
