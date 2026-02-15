package com.enterprise.messaging.config;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RedisConfigTest {
    @Test
    void testConfigurationExists() {
        RedisConfig config = new RedisConfig();
        assertThat(config).isNotNull();
    }
}
