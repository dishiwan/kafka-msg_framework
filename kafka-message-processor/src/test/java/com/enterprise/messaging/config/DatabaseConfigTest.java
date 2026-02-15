package com.enterprise.messaging.config;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DatabaseConfigTest {
    @Test
    void testConfigurationExists() {
        DatabaseConfig config = new DatabaseConfig();
        assertThat(config).isNotNull();
    }
}
