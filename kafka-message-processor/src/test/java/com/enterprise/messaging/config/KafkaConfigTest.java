package com.enterprise.messaging.config;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class KafkaConfigTest {
    @Test
    void testConfigurationExists() {
        KafkaConfig config = new KafkaConfig();
        assertThat(config).isNotNull();
    }
}
