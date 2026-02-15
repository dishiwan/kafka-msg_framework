package com.enterprise.messaging.integration;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
class EndToEndFlowTest {
    @Test
    void contextLoads() {
        assertThat(true).isTrue();
    }
}
