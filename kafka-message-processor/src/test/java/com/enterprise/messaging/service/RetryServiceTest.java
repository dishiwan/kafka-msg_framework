package com.enterprise.messaging.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class RetryServiceTest {
    private RetryService service;
    private List<String> nonRetriableErrors;

    @BeforeEach
    void setUp() {
        nonRetriableErrors = Arrays.asList(
            "VALIDATION_ERROR",
            "SCHEMA_INVALID",
            "DUPLICATE_MESSAGE"
        );
        service = new RetryService(nonRetriableErrors);
    }

    @Test
    void testRetriableError() {
        boolean result = service.isRetriable("NETWORK_ERROR");
        assertThat(result).isTrue();
    }

    @Test
    void testNonRetriableError() {
        boolean result = service.isRetriable("VALIDATION_ERROR");
        assertThat(result).isFalse();
    }

    @Test
    void testNullError() {
        boolean result = service.isRetriable(null);
        assertThat(result).isTrue();
    }

    @Test
    void testEmptyError() {
        boolean result = service.isRetriable("");
        assertThat(result).isTrue();
    }
}
