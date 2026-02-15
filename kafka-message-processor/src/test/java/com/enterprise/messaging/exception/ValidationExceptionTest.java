package com.enterprise.messaging.exception;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationExceptionTest {
    @Test
    void testConstructorWithMessage() {
        ValidationException ex = new ValidationException("Error message");
        assertThat(ex.getMessage()).isEqualTo("Error message");
    }

    @Test
    void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("Cause");
        ValidationException ex = new ValidationException("Error message", cause);
        assertThat(ex.getMessage()).isEqualTo("Error message");
        assertThat(ex.getCause()).isEqualTo(cause);
    }
}
