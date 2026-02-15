package com.enterprise.messaging.exception;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MessageProcessingExceptionTest {
    @Test
    void testConstructorWithMessage() {
        MessageProcessingException ex = new MessageProcessingException("Error");
        assertThat(ex.getMessage()).isEqualTo("Error");
    }

    @Test
    void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("Cause");
        MessageProcessingException ex = new MessageProcessingException("Error", cause);
        assertThat(ex.getMessage()).isEqualTo("Error");
        assertThat(ex.getCause()).isEqualTo(cause);
    }
}
