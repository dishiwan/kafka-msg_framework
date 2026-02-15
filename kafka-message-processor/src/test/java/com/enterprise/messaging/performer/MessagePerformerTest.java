package com.enterprise.messaging.performer;

import com.enterprise.messaging.model.IncomingTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import java.math.BigDecimal;
import static org.assertj.core.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class MessagePerformerTest {
    @InjectMocks
    private MessagePerformer performer;
    
    private IncomingTask task;

    @BeforeEach
    void setUp() {
        task = IncomingTask.builder()
                .msgId(new BigDecimal("1"))
                .originalMessage("{"test":"data"}")
                .build();
    }

    @Test
    void testPerform() {
        assertThatCode(() -> performer.perform(task))
                .doesNotThrowAnyException();
    }
}
