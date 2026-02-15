package com.enterprise.messaging.model;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.HashMap;
import static org.assertj.core.api.Assertions.*;

class IncomingTaskTest {
    @Test
    void testBuilder() {
        IncomingTask task = IncomingTask.builder()
                .msgId(new BigDecimal("1"))
                .source("SRC")
                .xCorrelationId("CORR")
                .internalSourceId(new BigDecimal("100"))
                .eventType("EVENT")
                .priority(5)
                .originalMessage("msg")
                .build();
        
        assertThat(task.getMsgId()).isEqualTo(new BigDecimal("1"));
        assertThat(task.getSource()).isEqualTo("SRC");
        assertThat(task.getPriority()).isEqualTo(5);
        assertThat(task.getOriginalMessage()).isEqualTo("msg");
    }

    @Test
    void testContextMaps() {
        IncomingTask task = IncomingTask.builder()
                .msgId(new BigDecimal("1"))
                .contextMap(new HashMap<>())
                .enrichmentMap(new HashMap<>())
                .validationMap(new HashMap<>())
                .build();
        
        assertThat(task.getContextMap()).isNotNull();
        assertThat(task.getEnrichmentMap()).isNotNull();
        assertThat(task.getValidationMap()).isNotNull();
    }
}
