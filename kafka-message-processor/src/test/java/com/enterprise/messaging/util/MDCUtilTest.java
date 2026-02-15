package com.enterprise.messaging.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import java.math.BigDecimal;
import static org.assertj.core.api.Assertions.*;

@DisplayName("MDC Util - Complete Coverage")
class MDCUtilTest {
    @AfterEach
    void cleanup() {
        MDC.clear();
    }

    @Test
    void testPutCorrelationId() {
        MDCUtil.putCorrelationId("CORR-123");
        assertThat(MDC.get("correlationId")).isEqualTo("CORR-123");
    }

    @Test
    void testPutMsgId() {
        MDCUtil.putMsgId(new BigDecimal("1001"));
        assertThat(MDC.get("msgId")).isEqualTo("1001");
    }

    @Test
    void testPutSource() {
        MDCUtil.putSource("SOURCE");
        assertThat(MDC.get("source")).isEqualTo("SOURCE");
    }

    @Test
    void testPutEventType() {
        MDCUtil.putEventType("EVENT");
        assertThat(MDC.get("eventType")).isEqualTo("EVENT");
    }

    @Test
    void testClear() {
        MDCUtil.putCorrelationId("CORR-123");
        MDCUtil.clear();
        assertThat(MDC.get("correlationId")).isNull();
    }

    @Test
    void testNullCorrelationId() {
        MDCUtil.putCorrelationId(null);
        assertThat(MDC.get("correlationId")).isEqualTo("null");
    }

    @Test
    void testAllContextValues() {
        MDCUtil.putCorrelationId("C1");
        MDCUtil.putMsgId(new BigDecimal("1"));
        MDCUtil.putSource("S1");
        MDCUtil.putEventType("E1");
        
        assertThat(MDC.get("correlationId")).isEqualTo("C1");
        assertThat(MDC.get("msgId")).isEqualTo("1");
        assertThat(MDC.get("source")).isEqualTo("S1");
        assertThat(MDC.get("eventType")).isEqualTo("E1");
    }
}
