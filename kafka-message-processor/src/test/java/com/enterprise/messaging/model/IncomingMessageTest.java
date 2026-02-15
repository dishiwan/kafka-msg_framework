package com.enterprise.messaging.model;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.Date;
import static org.assertj.core.api.Assertions.*;

class IncomingMessageTest {
    @Test
    void testBuilder() {
        IncomingMessage msg = IncomingMessage.builder()
                .msgId(new BigDecimal("1"))
                .source("SRC")
                .xCorrelationId("CORR")
                .internalSourceId(new BigDecimal("100"))
                .eventType("EVENT")
                .priority(5)
                .build();
        
        assertThat(msg.getMsgId()).isEqualTo(new BigDecimal("1"));
        assertThat(msg.getSource()).isEqualTo("SRC");
        assertThat(msg.getPriority()).isEqualTo(5);
    }

    @Test
    void testSettersAndGetters() {
        IncomingMessage msg = new IncomingMessage();
        msg.setMsgId(new BigDecimal("1"));
        msg.setSource("SRC");
        msg.setXCorrelationId("CORR");
        msg.setInternalSourceId(new BigDecimal("100"));
        msg.setEventType("EVENT");
        msg.setPriority(5);
        msg.setCycleDate(new Date());
        msg.setOriginalMsg("msg");
        msg.setInsertedBy("USER");
        msg.setRetryCount(0);
        
        assertThat(msg.getMsgId()).isEqualTo(new BigDecimal("1"));
        assertThat(msg.getSource()).isEqualTo("SRC");
        assertThat(msg.getXCorrelationId()).isEqualTo("CORR");
        assertThat(msg.getInternalSourceId()).isEqualTo(new BigDecimal("100"));
        assertThat(msg.getEventType()).isEqualTo("EVENT");
        assertThat(msg.getPriority()).isEqualTo(5);
        assertThat(msg.getOriginalMsg()).isEqualTo("msg");
        assertThat(msg.getInsertedBy()).isEqualTo("USER");
        assertThat(msg.getRetryCount()).isEqualTo(0);
    }
}
