package com.enterprise.messaging.model;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import static org.assertj.core.api.Assertions.*;

class DuplicateMessageTest {
    @Test
    void testAllFields() {
        DuplicateMessage msg = new DuplicateMessage();
        msg.setId(new BigDecimal("1"));
        msg.setMsgId(new BigDecimal("100"));
        msg.setXCorrelationId("CORR");
        msg.setSource("SRC");
        msg.setMessageHashcode("hash");
        msg.setEventType("EVENT");
        msg.setInsertTimestamp(LocalDateTime.now());
        
        assertThat(msg.getId()).isEqualTo(new BigDecimal("1"));
        assertThat(msg.getMsgId()).isEqualTo(new BigDecimal("100"));
        assertThat(msg.getXCorrelationId()).isEqualTo("CORR");
        assertThat(msg.getSource()).isEqualTo("SRC");
        assertThat(msg.getMessageHashcode()).isEqualTo("hash");
    }
}
