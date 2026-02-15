package com.enterprise.messaging.model;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.Date;
import static org.assertj.core.api.Assertions.*;

class OutgoingMessageTest {
    @Test
    void testAllFields() {
        OutgoingMessage msg = new OutgoingMessage();
        msg.setMsgId(new BigDecimal("1"));
        msg.setInternalSourceId(new BigDecimal("100"));
        msg.setOutgoingMsgId(new BigDecimal("200"));
        msg.setDestination("DEST");
        msg.setPublishedMsg("published");
        msg.setFinalStatus("SUCCESS");
        msg.setCycleDate(new Date());
        msg.setInsertTimestamp(new Date());
        msg.setUpdateTimestamp(new Date());
        
        assertThat(msg.getMsgId()).isEqualTo(new BigDecimal("1"));
        assertThat(msg.getInternalSourceId()).isEqualTo(new BigDecimal("100"));
        assertThat(msg.getOutgoingMsgId()).isEqualTo(new BigDecimal("200"));
        assertThat(msg.getDestination()).isEqualTo("DEST");
        assertThat(msg.getPublishedMsg()).isEqualTo("published");
        assertThat(msg.getFinalStatus()).isEqualTo("SUCCESS");
    }

    @Test
    void testBuilder() {
        OutgoingMessage msg = OutgoingMessage.builder()
                .msgId(new BigDecimal("1"))
                .destination("DEST")
                .build();
        
        assertThat(msg.getMsgId()).isEqualTo(new BigDecimal("1"));
        assertThat(msg.getDestination()).isEqualTo("DEST");
    }
}
