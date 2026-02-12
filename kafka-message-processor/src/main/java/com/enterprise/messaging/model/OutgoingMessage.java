package com.enterprise.messaging.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Date;

@Entity
@Table(name = "OUTGOING_MESSAGE_TABLE")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutgoingMessage {

    @Id
    @Column(name = "MSG_ID", precision = 16, scale = 0)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "outgoing_msg_id_seq_gen")
    @SequenceGenerator(name = "outgoing_msg_id_seq_gen", sequenceName = "OUTGOING_MSG_ID_SEQUENCE", allocationSize = 1)
    private BigDecimal msgId;

    @Column(name = "SOURCE", length = 100, nullable = false)
    private String source;

    @Column(name = "X_CORRELATION_ID", length = 200, nullable = false)
    private String xCorrelationId;

    @Column(name = "INTERNAL_SOURCE_ID", precision = 16, scale = 0, nullable = false)
    private BigDecimal internalSourceId;

    @Column(name = "OUTGOING_MSG_ID", length = 200, nullable = false)
    private String outgoingMsgId;

    @Column(name = "FINAL_STATUS", length = 50)
    private String finalStatus;

    @Column(name = "PHASE", length = 50)
    private String phase;

    @Column(name = "ERROR_MESSAGE", length = 2000)
    private String errorMessage;

    @Column(name = "EVENT_TYPE", length = 100)
    private String eventType;

    @Column(name = "PROCESS_TYPE", length = 100)
    private String processType;

    @Temporal(TemporalType.DATE)
    @Column(name = "CYCLE_DATE", nullable = false)
    private Date cycleDate;

    @Column(name = "ATTRIBUTE_KEY1", length = 500)
    private String attributeKey1;

    @Column(name = "ATTRIBUTE_KEY2", length = 500)
    private String attributeKey2;

    @Column(name = "ATTRIBUTE_KEY3", length = 500)
    private String attributeKey3;

    @Column(name = "ATTRIBUTE_KEY4", length = 500)
    private String attributeKey4;

    @Lob
    @Column(name = "ORIGINAL_MSG")
    private String originalMsg;

    @Lob
    @Column(name = "PROCESS_LOG")
    private String processLog;

    @CreationTimestamp
    @Column(name = "INSERT_TIMESTAMP", nullable = false, updatable = false)
    private LocalDateTime insertTimestamp;

    @UpdateTimestamp
    @Column(name = "UPDATE_TIMESTAMP")
    private LocalDateTime updateTimestamp;

    @Column(name = "INSERTED_BY", length = 100, nullable = false)
    private String insertedBy;

    @Column(name = "UPDATED_BY", length = 100)
    private String updatedBy;

    @Column(name = "RETRY_COUNT", precision = 3, scale = 0)
    private Integer retryCount;

    @PrePersist
    protected void onCreate() {
        if (insertedBy == null) {
            insertedBy = "SYSTEM";
        }
        if (cycleDate == null) {
            cycleDate = new Date();
        }
        if (retryCount == null) {
            retryCount = 0;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        if (updatedBy == null) {
            updatedBy = "SYSTEM";
        }
    }
}
