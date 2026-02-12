package com.enterprise.messaging.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "DUPLICATE_MESSAGE_TABLE")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DuplicateMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "dup_msg_seq_gen")
    @SequenceGenerator(name = "dup_msg_seq_gen", sequenceName = "DUPLICATE_MSG_SEQUENCE", allocationSize = 1)
    @Column(name = "ID", precision = 16, scale = 0)
    private BigDecimal id;

    @Column(name = "SOURCE", length = 100, nullable = false)
    private String source;

    @Column(name = "X_CORRELATION_ID", length = 200, nullable = false)
    private String xCorrelationId;

    @Column(name = "MESSAGE_HASHCODE", length = 500)
    private String messageHashcode;

    @Column(name = "DUPLICATE_TYPE", length = 50)
    private String duplicateType; // INCOMING or OUTGOING

    @Column(name = "ORIGINAL_MSG_ID", precision = 16, scale = 0)
    private BigDecimal originalMsgId;

    @Column(name = "DUPLICATE_MSG_ID", precision = 16, scale = 0)
    private BigDecimal duplicateMsgId;

    @Lob
    @Column(name = "DETAILS")
    private String details;

    @CreationTimestamp
    @Column(name = "DETECTED_TIMESTAMP", nullable = false, updatable = false)
    private LocalDateTime detectedTimestamp;

    @Column(name = "DETECTED_BY", length = 100)
    private String detectedBy;

    @PrePersist
    protected void onCreate() {
        if (detectedBy == null) {
            detectedBy = "SYSTEM";
        }
    }
}
