#!/bin/bash

# Create all model/entity classes

cat > src/main/java/com/enterprise/messaging/model/OutgoingMessage.java << 'EOFOUT'
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
EOFOUT

cat > src/main/java/com/enterprise/messaging/model/DuplicateMessage.java << 'EOFDUP'
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
EOFDUP

cat > src/main/java/com/enterprise/messaging/model/ProcessLock.java << 'EOFLOCK'
package com.enterprise.messaging.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "PROCESS_LOCK_TABLE")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessLock {

    @Id
    @Column(name = "LOCK_NAME", length = 100)
    private String lockName;

    @Column(name = "LOCKED_BY", length = 200)
    private String lockedBy;

    @Column(name = "LOCK_STATUS", length = 20)
    private String lockStatus; // ACTIVE, RELEASED

    @CreationTimestamp
    @Column(name = "LOCK_ACQUIRED_TIME")
    private LocalDateTime lockAcquiredTime;

    @UpdateTimestamp
    @Column(name = "LOCK_UPDATED_TIME")
    private LocalDateTime lockUpdatedTime;

    @Column(name = "LOCK_EXPIRY_TIME")
    private LocalDateTime lockExpiryTime;

    @Version
    @Column(name = "VERSION")
    private Long version;

    public boolean isExpired() {
        return lockExpiryTime != null && LocalDateTime.now().isAfter(lockExpiryTime);
    }
}
EOFLOCK

cat > src/main/java/com/enterprise/messaging/model/IncomingTask.java << 'EOFTASK'
package com.enterprise.messaging.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IncomingTask implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private BigDecimal msgId;
    private String source;
    private String xCorrelationId;
    private BigDecimal internalSourceId;
    private String eventType;
    private String processType;
    private String originalMessage;
    private String transformedMessage;
    private Integer priority;
    
    @Builder.Default
    private Map<String, Object> contextData = new HashMap<>();
    
    @Builder.Default
    private Map<String, String> enrichmentData = new HashMap<>();
    
    @Builder.Default
    private Map<String, Object> validationResult = new HashMap<>();
    
    @Builder.Default
    private Map<String, Object> processingMetadata = new HashMap<>();

    public void addContextData(String key, Object value) {
        this.contextData.put(key, value);
    }

    public Object getContextData(String key) {
        return this.contextData.get(key);
    }

    public void addEnrichmentData(String key, String value) {
        this.enrichmentData.put(key, value);
    }

    public String getEnrichmentData(String key) {
        return this.enrichmentData.get(key);
    }

    public void addValidationResult(String key, Object value) {
        this.validationResult.put(key, value);
    }

    public void addProcessingMetadata(String key, Object value) {
        this.processingMetadata.put(key, value);
    }
}
EOFTASK

echo "Model classes created successfully!"
