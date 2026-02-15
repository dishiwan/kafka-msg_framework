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
