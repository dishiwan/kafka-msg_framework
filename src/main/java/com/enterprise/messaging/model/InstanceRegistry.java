package com.enterprise.messaging.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "INSTANCE_REGISTRY_TABLE")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InstanceRegistry {

    @Id
    @Column(name = "INSTANCE_ID", length = 200)
    private String instanceId;

    @Column(name = "INSTANCE_HOSTNAME", length = 200)
    private String instanceHostname;

    @Column(name = "INSTANCE_IP", length = 50)
    private String instanceIp;

    @Column(name = "INSTANCE_STATUS", length = 20)
    private String instanceStatus; // ACTIVE, DEAD

    @UpdateTimestamp
    @Column(name = "LAST_HEARTBEAT")
    private LocalDateTime lastHeartbeat;

    @Column(name = "STARTED_TIMESTAMP")
    private LocalDateTime startedTimestamp;

    @Column(name = "STOPPED_TIMESTAMP")
    private LocalDateTime stoppedTimestamp;

    @Version
    @Column(name = "VERSION")
    private Long version;

    public boolean isAlive(int heartbeatTimeoutMinutes) {
        if (lastHeartbeat == null) {
            return false;
        }
        LocalDateTime threshold = LocalDateTime.now().minusMinutes(heartbeatTimeoutMinutes);
        return lastHeartbeat.isAfter(threshold) && "ACTIVE".equals(instanceStatus);
    }
}
