package com.enterprise.messaging.repository;

import com.enterprise.messaging.model.InstanceRegistry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface InstanceRegistryRepository extends JpaRepository<InstanceRegistry, String> {

    List<InstanceRegistry> findByInstanceStatus(String status);

    @Query("SELECT ir FROM InstanceRegistry ir WHERE ir.lastHeartbeat < :threshold")
    List<InstanceRegistry> findDeadInstances(@Param("threshold") LocalDateTime threshold);

    @Modifying
    @Query("UPDATE InstanceRegistry ir SET ir.instanceStatus = 'DEAD', ir.stoppedTimestamp = :timestamp WHERE ir.instanceId = :instanceId")
    int markInstanceAsDead(@Param("instanceId") String instanceId, @Param("timestamp") LocalDateTime timestamp);

    @Modifying
    @Query("UPDATE InstanceRegistry ir SET ir.lastHeartbeat = :timestamp WHERE ir.instanceId = :instanceId")
    int updateHeartbeat(@Param("instanceId") String instanceId, @Param("timestamp") LocalDateTime timestamp);
}
