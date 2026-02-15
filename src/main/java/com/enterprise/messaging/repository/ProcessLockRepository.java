package com.enterprise.messaging.repository;

import com.enterprise.messaging.model.ProcessLock;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;

@Repository
public interface ProcessLockRepository extends JpaRepository<ProcessLock, String> {

    Optional<ProcessLock> findByLockNameAndLockStatus(String lockName, String lockStatus);

    @Modifying
    @Query("UPDATE ProcessLock pl SET pl.lockStatus = 'RELEASED', pl.lockUpdatedTime = :timestamp WHERE pl.lockName = :lockName AND pl.lockedBy = :lockedBy")
    int releaseLock(@Param("lockName") String lockName, @Param("lockedBy") String lockedBy, @Param("timestamp") LocalDateTime timestamp);
}
