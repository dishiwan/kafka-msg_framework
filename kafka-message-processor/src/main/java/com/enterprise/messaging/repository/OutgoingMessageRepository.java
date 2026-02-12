package com.enterprise.messaging.repository;

import com.enterprise.messaging.model.OutgoingMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OutgoingMessageRepository extends JpaRepository<OutgoingMessage, BigDecimal> {

    List<OutgoingMessage> findByInternalSourceId(BigDecimal internalSourceId);

    @Query("SELECT om FROM OutgoingMessage om WHERE om.finalStatus = 'FAILED' AND om.errorMessage IN :retriableErrors AND om.retryCount < :maxRetries")
    List<OutgoingMessage> findRetriableFailedMessages(@Param("retriableErrors") List<String> retriableErrors, @Param("maxRetries") Integer maxRetries);

    @Modifying
    @Query("UPDATE OutgoingMessage om SET om.phase = :phase, om.updateTimestamp = :timestamp WHERE om.msgId = :msgId")
    int updatePhase(@Param("msgId") BigDecimal msgId, @Param("phase") String phase, @Param("timestamp") LocalDateTime timestamp);

    boolean existsByAttributeKey4AndInsertTimestampAfter(String hashcode, LocalDateTime timestamp);
}
