package com.enterprise.messaging.repository;

import com.enterprise.messaging.model.IncomingMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface IncomingMessageRepository extends JpaRepository<IncomingMessage, BigDecimal> {

    Optional<IncomingMessage> findByXCorrelationIdAndSource(String xCorrelationId, String source);

    List<IncomingMessage> findByFinalStatusAndRetryCountLessThan(String status, Integer maxRetryCount);

    @Query("SELECT im FROM IncomingMessage im WHERE im.internalStatus = :status AND im.insertTimestamp < :threshold")
    List<IncomingMessage> findInProgressMessages(@Param("status") String status, @Param("threshold") LocalDateTime threshold);

    @Query("SELECT im FROM IncomingMessage im WHERE im.finalStatus = 'FAILED' AND im.errorMessage IN :retriableErrors AND im.retryCount < :maxRetries")
    List<IncomingMessage> findRetriableFailedMessages(@Param("retriableErrors") List<String> retriableErrors, @Param("maxRetries") Integer maxRetries);

    @Modifying
    @Query("UPDATE IncomingMessage im SET im.internalStatus = :status, im.updateTimestamp = :timestamp, im.updatedBy = :updatedBy WHERE im.msgId = :msgId")
    int updateInternalStatus(@Param("msgId") BigDecimal msgId, @Param("status") String status, @Param("timestamp") LocalDateTime timestamp, @Param("updatedBy") String updatedBy);

    @Modifying
    @Query("UPDATE IncomingMessage im SET im.finalStatus = :status, im.updateTimestamp = :timestamp WHERE im.msgId = :msgId")
    int updateFinalStatus(@Param("msgId") BigDecimal msgId, @Param("status") String status, @Param("timestamp") LocalDateTime timestamp);

    boolean existsByAttributeKey4AndInsertTimestampAfter(String hashcode, LocalDateTime timestamp);
}
