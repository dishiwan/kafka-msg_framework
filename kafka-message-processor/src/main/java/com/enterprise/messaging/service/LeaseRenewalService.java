package com.enterprise.messaging.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class LeaseRenewalService {

    private final JdbcTemplate jdbcTemplate;
    private final InstanceRegistryService instanceRegistryService;

    @Value("${application.lease.duration-minutes:15}")
    private int leaseDurationMinutes;

    @Async("messageProcessorExecutor")
    public void renewLeaseAsync(BigDecimal msgId) {
        try {
            renewLease(msgId);
        } catch (Exception e) {
            log.error("Error renewing lease asynchronously for msgId={}", msgId, e);
        }
    }

    public void renewLease(BigDecimal msgId) {
        try {
            LocalDateTime newExpiry = LocalDateTime.now().plusMinutes(leaseDurationMinutes);
            String instanceId = instanceRegistryService.getCurrentInstanceId();
            
            String sql = "UPDATE INCOMING_MESSAGE_TABLE SET " +
                        "LEASE_EXPIRY_TIMESTAMP = ?, " +
                        "LAST_LEASE_RENEWAL = SYSTIMESTAMP, " +
                        "PROCESSING_INSTANCE_ID = ? " +
                        "WHERE MSG_ID = ?";
            
            int updated = jdbcTemplate.update(sql, newExpiry, instanceId, msgId);
            
            if (updated > 0) {
                log.debug("Lease renewed for msgId={}, newExpiry={}", msgId, newExpiry);
            }
        } catch (Exception e) {
            log.error("Error renewing lease for msgId={}", msgId, e);
        }
    }

    public void acquireLease(BigDecimal msgId) {
        try {
            LocalDateTime expiry = LocalDateTime.now().plusMinutes(leaseDurationMinutes);
            LocalDateTime startedAt = LocalDateTime.now();
            String instanceId = instanceRegistryService.getCurrentInstanceId();
            
            String sql = "UPDATE INCOMING_MESSAGE_TABLE SET " +
                        "PROCESSING_INSTANCE_ID = ?, " +
                        "PROCESSING_STARTED_TIMESTAMP = ?, " +
                        "LEASE_EXPIRY_TIMESTAMP = ?, " +
                        "LAST_LEASE_RENEWAL = SYSTIMESTAMP " +
                        "WHERE MSG_ID = ?";
            
            jdbcTemplate.update(sql, instanceId, startedAt, expiry, msgId);
            
            log.debug("Lease acquired for msgId={}, expiry={}, instance={}", msgId, expiry, instanceId);
        } catch (Exception e) {
            log.error("Error acquiring lease for msgId={}", msgId, e);
        }
    }

    public void releaseLease(BigDecimal msgId) {
        try {
            String sql = "UPDATE INCOMING_MESSAGE_TABLE SET " +
                        "PROCESSING_INSTANCE_ID = NULL, " +
                        "LEASE_EXPIRY_TIMESTAMP = NULL " +
                        "WHERE MSG_ID = ?";
            
            jdbcTemplate.update(sql, msgId);
            
            log.debug("Lease released for msgId={}", msgId);
        } catch (Exception e) {
            log.error("Error releasing lease for msgId={}", msgId, e);
        }
    }
}
