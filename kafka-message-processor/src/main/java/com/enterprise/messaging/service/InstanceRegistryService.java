package com.enterprise.messaging.service;

import com.enterprise.messaging.model.InstanceRegistry;
import com.enterprise.messaging.repository.InstanceRegistryRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class InstanceRegistryService {

    private final InstanceRegistryRepository instanceRegistryRepository;

    @Value("${application.instance.heartbeat-interval-seconds:30}")
    private int heartbeatIntervalSeconds;

    @Value("${application.instance.heartbeat-timeout-minutes:5}")
    private int heartbeatTimeoutMinutes;

    private String currentInstanceId;

    @PostConstruct
    public void registerInstance() {
        try {
            currentInstanceId = generateInstanceId();
            String hostname = InetAddress.getLocalHost().getHostName();
            String ip = InetAddress.getLocalHost().getHostAddress();

            InstanceRegistry instance = InstanceRegistry.builder()
                    .instanceId(currentInstanceId)
                    .instanceHostname(hostname)
                    .instanceIp(ip)
                    .instanceStatus("ACTIVE")
                    .startedTimestamp(LocalDateTime.now())
                    .lastHeartbeat(LocalDateTime.now())
                    .build();

            instanceRegistryRepository.save(instance);
            
            log.info("Instance registered: id={}, hostname={}, ip={}", currentInstanceId, hostname, ip);
        } catch (Exception e) {
            log.error("Error registering instance", e);
            throw new RuntimeException("Failed to register instance", e);
        }
    }

    @PreDestroy
    public void deregisterInstance() {
        try {
            instanceRegistryRepository.markInstanceAsDead(currentInstanceId, LocalDateTime.now());
            log.info("Instance deregistered: id={}", currentInstanceId);
        } catch (Exception e) {
            log.error("Error deregistering instance", e);
        }
    }

    @Scheduled(fixedDelayString = "${application.instance.heartbeat-interval-seconds:30}000")
    @Transactional
    public void sendHeartbeat() {
        try {
            instanceRegistryRepository.updateHeartbeat(currentInstanceId, LocalDateTime.now());
            log.debug("Heartbeat sent: instanceId={}", currentInstanceId);
        } catch (Exception e) {
            log.error("Error sending heartbeat", e);
        }
    }

    @Scheduled(fixedDelay = 60000) // Every minute
    @Transactional
    public void cleanupDeadInstances() {
        try {
            LocalDateTime threshold = LocalDateTime.now().minusMinutes(heartbeatTimeoutMinutes);
            List<InstanceRegistry> deadInstances = instanceRegistryRepository.findDeadInstances(threshold);
            
            for (InstanceRegistry instance : deadInstances) {
                if (!"DEAD".equals(instance.getInstanceStatus())) {
                    instanceRegistryRepository.markInstanceAsDead(instance.getInstanceId(), LocalDateTime.now());
                    log.warn("Marked instance as dead: id={}, lastHeartbeat={}", 
                            instance.getInstanceId(), instance.getLastHeartbeat());
                }
            }
        } catch (Exception e) {
            log.error("Error cleaning up dead instances", e);
        }
    }

    public String getCurrentInstanceId() {
        return currentInstanceId;
    }

    public List<InstanceRegistry> getAliveInstances() {
        return instanceRegistryRepository.findByInstanceStatus("ACTIVE");
    }

    public List<InstanceRegistry> getDeadInstances() {
        LocalDateTime threshold = LocalDateTime.now().minusMinutes(heartbeatTimeoutMinutes);
        return instanceRegistryRepository.findDeadInstances(threshold);
    }

    private String generateInstanceId() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            String shortUuid = UUID.randomUUID().toString().substring(0, 8);
            return hostname + "-" + shortUuid;
        } catch (Exception e) {
            return "UNKNOWN-" + UUID.randomUUID().toString().substring(0, 8);
        }
    }
}
