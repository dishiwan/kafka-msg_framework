package com.enterprise.messaging.service;

import lombok.Data;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class CompositeSequenceService {

    private final JdbcTemplate jdbcTemplate;
    
    @Value("${business.key.prefix:ABC}")
    private String keyPrefix;
    
    @Value("${business.key.sequence-length:6}")
    private int sequenceLength;
    
    @Value("${business.key.separator:-}")
    private String separator;
    
    @Value("${business.key.reset-strategy:DAILY}")
    private String resetStrategy;
    
    @Value("${business.key.enabled:false}")
    private boolean enabled;
    
    private final ConcurrentHashMap<LocalDate, String> sequenceCache = new ConcurrentHashMap<>();
    
    public CompositeSequenceService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public String generateBusinessKey() {
        if (!enabled) {
            return null;
        }
        
        LocalDate today = LocalDate.now();
        long sequenceNumber = getNextSequenceNumber(today);
        return formatBusinessKey(keyPrefix, today, sequenceNumber);
    }
    
    private long getNextSequenceNumber(LocalDate date) {
        String sequenceName = getSequenceName(date);
        ensureSequenceExists(sequenceName);
        
        String sql = "SELECT " + sequenceName + ".NEXTVAL FROM DUAL";
        return jdbcTemplate.queryForObject(sql, Long.class);
    }
    
    private String getSequenceName(LocalDate date) {
        if ("DAILY".equals(resetStrategy)) {
            return "BUSINESS_KEY_SEQ_" + date.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        } else {
            return "BUSINESS_KEY_CONTINUOUS_SEQ";
        }
    }
    
    @Cacheable(value = "sequences", key = "#sequenceName")
    private void ensureSequenceExists(String sequenceName) {
        if ("CONTINUOUS".equals(resetStrategy)) {
            return;
        }
        
        String checkSql = "SELECT COUNT(*) FROM USER_SEQUENCES WHERE SEQUENCE_NAME = ?";
        Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, sequenceName);
        
        if (count == 0) {
            String createSql = "CREATE SEQUENCE " + sequenceName + 
                             " START WITH 1 INCREMENT BY 1 MAXVALUE 999999 NOCYCLE CACHE 100";
            
            try {
                jdbcTemplate.execute(createSql);
                log.info("Created daily sequence: {}", sequenceName);
            } catch (Exception e) {
                log.warn("Sequence {} may already exist", sequenceName);
            }
        }
    }
    
    private String formatBusinessKey(String prefix, LocalDate date, long sequence) {
        String formattedSequence = String.format("%0" + sequenceLength + "d", sequence);
        String formattedDate = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        return prefix + separator + formattedDate + separator + formattedSequence;
    }
    
    public BusinessKeyComponents parseBusinessKey(String businessKey) {
        if (businessKey == null || businessKey.isEmpty()) {
            return null;
        }
        
        String[] parts = businessKey.split(separator);
        return BusinessKeyComponents.builder()
                .prefix(parts[0])
                .date(LocalDate.parse(parts[1], DateTimeFormatter.ofPattern("yyyyMMdd")))
                .sequence(Long.parseLong(parts[2]))
                .build();
    }
    
    @Builder
    @Data
    public static class BusinessKeyComponents {
        private String prefix;
        private LocalDate date;
        private long sequence;
    }
}
