package com.enterprise.messaging.cache;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sequence Cache Service for high-performance sequence generation
 * Caches database sequences in memory to reduce database round trips
 * Supports automatic replenishment and date-based cache refresh
 */
@Slf4j
@Service
public class SequenceCacheService {

    private final JdbcTemplate jdbcTemplate;

    @Value("${application.sequence.names}")
    private String sequenceNamesConfig;

    @Value("${application.sequence.max-size:10000}")
    private int maxSequenceSize;

    @Value("${application.sequence.threshold:2000}")
    private int sequenceThreshold;

    @Value("${application.sequence.refresh-date-based-cache:true}")
    private boolean refreshDateBasedCache;

    private final Map<String, Queue<BigDecimal>> sequenceCache = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> sequenceLocks = new ConcurrentHashMap<>();
    private LocalDate lastCacheDate;

    public SequenceCacheService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void initializeCache() {
        log.info("Initializing Sequence Cache Service");
        log.info("Sequence Names: {}", sequenceNamesConfig);
        log.info("Max Sequence Size: {}, Threshold: {}", maxSequenceSize, sequenceThreshold);
        log.info("Refresh Date Based Cache: {}", refreshDateBasedCache);

        String[] sequenceNames = sequenceNamesConfig.split(",");
        for (String sequenceName : sequenceNames) {
            String trimmedName = sequenceName.trim();
            sequenceCache.put(trimmedName, new LinkedList<>());
            sequenceLocks.put(trimmedName, new ReentrantLock());
            loadSequenceCache(trimmedName);
        }

        lastCacheDate = LocalDate.now();
        log.info("Sequence Cache initialized successfully with {} sequences", sequenceNames.length);
    }

    /**
     * Get next sequence value for the given sequence name
     */
    public BigDecimal getNextSequence(String sequenceName) {
        checkAndRefreshCacheOnDateChange();

        Queue<BigDecimal> cache = sequenceCache.get(sequenceName);
        if (cache == null) {
            log.error("Sequence {} not found in cache", sequenceName);
            throw new IllegalArgumentException("Unknown sequence: " + sequenceName);
        }

        ReentrantLock lock = sequenceLocks.get(sequenceName);
        lock.lock();
        try {
            // Check if cache needs replenishment
            if (cache.size() < sequenceThreshold) {
                log.info("Sequence cache {} below threshold ({} < {}), replenishing...", 
                         sequenceName, cache.size(), sequenceThreshold);
                loadSequenceCache(sequenceName);
            }

            BigDecimal nextValue = cache.poll();
            if (nextValue == null) {
                // Emergency fallback - load immediately
                log.warn("Sequence cache {} exhausted, emergency load", sequenceName);
                loadSequenceCache(sequenceName);
                nextValue = cache.poll();
            }

            return nextValue;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Load sequence values into cache from database
     */
    private void loadSequenceCache(String sequenceName) {
        log.debug("Loading {} sequence values for {}", maxSequenceSize, sequenceName);

        List<BigDecimal> sequences = new ArrayList<>();
        String sql = "SELECT " + sequenceName + ".NEXTVAL FROM DUAL";

        try {
            for (int i = 0; i < maxSequenceSize; i++) {
                BigDecimal value = jdbcTemplate.queryForObject(sql, BigDecimal.class);
                sequences.add(value);
            }

            Queue<BigDecimal> cache = sequenceCache.get(sequenceName);
            cache.addAll(sequences);

            log.info("Loaded {} sequences for {}. Cache size: {}", 
                     sequences.size(), sequenceName, cache.size());
        } catch (Exception e) {
            log.error("Error loading sequence cache for {}", sequenceName, e);
            throw new RuntimeException("Failed to load sequence cache", e);
        }
    }

    /**
     * Check if date has changed and refresh cache if configured
     */
    private void checkAndRefreshCacheOnDateChange() {
        if (!refreshDateBasedCache) {
            return;
        }

        LocalDate currentDate = LocalDate.now();
        if (!currentDate.equals(lastCacheDate)) {
            log.info("Date changed from {} to {}, refreshing sequence caches", lastCacheDate, currentDate);
            refreshAllCaches();
            lastCacheDate = currentDate;
        }
    }

    /**
     * Refresh all sequence caches
     */
    private void refreshAllCaches() {
        for (String sequenceName : sequenceCache.keySet()) {
            ReentrantLock lock = sequenceLocks.get(sequenceName);
            lock.lock();
            try {
                Queue<BigDecimal> cache = sequenceCache.get(sequenceName);
                cache.clear();
                loadSequenceCache(sequenceName);
                log.info("Refreshed cache for sequence: {}", sequenceName);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Get current cache size for a sequence (for monitoring)
     */
    public int getCacheSize(String sequenceName) {
        Queue<BigDecimal> cache = sequenceCache.get(sequenceName);
        return cache != null ? cache.size() : 0;
    }

    /**
     * Get hexadecimal representation of sequence (for date-based identifiers)
     */
    public String getHexSequence(String sequenceName) {
        BigDecimal sequence = getNextSequence(sequenceName);
        return String.format("%X", sequence.longValue());
    }
}
