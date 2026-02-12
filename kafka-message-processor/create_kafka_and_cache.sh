#!/bin/bash

echo "Creating Kafka Configuration..."

cat > src/main/java/com/enterprise/messaging/config/KafkaConfig.java << 'EOF'
package com.enterprise.messaging.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafka
@Profile("!local")
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.listener.concurrency:10}")
    private int concurrency;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, 
                  "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        log.info("Kafka Consumer configured with bootstrap-servers: {}", bootstrapServers);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(concurrency);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setPollTimeout(3000);
        
        log.info("Kafka Listener Container Factory configured with concurrency: {}", concurrency);
        return factory;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        log.info("Kafka Producer Factory configured");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory());
        log.info("Kafka Template created successfully");
        return template;
    }
}
EOF

echo "Kafka Configuration created!"

echo "Creating Sequence Cache Service..."

cat > src/main/java/com/enterprise/messaging/cache/SequenceCacheService.java << 'EOF'
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
EOF

echo "Sequence Cache Service created!"

echo "Creating Thread Pool Configuration..."

cat > src/main/java/com/enterprise/messaging/config/AsyncConfig.java << 'EOF'
package com.enterprise.messaging.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
@Configuration
@EnableAsync
public class AsyncConfig {

    @Value("${application.processing.thread-pool.core-size:20}")
    private int corePoolSize;

    @Value("${application.processing.thread-pool.max-size:50}")
    private int maxPoolSize;

    @Value("${application.processing.thread-pool.queue-capacity:1000}")
    private int queueCapacity;

    @Bean(name = "messageProcessorExecutor")
    public Executor messageProcessorExecutor() {
        log.info("Configuring Message Processor Thread Pool: core={}, max={}, queue={}", 
                 corePoolSize, maxPoolSize, queueCapacity);

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix("msg-processor-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();

        return executor;
    }

    @Bean(name = "retryExecutor")
    public Executor retryExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("retry-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();

        log.info("Retry Executor configured");
        return executor;
    }
}
EOF

echo "Thread Pool Configuration created!"

