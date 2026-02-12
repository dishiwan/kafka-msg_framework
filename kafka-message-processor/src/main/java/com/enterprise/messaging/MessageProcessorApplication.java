package com.enterprise.messaging;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.TimeZone;

/**
 * Main Spring Boot Application Class for High-Performance Kafka Message Processor
 * 
 * This application processes 10,000+ messages per second from Kafka topics,
 * persists them to Oracle Database, validates, transforms, and publishes to downstream systems.
 * 
 * Key Features:
 * - High-throughput Kafka consumption with batch processing
 * - Oracle Database persistence with HikariCP connection pooling
 * - Redis caching for performance optimization
 * - Sequence caching mechanism for high-volume inserts
 * - Autoloader for processing in-progress messages on restart
 * - Comprehensive retry mechanism with exponential backoff
 * - Duplicate detection using hashcode comparison
 * - Priority-based message processing
 * - Extensive logging with MDC correlation
 * - PII data masking
 * - RESTful endpoints for reprocessing failed messages
 * 
 * @author Enterprise Messaging Team
 * @version 1.0.0
 * @since 2024
 */
@Slf4j
@SpringBootApplication
@EnableCaching
@EnableAsync
@EnableScheduling
@EnableTransactionManagement
@EnableAspectJAutoProxy
public class MessageProcessorApplication {

    public static void main(String[] args) {
        try {
            log.info("=================================================================");
            log.info("Starting Kafka Message Processor Application");
            log.info("=================================================================");
            
            SpringApplication.run(MessageProcessorApplication.class, args);
            
            log.info("=================================================================");
            log.info("Application Started Successfully");
            log.info("=================================================================");
        } catch (Exception e) {
            log.error("Failed to start application", e);
            System.exit(1);
        }
    }

    /**
     * Post-construct initialization
     * Sets default timezone to UTC for consistent timestamp handling
     */
    @PostConstruct
    public void init() {
        // Set default timezone to UTC
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        log.info("Default timezone set to UTC");
        
        // Log JVM parameters
        logJvmParameters();
        
        log.info("Application initialization completed");
    }

    /**
     * Pre-destroy cleanup
     */
    @PreDestroy
    public void cleanup() {
        log.info("=================================================================");
        log.info("Shutting down Kafka Message Processor Application");
        log.info("=================================================================");
    }

    /**
     * Logs important JVM parameters for monitoring
     */
    private void logJvmParameters() {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory() / (1024 * 1024);
        long totalMemory = runtime.totalMemory() / (1024 * 1024);
        long freeMemory = runtime.freeMemory() / (1024 * 1024);
        int processors = runtime.availableProcessors();

        log.info("JVM Parameters:");
        log.info("  Max Memory: {} MB", maxMemory);
        log.info("  Total Memory: {} MB", totalMemory);
        log.info("  Free Memory: {} MB", freeMemory);
        log.info("  Available Processors: {}", processors);
    }
}
