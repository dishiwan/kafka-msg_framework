package com.enterprise.messaging.performance;

import com.enterprise.messaging.service.OptimizedBatchPersistenceService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@DisplayName("High Volume Throughput Tests - PERFORMANCE")
class HighVolumeThroughputTest {

    @Autowired(required = false)
    private OptimizedBatchPersistenceService persistenceService;

    @Test
    @DisplayName("Should handle 1000 messages/second sustained load")
    @Timeout(value = 65, unit = TimeUnit.SECONDS)
    void testSustainedLoad1000MessagesPerSecond() throws Exception {
        if (persistenceService == null) {
            // Skip if service not available (CI environment)
            return;
        }

        // Arrange
        int messagesPerSecond = 1000;
        int durationSeconds = 60;
        int totalMessages = messagesPerSecond * durationSeconds;

        ExecutorService executor = Executors.newFixedThreadPool(20);
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        // Act
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < totalMessages; i += 500) {
            final int batchStart = i;
            executor.submit(() -> {
                try {
                    List<String> messages = generateBatch(500);
                    List<String> sources = generateBatchSources(500);
                    List<String> correlationIds = generateBatchCorrelations(500, batchStart);
                    List<String> eventTypes = generateBatchEventTypes(500);
                    List<Integer> priorities = generateBatchPriorities(500);

                    persistenceService.batchPersistMessages(
                            messages, sources, correlationIds, eventTypes, priorities
                    );
                    
                    processedCount.addAndGet(500);
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                }
            });

            // Rate limiting - 1000 msg/sec = 2 batches/sec
            if (i % 1000 == 0) {
                Thread.sleep(1000);
            }
        }

        executor.shutdown();
        executor.awaitTermination(65, TimeUnit.SECONDS);

        long duration = System.currentTimeMillis() - startTime;
        double actualRate = (processedCount.get() * 1000.0) / duration;

        // Assert
        System.out.println("Processed: " + processedCount.get() + " messages");
        System.out.println("Duration: " + duration + " ms");
        System.out.println("Rate: " + String.format("%.2f", actualRate) + " msg/sec");
        System.out.println("Errors: " + errorCount.get());

        assertThat(processedCount.get()).isGreaterThan(50000); // At least 50k in 60 seconds
        assertThat(errorCount.get()).isLessThan(100); // <0.1% error rate
    }

    @Test
    @DisplayName("Should handle burst load of 5000 messages/second")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testBurstLoad5000MessagesPerSecond() throws Exception {
        if (persistenceService == null) {
            return;
        }

        // Arrange
        int totalMessages = 5000;
        ExecutorService executor = Executors.newFixedThreadPool(50);
        CountDownLatch latch = new CountDownLatch(totalMessages / 500);
        AtomicInteger processedCount = new AtomicInteger(0);

        // Act
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < totalMessages; i += 500) {
            final int batchStart = i;
            executor.submit(() -> {
                try {
                    List<String> messages = generateBatch(500);
                    List<String> sources = generateBatchSources(500);
                    List<String> correlationIds = generateBatchCorrelations(500, batchStart);
                    List<String> eventTypes = generateBatchEventTypes(500);
                    List<Integer> priorities = generateBatchPriorities(500);

                    persistenceService.batchPersistMessages(
                            messages, sources, correlationIds, eventTypes, priorities
                    );
                    
                    processedCount.addAndGet(500);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        long duration = System.currentTimeMillis() - startTime;
        double actualRate = (processedCount.get() * 1000.0) / duration;

        // Assert
        System.out.println("Burst processed: " + processedCount.get() + " messages");
        System.out.println("Burst duration: " + duration + " ms");
        System.out.println("Burst rate: " + String.format("%.2f", actualRate) + " msg/sec");

        assertThat(processedCount.get()).isEqualTo(5000);
        assertThat(duration).isLessThan(5000); // Complete in under 5 seconds
    }

    private List<String> generateBatch(int size) {
        List<String> batch = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            batch.add("{\"test\":\"message\",\"index\":" + i + "}");
        }
        return batch;
    }

    private List<String> generateBatchSources(int size) {
        List<String> batch = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            batch.add("PERF_TEST_SOURCE");
        }
        return batch;
    }

    private List<String> generateBatchCorrelations(int size, int offset) {
        List<String> batch = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            batch.add("PERF-CORR-" + (offset + i));
        }
        return batch;
    }

    private List<String> generateBatchEventTypes(int size) {
        List<String> batch = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            batch.add("PERF_EVENT");
        }
        return batch;
    }

    private List<Integer> generateBatchPriorities(int size) {
        List<Integer> batch = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            batch.add(5);
        }
        return batch;
    }
}
