package com.enterprise.messaging.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;

@DisplayName("Priority Calculator - Complete Coverage")
class PriorityCalculatorTest {
    private PriorityCalculator calculator;
    private Map<String, Integer> priorityMappings;

    @BeforeEach
    void setUp() {
        priorityMappings = new HashMap<>();
        priorityMappings.put("SOURCE_A|EVENT_1", 1);
        priorityMappings.put("SOURCE_A|EVENT_2", 2);
        priorityMappings.put("SOURCE_B|*", 3);
        priorityMappings.put("*|EVENT_3", 4);
        
        calculator = new PriorityCalculator(priorityMappings);
    }

    @Test
    void testExactMatch() {
        int priority = calculator.calculatePriority("SOURCE_A", "EVENT_1");
        assertThat(priority).isEqualTo(1);
    }

    @Test
    void testSourceWildcard() {
        int priority = calculator.calculatePriority("SOURCE_B", "ANYTHING");
        assertThat(priority).isEqualTo(3);
    }

    @Test
    void testEventWildcard() {
        int priority = calculator.calculatePriority("ANYTHING", "EVENT_3");
        assertThat(priority).isEqualTo(4);
    }

    @Test
    void testDefaultPriority() {
        int priority = calculator.calculatePriority("UNKNOWN", "UNKNOWN");
        assertThat(priority).isEqualTo(5);
    }

    @Test
    void testNullSource() {
        int priority = calculator.calculatePriority(null, "EVENT_1");
        assertThat(priority).isEqualTo(5);
    }

    @Test
    void testNullEventType() {
        int priority = calculator.calculatePriority("SOURCE_A", null);
        assertThat(priority).isEqualTo(5);
    }

    @Test
    void testEmptyMappings() {
        calculator = new PriorityCalculator(new HashMap<>());
        int priority = calculator.calculatePriority("SOURCE", "EVENT");
        assertThat(priority).isEqualTo(5);
    }

    @Test
    void testCaseSensitivity() {
        int priority1 = calculator.calculatePriority("source_a", "EVENT_1");
        int priority2 = calculator.calculatePriority("SOURCE_A", "event_1");
        assertThat(priority1).isEqualTo(5);
        assertThat(priority2).isEqualTo(5);
    }
}
