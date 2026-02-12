package com.enterprise.messaging.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class PriorityCalculator {

    @Value("${application.priority.default:5}")
    private int defaultPriority;

    @Value("#{'${application.priority.mappings}'.split(',')}")
    private List<String> priorityMappings;

    private Map<String, Integer> priorityMap = new HashMap<>();

    @PostConstruct
    public void init() {
        for (String mapping : priorityMappings) {
            String[] parts = mapping.split("\|");
            if (parts.length == 3) {
                String key = parts[0].trim() + "|" + parts[1].trim();
                int priority = Integer.parseInt(parts[2].trim());
                priorityMap.put(key, priority);
            }
        }
        log.info("Priority mappings initialized: {} mappings", priorityMap.size());
    }

    public Integer calculatePriority(String source, String eventType) {
        String key1 = source + "|" + eventType;
        String key2 = source + "|*";
        String key3 = "*|" + eventType;

        if (priorityMap.containsKey(key1)) {
            return priorityMap.get(key1);
        } else if (priorityMap.containsKey(key2)) {
            return priorityMap.get(key2);
        } else if (priorityMap.containsKey(key3)) {
            return priorityMap.get(key3);
        }
        
        return defaultPriority;
    }
}
