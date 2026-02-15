package com.enterprise.messaging.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IncomingTask implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private BigDecimal msgId;
    private String source;
    private String xCorrelationId;
    private BigDecimal internalSourceId;
    private String eventType;
    private String processType;
    private String originalMessage;
    private String transformedMessage;
    private Integer priority;
    
    @Builder.Default
    private Map<String, Object> contextData = new HashMap<>();
    
    @Builder.Default
    private Map<String, String> enrichmentData = new HashMap<>();
    
    @Builder.Default
    private Map<String, Object> validationResult = new HashMap<>();
    
    @Builder.Default
    private Map<String, Object> processingMetadata = new HashMap<>();

    public void addContextData(String key, Object value) {
        this.contextData.put(key, value);
    }

    public Object getContextData(String key) {
        return this.contextData.get(key);
    }

    public void addEnrichmentData(String key, String value) {
        this.enrichmentData.put(key, value);
    }

    public String getEnrichmentData(String key) {
        return this.enrichmentData.get(key);
    }

    public void addValidationResult(String key, Object value) {
        this.validationResult.put(key, value);
    }

    public void addProcessingMetadata(String key, Object value) {
        this.processingMetadata.put(key, value);
    }
}
