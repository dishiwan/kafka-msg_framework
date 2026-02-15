package com.enterprise.messaging.util;

import org.slf4j.MDC;

public class MDCUtil {
    
    private static final String CORRELATION_ID = "correlationId";
    private static final String MSG_ID = "msgId";
    private static final String SOURCE = "source";
    private static final String EVENT_TYPE = "eventType";

    public static void putCorrelationId(String correlationId) {
        if (correlationId != null) {
            MDC.put(CORRELATION_ID, correlationId);
        }
    }

    public static void put(String key, String value) {
        if (key != null && value != null) {
            MDC.put(key, value);
        }
    }

    public static String get(String key) {
        return MDC.get(key);
    }

    public static void clear() {
        MDC.clear();
    }
}
