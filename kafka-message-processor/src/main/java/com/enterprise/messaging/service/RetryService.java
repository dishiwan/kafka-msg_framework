package com.enterprise.messaging.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class RetryService {

    @Value("#{'${application.processing.retry.non-retriable-errors}'.split(',')}")
    private List<String> nonRetriableErrors;

    public boolean isRetriable(String errorCode) {
        boolean retriable = !nonRetriableErrors.contains(errorCode);
        log.debug("Error code {} is {}", errorCode, retriable ? "retriable" : "non-retriable");
        return retriable;
    }
}
