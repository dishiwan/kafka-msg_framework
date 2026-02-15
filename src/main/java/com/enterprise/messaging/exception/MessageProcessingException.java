package com.enterprise.messaging.exception;

import lombok.Getter;

@Getter
public class MessageProcessingException extends RuntimeException {
    
    private final String errorCode;
    
    public MessageProcessingException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }
    
    public MessageProcessingException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }
}
