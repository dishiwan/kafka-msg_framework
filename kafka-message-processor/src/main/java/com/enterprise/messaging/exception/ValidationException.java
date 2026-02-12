package com.enterprise.messaging.exception;

public class ValidationException extends MessageProcessingException {
    
    public ValidationException(String errorCode, String message) {
        super(errorCode, message);
    }
    
    public ValidationException(String errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
