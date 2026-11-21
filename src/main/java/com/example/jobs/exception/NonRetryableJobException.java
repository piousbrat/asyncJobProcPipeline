package com.example.jobs.exception;

/**
 * Base exception for unrecoverable errors that must bypass retry attempts and route immediately to the DLQ.
 */
public abstract class NonRetryableJobException extends RuntimeException {

    protected NonRetryableJobException(String message) {
        super(message);
    }

    protected NonRetryableJobException(String message, Throwable cause) {
        super(message, cause);
    }
}
