package com.example.jobs.exception;

/**
 * Represents a terminal domain/business failure that transitions the job directly to {@code FAILED}
 * without retrying or publishing to the operational dead-letter queue.
 */
public class TerminalJobFailureException extends RuntimeException {

    public TerminalJobFailureException(String message) {
        super(message);
    }

    public TerminalJobFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
