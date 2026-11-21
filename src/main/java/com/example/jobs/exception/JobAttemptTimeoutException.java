package com.example.jobs.exception;

public class JobAttemptTimeoutException extends RuntimeException {

    public JobAttemptTimeoutException(long timeoutMs) {
        super("Job execution attempt timed out after " + timeoutMs + "ms");
    }
}
