package com.example.jobs.exception;

public class InvalidPayloadException extends NonRetryableJobException {

    public InvalidPayloadException(String message) {
        super(message);
    }
}
