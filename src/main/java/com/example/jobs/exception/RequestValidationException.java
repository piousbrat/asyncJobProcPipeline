package com.example.jobs.exception;

import java.util.Map;

/** A semantically invalid request, reported like bean-validation failures: {@code 400} with per-field errors. */
public class RequestValidationException extends RuntimeException {

    private final Map<String, String> fieldErrors;

    public RequestValidationException(String field, String message) {
        super(message);
        this.fieldErrors = Map.of(field, message);
    }

    public Map<String, String> fieldErrors() {
        return fieldErrors;
    }
}
