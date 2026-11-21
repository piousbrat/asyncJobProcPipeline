package com.example.jobs.exception;

import java.util.Map;
import java.util.stream.Collectors;

public class InvalidPayloadException extends NonRetryableJobException {

    public InvalidPayloadException(String message) {
        super(message);
    }

    public static void throwIfAny(Map<String, String> violations) {
        if (!violations.isEmpty()) {
            throw new InvalidPayloadException(violations.entrySet().stream()
                    .map(e -> "payload." + e.getKey() + ": " + e.getValue())
                    .collect(Collectors.joining("; ")));
        }
    }
}
