package com.example.jobs.exception;

public class SimulatedTransientFailureException extends RuntimeException {

    public SimulatedTransientFailureException(String message) {
        super(message);
    }
}
