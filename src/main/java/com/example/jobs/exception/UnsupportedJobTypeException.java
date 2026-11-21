package com.example.jobs.exception;

public class UnsupportedJobTypeException extends NonRetryableJobException {

    public UnsupportedJobTypeException(String jobType) {
        super("Unsupported jobType: " + jobType);
    }
}
