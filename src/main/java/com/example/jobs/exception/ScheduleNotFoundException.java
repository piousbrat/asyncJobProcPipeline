package com.example.jobs.exception;

public class ScheduleNotFoundException extends RuntimeException {

    public ScheduleNotFoundException(String scheduleId) {
        super("Schedule not found: " + scheduleId);
    }
}
