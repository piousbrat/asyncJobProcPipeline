package com.example.jobs.domain;

public enum OutboxStatus {
    PENDING,
    PROCESSING,
    PUBLISHED,
    CANCELLED
}
