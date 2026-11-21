package com.example.jobs.web.dto;

import java.time.Instant;

public record ScheduleResponse(
        String id,
        String name,
        String cron,
        String timeZone,
        boolean enabled,
        Instant nextRunAt,
        Instant lastRunAt,
        String lastJobId,
        CreateJobRequest job,
        Instant createdAt) {
}
