package com.example.jobs.web.dto;

import com.example.jobs.domain.JobPriority;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;
import java.time.Instant;
import java.util.Map;

public record CreateJobRequest(
        @NotBlank(message = "jobType is required")
        @Size(max = 100, message = "jobType must be at most 100 characters")
        String jobType,

        @NotNull(message = "payload is required")
        JsonNode payload,

        Instant scheduledAt,

        @PositiveOrZero(message = "delayMs must be >= 0")
        Long delayMs,

        JobPriority priority,

        @Min(value = 0, message = "maxRetries must be >= 0")
        @Max(value = 100, message = "maxRetries must be <= 100")
        Integer maxRetries,

        @Min(value = 10, message = "timeoutMs must be >= 10")
        @Max(value = 3_600_000, message = "timeoutMs must be <= 3600000")
        Long timeoutMs,

        @Size(max = 128, message = "idempotencyKey must be at most 128 characters")
        String idempotencyKey,

        Map<String, String> metadata,

        Boolean routeToDlq
) {
}
