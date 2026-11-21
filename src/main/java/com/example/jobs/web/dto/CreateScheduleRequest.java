package com.example.jobs.web.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record CreateScheduleRequest(
        @NotBlank(message = "name is required")
        @Size(max = 100, message = "name must be at most 100 characters")
        String name,

        @NotBlank(message = "cron is required")
        @Size(max = 120, message = "cron must be at most 120 characters")
        String cron,

        String timeZone,

        @NotNull(message = "job is required")
        @Valid
        CreateJobRequest job
) {
}
