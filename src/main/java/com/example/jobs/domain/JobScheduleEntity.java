package com.example.jobs.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
@Table(name = "job_schedules")
public class JobScheduleEntity {

    @Id
    @Column(name = "id", length = 64, nullable = false, updatable = false)
    private String id;

    @Column(name = "name", length = 100, nullable = false, unique = true)
    private String name;

    @Column(name = "cron", length = 120, nullable = false)
    private String cron;

    @Column(name = "time_zone", length = 64, nullable = false)
    private String timeZone;

    @Column(name = "job_template_json", columnDefinition = "TEXT", nullable = false)
    private String jobTemplateJson;

    @Column(name = "enabled", nullable = false)
    private boolean enabled;

    @Column(name = "next_run_at")
    private Instant nextRunAt;

    @Column(name = "last_run_at")
    private Instant lastRunAt;

    @Column(name = "last_job_id", length = 64)
    private String lastJobId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
