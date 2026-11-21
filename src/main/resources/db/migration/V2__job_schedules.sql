CREATE TABLE job_schedules (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    cron VARCHAR(120) NOT NULL,
    time_zone VARCHAR(64) NOT NULL,
    job_template_json TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    next_run_at TIMESTAMP WITH TIME ZONE,
    last_run_at TIMESTAMP WITH TIME ZONE,
    last_job_id VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT uk_job_schedules_name UNIQUE (name)
);

CREATE INDEX idx_job_schedules_due ON job_schedules (enabled, next_run_at);
