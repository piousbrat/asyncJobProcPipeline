CREATE TABLE jobs (
    id VARCHAR(64) PRIMARY KEY,
    idempotency_key VARCHAR(128),
    job_type VARCHAR(100) NOT NULL,
    priority VARCHAR(16) NOT NULL,
    status VARCHAR(32) NOT NULL,
    payload_json TEXT NOT NULL,
    metadata_json TEXT,
    result_json TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL,
    timeout_ms BIGINT NOT NULL,
    route_to_dlq BOOLEAN NOT NULL DEFAULT TRUE,
    last_error_type VARCHAR(255),
    last_error_message TEXT,
    submitted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    scheduled_for TIMESTAMP WITH TIME ZONE NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    lease_owner VARCHAR(128),
    lease_token VARCHAR(64),
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    replay_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT uk_jobs_idempotency_key UNIQUE (idempotency_key)
);

CREATE INDEX idx_jobs_status_lease ON jobs (status, lease_expires_at);
CREATE INDEX idx_jobs_submitted_at ON jobs (submitted_at);

CREATE TABLE job_attempts (
    id VARCHAR(64) PRIMARY KEY,
    job_id VARCHAR(64) NOT NULL,
    attempt_number INTEGER NOT NULL,
    replay_generation INTEGER NOT NULL DEFAULT 0,
    worker_id VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE,
    duration_ms BIGINT,
    error_type VARCHAR(255),
    error_message TEXT,
    retryable BOOLEAN,
    CONSTRAINT fk_job_attempts_job FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE
);

CREATE INDEX idx_job_attempts_job_id ON job_attempts (job_id, replay_generation, attempt_number);

CREATE TABLE outbox_events (
    id VARCHAR(64) PRIMARY KEY,
    job_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    topic VARCHAR(128) NOT NULL,
    partition_key VARCHAR(128) NOT NULL,
    priority_rank INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    headers_json TEXT,
    status VARCHAR(32) NOT NULL,
    available_at TIMESTAMP WITH TIME ZONE NOT NULL,
    locked_by VARCHAR(128),
    lock_expires_at TIMESTAMP WITH TIME ZONE,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    published_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_outbox_poll ON outbox_events (status, available_at, priority_rank);
CREATE INDEX idx_outbox_lock_expiry ON outbox_events (status, lock_expires_at);
CREATE INDEX idx_outbox_job_id ON outbox_events (job_id);

CREATE TABLE dead_letter_jobs (
    id VARCHAR(64) PRIMARY KEY,
    job_id VARCHAR(64) NOT NULL,
    outbox_event_id VARCHAR(64),
    job_type VARCHAR(100) NOT NULL,
    priority VARCHAR(16) NOT NULL,
    payload_json TEXT NOT NULL,
    attempt_count INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    failure_reason VARCHAR(64) NOT NULL,
    error_type VARCHAR(255),
    error_message TEXT,
    stack_trace TEXT,
    original_topic VARCHAR(128) NOT NULL,
    dead_lettered_at TIMESTAMP WITH TIME ZONE NOT NULL,
    replayed BOOLEAN NOT NULL DEFAULT FALSE,
    replayed_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT uk_dlq_outbox_event UNIQUE (outbox_event_id)
);

CREATE INDEX idx_dlq_job_id ON dead_letter_jobs (job_id);
CREATE INDEX idx_dlq_dead_lettered_at ON dead_letter_jobs (dead_lettered_at);
