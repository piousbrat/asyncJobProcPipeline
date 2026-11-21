# Distributed Async Job Processing Pipeline

A production-grade, horizontally scalable asynchronous job processing pipeline built with **Java 17+**, **Spring Boot 3.4**, **Spring Kafka**, **Spring Data JPA**, **Flyway**, and **PostgreSQL** (with **H2 + EmbeddedKafka** for self-contained `mvn test` execution).

---

## Architecture Overview

```
                                   ┌───────────────────────────────────────────────┐
                                   │             PostgreSQL (Durable State)        │
  Client                           │  ┌─────────┐  ┌───────────────┐  ┌─────────┐  │
    │  POST /api/v1/jobs           │  │  jobs   │  │ outbox_events │  │job_att. │  │
    ▼                              │  └────▲────┘  └───────▲───────┘  └────▲────┘  │
┌───────────────────┐  Single DB   │       │               │               │       │
│ Stateless API     │─────────────▶│  1. Persist QUEUED + Outbox row       │       │
│ Node (Replica 1..N│◀── 202 Acc.  └───────┼───────────────┼───────────────┼───────┘
└───────────────────┘                      │               │               │
                                           │   2. Poll due outbox rows     │
                                           │   (FOR UPDATE SKIP LOCKED,    │
                                           │    ordered by priority & time)│
                                           │               ▼               │
                                           │    ┌────────────────────┐     │
                                           │    │ OutboxRelaySched.  │     │
                                           │    └──────────┬─────────┘     │
                                           │               │               │
                        ┌──────────────────┬───────────────┴──┬────────────┴──────┐
                        ▼                  ▼                  ▼                   │
                 ┌─────────────┐    ┌───────────────┐  ┌────────────┐             │
                 │  jobs.high  │    │  jobs.normal  │  │  jobs.low  │   Kafka     │
                 │ (6 parts,   │    │  (3 parts,    │  │ (2 parts,  │   Topics    │
                 │  conc = 4)  │    │   conc = 2)   │  │  conc = 1) │             │
                 └──────┬──────┘    └───────┬───────┘  └──────┬─────┘             │
                        └───────────────────┼─────────────────┘                   │
                                            ▼                                     │
                             ┌──────────────────────────────┐                     │
                             │ Stateless Worker (Replica 1..N)                    │
                             │  3. Atomic CAS claim + lease │─────────────────────┘
                             │  4. Bounded Future(timeoutMs)│
                             │  5. Lease heartbeat renewal  │
                             └──────┬───────────────┬───────┘
                                    │               │
              ┌─────────────────────┤               ├──────────────────────────────┐
              ▼                     ▼               ▼                              ▼
        [SUCCEEDED]        [Retryable &      [Non-DLQ Terminal]       [Retries Exhausted OR
      Persist JSON          attempt<=max]     (e.g. domain error       NonRetryableJobException]
       result_json         Compute jittered   / routeToDlq=false)      Mark FAILED + enqueue
                           backoff, set            │                   JOB_DLQ outbox event
                           QUEUED + nextRetryAt    ▼                              │
                           + new Outbox row     [FAILED]                          ▼
                           (NO thread sleep!)                          Publish to `jobs.dlq`
                                                                       -> Mark [DEAD_LETTERED]
                                                                       -> DeadLetterConsumer
                                                                          logs & stores DLQ row
```

### Key Distributed Guarantees

1. **Decoupled, Crash-Safe Submission (Transactional Outbox)**:
   - `POST /api/v1/jobs` validates input, writes the `jobs` row (`QUEUED`) and an `outbox_events` row (`PENDING`, `available_at = scheduledFor`) inside a **single database transaction**, and returns `202 Accepted` immediately without blocking on Kafka broker round-trips.
   - Optional `idempotencyKey` is enforced via a database `UNIQUE` constraint plus `REQUIRES_NEW` conflict recovery so concurrent duplicate submissions return `202 Accepted` with `"deduplicated": true` and never create duplicate jobs.

2. **Zero Double-Dispatch Across Replicas (`FOR UPDATE SKIP LOCKED`)**:
   - `OutboxRelayScheduler` claims due outbox batches (`available_at <= now`) using `SELECT ... FOR UPDATE SKIP LOCKED` ordered by `priority_rank ASC, available_at ASC`.
   - Immediate jobs, delayed/scheduled jobs (`scheduledAt` / `delayMs`), non-blocking retries (`nextRetryAt`), and DLQ publications all flow through this single unified crash-safe mechanism.

3. **Atomic Worker Claiming, Fencing Tokens & Stale-Running Recovery**:
   - Kafka provides at-least-once delivery. Workers claim attempts via an atomic conditional update (`WHERE id = :jobId AND status = 'QUEUED' AND attempt_count = :expectedPriorAttempt AND replay_count = :expectedReplayCount`), generating a unique `leaseToken` and `leaseExpiresAt`.
   - Active attempts periodically extend their lease via a background heartbeat. Completion and failure transitions verify `leaseToken` (plus JPA `@Version` optimistic locking) so a paused/zombie worker is fenced off.
   - `StaleJobRecoveryScheduler` uses `FOR UPDATE SKIP LOCKED` to reclaim `RUNNING` jobs whose worker crashed (`lease_expires_at <= now`), transitioning them into a scheduled retry or DLQ.

4. **Priority Queues Without Head-of-Line Blocking**:
   - Separate partitioned Kafka topics (`jobs.high`, `jobs.normal`, `jobs.low`) are paired with dedicated `ConcurrentKafkaListenerContainerFactory` instances (`concurrency`: `4` / `2` / `1`, tuned `max.poll.records`: `10` / `5` / `2`) and priority-ordered outbox polling so `HIGH` priority jobs bypass `NORMAL` and `LOW` backlogs.

5. **Strict Per-Attempt Timeout & Non-Blocking Jittered Exponential Retry**:
   - Every attempt runs on a bounded worker executor via `Future.get(timeoutMs, MILLISECONDS)`. On `TimeoutException`, `future.cancel(true)` interrupts the handler thread and treats the attempt as a retryable `JobAttemptTimeoutException`.
   - **Never sleeps on Kafka listener threads**: retryable failures compute `nextRetryAt` using capped exponential backoff with proportional random jitter:
     $$\text{cappedBase} = \min(\text{baseIntervalMs} \times \text{multiplier}^{\text{retry}-1}, \text{maxIntervalMs})$$
     $$\text{delay} \sim \text{Uniform}\big([\text{cappedBase} \times (1 - \text{jitterFactor}),\ \text{cappedBase}]\big)$$
     and persist a deferred `JOB_DISPATCH` outbox row with `available_at = nextRetryAt`.

---

## Job State Machine

`GET /api/v1/jobs/{jobId}` returns `state` as **exactly one of**:

| State | Meaning |
| --- | --- |
| `QUEUED` | Accepted and awaiting execution (initial immediate submission, future `scheduledAt`/`delayMs`, or awaiting `nextRetryAt` after a retryable failure). |
| `RUNNING` | Atomically claimed by a worker node with an active fencing `leaseToken`. |
| `SUCCEEDED` | Handler completed within `timeoutMs`; durable JSON `result` payload and `finishedAt` are populated. |
| `FAILED` | Terminal failure not published to the DLQ (e.g., `TerminalJobFailureException`, `routeToDlq=false`, or transient state immediately prior to DLQ outbox relay). |
| `DEAD_LETTERED` | Terminal failure (`maxRetries` exhausted or `NonRetryableJobException`) once published to the Kafka DLQ topic (`jobs.dlq`). |

### State Transitions

```
  POST /api/v1/jobs
         │
         ▼
     ┌────────┐   Worker CAS claim   ┌─────────┐   Handler returns JSON    ┌───────────┐
     │ QUEUED │ ───────────────────▶ │ RUNNING │ ────────────────────────▶ │ SUCCEEDED │
     └────────┘                      └─────────┘                           └───────────┘
         ▲                            │   │   │
         │   Retryable failure        │   │   │  Non-DLQ terminal failure
         │   (attempt <= maxRetries)  │   │   │  (TerminalJobFailureException
         └────────────────────────────┘   │   │   or routeToDlq=false)
          Compute jittered nextRetryAt    │   ▼
                                          │ ┌────────┐
          Exhausted maxRetries OR         └▶│ FAILED │
          NonRetryableJobException          └────┬───┘
          (with routeToDlq=true)                 │ Outbox publishes DeadLetterEnvelope
                                                 │ to Kafka `jobs.dlq`
                                                 ▼
                                         ┌───────────────┐
                                         │ DEAD_LETTERED │ ──(POST /api/v1/dlq/{id}/replay)──▶ QUEUED
                                         └───────────────┘
```

Note on `maxRetries`: `attemptCount` is 1-based. `maxRetries: N` allows **1 initial attempt + up to `N` retries** (`1 + N` total attempts). Setting `maxRetries: 0` executes once with zero retries.

---

## Idempotency & Duplicate Safety

| Layer | Mechanism |
| --- | --- |
| Submission | `idempotencyKey` has a DB `UNIQUE` constraint; repeats (even concurrent) return `202` with the original `jobId` and `"deduplicated": true`. |
| Dispatch | Kafka is at-least-once. A worker only runs a message if the conditional claim (`status = QUEUED AND attempt_count = expectedAttempt-1 AND replay_count = ...`) succeeds, so duplicate/stale messages are ignored. |
| Completion | Result/failure writes are fenced by `leaseToken`, so a zombie worker cannot overwrite a recovered attempt. |
| DLQ | Dead-letter records are unique per outbox event id; redelivery is a no-op. |

## Operational Controls (`/api/v1/ops`)

These endpoints are unauthenticated; expose them only on an internal network or behind your gateway.

| Endpoint | Behaviour |
| --- | --- |
| `GET /api/v1/ops/queue` | Backlog per priority: `ready` (QUEUED and due) vs `delayed` (scheduled or awaiting retry backoff), plus `running` and `outboxPending` (not yet published to Kafka). |
| `POST /api/v1/ops/jobs/{jobId}/cancel` | Cancels one `QUEUED` job (`404` unknown, `409` if not `QUEUED`). |
| `POST /api/v1/ops/queue/drain[?priority=LOW]` | Cancels every `QUEUED` job (optionally one priority) and returns `{"cancelled": n}`. |

A cancelled job ends as `FAILED` with `lastError.type = "JobCancelled"` (the state set is unchanged). Its pending outbox
dispatch is marked `CANCELLED`; if the Kafka message was already published, the worker's conditional claim skips it.
`RUNNING` attempts are never interrupted and finish normally.

```bash
curl -s localhost:8080/api/v1/ops/queue
curl -s -X POST localhost:8080/api/v1/ops/jobs/<jobId>/cancel
curl -s -X POST "localhost:8080/api/v1/ops/queue/drain?priority=LOW"
```

---

## Configuration (`application.yml`)

| Property | Default | Description |
| --- | --- | --- |
| `app.jobs.defaults.max-retries` | `3` | Default `maxRetries` when omitted on submission |
| `app.jobs.defaults.timeout-ms` | `10000` | Default per-attempt execution timeout (`ms`) |
| `app.jobs.topics.high` / `normal` / `low` / `dlq` | `jobs.high`, `jobs.normal`, `jobs.low`, `jobs.dlq` | Kafka topic names |
| `app.jobs.consumer.high-concurrency` / `normal-concurrency` / `low-concurrency` | `4` / `2` / `1` | Listener concurrency per priority tier |
| `app.jobs.retry.base-interval-ms` | `1000` | Initial backoff delay before retry #1 |
| `app.jobs.retry.multiplier` | `2.0` | Exponential growth factor per retry |
| `app.jobs.retry.max-interval-ms` | `60000` | Upper bound on retry delay |
| `app.jobs.retry.jitter-factor` | `0.3` | Random jitter factor in `[0.0, 1.0]` |
| `app.jobs.outbox.poll-interval-ms` | `200` | Outbox relay polling interval |
| `app.jobs.outbox.batch-size` | `50` | Outbox claim batch size per replica |
| `app.jobs.worker.lease-grace-ms` | `5000` | Extra lease buffer beyond `timeoutMs` |
| `app.jobs.worker.heartbeat-interval-ms` | `2000` | Active attempt lease extension cadence |

---

## Local Startup & Testing

### Self-Contained Test Suite (No Docker Required)

Uses **EmbeddedKafka** and **H2 (PostgreSQL compatibility mode)** with Flyway migrations:

```bash
mvn clean test
```

### Running Locally with PostgreSQL & Kafka (KRaft)

```bash
# 1. Start PostgreSQL 16 and Apache Kafka 3.9 (KRaft mode, no ZooKeeper)
docker compose up -d

# 2. Run node 1 on port 8080
mvn spring-boot:run

# 3. (Optional) Run a second stateless node on port 8081 to exercise distributed coordination
SERVER_PORT=8081 WORKER_ID=worker-node-2 mvn spring-boot:run
```

---

## Built-in Pluggable Job Handlers & Simulation Flags

| `jobType` | Expected `payload` | Sample `result` JSON |
| --- | --- | --- |
| `echo` | `{"message": "hello"}` | `{"echo":"hello","attempt":1,"priority":"NORMAL","processedBy":"..."}` |
| `sum` | `{"numbers": [10, 20, 12.5]}` | `{"sum":42.5,"count":3,"min":10.0,"max":20.0,"average":14.166...}` |
| `report.generate` | `{"reportName": "q4-revenue", "rows": 500}` | `{"reportName":"q4-revenue","rowsProcessed":500,"checksum":"...","attempt":1}` |
| `simulate` | `{"resultValue": {"ok": true}}` | `{"simulated":true,"attempt":1,"priority":"NORMAL","resultValue":{"ok":true}}` |

Any job payload can also include optional simulation directives to exercise failure & timeout paths:
- `"simulateDelayMs": 3000` — sleeps `3000ms` (interruptible by `timeoutMs`)
- `"simulateFailures": 2` — throws retryable `SimulatedTransientFailureException` on attempts `1..2`, then succeeds on attempt `3`
- `"simulateAlwaysFail": true` — throws retryable `SimulatedTransientFailureException` on every attempt until `maxRetries` exhausts -> `DEAD_LETTERED`
- `"simulateNonRetryable": true` — throws `InvalidPayloadException` (`NonRetryableJobException`), bypassing retries -> `DEAD_LETTERED`
- `"simulateTerminalFailure": true` — throws `TerminalJobFailureException`, transitioning directly to `FAILED` without DLQ publication

---

## API Walkthrough (`curl` Examples)

### 1. Immediate Job Submission (`202 Accepted` + Result Retrieval)

```bash
curl -i -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobType": "sum",
    "priority": "NORMAL",
    "payload": { "numbers": [10, 20, 30, 40] },
    "idempotencyKey": "sum-req-001",
    "metadata": { "requestedBy": "billing-service" }
  }'

# Check status, timestamps, attempt history, and persisted JSON result:
curl -s http://localhost:8080/api/v1/jobs/<jobId>
```

### 2. High-Priority Job (`jobs.high`)

```bash
curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobType": "echo",
    "priority": "HIGH",
    "payload": { "message": "urgent-alert" }
  }'
```

### 3. Deferred / Scheduled Job (`delayMs` or ISO-8601 `scheduledAt`)

```bash
# Relative delay (5 seconds)
curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobType": "report.generate",
    "delayMs": 5000,
    "payload": { "reportName": "daily-settlement", "rows": 1200 }
  }'

# Explicit future timestamp
curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobType": "echo",
    "scheduledAt": "2026-12-31T23:59:00Z",
    "payload": { "message": "year-end-close" }
  }'
```

### 4. Transient Failures -> Non-Blocking Jittered Retry -> `SUCCEEDED`

```bash
curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobType": "echo",
    "maxRetries": 3,
    "payload": {
      "message": "eventual-success",
      "simulateFailures": 2
    }
  }'
```

### 5. Per-Attempt Timeout Enforcement -> Retry -> Exhaustion -> `DEAD_LETTERED`

```bash
curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobType": "simulate",
    "timeoutMs": 200,
    "maxRetries": 1,
    "payload": { "simulateDelayMs": 5000 }
  }'
```

### 6. Non-Retryable Failure -> Immediate DLQ (`DEAD_LETTERED`)

```bash
curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobType": "sum",
    "maxRetries": 5,
    "payload": { "numbers": ["not-a-number"] }
  }'
```

### 7. Non-DLQ Terminal Failure (`FAILED`)

```bash
curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "jobType": "simulate",
    "payload": { "simulateTerminalFailure": true }
  }'
```

### 8. Inspect & Replay Dead-Lettered Jobs

```bash
# List all DLQ records
curl -s http://localhost:8080/api/v1/dlq

# Inspect DLQ diagnostics for a specific job
curl -s http://localhost:8080/api/v1/dlq/<jobId>

# Replay a DEAD_LETTERED job back to QUEUED
curl -i -X POST http://localhost:8080/api/v1/dlq/<jobId>/replay
```
