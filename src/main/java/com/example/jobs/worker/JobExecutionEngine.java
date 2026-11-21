package com.example.jobs.worker;

import com.example.jobs.config.JobPipelineProperties;
import com.example.jobs.domain.JobEntity;
import com.example.jobs.exception.InvalidPayloadException;
import com.example.jobs.exception.JobAttemptTimeoutException;
import com.example.jobs.exception.SimulatedTransientFailureException;
import com.example.jobs.exception.TerminalJobFailureException;
import com.example.jobs.exception.UnsupportedJobTypeException;
import com.example.jobs.messaging.JobDispatchEnvelope;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Coordinates atomic claiming, bounded-timeout async handler execution, lease heartbeating,
 * and non-blocking outcome transitions.
 */
@Service
public class JobExecutionEngine {

    private static final Logger log = LoggerFactory.getLogger(JobExecutionEngine.class);
    private static final TypeReference<Map<String, String>> METADATA_TYPE = new TypeReference<>() {};

    private final WorkerStateTxService workerStateTxService;
    private final JobHandlerRegistry handlerRegistry;
    private final ExecutorService jobAttemptExecutor;
    private final ScheduledExecutorService leaseHeartbeatScheduler;
    private final JobPipelineProperties properties;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;

    public JobExecutionEngine(
            WorkerStateTxService workerStateTxService,
            JobHandlerRegistry handlerRegistry,
            ExecutorService jobAttemptExecutor,
            JobPipelineProperties properties,
            MeterRegistry meterRegistry,
            ObjectMapper objectMapper) {
        this.workerStateTxService = workerStateTxService;
        this.handlerRegistry = handlerRegistry;
        this.jobAttemptExecutor = jobAttemptExecutor;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.objectMapper = objectMapper;
        this.leaseHeartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "job-lease-heartbeat");
            t.setDaemon(true);
            return t;
        });
    }

    @PreDestroy
    public void shutdown() {
        leaseHeartbeatScheduler.shutdownNow();
    }

    public void processDispatch(JobDispatchEnvelope envelope, String sourceTopic) {
        Optional<ClaimedJobAttempt> claimedOpt = workerStateTxService.tryClaimAttempt(envelope);
        if (claimedOpt.isEmpty()) {
            return;
        }

        ClaimedJobAttempt claimed = claimedOpt.get();
        JobEntity job = claimed.job();
        long timeoutMs = job.getTimeoutMs();

        long heartbeatPeriodMs = properties.worker().heartbeatIntervalMs();
        ScheduledFuture<?> heartbeatTask = leaseHeartbeatScheduler.scheduleAtFixedRate(
                () -> safeRenewLease(job.getId(), claimed.leaseToken(), timeoutMs),
                heartbeatPeriodMs,
                heartbeatPeriodMs,
                TimeUnit.MILLISECONDS);

        Future<JsonNode> attemptFuture = null;
        AtomicBoolean abandoned = new AtomicBoolean(false);
        try {
            JobExecutionContext context = buildContext(job);
            attemptFuture = jobAttemptExecutor.submit(() -> {
                JsonNode result = executeAttempt(context);
                if (abandoned.get()) {
                    // The handler ignored interruption and finished its work after the retry was already scheduled.
                    meterRegistry.counter("jobs.execution.completed_after_timeout").increment();
                    log.warn("Handler for job {} attempt {} completed after its {}ms timeout; its side effects may "
                            + "duplicate the retry's", context.jobId(), context.attemptNumber(), timeoutMs);
                }
                return result;
            });
            JsonNode result = attemptFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
            workerStateTxService.completeSuccess(claimed, result);
        } catch (TimeoutException timeoutEx) {
            abandoned.set(true);
            if (attemptFuture != null) {
                attemptFuture.cancel(true);
            }
            workerStateTxService.handleAttemptFailure(
                    claimed,
                    new JobAttemptTimeoutException(timeoutMs),
                    true,
                    sourceTopic);
        } catch (ExecutionException execEx) {
            Throwable cause = execEx.getCause() != null ? execEx.getCause() : execEx;
            workerStateTxService.handleAttemptFailure(claimed, cause, false, sourceTopic);
        } catch (InterruptedException interruptedEx) {
            if (attemptFuture != null) {
                attemptFuture.cancel(true);
            }
            Thread.currentThread().interrupt();
            workerStateTxService.handleAttemptFailure(claimed, interruptedEx, false, sourceTopic);
        } catch (Throwable unexpected) {
            workerStateTxService.handleAttemptFailure(claimed, unexpected, false, sourceTopic);
        } finally {
            heartbeatTask.cancel(false);
        }
    }

    private JsonNode executeAttempt(JobExecutionContext context) throws Exception {
        applySimulationDirectives(context);

        // Submissions are validated, but a node missing a handler during a rolling deploy still fails safely.
        JobHandler handler = handlerRegistry.find(context.jobType())
                .orElseThrow(() -> new UnsupportedJobTypeException(context.jobType()));
        return handler.execute(context);
    }

    private void applySimulationDirectives(JobExecutionContext context) throws InterruptedException {
        JsonNode payload = context.payload();
        if (payload == null || !payload.isObject()) {
            return;
        }

        long simulateDelayMs = payload.path("simulateDelayMs").asLong(0L);
        if (simulateDelayMs > 0) {
            Thread.sleep(simulateDelayMs);
        }

        if (payload.path("simulateTerminalFailure").asBoolean(false)) {
            throw new TerminalJobFailureException(
                    "Simulated terminal domain failure on attempt " + context.attemptNumber());
        }

        if (payload.path("simulateNonRetryable").asBoolean(false)
                || payload.path("simulateInvalid").asBoolean(false)) {
            throw new InvalidPayloadException(
                    "Simulated non-retryable payload error on attempt " + context.attemptNumber());
        }

        if (payload.path("simulateAlwaysFail").asBoolean(false)) {
            throw new SimulatedTransientFailureException(
                    "Simulated persistent transient failure on attempt " + context.attemptNumber());
        }

        int simulateFailures = payload.path("simulateFailures").asInt(0);
        if (simulateFailures > 0 && context.attemptNumber() <= simulateFailures) {
            throw new SimulatedTransientFailureException(
                    "Simulated transient failure on attempt " + context.attemptNumber() + " of " + simulateFailures);
        }
    }

    private JobExecutionContext buildContext(JobEntity job) throws Exception {
        JsonNode payload = objectMapper.readTree(job.getPayloadJson());
        Map<String, String> metadata = job.getMetadataJson() != null
                ? objectMapper.readValue(job.getMetadataJson(), METADATA_TYPE)
                : Map.of();
        return new JobExecutionContext(
                job.getId(),
                job.getJobType(),
                job.getPriority(),
                job.getAttemptCount(),
                job.getMaxRetries(),
                job.getTimeoutMs(),
                properties.workerId(),
                payload,
                metadata,
                job.getSubmittedAt());
    }

    private void safeRenewLease(String jobId, String leaseToken, long timeoutMs) {
        try {
            workerStateTxService.renewLease(jobId, leaseToken, timeoutMs);
        } catch (Exception e) {
            log.debug("Lease renewal skipped for jobId={}: {}", jobId, e.getMessage());
        }
    }
}
