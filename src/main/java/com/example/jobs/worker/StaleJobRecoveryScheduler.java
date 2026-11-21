package com.example.jobs.worker;

import com.example.jobs.config.JobPipelineProperties;
import java.time.Instant;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically scans for crashed/stuck {@code RUNNING} jobs whose lease has expired,
 * safely locking them with {@code FOR UPDATE SKIP LOCKED} so multiple replicas cooperate cleanly.
 */
@Component
public class StaleJobRecoveryScheduler {

    private final WorkerStateTxService workerStateTxService;
    private final JobPipelineProperties properties;

    public StaleJobRecoveryScheduler(
            WorkerStateTxService workerStateTxService,
            JobPipelineProperties properties) {
        this.workerStateTxService = workerStateTxService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${app.jobs.worker.stale-recovery-interval-ms:2000}")
    public void recoverStaleJobs() {
        workerStateTxService.recoverExpiredRunningJobs(
                Instant.now(),
                properties.worker().staleRecoveryBatchSize());
    }
}
