package com.example.jobs.service;

import com.example.jobs.config.JobPipelineProperties;
import com.example.jobs.outbox.OutboxRelayScheduler;
import java.time.Instant;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class RecurringJobScheduler {

    private final ScheduleService scheduleService;
    private final OutboxRelayScheduler outboxRelayScheduler;
    private final JobPipelineProperties properties;

    public RecurringJobScheduler(
            ScheduleService scheduleService,
            OutboxRelayScheduler outboxRelayScheduler,
            JobPipelineProperties properties) {
        this.scheduleService = scheduleService;
        this.outboxRelayScheduler = outboxRelayScheduler;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${app.jobs.schedules.poll-interval-ms:1000}")
    public void fireDueSchedules() {
        if (scheduleService.fireDue(Instant.now(), properties.schedules().batchSize()) > 0) {
            outboxRelayScheduler.triggerImmediateFlush();
        }
    }
}
