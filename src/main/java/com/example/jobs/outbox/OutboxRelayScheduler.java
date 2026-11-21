package com.example.jobs.outbox;

import com.example.jobs.config.JobPipelineProperties;
import com.example.jobs.domain.OutboxEventEntity;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Distributed Transactional Outbox relay using {@code FOR UPDATE SKIP LOCKED} batch claiming.
 * Multiple stateless replicas can run this scheduler simultaneously without double-publishing.
 */
@Component
public class OutboxRelayScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);
    private static final TypeReference<Map<String, String>> HEADERS_TYPE = new TypeReference<>() {};

    private final OutboxTxService outboxTxService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JobPipelineProperties properties;
    private final ObjectMapper objectMapper;
    private final ExecutorService flushExecutor;
    private final AtomicBoolean flushQueued = new AtomicBoolean(false);

    public OutboxRelayScheduler(
            OutboxTxService outboxTxService,
            KafkaTemplate<String, String> kafkaTemplate,
            JobPipelineProperties properties,
            ObjectMapper objectMapper) {
        this.outboxTxService = outboxTxService;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "outbox-async-flush");
            t.setDaemon(true);
            return t;
        });
    }

    @PreDestroy
    public void shutdown() {
        flushExecutor.shutdownNow();
    }

    public void triggerImmediateFlush() {
        if (!properties.outbox().enabled()) {
            return;
        }
        if (flushQueued.compareAndSet(false, true)) {
            flushExecutor.submit(() -> {
                try {
                    relayDueEvents();
                } finally {
                    flushQueued.set(false);
                }
            });
        }
    }

    @Scheduled(fixedDelayString = "${app.jobs.outbox.poll-interval-ms:200}")
    public void scheduledRelay() {
        if (!properties.outbox().enabled()) {
            return;
        }
        relayDueEvents();
    }

    public int relayDueEvents() {
        int totalPublished = 0;
        int batchSize = properties.outbox().batchSize();

        while (true) {
            List<OutboxEventEntity> batch = outboxTxService.claimDueBatch(Instant.now(), batchSize);
            if (batch.isEmpty()) {
                break;
            }
            for (OutboxEventEntity event : batch) {
                if (publishSingleEvent(event)) {
                    totalPublished++;
                }
            }
            if (batch.size() < batchSize) {
                break;
            }
        }
        return totalPublished;
    }

    private boolean publishSingleEvent(OutboxEventEntity event) {
        try {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(event.getTopic(), event.getPartitionKey(), event.getPayloadJson());
            addHeader(record, "x-outbox-event-id", event.getId());
            addHeader(record, "x-event-type", event.getEventType().name());
            addHeader(record, "x-job-id", event.getJobId());

            if (event.getHeadersJson() != null) {
                Map<String, String> customHeaders = objectMapper.readValue(event.getHeadersJson(), HEADERS_TYPE);
                customHeaders.forEach((k, v) -> addHeader(record, k, v));
            }

            kafkaTemplate.send(record)
                    .get(properties.outbox().sendTimeoutMs(), TimeUnit.MILLISECONDS);

            outboxTxService.markPublished(event.getId(), event.getJobId(), event.getEventType(), Instant.now());
            return true;
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            long backoffMs = Math.min(5_000L, 100L * (1L << Math.min(6, event.getPublishAttempts())));
            Instant retryAt = Instant.now().plusMillis(backoffMs);
            log.warn("Failed to publish outbox event {} to topic {} (attempt {}): {}",
                    event.getId(), event.getTopic(), event.getPublishAttempts(), ex.getMessage());
            outboxTxService.releaseOnFailure(event.getId(), ex.getMessage(), retryAt);
            return false;
        }
    }

    private void addHeader(ProducerRecord<String, String> record, String key, String value) {
        if (value != null) {
            record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
