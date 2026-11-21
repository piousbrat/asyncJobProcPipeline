package com.example.jobs.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app.jobs")
public class JobPipelineProperties {

    @NotBlank
    private String workerId = "worker-default";

    @Valid
    private Defaults defaults = new Defaults();

    @Valid
    private Topics topics = new Topics();

    @Valid
    private Consumer consumer = new Consumer();

    @Valid
    private Retry retry = new Retry();

    @Valid
    private Outbox outbox = new Outbox();

    @Valid
    private Worker worker = new Worker();

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public Defaults getDefaults() {
        return defaults;
    }

    public void setDefaults(Defaults defaults) {
        this.defaults = defaults;
    }

    public Topics getTopics() {
        return topics;
    }

    public void setTopics(Topics topics) {
        this.topics = topics;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    public Retry getRetry() {
        return retry;
    }

    public void setRetry(Retry retry) {
        this.retry = retry;
    }

    public Outbox getOutbox() {
        return outbox;
    }

    public void setOutbox(Outbox outbox) {
        this.outbox = outbox;
    }

    public Worker getWorker() {
        return worker;
    }

    public void setWorker(Worker worker) {
        this.worker = worker;
    }

    public static class Defaults {
        @Min(0)
        private int maxRetries = 3;

        @Min(10)
        private long timeoutMs = 10_000L;

        public int getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public long getTimeoutMs() {
            return timeoutMs;
        }

        public void setTimeoutMs(long timeoutMs) {
            this.timeoutMs = timeoutMs;
        }
    }

    public static class Topics {
        @NotBlank
        private String high = "jobs.high";

        @NotBlank
        private String normal = "jobs.normal";

        @NotBlank
        private String low = "jobs.low";

        @NotBlank
        private String dlq = "jobs.dlq";

        @Valid
        private Partitions partitions = new Partitions();

        @Min(1)
        private short replicationFactor = 1;

        public String getHigh() {
            return high;
        }

        public void setHigh(String high) {
            this.high = high;
        }

        public String getNormal() {
            return normal;
        }

        public void setNormal(String normal) {
            this.normal = normal;
        }

        public String getLow() {
            return low;
        }

        public void setLow(String low) {
            this.low = low;
        }

        public String getDlq() {
            return dlq;
        }

        public void setDlq(String dlq) {
            this.dlq = dlq;
        }

        public Partitions getPartitions() {
            return partitions;
        }

        public void setPartitions(Partitions partitions) {
            this.partitions = partitions;
        }

        public short getReplicationFactor() {
            return replicationFactor;
        }

        public void setReplicationFactor(short replicationFactor) {
            this.replicationFactor = replicationFactor;
        }
    }

    public static class Partitions {
        @Min(1)
        private int high = 6;

        @Min(1)
        private int normal = 3;

        @Min(1)
        private int low = 2;

        @Min(1)
        private int dlq = 3;

        public int getHigh() {
            return high;
        }

        public void setHigh(int high) {
            this.high = high;
        }

        public int getNormal() {
            return normal;
        }

        public void setNormal(int normal) {
            this.normal = normal;
        }

        public int getLow() {
            return low;
        }

        public void setLow(int low) {
            this.low = low;
        }

        public int getDlq() {
            return dlq;
        }

        public void setDlq(int dlq) {
            this.dlq = dlq;
        }
    }

    public static class Consumer {
        @Min(1)
        private int highConcurrency = 4;

        @Min(1)
        private int normalConcurrency = 2;

        @Min(1)
        private int lowConcurrency = 1;

        @Min(1)
        private int dlqConcurrency = 1;

        @Min(1)
        private int highMaxPollRecords = 10;

        @Min(1)
        private int normalMaxPollRecords = 5;

        @Min(1)
        private int lowMaxPollRecords = 2;

        public int getHighConcurrency() {
            return highConcurrency;
        }

        public void setHighConcurrency(int highConcurrency) {
            this.highConcurrency = highConcurrency;
        }

        public int getNormalConcurrency() {
            return normalConcurrency;
        }

        public void setNormalConcurrency(int normalConcurrency) {
            this.normalConcurrency = normalConcurrency;
        }

        public int getLowConcurrency() {
            return lowConcurrency;
        }

        public void setLowConcurrency(int lowConcurrency) {
            this.lowConcurrency = lowConcurrency;
        }

        public int getDlqConcurrency() {
            return dlqConcurrency;
        }

        public void setDlqConcurrency(int dlqConcurrency) {
            this.dlqConcurrency = dlqConcurrency;
        }

        public int getHighMaxPollRecords() {
            return highMaxPollRecords;
        }

        public void setHighMaxPollRecords(int highMaxPollRecords) {
            this.highMaxPollRecords = highMaxPollRecords;
        }

        public int getNormalMaxPollRecords() {
            return normalMaxPollRecords;
        }

        public void setNormalMaxPollRecords(int normalMaxPollRecords) {
            this.normalMaxPollRecords = normalMaxPollRecords;
        }

        public int getLowMaxPollRecords() {
            return lowMaxPollRecords;
        }

        public void setLowMaxPollRecords(int lowMaxPollRecords) {
            this.lowMaxPollRecords = lowMaxPollRecords;
        }
    }

    public static class Retry {
        @Min(1)
        private long baseIntervalMs = 1000L;

        @DecimalMin("1.0")
        private double multiplier = 2.0;

        @Min(1)
        private long maxIntervalMs = 60_000L;

        @DecimalMin("0.0")
        @DecimalMax("1.0")
        private double jitterFactor = 0.3;

        public long getBaseIntervalMs() {
            return baseIntervalMs;
        }

        public void setBaseIntervalMs(long baseIntervalMs) {
            this.baseIntervalMs = baseIntervalMs;
        }

        public double getMultiplier() {
            return multiplier;
        }

        public void setMultiplier(double multiplier) {
            this.multiplier = multiplier;
        }

        public long getMaxIntervalMs() {
            return maxIntervalMs;
        }

        public void setMaxIntervalMs(long maxIntervalMs) {
            this.maxIntervalMs = maxIntervalMs;
        }

        public double getJitterFactor() {
            return jitterFactor;
        }

        public void setJitterFactor(double jitterFactor) {
            this.jitterFactor = jitterFactor;
        }
    }

    public static class Outbox {
        private boolean enabled = true;

        @Min(10)
        private long pollIntervalMs = 200L;

        @Min(1)
        private int batchSize = 50;

        @Min(100)
        private long lockDurationMs = 30_000L;

        @Min(100)
        private long sendTimeoutMs = 5_000L;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public long getPollIntervalMs() {
            return pollIntervalMs;
        }

        public void setPollIntervalMs(long pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public long getLockDurationMs() {
            return lockDurationMs;
        }

        public void setLockDurationMs(long lockDurationMs) {
            this.lockDurationMs = lockDurationMs;
        }

        public long getSendTimeoutMs() {
            return sendTimeoutMs;
        }

        public void setSendTimeoutMs(long sendTimeoutMs) {
            this.sendTimeoutMs = sendTimeoutMs;
        }
    }

    public static class Worker {
        @Min(1)
        private int executorPoolSize = 16;

        @Min(100)
        private long leaseGraceMs = 5_000L;

        @Min(100)
        private long heartbeatIntervalMs = 2_000L;

        @Min(50)
        private long staleRecoveryIntervalMs = 2_000L;

        @Min(1)
        private int staleRecoveryBatchSize = 25;

        public int getExecutorPoolSize() {
            return executorPoolSize;
        }

        public void setExecutorPoolSize(int executorPoolSize) {
            this.executorPoolSize = executorPoolSize;
        }

        public long getLeaseGraceMs() {
            return leaseGraceMs;
        }

        public void setLeaseGraceMs(long leaseGraceMs) {
            this.leaseGraceMs = leaseGraceMs;
        }

        public long getHeartbeatIntervalMs() {
            return heartbeatIntervalMs;
        }

        public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
            this.heartbeatIntervalMs = heartbeatIntervalMs;
        }

        public long getStaleRecoveryIntervalMs() {
            return staleRecoveryIntervalMs;
        }

        public void setStaleRecoveryIntervalMs(long staleRecoveryIntervalMs) {
            this.staleRecoveryIntervalMs = staleRecoveryIntervalMs;
        }

        public int getStaleRecoveryBatchSize() {
            return staleRecoveryBatchSize;
        }

        public void setStaleRecoveryBatchSize(int staleRecoveryBatchSize) {
            this.staleRecoveryBatchSize = staleRecoveryBatchSize;
        }
    }
}
