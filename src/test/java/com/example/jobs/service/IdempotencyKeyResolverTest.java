package com.example.jobs.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.jobs.config.JobPipelineProperties;
import com.example.jobs.service.IdempotencyKeyResolver.ResolvedKey;
import com.example.jobs.service.IdempotencyKeyResolver.Source;
import com.example.jobs.web.dto.CreateJobRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class IdempotencyKeyResolverTest {

    private static final long WINDOW = 600_000;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final IdempotencyKeyResolver resolver = resolver(WINDOW);
    private final Instant now = Instant.parse("2026-11-21T08:00:30Z");

    @Test
    void jobIdWinsOverEveryOtherKeyAndIsTheSameForEveryClient() throws Exception {
        String json = "{\"jobId\":\"order-42\",\"jobType\":\"echo\",\"payload\":{},\"idempotencyKey\":\"k1\"}";
        ResolvedKey a = resolver.resolve(request(json), "client:a", "evt-1", now);
        ResolvedKey b = resolver.resolve(request(json.replace("{}", "{\"v\":2}")), "client:b", null, now);

        assertThat(a).isEqualTo(new ResolvedKey("job:order-42", "job:order-42", 86_400_000, Source.JOB_ID));
        assertThat(b).isEqualTo(a);
    }

    @Test
    void explicitKeyWinsOverEventIdAndFingerprint() throws Exception {
        ResolvedKey key = resolver.resolve(request("{\"jobType\":\"echo\",\"payload\":{},\"idempotencyKey\":\" k1 \"}"),
                "client:a", "evt-1", now);
        assertThat(key).isEqualTo(new ResolvedKey("k1", "k1", 86_400_000, Source.EXPLICIT));
    }

    @Test
    void eventIdIsScopedToTheClientAndIgnoresTheBody() throws Exception {
        ResolvedKey a1 = resolver.resolve(request("{\"jobType\":\"echo\",\"payload\":{\"v\":1}}"), "client:a", "e", now);
        ResolvedKey a2 = resolver.resolve(request("{\"jobType\":\"echo\",\"payload\":{\"v\":2}}"), "client:a", "e", now);
        ResolvedKey b = resolver.resolve(request("{\"jobType\":\"echo\",\"payload\":{\"v\":1}}"), "client:b", "e", now);

        assertThat(a1.source()).isEqualTo(Source.EVENT);
        assertThat(a1.dbKey()).isEqualTo(a1.cacheKey()).startsWith("evt:").isEqualTo(a2.dbKey()).isNotEqualTo(b.dbKey());
    }

    @Test
    void fingerprintIgnoresKeyOrderAndIsBucketedByWindowInTheDatabase() throws Exception {
        ResolvedKey a = resolver.resolve(request("{\"jobType\":\"echo\",\"payload\":{\"x\":1,\"y\":{\"b\":2,\"a\":1}}}"),
                "client:a", null, now);
        ResolvedKey reordered = resolver.resolve(request("{\"payload\":{\"y\":{\"a\":1,\"b\":2},\"x\":1},\"jobType\":\"echo\"}"),
                "client:a", " ", now.plusSeconds(60));
        ResolvedKey nextBucket = resolver.resolve(request("{\"jobType\":\"echo\",\"payload\":{\"x\":1,\"y\":{\"b\":2,\"a\":1}}}"),
                "client:a", null, now.plusMillis(WINDOW));

        assertThat(a.source()).isEqualTo(Source.FINGERPRINT);
        assertThat(a.cacheTtlMs()).isEqualTo(WINDOW);
        assertThat(a.cacheKey()).startsWith("fp:").isEqualTo(reordered.cacheKey()).isEqualTo(nextBucket.cacheKey());
        assertThat(a.dbKey()).isEqualTo(a.cacheKey() + ":" + now.toEpochMilli() / WINDOW).isEqualTo(reordered.dbKey());
        assertThat(nextBucket.dbKey()).isNotEqualTo(a.dbKey());
        assertThat(a.dbKey().length()).isLessThanOrEqualTo(128);

        assertThat(resolver.resolve(request("{\"jobType\":\"echo\",\"payload\":{\"x\":1}}"), "client:b", null, now)
                .cacheKey()).isNotEqualTo(resolver.resolve(
                        request("{\"jobType\":\"echo\",\"payload\":{\"x\":1}}"), "client:a", null, now).cacheKey());
    }

    @Test
    void fingerprintingCanBeDisabled() throws Exception {
        assertThat(resolver(0).resolve(request("{\"jobType\":\"echo\",\"payload\":{}}"), "client:a", null, now)).isNull();
    }

    private IdempotencyKeyResolver resolver(long window) {
        return new IdempotencyKeyResolver(mapper, new JobPipelineProperties("w", null, null, null, null, null, null,
                new JobPipelineProperties.Idempotency(86_400_000, 30_000, 2_000, window), null));
    }

    private CreateJobRequest request(String json) throws Exception {
        return mapper.readValue(json, CreateJobRequest.class);
    }
}
