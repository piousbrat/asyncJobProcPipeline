package com.example.jobs.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.jobs.domain.JobPriority;
import com.example.jobs.exception.InvalidPayloadException;
import com.example.jobs.worker.JobExecutionContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JobHandlersTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void echoHandlerReturnsStructuredJsonAndValidatesMessage() throws Exception {
        EchoJobHandler handler = new EchoJobHandler(objectMapper);
        JsonNode validPayload = objectMapper.readTree("{\"message\":\"hello distributed world\"}");

        JsonNode result = handler.execute(context("echo", validPayload));
        assertThat(result.get("echo").asText()).isEqualTo("hello distributed world");
        assertThat(result.get("attempt").asInt()).isEqualTo(1);
        assertThat(result.get("processedBy").asText()).isEqualTo("unit-worker");

        JsonNode invalidPayload = objectMapper.readTree("{\"message\":\"   \"}");
        assertThatThrownBy(() -> handler.execute(context("echo", invalidPayload)))
                .isInstanceOf(InvalidPayloadException.class);
    }

    @Test
    void sumHandlerComputesAggregatesAndRejectsInvalidElements() throws Exception {
        SumJobHandler handler = new SumJobHandler(objectMapper);
        JsonNode validPayload = objectMapper.readTree("{\"numbers\":[10, 20.5, 9.5]}");

        JsonNode result = handler.execute(context("sum", validPayload));
        assertThat(result.get("sum").asDouble()).isEqualTo(40.0);
        assertThat(result.get("count").asInt()).isEqualTo(3);
        assertThat(result.get("min").asDouble()).isEqualTo(9.5);
        assertThat(result.get("max").asDouble()).isEqualTo(20.5);

        JsonNode badPayload = objectMapper.readTree("{\"numbers\":[1, \"oops\"]}");
        assertThatThrownBy(() -> handler.execute(context("sum", badPayload)))
                .isInstanceOf(InvalidPayloadException.class);
    }

    @Test
    void reportGenerationHandlerProducesDeterministicChecksum() throws Exception {
        ReportGenerationJobHandler handler = new ReportGenerationJobHandler(objectMapper);
        JsonNode payload = objectMapper.readTree("{\"reportName\":\"monthly-ledger\",\"rows\":250}");

        JsonNode r1 = handler.execute(context("report.generate", payload));
        JsonNode r2 = handler.execute(context("report.generate", payload));

        assertThat(r1.get("reportName").asText()).isEqualTo("monthly-ledger");
        assertThat(r1.get("rowsProcessed").asInt()).isEqualTo(250);
        assertThat(r1.get("checksum").asText()).hasSize(16).isEqualTo(r2.get("checksum").asText());
    }

    private JobExecutionContext context(String type, JsonNode payload) {
        return new JobExecutionContext(
                "job-1",
                type,
                JobPriority.NORMAL,
                1,
                3,
                5000L,
                "unit-worker",
                payload,
                Map.of(),
                Instant.now());
    }
}
