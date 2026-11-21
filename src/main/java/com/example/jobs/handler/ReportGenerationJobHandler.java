package com.example.jobs.handler;

import com.example.jobs.exception.InvalidPayloadException;
import com.example.jobs.worker.JobExecutionContext;
import com.example.jobs.worker.JobHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.springframework.stereotype.Component;

@Component
public class ReportGenerationJobHandler implements JobHandler {

    private final ObjectMapper objectMapper;

    public ReportGenerationJobHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String jobType() {
        return "report.generate";
    }

    @Override
    public JsonNode execute(JobExecutionContext context) throws Exception {
        String reportName = context.payload().path("reportName").asText("");
        if (reportName.isBlank()) {
            throw new InvalidPayloadException("report.generate job requires a non-blank 'reportName' field");
        }
        int rows = context.payload().path("rows").asInt(100);
        if (rows < 0) {
            throw new InvalidPayloadException("'rows' must be >= 0");
        }

        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest((reportName + ":" + rows).getBytes(StandardCharsets.UTF_8));

        ObjectNode result = objectMapper.createObjectNode();
        result.put("reportName", reportName);
        result.put("rowsProcessed", rows);
        result.put("checksum", HexFormat.of().formatHex(hash).substring(0, 16));
        result.put("attempt", context.attemptNumber());
        return result;
    }
}
