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
import java.util.LinkedHashMap;
import java.util.Map;
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
    public Map<String, String> validatePayload(JsonNode payload) {
        Map<String, String> errors = new LinkedHashMap<>();
        JsonNode reportName = payload.get("reportName");
        if (reportName == null || !reportName.isTextual() || reportName.asText().isBlank()) {
            errors.put("reportName", "must be a non-blank string");
        }
        JsonNode rows = payload.get("rows");
        if (rows != null && !(rows.isInt() && rows.asInt() >= 0)) {
            errors.put("rows", "must be a non-negative integer");
        }
        return errors;
    }

    @Override
    public JsonNode execute(JobExecutionContext context) throws Exception {
        InvalidPayloadException.throwIfAny(validatePayload(context.payload()));
        String reportName = context.payload().get("reportName").asText();
        int rows = context.payload().path("rows").asInt(100);

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
