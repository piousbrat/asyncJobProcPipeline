package com.example.jobs.handler;

import com.example.jobs.exception.InvalidPayloadException;
import com.example.jobs.worker.JobExecutionContext;
import com.example.jobs.worker.JobHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class EchoJobHandler implements JobHandler {

    private final ObjectMapper objectMapper;

    public EchoJobHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String jobType() {
        return "echo";
    }

    @Override
    public Map<String, String> validatePayload(JsonNode payload) {
        JsonNode message = payload.get("message");
        if (message == null || !message.isTextual() || message.asText().isBlank()) {
            return Map.of("message", "must be a non-blank string");
        }
        return Map.of();
    }

    @Override
    public JsonNode execute(JobExecutionContext context) {
        InvalidPayloadException.throwIfAny(validatePayload(context.payload()));
        JsonNode messageNode = context.payload().get("message");
        ObjectNode result = objectMapper.createObjectNode();
        result.put("echo", messageNode.asText());
        result.put("attempt", context.attemptNumber());
        result.put("priority", context.priority().name());
        result.put("processedBy", context.workerId());
        return result;
    }
}
