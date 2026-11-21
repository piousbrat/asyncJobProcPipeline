package com.example.jobs.handler;

import com.example.jobs.exception.InvalidPayloadException;
import com.example.jobs.worker.JobExecutionContext;
import com.example.jobs.worker.JobHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
    public JsonNode execute(JobExecutionContext context) {
        JsonNode messageNode = context.payload().get("message");
        if (messageNode == null || messageNode.isNull() || messageNode.asText().isBlank()) {
            throw new InvalidPayloadException("echo job requires a non-blank 'message' field in payload");
        }
        ObjectNode result = objectMapper.createObjectNode();
        result.put("echo", messageNode.asText());
        result.put("attempt", context.attemptNumber());
        result.put("priority", context.priority().name());
        result.put("processedBy", context.workerId());
        return result;
    }
}
