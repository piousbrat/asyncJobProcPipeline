package com.example.jobs.web;

import com.example.jobs.service.JobService;
import com.example.jobs.web.dto.DeadLetterJobResponse;
import com.example.jobs.web.dto.JobAcceptedResponse;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/dlq")
public class DlqController {

    private final JobService jobService;

    public DlqController(JobService jobService) {
        this.jobService = jobService;
    }

    @GetMapping
    public ResponseEntity<List<DeadLetterJobResponse>> listDeadLetterJobs() {
        return ResponseEntity.ok(jobService.listDeadLetterJobs());
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<DeadLetterJobResponse> getDeadLetterJob(@PathVariable String jobId) {
        return ResponseEntity.ok(jobService.getDeadLetterJob(jobId));
    }

    @PostMapping("/{jobId}/replay")
    public ResponseEntity<JobAcceptedResponse> replayDeadLetterJob(@PathVariable String jobId) {
        JobAcceptedResponse response = jobService.replayDeadLetterJob(jobId);
        return ResponseEntity.accepted()
                .location(URI.create(response.statusUrl()))
                .body(response);
    }
}
