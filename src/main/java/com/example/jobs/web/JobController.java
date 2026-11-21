package com.example.jobs.web;

import com.example.jobs.service.JobService;
import com.example.jobs.web.dto.CreateJobRequest;
import com.example.jobs.web.dto.JobAcceptedResponse;
import com.example.jobs.web.dto.JobStatusResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import java.net.URI;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    public ResponseEntity<JobAcceptedResponse> submitJob(
            @Valid @RequestBody CreateJobRequest request,
            @RequestHeader(name = ClientIdentity.EVENT_HEADER, required = false) String eventId,
            HttpServletRequest httpRequest) {
        JobAcceptedResponse response = jobService.submitJob(request, ClientIdentity.of(httpRequest), eventId);
        return ResponseEntity.accepted()
                .location(URI.create(response.statusUrl()))
                .body(response);
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<JobStatusResponse> getJobStatus(@PathVariable String jobId) {
        return ResponseEntity.ok(jobService.getJobStatus(jobId));
    }
}
