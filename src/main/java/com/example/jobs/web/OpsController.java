package com.example.jobs.web;

import com.example.jobs.domain.JobPriority;
import com.example.jobs.service.JobService;
import com.example.jobs.service.OpsService;
import com.example.jobs.web.dto.JobStatusResponse;
import com.example.jobs.web.dto.QueueDepthResponse;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Operational endpoints; expose only on an internal network or behind authentication. */
@RestController
@RequestMapping("/api/v1/ops")
public class OpsController {

    private final OpsService opsService;
    private final JobService jobService;

    public OpsController(OpsService opsService, JobService jobService) {
        this.opsService = opsService;
        this.jobService = jobService;
    }

    @GetMapping("/queue")
    public QueueDepthResponse queueDepth() {
        return opsService.queueDepth();
    }

    @PostMapping("/jobs/{jobId}/cancel")
    public JobStatusResponse cancel(@PathVariable String jobId) {
        opsService.cancel(jobId);
        return jobService.getJobStatus(jobId);
    }

    @PostMapping("/queue/drain")
    public Map<String, Integer> drain(@RequestParam(required = false) JobPriority priority) {
        return Map.of("cancelled", opsService.drain(priority));
    }
}
