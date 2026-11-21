package com.example.jobs.repository;

import com.example.jobs.domain.JobAttemptEntity;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JobAttemptRepository extends JpaRepository<JobAttemptEntity, String> {

    List<JobAttemptEntity> findByJobIdOrderByReplayGenerationAscAttemptNumberAsc(String jobId);

    Optional<JobAttemptEntity> findByJobIdAndReplayGenerationAndAttemptNumber(
            String jobId, int replayGeneration, int attemptNumber);
}
