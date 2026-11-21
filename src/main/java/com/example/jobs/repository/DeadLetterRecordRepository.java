package com.example.jobs.repository;

import com.example.jobs.domain.DeadLetterRecordEntity;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DeadLetterRecordRepository extends JpaRepository<DeadLetterRecordEntity, String> {

    List<DeadLetterRecordEntity> findAllByOrderByDeadLetteredAtDesc();

    Optional<DeadLetterRecordEntity> findFirstByJobIdOrderByDeadLetteredAtDesc(String jobId);

    Optional<DeadLetterRecordEntity> findByOutboxEventId(String outboxEventId);
}
