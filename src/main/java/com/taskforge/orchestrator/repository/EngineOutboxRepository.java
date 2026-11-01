package com.taskforge.orchestrator.repository;

import com.taskforge.orchestrator.entity.EngineOutboxEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface EngineOutboxRepository extends JpaRepository<EngineOutboxEntity, UUID> {
    List<EngineOutboxEntity> findByStatusOrderByCreatedAtAsc(String status, Pageable pageable);
}
