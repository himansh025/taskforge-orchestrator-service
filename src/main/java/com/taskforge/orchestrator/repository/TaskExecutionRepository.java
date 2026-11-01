package com.taskforge.orchestrator.repository;

import com.taskforge.orchestrator.entity.TaskExecutionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface TaskExecutionRepository extends JpaRepository<TaskExecutionEntity, UUID> {
    List<TaskExecutionEntity> findByExecutionId(UUID executionId);
    Optional<TaskExecutionEntity> findByExecutionIdAndNodeId(UUID executionId, String nodeId);
}
