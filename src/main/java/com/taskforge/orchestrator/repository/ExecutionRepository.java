package com.taskforge.orchestrator.repository;

import com.taskforge.orchestrator.entity.ExecutionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ExecutionRepository extends JpaRepository<ExecutionEntity, UUID> {
    List<ExecutionEntity> findByWorkspaceIdOrderByCreatedAtDesc(UUID workspaceId);
}
