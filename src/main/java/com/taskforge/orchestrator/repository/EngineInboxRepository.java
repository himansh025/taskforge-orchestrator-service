package com.taskforge.orchestrator.repository;

import com.taskforge.orchestrator.entity.EngineInboxEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EngineInboxRepository extends JpaRepository<EngineInboxEntity, String> {
}
