package com.taskforge.orchestrator.controller;

import com.taskforge.orchestrator.entity.ExecutionEntity;
import com.taskforge.orchestrator.entity.TaskExecutionEntity;
import com.taskforge.orchestrator.repository.ExecutionRepository;
import com.taskforge.orchestrator.repository.TaskExecutionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/engine")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class ExecutionQueryController {

    private final ExecutionRepository executionRepository;
    private final TaskExecutionRepository taskExecutionRepository;

    @GetMapping("/executions/{executionId}")
    public ResponseEntity<Map<String, Object>> getExecution(@PathVariable UUID executionId) {
        ExecutionEntity execution = executionRepository.findById(executionId)
                .orElseThrow(() -> new IllegalArgumentException("Execution not found"));
        List<TaskExecutionEntity> tasks = taskExecutionRepository.findByExecutionId(executionId);

        return ResponseEntity.ok(Map.of(
                "execution", execution,
                "tasks", tasks
        ));
    }
}
