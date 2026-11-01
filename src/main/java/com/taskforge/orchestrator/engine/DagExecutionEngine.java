package com.taskforge.orchestrator.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.common.event.EventEnvelope;
import com.taskforge.common.event.TaskRequestedData;
import com.taskforge.common.event.WorkflowExecutionUpdatedData;
import com.taskforge.common.model.ExecutionStatus;
import com.taskforge.common.model.TaskExecutionStatus;
import com.taskforge.common.model.TaskType;
import com.taskforge.orchestrator.entity.EngineOutboxEntity;
import com.taskforge.orchestrator.entity.ExecutionEntity;
import com.taskforge.orchestrator.entity.TaskExecutionEntity;
import com.taskforge.orchestrator.repository.EngineOutboxRepository;
import com.taskforge.orchestrator.repository.ExecutionRepository;
import com.taskforge.orchestrator.repository.TaskExecutionRepository;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class DagExecutionEngine {

    private final ExecutionRepository executionRepository;
    private final TaskExecutionRepository taskExecutionRepository;
    private final EngineOutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    @SneakyThrows
    public void startExecution(ExecutionEntity execution, DagDefinition dag) {
        execution.setStatus(ExecutionStatus.RUNNING);
        execution.setStartedAt(Instant.now());
        executionRepository.save(execution);

        // Find incoming edge count per node
        Set<String> targetNodes = dag.getEdges().stream()
                .map(DagDefinition.DagEdge::getTo)
                .collect(Collectors.toSet());

        List<DagDefinition.DagNode> rootNodes = dag.getNodes().stream()
                .filter(n -> !targetNodes.contains(n.getId()))
                .toList();

        if (rootNodes.isEmpty() && !dag.getNodes().isEmpty()) {
            rootNodes = List.of(dag.getNodes().get(0));
        }

        for (DagDefinition.DagNode rootNode : rootNodes) {
            dispatchTask(execution, rootNode.getId(), rootNode.getType() != null ? rootNode.getType() : TaskType.CSV_VALIDATE, Collections.emptyMap());
        }

        emitExecutionUpdated(execution, 0, dag.getNodes().size());
    }

    @Transactional
    @SneakyThrows
    public void handleTaskCompleted(UUID executionId, String nodeId, Map<String, Object> output) {
        ExecutionEntity execution = executionRepository.findById(executionId)
                .orElseThrow(() -> new IllegalArgumentException("Execution not found: " + executionId));

        TaskExecutionEntity task = taskExecutionRepository.findByExecutionIdAndNodeId(executionId, nodeId)
                .orElseThrow(() -> new IllegalArgumentException("Task execution not found for node: " + nodeId));

        task.setStatus(TaskExecutionStatus.COMPLETED);
        task.setOutputPayload(objectMapper.writeValueAsString(output));
        task.setFinishedAt(Instant.now());
        taskExecutionRepository.save(task);

        // Parse DAG from execution or handle subsequent steps
        List<TaskExecutionEntity> allTasks = taskExecutionRepository.findByExecutionId(executionId);
        boolean anyPending = allTasks.stream().anyMatch(t -> t.getStatus() == TaskExecutionStatus.PENDING || t.getStatus() == TaskExecutionStatus.RUNNING || t.getStatus() == TaskExecutionStatus.DISPATCHED);

        if (!anyPending) {
            execution.setStatus(ExecutionStatus.COMPLETED);
            execution.setFinishedAt(Instant.now());
            executionRepository.save(execution);
            log.info("Execution {} fully COMPLETED", executionId);
        }

        emitExecutionUpdated(execution, (int) allTasks.stream().filter(t -> t.getStatus() == TaskExecutionStatus.COMPLETED).count(), allTasks.size());
    }

    @Transactional
    @SneakyThrows
    public void handleTaskFailed(UUID executionId, String nodeId, String errorMessage, boolean retryable) {
        ExecutionEntity execution = executionRepository.findById(executionId)
                .orElseThrow(() -> new IllegalArgumentException("Execution not found: " + executionId));

        TaskExecutionEntity task = taskExecutionRepository.findByExecutionIdAndNodeId(executionId, nodeId)
                .orElseThrow(() -> new IllegalArgumentException("Task execution not found for node: " + nodeId));

        task.setStatus(TaskExecutionStatus.FAILED);
        task.setErrorMessage(errorMessage);
        task.setFinishedAt(Instant.now());
        taskExecutionRepository.save(task);

        execution.setStatus(ExecutionStatus.FAILED);
        execution.setErrorMessage("Task " + nodeId + " failed: " + errorMessage);
        execution.setFinishedAt(Instant.now());
        executionRepository.save(execution);

        log.error("Execution {} FAILED at node {}", executionId, nodeId);
        emitExecutionUpdated(execution, 0, 1);
    }

    @SneakyThrows
    private void dispatchTask(ExecutionEntity execution, String nodeId, TaskType taskType, Map<String, Object> input) {
        TaskExecutionEntity task = TaskExecutionEntity.builder()
                .executionId(execution.getId())
                .nodeId(nodeId)
                .taskType(taskType)
                .status(TaskExecutionStatus.DISPATCHED)
                .currentAttempt(1)
                .startedAt(Instant.now())
                .inputPayload(objectMapper.writeValueAsString(input))
                .build();
        taskExecutionRepository.save(task);

        TaskRequestedData data = TaskRequestedData.builder()
                .executionId(execution.getId().toString())
                .workflowId(execution.getWorkflowId().toString())
                .taskExecutionId(task.getId().toString())
                .nodeId(nodeId)
                .taskType(taskType)
                .attempt(1)
                .input(input)
                .timeoutSeconds(300)
                .build();

        EventEnvelope<TaskRequestedData> envelope = EventEnvelope.<TaskRequestedData>builder()
                .eventType("task.requested")
                .workspaceId(execution.getWorkspaceId().toString())
                .correlationId(execution.getId().toString())
                .producer("orchestrator-service")
                .data(data)
                .build();

        EngineOutboxEntity outbox = EngineOutboxEntity.builder()
                .aggregateType("TaskExecution")
                .aggregateId(task.getId().toString())
                .eventType("task.requested")
                .topic("task.requested.v1")
                .payload(objectMapper.writeValueAsString(envelope))
                .status("PENDING")
                .build();

        outboxRepository.save(outbox);
        log.info("Dispatched task {} for execution {}", nodeId, execution.getId());
    }

    @SneakyThrows
    private void emitExecutionUpdated(ExecutionEntity execution, int completed, int total) {
        WorkflowExecutionUpdatedData data = WorkflowExecutionUpdatedData.builder()
                .executionId(execution.getId().toString())
                .workflowId(execution.getWorkflowId().toString())
                .status(execution.getStatus())
                .completedNodes(completed)
                .totalNodes(total)
                .build();

        EventEnvelope<WorkflowExecutionUpdatedData> envelope = EventEnvelope.<WorkflowExecutionUpdatedData>builder()
                .eventType("workflow.execution.updated")
                .workspaceId(execution.getWorkspaceId().toString())
                .correlationId(execution.getId().toString())
                .producer("orchestrator-service")
                .data(data)
                .build();

        EngineOutboxEntity outbox = EngineOutboxEntity.builder()
                .aggregateType("Execution")
                .aggregateId(execution.getId().toString())
                .eventType("workflow.execution.updated")
                .topic("workflow.execution.updated.v1")
                .payload(objectMapper.writeValueAsString(envelope))
                .status("PENDING")
                .build();

        outboxRepository.save(outbox);
    }
}
