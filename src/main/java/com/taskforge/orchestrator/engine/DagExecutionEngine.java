package com.taskforge.orchestrator.engine;

import com.fasterxml.jackson.core.type.TypeReference;
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
import org.springframework.data.redis.core.StringRedisTemplate;
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
    private final StringRedisTemplate redisTemplate;

    @Transactional
    @SneakyThrows
    public void startExecution(ExecutionEntity execution, DagDefinition dag) {
        execution.setStatus(ExecutionStatus.RUNNING);
        execution.setStartedAt(Instant.now());
        executionRepository.save(execution);

        Map<String, Object> initialInput = Collections.emptyMap();
        if (execution.getInputPayload() != null && !execution.getInputPayload().isBlank()) {
            try {
                initialInput = objectMapper.readValue(execution.getInputPayload(), new TypeReference<>() {});
            } catch (Exception e) {
                log.warn("Failed to parse initial input: {}", e.getMessage());
            }
        }

        // Identify root nodes (nodes with no incoming edges)
        Set<String> targetNodes = dag.getEdges().stream()
                .map(DagDefinition.DagEdge::getEffectiveTarget)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        List<DagDefinition.DagNode> rootNodes = dag.getNodes().stream()
                .filter(n -> !targetNodes.contains(n.getId()))
                .toList();

        if (rootNodes.isEmpty() && !dag.getNodes().isEmpty()) {
            rootNodes = List.of(dag.getNodes().get(0));
        }

        for (DagDefinition.DagNode rootNode : rootNodes) {
            dispatchTask(execution, rootNode, initialInput);
        }

        broadcastProgress(execution.getId(), "RUNNING", "DAG execution initiated", null, 0, dag.getNodes().size());
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

        log.info("Task {} for execution {} COMPLETED with output: {}", nodeId, executionId, output);

        DagDefinition dag = parseDag(execution.getDefinitionPayload());
        int totalNodes = dag.getNodes().size();

        broadcastProgress(executionId, "RUNNING", "Node completed: " + nodeId, nodeId, 
                (int) taskExecutionRepository.findByExecutionId(executionId).stream().filter(t -> t.getStatus() == TaskExecutionStatus.COMPLETED).count(), totalNodes);

        // Find child nodes connected from this completed node
        List<DagDefinition.DagEdge> outgoingEdges = dag.getEdges().stream()
                .filter(e -> nodeId.equals(e.getEffectiveSource()))
                .toList();

        for (DagDefinition.DagEdge edge : outgoingEdges) {
            String childNodeId = edge.getEffectiveTarget();
            if (childNodeId == null) continue;

            // Check if condition allows this edge
            if (!isConditionMet(edge, output)) {
                log.info("Edge condition not met for {} -> {}, skipping branch", nodeId, childNodeId);
                continue;
            }

            // Find all incoming parents for this child node
            List<DagDefinition.DagEdge> parentEdges = dag.getEdges().stream()
                    .filter(e -> childNodeId.equals(e.getEffectiveTarget()))
                    .toList();

            boolean allParentsCompleted = parentEdges.stream().allMatch(p -> {
                Optional<TaskExecutionEntity> parentTask = taskExecutionRepository.findByExecutionIdAndNodeId(executionId, p.getEffectiveSource());
                return parentTask.isPresent() && parentTask.get().getStatus() == TaskExecutionStatus.COMPLETED;
            });

            if (allParentsCompleted) {
                // If child not yet scheduled
                if (taskExecutionRepository.findByExecutionIdAndNodeId(executionId, childNodeId).isEmpty()) {
                    DagDefinition.DagNode childNode = dag.getNodes().stream()
                            .filter(n -> childNodeId.equals(n.getId()))
                            .findFirst()
                            .orElse(null);

                    if (childNode != null) {
                        // Forward outputs and initial inputs to child
                        Map<String, Object> childInput = new HashMap<>();
                        if (execution.getInputPayload() != null) {
                            try {
                                childInput.putAll(objectMapper.readValue(execution.getInputPayload(), new TypeReference<>() {}));
                            } catch (Exception ignored) {}
                        }
                        if (output != null) {
                            childInput.putAll(output);
                        }
                        dispatchTask(execution, childNode, childInput);
                    }
                }
            }
        }

        // Check if all active tasks are complete
        List<TaskExecutionEntity> allTasks = taskExecutionRepository.findByExecutionId(executionId);
        boolean anyPending = allTasks.stream().anyMatch(t ->
                t.getStatus() == TaskExecutionStatus.PENDING ||
                t.getStatus() == TaskExecutionStatus.RUNNING ||
                t.getStatus() == TaskExecutionStatus.DISPATCHED
        );

        if (!anyPending) {
            execution.setStatus(ExecutionStatus.COMPLETED);
            execution.setOutputPayload(objectMapper.writeValueAsString(output));
            execution.setFinishedAt(Instant.now());
            executionRepository.save(execution);

            log.info("Workflow execution {} fully COMPLETED!", executionId);
            broadcastProgress(executionId, "COMPLETED", "Workflow completed successfully", nodeId, allTasks.size(), totalNodes);
        }

        emitExecutionUpdated(execution, (int) allTasks.stream().filter(t -> t.getStatus() == TaskExecutionStatus.COMPLETED).count(), totalNodes);
    }

    @Transactional
    @SneakyThrows
    public void handleTaskFailed(UUID executionId, String nodeId, String errorMessage, boolean retryable) {
        ExecutionEntity execution = executionRepository.findById(executionId)
                .orElseThrow(() -> new IllegalArgumentException("Execution not found: " + executionId));

        TaskExecutionEntity task = taskExecutionRepository.findByExecutionIdAndNodeId(executionId, nodeId)
                .orElseThrow(() -> new IllegalArgumentException("Task execution not found for node: " + nodeId));

        if (retryable && task.getCurrentAttempt() < task.getMaxRetries()) {
            task.setCurrentAttempt(task.getCurrentAttempt() + 1);
            task.setStatus(TaskExecutionStatus.DISPATCHED);
            taskExecutionRepository.save(task);

            log.info("Retrying task {} for execution {} (attempt {})", nodeId, executionId, task.getCurrentAttempt());
            broadcastProgress(executionId, "RUNNING", "Retrying task: " + nodeId + " (attempt " + task.getCurrentAttempt() + ")", nodeId, 0, 1);
            return;
        }

        task.setStatus(TaskExecutionStatus.FAILED);
        task.setErrorMessage(errorMessage);
        task.setFinishedAt(Instant.now());
        taskExecutionRepository.save(task);

        execution.setStatus(ExecutionStatus.FAILED);
        execution.setErrorMessage("Task " + nodeId + " failed: " + errorMessage);
        execution.setFinishedAt(Instant.now());
        executionRepository.save(execution);

        log.error("Execution {} FAILED at node {}: {}", executionId, nodeId, errorMessage);
        broadcastProgress(executionId, "FAILED", errorMessage, nodeId, 0, 1);
        emitExecutionUpdated(execution, 0, 1);
    }

    @Transactional
    public void cancelExecution(UUID executionId) {
        ExecutionEntity execution = executionRepository.findById(executionId)
                .orElseThrow(() -> new IllegalArgumentException("Execution not found: " + executionId));

        if (execution.getStatus() == ExecutionStatus.RUNNING || execution.getStatus() == ExecutionStatus.PENDING) {
            execution.setStatus(ExecutionStatus.CANCELLED);
            execution.setFinishedAt(Instant.now());
            executionRepository.save(execution);

            List<TaskExecutionEntity> tasks = taskExecutionRepository.findByExecutionId(executionId);
            for (TaskExecutionEntity t : tasks) {
                if (t.getStatus() == TaskExecutionStatus.RUNNING || t.getStatus() == TaskExecutionStatus.DISPATCHED) {
                    t.setStatus(TaskExecutionStatus.CANCELLED);
                    taskExecutionRepository.save(t);
                }
            }
            broadcastProgress(executionId, "CANCELLED", "Execution cancelled by user", null, 0, 0);
        }
    }

    @SneakyThrows
    private void dispatchTask(ExecutionEntity execution, DagDefinition.DagNode node, Map<String, Object> input) {
        TaskType taskType = node.getResolvedTaskType();
        Map<String, Object> mergedInput = new HashMap<>(input);
        if (node.getResolvedConfig() != null) {
            mergedInput.putAll(node.getResolvedConfig());
        }

        TaskExecutionEntity task = TaskExecutionEntity.builder()
                .executionId(execution.getId())
                .nodeId(node.getId())
                .taskType(taskType)
                .status(TaskExecutionStatus.DISPATCHED)
                .currentAttempt(1)
                .maxRetries(3)
                .startedAt(Instant.now())
                .inputPayload(objectMapper.writeValueAsString(mergedInput))
                .build();
        taskExecutionRepository.save(task);

        TaskRequestedData data = TaskRequestedData.builder()
                .executionId(execution.getId().toString())
                .workflowId(execution.getWorkflowId().toString())
                .taskExecutionId(task.getId().toString())
                .nodeId(node.getId())
                .taskType(taskType)
                .attempt(1)
                .input(mergedInput)
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
        log.info("Dispatched task {} ({}) for execution {}", node.getId(), taskType, execution.getId());

        broadcastProgress(execution.getId(), "RUNNING", "Dispatched task: " + node.getResolvedName(), node.getId(), 0, 1);
    }

    private boolean isConditionMet(DagDefinition.DagEdge edge, Map<String, Object> output) {
        if (edge.getConditionValue() == null && edge.getLabel() == null) {
            return true;
        }
        if (output == null) return true;

        String expected = edge.getConditionValue() != null ? edge.getConditionValue().toLowerCase() : edge.getLabel().toLowerCase();
        Object branch = output.get("branch");
        Object result = output.get("result");

        if (branch != null) {
            return expected.contains(branch.toString().toLowerCase());
        }
        if (result != null) {
            if (result instanceof Boolean b) {
                return (b && expected.contains("pass") || expected.contains("true")) ||
                       (!b && expected.contains("fail") || expected.contains("review") || expected.contains("false"));
            }
        }
        return true;
    }

    private DagDefinition parseDag(String definitionJson) {
        if (definitionJson != null && !definitionJson.isBlank()) {
            try {
                return objectMapper.readValue(definitionJson, DagDefinition.class);
            } catch (Exception e) {
                log.warn("Failed to parse DAG JSON: {}", e.getMessage());
            }
        }
        return new DagDefinition();
    }

    private void broadcastProgress(UUID executionId, String status, String message, String nodeId, int completed, int total) {
        try {
            Map<String, Object> payload = Map.of(
                    "executionId", executionId.toString(),
                    "status", status,
                    "message", message != null ? message : "",
                    "nodeId", nodeId != null ? nodeId : "",
                    "completedNodes", completed,
                    "totalNodes", total,
                    "timestamp", Instant.now().toString()
            );
            String json = objectMapper.writeValueAsString(payload);
            redisTemplate.convertAndSend("execution:" + executionId + ":progress", json);
            log.debug("Broadcasted progress to Redis channel execution:{}:progress", executionId);
        } catch (Exception e) {
            log.warn("Redis progress broadcast failed: {}", e.getMessage());
        }
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
