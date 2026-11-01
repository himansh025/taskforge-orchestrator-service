package com.taskforge.orchestrator.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.common.event.EventEnvelope;
import com.taskforge.common.event.WorkflowExecutionRequestedData;
import com.taskforge.common.model.ExecutionStatus;
import com.taskforge.orchestrator.engine.DagDefinition;
import com.taskforge.orchestrator.engine.DagExecutionEngine;
import com.taskforge.orchestrator.entity.EngineInboxEntity;
import com.taskforge.orchestrator.entity.ExecutionEntity;
import com.taskforge.orchestrator.repository.EngineInboxRepository;
import com.taskforge.orchestrator.repository.ExecutionRepository;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class WorkflowExecutionRequestedConsumer {

    private final ObjectMapper objectMapper;
    private final ExecutionRepository executionRepository;
    private final EngineInboxRepository inboxRepository;
    private final DagExecutionEngine executionEngine;

    @KafkaListener(topics = "${taskforge.kafka.topics.workflow-execution-requested:workflow.execution.requested.v1}", groupId = "orchestrator-service-group")
    @Transactional
    @SneakyThrows
    public void onMessage(String message) {
        EventEnvelope<WorkflowExecutionRequestedData> envelope = objectMapper.readValue(
                message, new TypeReference<>() {});

        if (inboxRepository.existsById(envelope.getEventId())) {
            log.info("Duplicate event ignored: {}", envelope.getEventId());
            return;
        }

        WorkflowExecutionRequestedData data = envelope.getData();
        UUID executionId = UUID.fromString(data.getExecutionId());

        ExecutionEntity execution = ExecutionEntity.builder()
                .id(executionId)
                .workspaceId(UUID.fromString(envelope.getWorkspaceId()))
                .workflowId(UUID.fromString(data.getWorkflowId()))
                .workflowVersionId(UUID.fromString(data.getWorkflowVersionId()))
                .status(ExecutionStatus.PENDING)
                .triggerType(data.getTriggerType())
                .inputPayload(objectMapper.writeValueAsString(data.getInput()))
                .startedAt(Instant.now())
                .build();

        executionRepository.save(execution);
        inboxRepository.save(EngineInboxEntity.builder()
                .eventId(envelope.getEventId())
                .consumerGroup("orchestrator-service-group")
                .build());

        // Simple default DAG if definition not yet loaded over wire
        DagDefinition dag = new DagDefinition();
        dag.getNodes().add(createNode("node-1", "Invoice Intake", com.taskforge.common.model.TaskType.CSV_VALIDATE));
        dag.getNodes().add(createNode("node-2", "Process & Transform", com.taskforge.common.model.TaskType.DATA_TRANSFORM));
        dag.getNodes().add(createNode("node-3", "Send Summary", com.taskforge.common.model.TaskType.SEND_EMAIL));

        executionEngine.startExecution(execution, dag);
    }

    private DagDefinition.DagNode createNode(String id, String name, com.taskforge.common.model.TaskType type) {
        DagDefinition.DagNode n = new DagDefinition.DagNode();
        n.setId(id);
        n.setName(name);
        n.setType(type);
        return n;
    }
}
