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

        DagDefinition dag;
        String definitionJson = data.getDefinitionJson();
        if (definitionJson != null && !definitionJson.isBlank()) {
            dag = objectMapper.readValue(definitionJson, DagDefinition.class);
        } else {
            dag = new DagDefinition();
            DagDefinition.DagNode n1 = new DagDefinition.DagNode();
            n1.setId("node-1");
            n1.setName("CSV Intake");
            n1.setType("CSV_VALIDATE");
            DagDefinition.DagNode n2 = new DagDefinition.DagNode();
            n2.setId("node-2");
            n2.setName("Data Transform");
            n2.setType("DATA_TRANSFORM");
            dag.getNodes().add(n1);
            dag.getNodes().add(n2);
            DagDefinition.DagEdge edge = new DagDefinition.DagEdge();
            edge.setFrom("node-1");
            edge.setTo("node-2");
            dag.getEdges().add(edge);
            definitionJson = objectMapper.writeValueAsString(dag);
        }

        ExecutionEntity execution = ExecutionEntity.builder()
                .id(executionId)
                .workspaceId(UUID.fromString(envelope.getWorkspaceId()))
                .workflowId(UUID.fromString(data.getWorkflowId()))
                .workflowVersionId(UUID.fromString(data.getWorkflowVersionId()))
                .status(ExecutionStatus.PENDING)
                .triggerType(data.getTriggerType())
                .inputPayload(objectMapper.writeValueAsString(data.getInput()))
                .definitionPayload(definitionJson)
                .startedAt(Instant.now())
                .build();

        executionRepository.save(execution);
        inboxRepository.save(EngineInboxEntity.builder()
                .eventId(envelope.getEventId())
                .consumerGroup("orchestrator-service-group")
                .build());

        log.info("Starting DAG execution for executionId={}, total nodes={}", executionId, dag.getNodes().size());
        executionEngine.startExecution(execution, dag);
    }
}
