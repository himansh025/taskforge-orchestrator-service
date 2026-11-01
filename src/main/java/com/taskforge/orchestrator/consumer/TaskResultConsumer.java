package com.taskforge.orchestrator.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.common.event.EventEnvelope;
import com.taskforge.common.event.TaskCompletedData;
import com.taskforge.common.event.TaskFailedData;
import com.taskforge.orchestrator.engine.DagExecutionEngine;
import com.taskforge.orchestrator.entity.EngineInboxEntity;
import com.taskforge.orchestrator.repository.EngineInboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class TaskResultConsumer {

    private final ObjectMapper objectMapper;
    private final EngineInboxRepository inboxRepository;
    private final DagExecutionEngine executionEngine;

    @KafkaListener(topics = "${taskforge.kafka.topics.task-completed:task.completed.v1}", groupId = "orchestrator-service-group")
    @Transactional
    @SneakyThrows
    public void onTaskCompleted(String message) {
        EventEnvelope<TaskCompletedData> envelope = objectMapper.readValue(message, new TypeReference<>() {});
        if (inboxRepository.existsById(envelope.getEventId())) return;

        TaskCompletedData data = envelope.getData();
        executionEngine.handleTaskCompleted(
                UUID.fromString(data.getExecutionId()),
                data.getNodeId(),
                data.getOutput()
        );

        inboxRepository.save(EngineInboxEntity.builder()
                .eventId(envelope.getEventId())
                .consumerGroup("orchestrator-service-group")
                .build());
    }

    @KafkaListener(topics = "${taskforge.kafka.topics.task-failed:task.failed.v1}", groupId = "orchestrator-service-group")
    @Transactional
    @SneakyThrows
    public void onTaskFailed(String message) {
        EventEnvelope<TaskFailedData> envelope = objectMapper.readValue(message, new TypeReference<>() {});
        if (inboxRepository.existsById(envelope.getEventId())) return;

        TaskFailedData data = envelope.getData();
        executionEngine.handleTaskFailed(
                UUID.fromString(data.getExecutionId()),
                data.getNodeId(),
                data.getErrorMessage(),
                data.isRetryable()
        );

        inboxRepository.save(EngineInboxEntity.builder()
                .eventId(envelope.getEventId())
                .consumerGroup("orchestrator-service-group")
                .build());
    }
}
