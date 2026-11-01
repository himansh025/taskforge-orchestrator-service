package com.taskforge.orchestrator.publisher;

import com.taskforge.orchestrator.entity.EngineOutboxEntity;
import com.taskforge.orchestrator.repository.EngineOutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class EngineOutboxPublisher {

    private final EngineOutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPendingEvents() {
        List<EngineOutboxEntity> pending = outboxRepository.findByStatusOrderByCreatedAtAsc("PENDING", PageRequest.of(0, 50));
        for (EngineOutboxEntity event : pending) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload()).get();
                event.setStatus("PROCESSED");
                event.setProcessedAt(Instant.now());
                log.info("Orchestrator dispatched outbox event {} to topic {}", event.getId(), event.getTopic());
            } catch (Exception e) {
                event.setRetryCount(event.getRetryCount() + 1);
                if (event.getRetryCount() >= 5) {
                    event.setStatus("FAILED");
                }
                log.error("Failed to publish engine outbox event {}: {}", event.getId(), e.getMessage());
            }
            outboxRepository.save(event);
        }
    }
}
