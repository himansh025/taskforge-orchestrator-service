package com.taskforge.orchestrator.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;

@Entity
@Table(name = "engine_inbox")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EngineInboxEntity {

    @Id
    @Column(name = "event_id")
    private String eventId;

    @Column(name = "consumer_group", nullable = false)
    private String consumerGroup;

    @CreationTimestamp
    @Column(name = "processed_at", nullable = false, updatable = false)
    private Instant processedAt;
}
