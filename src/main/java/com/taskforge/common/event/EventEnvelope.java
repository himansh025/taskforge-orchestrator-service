package com.taskforge.common.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventEnvelope<T> {
    @Builder.Default
    private String eventId = UUID.randomUUID().toString();
    private String eventType;
    @Builder.Default
    private int schemaVersion = 1;
    @Builder.Default
    private Instant occurredAt = Instant.now();
    private String workspaceId;
    private String correlationId;
    private String causationId;
    private String producer;
    private T data;
}
