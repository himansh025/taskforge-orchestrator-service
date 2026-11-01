package com.taskforge.common.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskCompletedData {
    private String executionId;
    private String taskExecutionId;
    private String nodeId;
    private int attempt;
    private Map<String, Object> output;
    @Builder.Default
    private Instant completedAt = Instant.now();
}
