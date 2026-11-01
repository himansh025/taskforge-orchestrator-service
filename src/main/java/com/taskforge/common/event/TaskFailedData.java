package com.taskforge.common.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskFailedData {
    private String executionId;
    private String taskExecutionId;
    private String nodeId;
    private int attempt;
    private String errorCode;
    private String errorMessage;
    private boolean retryable;
    @Builder.Default
    private Instant failedAt = Instant.now();
}
