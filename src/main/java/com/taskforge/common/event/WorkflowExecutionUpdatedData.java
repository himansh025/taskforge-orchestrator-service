package com.taskforge.common.event;

import com.taskforge.common.model.ExecutionStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowExecutionUpdatedData {
    private String executionId;
    private String workflowId;
    private ExecutionStatus status;
    private int completedNodes;
    private int totalNodes;
    @Builder.Default
    private Instant updatedAt = Instant.now();
}
