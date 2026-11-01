package com.taskforge.common.event;

import com.taskforge.common.model.TaskType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskRequestedData {
    private String executionId;
    private String workflowId;
    private String taskExecutionId;
    private String nodeId;
    private TaskType taskType;
    private int attempt;
    private Map<String, Object> input;
    private int timeoutSeconds;
}
