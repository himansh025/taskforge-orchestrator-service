package com.taskforge.common.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowExecutionRequestedData {
    private String executionId;
    private String workflowId;
    private String workflowVersionId;
    private String triggerType;
    private Map<String, Object> input;
    private String definitionHash;
}
