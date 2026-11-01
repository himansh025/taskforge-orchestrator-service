package com.taskforge.orchestrator.engine;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.taskforge.common.model.TaskType;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DagDefinition {
    private List<DagNode> nodes = new ArrayList<>();
    private List<DagEdge> edges = new ArrayList<>();

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DagNode {
        private String id;
        private TaskType type;
        private String name;
        private Map<String, Object> config;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DagEdge {
        private String from;
        private String to;
        private String conditionValue;
    }
}
