package com.taskforge.orchestrator.engine;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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
        private String type;
        private String name;
        private Map<String, Object> data;
        private Map<String, Object> config;

        public TaskType getResolvedTaskType() {
            // 1. Check data.taskType
            if (data != null && data.get("taskType") != null) {
                try {
                    return TaskType.valueOf(data.get("taskType").toString().toUpperCase());
                } catch (Exception ignored) {}
            }
            // 2. Check direct type
            if (type != null) {
                try {
                    return TaskType.valueOf(type.toUpperCase());
                } catch (Exception ignored) {}
            }
            // 3. Infer from label or name
            String label = "";
            if (data != null && data.get("label") != null) {
                label = data.get("label").toString().toLowerCase();
            } else if (name != null) {
                label = name.toLowerCase();
            }

            if (label.contains("csv") || label.contains("intake") || label.contains("validate")) {
                return TaskType.CSV_VALIDATE;
            } else if (label.contains("threshold") || label.contains("check") || label.contains("condition")) {
                return TaskType.CONDITION;
            } else if (label.contains("report") || label.contains("summary")) {
                return TaskType.GENERATE_REPORT;
            } else if (label.contains("email") || label.contains("dispatch") || label.contains("notify")) {
                return TaskType.SEND_EMAIL;
            }
            return TaskType.DATA_TRANSFORM;
        }

        public String getResolvedName() {
            if (data != null && data.get("label") != null) {
                return data.get("label").toString();
            }
            return name != null ? name : id;
        }

        public Map<String, Object> getResolvedConfig() {
            if (config != null) return config;
            if (data != null && data.get("config") instanceof Map) {
                return (Map<String, Object>) data.get("config");
            }
            return Map.of();
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DagEdge {
        private String id;
        private String source;
        private String target;
        private String from;
        private String to;
        private String label;
        private String conditionValue;

        public String getEffectiveSource() {
            return source != null ? source : from;
        }

        public String getEffectiveTarget() {
            return target != null ? target : to;
        }
    }
}
