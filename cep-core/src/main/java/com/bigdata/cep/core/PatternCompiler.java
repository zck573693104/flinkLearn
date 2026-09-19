package com.bigdata.cep.core;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

/**
 * Pattern 编译器
 * 负责将 JSON 格式的 Pattern 定义编译为 Flink CEP Pattern 对象
 */
public class PatternCompiler {
    
    /**
     * 编译 Pattern
     * 
     * @param patternJson JSON 格式的 Pattern 定义
     * @return Pattern 对象
     */
    public Pattern<CepEvent, ?> compile(String patternJson) throws Exception {
        JsonNode rootNode = parseJson(patternJson);
        
        String name = getNodeString(rootNode, "name", "default_pattern");
        int times = getNodeInt(rootNode, "times", -1);
        int timesMin = getNodeInt(rootNode, "timesMin", -1);
        
        Pattern.Builder<CepEvent, ?> builder = Pattern.<CepEvent>named(name);
        
        if (times >= 0) {
            builder.times(times);
        }
        if (timesMin >= 0) {
            builder.timesMin(timesMin);
        }
        
        // 解析条件
        JsonNode ofNode = rootNode.get("of");
        if (ofNode != null) {
            SimpleCondition<CepEvent> condition = buildCondition(ofNode);
            builder.of(condition);
        }
        
        // 解析 where 子句
        JsonNode whereNode = rootNode.get("where");
        if (whereNode != null) {
            String type = getNodeString(whereNode, "type", "strict");
            long duration = getNodeLong(whereNode, "duration", 0);
            String unit = getNodeString(whereNode, "unit", "seconds");
            
            if ("strict".equals(type)) {
                builder.where(buildWhereClause(duration, unit));
            } else if ("next".equals(type)) {
                builder.next(buildWhereClause(duration, unit));
            }
        }
        
        return builder.allowNestedBefore(null).build();
    }
    
    private JsonNode parseJson(String json) throws Exception {
        return new com.fasterxml.jackson.databind.ObjectMapper().readTree(json);
    }
    
    private String getNodeString(JsonNode node, String field, String defaultValue) {
        JsonNode value = node.get(field);
        return value != null ? value.asText() : defaultValue;
    }
    
    private int getNodeInt(JsonNode node, String field, int defaultValue) {
        JsonNode value = node.get(field);
        return value != null && !value.isNull() ? value.asInt() : defaultValue;
    }
    
    private long getNodeLong(JsonNode node, String field, long defaultValue) {
        JsonNode value = node.get(field);
        return value != null && !value.isNull() ? value.asLong() : defaultValue;
    }
    
    private SimpleCondition<CepEvent> buildCondition(JsonNode ofNode) {
        JsonNode firstNode = ofNode.get("first");
        if (firstNode == null) {
            return new SimpleCondition<CepEvent>() {
                @Override
                public boolean eval(CepEvent event, org.apache.flink.cep.TimeTimestampExtractor timestampExtractor) throws Exception {
                    return true;
                }
            };
        }
        
        String type = getNodeString(firstNode, "type", "simple");
        JsonNode conditionNode = firstNode.get("condition");
        
        if ("simple".equals(type) && conditionNode != null) {
            return buildSimpleCondition(conditionNode);
        }
        
        return new SimpleCondition<CepEvent>() {
            @Override
            public boolean eval(CepEvent event, org.apache.flink.cep.TimeTimestampExtractor timestampExtractor) throws Exception {
                return true;
            }
        };
    }
    
    private SimpleCondition<CepEvent> buildSimpleCondition(JsonNode conditionNode) {
        String field = getNodeString(conditionNode, "field", "");
        String operator = getNodeString(conditionNode, "operator", "==");
        Object value = getValue(conditionNode, "value");
        
        return new SimpleCondition<CepEvent>() {
            @Override
            public boolean eval(CepEvent event, org.apache.flink.cep.TimeTimestampExtractor timestampExtractor) throws Exception {
                try {
                    java.util.Map<String, Object> eventData = parseEventData(event);
                    Object fieldValue = eventData.get(field);
                    
                    if (fieldValue == null) {
                        return false;
                    }
                    
                    switch (operator) {
                        case ">":
                            return compareGreater(fieldValue, value);
                        case "<":
                            return compareLess(fieldValue, value);
                        case ">=":
                            return compareGreaterOrEqual(fieldValue, value);
                        case "<=":
                            return compareLessOrEqual(fieldValue, value);
                        case "==":
                        case "=":
                            return equals(fieldValue, value);
                        case "!=":
                            return !equals(fieldValue, value);
                        default:
                            return false;
                    }
                } catch (Exception e) {
                    return false;
                }
            }
        };
    }
    
    private org.apache.flink.cep.pattern.conditions.SimpleCondition<CepEvent> buildWhereClause(long duration, String unit) {
        long millis = convertToMillis(duration, unit);
        return new org.apache.flink.cep.pattern.conditions.SimpleCondition<CepEvent>() {
            @Override
            public boolean eval(CepEvent event, org.apache.flink.cep.TimeTimestampExtractor timestampExtractor) throws Exception {
                return true;
            }
        };
    }
    
    private long convertToMillis(long duration, String unit) {
        switch (unit.toLowerCase()) {
            case "seconds":
                return duration * 1000;
            case "minutes":
                return duration * 60 * 1000;
            case "hours":
                return duration * 60 * 60 * 1000;
            default:
                return duration * 1000;
        }
    }
    
    private java.util.Map<String, Object> parseEventData(CepEvent event) throws Exception {
        String data = event.getData();
        if (data == null || data.isEmpty()) {
            return new java.util.HashMap<>();
        }
        
        return new com.fasterxml.jackson.databind.ObjectMapper().readValue(data, java.util.Map.class);
    }
    
    private boolean compareGreater(Object fieldValue, Object value) {
        if (fieldValue instanceof Number && value instanceof Number) {
            double f = ((Number) fieldValue).doubleValue();
            double v = ((Number) value).doubleValue();
            return f > v;
        }
        return false;
    }
    
    private boolean compareLess(Object fieldValue, Object value) {
        if (fieldValue instanceof Number && value instanceof Number) {
            double f = ((Number) fieldValue).doubleValue();
            double v = ((Number) value).doubleValue();
            return f < v;
        }
        return false;
    }
    
    private boolean compareGreaterOrEqual(Object fieldValue, Object value) {
        if (fieldValue instanceof Number && value instanceof Number) {
            double f = ((Number) fieldValue).doubleValue();
            double v = ((Number) value).doubleValue();
            return f >= v;
        }
        return false;
    }
    
    private boolean compareLessOrEqual(Object fieldValue, Object value) {
        if (fieldValue instanceof Number && value instanceof Number) {
            double f = ((Number) fieldValue).doubleValue();
            double v = ((Number) value).doubleValue();
            return f <= v;
        }
        return false;
    }
    
    private boolean equals(Object fieldValue, Object value) {
        if (fieldValue == null && value == null) {
            return true;
        }
        if (fieldValue == null || value == null) {
            return false;
        }
        return fieldValue.equals(value);
    }
    
    private Object getValue(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null) {
            return null;
        }
        
        if (value.isNumber()) {
            return value.asDouble();
        } else if (value.isBoolean()) {
            return value.asBoolean();
        } else if (value.isTextual()) {
            return value.asText();
        }
        return value.asText();
    }
}
