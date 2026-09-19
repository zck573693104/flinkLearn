package com.bigdata.cep.core;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CEP 规则加载器
 * 负责动态加载和管理 CEP 规则
 */
public class CepRuleLoader {
    
    private final StreamExecutionEnvironment env;
    private final Map<String, PatternStream> patternStreams;
    private final Map<String, Pattern<CepEvent, ?>> compiledPatterns;
    private final PatternCompiler patternCompiler;
    
    public CepRuleLoader(StreamExecutionEnvironment env) {
        this.env = env;
        this.patternStreams = new ConcurrentHashMap<>();
        this.compiledPatterns = new ConcurrentHashMap<>();
        this.patternCompiler = new PatternCompiler();
    }
    
    /**
     * 加载 CEP 规则到引擎
     * 
     * @param ruleId 规则 ID
     * @param ruleName 规则名称
     * @param patternJson JSON 格式的 Pattern 定义
     * @param inputStream 输入数据流
     * @return PatternStream
     */
    public PatternStream loadRule(String ruleId, String ruleName, String patternJson, DataStream<CepEvent> inputStream) throws Exception {
        // 编译 Pattern
        Pattern<CepEvent, ?> pattern = patternCompiler.compile(patternJson);
        compiledPatterns.put(ruleId, pattern);
        
        // 创建 PatternStream
        PatternStream patternStream = CEP.pattern(inputStream, pattern);
        
        // 注册到映射中
        patternStreams.put(ruleId, patternStream);
        
        System.out.println("Successfully loaded CEP rule: " + ruleName + " (ID: " + ruleId + ")");
        
        return patternStream;
    }
    
    /**
     * 更新规则（热更新）
     */
    public void updateRule(String ruleId, String patternJson) throws Exception {
        // 停止旧规则
        stopRule(ruleId);
        
        // TODO: 重新加载新规则需要更复杂的流处理逻辑
        // 这里仅从映射中移除，实际项目中需要重建整个流处理拓扑
        System.out.println("Rule updated: " + ruleId);
    }
    
    /**
     * 删除规则
     */
    public void removeRule(String ruleId) {
        stopRule(ruleId);
        patternStreams.remove(ruleId);
        compiledPatterns.remove(ruleId);
        System.out.println("Rule removed: " + ruleId);
    }
    
    /**
     * 停止规则
     */
    private void stopRule(String ruleId) {
        PatternStream stream = patternStreams.get(ruleId);
        if (stream != null) {
            // TODO: 在实际应用中，需要正确关闭流处理
            // 这里仅为占位符实现
            patternStreams.remove(ruleId);
        }
    }
    
    /**
     * 获取所有活跃的规则
     */
    public Map<String, PatternStream> getAllRules() {
        return new ConcurrentHashMap<>(patternStreams);
    }
    
    /**
     * 检查规则是否存在
     */
    public boolean hasRule(String ruleId) {
        return patternStreams.containsKey(ruleId);
    }
    
    /**
     * 获取规则数量
     */
    public int getRuleCount() {
        return patternStreams.size();
    }
    
    /**
     * 清空所有规则
     */
    public void clearAllRules() {
        for (String ruleId : patternStreams.keySet()) {
            stopRule(ruleId);
        }
        patternStreams.clear();
        compiledPatterns.clear();
        System.out.println("All rules cleared");
    }
}
