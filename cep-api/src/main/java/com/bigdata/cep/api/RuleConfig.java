package com.bigdata.cep.api;

/**
 * 规则配置类
 */
public class RuleConfig {
    private String ruleId;
    private String ruleName;
    private String patternJson;
    private boolean enabled;
    
    public String getRuleId() {
        return ruleId;
    }
    
    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }
    
    public String getRuleName() {
        return ruleName;
    }
    
    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }
    
    public String getPatternJson() {
        return patternJson;
    }
    
    public void setPatternJson(String patternJson) {
        this.patternJson = patternJson;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
