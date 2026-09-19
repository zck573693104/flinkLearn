package com.bigdata.cep.api;

import com.bigdata.cep.core.CepRuleLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * CEP 规则 REST API 控制器
 */
@RestController
@RequestMapping("/api/cep/rules")
public class CepRuleController {
    
    @Autowired
    private CepRuleLoader ruleLoader;
    
    /**
     * 获取所有活跃的 CEP 规则
     */
    @GetMapping
    public Map<String, Object> getAllRules() {
        return ruleLoader.getAllRules();
    }
    
    /**
     * 加载新的 CEP 规则
     */
    @PostMapping
    public String createRule(@RequestBody RuleConfig config) {
        try {
            // TODO: 实现规则加载逻辑
            return "Rule created successfully";
        } catch (Exception e) {
            return "Failed to create rule: " + e.getMessage();
        }
    }
    
    /**
     * 更新现有规则
     */
    @PutMapping("/{ruleId}")
    public String updateRule(@PathVariable String ruleId, @RequestBody RuleConfig config) {
        try {
            // TODO: 实现规则更新逻辑
            return "Rule updated successfully";
        } catch (Exception e) {
            return "Failed to update rule: " + e.getMessage();
        }
    }
    
    /**
     * 删除规则
     */
    @DeleteMapping("/{ruleId}")
    public String deleteRule(@PathVariable String ruleId) {
        try {
            ruleLoader.removeRule(ruleId);
            return "Rule deleted successfully";
        } catch (Exception e) {
            return "Failed to delete rule: " + e.getMessage();
        }
    }
}
