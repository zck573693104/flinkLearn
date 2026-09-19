package com.bigdata.cep.api.service;

import com.bigdata.cep.api.model.CepRuleEntity;
import com.bigdata.cep.api.repository.CepRuleRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * CEP 规则服务类
 */
@Service
@Transactional
public class CepRuleService {
    
    @Autowired
    private CepRuleRepository ruleRepository;
    
    /**
     * 保存规则
     */
    public CepRuleEntity saveRule(CepRuleEntity rule) {
        return ruleRepository.save(rule);
    }
    
    /**
     * 根据 ID 查询规则
     */
    public Optional<CepRuleEntity> getRuleById(Long id) {
        return ruleRepository.findById(id);
    }
    
    /**
     * 根据规则 ID 查询
     */
    public Optional<CepRuleEntity> getRuleByRuleId(String ruleId) {
        return ruleRepository.findByRuleId(ruleId);
    }
    
    /**
     * 获取所有规则
     */
    public List<CepRuleEntity> getAllRules() {
        return ruleRepository.findAll();
    }
    
    /**
     * 获取所有启用的规则
     */
    public List<CepRuleEntity> getEnabledRules() {
        return ruleRepository.findByEnabledTrue();
    }
    
    /**
     * 删除规则
     */
    public void deleteRuleById(Long id) {
        ruleRepository.deleteById(id);
    }
    
    /**
     * 检查规则 ID 是否存在
     */
    public boolean ruleIdExists(String ruleId) {
        return ruleRepository.existsByRuleId(ruleId);
    }
    
    /**
     * 根据名称搜索规则
     */
    public List<CepRuleEntity> searchRulesByName(String name) {
        return ruleRepository.findByRuleNameContaining(name);
    }
}
