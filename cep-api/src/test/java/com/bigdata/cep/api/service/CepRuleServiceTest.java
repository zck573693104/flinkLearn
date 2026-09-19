package com.bigdata.cep.api.service;

import com.bigdata.cep.api.model.CepRuleEntity;
import com.bigdata.cep.api.repository.CepRuleRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * CepRuleService 单元测试
 */
class CepRuleServiceTest {
    
    @Mock
    private CepRuleRepository ruleRepository;
    
    @InjectMocks
    private CepRuleService cepRuleService;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    @DisplayName("保存规则")
    void testSaveRule() {
        CepRuleEntity rule = new CepRuleEntity();
        rule.setRuleId("test_rule_001");
        rule.setRuleName("测试规则");
        
        when(ruleRepository.save(any(CepRuleEntity.class))).thenReturn(rule);
        
        CepRuleEntity saved = cepRuleService.saveRule(rule);
        
        assertNotNull(saved);
        assertEquals("test_rule_001", saved.getRuleId());
        verify(ruleRepository, times(1)).save(rule);
    }
    
    @Test
    @DisplayName("根据 ID 查询规则")
    void testGetRuleById() {
        Long id = 1L;
        CepRuleEntity rule = new CepRuleEntity();
        rule.setId(id);
        rule.setRuleId("rule_001");
        
        when(ruleRepository.findById(id)).thenReturn(Optional.of(rule));
        
        Optional<CepRuleEntity> result = cepRuleService.getRuleById(id);
        
        assertTrue(result.isPresent());
        assertEquals("rule_001", result.get().getRuleId());
        verify(ruleRepository, times(1)).findById(id);
    }
    
    @Test
    @DisplayName("根据规则 ID 查询不存在的规则")
    void testGetRuleByRuleIdNotFound() {
        when(ruleRepository.findByRuleId("nonexistent")).thenReturn(Optional.empty());
        
        Optional<CepRuleEntity> result = cepRuleService.getRuleByRuleId("nonexistent");
        
        assertFalse(result.isPresent());
        verify(ruleRepository, times(1)).findByRuleId("nonexistent");
    }
    
    @Test
    @DisplayName("获取所有规则")
    void testGetAllRules() {
        List<CepRuleEntity> rules = List.of(
            createTestRule("rule_001"),
            createTestRule("rule_002")
        );
        
        when(ruleRepository.findAll()).thenReturn(rules);
        
        List<CepRuleEntity> result = cepRuleService.getAllRules();
        
        assertEquals(2, result.size());
        verify(ruleRepository, times(1)).findAll();
    }
    
    @Test
    @DisplayName("获取启用的规则")
    void testGetEnabledRules() {
        List<CepRuleEntity> enabledRules = List.of(createTestRule("enabled_rule"));
        enabledRules.get(0).setEnabled(true);
        
        when(ruleRepository.findByEnabledTrue()).thenReturn(enabledRules);
        
        List<CepRuleEntity> result = cepRuleService.getEnabledRules();
        
        assertEquals(1, result.size());
        assertTrue(result.get(0).isEnabled());
        verify(ruleRepository, times(1)).findByEnabledTrue();
    }
    
    @Test
    @DisplayName("删除规则")
    void testDeleteRule() {
        Long id = 1L;
        
        cepRuleService.deleteRuleById(id);
        
        verify(ruleRepository, times(1)).deleteById(id);
    }
    
    @Test
    @DisplayName("检查规则 ID 是否存在")
    void testRuleIdExists() {
        when(ruleRepository.existsByRuleId("existing_rule")).thenReturn(true);
        when(ruleRepository.existsByRuleId("nonexistent_rule")).thenReturn(false);
        
        assertTrue(cepRuleService.ruleIdExists("existing_rule"));
        assertFalse(cepRuleService.ruleIdExists("nonexistent_rule"));
        
        verify(ruleRepository, times(2)).existsByRuleId(anyString());
    }
    
    @Test
    @DisplayName("根据名称搜索规则")
    void testSearchRulesByName() {
        List<CepRuleEntity> results = List.of(
            createTestRule("search_test_001")
        );
        
        when(ruleRepository.findByRuleNameContaining("search_test")).thenReturn(results);
        
        List<CepRuleEntity> result = cepRuleService.searchRulesByName("search_test");
        
        assertEquals(1, result.size());
        verify(ruleRepository, times(1)).findByRuleNameContaining("search_test");
    }
    
    private CepRuleEntity createTestRule(String ruleId) {
        CepRuleEntity entity = new CepRuleEntity();
        entity.setRuleId(ruleId);
        entity.setRuleName("Test Rule " + ruleId);
        return entity;
    }
}
