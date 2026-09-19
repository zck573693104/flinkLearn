package com.bigdata.cep.api.repository;

import com.bigdata.cep.api.model.CepRuleEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * CEP 规则数据访问层
 */
@Repository
public interface CepRuleRepository extends JpaRepository<CepRuleEntity, Long> {
    
    /**
     * 根据规则 ID 查询
     */
    Optional<CepRuleEntity> findByRuleId(String ruleId);
    
    /**
     * 查询所有启用的规则
     */
    List<CepRuleEntity> findByEnabledTrue();
    
    /**
     * 检查规则 ID 是否存在
     */
    boolean existsByRuleId(String ruleId);
    
    /**
     * 根据规则名称模糊查询
     */
    List<CepRuleEntity> findByRuleNameContaining(String name);
}
