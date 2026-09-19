package com.bigdata.cep.api.repository;

import com.bigdata.cep.api.model.AlertRecordEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 告警记录数据访问层
 */
@Repository
public interface AlertRecordRepository extends JpaRepository<AlertRecordEntity, Long> {
    
    /**
     * 根据规则 ID 查询告警记录
     */
    Page<AlertRecordEntity> findByRuleId(String ruleId, Pageable pageable);
    
    /**
     * 根据状态查询告警记录
     */
    List<AlertRecordEntity> findByStatus(String status);
    
    /**
     * 查询最近的告警记录
     */
    List<AlertRecordEntity> findTop10ByOrderByTimestampDesc();
    
    /**
     * 统计某个规则的告警数量
     */
    long countByRuleIdAndStatus(String ruleId, String status);
}
