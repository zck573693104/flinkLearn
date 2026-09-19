package com.bigdata.cep.api.service;

import com.bigdata.cep.core.CepEvent;
import com.bigdata.cep.core.CepRuleLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Map;

/**
 * 告警通知服务
 * 负责告警的发送和推送
 */
@Service
public class AlertNotificationService {
    
    @Autowired
    private SimpMessagingTemplate messagingTemplate;
    
    @Autowired
    private CepRuleLoader ruleLoader;
    
    /**
     * 发送告警通知
     */
    public void sendAlert(String ruleId, String ruleName, CepEvent event) {
        AlertRecord record = new AlertRecord(ruleId, ruleName, event);
        
        // 1. WebSocket 推送给前端
        messagingTemplate.convertAndSend("/topic/alerts", record);
        
        // 2. 记录日志
        System.out.println("🚨 ALERT TRIGGERED:");
        System.out.println("   Rule ID: " + ruleId);
        System.out.println("   Rule Name: " + ruleName);
        System.out.println("   Event: " + event.getData());
        System.out.println("   Time: " + new Date(record.getTimestamp()));
    }
    
    /**
     * 广播系统消息
     */
    public void broadcastMessage(String message) {
        messagingTemplate.convertAndSend("/topic/system", Map.of(
            "message", message,
            "timestamp", System.currentTimeMillis()
        ));
    }
    
    /**
     * 更新规则状态
     */
    public void updateRuleStatus(String ruleId, boolean enabled) {
        messagingTemplate.convertAndSend("/topic/rules/status", Map.of(
            "ruleId", ruleId,
            "enabled", enabled,
            "timestamp", System.currentTimeMillis()
        ));
    }
    
    // 内部类：告警记录
    public static class AlertRecord {
        private String id;
        private String ruleId;
        private String ruleName;
        private String eventType;
        private String eventData;
        private long timestamp;
        private String status;
        
        public AlertRecord(String ruleId, String ruleName, CepEvent event) {
            this.ruleId = ruleId;
            this.ruleName = ruleName;
            this.eventType = event.getEventType();
            this.eventData = event.getData();
            this.timestamp = System.currentTimeMillis();
            this.status = "triggered";
            this.id = "alert_" + System.currentTimeMillis();
        }
        
        // Getters and Setters
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        
        public String getRuleId() { return ruleId; }
        public void setRuleId(String ruleId) { this.ruleId = ruleId; }
        
        public String getRuleName() { return ruleName; }
        public void setRuleName(String ruleName) { this.ruleName = ruleName; }
        
        public String getEventType() { return eventType; }
        public void setEventType(String eventType) { this.eventType = eventType; }
        
        public String getEventData() { return eventData; }
        public void setEventData(String eventData) { this.eventData = eventData; }
        
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
    }
}
