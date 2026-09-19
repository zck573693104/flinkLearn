package com.bigdata.cep.core;

import java.io.Serializable;
import java.util.Date;

/**
 * CEP 事件基类
 */
public class CepEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /**
     * 事件 ID
     */
    private String eventId;
    
    /**
     * 事件类型
     */
    private String eventType;
    
    /**
     * 事件数据（JSON 格式）
     */
    private String data;
    
    /**
     * 时间戳
     */
    private Long timestamp;
    
    /**
     * 创建时间
     */
    private Date createdAt;
    
    public CepEvent() {
        this.timestamp = System.currentTimeMillis();
        this.createdAt = new Date();
    }
    
    public String getEventId() {
        return eventId;
    }
    
    public void setEventId(String eventId) {
        this.eventId = eventId;
    }
    
    public String getEventType() {
        return eventType;
    }
    
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
    
    public String getData() {
        return data;
    }
    
    public void setData(String data) {
        this.data = data;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    public Date getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }
}
