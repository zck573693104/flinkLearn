package org.fuwushe.sideoutput;

import lombok.Data;

import java.io.Serializable;

@Data
public class OrderEvent implements Serializable {

    private String userId;
    private String type;

    public OrderEvent(String userId, String type) {
        this.userId = userId;
        this.type = type;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "userId='" + userId + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}