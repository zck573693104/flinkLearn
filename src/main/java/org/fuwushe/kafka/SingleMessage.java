package org.fuwushe.kafka;

import lombok.Data;

@Data
public class SingleMessage {

    private long timeLong;
    private String name;
    private String bizID;
    private String time;
    private String message;


}
