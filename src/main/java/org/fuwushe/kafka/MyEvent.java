package org.fuwushe.kafka;

import lombok.Data;

import java.io.Serializable;
@Data
public class MyEvent implements Serializable {

    private static final long serialVersionUID = 4293717541649215493L;

    private long CreationTime;
    private String name;

}
