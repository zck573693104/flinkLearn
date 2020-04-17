package com.flink.jdbc;

import lombok.Data;

@Data
public class User {
    public String id;
    public String username;
    public String password;
    public String phone;

    public User() {
    }



}
