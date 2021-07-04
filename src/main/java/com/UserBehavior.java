package com;

import lombok.Data;

@Data
public class UserBehavior {
    private int user_id;

    public UserBehavior(int user_id) {
        this.user_id = user_id;
    }
}
