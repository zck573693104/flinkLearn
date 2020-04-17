package org.fuwushe.drools;

import lombok.Data;

@Data
public class Person {
    public Person() {

    }

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String name;
    public int age;
    //false 过滤
    public boolean whetherFilter = false;


}