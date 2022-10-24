package com.malf;

public class StaticGeneric {
    public static <T> void setName(T name){
        System.out.println(name);
    }

    public static <T> T getName(T name){
        return name;
    }

    public static void main(String[] args) {
        StaticGeneric.setName("科比");
        Integer username = StaticGeneric.<Integer>getName(1);
        System.out.println(username);
    }
}
