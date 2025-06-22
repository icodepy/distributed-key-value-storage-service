package com.dockerizedsb.kvstore.lua;

public class PutIfAbsent {

    public final static String PUT_IF_ABSENT = "if redis.call (\"exists\", KEYS[1]) == 0 then " +
            "redis.call(\"set\", KEYS[1], ARGS[1]) " +
            "return 1" +
            "else return 0" +
            "end";
}
