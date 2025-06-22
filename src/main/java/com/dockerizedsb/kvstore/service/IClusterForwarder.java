package com.dockerizedsb.kvstore.service;

public interface IClusterForwarder {
    void put(String key, String value);
    String get(String key);
    void remove(String key);
}
