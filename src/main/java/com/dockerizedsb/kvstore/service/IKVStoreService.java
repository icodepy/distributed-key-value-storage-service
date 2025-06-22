package com.dockerizedsb.kvstore.service;

public interface IKVStoreService {


    void putIfAbsent(String key, String value);
    String get(String key);
    void remove(String key);
}
