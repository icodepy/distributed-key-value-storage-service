package com.dockerizedsb.kvstore.service.impl;

import com.dockerizedsb.kvstore.service.IKVStoreService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;


@Service
public class KVStoreServiceImpl implements IKVStoreService {

    @Autowired
    private RedisTemplate redisTemplate;

    @Override
    public void putIfAbsent(String key, String value) {
        redisTemplate.opsForValue().set(key, value);
    }

    @Override
    public String get(String key) {
        return (String)redisTemplate.opsForValue().get(key);
    }

    @Override
    public void remove(String key) {
        redisTemplate.opsForValue().getOperations().delete(key);
    }
}
