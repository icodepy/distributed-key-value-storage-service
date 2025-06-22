package com.dockerizedsb.kvstore.controller;

import com.dockerizedsb.kvstore.model.KVModel;
import com.dockerizedsb.kvstore.service.IClusterForwarder;
import com.dockerizedsb.kvstore.service.IKVStoreService;
import com.dockerizedsb.kvstore.service.impl.KVStoreServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/kv")
public class KVController {

    @Autowired
    private IClusterForwarder clusterForwarder;

    @PostMapping("/{key}")
    public ResponseEntity<String> put(@PathVariable String key, @RequestBody String value,
                                      @RequestHeader(value = "X-FORWARDED", required = false) String forwarded) {
        if (forwarded == null) {
            // Initial request from client, do replication
            clusterForwarder.put(key, value);
        } else {
            // Already forwarded, store only
            clusterForwarder.put(key, value);
        }
        return ResponseEntity.status(HttpStatus.CREATED).body("Key Saved!");
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> get(@PathVariable String key) {
        String value = clusterForwarder.get(key);
        if (value == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.status(HttpStatus.FOUND).body("Value="+value);
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<String> delete(@PathVariable String key) {
        clusterForwarder.remove(key);
        return ResponseEntity.status(HttpStatus.OK).body("Deleted!");
    }

    @GetMapping("")
    public ResponseEntity<String> testHealth() {
        return ResponseEntity.status(HttpStatus.OK).body("Healthy!");
    }
}

