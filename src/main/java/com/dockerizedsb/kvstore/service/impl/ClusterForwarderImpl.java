package com.dockerizedsb.kvstore.service.impl;

import com.dockerizedsb.kvstore.registry.NodeRegistry;
import com.dockerizedsb.kvstore.service.IClusterForwarder;
import com.dockerizedsb.kvstore.service.IKVStoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Set;

import static com.dockerizedsb.kvstore.constant.Constants.*;

@Service
@Slf4j
public class ClusterForwarderImpl implements IClusterForwarder {

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private NodeRegistry nodeRegistry;

    @Autowired
    private IKVStoreService kvStoreService;


    @Override
    public void put(String key, String value) {
        Set<String> nodes = nodeRegistry.getReplicasForKey(key, REPLICATION_FACTOR);
        for(String replica : nodes) {
            if(replica.equals(nodeRegistry.getSelfNode())) {
                kvStoreService.putIfAbsent(key, value);
            }else{
                try{
                    log.info("forwarding in the post request to the other app instances");
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.TEXT_PLAIN);
                    headers.add("X-FORWARDED", "true");

                    HttpEntity<String> entity = new HttpEntity<>(value, headers);


                    restTemplate.exchange(
                            replica+"/kv/"+key,
                            HttpMethod.POST,
                            entity,
                            Void.class
                    );
                }catch (Exception e){
                    log.error("Failed to PUT to node:{}", replica, e);
                }
            }
        }
    }

    @Override
    public String get(String key) {
        String node = nodeRegistry.getResponsibleNodeId(key);
        log.info("node found={}", node);
        log.info("SelfNode={}", nodeRegistry.getSelfNode());
        if(node.equals(nodeRegistry.getSelfNode())) {
            return kvStoreService.get(key);
        }else {
            try {
                log.info("calling the end-point={}",node + "/kv/" + key);
                return restTemplate.getForEntity(
                        node + "/kv/" + key,
                        String.class
                ).getBody();
            }catch (Exception e) {
                log.error("GET from primary failed, falling back.");
                return "Not Found";
            }
        }
    }

    @Override
    public void remove(String key) {
        Set<String> replicas = nodeRegistry.getReplicasForKey(key, REPLICATION_FACTOR);
        for (String node : replicas) {
            if (node.equals(nodeRegistry.getSelfNode())) {
                kvStoreService.remove(key);
            } else {
                try {
                    restTemplate.delete(node + "/kv/" + key);
                } catch (Exception e) {
                    System.err.println("Failed to DELETE on node: " + node);
                }
            }
        }
    }
}
