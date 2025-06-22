package com.dockerizedsb.kvstore.registry;

import com.dockerizedsb.kvstore.component.ConsistentHashingRing;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Slf4j
public class NodeRegistry {

    @Value("${kvstore.nodes}")
    private String kvStoreNodes;

    @Autowired
    private ConsistentHashingRing cHashRing;

    @Getter
    private Set<String> allNodes;

    @Getter
    private String selfNode; // the node on which the request has fallen

    @PostConstruct
    public void init() {
        log.info("Initializing NodeRegistry with kvStoreNodes= {}", kvStoreNodes);
        allNodes = Arrays.stream(kvStoreNodes.split(","))
                .map(String::trim)
                .collect(Collectors.toSet());
        selfNode = resolveSelfNodeId();
        log.info("Self node: {}", selfNode);
        cHashRing.updateRing(allNodes);
    }

    public String getResponsibleNodeId(String key){
        return cHashRing.getPhysicalNodeForKey(key);
    }

    public  Set<String> getReplicasForKey(String key, Integer count) {
        return cHashRing.getReplicas(key, count);
    }


    private String resolveSelfNodeId(){
        log.info("Resolving self node");
        String envNodeId = System.getenv("SELF_NODE_ID");
        if(!Objects.isNull(envNodeId) && kvStoreNodes.contains(envNodeId)  ){
            return envNodeId;
        }
        try{
            String hostName = InetAddress.getLocalHost().getHostName();
            for(String node : allNodes){
                if(node.contains(hostName)){
                    return node;
                }
            }
        }catch (UnknownHostException e){
            log.error("Error resolving self node id", e);
            throw new RuntimeException("Error resolving self node id");
        }

        //Default fallback
        return allNodes.iterator().next();

    }



}
