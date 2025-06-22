package com.dockerizedsb.kvstore.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static com.dockerizedsb.kvstore.constant.Constants.*;



@Component
@Slf4j
public class ConsistentHashingRing {

    @Autowired
    private RedisTemplate redisTemplate;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;


    /**
     * Add a physical node to the hash ring with virtual nodes
     */
    public void addNode(String nodeName){
        for(int i = 0; i < VIRTUAL_NODE_COUNT; i++){
            String nodeKey = nodeName + "#" +i;
            double virtualNodeHash = hashSHA256(nodeKey);
            redisTemplate.opsForZSet().add(RING_KEY, nodeKey, virtualNodeHash);
        }
    }

    public void updateRing(Set<String> nodes){
        log.info("Updating ring with "+nodes.size()+" nodes");
        redisTemplate.delete(RING_KEY);
        log.info("Deleted the hashRing to build a newer one");
        for(String nodeName : nodes){
            addNode(nodeName);
        }
        log.info("Ring updated");
    }

    /*
     Get the physical node responsible for a given key
     */
    public String getPhysicalNodeForKey(String key){
        double keyHash = hashSHA256(key);
        log.info("1.find the next node clockwise in the ring");
        Set<String> tailNodes = redisTemplate.opsForZSet().rangeByScore(RING_KEY, keyHash, Double.MAX_VALUE,0,1);

        if(!Objects.isNull(tailNodes) && !tailNodes.isEmpty()){
            String node = tailNodes.iterator().next();
            log.info("Node found={}",node);
            return node.split("#")[0];
        }

        log.info("2. Wrap-around (if key hash > all node hashes");
        Set<String> headNodes = redisTemplate.opsForZSet().range(RING_KEY, 0,0);
        if(!Objects.isNull(headNodes) && !headNodes.isEmpty()){
            String node = headNodes.iterator().next();
            log.info("Node found={}",node);
            return node.split("#")[0];
        }

        log.info("3. No nodes present");
        throw new IllegalStateException("No nodes present in the hash ring.");
    }

    /*
      Remove a physical node (all its virtual nodes) from the ring
     */
    public void removeNode(String key){
        for(int i = 0; i < VIRTUAL_NODE_COUNT; i++){
            String nodeKey = key + "#" +i;
            double virtualNodeHash = hashSHA256(nodeKey);
            redisTemplate.opsForZSet().removeRangeByScore(RING_KEY, virtualNodeHash, virtualNodeHash);
        }
    }

    /*
        Returns given number of replicas
     */
    public Set<String> getReplicas(String key, Integer count){
        log.info("get Replicas.");
        Set<String> replicas = new HashSet<>();

        double hashKey = hashSHA256(key);
        log.info("hashKey: " + hashKey);

        Set<String> nodes = redisTemplate.opsForZSet().rangeByScore(RING_KEY, hashKey, Double.MAX_VALUE,0,count);
        log.info("getReplicas::found nodes: " + nodes);
        if(Objects.isNull(nodes) || nodes.isEmpty()){
            nodes = redisTemplate.opsForZSet().range(RING_KEY, 0,-1);
        }
        for(String virtualNode : nodes){
            String node = virtualNode.split("#")[0];
            if(!replicas.contains(node)){
                replicas.add(node);
            }
            if(replicas.size() >= count){
                return replicas;
            }
        }
        log.info("getReplicas::found {} nodes: {}",count, nodes);
        return replicas;
    }

    public double hashSHA256(String key){
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(key.getBytes(StandardCharsets.UTF_8));

            long longHash = ByteBuffer.wrap(hash).getLong();
            return  (double) ( longHash & 0x7FFFFFFFFFFFFFFFL);
            /*
             0x → hexadecimal prefix
             7FFFFFFFFFFFFFFF → a 63-bit number where all bits are 1 except the sign bit (MSB)
             L → means it's a long literal (64-bit signed integer in Java)

             0x7FFFFFFFFFFFFFFF = 9,223,372,036,854,775,807 (i.e., Long.MAX_VALUE)

             When we convert the first 8 bytes of the SHA-256 hash to a long:
             long value = ByteBuffer.wrap(hash).getLong();
               The result could be:
                positive (if the first bit is 0)
                negative (if the first bit is 1 — since Java long is signed)
             We want: A positive number only (for consistent hashing)
                      Because Redis Sorted Set scores must be non-negative
                      (or at least predictable)


            */
        }catch (Exception e){
            throw new RuntimeException("Hashing failed", e);
        }
    }
}
