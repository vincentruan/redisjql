package org.sqlfans.redisjql.cache.redis;

import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveZSetOperations;
import org.springframework.data.redis.core.ReactiveSetOperations;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Range;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.sqlfans.redisjql.cache.CacheOperationService;

/**
 * Lettuce实现的缓存操作服务
 * 用于处理缓存的读写操作
 *
 * @author vincentruan
 * @version 1.0.0
 */
public class LettuceCacheOperationService implements CacheOperationService {
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final ReactiveZSetOperations<String, String> zSetOps;
    private final ReactiveSetOperations<String, String> setOps;
    private final ReactiveHashOperations<String, String, String> hashOps;
    private final ReactiveValueOperations<String, String> valueOps;
    
    @Autowired
    public LettuceCacheOperationService(ReactiveRedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.zSetOps = redisTemplate.opsForZSet();
        this.setOps = redisTemplate.opsForSet();
        this.hashOps = redisTemplate.opsForHash();
        this.valueOps = redisTemplate.opsForValue();
    }
    
    @Override
    public void addIndexRecord(String indexKey, String primaryKey, double score) {
        zSetOps.add(indexKey, primaryKey, score).block();
    }
    
    @Override
    public void addPrimaryKeyToIndexMapping(String tableName, String primaryKey, String indexKey) {
        String key = tableName + "_" + primaryKey;
        setOps.add(key, indexKey).block();
    }
    
    @Override
    public Set<String> queryPrimaryKeysByIndex(String indexKey, long start, long end) {
        return zSetOps.range(indexKey, 
                 Range.closed(start, end < 0 ? Long.MAX_VALUE : end))
            .collectList()
            .map(HashSet::new)
            .block();
    }
    
    @Override
    public void markForDeletion(String tableName, String primaryKey) {
        String key = tableName + "_" + primaryKey;
        valueOps.set(key + "_deleted", "1").block();
        // 添加0-600秒的随机值，避免集中过期
        long expireTime = 600L + (long)(Math.random() * 600);
        redisTemplate.expire(key + "_deleted", Duration.ofSeconds(expireTime)).block();
    }
    
    @Override
    public void cleanupMarkedRecords() {
        // 使用SCAN代替KEYS获取删除标记
        Set<String> keysToDelete = new HashSet<>();
        redisTemplate.scan(ScanOptions.scanOptions().match("*_deleted").build())
                .doOnNext(keysToDelete::add)
                .collectList()
                .block();
        
        for (String key : keysToDelete) {
            // 获取原始key（去掉_deleted后缀）
            String originalKey = key.substring(0, key.length() - 8);
            
            // 删除原始记录
            redisTemplate.delete(originalKey).block();
            
            // 如果是主键映射，还需要删除相关的索引记录
            if (originalKey.contains("_")) {
                String[] parts = originalKey.split("_", 2);
                // String tableName = parts[0];
                String primaryKey = parts[1];
                
                // 获取并删除该主键对应的所有索引记录
                Set<String> indexKeys = setOps.members(originalKey)
                        .collectList()
                        .map(HashSet::new)
                        .block();
                
                if (indexKeys != null) {
                    for (String indexKey : indexKeys) {
                        zSetOps.remove(indexKey, primaryKey).block();
                    }
                }
            }
            
            // 删除删除标记
            redisTemplate.delete(key).block();
        }
    }
    
    @Override
    public void removeIndexRecord(String indexKey, String primaryKey) {
        zSetOps.remove(indexKey, primaryKey).block();
    }
    
    @Override
    public Set<String> getPrimaryKeyMappings(String tableName, String primaryKey) {
        String key = tableName + "_" + primaryKey;
        return setOps.members(key)
                .collectList()
                .map(HashSet::new)
                .block();
    }
    
    @Override
    public void addDataField(String dataKey, String fieldName, String fieldValue) {
        hashOps.put(dataKey, fieldName, fieldValue).block();
    }
    
    @Override
    public String getFieldValue(String dataKey, String fieldName) {
        return hashOps.get(dataKey, fieldName).block();
    }
    
    @Override
    public Set<String> getAllKeys(String pattern) {
        return redisTemplate.scan(ScanOptions.scanOptions().match(pattern).build())
                .collectList()
                .map(HashSet::new)
                .block();
    }
}

