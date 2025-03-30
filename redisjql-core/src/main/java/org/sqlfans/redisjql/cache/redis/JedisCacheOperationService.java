package org.sqlfans.redisjql.cache.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.sqlfans.redisjql.cache.CacheOperationService;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Jedis实现的缓存操作服务
 * 用于处理缓存的读写操作
 *
 * @author vincentruan
 * @version 1.0.0
 */
public class JedisCacheOperationService implements CacheOperationService {
    private JedisPool jedisPool;
    
    @Autowired
    public JedisCacheOperationService(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }
    
    @Override
    public void addIndexRecord(String indexKey, String primaryKey, double score) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, Double> scoreMembers = new HashMap<>();
            scoreMembers.put(primaryKey, score);
            jedis.zadd(indexKey, scoreMembers);
        }
    }
    
    @Override
    public void addPrimaryKeyToIndexMapping(String tableName, String primaryKey, String indexKey) {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = tableName + "_" + primaryKey;
            jedis.sadd(key, indexKey);
        }
    }
    
    @Override
    public Set<String> queryPrimaryKeysByIndex(String indexKey, long start, long end) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.zrange(indexKey, start, end);
        }
    }
    
    @Override
    public void markForDeletion(String tableName, String primaryKey) {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = tableName + "_" + primaryKey;
            jedis.set(key + "_deleted", "1");
            // 添加0-600秒的随机值，避免集中过期
            long expireTime = 600L + (long)(Math.random() * 600);
            jedis.expire(key + "_deleted", expireTime);
        }
    }
    
    @Override
    public void cleanupMarkedRecords() {
        try (Jedis jedis = jedisPool.getResource()) {
            // 获取所有标记为删除的记录
            Set<String> keys = jedis.keys("*_deleted");
            for (String key : keys) {
                // 获取原始key（去掉_deleted后缀）
                String originalKey = key.substring(0, key.length() - 8);
                
                // 删除原始记录
                jedis.del(originalKey);
                
                // 如果是主键映射，还需要删除相关的索引记录
                if (originalKey.contains("_")) {
                    String[] parts = originalKey.split("_", 2);
                    // String tableName = parts[0];
                    String primaryKey = parts[1];
                    
                    // 获取并删除该主键对应的所有索引记录
                    Set<String> indexKeys = jedis.smembers(originalKey);
                    for (String indexKey : indexKeys) {
                        jedis.zrem(indexKey, primaryKey);
                    }
                }
                
                // 删除删除标记
                jedis.del(key);
            }
        }
    }
    
    @Override
    public void removeIndexRecord(String indexKey, String primaryKey) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.zrem(indexKey, primaryKey);
        }
    }
    
    @Override
    public Set<String> getPrimaryKeyMappings(String tableName, String primaryKey) {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = tableName + "_" + primaryKey;
            return jedis.smembers(key);
        }
    }
    
    @Override
    public void addDataField(String dataKey, String fieldName, String fieldValue) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hset(dataKey, fieldName, fieldValue);
        }
    }
    
    @Override
    public String getFieldValue(String dataKey, String fieldName) {
        try (Jedis jedis = jedisPool.getResource()) {
            // 使用hash结构存储字段值
            return jedis.hget(dataKey, fieldName);
        }
    }
    
    @Override
    public Set<String> getAllKeys(String pattern) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.keys(pattern);
        }
    }
}
