package org.sqlfans.redisjql.cache.caffine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.sqlfans.redisjql.cache.CacheOperationService;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Caffeine本地缓存实现
 *
 * @author vincentruan
 * @version 1.0.0
 */
public class CaffeineCacheOperationService implements CacheOperationService {
    // 有序集合缓存 - 模拟Redis的ZSET
    private final Cache<String, ConcurrentSkipListMap<Double, Set<String>>> sortedSetCache;
    
    // 集合缓存 - 模拟Redis的SET
    private final Cache<String, Set<String>> setCache;
    
    // 哈希表缓存 - 模拟Redis的HASH
    private final Cache<String, Map<String, String>> hashCache;
    
    // 键值缓存 - 模拟Redis的STRING
    private final Cache<String, String> valueCache;
    
    // 删除标记的过期时间
    private final Map<String, Long> expirations = new ConcurrentHashMap<>();
    
    public CaffeineCacheOperationService() {
        this.sortedSetCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build();
        
        this.setCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build();
        
        this.hashCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build();
                
        this.valueCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build();
    }
    
    @Override
    public void addIndexRecord(String indexKey, String primaryKey, double score) {
        ConcurrentSkipListMap<Double, Set<String>> zset = sortedSetCache.get(indexKey, k -> new ConcurrentSkipListMap<>());
        zset.computeIfAbsent(score, k -> ConcurrentHashMap.newKeySet()).add(primaryKey);
    }
    
    @Override
    public void addPrimaryKeyToIndexMapping(String tableName, String primaryKey, String indexKey) {
        String key = tableName + "_" + primaryKey;
        Set<String> set = setCache.get(key, k -> ConcurrentHashMap.newKeySet());
        set.add(indexKey);
    }
    
    @Override
    public Set<String> queryPrimaryKeysByIndex(String indexKey, long start, long end) {
        ConcurrentSkipListMap<Double, Set<String>> zset = sortedSetCache.getIfPresent(indexKey);
        if (zset == null) {
            return Collections.emptySet();
        }
        
        // 收集所有成员
        List<String> allMembers = new ArrayList<>();
        for (Set<String> members : zset.values()) {
            allMembers.addAll(members);
        }
        
        // 应用范围限制
        int fromIndex = (int) Math.max(0, start);
        int toIndex = (int) Math.min(allMembers.size(), end < 0 ? allMembers.size() : end + 1);
        
        if (fromIndex >= toIndex) {
            return Collections.emptySet();
        }
        
        return new HashSet<>(allMembers.subList(fromIndex, toIndex));
    }
    
    @Override
    public void markForDeletion(String tableName, String primaryKey) {
        String key = tableName + "_" + primaryKey;
        valueCache.put(key + "_deleted", "1");
        // 记录过期时间
        expirations.put(key + "_deleted", System.currentTimeMillis() + 600 * 1000);
    }
    
    @Override
    public void cleanupMarkedRecords() {
        // 获取当前时间
        long now = System.currentTimeMillis();
        
        // 查找未过期的删除标记
        List<String> keysToProcess = expirations.entrySet().stream()
                .filter(entry -> entry.getValue() > now)  // 只处理未过期的
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        
        for (String key : keysToProcess) {
            // 获取原始key（去掉_deleted后缀）
            String originalKey = key.substring(0, key.length() - 8);
            
            // 删除原始记录
            setCache.invalidate(originalKey);
            
            // 如果是主键映射，还需要删除相关的索引记录
            if (originalKey.contains("_")) {
                String[] parts = originalKey.split("_", 2);
                // String tableName = parts[0];
                String primaryKey = parts[1];
                
                // 获取该主键对应的所有索引记录
                Set<String> indexKeys = Optional.ofNullable(setCache.getIfPresent(originalKey))
                        .orElse(Collections.emptySet());
                
                // 从所有相关索引中删除该主键
                for (String indexKey : indexKeys) {
                    removeIndexRecord(indexKey, primaryKey);
                }
            }
            
            // 删除过期标记
            valueCache.invalidate(key);
            expirations.remove(key);
        }
    }
    
    @Override
    public void removeIndexRecord(String indexKey, String primaryKey) {
        ConcurrentSkipListMap<Double, Set<String>> zset = sortedSetCache.getIfPresent(indexKey);
        if (zset != null) {
            // 在所有分数集合中查找并删除主键
            for (Set<String> members : zset.values()) {
                members.remove(primaryKey);
            }
            
            // 清理空集合
            zset.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        }
    }
    
    @Override
    public Set<String> getPrimaryKeyMappings(String tableName, String primaryKey) {
        String key = tableName + "_" + primaryKey;
        return Optional.ofNullable(setCache.getIfPresent(key))
                .orElse(Collections.emptySet());
    }
    
    @Override
    public void addDataField(String dataKey, String fieldName, String fieldValue) {
        Map<String, String> hash = hashCache.get(dataKey, k -> new ConcurrentHashMap<>());
        hash.put(fieldName, fieldValue);
    }
    
    @Override
    public String getFieldValue(String dataKey, String fieldName) {
        Map<String, String> hash = hashCache.getIfPresent(dataKey);
        return hash != null ? hash.get(fieldName) : null;
    }
    
    @Override
    public Set<String> getAllKeys(String pattern) {
        // 简易模式匹配实现 
        Set<String> result = new HashSet<>();
        
        // 将正则表达式模式转换为Java正则表达式
        String regex = pattern.replace("*", ".*").replace("?", ".");
        
        // 从所有缓存中收集匹配的键
        sortedSetCache.asMap().keySet().stream()
                .filter(key -> key.matches(regex))
                .forEach(result::add);
        
        setCache.asMap().keySet().stream()
                .filter(key -> key.matches(regex))
                .forEach(result::add);
        
        hashCache.asMap().keySet().stream()
                .filter(key -> key.matches(regex))
                .forEach(result::add);
        
        valueCache.asMap().keySet().stream()
                .filter(key -> key.matches(regex))
                .forEach(result::add);
        
        return result;
    }
}
