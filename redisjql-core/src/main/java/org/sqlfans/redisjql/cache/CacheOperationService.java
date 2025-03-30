package org.sqlfans.redisjql.cache;

import java.util.Set;

/**
 * 缓存操作服务接口
 * 用于处理缓存的读写操作
 *
 * @author vincentruan
 * @version 1.0.0
 */
public interface CacheOperationService {
    /**
     * 添加索引记录（KV类型一）
     * @param indexKey 索引键
     * @param primaryKey 主键值
     * @param score 排序分数
     */
    void addIndexRecord(String indexKey, String primaryKey, double score);
    
    /**
     * 添加主键到索引的映射（KV类型二）
     * @param tableName 表名
     * @param primaryKey 主键值
     * @param indexKey 索引键
     */
    void addPrimaryKeyToIndexMapping(String tableName, String primaryKey, String indexKey);
    
    /**
     * 根据索引键查询主键列表
     * @param indexKey 索引键
     * @param start 起始位置
     * @param end 结束位置
     * @return 主键列表
     */
    Set<String> queryPrimaryKeysByIndex(String indexKey, long start, long end);
    
    /**
     * 标记删除记录
     * @param tableName 表名
     * @param primaryKey 主键值
     */
    void markForDeletion(String tableName, String primaryKey);
    
    /**
     * 清理标记为删除的记录
     */
    void cleanupMarkedRecords();
    
    /**
     * 移除索引记录
     */
    void removeIndexRecord(String indexKey, String primaryKey);
    
    /**
     * 获取主键对应的索引键集合
     */
    Set<String> getPrimaryKeyMappings(String tableName, String primaryKey);
    
    /**
     * 添加数据字段
     */
    void addDataField(String dataKey, String fieldName, String fieldValue);
    
    /**
     * 获取指定数据键的字段值
     * @param dataKey 数据键
     * @param fieldName 字段名
     * @return 字段值
     */
    String getFieldValue(String dataKey, String fieldName);
    
    /**
     * 获取匹配模式的所有键
     * @param pattern 匹配模式
     * @return 键集合
     */
    Set<String> getAllKeys(String pattern);
}
