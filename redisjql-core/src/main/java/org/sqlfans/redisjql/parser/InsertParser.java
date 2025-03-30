package org.sqlfans.redisjql.parser;

import net.sf.jsqlparser.statement.insert.Insert;

import java.util.Map;

/**
 * Insert语句解析器接口
 */
public interface InsertParser {
    /**
     * 解析Insert语句
     * @param insert Insert语句
     * @return 解析结果
     */
    Integer parse(Insert insert);
    
    /**
     * 判断是否可以使用Redis缓存
     * @param insert Insert语句
     * @return 是否可以使用Redis缓存
     */
    boolean canUseRedisCache(Insert insert);
    
    /**
     * 记录缓存未命中日志
     * @param insert Insert语句
     * @param reason 未命中原因
     */
    void logCacheMiss(Insert insert, String reason);
    
    /**
     * 提取字段和值
     * @param insert Insert语句
     * @return 字段和值的映射
     */
    Map<String, Object> extractColumnsAndValues(Insert insert);
    
    /**
     * 获取生成的主键值
     * @param insert Insert语句
     * @param generatedKey 生成的主键
     * @return 主键值
     */
    String getGeneratedPrimaryKey(Insert insert, Object generatedKey);
    
    /**
     * 更新Redis索引
     * @param tableName 表名
     * @param primaryKey 主键值
     * @param columnValues 字段和值的映射
     * @return 更新的索引数量
     */
    int updateRedisIndices(String tableName, String primaryKey, Map<String, Object> columnValues);
}
