package org.sqlfans.redisjql.parser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.update.Update;

import java.util.Map;

/**
 * Update语句解析器接口
 */
public interface UpdateParser {
    /**
     * 解析Update语句
     * @param update Update语句
     * @return 解析结果
     */
    Integer parse(Update update);
    
    /**
     * 判断是否可以使用Redis缓存
     * @param update Update语句
     * @return 是否可以使用Redis缓存
     */
    boolean canUseRedisCache(Update update);
    
    /**
     * 记录缓存未命中日志
     * @param update Update语句
     * @param reason 未命中原因
     */
    void logCacheMiss(Update update, String reason);
    
    /**
     * 提取SET子句内容
     * @param update Update语句
     * @return 字段和值的映射
     */
    Map<String, Object> extractSetClause(Update update);
    
    /**
     * 提取WHERE条件
     * @param update Update语句
     * @return WHERE条件表达式
     */
    Expression extractWhereCondition(Update update);
    
    /**
     * 判断WHERE条件是否有效
     * @param update Update语句
     * @param indexConfig 索引配置
     * @return 是否有效
     */
    boolean isValidWhereCondition(Update update, Object indexConfig);
    
    /**
     * 更新Redis索引
     * @param tableName 表名
     * @param primaryKey 主键值
     * @param columnValues 字段和值的映射
     * @return 更新的索引数量
     */
    int updateRedisIndices(String tableName, String primaryKey, Map<String, Object> columnValues);
}
