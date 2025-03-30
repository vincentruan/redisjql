package org.sqlfans.redisjql.parser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.delete.Delete;

/**
 * Delete语句解析器接口
 */
public interface DeleteParser {
    /**
     * 解析Delete语句
     * @param delete Delete语句
     * @return 解析结果
     */
    Integer parse(Delete delete);
    
    /**
     * 判断是否可以使用Redis缓存
     * @param delete Delete语句
     * @return 是否可以使用Redis缓存
     */
    boolean canUseRedisCache(Delete delete);
    
    /**
     * 记录缓存未命中日志
     * @param delete Delete语句
     * @param reason 未命中原因
     */
    void logCacheMiss(Delete delete, String reason);
    
    /**
     * 提取WHERE条件
     * @param delete Delete语句
     * @return WHERE条件表达式
     */
    Expression extractWhereCondition(Delete delete);
    
    /**
     * 判断WHERE条件是否有效
     * @param delete Delete语句
     * @param indexConfig 索引配置
     * @return 是否有效
     */
    boolean isValidWhereCondition(Delete delete, Object indexConfig);
    
    /**
     * 标记删除Redis数据
     * @param tableName 表名
     * @param primaryKey 主键值
     * @return 标记删除的记录数量
     */
    int markForDeletion(String tableName, String primaryKey);
}
