package org.sqlfans.redisjql.parser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.Select;

import java.util.List;

/**
 * Select语句解析器接口
 */
public interface SelectParser {
    /**
     * 解析Select语句
     * @param select Select语句
     * @return 解析结果
     */
    Object parse(Select select);
    
    /**
     * 判断是否可以使用Redis缓存
     * @param select Select语句
     * @return 是否可以使用Redis缓存
     */
    boolean canUseRedisCache(Select select);
    
    /**
     * 记录缓存未命中日志
     * @param select Select语句
     * @param reason 未命中原因
     */
    void logCacheMiss(Select select, String reason);
    
    /**
     * 提取WHERE条件
     * @param select Select语句
     * @return WHERE条件表达式
     */
    Expression extractWhereCondition(Select select);
    
    /**
     * 提取ORDER BY子句
     * @param select Select语句
     * @return ORDER BY元素列表
     */
    List<OrderByElement> extractOrderByClause(Select select);
    
    /**
     * 提取GROUP BY子句
     * @param select Select语句
     * @return GROUP BY表达式列表
     */
    List<Expression> extractGroupByClause(Select select);
    
    /**
     * 使用Redis索引查询
     * @param tableName 表名
     * @param whereCondition WHERE条件
     * @param orderByElements ORDER BY元素列表
     * @param groupByExpressions GROUP BY表达式列表
     * @return 查询结果
     */
    Object queryWithRedisIndex(String tableName, Expression whereCondition, 
                              List<OrderByElement> orderByElements, 
                              List<Expression> groupByExpressions);
    
    /**
     * 重写Select SQL
     * @param originalSql 原始SQL
     * @param primaryKeys 主键列表
     * @return 重写后的SQL
     */
    String rewriteSelectSql(String originalSql, List<String> primaryKeys);
}
