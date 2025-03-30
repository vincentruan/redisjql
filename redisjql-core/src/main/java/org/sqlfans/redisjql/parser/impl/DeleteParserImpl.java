package org.sqlfans.redisjql.parser.impl;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlfans.redisjql.cache.CacheOperationService;
import org.sqlfans.redisjql.config.IndexConfig;
import org.sqlfans.redisjql.parser.DeleteParser;

import java.util.List;
import java.util.Set;

/**
 * Delete语句解析器实现类
 */
public class DeleteParserImpl implements DeleteParser {
    private static final Logger logger = LoggerFactory.getLogger(DeleteParserImpl.class);
    
    private CacheOperationService redisOperationService;
    private List<IndexConfig> indexConfigs;
    
    public DeleteParserImpl(CacheOperationService redisOperationService, List<IndexConfig> indexConfigs) {
        this.redisOperationService = redisOperationService;
        this.indexConfigs = indexConfigs;
    }
    
    @Override
    public Integer parse(Delete delete) {
        if (!canUseRedisCache(delete)) {
            logCacheMiss(delete, "Redis cache conditions not met");
            return 0;
        }
        
        String tableName = delete.getTable().getName();
        IndexConfig indexConfig = findIndexConfig(tableName);
        
        if (indexConfig == null) {
            logCacheMiss(delete, "Index configuration not found for table " + tableName);
            return 0;
        }
        
        // 判断WHERE条件是否满足要求
        if (!isValidWhereCondition(delete, indexConfig)) {
            throw new IllegalArgumentException("WHERE condition in DELETE statement is neither primary key condition nor satisfies Redis index condition");
        }
        
        // 提取主键值
        String primaryKey = extractPrimaryKeyFromWhere(delete, indexConfig.getPrimaryKey());
        
        // 如果是主键删除
        if (primaryKey != null) {
            return markForDeletion(tableName, primaryKey);
        }
        
        // 如果是索引字段删除
        for (IndexConfig.IndexDefinition indexDef : indexConfig.getIndexes()) {
            for (String fieldName : indexDef.getFields()) {
                String fieldValue = extractValueFromCondition(delete.getWhere(), fieldName);
                if (fieldValue != null) {
                    // 根据索引查找主键
                    String indexKey = tableName + ":" + fieldName + ":" + fieldValue;
                    Set<String> primaryKeys = redisOperationService.queryPrimaryKeysByIndex(indexKey, 0, -1);
                    
                    int deletedCount = 0;
                    for (String pk : primaryKeys) {
                        deletedCount += markForDeletion(tableName, pk);
                    }
                    
                    return deletedCount;
                }
            }
        }
        
        // 如果都不满足，抛出异常
        throw new IllegalArgumentException("WHERE condition in DELETE statement does not meet Redis cache conditions");
    }
    
    @Override
    public boolean canUseRedisCache(Delete delete) {
        if (delete == null || delete.getWhere() == null) {
            return false;
        }
        
        String tableName = delete.getTable().getName();
        IndexConfig indexConfig = findIndexConfig(tableName);
        
        return indexConfig != null;
    }
    
    @Override
    public void logCacheMiss(Delete delete, String reason) {
        logger.debug("Redis cache miss: {} - {}", delete, reason);
    }
    
    @Override
    public Expression extractWhereCondition(Delete delete) {
        return delete.getWhere();
    }
    
    @Override
    public boolean isValidWhereCondition(Delete delete, Object indexConfig) {
        if (delete.getWhere() == null || !(indexConfig instanceof IndexConfig)) {
            return false;
        }
        
        IndexConfig config = (IndexConfig) indexConfig;
        String primaryKeyName = config.getPrimaryKey();
        
        // 检查是否是主键条件
        if (containsFieldCondition(delete.getWhere(), primaryKeyName)) {
            return true;
        }
        
        // 检查是否满足索引条件
        for (IndexConfig.IndexDefinition indexDef : config.getIndexes()) {
            for (String fieldName : indexDef.getFields()) {
                if (containsFieldCondition(delete.getWhere(), fieldName)) {
                    return true;
                }
            }
        }
        
        return false;
    }
    
    @Override
    public int markForDeletion(String tableName, String primaryKey) {
        try {
            // 标记删除记录（通过状态字段标记，10分钟后实际删除）
            redisOperationService.markForDeletion(tableName, primaryKey);
            
            return 1;
        } catch (Exception e) {
            logger.error("Failed to mark Redis data for deletion: {}", e.getMessage(), e);
            return 0;
        }
    }
    
    /**
     * 查找表的索引配置
     * @param tableName 表名
     * @return 索引配置
     */
    private IndexConfig findIndexConfig(String tableName) {
        return indexConfigs.stream()
                .filter(config -> config.getTableName().equals(tableName))
                .findFirst()
                .orElse(null);
    }
    
    /**
     * 检查条件表达式是否包含指定字段
     * @param expression 条件表达式
     * @param fieldName 字段名
     * @return 是否包含
     */
    private boolean containsFieldCondition(Expression expression, String fieldName) {
        if (expression instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) expression;
            if (equalsTo.getLeftExpression() instanceof Column) {
                Column column = (Column) equalsTo.getLeftExpression();
                return column.getColumnName().equals(fieldName);
            }
        } else if (expression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expression;
            return containsFieldCondition(andExpression.getLeftExpression(), fieldName) ||
                   containsFieldCondition(andExpression.getRightExpression(), fieldName);
        }
        
        return false;
    }
    
    /**
     * 从WHERE条件中提取主键值
     * @param delete Delete语句对象
     * @param primaryKeyName 主键字段名
     * @return 主键值
     */
    private String extractPrimaryKeyFromWhere(Delete delete, String primaryKeyName) {
        if (delete.getWhere() == null) {
            return null;
        }
        
        return extractValueFromCondition(delete.getWhere(), primaryKeyName);
    }
    
    /**
     * 从条件表达式中提取字段值
     * @param expression 条件表达式
     * @param fieldName 字段名
     * @return 字段值
     */
    private String extractValueFromCondition(Expression expression, String fieldName) {
        if (expression instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) expression;
            if (equalsTo.getLeftExpression() instanceof Column) {
                Column column = (Column) equalsTo.getLeftExpression();
                if (column.getColumnName().equals(fieldName)) {
                    String value = equalsTo.getRightExpression().toString();
                    // 去除值中的引号
                    if (value.startsWith("'") && value.endsWith("'")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    return value;
                }
            }
        } else if (expression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expression;
            String leftValue = extractValueFromCondition(andExpression.getLeftExpression(), fieldName);
            if (leftValue != null) {
                return leftValue;
            }
            return extractValueFromCondition(andExpression.getRightExpression(), fieldName);
        }
        
        return null;
    }
}