package org.sqlfans.redisjql.parser.impl;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlfans.redisjql.cache.CacheOperationService;
import org.sqlfans.redisjql.config.IndexConfig;
import org.sqlfans.redisjql.parser.UpdateParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Update语句解析器实现类
 */
public class UpdateParserImpl implements UpdateParser {
    private static final Logger logger = LoggerFactory.getLogger(UpdateParserImpl.class);
    
    private CacheOperationService redisOperationService;
    private List<IndexConfig> indexConfigs;
    
    public UpdateParserImpl(CacheOperationService redisOperationService, List<IndexConfig> indexConfigs) {
        this.redisOperationService = redisOperationService;
        this.indexConfigs = indexConfigs;
    }
    
    @Override
    public Integer parse(Update update) {
        if (!canUseRedisCache(update)) {
            logCacheMiss(update, "Redis cache conditions not met");
            return 0;
        }
        
        String tableName = update.getTable().getName();
        IndexConfig indexConfig = findIndexConfig(tableName);
        
        if (indexConfig == null) {
            logCacheMiss(update, "Index configuration not found for table " + tableName);
            return 0;
        }
        
        // 判断WHERE条件是否满足要求
        if (!isValidWhereCondition(update, indexConfig)) {
            throw new IllegalArgumentException("WHERE condition in UPDATE statement is neither primary key condition nor satisfies Redis index condition");
        }
        
        // 提取主键值
        String primaryKey = extractPrimaryKeyFromWhere(update, indexConfig.getPrimaryKey());
        if (primaryKey == null) {
            logCacheMiss(update, "Failed to extract primary key from WHERE condition");
            return 0;
        }
        
        // 提取SET子句内容
        Map<String, Object> columnValues = extractSetClause(update);
        if (columnValues.isEmpty()) {
            logCacheMiss(update, "SET clause is empty");
            return 0;
        }
        
        // 更新Redis索引
        return updateRedisIndices(tableName, primaryKey, columnValues);
    }
    
    @Override
    public boolean canUseRedisCache(Update update) {
        if (update == null || update.getWhere() == null) {
            return false;
        }
        
        String tableName = update.getTable().getName();
        IndexConfig indexConfig = findIndexConfig(tableName);
        
        return indexConfig != null;
    }
    
    @Override
    public void logCacheMiss(Update update, String reason) {
        logger.debug("Redis cache miss: {} - {}", update, reason);
    }
    
    @Override
    public Map<String, Object> extractSetClause(Update update) {
        Map<String, Object> result = new HashMap<>();
        
        List<net.sf.jsqlparser.statement.update.UpdateSet> updateSets = update.getUpdateSets();
        
        if (updateSets == null || updateSets.isEmpty()) {
            return result;
        }
        
        for (net.sf.jsqlparser.statement.update.UpdateSet updateSet : updateSets) {
            if (updateSet.getColumns().size() == 1 && updateSet.getExpressions().size() == 1) {
                String columnName = updateSet.getColumns().get(0).getColumnName();
                Object value = updateSet.getExpressions().get(0).toString();
                // 去除值中的引号
                if (value instanceof String) {
                    String strValue = (String) value;
                    if (strValue.startsWith("'") && strValue.endsWith("'")) {
                        value = strValue.substring(1, strValue.length() - 1);
                    }
                }
                result.put(columnName, value);
            }
        }
        
        return result;
    }
    
    @Override
    public Expression extractWhereCondition(Update update) {
        return update.getWhere();
    }
    
    @Override
    public boolean isValidWhereCondition(Update update, Object indexConfig) {
        if (update.getWhere() == null || !(indexConfig instanceof IndexConfig)) {
            return false;
        }
        
        IndexConfig config = (IndexConfig) indexConfig;
        String primaryKeyName = config.getPrimaryKey();
        
        // 检查是否是主键条件
        if (containsFieldCondition(update.getWhere(), primaryKeyName)) {
            return true;
        }
        
        // 检查是否满足索引条件
        for (IndexConfig.IndexDefinition indexDef : config.getIndexes()) {
            for (String fieldName : indexDef.getFields()) {
                if (containsFieldCondition(update.getWhere(), fieldName)) {
                    return true;
                }
            }
        }
        
        return false;
    }
    
    @Override
    public int updateRedisIndices(String tableName, String primaryKey, Map<String, Object> columnValues) {
        IndexConfig indexConfig = findIndexConfig(tableName);
        if (indexConfig == null) {
            return 0;
        }
        
        int updatedIndices = 0;
        
        try {
            // 更新主键到值的映射
            String dataKey = tableName + ":" + primaryKey;
            
            // 检查版本字段
            String versionField = indexConfig.getVersionField();
            if (columnValues.containsKey(versionField)) {
                // 如果包含版本字段，需要先检查版本是否匹配
                Object newVersion = columnValues.get(versionField);
                // 获取当前版本号
                String currentVersion = redisOperationService.getFieldValue(dataKey, versionField);
                
                if (currentVersion != null) {
                    // 检查版本号是否匹配预期
                    // 预期新版本通常是表达式如 "jpa_version + 1"，需要解析其值
                    String expectedOldVersion = null;
                    
                    if (newVersion.toString().contains("+")) {
                        // 提取加号前的部分作为字段名
                        expectedOldVersion = newVersion.toString().split("\\+")[0].trim();
                        
                        // 如果是字段引用而不是具体值
                        if (expectedOldVersion.equals(versionField)) {
                            // 版本号递增，不需额外处理
                        } else {
                            // 使用具体值比较
                            if (!currentVersion.equals(expectedOldVersion)) {
                                logger.warn("Version check failed: current={}, expected={}", currentVersion, expectedOldVersion);
                                return 0; // 版本不匹配，不执行更新
                            }
                        }
                    } else {
                        // 如果是直接设置新版本号，可能需要业务层自行确保一致性
                        logger.info("Setting version directly to: {}", newVersion);
                    }
                }
            }
            
            // 将数据存储到Redis
            for (Map.Entry<String, Object> entry : columnValues.entrySet()) {
                redisOperationService.addDataField(dataKey, entry.getKey(), entry.getValue().toString());
            }
            updatedIndices++;
            
            // 更新索引
            for (IndexConfig.IndexDefinition indexDef : indexConfig.getIndexes()) {
                for (String fieldName : indexDef.getFields()) {
                    if (columnValues.containsKey(fieldName)) {
                        // 如果更新了索引字段，需要先清理旧索引
                        // 获取旧值
                        // String mappingKey = tableName + "_" + primaryKey;

                        String oldIndexKey = tableName + ":" + fieldName + ":" + columnValues.get(fieldName);
                        
                        // 从旧索引中移除记录
                        try {
                            // 如果有removeIndexRecord方法，可以使用它
                            // redisOperationService.removeIndexRecord(oldIndexKey, primaryKey);
                            // 否则使用zrem命令
                            redisOperationService.addIndexRecord(oldIndexKey, primaryKey, -1); // 使用负分数标记为删除
                        } catch (Exception ex) {
                            logger.warn("Failed to clean old index: {}", ex.getMessage());
                        }
                        
                        // 如果更新了索引字段，需要更新索引
                        Object fieldValue = columnValues.get(fieldName);
                        String indexKey = tableName + ":" + fieldName + ":" + fieldValue;
                        
                        // 添加到索引
                        redisOperationService.addIndexRecord(indexKey, primaryKey, 0);
                        
                        // 添加主键到索引的映射
                        redisOperationService.addPrimaryKeyToIndexMapping(tableName, primaryKey, indexKey);
                        
                        updatedIndices++;
                    }
                }
            }
            
            return updatedIndices;
        } catch (Exception e) {
            logger.error("Failed to update Redis indices: {}", e.getMessage(), e);
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
     * @param update Update语句对象
     * @param primaryKeyName 主键字段名
     * @return 主键值
     */
    private String extractPrimaryKeyFromWhere(Update update, String primaryKeyName) {
        if (update.getWhere() == null) {
            return null;
        }
        
        return extractValueFromCondition(update.getWhere(), primaryKeyName);
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