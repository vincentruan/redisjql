package org.sqlfans.redisjql.parser.impl;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlfans.redisjql.cache.CacheOperationService;
import org.sqlfans.redisjql.config.IndexConfig;
import org.sqlfans.redisjql.parser.InsertParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Insert语句解析器实现类
 */
public class InsertParserImpl implements InsertParser {
    private static final Logger logger = LoggerFactory.getLogger(InsertParserImpl.class);
    
    private CacheOperationService redisOperationService;
    private List<IndexConfig> indexConfigs;
    
    public InsertParserImpl(CacheOperationService redisOperationService, List<IndexConfig> indexConfigs) {
        this.redisOperationService = redisOperationService;
        this.indexConfigs = indexConfigs;
    }
    
    @Override
    public Integer parse(Insert insert) {
        if (!canUseRedisCache(insert)) {
            logCacheMiss(insert, "Redis cache conditions not met");
            return 0;
        }
        
        String tableName = insert.getTable().getName();
        IndexConfig indexConfig = findIndexConfig(tableName);
        
        if (indexConfig == null) {
            logCacheMiss(insert, "Index configuration not found for table " + tableName);
            return 0;
        }
        
        // 提取字段和值
        Map<String, Object> columnValues = extractColumnsAndValues(insert);
        if (columnValues.isEmpty()) {
            logCacheMiss(insert, "Failed to extract columns and values from insert statement");
            return 0;
        }
        
        // 获取主键值
        String primaryKey = (String) columnValues.get(indexConfig.getPrimaryKey());
        if (primaryKey == null) {
            // 如果是自增主键，尝试从返回的生成键中获取
            logger.debug("Primary key not found in insert statement, trying to use generated key");
            return 0; // 此处返回0，表示需要在执行SQL后再处理
        }
        
        // 更新Redis索引
        return updateRedisIndices(tableName, primaryKey, columnValues);
    }
    
    @Override
    public boolean canUseRedisCache(Insert insert) {
        if (insert == null) {
            return false;
        }
        
        String tableName = insert.getTable().getName();
        IndexConfig indexConfig = findIndexConfig(tableName);
        
        return indexConfig != null && containsVersionField(insert);
    }
    
    @Override
    public void logCacheMiss(Insert insert, String reason) {
        logger.debug("Redis cache miss: {} - {}", insert, reason);
    }
    
    @Override
    public Map<String, Object> extractColumnsAndValues(Insert insert) {
        Map<String, Object> result = new HashMap<>();
        
        List<Column> columns = insert.getColumns();
        ItemsList itemsList = insert.getItemsList();
        
        if (columns == null || columns.isEmpty()) {
            // 如果没有指定字段，尝试从表的索引配置中获取字段信息
            String tableName = insert.getTable().getName();
            IndexConfig indexConfig = findIndexConfig(tableName);
            
            if (indexConfig == null) {
                logger.warn("Index configuration not found for table {}, cannot process insert statement without specified columns", tableName);
                return result;
            }
            
            // 尝试从ItemsList中获取值
            if (itemsList instanceof ExpressionList) {
                ExpressionList expressionList = (ExpressionList) itemsList;
                List<Expression> expressions = expressionList.getExpressions();
                
                // 获取表的所有索引字段，包括主键
                List<String> allFields = new ArrayList<>();
                allFields.add(indexConfig.getPrimaryKey()); // 添加主键
                if (indexConfig.getVersionField() != null) {
                    allFields.add(indexConfig.getVersionField()); // 添加版本字段
                }
                
                // 添加所有索引字段
                for (IndexConfig.IndexDefinition indexDef : indexConfig.getIndexes()) {
                    for (String field : indexDef.getFields()) {
                        if (!allFields.contains(field)) {
                            allFields.add(field);
                        }
                    }
                }
                
                // 检查值的数量是否与字段数量匹配
                if (expressions.size() != allFields.size()) {
                    logger.warn("Number of values ({}) does not match expected number of fields ({})", expressions.size(), allFields.size());
                    return result;
                }
                
                // 将值与字段对应
                for (int i = 0; i < allFields.size(); i++) {
                    String fieldName = allFields.get(i);
                    Object value = expressions.get(i).toString();
                    // 去除值中的引号
                    if (value instanceof String) {
                        String strValue = (String) value;
                        if (strValue.startsWith("'") && strValue.endsWith("'")) {
                            value = strValue.substring(1, strValue.length() - 1);
                        }
                    }
                    result.put(fieldName, value);
                }
            }
        } else if (itemsList instanceof ExpressionList) {
            ExpressionList expressionList = (ExpressionList) itemsList;
            List<Expression> expressions = expressionList.getExpressions();
            
            if (expressions.size() != columns.size()) {
                // 字段数量和值数量不匹配
                logger.warn("Number of columns ({}) does not match number of values ({})", columns.size(), expressions.size());
                return result;
            }
            
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i).getColumnName();
                Object value = expressions.get(i).toString();
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
    public String getGeneratedPrimaryKey(Insert insert, Object generatedKey) {
        if (generatedKey == null) {
            return null;
        }
        
        // 处理不同类型的生成键
        if (generatedKey instanceof Number) {
            // 数字类型的主键（如自增ID）
            return generatedKey.toString();
        } else if (generatedKey instanceof String) {
            // 字符串类型的主键
            return (String) generatedKey;
        } else if (generatedKey instanceof List) {
            // 如果返回的是列表（某些JDBC驱动会这样），取第一个元素
            List<?> keyList = (List<?>) generatedKey;
            if (!keyList.isEmpty()) {
                return keyList.get(0).toString();
            }
        }
        
        // 其他情况，直接转为字符串
        return generatedKey.toString();
    }
    
    /**
     * 处理自增主键的情况，在SQL执行后更新Redis索引
     * @param insert Insert语句
     * @param generatedKey 生成的主键
     * @return 更新的索引数量
     */
    public Integer parseWithGeneratedKey(Insert insert, Object generatedKey) {
        if (!canUseRedisCache(insert)) {
            logCacheMiss(insert, "Redis cache conditions not met");
            return 0;
        }
        
        String tableName = insert.getTable().getName();
        IndexConfig indexConfig = findIndexConfig(tableName);
        
        if (indexConfig == null) {
            logCacheMiss(insert, "Index configuration not found for table " + tableName);
            return 0;
        }
        
        // 提取字段和值
        Map<String, Object> columnValues = extractColumnsAndValues(insert);
        if (columnValues.isEmpty()) {
            logCacheMiss(insert, "Failed to extract columns and values from insert statement");
            return 0;
        }
        
        // 获取生成的主键值
        String primaryKey = getGeneratedPrimaryKey(insert, generatedKey);
        if (primaryKey == null) {
            logCacheMiss(insert, "Failed to get generated primary key");
            return 0;
        }
        
        // 将主键值添加到字段值映射中
        columnValues.put(indexConfig.getPrimaryKey(), primaryKey);
        
        // 更新Redis索引
        return updateRedisIndices(tableName, primaryKey, columnValues);
    }
    
    @Override
    public int updateRedisIndices(String tableName, String primaryKey, Map<String, Object> columnValues) {
        IndexConfig indexConfig = findIndexConfig(tableName);
        if (indexConfig == null) {
            return 0;
        }
        
        int updatedIndices = 0;
        
        // 更新主键到值的映射
        String dataKey = tableName + ":" + primaryKey;
        try {
            // 将数据存储到Redis
            for (Map.Entry<String, Object> entry : columnValues.entrySet()) {
                redisOperationService.addDataField(dataKey, entry.getKey(), entry.getValue().toString());
            }
            updatedIndices++;
            
            // 更新索引
            for (IndexConfig.IndexDefinition indexDef : indexConfig.getIndexes()) {
                List<String> fields = indexDef.getFields();
                if (fields == null || fields.isEmpty()) {
                    continue;
                }
                
                // 处理复合索引
                if (fields.size() > 1) {
                    // 检查是否所有字段都存在于columnValues中
                    boolean allFieldsExist = true;
                    for (String field : fields) {
                        if (!columnValues.containsKey(field)) {
                            allFieldsExist = false;
                            break;
                        }
                    }
                    
                    if (allFieldsExist) {
                        // 构建复合索引键
                        StringBuilder indexKeyBuilder = new StringBuilder(tableName);
                        for (String field : fields) {
                            Object fieldValue = columnValues.get(field);
                            indexKeyBuilder.append(":").append(field).append(":").append(fieldValue);
                        }
                        String indexKey = indexKeyBuilder.toString();
                        
                        // 添加到索引
                        double score = 0;
                        // 如果有排序字段，使用排序字段的值作为分数
                        String sortField = indexDef.getSortField();
                        if (sortField != null && columnValues.containsKey(sortField)) {
                            try {
                                Object sortValue = columnValues.get(sortField);
                                if (sortValue instanceof Number) {
                                    score = ((Number) sortValue).doubleValue();
                                } else {
                                    score = Double.parseDouble(sortValue.toString());
                                }
                            } catch (NumberFormatException e) {
                                logger.warn("Sort field {} value is not a number: {}", sortField, columnValues.get(sortField));
                            }
                        }
                        
                        redisOperationService.addIndexRecord(indexKey, primaryKey, score);
                        
                        // 添加主键到索引的映射
                        redisOperationService.addPrimaryKeyToIndexMapping(tableName, primaryKey, indexKey);
                        
                        updatedIndices++;
                    }
                } else {
                    // 处理单字段索引
                    String fieldName = fields.get(0);
                    if (columnValues.containsKey(fieldName)) {
                        Object fieldValue = columnValues.get(fieldName);
                        
                        // 处理多值索引（如果字段值是逗号分隔的字符串）
                        if (fieldValue instanceof String && ((String) fieldValue).contains(",")) {
                            String[] values = ((String) fieldValue).split(",");
                            for (String value : values) {
                                String indexKey = tableName + ":" + fieldName + ":" + value.trim();
                                
                                // 添加到索引
                                double score = 0;
                                // 如果有排序字段，使用排序字段的值作为分数
                                String sortField = indexDef.getSortField();
                                if (sortField != null && columnValues.containsKey(sortField)) {
                                    try {
                                        Object sortValue = columnValues.get(sortField);
                                        if (sortValue instanceof Number) {
                                            score = ((Number) sortValue).doubleValue();
                                        } else {
                                            score = Double.parseDouble(sortValue.toString());
                                        }
                                    } catch (NumberFormatException e) {
                                        logger.warn("Sort field {} value is not a number: {}", sortField, columnValues.get(sortField));
                                    }
                                }
                                
                                redisOperationService.addIndexRecord(indexKey, primaryKey, score);
                                
                                // 添加主键到索引的映射
                                redisOperationService.addPrimaryKeyToIndexMapping(tableName, primaryKey, indexKey);
                                
                                updatedIndices++;
                            }
                        } else {
                            // 普通单值索引
                            String indexKey = tableName + ":" + fieldName + ":" + fieldValue;
                            
                            // 添加到索引
                            double score = 0;
                            // 如果有排序字段，使用排序字段的值作为分数
                            String sortField = indexDef.getSortField();
                            if (sortField != null && columnValues.containsKey(sortField)) {
                                try {
                                    Object sortValue = columnValues.get(sortField);
                                    if (sortValue instanceof Number) {
                                        score = ((Number) sortValue).doubleValue();
                                    } else {
                                        score = Double.parseDouble(sortValue.toString());
                                    }
                                } catch (NumberFormatException e) {
                                    logger.warn("Sort field {} value is not a number: {}", sortField, columnValues.get(sortField));
                                }
                            }
                            
                            redisOperationService.addIndexRecord(indexKey, primaryKey, score);
                            
                            // 添加主键到索引的映射
                            redisOperationService.addPrimaryKeyToIndexMapping(tableName, primaryKey, indexKey);
                            
                            updatedIndices++;
                        }
                    }
                }
            }
            
            // 添加版本字段索引
            String versionField = indexConfig.getVersionField();
            if (versionField != null && !versionField.isEmpty() && columnValues.containsKey(versionField)) {
                Object versionValue = columnValues.get(versionField);
                String versionIndexKey = tableName + ":" + versionField + ":" + versionValue;
                
                // 添加到版本索引
                redisOperationService.addIndexRecord(versionIndexKey, primaryKey, 0);
                
                // 添加主键到版本索引的映射
                redisOperationService.addPrimaryKeyToIndexMapping(tableName, primaryKey, versionIndexKey);
                
                updatedIndices++;
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
     * 检查Insert语句是否包含版本字段
     * @param insert Insert语句
     * @return 是否包含版本字段
     */
    private boolean containsVersionField(Insert insert) {
        if (insert == null || insert.getColumns() == null) {
            return false;
        }
        
        // 获取表对应的索引配置
        String tableName = insert.getTable().getName();
        IndexConfig indexConfig = findIndexConfig(tableName);
        if (indexConfig == null) {
            return false;
        }
        
        // 获取版本字段名
        String versionField = indexConfig.getVersionField();
        if (versionField == null || versionField.isEmpty()) {
            // 如果没有配置版本字段，默认为true
            return true;
        }
        
        // 检查Insert语句的字段列表中是否包含版本字段
        List<Column> columns = insert.getColumns();
        for (Column column : columns) {
            if (column.getColumnName().equalsIgnoreCase(versionField)) {
                return true;
            }
        }
        
        return false;
    }
}