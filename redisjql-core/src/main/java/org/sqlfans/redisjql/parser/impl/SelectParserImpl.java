package org.sqlfans.redisjql.parser.impl;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Invocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlfans.redisjql.cache.CacheOperationService;
import org.sqlfans.redisjql.config.IndexConfig;
import org.sqlfans.redisjql.parser.SelectParser;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Select语句解析器实现类
 */
public class SelectParserImpl implements SelectParser {
    private static final Logger logger = LoggerFactory.getLogger(SelectParserImpl.class);
    
    private CacheOperationService redisOperationService;
    private List<IndexConfig> indexConfigs;
    
    public SelectParserImpl(CacheOperationService redisOperationService, List<IndexConfig> indexConfigs) {
        this.redisOperationService = redisOperationService;
        this.indexConfigs = indexConfigs;
    }
    
    @Override
    public Object parse(Select select) {
        if (!canUseRedisCache(select)) {
            logCacheMiss(select, "Redis cache conditions not met");
            return null;
        }
        
        SelectBody selectBody = select.getSelectBody();
        if (!(selectBody instanceof PlainSelect)) {
            logCacheMiss(select, "Unsupported select type, only PlainSelect is supported");
            return null;
        }
        
        PlainSelect plainSelect = (PlainSelect) selectBody;
        if (!(plainSelect.getFromItem() instanceof Table)) {
            logCacheMiss(select, "FROM clause is not a single table query");
            return null;
        }
        
        Table table = (Table) plainSelect.getFromItem();
        String tableName = table.getName();
        IndexConfig indexConfig = findIndexConfig(tableName);
        
        if (indexConfig == null) {
            logCacheMiss(select, "No index configuration found for table: " + tableName);
            return null;
        }
        
        // Extract query conditions and sort information
        Expression whereCondition = extractWhereCondition(select);
        List<OrderByElement> orderByElements = extractOrderByClause(select);
        List<Expression> groupByExpressions = extractGroupByClause(select);
        
        // Query using Redis index
        return queryWithRedisIndex(tableName, whereCondition, orderByElements, groupByExpressions);
    }
    
    @Override
    public boolean canUseRedisCache(Select select) {
        if (select == null || !(select.getSelectBody() instanceof PlainSelect)) {
            return false;
        }
        
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        if (plainSelect.getFromItem() == null || !(plainSelect.getFromItem() instanceof Table)) {
            return false;
        }
        
        if (plainSelect.getWhere() == null) {
            return false;
        }
        
        Table table = (Table) plainSelect.getFromItem();
        String tableName = table.getName();
        IndexConfig indexConfig = findIndexConfig(tableName);
        
        if (indexConfig == null) {
            return false;
        }

        // 检查是否是单值查询（通过主键或唯一索引）
        Expression whereExpr = plainSelect.getWhere();
        if (isSingleValueQuery(whereExpr, indexConfig)) {
            return false; // 单值查询直接走数据库
        }
        
        return true;
    }
    
    @Override
    public void logCacheMiss(Select select, String reason) {
        logger.debug("Redis cache miss: {} - {}", select, reason);
    }
    
    @Override
    public Expression extractWhereCondition(Select select) {
        if (!(select.getSelectBody() instanceof PlainSelect)) {
            return null;
        }
        
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        return plainSelect.getWhere();
    }
    
    @Override
    public List<OrderByElement> extractOrderByClause(Select select) {
        if (!(select.getSelectBody() instanceof PlainSelect)) {
            return Collections.emptyList();
        }
        
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        return plainSelect.getOrderByElements() != null ? plainSelect.getOrderByElements() : Collections.emptyList();
    }
    
    @Override
    public List<Expression> extractGroupByClause(Select select) {
        if (!(select.getSelectBody() instanceof PlainSelect)) {
            return Collections.emptyList();
        }
        
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        return plainSelect.getGroupBy() != null ? plainSelect.getGroupBy().getGroupByExpressionList().getExpressions() : Collections.emptyList();
    }
    
    @Override
    public Object queryWithRedisIndex(String tableName, Expression whereCondition, 
                                     List<OrderByElement> orderByElements, 
                                     List<Expression> groupByExpressions) {
        if (whereCondition == null) {
            return null;
        }
        
        IndexConfig indexConfig = findIndexConfig(tableName);
        if (indexConfig == null) {
            return null;
        }
        
        // 查找匹配的索引字段
        Map<String, String> indexFieldValues = extractIndexFieldValues(whereCondition, indexConfig);
        if (indexFieldValues.isEmpty()) {
            return null;
        }
        
        // 构建Redis索引键并查询
        List<String> primaryKeys = new ArrayList<>();
        Map<String, Set<String>> indexResults = new HashMap<>();
        
        for (Map.Entry<String, String> entry : indexFieldValues.entrySet()) {
            String fieldName = entry.getKey();
            String fieldValue = entry.getValue();
            String indexKey = tableName + ":" + fieldName + ":" + fieldValue;
            
            // 查询索引
            Set<String> keys = redisOperationService.queryPrimaryKeysByIndex(indexKey, 0, -1);
            if (keys != null && !keys.isEmpty()) {
                indexResults.put(fieldName, keys);
            }
        }
        
        // 如果有多个索引条件，取交集
        if (!indexResults.isEmpty()) {
            // 初始化结果集
            primaryKeys = new ArrayList<>(indexResults.values().iterator().next());
            
            // 取所有索引结果的交集
            for (Set<String> keys : indexResults.values()) {
                primaryKeys.retainAll(keys);
            }
        }
        
        if (primaryKeys.isEmpty()) {
            return null;
        }
        
        // 处理排序
        if (!orderByElements.isEmpty()) {
            // 获取排序字段
            final Map<String, Map<String, String>> sortFieldsData = new HashMap<>();
            
            // 对每个主键，获取排序字段的值
            for (String primaryKey : primaryKeys) {
                Map<String, String> rowData = new HashMap<>();
                
                // 获取每个排序字段的值
                for (OrderByElement orderBy : orderByElements) {
                    if (orderBy.getExpression() instanceof Column) {
                        Column column = (Column) orderBy.getExpression();
                        String fieldName = column.getColumnName();
                        
                        // 这里应该从Redis获取该字段的值
                        // 简化实现，实际应该查询Redis
                        String dataKey = tableName + ":data:" + primaryKey;
                        // 从Redis获取排序字段的值
                        String fieldValue = redisOperationService.getFieldValue(dataKey, fieldName);
                        rowData.put(fieldName, fieldValue);
                    }
                }
                
                sortFieldsData.put(primaryKey, rowData);
            }
            
            // 根据排序字段对主键进行排序
            primaryKeys.sort((pk1, pk2) -> {
                for (OrderByElement orderBy : orderByElements) {
                    if (orderBy.getExpression() instanceof Column) {
                        Column column = (Column) orderBy.getExpression();
                        String fieldName = column.getColumnName();
                        
                        String value1 = sortFieldsData.get(pk1).get(fieldName);
                        String value2 = sortFieldsData.get(pk2).get(fieldName);
                        
                        if (value1 == null || value2 == null) {
                            continue;
                        }
                        
                        int comparison;
                        try {
                            // 尝试数值比较
                            double d1 = Double.parseDouble(value1);
                            double d2 = Double.parseDouble(value2);
                            comparison = Double.compare(d1, d2);
                        } catch (NumberFormatException e) {
                            // 字符串比较
                            comparison = value1.compareTo(value2);
                        }
                        
                        if (comparison != 0) {
                            return orderBy.isAsc() ? comparison : -comparison;
                        }
                    }
                }
                return 0;
            });
        }
        
        // 处理分组
        if (!groupByExpressions.isEmpty()) {
            // 获取分组字段
            Map<String, Set<String>> groupedKeys = new HashMap<>();
            
            for (String primaryKey : primaryKeys) {
                StringBuilder groupKey = new StringBuilder();
                
                for (Expression groupExpr : groupByExpressions) {
                    if (groupExpr instanceof Column) {
                        Column column = (Column) groupExpr;
                        String fieldName = column.getColumnName();
                        String dataKey = tableName + ":data:" + primaryKey;
                        String fieldValue = redisOperationService.getFieldValue(dataKey, fieldName);
                        groupKey.append(fieldValue).append(":");
                    }
                }
                
                groupedKeys.computeIfAbsent(groupKey.toString(), k -> new LinkedHashSet<>())
                          .add(primaryKey);
            }
            
            // 更新主键列表，保持分组顺序
            primaryKeys = groupedKeys.values().stream()
                                   .flatMap(Set::stream)
                                   .collect(Collectors.toList());
        }
        
        return primaryKeys;
    }
    
    @Override
    public String rewriteSelectSql(String originalSql, List<String> primaryKeys) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            return originalSql;
        }

        try {
            // Parse original SQL
            Select select = (Select) CCJSqlParserUtil.parse(originalSql);
            SelectBody selectBody = select.getSelectBody();
            
            if (!(selectBody instanceof PlainSelect)) {
                return originalSql;
            }
            
            PlainSelect plainSelect = (PlainSelect) selectBody;
            Table table = (Table) plainSelect.getFromItem();
            
            // Get primary key column name
            IndexConfig indexConfig = findIndexConfig(table.getName());
            if (indexConfig == null) {
                return originalSql;
            }
            
            String primaryKeyColumn = indexConfig.getPrimaryKey();
            
            // Build IN condition
            StringBuilder inValues = new StringBuilder();
            for (int i = 0; i < primaryKeys.size(); i++) {
                if (i > 0) {
                    inValues.append(",");
                }
                inValues.append("'").append(primaryKeys.get(i)).append("'");
            }
            
            // Create IN expression
            Column primaryKeyCol = new Column(table, primaryKeyColumn);
            InExpression inExpression = new InExpression(
                primaryKeyCol,
                new ExpressionList(primaryKeys.stream()
                    .map(pk -> new StringValue(pk))
                    .collect(Collectors.toList()))
            );
            
            // Replace WHERE condition
            Expression whereExpression = plainSelect.getWhere();
            if (whereExpression != null) {
                plainSelect.setWhere(new AndExpression(whereExpression, inExpression));
            } else {
                plainSelect.setWhere(inExpression);
            }
            
            // If has ORDER BY, ensure ordering by primary key list order
            if (!primaryKeys.isEmpty() && plainSelect.getOrderByElements() != null) {
                // Build CASE expression for sorting
                StringBuilder caseWhen = new StringBuilder();
                caseWhen.append("CASE ").append(primaryKeyColumn);
                for (int i = 0; i < primaryKeys.size(); i++) {
                    caseWhen.append(" WHEN '").append(primaryKeys.get(i))
                           .append("' THEN ").append(i);
                }
                caseWhen.append(" END");
                
                // Add CASE expression as first sort condition
                Expression caseExpression = CCJSqlParserUtil.parseExpression(caseWhen.toString());
                OrderByElement orderByCase = new OrderByElement();
                orderByCase.setExpression(caseExpression);
                orderByCase.setAsc(true);
                
                List<OrderByElement> newOrderByElements = new ArrayList<>();
                newOrderByElements.add(orderByCase);
                if (plainSelect.getOrderByElements() != null) {
                    newOrderByElements.addAll(plainSelect.getOrderByElements());
                }
                plainSelect.setOrderByElements(newOrderByElements);
            }
            
            return select.toString();
        } catch (JSQLParserException e) {
            logger.error("Failed to rewrite SQL", e);
            return originalSql;
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
     * 从WHERE条件中提取索引字段和值
     * @param expression WHERE条件表达式
     * @param indexConfig 索引配置
     * @return 索引字段和值的映射
     */
    private Map<String, String> extractIndexFieldValues(Expression expression, IndexConfig indexConfig) {
        Map<String, String> fieldValues = new HashMap<>();
        
        if (expression instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) expression;
            if (equalsTo.getLeftExpression() instanceof Column) {
                Column column = (Column) equalsTo.getLeftExpression();
                String fieldName = column.getColumnName();
                
                // 检查字段是否在任何索引中
                for (IndexConfig.IndexDefinition index : indexConfig.getIndexes()) {
                    if (index.getFields().contains(fieldName)) {
                        String value = extractValueFromExpression(equalsTo.getRightExpression());
                        if (value != null) {
                            fieldValues.put(fieldName, value);
                        }
                        break;
                    }
                }
            }
        } else if (expression instanceof AndExpression) {
            AndExpression andExpr = (AndExpression) expression;
            // 递归处理AND条件的两边
            fieldValues.putAll(extractIndexFieldValues(andExpr.getLeftExpression(), indexConfig));
            fieldValues.putAll(extractIndexFieldValues(andExpr.getRightExpression(), indexConfig));
        }
        
        return fieldValues;
    }
    
    /**
     * 从表达式中提取值
     * @param expression 表达式
     * @return 值
     */
    private String extractValueFromExpression(Expression expression) {
        if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        } else if (expression instanceof net.sf.jsqlparser.expression.LongValue) {
            return String.valueOf(((net.sf.jsqlparser.expression.LongValue) expression).getValue());
        } else if (expression instanceof net.sf.jsqlparser.expression.DoubleValue) {
            return String.valueOf(((net.sf.jsqlparser.expression.DoubleValue) expression).getValue());
        } else if (expression instanceof net.sf.jsqlparser.expression.DateValue) {
            return ((net.sf.jsqlparser.expression.DateValue) expression).getValue().toString();
        } else if (expression instanceof net.sf.jsqlparser.expression.TimeValue) {
            return ((net.sf.jsqlparser.expression.TimeValue) expression).getValue().toString();
        } else if (expression instanceof net.sf.jsqlparser.expression.TimestampValue) {
            return ((net.sf.jsqlparser.expression.TimestampValue) expression).getValue().toString();
        }
        return null;
    }

    /**
     * 判断是否是单值查询（静态方法，可供拦截器调用）
     * @param whereExpr WHERE条件表达式
     * @param indexConfig 索引配置
     * @return 是否是单值查询
     */
    public static boolean isSingleValueQuery(Expression whereExpr, IndexConfig indexConfig) {
        // 先检查是否为空,避免后续空指针
        if (whereExpr == null || indexConfig == null) {
            return false;
        }

        // 缓存主键和唯一索引信息,避免重复获取
        String primaryKey = indexConfig.getPrimaryKey();
        List<IndexConfig.IndexDefinition> uniqueIndexes = indexConfig.getIndexes().stream()
            .filter(IndexConfig.IndexDefinition::isUnique)
            .collect(Collectors.toList());

        if (whereExpr instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) whereExpr;
            if (equalsTo.getLeftExpression() instanceof Column) {
                Column column = (Column) equalsTo.getLeftExpression();
                String columnName = column.getColumnName();
                
                // 检查主键
                if (columnName.equals(primaryKey)) {
                    return true;
                }
                
                // 检查单字段唯一索引
                return uniqueIndexes.stream()
                    .anyMatch(index -> index.getFields().size() == 1 
                        && index.getFields().get(0).equals(columnName));
            }
        } else if (whereExpr instanceof AndExpression) {
            AndExpression andExpr = (AndExpression) whereExpr;
            
            // 从where条件中提取所有相等条件的字段
            Map<String, String> fieldValues = new HashMap<>();
            extractFieldValuesFromExpression(whereExpr, indexConfig, fieldValues);
            
            // 检查联合唯一索引
            for (IndexConfig.IndexDefinition index : uniqueIndexes) {
                if (index.getFields().size() > 1) {
                    boolean allFieldsMatched = true;
                    for (String field : index.getFields()) {
                        if (!fieldValues.containsKey(field)) {
                            allFieldsMatched = false;
                            break;
                        }
                    }
                    if (allFieldsMatched) {
                        return true;
                    }
                }
            }
            
            // 递归检查左右子表达式
            return isSingleValueQuery(andExpr.getLeftExpression(), indexConfig) || 
                   isSingleValueQuery(andExpr.getRightExpression(), indexConfig);
        }
        return false;
    }
    
    /**
     * 辅助方法：从表达式中提取字段和值，用于判断唯一索引条件
     */
    private static void extractFieldValuesFromExpression(
            Expression expression, IndexConfig indexConfig, Map<String, String> fieldValues) {
        if (expression instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) expression;
            if (equalsTo.getLeftExpression() instanceof Column) {
                Column column = (Column) equalsTo.getLeftExpression();
                String fieldName = column.getColumnName();
                
                // 提取值
                String value = null;
                if (equalsTo.getRightExpression() instanceof StringValue) {
                    value = ((StringValue) equalsTo.getRightExpression()).getValue();
                } else if (equalsTo.getRightExpression() instanceof net.sf.jsqlparser.expression.LongValue) {
                    value = String.valueOf(((net.sf.jsqlparser.expression.LongValue) equalsTo.getRightExpression()).getValue());
                } else if (equalsTo.getRightExpression() instanceof net.sf.jsqlparser.expression.DoubleValue) {
                    value = String.valueOf(((net.sf.jsqlparser.expression.DoubleValue) equalsTo.getRightExpression()).getValue());
                }
                
                if (value != null) {
                    fieldValues.put(fieldName, value);
                }
            }
        } else if (expression instanceof AndExpression) {
            AndExpression andExpr = (AndExpression) expression;
            extractFieldValuesFromExpression(andExpr.getLeftExpression(), indexConfig, fieldValues);
            extractFieldValuesFromExpression(andExpr.getRightExpression(), indexConfig, fieldValues);
        }
    }
    
    /**
     * 执行重写后的SQL
     */
    @SuppressWarnings("unused")
    private Object executeRewrittenSql(Invocation invocation, MappedStatement ms, BoundSql boundSql, String newSql) throws Throwable {
        // 创建新的BoundSql对象
        BoundSql newBoundSql = new BoundSql(
            ms.getConfiguration(),
            newSql,
            boundSql.getParameterMappings(),
            boundSql.getParameterObject()
        );
        
        // 复制附加参数
        for (String key : boundSql.getAdditionalParameters().keySet()) {
            newBoundSql.setAdditionalParameter(key, boundSql.getAdditionalParameter(key));
        }
        
        // 创建新的MappedStatement
        MappedStatement newMs = copyMappedStatement(ms, newBoundSql);
        
        // 替换参数并执行
        invocation.getArgs()[0] = newMs;
        return invocation.proceed();
    }
    
    /**
     * 复制MappedStatement对象
     */
    private MappedStatement copyMappedStatement(MappedStatement ms, BoundSql newBoundSql) {
        MappedStatement.Builder builder = new MappedStatement.Builder(
            ms.getConfiguration(),
            ms.getId(),
            parameterObject -> newBoundSql,
            ms.getSqlCommandType()
        );
        
        builder.resource(ms.getResource());
        builder.fetchSize(ms.getFetchSize());
        builder.statementType(ms.getStatementType());
        builder.keyGenerator(ms.getKeyGenerator());
        builder.timeout(ms.getTimeout());
        builder.parameterMap(ms.getParameterMap());
        builder.resultMaps(ms.getResultMaps());
        builder.resultSetType(ms.getResultSetType());
        builder.cache(ms.getCache());
        builder.flushCacheRequired(ms.isFlushCacheRequired());
        builder.useCache(ms.isUseCache());
        
        return builder.build();
    }
}