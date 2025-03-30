package org.sqlfans.redisjql.interceptor;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlfans.redisjql.cache.CacheOperationService;
import org.sqlfans.redisjql.config.IndexConfig;
import org.sqlfans.redisjql.parser.StatementParser;

import java.util.Properties;
import java.util.Set;

/**
 * MyBatis 拦截器
 * 用于拦截 SQL 语句并进行处理
 */
@Intercepts({
    @Signature(type = Executor.class, method = "update", args = {MappedStatement.class, Object.class}),
    @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class})
})
public class RedisJqlInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(RedisJqlInterceptor.class);
    private StatementParser statementParser;
    private CacheOperationService redisOperationService;
    
    public RedisJqlInterceptor(StatementParser statementParser, CacheOperationService redisOperationService) {
        this.statementParser = statementParser;
        this.redisOperationService = redisOperationService;
    }
    
    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        Object[] args = invocation.getArgs();
        MappedStatement ms = (MappedStatement) args[0];
        Object parameter = args[1];
        
        BoundSql boundSql = ms.getBoundSql(parameter);
        String sql = boundSql.getSql();
        
        // 根据 SQL 类型进行处理
        if (sql.trim().toLowerCase().startsWith("select")) {
            return handleSelect(invocation, sql);
        } else if (sql.trim().toLowerCase().startsWith("insert")) {
            return handleInsert(invocation, sql);
        } else if (sql.trim().toLowerCase().startsWith("update")) {
            return handleUpdate(invocation, sql);
        } else if (sql.trim().toLowerCase().startsWith("delete")) {
            return handleDelete(invocation, sql);
        }
        
        return invocation.proceed();
    }
    
    private Object handleSelect(Invocation invocation, String sql) throws Throwable {
        try {
            // 解析SQL语句
            net.sf.jsqlparser.statement.Statement statement = statementParser.parse(sql);
            if (!(statement instanceof net.sf.jsqlparser.statement.select.Select)) {
                return invocation.proceed();
            }
            
            net.sf.jsqlparser.statement.select.Select select = 
                (net.sf.jsqlparser.statement.select.Select) statement;
            
            // 判断是否为单值查询场景（主键或唯一索引）
            if (isSingleValueQuery(select)) {
                // 如果是单值查询，直接使用数据库查询，不修改SQL
                return invocation.proceed();
            }
            
            // 判断是否可以使用Redis索引优化
            if (!canUseRedisCache(select)) {
                // 如果不满足Redis索引条件，直接执行原SQL
                return invocation.proceed();
            }
            
            // 获取查询条件和排序信息
            Object result = statementParser.processStatement(statement);
            if (result == null) {
                // 如果不满足Redis索引条件，直接执行原SQL
                return invocation.proceed();
            }
            
            // 获取Redis缓存数据的主键
            java.util.List<String> primaryKeys = getPrimaryKeysFromRedis(select);
            if (primaryKeys == null || primaryKeys.isEmpty()) {
                // 如果在Redis中找不到匹配的记录，直接执行原SQL
                return invocation.proceed();
            }
            
            // 改写SQL语句，使用IN条件查询
            MappedStatement ms = (MappedStatement) invocation.getArgs()[0];
            BoundSql boundSql = ms.getBoundSql(invocation.getArgs()[1]);
            String newSql = rewriteSelectSql(sql, primaryKeys, select);
            
            // 执行改写后的SQL
            return executeRewrittenSql(invocation, ms, boundSql, newSql);
        } catch (Exception e) {
            // 发生异常时，使用原SQL执行
            return invocation.proceed();
        }
    }
    
    /**
     * 判断是否为单值查询
     * @param select Select语句
     * @return 是否为单值查询
     */
    private boolean isSingleValueQuery(net.sf.jsqlparser.statement.select.Select select) {
        try {
            if (!(select.getSelectBody() instanceof net.sf.jsqlparser.statement.select.PlainSelect)) {
                return false;
            }
            
            net.sf.jsqlparser.statement.select.PlainSelect plainSelect = 
                (net.sf.jsqlparser.statement.select.PlainSelect) select.getSelectBody();
            
            if (plainSelect.getFromItem() == null || 
                !(plainSelect.getFromItem() instanceof net.sf.jsqlparser.schema.Table)) {
                return false;
            }
            
            net.sf.jsqlparser.schema.Table table = 
                (net.sf.jsqlparser.schema.Table) plainSelect.getFromItem();
            
            String tableName = table.getName();
            IndexConfig indexConfig = findIndexConfig(tableName);
            
            if (indexConfig == null || plainSelect.getWhere() == null) {
                return false;
            }
            
            // 使用SelectParserImpl中的静态isSingleValueQuery方法
            return org.sqlfans.redisjql.parser.impl.SelectParserImpl.isSingleValueQuery(
                plainSelect.getWhere(), indexConfig);
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * 判断是否可以使用Redis缓存
     * @param select Select语句
     * @return 是否可以使用Redis缓存
     */
    private boolean canUseRedisCache(net.sf.jsqlparser.statement.select.Select select) {
        try {
            if (!(select.getSelectBody() instanceof net.sf.jsqlparser.statement.select.PlainSelect)) {
                return false;
            }
            
            net.sf.jsqlparser.statement.select.PlainSelect plainSelect = 
                (net.sf.jsqlparser.statement.select.PlainSelect) select.getSelectBody();
            
            if (plainSelect.getFromItem() == null || 
                !(plainSelect.getFromItem() instanceof net.sf.jsqlparser.schema.Table)) {
                return false;
            }
            
            if (plainSelect.getWhere() == null) {
                return false;
            }
            
            net.sf.jsqlparser.schema.Table table = 
                (net.sf.jsqlparser.schema.Table) plainSelect.getFromItem();
            
            String tableName = table.getName();
            IndexConfig indexConfig = findIndexConfig(tableName);
            
            return indexConfig != null;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * 从Redis获取主键列表
     * @param select Select语句
     * @return 主键列表
     */
    private java.util.List<String> getPrimaryKeysFromRedis(net.sf.jsqlparser.statement.select.Select select) {
        try {
            // 获取表名
            net.sf.jsqlparser.statement.select.PlainSelect plainSelect = 
                (net.sf.jsqlparser.statement.select.PlainSelect) select.getSelectBody();
            
            net.sf.jsqlparser.schema.Table table = 
                (net.sf.jsqlparser.schema.Table) plainSelect.getFromItem();
            
            String tableName = table.getName();
            
            // 解析WHERE条件
            net.sf.jsqlparser.expression.Expression whereExpr = plainSelect.getWhere();
            
            // 提取查询条件中的索引字段和值
            IndexConfig indexConfig = findIndexConfig(tableName);
            java.util.Map<String, String> fieldValues = 
                extractIndexFieldValues(whereExpr, indexConfig);
            
            if (fieldValues.isEmpty()) {
                return null;
            }
            
            // 构建Redis索引键并查询
            java.util.List<String> primaryKeys = new java.util.ArrayList<>();
            java.util.Map<String, java.util.Set<String>> indexResults = new java.util.HashMap<>();
            
            for (java.util.Map.Entry<String, String> entry : fieldValues.entrySet()) {
                String fieldName = entry.getKey();
                String fieldValue = entry.getValue();
                String indexKey = tableName + ":" + fieldName + ":" + fieldValue;
                
                // 查询索引
                java.util.Set<String> keys = redisOperationService.queryPrimaryKeysByIndex(indexKey, 0, -1);
                if (keys != null && !keys.isEmpty()) {
                    indexResults.put(fieldName, keys);
                }
            }
            
            // 如果有多个索引条件，取交集
            if (!indexResults.isEmpty()) {
                // 初始化结果集
                primaryKeys = new java.util.ArrayList<>(indexResults.values().iterator().next());
                
                // 取所有索引结果的交集
                for (java.util.Set<String> keys : indexResults.values()) {
                    primaryKeys.retainAll(keys);
                }
            }
            
            // 处理排序
            if (!primaryKeys.isEmpty() && plainSelect.getOrderByElements() != null) {
                sortPrimaryKeys(primaryKeys, plainSelect.getOrderByElements(), tableName);
            }
            
            return primaryKeys;
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * 对主键列表进行排序
     */
    private void sortPrimaryKeys(java.util.List<String> primaryKeys, 
                               java.util.List<net.sf.jsqlparser.statement.select.OrderByElement> orderByElements,
                               String tableName) {
        try {
            // 获取排序字段
            final java.util.Map<String, java.util.Map<String, String>> sortFieldsData = new java.util.HashMap<>();
            
            // 对每个主键，获取排序字段的值
            for (String primaryKey : primaryKeys) {
                java.util.Map<String, String> rowData = new java.util.HashMap<>();
                
                // 获取每个排序字段的值
                for (net.sf.jsqlparser.statement.select.OrderByElement orderBy : orderByElements) {
                    if (orderBy.getExpression() instanceof net.sf.jsqlparser.schema.Column) {
                        net.sf.jsqlparser.schema.Column column = 
                            (net.sf.jsqlparser.schema.Column) orderBy.getExpression();
                        String fieldName = column.getColumnName();
                        
                        // 从Redis获取排序字段的值
                        String dataKey = tableName + ":data:" + primaryKey;
                        String fieldValue = redisOperationService.getFieldValue(dataKey, fieldName);
                        rowData.put(fieldName, fieldValue);
                    }
                }
                
                sortFieldsData.put(primaryKey, rowData);
            }
            
            // 根据排序字段对主键进行排序
            primaryKeys.sort((pk1, pk2) -> {
                for (net.sf.jsqlparser.statement.select.OrderByElement orderBy : orderByElements) {
                    if (orderBy.getExpression() instanceof net.sf.jsqlparser.schema.Column) {
                        net.sf.jsqlparser.schema.Column column = 
                            (net.sf.jsqlparser.schema.Column) orderBy.getExpression();
                        String fieldName = column.getColumnName();
                        
                        java.util.Map<String, String> values1 = sortFieldsData.get(pk1);
                        java.util.Map<String, String> values2 = sortFieldsData.get(pk2);
                        
                        if (values1 == null || values2 == null) {
                            continue;
                        }
                        
                        String value1 = values1.get(fieldName);
                        String value2 = values2.get(fieldName);
                        
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
        } catch (Exception e) {
            // 排序失败时不处理，保持原顺序
        }
    }
    
    /**
     * 从WHERE条件中提取索引字段和值
     */
    private java.util.Map<String, String> extractIndexFieldValues(
            net.sf.jsqlparser.expression.Expression expression, IndexConfig indexConfig) {
        java.util.Map<String, String> fieldValues = new java.util.HashMap<>();
        
        if (expression instanceof net.sf.jsqlparser.expression.operators.relational.EqualsTo) {
            net.sf.jsqlparser.expression.operators.relational.EqualsTo equalsTo = 
                (net.sf.jsqlparser.expression.operators.relational.EqualsTo) expression;
            
            if (equalsTo.getLeftExpression() instanceof net.sf.jsqlparser.schema.Column) {
                net.sf.jsqlparser.schema.Column column = 
                    (net.sf.jsqlparser.schema.Column) equalsTo.getLeftExpression();
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
        } else if (expression instanceof net.sf.jsqlparser.expression.operators.conditional.AndExpression) {
            net.sf.jsqlparser.expression.operators.conditional.AndExpression andExpr = 
                (net.sf.jsqlparser.expression.operators.conditional.AndExpression) expression;
            
            // 递归处理AND条件的两边
            fieldValues.putAll(extractIndexFieldValues(andExpr.getLeftExpression(), indexConfig));
            fieldValues.putAll(extractIndexFieldValues(andExpr.getRightExpression(), indexConfig));
        }
        
        return fieldValues;
    }
    
    /**
     * 从表达式中提取值
     */
    private String extractValueFromExpression(net.sf.jsqlparser.expression.Expression expression) {
        if (expression instanceof net.sf.jsqlparser.expression.StringValue) {
            return ((net.sf.jsqlparser.expression.StringValue) expression).getValue();
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
    
    private Object handleInsert(Invocation invocation, String sql) throws Throwable {
        try {
            // 解析SQL语句
            net.sf.jsqlparser.statement.Statement statement = statementParser.parse(sql);
            if (!(statement instanceof net.sf.jsqlparser.statement.insert.Insert)) {
                return invocation.proceed();
            }
            
            // 检查是否包含版本号字段
            if (!containsVersionField(statement)) {
                // 输出debug日志
                return invocation.proceed();
            }
            
            // 执行原SQL并获取结果（包括自增主键）
            Object result = invocation.proceed();
            
            // 获取插入记录的主键值
            String primaryKey = extractPrimaryKey(invocation);
            if (primaryKey == null) {
                return result;
            }
            
            // 根据索引配置更新Redis缓存
            Object indexInfo = statementParser.processStatement(statement);
            if (indexInfo != null) {
                String tableName = ((net.sf.jsqlparser.statement.insert.Insert) statement).getTable().getName();
                String indexKey = indexInfo.toString();
                double score = extractScore(statement); // 从排序字段获取分数
                
                // 添加索引记录
                redisOperationService.addIndexRecord(indexKey, primaryKey, score);
                // 添加主键到索引的映射
                redisOperationService.addPrimaryKeyToIndexMapping(tableName, primaryKey, indexKey);
            }
            
            return result;
        } catch (Exception e) {
            // 发生异常时，使用原SQL执行
            return invocation.proceed();
        }
    }
    
    private Object handleUpdate(Invocation invocation, String sql) throws Throwable {
        try {
            // 解析SQL语句
            net.sf.jsqlparser.statement.Statement statement = statementParser.parse(sql);
            if (!(statement instanceof net.sf.jsqlparser.statement.update.Update)) {
                return invocation.proceed();
            }
            
            // 检查是否包含版本号字段
            if (!containsVersionField(statement)) {
                // 输出debug日志
                return invocation.proceed();
            }
            
            // 执行原SQL并验证影响行数（乐观锁更新是否成功）
            Object result = invocation.proceed();
            int affectedRows = getAffectedRows(result);
            if (affectedRows <= 0) {
                return result;
            }
            
            // 获取更新记录的主键值
            String primaryKey = extractPrimaryKeyFromUpdate(statement);
            if (primaryKey == null) {
                return result;
            }
            
            // 获取表名
            String tableName = ((net.sf.jsqlparser.statement.update.Update) statement).getTable().getName();
            
            // 获取旧的索引键
            Set<String> oldIndexKeys = redisOperationService.getPrimaryKeyMappings(tableName, primaryKey);
            
            // 根据新值生成新的索引信息
            Object indexInfo = statementParser.processStatement(statement);
            if (indexInfo != null) {
                String newIndexKey = indexInfo.toString();
                double newScore = extractScore(statement); // 从排序字段获取新的分数
                
                // 删除旧的索引记录
                if (oldIndexKeys != null) {
                    for (String oldIndexKey : oldIndexKeys) {
                        redisOperationService.removeIndexRecord(oldIndexKey, primaryKey);
                    }
                }
                
                // 添加新的索引记录
                redisOperationService.addIndexRecord(newIndexKey, primaryKey, newScore);
                redisOperationService.addPrimaryKeyToIndexMapping(tableName, primaryKey, newIndexKey);
            }
            
            return result;
        } catch (Exception e) {
            // 发生异常时，使用原SQL执行
            return invocation.proceed();
        }
    }
    
    private Object handleDelete(Invocation invocation, String sql) throws Throwable {
        try {
            // 解析SQL语句
            net.sf.jsqlparser.statement.Statement statement = statementParser.parse(sql);
            if (!(statement instanceof net.sf.jsqlparser.statement.delete.Delete)) {
                return invocation.proceed();
            }
            
            // 获取删除记录的主键值
            String primaryKey = extractPrimaryKeyFromDelete(statement);
            if (primaryKey == null) {
                return invocation.proceed();
            }
            
            // 获取表名
            String tableName = ((net.sf.jsqlparser.statement.delete.Delete) statement).getTable().getName();
            
            // 执行原SQL
            Object result = invocation.proceed();
            
            // 标记记录为删除状态（10分钟后过期）
            redisOperationService.markForDeletion(tableName, primaryKey);
            
            // 获取该主键对应的所有索引键
            Set<String> indexKeys = redisOperationService.getPrimaryKeyMappings(tableName, primaryKey);
            if (indexKeys != null) {
                for (String indexKey : indexKeys) {
                    // 标记索引记录为删除状态
                    redisOperationService.markForDeletion(indexKey, primaryKey);
                }
            }
            
            return result;
        } catch (Exception e) {
            // 发生异常时，使用原SQL执行
            return invocation.proceed();
        }
    }
    
    /**
     * 重写SELECT SQL语句，使用IN条件查询
     * @param sql 原始SQL
     * @param primaryKeys 主键集合
     * @param select 原始Select语句
     * @return 重写后的SQL
     */
    private String rewriteSelectSql(String sql, java.util.List<String> primaryKeys, 
                                  net.sf.jsqlparser.statement.select.Select select) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            return sql;
        }
        
        try {
            net.sf.jsqlparser.statement.select.PlainSelect plainSelect = 
                (net.sf.jsqlparser.statement.select.PlainSelect) select.getSelectBody();
            
            net.sf.jsqlparser.schema.Table table = 
                (net.sf.jsqlparser.schema.Table) plainSelect.getFromItem();
            
            String tableName = table.getName();
            IndexConfig indexConfig = findIndexConfig(tableName);
            
            if (indexConfig == null) {
                return sql;
            }
            
            String primaryKeyColumn = indexConfig.getPrimaryKey();
            
            // 创建IN表达式
            net.sf.jsqlparser.schema.Column primaryKeyCol = 
                new net.sf.jsqlparser.schema.Column(table, primaryKeyColumn);
            
            // 转换主键值为表达式列表
            java.util.List<net.sf.jsqlparser.expression.Expression> expressions = 
                new java.util.ArrayList<>();
            
            for (String pk : primaryKeys) {
                expressions.add(new net.sf.jsqlparser.expression.StringValue(pk));
            }
            
            net.sf.jsqlparser.expression.operators.relational.ExpressionList exprList = 
                new net.sf.jsqlparser.expression.operators.relational.ExpressionList(expressions);
            
            net.sf.jsqlparser.expression.operators.relational.InExpression inExpression = 
                new net.sf.jsqlparser.expression.operators.relational.InExpression(primaryKeyCol, exprList);
            
            // 替换或添加WHERE条件
            net.sf.jsqlparser.expression.Expression whereExpression = plainSelect.getWhere();
            if (whereExpression != null) {
                plainSelect.setWhere(
                    new net.sf.jsqlparser.expression.operators.conditional.AndExpression(
                        whereExpression, inExpression));
            } else {
                plainSelect.setWhere(inExpression);
            }
            
            // 处理排序
            if (!primaryKeys.isEmpty() && plainSelect.getOrderByElements() != null 
                && !plainSelect.getOrderByElements().isEmpty()) {
                // 保持Redis查询结果的排序顺序
                addCaseWhenOrderBy(plainSelect, primaryKeyColumn, primaryKeys);
            }
            
            return select.toString();
        } catch (Exception e) {
            // 如果使用JSqlParser失败，使用简单的字符串替换
            return fallbackRewriteSelectSql(sql, primaryKeys);
        }
    }
    
    /**
     * 添加CASE WHEN排序表达式，确保结果按照Redis查询的顺序排序
     */
    private void addCaseWhenOrderBy(net.sf.jsqlparser.statement.select.PlainSelect plainSelect, 
                                  String primaryKeyColumn, 
                                  java.util.List<String> primaryKeys) {
        try {
            // 构建CASE表达式字符串
            StringBuilder caseWhen = new StringBuilder();
            caseWhen.append("CASE ").append(primaryKeyColumn);
            
            for (int i = 0; i < primaryKeys.size(); i++) {
                caseWhen.append(" WHEN '").append(primaryKeys.get(i))
                       .append("' THEN ").append(i);
            }
            
            caseWhen.append(" ELSE ").append(primaryKeys.size()).append(" END");
            
            // 解析CASE表达式
            net.sf.jsqlparser.expression.Expression caseExpression = 
                net.sf.jsqlparser.parser.CCJSqlParserUtil.parseExpression(caseWhen.toString());
            
            // 创建新的OrderByElement
            net.sf.jsqlparser.statement.select.OrderByElement orderByCase = 
                new net.sf.jsqlparser.statement.select.OrderByElement();
            orderByCase.setExpression(caseExpression);
            orderByCase.setAsc(true);
            
            // 添加到ORDER BY子句
            java.util.List<net.sf.jsqlparser.statement.select.OrderByElement> newOrderByElements = 
                new java.util.ArrayList<>();
            newOrderByElements.add(orderByCase);
            
            // 保留原有的ORDER BY元素
            if (plainSelect.getOrderByElements() != null) {
                newOrderByElements.addAll(plainSelect.getOrderByElements());
            }
            
            plainSelect.setOrderByElements(newOrderByElements);
        } catch (Exception e) {
            // 如果添加CASE WHEN排序失败，保持原有排序
        }
    }
    
    /**
     * 备用的SQL重写方法，使用字符串替换
     */
    private String fallbackRewriteSelectSql(String sql, java.util.List<String> primaryKeys) {
        // 构建IN条件
        StringBuilder inClause = new StringBuilder();
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) {
                inClause.append(", ");
            }
            inClause.append("'").append(primaryKeys.get(i)).append("'");
        }
        
        // 检查SQL是否已有WHERE子句
        String lowerSql = sql.toLowerCase();
        int whereIndex = lowerSql.indexOf(" where ");
        
        StringBuilder newSql = new StringBuilder(sql);
        if (whereIndex >= 0) {
            // 在WHERE子句后添加AND id IN (...)
            int orderByIndex = lowerSql.indexOf(" order by ");
            int groupByIndex = lowerSql.indexOf(" group by ");
            int insertPos = orderByIndex >= 0 ? orderByIndex : 
                            (groupByIndex >= 0 ? groupByIndex : sql.length());
            
            newSql.insert(insertPos, " AND id IN (" + inClause + ")");
        } else {
            // 添加WHERE id IN (...)
            int orderByIndex = lowerSql.indexOf(" order by ");
            int groupByIndex = lowerSql.indexOf(" group by ");
            int insertPos = orderByIndex >= 0 ? orderByIndex : 
                            (groupByIndex >= 0 ? groupByIndex : sql.length());
            
            newSql.insert(insertPos, " WHERE id IN (" + inClause + ")");
        }
        
        return newSql.toString();
    }
    
    /**
     * 检查SQL语句是否包含版本号字段
     * @param statement SQL语句
     * @return 是否包含版本号字段
     */
    private boolean containsVersionField(net.sf.jsqlparser.statement.Statement statement) {
        try {
            if (statement instanceof net.sf.jsqlparser.statement.insert.Insert) {
                net.sf.jsqlparser.statement.insert.Insert insert = (net.sf.jsqlparser.statement.insert.Insert) statement;
                String tableName = insert.getTable().getName();
                
                // 获取表对应的索引配置
                IndexConfig indexConfig = findIndexConfig(tableName);
                if (indexConfig == null) {
                    return false;
                }
                
                // 检查是否包含版本号字段
                String versionField = indexConfig.getVersionField();
                if (insert.getColumns() != null) {
                    return insert.getColumns().stream()
                        .anyMatch(column -> column.getColumnName().equals(versionField));
                }
                
                // 如果没有指定字段，则假设包含所有字段（包括版本号字段）
                return true;
            } else if (statement instanceof net.sf.jsqlparser.statement.update.Update) {
                net.sf.jsqlparser.statement.update.Update update = (net.sf.jsqlparser.statement.update.Update) statement;
                String tableName = update.getTable().getName();
                
                // 获取表对应的索引配置
                IndexConfig indexConfig = findIndexConfig(tableName);
                if (indexConfig == null) {
                    return false;
                }
                
                // 检查是否包含版本号字段
                String versionField = indexConfig.getVersionField();
                return update.getUpdateSets().stream()
                    .map(updateSet -> updateSet.getColumns().get(0).getColumnName())
                    .anyMatch(columnName -> columnName.equals(versionField));
            }
            
            return false;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * 查找表对应的索引配置
     * @param tableName 表名
     * @return 索引配置
     */
    private IndexConfig findIndexConfig(String tableName) {
        // 这里应该从配置中心或缓存中获取索引配置
        // 简化实现，返回null
        return null;
    }
    
    /**
     * 从插入操作中提取主键值
     * @param invocation 调用对象
     * @return 主键值
     */
    private String extractPrimaryKey(Invocation invocation) {
        try {
            // 获取参数对象
            Object parameter = invocation.getArgs()[1];
            
            // 如果参数是Map类型，尝试从Map中获取主键值
            if (parameter instanceof java.util.Map) {
                java.util.Map<?, ?> paramMap = (java.util.Map<?, ?>) parameter;
                
                // 尝试获取常见的主键字段名
                String[] possibleKeys = {"id", "ID", "Id", "primaryKey", "primary_key"};
                for (String key : possibleKeys) {
                    Object value = paramMap.get(key);
                    if (value != null) {
                        return value.toString();
                    }
                }
            }
            
            // 如果参数是POJO类型，尝试通过反射获取主键值
            if (parameter != null) {
                try {
                    java.lang.reflect.Method method = parameter.getClass().getMethod("getId");
                    Object result = method.invoke(parameter);
                    if (result != null) {
                        return result.toString();
                    }
                } catch (Exception e) {
                    // 忽略异常，继续尝试其他方法
                }
            }
            
            return null;
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * 从更新语句中提取主键值
     * @param statement 更新语句
     * @return 主键值
     */
    private String extractPrimaryKeyFromUpdate(net.sf.jsqlparser.statement.Statement statement) {
        try {
            net.sf.jsqlparser.statement.update.Update update = (net.sf.jsqlparser.statement.update.Update) statement;
            String tableName = update.getTable().getName();
            
            // 获取表对应的索引配置
            IndexConfig indexConfig = findIndexConfig(tableName);
            if (indexConfig == null) {
                return null;
            }
            
            // 获取主键字段名
            String primaryKeyField = indexConfig.getPrimaryKey();
            
            // 从WHERE条件中提取主键值
            net.sf.jsqlparser.expression.Expression where = update.getWhere();
            if (where == null) {
                return null;
            }
            
            // 解析WHERE条件，查找主键条件
            return extractValueFromCondition(where, primaryKeyField);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * 从删除语句中提取主键值
     * @param statement 删除语句
     * @return 主键值
     */
    private String extractPrimaryKeyFromDelete(net.sf.jsqlparser.statement.Statement statement) {
        try {
            net.sf.jsqlparser.statement.delete.Delete delete = (net.sf.jsqlparser.statement.delete.Delete) statement;
            String tableName = delete.getTable().getName();
            
            // 获取表对应的索引配置
            IndexConfig indexConfig = findIndexConfig(tableName);
            if (indexConfig == null) {
                return null;
            }
            
            // 获取主键字段名
            String primaryKeyField = indexConfig.getPrimaryKey();
            
            // 从WHERE条件中提取主键值
            net.sf.jsqlparser.expression.Expression where = delete.getWhere();
            if (where == null) {
                return null;
            }
            
            // 解析WHERE条件，查找主键条件
            return extractValueFromCondition(where, primaryKeyField);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * 从条件表达式中提取字段值
     * @param expression 条件表达式
     * @param fieldName 字段名
     * @return 字段值
     */
    private String extractValueFromCondition(net.sf.jsqlparser.expression.Expression expression, String fieldName) {
        if (expression instanceof net.sf.jsqlparser.expression.operators.relational.EqualsTo) {
            net.sf.jsqlparser.expression.operators.relational.EqualsTo equalsTo = 
                (net.sf.jsqlparser.expression.operators.relational.EqualsTo) expression;
            
            if (equalsTo.getLeftExpression() instanceof net.sf.jsqlparser.schema.Column) {
                net.sf.jsqlparser.schema.Column column = 
                    (net.sf.jsqlparser.schema.Column) equalsTo.getLeftExpression();
                
                if (column.getColumnName().equals(fieldName)) {
                    return equalsTo.getRightExpression().toString().replaceAll("'", "");
                }
            }
            
            if (equalsTo.getRightExpression() instanceof net.sf.jsqlparser.schema.Column) {
                net.sf.jsqlparser.schema.Column column = 
                    (net.sf.jsqlparser.schema.Column) equalsTo.getRightExpression();
                
                if (column.getColumnName().equals(fieldName)) {
                    return equalsTo.getLeftExpression().toString().replaceAll("'", "");
                }
            }
        } else if (expression instanceof net.sf.jsqlparser.expression.operators.conditional.AndExpression) {
            net.sf.jsqlparser.expression.operators.conditional.AndExpression andExpression = 
                (net.sf.jsqlparser.expression.operators.conditional.AndExpression) expression;
            
            String leftResult = extractValueFromCondition(andExpression.getLeftExpression(), fieldName);
            if (leftResult != null) {
                return leftResult;
            }
            
            return extractValueFromCondition(andExpression.getRightExpression(), fieldName);
        }
        
        return null;
    }
    
    /**
     * 从语句中提取排序分数
     * @param statement SQL语句
     * @return 排序分数
     */
    private double extractScore(net.sf.jsqlparser.statement.Statement statement) {
        // 默认分数为当前时间戳
        return System.currentTimeMillis();
    }
    
    /**
     * 获取受影响的行数
     * @param result 执行结果
     * @return 受影响的行数
     */
    private int getAffectedRows(Object result) {
        if (result == null) {
            return 0;
        }
        if (result instanceof Integer) {
            return (Integer) result;
        }
        if (result instanceof Long) {
            return ((Long) result).intValue();
        }
        if (result instanceof Number) {
            return ((Number) result).intValue(); 
        }
        try {
            return Integer.parseInt(result.toString());
        } catch (Exception e) {
            logger.warn("Failed to parse affected rows from result: {}, error: {}", result, e.getMessage());
            return 0;
        }
    }
    
    /**
     * 执行改写后的SQL
     * @param invocation 调用对象
     * @param ms MappedStatement对象
     * @param boundSql 绑定SQL对象
     * @param newSql 新SQL
     * @return 执行结果
     * @throws Throwable 异常
     */
    private Object executeRewrittenSql(Invocation invocation, MappedStatement ms, BoundSql boundSql, String newSql) throws Throwable {
        // 替换原始SQL
        java.lang.reflect.Field field = boundSql.getClass().getDeclaredField("sql");
        field.setAccessible(true);
        field.set(boundSql, newSql);
        
        // 执行改写后的SQL
        return invocation.proceed();
    }
    
    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }
    
    @Override
    public void setProperties(Properties properties) {
        // do nothing
    }
}