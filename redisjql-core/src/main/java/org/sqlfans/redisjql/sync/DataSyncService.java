package org.sqlfans.redisjql.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.sqlfans.redisjql.cache.CacheOperationService;
import org.sqlfans.redisjql.config.IndexConfig;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 数据同步服务
 * 用于定期同步 MySQL 和 Redis 的数据
 */
public class DataSyncService {
    private static final Logger logger = LoggerFactory.getLogger(DataSyncService.class);

    private JdbcTemplate jdbcTemplate;
    private CacheOperationService redisOperationService;
    private List<IndexConfig> indexConfigs;
    private ScheduledExecutorService scheduler;
    private int syncIntervalMinutes = 10;
    private int fullSyncIntervalHours = 24;
    private String lastModifiedField = "update_time"; // 默认最后修改时间字段
    private int batchSize = 1000; // 批处理大小
    private Map<String, LocalDateTime> lastSyncTimeMap = new HashMap<>(); // 记录每个表最后同步时间
    
    public DataSyncService(JdbcTemplate jdbcTemplate, CacheOperationService redisOperationService, List<IndexConfig> indexConfigs) {
        this.jdbcTemplate = jdbcTemplate;
        this.redisOperationService = redisOperationService;
        this.indexConfigs = indexConfigs;
        this.scheduler = Executors.newScheduledThreadPool(2); // 增加线程池大小，支持增量和全量同步
    }
    
    /**
     * 启动同步服务
     */
    public void start() {
        logger.info("Starting data sync service with incremental sync interval of {} minutes and full sync interval of {} hours", syncIntervalMinutes, fullSyncIntervalHours);
        // 启动增量同步任务
        scheduler.scheduleAtFixedRate(this::incrementalSync, 0, syncIntervalMinutes, TimeUnit.MINUTES);
        // 启动全量同步任务
        scheduler.scheduleAtFixedRate(this::fullSyncAll, 1, fullSyncIntervalHours, TimeUnit.HOURS);
    }
    
    /**
     * 停止同步服务
     */
    public void stop() {
        logger.info("Stopping data sync service");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 设置同步间隔时间
     * @param minutes 分钟
     */
    public void setSyncIntervalMinutes(int minutes) {
        this.syncIntervalMinutes = minutes;
        logger.info("Incremental sync interval set to {} minutes", minutes);
    }
    
    /**
     * 设置全量同步间隔时间
     * @param hours 小时
     */
    public void setFullSyncIntervalHours(int hours) {
        this.fullSyncIntervalHours = hours;
        logger.info("Full sync interval set to {} hours", hours);
    }
    
    /**
     * 设置最后修改时间字段
     * @param fieldName 字段名
     */
    public void setLastModifiedField(String fieldName) {
        this.lastModifiedField = fieldName;
        logger.info("Last modified field set to {}", fieldName);
    }
    
    /**
     * 设置批处理大小
     * @param size 批处理大小
     */
    public void setBatchSize(int size) {
        this.batchSize = size;
        logger.info("Batch size set to {}", size);
    }
    
    /**
     * 增量同步数据
     */
    private void incrementalSync() {
        try {
            logger.info("Starting incremental data synchronization");
            for (IndexConfig config : indexConfigs) {
                incrementalSyncTable(config);
            }
            logger.info("Incremental data synchronization completed");
        } catch (Exception e) {
            logger.error("Error during incremental data synchronization", e);
        }
    }
    
    /**
     * 全量同步所有表
     */
    private void fullSyncAll() {
        try {
            logger.info("Starting full data synchronization");
            for (IndexConfig config : indexConfigs) {
                syncTable(config);
            }
            logger.info("Full data synchronization completed");
        } catch (Exception e) {
            logger.error("Error during full data synchronization", e);
        }
    }
    
    /**
     * 增量同步表数据
     * @param config 索引配置
     */
    private void incrementalSyncTable(IndexConfig config) {
        String tableName = config.getTableName();
        
        // 检查表是否有最后修改时间字段
        if (!hasLastModifiedField(tableName, lastModifiedField)) {
            logger.info("Table {} does not have last modified field {}, skipping incremental sync", tableName, lastModifiedField);
            return;
        }
        
        LocalDateTime lastSyncTime = lastSyncTimeMap.getOrDefault(tableName, LocalDateTime.now().minusDays(1));
        logger.info("Starting incremental sync for table {}, last sync time: {}", tableName, lastSyncTime);
        
        // 获取最后修改时间大于上次同步时间的记录
        String timeFormat = lastSyncTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String sql = String.format("SELECT %s FROM %s WHERE %s > '%s' ORDER BY %s", 
                                  config.getPrimaryKey(), tableName, lastModifiedField, timeFormat, lastModifiedField);
        
        List<String> updatedPrimaryKeys = jdbcTemplate.queryForList(sql, String.class);
        
        if (updatedPrimaryKeys.isEmpty()) {
            logger.info("No data to sync incrementally for table {}", tableName);
            return;
        }
        
        logger.info("Found {} records to sync incrementally for table {}", updatedPrimaryKeys.size(), tableName);
        
        // 分批处理更新的记录
        for (int i = 0; i < updatedPrimaryKeys.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, updatedPrimaryKeys.size());
            List<String> batch = updatedPrimaryKeys.subList(i, endIndex);
            
            // 同步这批记录
            syncBatchRecords(config, new HashSet<>(batch));
        }
        
        // 更新最后同步时间
        lastSyncTimeMap.put(tableName, LocalDateTime.now());
    }
    
    /**
     * 检查表是否有最后修改时间字段
     * @param tableName 表名
     * @param fieldName 字段名
     * @return 是否有该字段
     */
    private boolean hasLastModifiedField(String tableName, String fieldName) {
        try {
            String sql = "SELECT COUNT(*) FROM information_schema.columns " +
                         "WHERE table_name = ? AND column_name = ?";
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, tableName, fieldName);
            return count != null && count > 0;
        } catch (Exception e) {
            logger.error("Error checking if table {} has field {}", tableName, fieldName, e);
            return false;
        }
    }
    
    /**
     * 同步表数据
     * @param config 索引配置
     */
    private void syncTable(IndexConfig config) {
        String tableName = config.getTableName();
        
        logger.info("Starting synchronization for table {}", tableName);
        
        // 1. 从数据库获取记录数量
        String countSql = "SELECT COUNT(*) FROM " + tableName;
        Integer dbRowCount = jdbcTemplate.queryForObject(countSql, Integer.class);
        
        if (dbRowCount == null || dbRowCount == 0) {
            logger.info("Table {} is empty, nothing to synchronize", tableName);
            return;
        }
        
        // 2. 获取Redis中记录的所有主键
        Set<String> redisKeys = getAllPrimaryKeys(tableName);
        int redisRowCount = redisKeys.size();
        
        logger.info("Table {}: Database has {} rows, Redis has {} records", 
                    tableName, dbRowCount, redisRowCount);
        
        // 3. 检查数据一致性
        if (Math.abs(dbRowCount - redisRowCount) < dbRowCount * 0.01) { // 如果差异小于1%，只做主键级别的对比
            logger.info("Record count is close for table {}, performing primary key level verification", tableName);
            verifyDataConsistencyByPrimaryKey(config);
        } else {
            logger.warn("Record count mismatch for table {}: DB={}, Redis={}", 
                       tableName, dbRowCount, redisRowCount);
            // 记录数量差异较大，需要分批同步
            batchFullSync(config);
        }
    }
    
    /**
     * 分批全量同步
     * @param config 索引配置
     */
    private void batchFullSync(IndexConfig config) {
        String tableName = config.getTableName();
        String primaryKey = config.getPrimaryKey();
        
        logger.info("Performing batch full synchronization for table {}", tableName);
        
        // 1. 获取所有主键
        String sql = "SELECT " + primaryKey + " FROM " + tableName;
        List<String> allPrimaryKeys = jdbcTemplate.queryForList(sql, String.class);
        
        // 2. 获取Redis中的所有主键
        Set<String> redisKeys = getAllPrimaryKeys(tableName);
        
        // 3. 找出需要添加和删除的主键
        Set<String> dbKeySet = new HashSet<>(allPrimaryKeys);
        Set<String> missingInRedis = new HashSet<>(dbKeySet);
        missingInRedis.removeAll(redisKeys);
        
        Set<String> extraInRedis = new HashSet<>(redisKeys);
        extraInRedis.removeAll(dbKeySet);
        
        logger.info("Data consistency check for table {}: {} missing in Redis, {} extra in Redis",
                   tableName, missingInRedis.size(), extraInRedis.size());
        
        // 4. 分批处理缺失的记录
        for (int i = 0; i < missingInRedis.size(); i += batchSize) {
            Set<String> batch = new HashSet<>();
            Iterator<String> iterator = missingInRedis.iterator();
            
            int count = 0;
            while (iterator.hasNext() && count < batchSize) {
                batch.add(iterator.next());
                count++;
            }
            
            syncBatchRecords(config, batch);
        }
        
        // 5. 处理多余的记录
        if (!extraInRedis.isEmpty()) {
            removeExtraRecords(tableName, extraInRedis);
        }
    }
    
    /**
     * 通过主键验证数据一致性
     * @param config 索引配置
     */
    private void verifyDataConsistencyByPrimaryKey(IndexConfig config) {
        String tableName = config.getTableName();
        String primaryKey = config.getPrimaryKey();
        
        // 从数据库获取主键列表
        String sql = "SELECT " + primaryKey + " FROM " + tableName;
        List<String> dbPrimaryKeys = jdbcTemplate.queryForList(sql, String.class);
        
        // 获取Redis中的主键
        Set<String> redisKeys = getAllPrimaryKeys(tableName);
        
        // 对比主键列表
        Set<String> dbKeySet = new HashSet<>(dbPrimaryKeys);
        
        // 找出在DB中但不在Redis中的记录
        Set<String> missingInRedis = new HashSet<>(dbKeySet);
        missingInRedis.removeAll(redisKeys);
        
        // 找出在Redis中但不在DB中的记录
        Set<String> extraInRedis = new HashSet<>(redisKeys);
        extraInRedis.removeAll(dbKeySet);
        
        logger.info("Data consistency check for table {}: {} missing in Redis, {} extra in Redis",
                   tableName, missingInRedis.size(), extraInRedis.size());
        
        // 处理不一致的数据
        if (!missingInRedis.isEmpty()) {
            // 同步DB中有但Redis中没有的记录
            syncBatchRecords(config, missingInRedis);
        }
        
        if (!extraInRedis.isEmpty()) {
            // 删除Redis中有但DB中没有的记录
            removeExtraRecords(tableName, extraInRedis);
        }
    }
    
    /**
     * 获取Redis中某个表的所有主键
     * @param tableName 表名
     * @return 主键集合
     */
    private Set<String> getAllPrimaryKeys(String tableName) {
        // 通过Redis的keys命令获取所有主键映射
        // 由于性能考虑，实际生产中可能需要其他实现方式
        Set<String> allKeys = redisOperationService.getAllKeys(tableName + "_*");
        Set<String> primaryKeys = new HashSet<>();
        
        for (String key : allKeys) {
            // 排除标记为删除的键
            if (!key.endsWith("_deleted")) {
                String[] parts = key.split("_", 2);
                if (parts.length > 1) {
                    primaryKeys.add(parts[1]);
                }
            }
        }
        
        return primaryKeys;
    }
    
    /**
     * 同步一批记录
     * @param config 索引配置
     * @param batchKeys 需要同步的主键集合
     */
    private void syncBatchRecords(IndexConfig config, Set<String> batchKeys) {
        String tableName = config.getTableName();
        String primaryKey = config.getPrimaryKey();
        
        if (batchKeys.isEmpty()) {
            return;
        }
        
        logger.info("Synchronizing {} records for table {}", batchKeys.size(), tableName);
        
        // 构建IN条件
        StringBuilder inClause = new StringBuilder();
        int i = 0;
        for (String key : batchKeys) {
            if (i > 0) {
                inClause.append(",");
            }
            inClause.append("'").append(key).append("'");
            i++;
            
            // 如果IN子句过长，分批查询
            if (i % batchSize == 0 || i == batchKeys.size()) {
                // 从数据库获取记录
                String sql = "SELECT * FROM " + tableName + " WHERE " + primaryKey + " IN (" + inClause + ")";
                List<Map<String, Object>> records = jdbcTemplate.queryForList(sql);
                
                // 更新Redis索引
                for (Map<String, Object> record : records) {
                    updateRedisIndices(config, record);
                }
                
                // 重置IN子句
                inClause = new StringBuilder();
                i = 0;
            }
        }
    }
    
    /**
     * 删除Redis中多余的记录
     * @param tableName 表名
     * @param extraKeys 多余的主键集合
     */
    private void removeExtraRecords(String tableName, Set<String> extraKeys) {
        if (extraKeys.isEmpty()) {
            return;
        }
        
        logger.info("Removing {} extra records for table {}", extraKeys.size(), tableName);
        
        // 标记删除
        for (String key : extraKeys) {
            redisOperationService.markForDeletion(tableName, key);
        }
    }
    
    /**
     * 更新Redis索引
     * @param config 索引配置
     * @param record 记录数据
     */
    private void updateRedisIndices(IndexConfig config, Map<String, Object> record) {
        String tableName = config.getTableName();
        String primaryKey = config.getPrimaryKey();
        
        // 获取主键值
        Object pkValue = record.get(primaryKey);
        if (pkValue == null) {
            logger.warn("Record missing primary key {}, skipping", primaryKey);
            return;
        }
        
        String pkStr = pkValue.toString();
        
        // 1. 存储所有字段
        String dataKey = tableName + ":" + pkStr;
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            if (entry.getValue() != null) {
                redisOperationService.addDataField(dataKey, entry.getKey(), entry.getValue().toString());
            }
        }
        
        // 2. 更新所有索引
        for (IndexConfig.IndexDefinition indexDef : config.getIndexes()) {
            // 处理索引字段
            List<String> fields = indexDef.getFields();
            if (fields == null || fields.isEmpty()) {
                continue;
            }
            
            // 处理复合索引
            if (fields.size() > 1) {
                boolean allFieldsExist = true;
                for (String field : fields) {
                    if (!record.containsKey(field) || record.get(field) == null) {
                        allFieldsExist = false;
                        break;
                    }
                }
                
                if (allFieldsExist) {
                    // 构建复合索引键
                    StringBuilder indexKeyBuilder = new StringBuilder(tableName);
                    for (String field : fields) {
                        Object fieldValue = record.get(field);
                        indexKeyBuilder.append(":").append(field).append(":").append(fieldValue);
                    }
                    String indexKey = indexKeyBuilder.toString();
                    
                    // 添加到索引
                    double score = 0;
                    String sortField = indexDef.getSortField();
                    if (sortField != null && record.containsKey(sortField) && record.get(sortField) != null) {
                        try {
                            Object sortValue = record.get(sortField);
                            if (sortValue instanceof Number) {
                                score = ((Number) sortValue).doubleValue();
                            } else {
                                score = Double.parseDouble(sortValue.toString());
                            }
                        } catch (NumberFormatException e) {
                            logger.warn("Sort field {} value is not a number: {}", sortField, record.get(sortField));
                        }
                    }
                    
                    redisOperationService.addIndexRecord(indexKey, pkStr, score);
                    redisOperationService.addPrimaryKeyToIndexMapping(tableName, pkStr, indexKey);
                }
            } else {
                // 处理单字段索引
                String fieldName = fields.get(0);
                if (record.containsKey(fieldName) && record.get(fieldName) != null) {
                    Object fieldValue = record.get(fieldName);
                    
                    // 处理多值索引
                    if (fieldValue instanceof String && ((String) fieldValue).contains(",")) {
                        String[] values = ((String) fieldValue).split(",");
                        for (String value : values) {
                            String indexKey = tableName + ":" + fieldName + ":" + value.trim();
                            
                            // 添加到索引
                            double score = 0;
                            String sortField = indexDef.getSortField();
                            if (sortField != null && record.containsKey(sortField) && record.get(sortField) != null) {
                                try {
                                    Object sortValue = record.get(sortField);
                                    if (sortValue instanceof Number) {
                                        score = ((Number) sortValue).doubleValue();
                                    } else {
                                        score = Double.parseDouble(sortValue.toString());
                                    }
                                } catch (NumberFormatException e) {
                                    logger.warn("Sort field {} value is not a number: {}", sortField, record.get(sortField));
                                }
                            }
                            
                            redisOperationService.addIndexRecord(indexKey, pkStr, score);
                            redisOperationService.addPrimaryKeyToIndexMapping(tableName, pkStr, indexKey);
                        }
                    } else {
                        String indexKey = tableName + ":" + fieldName + ":" + fieldValue;
                        
                        // 添加到索引
                        double score = 0;
                        String sortField = indexDef.getSortField();
                        if (sortField != null && record.containsKey(sortField) && record.get(sortField) != null) {
                            try {
                                Object sortValue = record.get(sortField);
                                if (sortValue instanceof Number) {
                                    score = ((Number) sortValue).doubleValue();
                                } else {
                                    score = Double.parseDouble(sortValue.toString());
                                }
                            } catch (NumberFormatException e) {
                                logger.warn("Sort field {} value is not a number: {}", sortField, record.get(sortField));
                            }
                        }
                        
                        redisOperationService.addIndexRecord(indexKey, pkStr, score);
                        redisOperationService.addPrimaryKeyToIndexMapping(tableName, pkStr, indexKey);
                    }
                }
            }
        }
        
        // 3. 添加版本字段索引
        String versionField = config.getVersionField();
        if (versionField != null && !versionField.isEmpty() && 
            record.containsKey(versionField) && record.get(versionField) != null) {
            Object versionValue = record.get(versionField);
            String versionIndexKey = tableName + ":" + versionField + ":" + versionValue;
            
            redisOperationService.addIndexRecord(versionIndexKey, pkStr, 0);
            redisOperationService.addPrimaryKeyToIndexMapping(tableName, pkStr, versionIndexKey);
        }
    }
}