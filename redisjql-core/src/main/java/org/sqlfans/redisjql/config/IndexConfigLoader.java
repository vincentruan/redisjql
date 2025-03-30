package org.sqlfans.redisjql.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlfans.redisjql.annotation.RedisIndex;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 索引配置加载器
 * 用于加载和刷新索引配置
 *
 * @author vincentruan
 * @version 1.0.0
 */
public class IndexConfigLoader {
    private static final Logger logger = LoggerFactory.getLogger(IndexConfigLoader.class);
    
    private final Map<String, IndexConfig> indexConfigCache = new ConcurrentHashMap<>();
    private final String basePackage;
    
    public IndexConfigLoader(String basePackage) {
        this.basePackage = basePackage;
    }
    
    /**
     * 加载索引配置
     */
    public List<IndexConfig> loadIndexConfigs() {
        logger.info("Start loading index configurations from package: {}", basePackage);
        List<IndexConfig> configs = new ArrayList<>();
        
        try {
            ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
            scanner.addIncludeFilter(new AnnotationTypeFilter(RedisIndex.class));
            
            for (BeanDefinition bd : scanner.findCandidateComponents(basePackage)) {
                Class<?> clazz = Class.forName(bd.getBeanClassName());
                RedisIndex annotation = clazz.getAnnotation(RedisIndex.class);
                
                if (annotation != null) {
                    IndexConfig config = convertToIndexConfig(annotation);
                    configs.add(config);
                    indexConfigCache.put(config.getTableName(), config);
                    logger.info("Loaded index configuration for table: {}", config.getTableName());
                }
            }
        } catch (Exception e) {
            logger.error("Failed to load index configurations", e);
        }
        
        return configs;
    }
    
    /**
     * 刷新索引配置
     */
    public List<IndexConfig> refreshIndexConfigs() {
        logger.info("Start refreshing index configurations");
        indexConfigCache.clear();
        return loadIndexConfigs();
    }
    
    /**
     * 获取表的索引配置
     * @param tableName 表名
     * @return 索引配置
     */
    public IndexConfig getIndexConfig(String tableName) {
        return indexConfigCache.get(tableName);
    }
    
    /**
     * 将注解转换为索引配置对象
     */
    private IndexConfig convertToIndexConfig(RedisIndex annotation) {
        IndexConfig config = new IndexConfig();
        config.setTableName(annotation.table());
        config.setPrimaryKey(annotation.primaryKey());
        config.setVersionField(annotation.versionField());
        
        for (RedisIndex.Index idx : annotation.indexes()) {
            IndexConfig.IndexDefinition indexDef = new IndexConfig.IndexDefinition();
            indexDef.setName(idx.name());
            indexDef.setSortField(idx.sortField());
            indexDef.setUnique(idx.unique());
            
            for (String field : idx.fields()) {
                indexDef.addField(field);
            }
            
            config.addIndex(indexDef);
        }
        
        return config;
    }

    /**
     * 从输入流加载 JSON 配置
     * @param inputStream 包含 JSON 配置的输入流
     * @return 加载的索引配置列表
     */
    public List<IndexConfig> load(InputStream inputStream) {
        logger.info("开始从 JSON 文件加载索引配置");
        List<IndexConfig> configs = new ArrayList<>();
        
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            IndexConfig[] indexConfigs = objectMapper.readValue(inputStream, IndexConfig[].class);
            
            for (IndexConfig config : indexConfigs) {
                configs.add(config);
                indexConfigCache.put(config.getTableName(), config);
                logger.info("已加载表的索引配置: {}", config.getTableName());
            }
        } catch (Exception e) {
            logger.error("加载 JSON 配置文件失败", e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                logger.error("关闭输入流失败", e);
            }
        }
        
        return configs;
    }
} 