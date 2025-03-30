package org.sqlfans.redisjql.config;

import java.util.List;
import java.util.ArrayList;

/**
 * 索引配置类
 * 用于定义表的索引字段和 Redis 键的映射关系
 */
public class IndexConfig {
    private String tableName;
    private String primaryKey;
    private String versionField = "jpa_version";
    private List<IndexDefinition> indexes = new ArrayList<>();
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public String getPrimaryKey() {
        return primaryKey;
    }
    
    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }
    
    public String getVersionField() {
        return versionField;
    }
    
    public void setVersionField(String versionField) {
        this.versionField = versionField;
    }
    
    public List<IndexDefinition> getIndexes() {
        return indexes;
    }
    
    public void setIndexes(List<IndexDefinition> indexes) {
        this.indexes = indexes;
    }
    
    public void addIndex(IndexDefinition index) {
        this.indexes.add(index);
    }
    
    /**
     * 索引定义
     */
    public static class IndexDefinition {
        private String name;
        private List<String> fields = new ArrayList<>();
        private String sortField;
        private boolean unique;
        
        public String getName() {
            return name;
        }
        
        public void setName(String name) {
            this.name = name;
        }
        
        public List<String> getFields() {
            return fields;
        }
        
        public void setFields(List<String> fields) {
            this.fields = fields;
        }
        
        public void addField(String field) {
            this.fields.add(field);
        }
        
        public String getSortField() {
            return sortField;
        }
        
        public void setSortField(String sortField) {
            this.sortField = sortField;
        }

        public boolean isUnique() {
            return unique;
        }

        public void setUnique(boolean unique) {
            this.unique = unique;
        }
    }
}