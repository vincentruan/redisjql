package org.sqlfans.redisjql.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * RedisJQL 配置属性类
 * 用于管理所有与RedisJQL相关的配置，包括缓存类型、Redis连接信息等
 * 可在application.yml或application.properties中通过redisjql前缀进行配置
 *
 * @author vincentruan
 * @version 1.0.0
 */
@ConfigurationProperties(prefix = "redisjql")
public class RedisJqlProperties {
    
    /**
     * 基础包路径，用于扫描IndexConfig类
     */
    private String basePackage;
    
    /**
     * 缓存配置，包含缓存类型和Redis客户端类型
     */
    private CacheConfig cache = new CacheConfig();
    
    /**
     * Redis连接配置
     */
    private RedisConfig redis = new RedisConfig();
    
    public String getBasePackage() {
        return basePackage;
    }
    
    public void setBasePackage(String basePackage) {
        this.basePackage = basePackage;
    }
    
    public CacheConfig getCache() {
        return cache;
    }
    
    public void setCache(CacheConfig cache) {
        this.cache = cache;
    }
    
    public RedisConfig getRedis() {
        return redis;
    }
    
    public void setRedis(RedisConfig redis) {
        this.redis = redis;
    }
    
    /**
     * 缓存配置类
     * 包含缓存类型和Redis客户端类型的选择
     */
    public static class CacheConfig {
        /**
         * 缓存类型，可选值：
         * - redis: 使用Redis作为缓存存储
         * - local: 使用本地Caffeine缓存
         */
        private String type = "redis";
        
        /**
         * Redis客户端类型，可选值：
         * - jedis: 使用Jedis客户端
         * - lettuce: 使用Lettuce反应式客户端
         */
        private String redisClient = "jedis";
        
        public String getType() {
            return type;
        }
        
        public void setType(String type) {
            this.type = type;
        }
        
        public String getRedisClient() {
            return redisClient;
        }
        
        public void setRedisClient(String redisClient) {
            this.redisClient = redisClient;
        }
    }
    
    /**
     * Redis配置类
     * 包含Redis连接的详细配置
     */
    public static class RedisConfig {
        /**
         * Redis服务器主机地址
         */
        private String host = "localhost";
        
        /**
         * Redis服务器端口
         */
        private int port = 6379;
        
        /**
         * Redis服务器密码，如果没有可留空
         */
        private String password = "";
        
        /**
         * Redis数据库索引
         */
        private int database = 0;
        
        /**
         * Redis连接超时时间(毫秒)
         */
        private int timeout = 2000;
        
        /**
         * Redis连接池配置
         */
        private PoolConfig pool = new PoolConfig();
        
        public String getHost() {
            return host;
        }
        
        public void setHost(String host) {
            this.host = host;
        }
        
        public int getPort() {
            return port;
        }
        
        public void setPort(int port) {
            this.port = port;
        }
        
        public String getPassword() {
            return password;
        }
        
        public void setPassword(String password) {
            this.password = password;
        }
        
        public int getDatabase() {
            return database;
        }
        
        public void setDatabase(int database) {
            this.database = database;
        }
        
        public int getTimeout() {
            return timeout;
        }
        
        public void setTimeout(int timeout) {
            this.timeout = timeout;
        }
        
        public PoolConfig getPool() {
            return pool;
        }
        
        public void setPool(PoolConfig pool) {
            this.pool = pool;
        }
        
        /**
         * Redis连接池配置类
         */
        public static class PoolConfig {
            /**
             * 连接池最大连接数
             */
            private int maxTotal = 8;
            
            /**
             * 连接池中最大空闲连接数
             */
            private int maxIdle = 8;
            
            /**
             * 连接池中最小空闲连接数
             */
            private int minIdle = 0;
            
            public int getMaxTotal() {
                return maxTotal;
            }
            
            public void setMaxTotal(int maxTotal) {
                this.maxTotal = maxTotal;
            }
            
            public int getMaxIdle() {
                return maxIdle;
            }
            
            public void setMaxIdle(int maxIdle) {
                this.maxIdle = maxIdle;
            }
            
            public int getMinIdle() {
                return minIdle;
            }
            
            public void setMinIdle(int minIdle) {
                this.minIdle = minIdle;
            }
        }
    }
}