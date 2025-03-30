package org.sqlfans.redisjql.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import org.sqlfans.redisjql.cache.CacheOperationService;
import org.sqlfans.redisjql.cache.caffine.CaffeineCacheOperationService;
import org.sqlfans.redisjql.cache.redis.JedisCacheOperationService;
import org.sqlfans.redisjql.cache.redis.LettuceCacheOperationService;
import org.sqlfans.redisjql.parser.SelectParser;
import org.sqlfans.redisjql.parser.impl.SelectParserImpl;

/**
 * RedisJQL自动配置类
 * 负责根据配置自动创建和装配相关的Bean，包括Redis连接、缓存服务和解析器等
 * 通过Spring Boot的自动配置机制，简化RedisJQL的使用和集成
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Configuration
@EnableConfigurationProperties(RedisJqlProperties.class)
public class RedisJqlAutoConfiguration {
    
    /**
     * RedisJQL配置属性
     */
    private final RedisJqlProperties properties;
    
    /**
     * 构造函数，注入配置属性
     * @param properties RedisJQL配置属性
     */
    public RedisJqlAutoConfiguration(RedisJqlProperties properties) {
        this.properties = properties;
    }
    
    /**
     * 创建JedisPool连接池Bean
     * 用于Jedis客户端连接Redis服务器
     * 仅当cache.type=redis且redis.client=jedis时创建
     * 
     * @return JedisPool连接池实例
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "redisjql", name = {"cache.type", "cache.redis-client"}, havingValue = "jedis", matchIfMissing = false)
    public JedisPool jedisPool() {
        RedisJqlProperties.RedisConfig redisConfig = properties.getRedis();
        RedisJqlProperties.RedisConfig.PoolConfig poolConfig = redisConfig.getPool();
        
        // 构建Jedis连接池配置
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(poolConfig.getMaxTotal());
        config.setMaxIdle(poolConfig.getMaxIdle());
        config.setMinIdle(poolConfig.getMinIdle());
        
        // 创建连接池，根据是否配置密码选择不同的构造函数
        String password = redisConfig.getPassword();
        if (password != null && !password.isEmpty()) {
            return new JedisPool(config, redisConfig.getHost(), redisConfig.getPort(), 
                                redisConfig.getTimeout(), password, redisConfig.getDatabase());
        } else {
            return new JedisPool(config, redisConfig.getHost(), redisConfig.getPort(), redisConfig.getTimeout());
        }
    }
    
    /**
     * 创建ReactiveRedisConnectionFactory连接工厂Bean
     * 用于反应式Redis客户端连接
     * 仅当cache.type=redis且redis.client=lettuce时创建
     * 
     * @return ReactiveRedisConnectionFactory实例
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "redisjql", name = {"cache.type", "cache.redis-client"}, havingValue = "lettuce", matchIfMissing = false)
    public ReactiveRedisConnectionFactory reactiveRedisConnectionFactory() {
        RedisJqlProperties.RedisConfig redisConfig = properties.getRedis();
        
        // 构建Redis连接配置
        RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration();
        configuration.setHostName(redisConfig.getHost());
        configuration.setPort(redisConfig.getPort());
        configuration.setDatabase(redisConfig.getDatabase());
        
        // 设置密码（如果有）
        String password = redisConfig.getPassword();
        if (password != null && !password.isEmpty()) {
            configuration.setPassword(password);
        }
        
        // 创建Lettuce连接工厂（支持反应式编程）
        return new LettuceConnectionFactory(configuration);
    }
    
    /**
     * 创建ReactiveRedisTemplate Bean
     * 用于反应式Redis操作
     * 仅当cache.type=redis且redis.client=lettuce时创建
     * 
     * @param factory Redis连接工厂
     * @return ReactiveRedisTemplate实例
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "redisjql", name = {"cache.type", "cache.redis-client"}, havingValue = "lettuce", matchIfMissing = false)
    public ReactiveRedisTemplate<String, String> reactiveRedisTemplate(ReactiveRedisConnectionFactory factory) {
        return new ReactiveStringRedisTemplate(factory);
    }
    
    /**
     * 创建IndexConfigLoader Bean
     * 用于加载索引配置
     * 
     * @return IndexConfigLoader实例
     */
    @Bean
    @ConditionalOnMissingBean
    public IndexConfigLoader indexConfigLoader() {
        return new IndexConfigLoader(properties.getBasePackage());
    }
    
    /**
     * 创建本地缓存实现的CacheOperationService Bean
     * 当cache.type=local时创建
     * 
     * @return CaffeineCacheOperationService实例
     */
    @Bean
    @ConditionalOnMissingBean(CacheOperationService.class)
    @ConditionalOnProperty(prefix = "redisjql", name = "cache.type", havingValue = "local")
    public CacheOperationService caffeineCacheOperationService() {
        return new CaffeineCacheOperationService();
    }
    
    /**
     * 创建Jedis实现的CacheOperationService Bean
     * 当cache.type=redis且redis.client=jedis时创建
     * 
     * @param jedisPool Jedis连接池
     * @return JedisCacheOperationService实例
     */
    @Bean
    @ConditionalOnMissingBean(CacheOperationService.class)
    @ConditionalOnProperty(prefix = "redisjql", name = {"cache.type", "cache.redis-client"}, havingValue = "jedis", matchIfMissing = false)
    public CacheOperationService jedisCacheOperationService(JedisPool jedisPool) {
        return new JedisCacheOperationService(jedisPool);
    }
    
    /**
     * 创建Lettuce实现的CacheOperationService Bean
     * 当cache.type=redis且redis.client=lettuce时创建
     * 
     * @param redisTemplate 反应式Redis模板
     * @return LettuceCacheOperationService实例
     */
    @Bean
    @ConditionalOnMissingBean(CacheOperationService.class)
    @ConditionalOnProperty(prefix = "redisjql", name = {"cache.type", "cache.redis-client"}, havingValue = "lettuce", matchIfMissing = true)
    public CacheOperationService lettuceCacheOperationService(ReactiveRedisTemplate<String, String> redisTemplate) {
        return new LettuceCacheOperationService(redisTemplate);
    }
    
    /**
     * 创建SelectParser Bean
     * 用于解析SQL SELECT语句
     * 
     * @param cacheOperationService 缓存操作服务
     * @param indexConfigLoader 索引配置加载器
     * @return SelectParser实例
     */
    @Bean
    @ConditionalOnMissingBean
    public SelectParser selectParser(CacheOperationService cacheOperationService, 
                                   IndexConfigLoader indexConfigLoader) {
        return new SelectParserImpl(cacheOperationService, indexConfigLoader.loadIndexConfigs());
    }
} 