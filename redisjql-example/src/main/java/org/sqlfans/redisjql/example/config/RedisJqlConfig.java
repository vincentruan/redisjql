package org.sqlfans.redisjql.example.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.sqlfans.redisjql.cache.CacheOperationService;
import org.sqlfans.redisjql.config.IndexConfig;
import org.sqlfans.redisjql.config.IndexConfigLoader;
import org.sqlfans.redisjql.interceptor.RedisJqlInterceptor;
import org.sqlfans.redisjql.parser.StatementParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * RedisJQL配置类
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Configuration
public class RedisJqlConfig {

    /**
     * 加载索引配置
     *
     * @return 索引配置对象
     * @throws IOException 配置文件加载异常
     */
    @Bean
    public IndexConfig indexConfig() throws IOException {
        // 从classpath中加载redisjql-index-config.json配置文件
        ClassPathResource resource = new ClassPathResource("redisjql-index-config.json");
        IndexConfigLoader loader = new IndexConfigLoader("");
        List<IndexConfig> configs = loader.load(resource.getInputStream());
        return configs.isEmpty() ? null : configs.get(0);
    }

    @Bean
    public RedisJqlInterceptor redisJqlInterceptor(StatementParser statementParser,
            CacheOperationService cacheOperationService) {
        Set<String> tableWhitelist = new HashSet<>();
        tableWhitelist.add("t_user");
        tableWhitelist.add("t_product");

        Set<String> mapperWhitelist = new HashSet<>();
        mapperWhitelist.add("org.sqlfans.redisjql.example.mapper");

        return new RedisJqlInterceptor(statementParser, cacheOperationService)
                .setTableWhitelist(tableWhitelist)
                .setMapperWhitelist(mapperWhitelist);
    }
}