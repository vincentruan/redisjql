package org.sqlfans.redisjql.example.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.sqlfans.redisjql.config.IndexConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 首页控制器
 *
 * @author vincentruan
 * @version 1.0.0
 */
@RestController
@RequestMapping("/")
public class IndexController {

    @Autowired
    private IndexConfig indexConfig;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    /**
     * 获取系统信息
     *
     * @return 系统信息
     */
    @GetMapping
    public Map<String, Object> index() {
        Map<String, Object> result = new HashMap<>();
        
        // 应用信息
        result.put("name", "RedisJQL Example Application");
        result.put("version", "1.0.0");
        result.put("description", "RedisJQL示例应用，演示Redis和SQL结合实现高性能查询");
        
        // 系统信息
        Properties props = System.getProperties();
        Map<String, String> system = new HashMap<>();
        system.put("os", props.getProperty("os.name") + " " + props.getProperty("os.version"));
        system.put("java", props.getProperty("java.version"));
        system.put("memory", Runtime.getRuntime().maxMemory() / (1024 * 1024) + "MB");
        result.put("system", system);
        
        // RedisJQL配置信息
        result.put("indexConfig", indexConfig);
        
        // Redis连接检测
        boolean redisConnected = false;
        try {
            redisConnected = redisTemplate != null && 
                             redisTemplate.getConnectionFactory() != null && 
                             redisTemplate.getConnectionFactory().getConnection().ping() != null;
        } catch (Exception e) {
            // 连接失败
        }
        result.put("redisConnected", redisConnected);
        
        // API列表
        Map<String, String> apis = new HashMap<>();
        apis.put("用户API", "/api/users");
        apis.put("商品API", "/api/products");
        apis.put("订单API", "/api/orders");
        apis.put("示例接口", "/api/demo");
        apis.put("H2控制台", "/h2-console");
        result.put("apis", apis);
        
        return result;
    }
} 