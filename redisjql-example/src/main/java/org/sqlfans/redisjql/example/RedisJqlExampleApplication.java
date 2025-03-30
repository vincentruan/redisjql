package org.sqlfans.redisjql.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * RedisJQL示例应用启动类
 *
 * @author vincentruan
 * @version 1.0.0
 */
@SpringBootApplication
@EnableTransactionManagement
public class RedisJqlExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedisJqlExampleApplication.class, args);
    }
} 