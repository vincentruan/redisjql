package org.sqlfans.redisjql.example.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.lang.NonNull;

/**
 * Web配置类
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    /**
     * 跨域配置
     */
    @Override
    public void addCorsMappings(@NonNull CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOrigins("*")
                .allowedMethods("GET", "POST", "PUT", "DELETE", "PATCH")
                .allowedHeaders("*")
                .maxAge(3600);
    }
} 