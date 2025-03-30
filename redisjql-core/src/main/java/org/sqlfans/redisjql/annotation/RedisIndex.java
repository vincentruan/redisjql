package org.sqlfans.redisjql.annotation;

import java.lang.annotation.*;

/**
 * Redis索引注解
 * 用于在实体类上声明Redis索引配置
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RedisIndex {
    /**
     * 表名
     */
    String table() default "";
    
    /**
     * 主键字段
     */
    String primaryKey() default "id";
    
    /**
     * 版本字段
     */
    String versionField() default "jpa_version";
    
    /**
     * 索引配置
     */
    Index[] indexes() default {};
    
    /**
     * 索引定义
     */
    @Target({})
    @Retention(RetentionPolicy.RUNTIME)
    @interface Index {
        /**
         * 索引名称
         */
        String name();
        
        /**
         * 索引字段列表
         */
        String[] fields();
        
        /**
         * 排序字段
         */
        String sortField() default "";
        
        /**
         * 是否是唯一索引
         */
        boolean unique() default false;
    }
}