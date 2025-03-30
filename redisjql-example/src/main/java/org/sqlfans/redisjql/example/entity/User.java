package org.sqlfans.redisjql.example.entity;

import lombok.Data;
import java.time.LocalDateTime;

/**
 * 用户实体类
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Data
public class User {
    /**
     * 用户ID
     */
    private String id;
    
    /**
     * 用户名
     */
    private String username;
    
    /**
     * 姓名
     */
    private String name;
    
    /**
     * 年龄
     */
    private Integer age;
    
    /**
     * 性别
     */
    private String gender;
    
    /**
     * 邮箱
     */
    private String email;
    
    /**
     * 电话
     */
    private String phone;
    
    /**
     * 地址
     */
    private String address;
    
    /**
     * 创建时间
     */
    private LocalDateTime createTime;
    
    /**
     * 更新时间
     */
    private LocalDateTime updateTime;
    
    /**
     * 版本号
     */
    private Integer version;
} 