package org.sqlfans.redisjql.example.entity;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 商品实体类
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Data
public class Product {
    /**
     * 商品ID
     */
    private String id;
    
    /**
     * 商品名称
     */
    private String productName;
    
    /**
     * 商品分类
     */
    private String category;
    
    /**
     * 价格
     */
    private BigDecimal price;
    
    /**
     * 库存
     */
    private Integer stock;
    
    /**
     * 描述
     */
    private String description;
    
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