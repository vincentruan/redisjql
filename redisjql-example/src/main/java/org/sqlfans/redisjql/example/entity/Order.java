package org.sqlfans.redisjql.example.entity;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 订单实体类
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Data
public class Order {
    /**
     * 订单ID
     */
    private String id;
    
    /**
     * 用户ID
     */
    private String userId;
    
    /**
     * 商品ID
     */
    private String productId;
    
    /**
     * 购买数量
     */
    private Integer quantity;
    
    /**
     * 总价
     */
    private BigDecimal totalPrice;
    
    /**
     * 下单时间
     */
    private LocalDateTime orderTime;
    
    /**
     * 订单状态
     */
    private String status;
    
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
    
    // 订单关联的用户信息（非数据库字段）
    private User user;
    
    // 订单关联的商品信息（非数据库字段）
    private Product product;
} 