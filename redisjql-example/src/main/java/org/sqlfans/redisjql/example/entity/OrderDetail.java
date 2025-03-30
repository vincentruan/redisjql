package org.sqlfans.redisjql.example.entity;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 订单详情实体类
 * 用于连接查询，不对应数据库表
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Data
public class OrderDetail {
    /**
     * 订单ID
     */
    private String orderId;
    
    /**
     * 用户ID
     */
    private String userId;
    
    /**
     * 用户姓名
     */
    private String userName;
    
    /**
     * 商品ID
     */
    private String productId;
    
    /**
     * 商品名称
     */
    private String productName;
    
    /**
     * 商品分类
     */
    private String category;
    
    /**
     * 商品价格
     */
    private BigDecimal price;
    
    /**
     * 购买数量
     */
    private Integer quantity;
    
    /**
     * 总价
     */
    private BigDecimal totalPrice;
    
    /**
     * 订单状态
     */
    private String status;
    
    /**
     * 下单时间
     */
    private LocalDateTime orderTime;
    
    /**
     * 订单创建时间
     */
    private LocalDateTime createTime;
} 