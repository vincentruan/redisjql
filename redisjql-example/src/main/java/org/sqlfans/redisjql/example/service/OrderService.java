package org.sqlfans.redisjql.example.service;

import org.sqlfans.redisjql.example.entity.Order;
import org.sqlfans.redisjql.example.entity.OrderDetail;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 订单服务接口
 *
 * @author vincentruan
 * @version 1.0.0
 */
public interface OrderService {
    /**
     * 根据ID查询订单
     *
     * @param id 订单ID
     * @return 订单对象
     */
    Order findById(String id);
    
    /**
     * 根据用户ID查询订单
     *
     * @param userId 用户ID
     * @return 订单列表
     */
    List<Order> findByUserId(String userId);
    
    /**
     * 根据商品ID查询订单
     *
     * @param productId 商品ID
     * @return 订单列表
     */
    List<Order> findByProductId(String productId);
    
    /**
     * 根据订单状态查询
     *
     * @param status 订单状态
     * @return 订单列表
     */
    List<Order> findByStatus(String status);
    
    /**
     * 根据下单时间范围查询
     *
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 订单列表
     */
    List<Order> findByOrderTimeRange(LocalDateTime startTime, LocalDateTime endTime);
    
    /**
     * 查询用户购买的商品订单详情
     *
     * @param userId 用户ID
     * @return 订单详情列表
     */
    List<OrderDetail> findOrderDetailsByUserId(String userId);
    
    /**
     * 查询某个商品的购买记录
     *
     * @param productId 商品ID
     * @return 订单详情列表
     */
    List<OrderDetail> findOrderDetailsByProductId(String productId);
    
    /**
     * 根据用户ID和商品ID查询购买记录
     *
     * @param userId 用户ID
     * @param productId 商品ID
     * @return 订单详情列表
     */
    List<OrderDetail> findOrderDetailsByUserIdAndProductId(String userId, String productId);
    
    /**
     * 创建订单
     *
     * @param order 订单对象
     * @return 创建成功的订单
     */
    Order createOrder(Order order);
    
    /**
     * 更新订单
     *
     * @param order 订单对象
     * @return 更新后的订单
     */
    Order updateOrder(Order order);
    
    /**
     * 更新订单状态
     *
     * @param id 订单ID
     * @param status 新状态
     * @return 更新成功返回true，否则返回false
     */
    boolean updateStatus(String id, String status);
    
    /**
     * 删除订单
     *
     * @param id 订单ID
     * @return 删除成功返回true，否则返回false
     */
    boolean deleteOrder(String id);
} 