package org.sqlfans.redisjql.example.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.sqlfans.redisjql.example.entity.Order;
import org.sqlfans.redisjql.example.entity.OrderDetail;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 订单Mapper接口
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Mapper
public interface OrderMapper {
    /**
     * 根据ID查询订单
     *
     * @param id 订单ID
     * @return 订单对象
     */
    Order findById(@Param("id") String id);
    
    /**
     * 根据用户ID查询订单
     *
     * @param userId 用户ID
     * @return 订单列表
     */
    List<Order> findByUserId(@Param("userId") String userId);
    
    /**
     * 根据商品ID查询订单
     *
     * @param productId 商品ID
     * @return 订单列表
     */
    List<Order> findByProductId(@Param("productId") String productId);
    
    /**
     * 根据订单状态查询
     *
     * @param status 订单状态
     * @return 订单列表
     */
    List<Order> findByStatus(@Param("status") String status);
    
    /**
     * 根据下单时间范围查询
     *
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 订单列表
     */
    List<Order> findByOrderTimeRange(@Param("startTime") LocalDateTime startTime, @Param("endTime") LocalDateTime endTime);
    
    /**
     * 查询用户购买的商品订单详情
     *
     * @param userId 用户ID
     * @return 订单详情列表
     */
    List<OrderDetail> findOrderDetailsByUserId(@Param("userId") String userId);
    
    /**
     * 查询某个商品的购买记录
     *
     * @param productId 商品ID
     * @return 订单详情列表
     */
    List<OrderDetail> findOrderDetailsByProductId(@Param("productId") String productId);
    
    /**
     * 根据用户ID和商品ID查询购买记录
     *
     * @param userId 用户ID
     * @param productId 商品ID
     * @return 订单详情列表
     */
    List<OrderDetail> findOrderDetailsByUserIdAndProductId(@Param("userId") String userId, @Param("productId") String productId);
    
    /**
     * 插入订单
     *
     * @param order 订单对象
     * @return 影响行数
     */
    int insert(Order order);
    
    /**
     * 更新订单
     *
     * @param order 订单对象
     * @return 影响行数
     */
    int update(Order order);
    
    /**
     * 更新订单状态
     *
     * @param id 订单ID
     * @param status 新状态
     * @param version 版本号
     * @return 影响行数
     */
    int updateStatus(@Param("id") String id, @Param("status") String status, @Param("version") Integer version);
    
    /**
     * 删除订单
     *
     * @param id 订单ID
     * @return 影响行数
     */
    int deleteById(@Param("id") String id);
} 