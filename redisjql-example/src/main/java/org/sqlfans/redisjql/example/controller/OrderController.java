package org.sqlfans.redisjql.example.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sqlfans.redisjql.example.entity.Order;
import org.sqlfans.redisjql.example.entity.OrderDetail;
import org.sqlfans.redisjql.example.service.OrderService;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 订单控制器
 *
 * @author vincentruan
 * @version 1.0.0
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    @Autowired
    private OrderService orderService;

    /**
     * 根据ID查询订单
     *
     * @param id 订单ID
     * @return 订单信息
     */
    @GetMapping("/{id}")
    public ResponseEntity<Order> getOrderById(@PathVariable String id) {
        Order order = orderService.findById(id);
        if (order == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(order);
    }

    /**
     * 根据用户ID查询订单
     *
     * @param userId 用户ID
     * @return 订单列表
     */
    @GetMapping("/user/{userId}")
    public ResponseEntity<List<Order>> getOrdersByUserId(@PathVariable String userId) {
        List<Order> orders = orderService.findByUserId(userId);
        return ResponseEntity.ok(orders);
    }

    /**
     * 根据商品ID查询订单
     *
     * @param productId 商品ID
     * @return 订单列表
     */
    @GetMapping("/product/{productId}")
    public ResponseEntity<List<Order>> getOrdersByProductId(@PathVariable String productId) {
        List<Order> orders = orderService.findByProductId(productId);
        return ResponseEntity.ok(orders);
    }

    /**
     * 根据订单状态查询
     *
     * @param status 订单状态
     * @return 订单列表
     */
    @GetMapping("/status/{status}")
    public ResponseEntity<List<Order>> getOrdersByStatus(@PathVariable String status) {
        List<Order> orders = orderService.findByStatus(status);
        return ResponseEntity.ok(orders);
    }

    /**
     * 根据下单时间范围查询
     *
     * @param startTime 开始时间
     * @param endTime   结束时间
     * @return 订单列表
     */
    @GetMapping("/time-range")
    public ResponseEntity<List<Order>> getOrdersByTimeRange(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startTime,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endTime) {
        List<Order> orders = orderService.findByOrderTimeRange(startTime, endTime);
        return ResponseEntity.ok(orders);
    }

    /**
     * 查询用户购买的商品订单详情
     *
     * @param userId 用户ID
     * @return 订单详情列表
     */
    @GetMapping("/details/user/{userId}")
    public ResponseEntity<List<OrderDetail>> getOrderDetailsByUserId(@PathVariable String userId) {
        List<OrderDetail> orderDetails = orderService.findOrderDetailsByUserId(userId);
        return ResponseEntity.ok(orderDetails);
    }

    /**
     * 查询某个商品的购买记录
     *
     * @param productId 商品ID
     * @return 订单详情列表
     */
    @GetMapping("/details/product/{productId}")
    public ResponseEntity<List<OrderDetail>> getOrderDetailsByProductId(@PathVariable String productId) {
        List<OrderDetail> orderDetails = orderService.findOrderDetailsByProductId(productId);
        return ResponseEntity.ok(orderDetails);
    }

    /**
     * 根据用户ID和商品ID查询购买记录
     *
     * @param userId    用户ID
     * @param productId 商品ID
     * @return 订单详情列表
     */
    @GetMapping("/details/user/{userId}/product/{productId}")
    public ResponseEntity<List<OrderDetail>> getOrderDetailsByUserIdAndProductId(
            @PathVariable String userId,
            @PathVariable String productId) {
        List<OrderDetail> orderDetails = orderService.findOrderDetailsByUserIdAndProductId(userId, productId);
        return ResponseEntity.ok(orderDetails);
    }

    /**
     * 创建订单
     *
     * @param order 订单信息
     * @return 创建的订单
     */
    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody Order order) {
        Order createdOrder = orderService.createOrder(order);
        return ResponseEntity.ok(createdOrder);
    }

    /**
     * 更新订单
     *
     * @param id    订单ID
     * @param order 订单信息
     * @return 更新后的订单
     */
    @PutMapping("/{id}")
    public ResponseEntity<Order> updateOrder(@PathVariable String id, @RequestBody Order order) {
        order.setId(id);
        Order updatedOrder = orderService.updateOrder(order);
        return ResponseEntity.ok(updatedOrder);
    }

    /**
     * 更新订单状态
     *
     * @param id     订单ID
     * @param status 订单状态
     * @return 操作结果
     */
    @PatchMapping("/{id}/status")
    public ResponseEntity<Void> updateOrderStatus(
            @PathVariable String id,
            @RequestParam String status) {
        boolean success = orderService.updateStatus(id, status);
        if (success) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * 删除订单
     *
     * @param id 订单ID
     * @return 操作结果
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteOrder(@PathVariable String id) {
        boolean success = orderService.deleteOrder(id);
        if (success) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }
} 