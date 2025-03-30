package org.sqlfans.redisjql.example.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.sqlfans.redisjql.example.entity.Order;
import org.sqlfans.redisjql.example.entity.OrderDetail;
import org.sqlfans.redisjql.example.entity.Product;
import org.sqlfans.redisjql.example.mapper.OrderMapper;
import org.sqlfans.redisjql.example.service.OrderService;
import org.sqlfans.redisjql.example.service.ProductService;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * 订单服务实现类
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Service
public class OrderServiceImpl implements OrderService {
    
    @Autowired
    private OrderMapper orderMapper;
    
    @Autowired
    private ProductService productService;
    
    @Override
    public Order findById(String id) {
        return orderMapper.findById(id);
    }
    
    @Override
    public List<Order> findByUserId(String userId) {
        return orderMapper.findByUserId(userId);
    }
    
    @Override
    public List<Order> findByProductId(String productId) {
        return orderMapper.findByProductId(productId);
    }
    
    @Override
    public List<Order> findByStatus(String status) {
        return orderMapper.findByStatus(status);
    }
    
    @Override
    public List<Order> findByOrderTimeRange(LocalDateTime startTime, LocalDateTime endTime) {
        return orderMapper.findByOrderTimeRange(startTime, endTime);
    }
    
    @Override
    public List<OrderDetail> findOrderDetailsByUserId(String userId) {
        return orderMapper.findOrderDetailsByUserId(userId);
    }
    
    @Override
    public List<OrderDetail> findOrderDetailsByProductId(String productId) {
        return orderMapper.findOrderDetailsByProductId(productId);
    }
    
    @Override
    public List<OrderDetail> findOrderDetailsByUserIdAndProductId(String userId, String productId) {
        return orderMapper.findOrderDetailsByUserIdAndProductId(userId, productId);
    }
    
    @Override
    @Transactional
    public Order createOrder(Order order) {
        // 生成订单ID
        if (order.getId() == null) {
            order.setId("O" + UUID.randomUUID().toString().replace("-", "").substring(0, 29));
        }
        
        // 设置订单时间
        if (order.getOrderTime() == null) {
            order.setOrderTime(LocalDateTime.now());
        }
        
        // 默认处理中状态
        if (order.getStatus() == null) {
            order.setStatus("PROCESSING");
        }
        
        // 设置创建和更新时间
        LocalDateTime now = LocalDateTime.now();
        order.setCreateTime(now);
        order.setUpdateTime(now);
        order.setVersion(0);
        
        // 更新商品库存
        Product product = productService.findById(order.getProductId());
        int newStock = product.getStock() - order.getQuantity();
        if (newStock < 0) {
            throw new RuntimeException("Insufficient stock for product: " + product.getProductName());
        }
        
        productService.updateStock(order.getProductId(), newStock);
        
        // 保存订单
        orderMapper.insert(order);
        return order;
    }
    
    @Override
    @Transactional
    public Order updateOrder(Order order) {
        Order existingOrder = orderMapper.findById(order.getId());
        if (existingOrder == null) {
            throw new RuntimeException("Order not found with id: " + order.getId());
        }
        
        // 更新时间戳
        order.setUpdateTime(LocalDateTime.now());
        
        // 乐观锁更新
        int affected = orderMapper.update(order);
        if (affected <= 0) {
            throw new RuntimeException("Update failed due to concurrent modification");
        }
        
        return orderMapper.findById(order.getId());
    }
    
    @Override
    @Transactional
    public boolean updateStatus(String id, String status) {
        Order order = orderMapper.findById(id);
        if (order == null) {
            throw new RuntimeException("Order not found with id: " + id);
        }
        
        // 乐观锁更新状态
        int affected = orderMapper.updateStatus(id, status, order.getVersion());
        return affected > 0;
    }
    
    @Override
    @Transactional
    public boolean deleteOrder(String id) {
        int affected = orderMapper.deleteById(id);
        return affected > 0;
    }
} 