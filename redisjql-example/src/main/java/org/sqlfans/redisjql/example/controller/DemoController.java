package org.sqlfans.redisjql.example.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.sqlfans.redisjql.example.entity.OrderDetail;
import org.sqlfans.redisjql.example.entity.Product;
import org.sqlfans.redisjql.example.entity.User;
import org.sqlfans.redisjql.example.service.OrderService;
import org.sqlfans.redisjql.example.service.ProductService;
import org.sqlfans.redisjql.example.service.UserService;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 演示控制器
 * 用于展示RedisJQL的性能优势
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Slf4j
@RestController
@RequestMapping("/api/demo")
public class DemoController {

    @Autowired
    private UserService userService;

    @Autowired
    private ProductService productService;

    @Autowired
    private OrderService orderService;

    /**
     * 用户名称查询性能测试
     *
     * @param name 用户名称
     * @param times 测试次数
     * @return 测试结果
     */
    @GetMapping("/user-name-search")
    public Map<String, Object> userNameSearchTest(
            @RequestParam String name,
            @RequestParam(defaultValue = "10") int times) {
        
        Map<String, Object> result = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        // 执行多次查询
        for (int i = 0; i < times; i++) {
            List<User> users = userService.findByName(name);
            if (i == 0) {
                result.put("users", users);
            }
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        result.put("queryTimes", times);
        result.put("totalDuration", duration);
        result.put("averageDuration", (double) duration / times);
        
        log.info("用户名称查询性能测试 - 名称: {}, 次数: {}, 总耗时: {}ms, 平均耗时: {}ms", 
                name, times, duration, (double) duration / times);
        
        return result;
    }

    /**
     * 商品分类查询性能测试
     *
     * @param category 商品分类
     * @param times 测试次数
     * @return 测试结果
     */
    @GetMapping("/product-category-search")
    public Map<String, Object> productCategorySearchTest(
            @RequestParam String category,
            @RequestParam(defaultValue = "10") int times) {
        
        Map<String, Object> result = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        // 执行多次查询
        for (int i = 0; i < times; i++) {
            List<Product> products = productService.findByCategory(category);
            if (i == 0) {
                result.put("products", products);
            }
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        result.put("queryTimes", times);
        result.put("totalDuration", duration);
        result.put("averageDuration", (double) duration / times);
        
        log.info("商品分类查询性能测试 - 分类: {}, 次数: {}, 总耗时: {}ms, 平均耗时: {}ms", 
                category, times, duration, (double) duration / times);
        
        return result;
    }

    /**
     * 商品价格排序性能测试
     *
     * @param ascending 是否升序
     * @param times 测试次数
     * @return 测试结果
     */
    @GetMapping("/product-price-sort")
    public Map<String, Object> productPriceSortTest(
            @RequestParam(defaultValue = "true") boolean ascending,
            @RequestParam(defaultValue = "10") int times) {
        
        Map<String, Object> result = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        // 执行多次查询
        for (int i = 0; i < times; i++) {
            List<Product> products = productService.findAllOrderByPrice(ascending);
            if (i == 0) {
                result.put("products", products);
            }
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        result.put("queryTimes", times);
        result.put("totalDuration", duration);
        result.put("averageDuration", (double) duration / times);
        
        log.info("商品价格排序性能测试 - 升序: {}, 次数: {}, 总耗时: {}ms, 平均耗时: {}ms", 
                ascending, times, duration, (double) duration / times);
        
        return result;
    }

    /**
     * 用户订单查询性能测试
     *
     * @param userId 用户ID
     * @param times 测试次数
     * @return 测试结果
     */
    @GetMapping("/user-order-search")
    public Map<String, Object> userOrderSearchTest(
            @RequestParam String userId,
            @RequestParam(defaultValue = "10") int times) {
        
        Map<String, Object> result = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        // 执行多次查询
        for (int i = 0; i < times; i++) {
            List<OrderDetail> orderDetails = orderService.findOrderDetailsByUserId(userId);
            if (i == 0) {
                result.put("orderDetails", orderDetails);
            }
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        result.put("queryTimes", times);
        result.put("totalDuration", duration);
        result.put("averageDuration", (double) duration / times);
        
        log.info("用户订单查询性能测试 - 用户ID: {}, 次数: {}, 总耗时: {}ms, 平均耗时: {}ms", 
                userId, times, duration, (double) duration / times);
        
        return result;
    }

    /**
     * 商品价格范围查询性能测试
     *
     * @param minPrice 最小价格
     * @param maxPrice 最大价格
     * @param times 测试次数
     * @return 测试结果
     */
    @GetMapping("/product-price-range")
    public Map<String, Object> productPriceRangeTest(
            @RequestParam BigDecimal minPrice,
            @RequestParam BigDecimal maxPrice,
            @RequestParam(defaultValue = "10") int times) {
        
        Map<String, Object> result = new HashMap<>();
        long startTime = System.currentTimeMillis();
        
        // 执行多次查询
        for (int i = 0; i < times; i++) {
            List<Product> products = productService.findByPriceRange(minPrice, maxPrice);
            if (i == 0) {
                result.put("products", products);
            }
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        result.put("queryTimes", times);
        result.put("totalDuration", duration);
        result.put("averageDuration", (double) duration / times);
        
        log.info("商品价格范围查询性能测试 - 价格范围: {} - {}, 次数: {}, 总耗时: {}ms, 平均耗时: {}ms", 
                minPrice, maxPrice, times, duration, (double) duration / times);
        
        return result;
    }
} 