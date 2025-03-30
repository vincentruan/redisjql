package org.sqlfans.redisjql.example.service;

import org.sqlfans.redisjql.example.entity.Product;
import java.math.BigDecimal;
import java.util.List;

/**
 * 商品服务接口
 *
 * @author vincentruan
 * @version 1.0.0
 */
public interface ProductService {
    /**
     * 根据ID查询商品
     *
     * @param id 商品ID
     * @return 商品信息
     */
    Product findById(String id);
    
    /**
     * 根据名称查询商品（模糊查询）
     *
     * @param name 商品名称
     * @return 商品列表
     */
    List<Product> findByName(String name);
    
    /**
     * 根据分类查询商品
     *
     * @param category 商品分类
     * @return 商品列表
     */
    List<Product> findByCategory(String category);
    
    /**
     * 根据价格范围查询商品
     *
     * @param minPrice 最低价格
     * @param maxPrice 最高价格
     * @return 商品列表
     */
    List<Product> findByPriceRange(BigDecimal minPrice, BigDecimal maxPrice);
    
    /**
     * 根据库存范围查询商品
     *
     * @param minStock 最低库存
     * @param maxStock 最高库存
     * @return 商品列表
     */
    List<Product> findByStockRange(Integer minStock, Integer maxStock);
    
    /**
     * 查询所有商品，按库存排序
     *
     * @param ascending 是否升序排序
     * @return 商品列表
     */
    List<Product> findAllOrderByStock(boolean ascending);
    
    /**
     * 查询所有商品，按价格排序
     *
     * @param ascending 是否升序排序
     * @return 商品列表
     */
    List<Product> findAllOrderByPrice(boolean ascending);
    
    /**
     * 创建商品
     *
     * @param product 商品信息
     * @return 创建后的商品
     */
    Product createProduct(Product product);
    
    /**
     * 更新商品
     *
     * @param product 商品信息
     * @return 更新后的商品
     */
    Product updateProduct(Product product);
    
    /**
     * 更新商品库存
     *
     * @param id    商品ID
     * @param stock 新库存
     * @return 是否更新成功
     */
    boolean updateStock(String id, Integer stock);
    
    /**
     * 删除商品
     *
     * @param id 商品ID
     * @return 是否删除成功
     */
    boolean deleteProduct(String id);
} 