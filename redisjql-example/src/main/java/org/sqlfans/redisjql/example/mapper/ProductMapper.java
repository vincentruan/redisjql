package org.sqlfans.redisjql.example.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.sqlfans.redisjql.example.entity.Product;

import java.math.BigDecimal;
import java.util.List;

/**
 * 商品Mapper接口
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Mapper
public interface ProductMapper {
    /**
     * 根据ID查询商品
     *
     * @param id 商品ID
     * @return 商品对象
     */
    Product findById(@Param("id") String id);
    
    /**
     * 根据商品名称查询（模糊匹配）
     *
     * @param name 商品名称
     * @return 商品列表
     */
    List<Product> findByName(@Param("name") String name);
    
    /**
     * 根据商品分类查询
     *
     * @param category 商品分类
     * @return 商品列表
     */
    List<Product> findByCategory(@Param("category") String category);
    
    /**
     * 根据价格范围查询
     *
     * @param minPrice 最小价格
     * @param maxPrice 最大价格
     * @return 商品列表
     */
    List<Product> findByPriceRange(@Param("minPrice") BigDecimal minPrice, @Param("maxPrice") BigDecimal maxPrice);
    
    /**
     * 根据库存数量范围查询
     *
     * @param minStock 最小库存
     * @param maxStock 最大库存
     * @return 商品列表
     */
    List<Product> findByStockRange(@Param("minStock") Integer minStock, @Param("maxStock") Integer maxStock);
    
    /**
     * 按库存排序查询所有商品
     *
     * @param ascending 是否升序排序
     * @return 商品列表
     */
    List<Product> findAllOrderByStock(@Param("ascending") boolean ascending);
    
    /**
     * 按价格排序查询所有商品
     *
     * @param ascending 是否升序排序
     * @return 商品列表
     */
    List<Product> findAllOrderByPrice(@Param("ascending") boolean ascending);
    
    /**
     * 插入商品
     *
     * @param product 商品对象
     * @return 影响行数
     */
    int insert(Product product);
    
    /**
     * 更新商品
     *
     * @param product 商品对象
     * @return 影响行数
     */
    int update(Product product);
    
    /**
     * 更新商品库存
     *
     * @param id 商品ID
     * @param stock 新库存
     * @param version 版本号
     * @return 影响行数
     */
    int updateStock(@Param("id") String id, @Param("stock") Integer stock, @Param("version") Integer version);
    
    /**
     * 删除商品
     *
     * @param id 商品ID
     * @return 影响行数
     */
    int deleteById(@Param("id") String id);
} 