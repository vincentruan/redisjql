package org.sqlfans.redisjql.example.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.sqlfans.redisjql.example.entity.Product;
import org.sqlfans.redisjql.example.mapper.ProductMapper;
import org.sqlfans.redisjql.example.service.ProductService;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * 商品服务实现类
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Service
public class ProductServiceImpl implements ProductService {
    
    @Autowired
    private ProductMapper productMapper;
    
    @Override
    public Product findById(String id) {
        return productMapper.findById(id);
    }
    
    @Override
    public List<Product> findByName(String name) {
        return productMapper.findByName(name);
    }
    
    @Override
    public List<Product> findByCategory(String category) {
        return productMapper.findByCategory(category);
    }
    
    @Override
    public List<Product> findByPriceRange(BigDecimal minPrice, BigDecimal maxPrice) {
        return productMapper.findByPriceRange(minPrice, maxPrice);
    }
    
    @Override
    public List<Product> findByStockRange(Integer minStock, Integer maxStock) {
        return productMapper.findByStockRange(minStock, maxStock);
    }
    
    @Override
    public List<Product> findAllOrderByStock(boolean ascending) {
        return productMapper.findAllOrderByStock(ascending);
    }
    
    @Override
    public List<Product> findAllOrderByPrice(boolean ascending) {
        return productMapper.findAllOrderByPrice(ascending);
    }
    
    @Override
    @Transactional
    public Product createProduct(Product product) {
        // 填充ID和时间戳
        if (product.getId() == null) {
            product.setId("P" + UUID.randomUUID().toString().replace("-", "").substring(0, 29));
        }
        
        LocalDateTime now = LocalDateTime.now();
        product.setCreateTime(now);
        product.setUpdateTime(now);
        product.setVersion(0);
        
        productMapper.insert(product);
        return product;
    }
    
    @Override
    @Transactional
    public Product updateProduct(Product product) {
        Product existingProduct = productMapper.findById(product.getId());
        if (existingProduct == null) {
            throw new RuntimeException("Product not found with id: " + product.getId());
        }
        
        // 设置更新时间
        product.setUpdateTime(LocalDateTime.now());
        
        // 乐观锁更新，保留版本号
        int affected = productMapper.update(product);
        if (affected <= 0) {
            throw new RuntimeException("Update failed due to concurrent modification");
        }
        
        // 返回更新后的数据
        return productMapper.findById(product.getId());
    }
    
    @Override
    @Transactional
    public boolean updateStock(String id, Integer stock) {
        Product product = productMapper.findById(id);
        if (product == null) {
            throw new RuntimeException("Product not found with id: " + id);
        }
        
        // 乐观锁更新库存
        int affected = productMapper.updateStock(id, stock, product.getVersion());
        return affected > 0;
    }
    
    @Override
    @Transactional
    public boolean deleteProduct(String id) {
        int affected = productMapper.deleteById(id);
        return affected > 0;
    }
} 