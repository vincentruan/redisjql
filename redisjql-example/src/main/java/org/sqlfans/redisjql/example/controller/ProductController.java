package org.sqlfans.redisjql.example.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sqlfans.redisjql.example.entity.Product;
import org.sqlfans.redisjql.example.service.ProductService;

import java.math.BigDecimal;
import java.util.List;

/**
 * 商品控制器
 *
 * @author vincentruan
 * @version 1.0.0
 */
@RestController
@RequestMapping("/api/products")
public class ProductController {

    @Autowired
    private ProductService productService;

    /**
     * 根据ID查询商品
     *
     * @param id 商品ID
     * @return 商品信息
     */
    @GetMapping("/{id}")
    public ResponseEntity<Product> getProductById(@PathVariable String id) {
        Product product = productService.findById(id);
        if (product == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(product);
    }

    /**
     * 根据商品名称查询（模糊匹配）
     *
     * @param name 商品名称
     * @return 商品列表
     */
    @GetMapping("/name/{name}")
    public ResponseEntity<List<Product>> getProductsByName(@PathVariable String name) {
        List<Product> products = productService.findByName(name);
        return ResponseEntity.ok(products);
    }

    /**
     * 根据商品分类查询
     *
     * @param category 商品分类
     * @return 商品列表
     */
    @GetMapping("/category/{category}")
    public ResponseEntity<List<Product>> getProductsByCategory(@PathVariable String category) {
        List<Product> products = productService.findByCategory(category);
        return ResponseEntity.ok(products);
    }

    /**
     * 根据价格范围查询
     *
     * @param minPrice 最小价格
     * @param maxPrice 最大价格
     * @return 商品列表
     */
    @GetMapping("/price")
    public ResponseEntity<List<Product>> getProductsByPriceRange(
            @RequestParam BigDecimal minPrice,
            @RequestParam BigDecimal maxPrice) {
        List<Product> products = productService.findByPriceRange(minPrice, maxPrice);
        return ResponseEntity.ok(products);
    }

    /**
     * 根据库存数量范围查询
     *
     * @param minStock 最小库存
     * @param maxStock 最大库存
     * @return 商品列表
     */
    @GetMapping("/stock")
    public ResponseEntity<List<Product>> getProductsByStockRange(
            @RequestParam Integer minStock,
            @RequestParam Integer maxStock) {
        List<Product> products = productService.findByStockRange(minStock, maxStock);
        return ResponseEntity.ok(products);
    }

    /**
     * 获取所有商品，按库存排序
     *
     * @param ascending 是否升序排序
     * @return 商品列表
     */
    @GetMapping("/sort/stock")
    public ResponseEntity<List<Product>> getAllProductsOrderByStock(
            @RequestParam(defaultValue = "true") boolean ascending) {
        List<Product> products = productService.findAllOrderByStock(ascending);
        return ResponseEntity.ok(products);
    }

    /**
     * 获取所有商品，按价格排序
     *
     * @param ascending 是否升序排序
     * @return 商品列表
     */
    @GetMapping("/sort/price")
    public ResponseEntity<List<Product>> getAllProductsOrderByPrice(
            @RequestParam(defaultValue = "true") boolean ascending) {
        List<Product> products = productService.findAllOrderByPrice(ascending);
        return ResponseEntity.ok(products);
    }

    /**
     * 创建商品
     *
     * @param product 商品信息
     * @return 创建的商品
     */
    @PostMapping
    public ResponseEntity<Product> createProduct(@RequestBody Product product) {
        Product createdProduct = productService.createProduct(product);
        return ResponseEntity.ok(createdProduct);
    }

    /**
     * 更新商品
     *
     * @param id      商品ID
     * @param product 商品信息
     * @return 更新后的商品
     */
    @PutMapping("/{id}")
    public ResponseEntity<Product> updateProduct(@PathVariable String id, @RequestBody Product product) {
        product.setId(id);
        Product updatedProduct = productService.updateProduct(product);
        return ResponseEntity.ok(updatedProduct);
    }

    /**
     * 更新商品库存
     *
     * @param id    商品ID
     * @param stock 新库存
     * @return 操作结果
     */
    @PatchMapping("/{id}/stock")
    public ResponseEntity<Void> updateProductStock(
            @PathVariable String id,
            @RequestParam Integer stock) {
        boolean success = productService.updateStock(id, stock);
        if (success) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * 删除商品
     *
     * @param id 商品ID
     * @return 操作结果
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteProduct(@PathVariable String id) {
        boolean success = productService.deleteProduct(id);
        if (success) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }
} 