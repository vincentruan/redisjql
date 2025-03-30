package org.sqlfans.redisjql.example.service;

import org.sqlfans.redisjql.example.entity.User;
import java.util.List;

/**
 * 用户服务接口
 *
 * @author vincentruan
 * @version 1.0.0
 */
public interface UserService {
    /**
     * 根据ID查询用户
     *
     * @param id 用户ID
     * @return 用户信息
     */
    User findById(String id);
    
    /**
     * 根据姓名查询用户
     *
     * @param name 姓名
     * @return 用户列表
     */
    List<User> findByName(String name);
    
    /**
     * 根据年龄范围查询用户
     *
     * @param minAge 最小年龄
     * @param maxAge 最大年龄
     * @return 用户列表
     */
    List<User> findByAgeRange(Integer minAge, Integer maxAge);
    
    /**
     * 根据性别查询用户
     *
     * @param gender 性别
     * @return 用户列表
     */
    List<User> findByGender(String gender);
    
    /**
     * 创建用户
     *
     * @param user 用户信息
     * @return 创建后的用户
     */
    User createUser(User user);
    
    /**
     * 更新用户
     *
     * @param user 用户信息
     * @return 更新后的用户
     */
    User updateUser(User user);
    
    /**
     * 删除用户
     *
     * @param id 用户ID
     * @return 是否删除成功
     */
    boolean deleteUser(String id);
} 