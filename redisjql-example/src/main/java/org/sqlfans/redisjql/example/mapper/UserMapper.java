package org.sqlfans.redisjql.example.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.sqlfans.redisjql.example.entity.User;

import java.util.List;

/**
 * 用户Mapper接口
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Mapper
public interface UserMapper {
    /**
     * 根据ID查询用户
     *
     * @param id 用户ID
     * @return 用户对象
     */
    User findById(@Param("id") String id);
    
    /**
     * 根据姓名查询用户
     *
     * @param name 姓名
     * @return 用户列表
     */
    List<User> findByName(@Param("name") String name);
    
    /**
     * 根据年龄范围查询用户
     *
     * @param minAge 最小年龄
     * @param maxAge 最大年龄
     * @return 用户列表
     */
    List<User> findByAgeRange(@Param("minAge") Integer minAge, @Param("maxAge") Integer maxAge);
    
    /**
     * 根据性别查询用户
     *
     * @param gender 性别
     * @return 用户列表
     */
    List<User> findByGender(@Param("gender") String gender);
    
    /**
     * 插入用户
     *
     * @param user 用户对象
     * @return 影响行数
     */
    int insert(User user);
    
    /**
     * 更新用户
     *
     * @param user 用户对象
     * @return 影响行数
     */
    int update(User user);
    
    /**
     * 删除用户
     *
     * @param id 用户ID
     * @return 影响行数
     */
    int deleteById(@Param("id") String id);
} 