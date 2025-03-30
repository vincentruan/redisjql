package org.sqlfans.redisjql.example.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.sqlfans.redisjql.example.entity.User;
import org.sqlfans.redisjql.example.mapper.UserMapper;
import org.sqlfans.redisjql.example.service.UserService;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * 用户服务实现类
 *
 * @author vincentruan
 * @version 1.0.0
 */
@Service
public class UserServiceImpl implements UserService {
    
    @Autowired
    private UserMapper userMapper;
    
    @Override
    public User findById(String id) {
        return userMapper.findById(id);
    }
    
    @Override
    public List<User> findByName(String name) {
        return userMapper.findByName(name);
    }
    
    @Override
    public List<User> findByAgeRange(Integer minAge, Integer maxAge) {
        return userMapper.findByAgeRange(minAge, maxAge);
    }
    
    @Override
    public List<User> findByGender(String gender) {
        return userMapper.findByGender(gender);
    }
    
    @Override
    @Transactional
    public User createUser(User user) {
        // 填充ID和时间戳
        if (user.getId() == null) {
            user.setId("U" + UUID.randomUUID().toString().replace("-", "").substring(0, 29));
        }
        
        LocalDateTime now = LocalDateTime.now();
        user.setCreateTime(now);
        user.setUpdateTime(now);
        user.setVersion(0);
        
        userMapper.insert(user);
        return user;
    }
    
    @Override
    @Transactional
    public User updateUser(User user) {
        User existingUser = userMapper.findById(user.getId());
        if (existingUser == null) {
            throw new RuntimeException("User not found with id: " + user.getId());
        }
        
        // 更新时间戳
        user.setUpdateTime(LocalDateTime.now());
        
        // 乐观锁更新，保留版本号
        int affected = userMapper.update(user);
        if (affected <= 0) {
            throw new RuntimeException("Update failed due to concurrent modification");
        }
        
        // 返回更新后的数据
        return userMapper.findById(user.getId());
    }
    
    @Override
    @Transactional
    public boolean deleteUser(String id) {
        int affected = userMapper.deleteById(id);
        return affected > 0;
    }
} 