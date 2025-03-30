package org.sqlfans.redisjql.example.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.sqlfans.redisjql.example.entity.User;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 用户服务测试类
 *
 * @author vincentruan
 * @version 1.0.0
 */
@SpringBootTest
public class UserServiceTests {

    @Autowired
    private UserService userService;

    @Test
    public void testFindById() {
        User user = userService.findById("U001");
        assertNotNull(user);
        assertEquals("张三", user.getName());
    }

    @Test
    public void testFindByName() {
        List<User> users = userService.findByName("张三");
        assertNotNull(users);
        assertFalse(users.isEmpty());
        assertEquals("U001", users.get(0).getId());
    }

    @Test
    public void testFindByAgeRange() {
        List<User> users = userService.findByAgeRange(25, 30);
        assertNotNull(users);
        assertFalse(users.isEmpty());
        
        // 验证所有用户的年龄在25-30之间
        for (User user : users) {
            assertTrue(user.getAge() >= 25 && user.getAge() <= 30);
        }
    }

    @Test
    public void testFindByGender() {
        List<User> maleUsers = userService.findByGender("男");
        List<User> femaleUsers = userService.findByGender("女");
        
        assertNotNull(maleUsers);
        assertNotNull(femaleUsers);
        assertFalse(maleUsers.isEmpty());
        assertFalse(femaleUsers.isEmpty());
        
        // 验证性别正确
        for (User user : maleUsers) {
            assertEquals("男", user.getGender());
        }
        
        for (User user : femaleUsers) {
            assertEquals("女", user.getGender());
        }
    }

    @Test
    public void testCreateAndUpdateUser() {
        // 创建新用户
        User newUser = new User();
        newUser.setUsername("testuser");
        newUser.setName("测试用户");
        newUser.setAge(28);
        newUser.setGender("男");
        newUser.setEmail("test@example.com");
        newUser.setPhone("13712345678");
        newUser.setAddress("测试地址");
        
        User createdUser = userService.createUser(newUser);
        assertNotNull(createdUser);
        assertNotNull(createdUser.getId());
        assertEquals("测试用户", createdUser.getName());
        
        // 更新用户
        createdUser.setAge(29);
        createdUser.setEmail("updated@example.com");
        
        User updatedUser = userService.updateUser(createdUser);
        assertNotNull(updatedUser);
        assertEquals(29, updatedUser.getAge());
        assertEquals("updated@example.com", updatedUser.getEmail());
        
        // 删除测试用户
        boolean deleted = userService.deleteUser(createdUser.getId());
        assertTrue(deleted);
    }
} 