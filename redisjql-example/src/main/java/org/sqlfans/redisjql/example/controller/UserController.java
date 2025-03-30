package org.sqlfans.redisjql.example.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sqlfans.redisjql.example.entity.User;
import org.sqlfans.redisjql.example.service.UserService;

import java.util.List;

/**
 * 用户控制器
 *
 * @author vincentruan
 * @version 1.0.0
 */
@RestController
@RequestMapping("/api/users")
public class UserController {

    @Autowired
    private UserService userService;

    /**
     * 根据ID查询用户
     *
     * @param id 用户ID
     * @return 用户信息
     */
    @GetMapping("/{id}")
    public ResponseEntity<User> getUserById(@PathVariable String id) {
        User user = userService.findById(id);
        if (user == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(user);
    }

    /**
     * 根据姓名查询用户
     *
     * @param name 姓名
     * @return 用户列表
     */
    @GetMapping("/name/{name}")
    public ResponseEntity<List<User>> getUsersByName(@PathVariable String name) {
        List<User> users = userService.findByName(name);
        return ResponseEntity.ok(users);
    }

    /**
     * 根据年龄范围查询用户
     *
     * @param minAge 最小年龄
     * @param maxAge 最大年龄
     * @return 用户列表
     */
    @GetMapping("/age")
    public ResponseEntity<List<User>> getUsersByAgeRange(
            @RequestParam Integer minAge,
            @RequestParam Integer maxAge) {
        List<User> users = userService.findByAgeRange(minAge, maxAge);
        return ResponseEntity.ok(users);
    }

    /**
     * 根据性别查询用户
     *
     * @param gender 性别
     * @return 用户列表
     */
    @GetMapping("/gender/{gender}")
    public ResponseEntity<List<User>> getUsersByGender(@PathVariable String gender) {
        List<User> users = userService.findByGender(gender);
        return ResponseEntity.ok(users);
    }

    /**
     * 创建用户
     *
     * @param user 用户信息
     * @return 创建的用户
     */
    @PostMapping
    public ResponseEntity<User> createUser(@RequestBody User user) {
        User createdUser = userService.createUser(user);
        return ResponseEntity.ok(createdUser);
    }

    /**
     * 更新用户
     *
     * @param id   用户ID
     * @param user 用户信息
     * @return 更新后的用户
     */
    @PutMapping("/{id}")
    public ResponseEntity<User> updateUser(@PathVariable String id, @RequestBody User user) {
        user.setId(id);
        User updatedUser = userService.updateUser(user);
        return ResponseEntity.ok(updatedUser);
    }

    /**
     * 删除用户
     *
     * @param id 用户ID
     * @return 操作结果
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteUser(@PathVariable String id) {
        boolean success = userService.deleteUser(id);
        if (success) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }
} 