package org.sqlfans.redisjql.example.util;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.HashSet;
import java.util.Set;

/**
 * H2数据库大批量测试数据生成器
 * 生成的SQL适用于H2数据库
 */
public class H2DataGenerator {

    private static final String[] FAMILY_NAMES = {"张", "李", "王", "赵", "陈", "刘", "杨", "黄", "周", "吴", "徐", "孙", "马", "朱", "胡", "林", "郭", "何", "高", "罗"};
    
    private static final String[] GIVEN_NAMES = {"伟", "芳", "娜", "秀英", "敏", "静", "丽", "强", "磊", "洋", "艳", "勇", "军", "杰", "娟", "涛", "明", "超", "秀兰", "霞"};
    
    private static final String[] DOMAINS = {"gmail.com", "163.com", "qq.com", "126.com", "outlook.com"};
    
    private static final String[] CITIES = {"北京", "上海", "广州", "深圳", "杭州", "南京", "武汉", "成都", "重庆", "西安"};
    
    private static final String[] DISTRICTS = {"朝阳区", "海淀区", "西城区", "东城区", "丰台区", "南山区", "福田区", "罗湖区", "江干区", "西湖区"};
    
    private static final String[] STREETS = {"人民路", "中山路", "解放路", "建设路", "和平路", "民族路", "新华路", "胜利路", "朝阳路", "东风路"};
    
    private static final String[] PRODUCT_CATEGORIES = {"手机", "笔记本电脑", "平板电脑", "耳机", "智能手表"};
    
    private static final String[] BRANDS = {"Apple", "Samsung", "Xiaomi", "Huawei", "OPPO", "vivo", "Lenovo", "Dell", "HP", "Asus"};
    
    private static final String[] ORDER_STATUSES = {"PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "COMPLETED", "CANCELLED", "REFUNDED"};
    
    private static final Random random = new Random();
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    public static void main(String[] args) {
        try {
            String filePath = "redisjql-example/src/main/resources/large-data.sql";
            try (PrintWriter writer = new PrintWriter(new FileWriter(filePath))) {
                // 生成表结构
                writer.println("-- 创建用户表");
                writer.println("CREATE TABLE IF NOT EXISTS t_user (");
                writer.println("    id VARCHAR(50) PRIMARY KEY,");
                writer.println("    username VARCHAR(100) NOT NULL,");
                writer.println("    name VARCHAR(100) NOT NULL,");
                writer.println("    age INT,");
                writer.println("    gender VARCHAR(10),");
                writer.println("    email VARCHAR(100),");
                writer.println("    phone VARCHAR(20),");
                writer.println("    address VARCHAR(200),");
                writer.println("    create_time TIMESTAMP,");
                writer.println("    update_time TIMESTAMP,");
                writer.println("    version INT");
                writer.println(");");
                writer.println();
                
                writer.println("-- 创建商品表");
                writer.println("CREATE TABLE IF NOT EXISTS t_product (");
                writer.println("    id VARCHAR(50) PRIMARY KEY,");
                writer.println("    product_name VARCHAR(200) NOT NULL,");
                writer.println("    category VARCHAR(100),");
                writer.println("    price DECIMAL(10,2),");
                writer.println("    stock INT,");
                writer.println("    description TEXT,");
                writer.println("    create_time TIMESTAMP,");
                writer.println("    update_time TIMESTAMP,");
                writer.println("    version INT");
                writer.println(");");
                writer.println();
                
                writer.println("-- 创建订单表");
                writer.println("CREATE TABLE IF NOT EXISTS t_order (");
                writer.println("    id VARCHAR(50) PRIMARY KEY,");
                writer.println("    user_id VARCHAR(50) NOT NULL,");
                writer.println("    product_id VARCHAR(50) NOT NULL,");
                writer.println("    quantity INT,");
                writer.println("    total_price DECIMAL(10,2),");
                writer.println("    order_time TIMESTAMP,");
                writer.println("    status VARCHAR(20),");
                writer.println("    create_time TIMESTAMP,");
                writer.println("    update_time TIMESTAMP,");
                writer.println("    version INT");
                writer.println(");");
                writer.println();
                
                // 生成用户数据
                generateUsers(writer, 10000);
                
                // 生成商品数据
                generateProducts(writer, 1000);
                
                // 生成订单数据
                generateOrders(writer, 100000, 10000, 1000);
            }
            
            System.out.println("大数据量SQL脚本生成完成，路径: " + filePath);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void generateUsers(PrintWriter writer, int count) {
        System.out.println("正在生成" + count + "条用户数据...");
        
        writer.println("-- 开始插入用户数据");
        writer.println("INSERT INTO t_user (id, username, name, age, gender, email, phone, address, create_time, update_time, version) VALUES");
        
        for (int i = 1; i <= count; i++) {
            String id = "U" + String.format("%06d", i);
            String name = generateName();
            String username = generateUsername(name, i);
            int age = 18 + random.nextInt(60);
            String gender = random.nextBoolean() ? "男" : "女";
            String email = username + "@" + DOMAINS[random.nextInt(DOMAINS.length)];
            String phone = generatePhone();
            String address = generateAddress();
            String createTime = generateRandomPastDate(365 * 2);
            
            writer.print(String.format("('%s', '%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s', 0)",
                    id, username, name, age, gender, email, phone, address, createTime, createTime));
            
            if (i == count) {
                writer.println(";");
            } else {
                writer.println(",");
            }
            
            // 每10000条提交一次，避免数据过大
            if (i % 10000 == 0) {
                writer.println("COMMIT;");
            }
        }
        
        writer.println("COMMIT;");
        writer.println();
    }
    
    private static void generateProducts(PrintWriter writer, int count) {
        System.out.println("正在生成" + count + "条商品数据...");
        
        writer.println("-- 开始插入商品数据");
        writer.println("INSERT INTO t_product (id, product_name, category, price, stock, description, create_time, update_time, version) VALUES");
        
        for (int i = 1; i <= count; i++) {
            String id = "P" + String.format("%06d", i);
            String category = PRODUCT_CATEGORIES[random.nextInt(PRODUCT_CATEGORIES.length)];
            String brand = BRANDS[random.nextInt(BRANDS.length)];
            String productName = brand + " " + category + " " + (random.nextInt(10) + 1);
            double price = generatePrice(category);
            int stock = 10 + random.nextInt(991);
            String description = productName + " - " + category + "，高性能，超长续航，精美外观";
            String createTime = generateRandomPastDate(365);
            
            writer.print(String.format("('%s', '%s', '%s', %.2f, %d, '%s', '%s', '%s', 0)",
                    id, productName, category, price, stock, description, createTime, createTime));
            
            if (i == count) {
                writer.println(";");
            } else {
                writer.println(",");
            }
            
            // 每1000条提交一次
            if (i % 1000 == 0) {
                writer.println("COMMIT;");
            }
        }
        
        writer.println("COMMIT;");
        writer.println();
    }
    
    private static void generateOrders(PrintWriter writer, int count, int userCount, int productCount) {
        System.out.println("正在生成" + count + "条订单数据...");
        
        writer.println("-- 开始插入订单数据");
        writer.println("INSERT INTO t_order (id, user_id, product_id, quantity, total_price, order_time, status, create_time, update_time, version) VALUES");
        
        for (int i = 1; i <= count; i++) {
            String id = "O" + String.format("%07d", i);
            int userId = random.nextInt(userCount) + 1;
            String userIdStr = "U" + String.format("%06d", userId);
            int productId = random.nextInt(productCount) + 1;
            String productIdStr = "P" + String.format("%06d", productId);
            int quantity = 1 + random.nextInt(5);
            double price = 100 + random.nextInt(9901);
            double totalPrice = price * quantity;
            String orderTime = generateRandomPastDate(365);
            String status = ORDER_STATUSES[random.nextInt(ORDER_STATUSES.length)];
            
            writer.print(String.format("('%s', '%s', '%s', %d, %.2f, '%s', '%s', '%s', '%s', 0)",
                    id, userIdStr, productIdStr, quantity, totalPrice, orderTime, orderTime, orderTime, orderTime));
            
            if (i == count) {
                writer.println(";");
            } else {
                writer.println(",");
            }
            
            // 每10000条提交一次
            if (i % 10000 == 0) {
                writer.println("COMMIT;");
                System.out.println("已生成 " + i + " 条订单数据...");
            }
        }
        
        writer.println("COMMIT;");
    }
    
    private static String generateName() {
        String familyName = FAMILY_NAMES[random.nextInt(FAMILY_NAMES.length)];
        String givenName = GIVEN_NAMES[random.nextInt(GIVEN_NAMES.length)];
        
        // 30%概率双字名
        if (random.nextDouble() < 0.3) {
            givenName += GIVEN_NAMES[random.nextInt(GIVEN_NAMES.length)];
        }
        
        return familyName + givenName;
    }
    
    private static String generateUsername(String name, int id) {
        // 简化拼音处理
        char initial = name.charAt(0);
        return initial + "user" + id;
    }
    
    private static String generatePhone() {
        String[] prefixes = {"139", "138", "137", "136", "135", "158", "157", "150", "151", "152", "188", "187", "186"};
        StringBuilder sb = new StringBuilder(prefixes[random.nextInt(prefixes.length)]);
        
        for (int i = 0; i < 8; i++) {
            sb.append(random.nextInt(10));
        }
        
        return sb.toString();
    }
    
    private static String generateAddress() {
        String city = CITIES[random.nextInt(CITIES.length)];
        String district = DISTRICTS[random.nextInt(DISTRICTS.length)];
        String street = STREETS[random.nextInt(STREETS.length)];
        int buildingNumber = 1 + random.nextInt(100);
        
        return city + "市" + district + street + buildingNumber + "号";
    }
    
    private static String generateRandomPastDate(int daysBack) {
        LocalDateTime now = LocalDateTime.now();
        int randomDays = random.nextInt(daysBack);
        int randomHours = random.nextInt(24);
        int randomMinutes = random.nextInt(60);
        int randomSeconds = random.nextInt(60);
        
        LocalDateTime randomDate = now.minusDays(randomDays)
                                     .minusHours(randomHours)
                                     .minusMinutes(randomMinutes)
                                     .minusSeconds(randomSeconds);
        
        return randomDate.format(dateFormatter);
    }
    
    private static double generatePrice(String category) {
        switch (category) {
            case "手机":
                return 1999 + random.nextInt(5000);
            case "笔记本电脑":
                return 3999 + random.nextInt(8000);
            case "平板电脑":
                return 1499 + random.nextInt(4000);
            case "耳机":
                return 199 + random.nextInt(1500);
            case "智能手表":
                return 899 + random.nextInt(2000);
            default:
                return 299 + random.nextInt(3000);
        }
    }
} 