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
 * 大数据量测试数据生成器
 */
public class DataGenerator {

    private static final String[] FAMILY_NAMES = {"张", "李", "王", "赵", "陈", "刘", "杨", "黄", "周", "吴", "徐", "孙", "马", "朱", "胡", "林", "郭", "何", "高", "罗", "郑", "梁", "谢", "宋", "唐", "许", "邓", "冯", "韩", "曹", "曾", "彭", "萧", "蔡", "潘", "田", "董", "袁", "于", "余", "蒋", "叶", "杜", "苏", "魏", "程", "吕", "丁", "沈", "任", "姚", "卢", "傅", "钟", "姜", "崔", "谭", "廖", "范", "汪", "陆", "金", "石", "戴", "贾", "韦", "夏", "邱", "方", "侯", "邹", "熊", "孟", "秦", "白", "江", "阎", "薛", "尹", "段", "雷", "黎", "史", "龙", "贺", "陶", "顾", "毛", "郝", "龚", "邵", "万", "钱", "严", "赖", "覃", "洪", "武", "莫", "孔"};
    
    private static final String[] GIVEN_NAMES = {"伟", "芳", "娜", "秀英", "敏", "静", "丽", "强", "磊", "洋", "艳", "勇", "军", "杰", "娟", "涛", "明", "超", "秀兰", "霞", "平", "刚", "桂英"};
    
    private static final String[] SECOND_NAMES = {"小", "大", "老", "明", "建", "光", "天", "文", "德", "广", "宇", "江", "海", "金", "子", "冰", "玉", "鹏", "山", "水", "国", "宝", "福", "生"};
    
    private static final String[] DOMAINS = {"gmail.com", "163.com", "qq.com", "126.com", "outlook.com", "hotmail.com", "yahoo.com", "sina.com", "sohu.com", "foxmail.com"};
    
    private static final String[] CITIES = {"北京", "上海", "广州", "深圳", "杭州", "南京", "武汉", "成都", "重庆", "西安", "天津", "苏州", "郑州", "长沙", "青岛", "宁波", "东莞", "沈阳", "大连", "厦门", "福州", "无锡", "合肥", "昆明", "哈尔滨", "济南", "佛山", "长春", "温州", "石家庄", "南宁", "常州", "泉州", "南昌", "贵阳", "太原", "烟台", "嘉兴", "南通", "金华", "珠海", "惠州", "徐州", "海口", "乌鲁木齐", "绍兴", "中山", "台州", "兰州"};
    
    private static final String[] DISTRICTS = {"朝阳区", "海淀区", "西城区", "东城区", "丰台区", "石景山区", "门头沟区", "房山区", "通州区", "顺义区", "昌平区", "大兴区", "怀柔区", "平谷区", "密云区", "延庆区", "浦东新区", "黄浦区", "徐汇区", "长宁区", "静安区", "普陀区", "虹口区", "杨浦区", "宝山区", "闵行区", "嘉定区", "金山区", "松江区", "青浦区", "奉贤区", "崇明区", "南山区", "福田区", "罗湖区", "盐田区", "龙岗区", "宝安区", "龙华区", "坪山区", "光明区"};
    
    private static final String[] STREETS = {"人民路", "中山路", "解放路", "建设路", "和平路", "民族路", "新华路", "胜利路", "朝阳路", "东风路", "建国路", "南京路", "长江路", "友谊路", "红旗路", "工人路", "青年路", "五四路", "北京路", "前进路", "东北路", "健康路", "劳动路", "团结路", "幸福路", "新兴路", "先锋路", "园林路", "环城路", "荣华路", "育才路", "锦绣路", "文化路", "芙蓉路", "体育路", "高新路", "科技路", "创业路", "发展路", "兴业路", "繁华路", "昌盛路", "富强路", "振兴路", "望江路", "临江路", "沿江路", "滨江路", "江滨路", "湖滨路", "海滨路"};
    
    private static final String[] PRODUCT_CATEGORIES = {"手机", "笔记本电脑", "平板电脑", "耳机", "智能手表", "智能音箱", "相机", "游戏机", "电视", "投影仪", "智能家居", "健身器材", "厨房电器", "生活电器"};
    
    private static final String[] BRANDS = {
        "Apple", "Samsung", "Xiaomi", "Huawei", "OPPO", "vivo", "OnePlus", "Lenovo", "Dell", "HP", 
        "Asus", "Acer", "Microsoft", "Sony", "LG", "Philips", "Panasonic", "Bose", "Logitech", "Canon", 
        "Nikon", "DJI", "GoPro", "Nintendo", "Dyson", "Beats", "JBL", "Sennheiser", "Fitbit", "Garmin"
    };
    
    private static final String[] MODEL_PREFIXES = {"Pro", "Plus", "Max", "Ultra", "Lite", "Air", "Mini", "SE", "X", "S", "T", "Neo", "Slim", "Elite", "Prime", "Flex", "Go", "Yoga", "Zen", "Joy"};
    
    private static final String[] ORDER_STATUSES = {"PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "COMPLETED", "CANCELLED", "REFUNDED"};
    
    private static final Random random = new Random();
    private static final DecimalFormat priceFormat = new DecimalFormat("#.##");
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    public static void main(String[] args) {
        try {
            // 确保生成的ID不重复
            Set<String> userIds = new HashSet<>();
            Set<String> productIds = new HashSet<>();
            Set<String> orderIds = new HashSet<>();
            
            // 生成10000个用户数据
            try (PrintWriter writer = new PrintWriter(new FileWriter("users.sql"))) {
                writer.println("-- 插入用户数据");
                writer.println("INSERT INTO t_user (id, username, name, age, gender, email, phone, address, create_time, update_time, version) VALUES");
                
                for (int i = 1; i <= 10000; i++) {
                    String userId = generateUniqueId("U", 7, userIds);
                    String name = generateChineseName();
                    String username = generateUsername(name);
                    int age = 18 + random.nextInt(60);
                    String gender = random.nextBoolean() ? "男" : "女";
                    String email = generateEmail(username);
                    String phone = generatePhone();
                    String address = generateAddress();
                    String createTime = generateRandomPastDate(365 * 3); // 三年内
                    
                    writer.println(String.format("('%s', '%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s', 0)%s",
                            userId, username, name, age, gender, email, phone, address, createTime, createTime,
                            i == 10000 ? ";" : ","));
                }
            }
            
            // 生成1000个产品数据
            try (PrintWriter writer = new PrintWriter(new FileWriter("products.sql"))) {
                writer.println("-- 插入商品数据");
                writer.println("INSERT INTO t_product (id, product_name, category, price, stock, description, create_time, update_time, version) VALUES");
                
                for (int i = 1; i <= 1000; i++) {
                    String productId = generateUniqueId("P", 7, productIds);
                    String category = PRODUCT_CATEGORIES[random.nextInt(PRODUCT_CATEGORIES.length)];
                    String brand = BRANDS[random.nextInt(BRANDS.length)];
                    String model = generateProductModel();
                    String productName = brand + " " + model;
                    double price = generatePrice(category);
                    int stock = 10 + random.nextInt(991); // 10-1000
                    String description = generateProductDescription(brand, model, category);
                    String createTime = generateRandomPastDate(365 * 2); // 两年内
                    
                    writer.println(String.format("('%s', '%s', '%s', %.2f, %d, '%s', '%s', '%s', 0)%s",
                            productId, productName, category, price, stock, description, createTime, createTime,
                            i == 1000 ? ";" : ","));
                }
            }
            
            // 生成100000个订单数据
            try (PrintWriter writer = new PrintWriter(new FileWriter("orders.sql"))) {
                writer.println("-- 插入订单数据");
                writer.println("INSERT INTO t_order (id, user_id, product_id, quantity, total_price, order_time, status, create_time, update_time, version) VALUES");
                
                // 将用户ID和产品ID转换为数组以便随机访问
                String[] userIdArray = userIds.toArray(new String[0]);
                String[] productIdArray = productIds.toArray(new String[0]);
                
                for (int i = 1; i <= 100000; i++) {
                    String orderId = generateUniqueId("O", 7, orderIds);
                    String userId = userIdArray[random.nextInt(userIdArray.length)];
                    String productId = productIdArray[random.nextInt(productIdArray.length)];
                    int quantity = 1 + random.nextInt(5); // 1-5
                    double price = 100 + random.nextInt(9901); // 假设价格在100-10000之间
                    double totalPrice = price * quantity;
                    String orderTime = generateRandomPastDate(365); // 一年内
                    String status = ORDER_STATUSES[random.nextInt(ORDER_STATUSES.length)];
                    
                    writer.println(String.format("('%s', '%s', '%s', %d, %.2f, '%s', '%s', '%s', '%s', 0)%s",
                            orderId, userId, productId, quantity, totalPrice, orderTime, status, orderTime, orderTime,
                            i == 100000 ? ";" : ","));
                }
            }
            
            System.out.println("数据生成完成！");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static String generateUniqueId(String prefix, int length, Set<String> existingIds) {
        String id;
        do {
            StringBuilder sb = new StringBuilder(prefix);
            for (int i = 0; i < length - prefix.length(); i++) {
                sb.append(random.nextInt(10));
            }
            id = sb.toString();
        } while (existingIds.contains(id));
        
        existingIds.add(id);
        return id;
    }
    
    private static String generateChineseName() {
        String familyName = FAMILY_NAMES[random.nextInt(FAMILY_NAMES.length)];
        
        if (random.nextDouble() < 0.5) { // 单字名
            return familyName + GIVEN_NAMES[random.nextInt(GIVEN_NAMES.length)];
        } else { // 双字名
            if (random.nextDouble() < 0.5) {
                return familyName + SECOND_NAMES[random.nextInt(SECOND_NAMES.length)] + 
                       GIVEN_NAMES[random.nextInt(GIVEN_NAMES.length)];
            } else {
                return familyName + GIVEN_NAMES[random.nextInt(GIVEN_NAMES.length)] + 
                       GIVEN_NAMES[random.nextInt(GIVEN_NAMES.length)];
            }
        }
    }
    
    private static String generateUsername(String name) {
        // 拼音化处理（简化版）
        String pinyin = name.charAt(0) + ""; // 仅取第一个字符作为演示
        
        // 添加随机数字
        int randomNum = 100 + random.nextInt(9900); // 100-9999
        return pinyin.toLowerCase() + randomNum;
    }
    
    private static String generateEmail(String username) {
        return username + "@" + DOMAINS[random.nextInt(DOMAINS.length)];
    }
    
    private static String generatePhone() {
        String[] prefixes = {"139", "138", "137", "136", "135", "134", "159", "158", "157", "188", "187", "152", "151", "150"};
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
        int buildingNumber = 1 + random.nextInt(200);
        
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
                return 999 + random.nextInt(9001); // 999-9999
            case "笔记本电脑":
                return 2999 + random.nextInt(12001); // 2999-14999
            case "平板电脑":
                return 1999 + random.nextInt(4001); // 1999-5999
            case "耳机":
                return 99 + random.nextInt(2901); // 99-2999
            case "智能手表":
                return 999 + random.nextInt(2001); // 999-2999
            default:
                return 99 + random.nextInt(4901); // 99-4999
        }
    }
    
    private static String generateProductModel() {
        StringBuilder model = new StringBuilder();
        
        // 添加型号前缀
        if (random.nextDouble() < 0.7) {
            model.append(MODEL_PREFIXES[random.nextInt(MODEL_PREFIXES.length)]).append(" ");
        }
        
        // 添加数字
        model.append(random.nextInt(10) + 1);
        
        // 可能添加后缀
        if (random.nextDouble() < 0.3) {
            model.append(random.nextBoolean() ? "s" : "i");
        }
        
        return model.toString();
    }
    
    private static String generateProductDescription(String brand, String model, String category) {
        String[] features;
        
        switch (category) {
            case "手机":
                features = new String[] {
                    random.nextInt(4) + 8 + "GB RAM", 
                    (random.nextInt(8) + 1) * 128 + "GB 存储",
                    random.nextDouble() < 0.5 ? "OLED屏幕" : "LCD屏幕",
                    random.nextInt(40) + 60 + "MP 摄像头",
                    random.nextInt(1500) + 3500 + "mAh 电池"
                };
                break;
            case "笔记本电脑":
                features = new String[] {
                    "第" + (random.nextInt(4) + 10) + "代处理器",
                    random.nextInt(4) + 8 + "GB RAM",
                    (random.nextInt(4) + 1) * 256 + "GB SSD",
                    (random.nextInt(4) + 13) + "英寸屏幕",
                    random.nextDouble() < 0.5 ? "集成显卡" : "独立显卡"
                };
                break;
            case "平板电脑":
                features = new String[] {
                    random.nextInt(4) + 8 + "英寸屏幕",
                    random.nextInt(4) + 4 + "GB RAM",
                    (random.nextInt(4) + 1) * 32 + "GB 存储",
                    random.nextDouble() < 0.5 ? "WiFi版" : "全网通",
                    random.nextInt(4000) + 4000 + "mAh 电池"
                };
                break;
            default:
                features = new String[] {
                    "高品质", "精致做工", "时尚外观", "耐用材质", "智能操作"
                };
        }
        
        return brand + " " + model + " " + category + "，" + 
               String.join("，", features);
    }
} 