-- 用户表
CREATE TABLE IF NOT EXISTS t_user (
    id VARCHAR(36) PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(50) NOT NULL,
    age INT,
    gender VARCHAR(10),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    version INT DEFAULT 0
);

-- 商品表
CREATE TABLE IF NOT EXISTS t_product (
    id VARCHAR(36) PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock INT NOT NULL DEFAULT 0,
    description TEXT,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    version INT DEFAULT 0
);

-- 订单表
CREATE TABLE IF NOT EXISTS t_order (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    product_id VARCHAR(36) NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    total_price DECIMAL(10, 2) NOT NULL,
    order_time TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    version INT DEFAULT 0,
    FOREIGN KEY (user_id) REFERENCES t_user(id),
    FOREIGN KEY (product_id) REFERENCES t_product(id)
);

-- 索引创建
CREATE INDEX IF NOT EXISTS idx_user_name ON t_user(name);
CREATE INDEX IF NOT EXISTS idx_user_age ON t_user(age);
CREATE INDEX IF NOT EXISTS idx_user_gender ON t_user(gender);

CREATE INDEX IF NOT EXISTS idx_product_name ON t_product(product_name);
CREATE INDEX IF NOT EXISTS idx_product_category ON t_product(category);
CREATE INDEX IF NOT EXISTS idx_product_price ON t_product(price);
CREATE INDEX IF NOT EXISTS idx_product_stock ON t_product(stock);

CREATE INDEX IF NOT EXISTS idx_order_user_id ON t_order(user_id);
CREATE INDEX IF NOT EXISTS idx_order_product_id ON t_order(product_id);
CREATE INDEX IF NOT EXISTS idx_order_status ON t_order(status);
CREATE INDEX IF NOT EXISTS idx_order_time ON t_order(order_time); 