-- Enable JSON profiling for DuckDB
PRAGMA enable_profiling='json';

-- Create results directory (DuckDB will create it if needed)
-- Note: Run mkdir -p results before executing this script

-- Create demo database tables
SET profiling_output='results/profiling_query_1.json';
DROP TABLE IF EXISTS users;

SET profiling_output='results/profiling_query_2.json';
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    email VARCHAR NOT NULL,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SET profiling_output='results/profiling_query_3.json';
DROP TABLE IF EXISTS orders;

SET profiling_output='results/profiling_query_4.json';
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    product_name VARCHAR,
    quantity INTEGER,
    price DOUBLE,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
SET profiling_output='results/profiling_query_5.json';
CREATE INDEX idx_users_email ON users(email);

SET profiling_output='results/profiling_query_6.json';
CREATE INDEX idx_orders_user_id ON orders(user_id);

SET profiling_output='results/profiling_query_7.json';
CREATE INDEX idx_orders_date ON orders(order_date);

-- Insert random user data (1000 users)
SET profiling_output='results/profiling_query_8.json';
INSERT INTO users (id, name, email, age)
SELECT 
    row_number() OVER () as id,
    'User_' || row_number() OVER () as name,
    'user' || row_number() OVER () || '@example.com' as email,
    18 + (row_number() OVER () % 50) as age
FROM range(1000);

-- Insert random order data (5000 orders)
SET profiling_output='results/profiling_query_9.json';
INSERT INTO orders (id, user_id, product_name, quantity, price)
SELECT 
    row_number() OVER () as id,
    1 + (random() * 999)::INTEGER as user_id,
    'Product_' || (random() * 99)::INTEGER as product_name,
    1 + (random() * 9)::INTEGER as quantity,
    10.0 + random() * 990 as price
FROM range(5000);

-- Query 1: Count total users
SET profiling_output='results/profiling_query_10.json';
SELECT COUNT(*) as total_users FROM users;

-- Query 2: Count orders
SET profiling_output='results/profiling_query_11.json';
SELECT COUNT(*) as total_orders FROM orders;

-- Query 3: Users by age group
SET profiling_output='results/profiling_query_12.json';
SELECT 
    CASE 
        WHEN age < 25 THEN '18-24'
        WHEN age < 35 THEN '25-34'
        WHEN age < 45 THEN '35-44'
        ELSE '45+'
    END as age_group,
    COUNT(*) as count
FROM users
GROUP BY age_group
ORDER BY age_group;

-- Query 4: Top 10 users by order count
SET profiling_output='results/profiling_query_13.json';
SELECT 
    u.id,
    u.name,
    u.email,
    COUNT(o.id) as order_count,
    SUM(o.price * o.quantity) as total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.name, u.email
ORDER BY order_count DESC
LIMIT 10;

-- Query 5: Top 10 products by revenue
SET profiling_output='results/profiling_query_14.json';
SELECT 
    product_name,
    COUNT(*) as times_ordered,
    SUM(quantity) as total_quantity,
    SUM(price * quantity) as total_revenue
FROM orders
GROUP BY product_name
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 6: Average order value by user age group
SET profiling_output='results/profiling_query_15.json';
SELECT 
    CASE 
        WHEN u.age < 25 THEN '18-24'
        WHEN u.age < 35 THEN '25-34'
        WHEN u.age < 45 THEN '35-44'
        ELSE '45+'
    END as age_group,
    COUNT(DISTINCT u.id) as user_count,
    COUNT(o.id) as order_count,
    AVG(o.price * o.quantity) as avg_order_value
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY age_group
ORDER BY age_group;

-- Query 7: Complex aggregation with window functions
SET profiling_output='results/profiling_query_16.json';
SELECT 
    user_id,
    COUNT(*) as order_count,
    AVG(price * quantity) as avg_order_value,
    MIN(price * quantity) as min_order_value,
    MAX(price * quantity) as max_order_value,
    SUM(price * quantity) as total_spent
FROM orders
GROUP BY user_id
HAVING order_count > 3
ORDER BY total_spent DESC
LIMIT 20;
