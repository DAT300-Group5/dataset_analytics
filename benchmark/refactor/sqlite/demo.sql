-- Enable timer and statistics
.timer on
.stats on

-- Redirect output to log file
.output output.log

-- Print header
.print "=== SQLite Performance Demo ==="
.print ""

-- Create demo database
.print "Creating table..."
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    product_name TEXT,
    quantity INTEGER,
    price REAL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Create indexes
.print "Creating indexes..."
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_date ON orders(order_date);

.print ""
.print "Inserting test data..."

-- Insert random user data (1000 users)
INSERT INTO users (name, email, age) VALUES
    ('User_' || abs(random() % 1000), 'user' || abs(random() % 1000) || '@example.com', 18 + abs(random() % 50)),
    ('User_' || abs(random() % 1000), 'user' || abs(random() % 1000) || '@example.com', 18 + abs(random() % 50)),
    ('User_' || abs(random() % 1000), 'user' || abs(random() % 1000) || '@example.com', 18 + abs(random() % 50)),
    ('User_' || abs(random() % 1000), 'user' || abs(random() % 1000) || '@example.com', 18 + abs(random() % 50)),
    ('User_' || abs(random() % 1000), 'user' || abs(random() % 1000) || '@example.com', 18 + abs(random() % 50));

-- Generate more users using a recursive CTE
WITH RECURSIVE generate_users(x) AS (
    SELECT 1
    UNION ALL
    SELECT x + 1 FROM generate_users WHERE x < 995
)
INSERT INTO users (name, email, age)
SELECT 
    'User_' || x,
    'user' || x || '@example.com',
    18 + (x % 50)
FROM generate_users;

-- Insert random order data (5000 orders)
WITH RECURSIVE generate_orders(x) AS (
    SELECT 1
    UNION ALL
    SELECT x + 1 FROM generate_orders WHERE x < 5000
)
INSERT INTO orders (user_id, product_name, quantity, price)
SELECT 
    1 + (abs(random()) % 1000),
    'Product_' || (abs(random()) % 100),
    1 + (abs(random()) % 10),
    10.0 + (abs(random()) % 990) * 0.1
FROM generate_orders;

.print ""
.print "=== Running Queries ==="
.print ""

-- Query 1: Count users
.print "Query 1: Count total users"
SELECT COUNT(*) as total_users FROM users;

.print ""
.print "Query 2: Count orders"
SELECT COUNT(*) as total_orders FROM orders;

.print ""
.print "Query 3: Users by age group"
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

.print ""
.print "Query 4: Top 10 users by order count"
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

.print ""
.print "Query 5: Top 10 products by revenue"
SELECT 
    product_name,
    COUNT(*) as times_ordered,
    SUM(quantity) as total_quantity,
    SUM(price * quantity) as total_revenue
FROM orders
GROUP BY product_name
ORDER BY total_revenue DESC
LIMIT 10;

.print ""
.print "Query 6: Average order value by user age group"
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

.print ""
.print "Query 7: Complex aggregation with window functions"
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

.print ""
.print "=== Demo Complete ==="
