-- Create Users Table
CREATE TABLE IF NOT EXISTS users (
    user_id INT,
    name STRING,
    email STRING,
    status STRING
);

-- Create Transactions Table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INT,
    user_id INT,
    amount DOUBLE,
    transaction_date TIMESTAMP
);

-- Query Active Users
SELECT * FROM users WHERE status = 'active';

-- Aggregate Transactions by User
SELECT user_id, SUM(amount) AS total_spent
FROM transactions
GROUP BY user_id
ORDER BY total_spent DESC;

-- Join Users and Transactions
SELECT u.user_id, u.name, t.transaction_id, t.amount, t.transaction_date
FROM users u
JOIN transactions t ON u.user_id = t.user_id
ORDER BY t.transaction_date DESC;

-- Filter Transactions in the Last 30 Days
SELECT * FROM transactions
WHERE transaction_date >= date_sub(current_date, 30);

-- Count Active Users with Transactions
SELECT COUNT(DISTINCT u.user_id) AS active_users_with_transactions
FROM users u
JOIN transactions t ON u.user_id = t.user_id
WHERE u.status = 'active';
