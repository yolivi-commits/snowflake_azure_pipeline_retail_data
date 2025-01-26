USE retail_db.gold;

-- Verification of data
SELECT * FROM SILVER.ORDERS;
SELECT * FROM SILVER.PRODUCT;
SELECT * FROM SILVER.CUSTOMER;
SELECT * FROM bronze.order_raw_data;
SELECT * FROM bronze.order_changes_stream;
DESC STREAM bronze.order_changes_stream;
SELECT * FROM retail_db.bronze.customer_raw_data;
INSERT INTO retail_db.bronze.customer_raw_data (customer_id, name, email, country, customer_type, registration_date)
VALUES (999, 'Test User', 'test@example.com', 'USA', 'Retail', '2025-01-26');
SELECT * FROM retail_db.bronze.customer_changes_stream;


CREATE OR REPLACE VIEW VW_DAILY_SALES_ANALYSIS AS
SELECT 
    o.transaction_date,
    p.product_id,
    p.name AS product_name,
    p.category AS product_category,
    c.customer_id,
    c.customer_type,
    SUM(o.quantity) AS total_quantity,
    SUM(o.total_amount) AS total_sales,
    COUNT(DISTINCT o.transaction_id) AS num_transactions,
    SUM(o.total_amount) / NULLIF(SUM(o.quantity), 0) AS avg_price_per_unit,
    SUM(o.total_amount) / NULLIF(COUNT(DISTINCT o.transaction_id), 0) AS avg_transaction_value
FROM SILVER.ORDERS o
JOIN SILVER.PRODUCT p ON o.product_id = p.product_id
JOIN SILVER.CUSTOMER c ON o.customer_id = c.customer_id
GROUP BY 
    o.transaction_date,
    p.product_id,
    p.name,
    p.category,
    c.customer_id,
    c.customer_type;

-- Checking Output
SELECT * FROM VW_DAILY_SALES_ANALYSIS;