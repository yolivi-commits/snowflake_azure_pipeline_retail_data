CREATE SCHEMA IF NOT EXISTS GOLD;

-- Using Bronze 
USE retail_db.bronze;

-- Confirming Task Execution 
SELECT * FROM customer_raw_data;
SHOW TASKS;

-- Manual Task Activation Loading Data
EXECUTE TASK LOAD_CUSTOMER_DATA_TASK;
EXECUTE TASK LOAD_PRODUCT_DATA_TASK;
EXECUTE TASK LOAD_ORDER_DATA_TASK;

-- Confirming data post Task Activation
SELECT * FROM customer_raw_data;
SELECT * FROM product_raw_data;
SELECT * FROM order_raw_data;

-- Checking Streams
SHOW STREAMS;
DESC STREAM CUSTOMER_CHANGES_STREAM;
SELECT * FROM CUSTOMER_CHANGES_STREAM;


-- Changing to Silver Schema
USE retail_db.silver;

SHOW TASKS;

-- Manual Task Activation Merging Data
EXECUTE TASK ORDER_SILVER_MERGING_TASK;
EXECUTE TASK PRODUCT_SILVER_MERGING_TASK;
EXECUTE TASK SILVER_CUSTOMER_MERGING_TASK;

-- Confirming data post Silver Task Activation
SELECT * FROM customer;
SELECT * FROM product;
SELECT * FROM orders;

DESC STREAM bronze.order_changes_stream;
DESC TABLE silver.orders;
SELECT * FROM silver.orders;




