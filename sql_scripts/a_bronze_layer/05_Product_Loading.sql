-- Creating File format
CREATE OR REPLACE FILE FORMAT json_file_format
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE
    IGNORE_UTF8_ERRORS = TRUE;

-- Verification of File Format
SELECT 
    $1
FROM @adls_stage/Product/
    (FILE_FORMAT => json_file_format)
LIMIT 10;

-- Product Table Creation
CREATE TABLE IF NOT EXISTS product_raw_data (
    product_id INT,
    name STRING,
    category STRING,
    brand STRING,
    price FLOAT,
    stock_quantity INT,
    rating FLOAT,
    is_active BOOLEAN,
    source_file_name STRING,
    source_file_row_number INT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Task
CREATE OR REPLACE TASK load_product_data_task
    WAREHOUSE = compute_wh
    SCHEDULE = 'USING CRON 0 3 * * * America/New_York'
AS
    COPY INTO product_raw_data (
        product_id,
        name,
        category,
        brand,
        price,
        stock_quantity ,
        rating ,
        is_active ,
        source_file_name,
        source_file_row_number
    )
    FROM (
        SELECT
            $1:product_id::INT,
            $1:name::STRING,
            $1:category::STRING,
            $1:brand::STRING,
            $1:price::FLOAT,
            $1:stock_quantity::INT,
            $1:rating::FLOAT,
            $1:is_active::BOOLEAN,
            metadata$filename,
            metadata$file_row_number
        FROM @adls_stage/Product/
    )
    FILE_FORMAT = (FORMAT_NAME = 'json_file_format')
    ON_ERROR = 'CONTINUE'
    PATTERN = '.*[.]json';

-- Starting the task
alter task load_product_data_task resume