USE retail_db.bronze;

CREATE OR REPLACE STREAM product_changes_stream ON TABLE product_raw_data
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM customer_changes_stream ON TABLE customer_raw_data
    APPEND_ONLY = TRUE;
    
CREATE OR REPLACE STREAM order_changes_stream ON TABLE order_raw_data
    APPEND_ONLY = TRUE;

SHOW STREAMS IN retail_db.bronze

    