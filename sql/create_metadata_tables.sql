-- Tạo các schema
-- Controller table 
CREATE TABLE IF NOT EXISTS controller (
    id SERIAL PRIMARY KEY,
    data_source TEXT NOT NULL,                    -- Ten bang/file nguon (input)          
    destination_table TEXT NOT NULL,              -- Ten bang dich (output)
    source_table TEXT NOT NULL,
    schema_name TEXT DEFAULT 'public',            -- Schema chua bang dich
    load_type TEXT NOT NULL,                      -- Loai load: full/incremental
    active BOOLEAN DEFAULT TRUE,                  -- Pipeline co hoat dong khong
    status TEXT DEFAULT 'PENDING',                -- Trang thai hien tai 
    description TEXT,                             -- Mo ta ve pipeline
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Audit table 
CREATE TABLE IF NOT EXISTS audit (
    audit_id SERIAL PRIMARY KEY,
    pipeline_id INT REFERENCES controller(id),    -- Link den bang Controller
    status VARCHAR(50) NOT NULL,                  -- 'success', 'failed', 'running'
    records_processed INT,                        -- So ban ghi da xu ly
    start_time TIMESTAMP,                         -- Thoi gian bat dau
    end_time TIMESTAMP,                           -- Thoi gian ket thuc
    error_message TEXT,                           -- Thong bao loi neu that bai
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO controller (data_source, destination_table, source_table, schema_name, load_type, description) 
VALUES
  ('customers', 'bro_customers','customers', 'bronze', 'full', 'Ingest raw customer data from S3'),
  ('orders', 'bro_orders', 'orders', 'bronze', 'full', 'Ingest raw order data from S3')
    

