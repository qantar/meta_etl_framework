-- ==========================================
-- sql/sample_data.sql
-- Source Database Sample Data
-- ==========================================

-- Create sample source schemas and tables
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS finance;

-- ==========================================
-- Customers Table
-- ==========================================
CREATE TABLE IF NOT EXISTS public.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    date_of_birth DATE,
    address_line1 VARCHAR(100),
    address_line2 VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50) DEFAULT 'USA',
    customer_segment VARCHAR(20) DEFAULT 'Standard',
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Trigger to update last_updated
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_customers_modtime 
    BEFORE UPDATE ON public.customers 
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- ==========================================
-- Products Table
-- ==========================================
CREATE TABLE IF NOT EXISTS public.products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    product_category VARCHAR(50),
    product_subcategory VARCHAR(50),
    brand VARCHAR(50),
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2),
    weight_kg DECIMAL(8,3),
    dimensions_cm VARCHAR(50),
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TRIGGER update_products_modtime 
    BEFORE UPDATE ON public.products 
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- ==========================================
-- Orders Table
-- ==========================================
CREATE TABLE IF NOT EXISTS sales.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES public.customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    order_status VARCHAR(20) DEFAULT 'Pending',
    total_amount DECIMAL(12,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    tax_amount DECIMAL(10,2) DEFAULT 0,
    shipping_amount DECIMAL(8,2) DEFAULT 0,
    payment_method VARCHAR(20),
    payment_status VARCHAR(20) DEFAULT 'Pending',
    shipping_address TEXT,
    billing_address TEXT,
    order_notes TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_by INTEGER
);

CREATE TRIGGER update_orders_modtime 
    BEFORE UPDATE ON sales.orders 
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- ==========================================
-- Order Items Table
-- ==========================================
CREATE TABLE IF NOT EXISTS sales.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES sales.orders(order_id),
    product_id INTEGER REFERENCES public.products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0,
    line_total DECIMAL(12,2) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- Employees Table
-- ==========================================
CREATE TABLE IF NOT EXISTS public.employees (
    employee_id SERIAL PRIMARY KEY,
    employee_number VARCHAR(20) UNIQUE NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    hire_date DATE NOT NULL,
    job_title VARCHAR(100),
    department VARCHAR(50),
    manager_id INTEGER REFERENCES public.employees(employee_id),
    salary DECIMAL(10,2),
    commission_rate DECIMAL(5,4),
    employment_status VARCHAR(20) DEFAULT 'Active',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_employees_modtime 
    BEFORE UPDATE ON public.employees 
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- ==========================================
-- Financial Transactions Table
-- ==========================================
CREATE TABLE IF NOT EXISTS finance.transactions (
    transaction_id SERIAL PRIMARY KEY,
    transaction_number VARCHAR(50) UNIQUE NOT NULL,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transaction_type VARCHAR(20) NOT NULL, -- 'Sale', 'Refund', 'Payment', etc.
    account_id VARCHAR(20),
    reference_id INTEGER, -- Could reference order_id, etc.
    reference_type VARCHAR(20), -- 'Order', 'Invoice', etc.
    amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    description TEXT,
    status VARCHAR(20) DEFAULT 'Pending',
    processed_by INTEGER REFERENCES public.employees(employee_id),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_transactions_modtime 
    BEFORE UPDATE ON finance.transactions 
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- ==========================================
-- Customer Analytics Table
-- ==========================================
CREATE TABLE IF NOT EXISTS analytics.customer_metrics (
    metric_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES public.customers(customer_id),
    metric_date DATE NOT NULL,
    total_orders INTEGER DEFAULT 0,
    total_spent DECIMAL(12,2) DEFAULT 0,
    avg_order_value DECIMAL(10,2) DEFAULT 0,
    days_since_last_order INTEGER,
    lifetime_value DECIMAL(15,2) DEFAULT 0,
    customer_score INTEGER, -- 1-100 scoring
    preferred_category VARCHAR(50),
    last_calculated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id, metric_date)
);

-- ==========================================
-- Product Performance Table
-- ==========================================
CREATE TABLE IF NOT EXISTS analytics.product_performance (
    performance_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES public.products(product_id),
    analysis_date DATE NOT NULL,
    units_sold INTEGER DEFAULT 0,
    revenue DECIMAL(12,2) DEFAULT 0,
    profit DECIMAL(12,2) DEFAULT 0,
    return_rate DECIMAL(5,2) DEFAULT 0,
    customer_rating DECIMAL(3,2),
    inventory_turns DECIMAL(8,2),
    market_share_percent DECIMAL(5,2),
    last_calculated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, analysis_date)
);

-- ==========================================
-- Insert Sample Data
-- ==========================================

-- Sample Customers
INSERT INTO public.customers (first_name, last_name, email, phone, city, state, customer_segment) VALUES
('John', 'Doe', 'john.doe@email.com', '555-0101', 'New York', 'NY', 'Premium'),
('Jane', 'Smith', 'jane.smith@email.com', '555-0102', 'Los Angeles', 'CA', 'Standard'),
('Bob', 'Johnson', 'bob.johnson@email.com', '555-0103', 'Chicago', 'IL', 'Standard'),
('Alice', 'Williams', 'alice.williams@email.com', '555-0104', 'Houston', 'TX', 'Premium'),
('Charlie', 'Brown', 'charlie.brown@email.com', '555-0105', 'Phoenix', 'AZ', 'Basic'),
('Diana', 'Davis', 'diana.davis@email.com', '555-0106', 'Philadelphia', 'PA', 'Standard'),
('Eve', 'Miller', 'eve.miller@email.com', '555-0107', 'San Antonio', 'TX', 'Premium'),
('Frank', 'Wilson', 'frank.wilson@email.com', '555-0108', 'San Diego', 'CA', 'Standard'),
('Grace', 'Moore', 'grace.moore@email.com', '555-0109', 'Dallas', 'TX', 'Basic'),
('Henry', 'Taylor', 'henry.taylor@email.com', '555-0110', 'San Jose', 'CA', 'Premium');

-- Sample Products
INSERT INTO public.products (product_name, product_category, brand, price, cost) VALUES
('Laptop Pro 15"', 'Electronics', 'TechBrand', 1299.99, 800.00),
('Wireless Headphones', 'Electronics', 'AudioMax', 199.99, 120.00),
('Coffee Maker Deluxe', 'Home & Kitchen', 'BrewMaster', 89.99, 45.00),
('Running Shoes', 'Sports', 'RunFast', 129.99, 65.00),
('Office Chair', 'Furniture', 'ComfortSeating', 299.99, 150.00),
('Smartphone X1', 'Electronics', 'MobileInc', 699.99, 400.00),
('Desk Lamp LED', 'Home & Kitchen', 'BrightLight', 49.99, 25.00),
('Fitness Tracker', 'Electronics', 'HealthTech', 249.99, 140.00),
('Backpack Pro', 'Accessories', 'TravelGear', 79.99, 35.00),
('Tablet 10"', 'Electronics', 'TechBrand', 399.99, 250.00);

-- Sample Employees
INSERT INTO public.employees (employee_number, first_name, last_name, email, hire_date, job_title, department, salary) VALUES
('EMP001', 'Sarah', 'Manager', 'sarah.manager@company.com', '2020-01-15', 'Sales Manager', 'Sales', 75000),
('EMP002', 'Mike', 'Sales', 'mike.sales@company.com', '2021-03-10', 'Sales Representative', 'Sales', 45000),
('EMP003', 'Lisa', 'Support', 'lisa.support@company.com', '2019-08-20', 'Customer Support', 'Support', 40000),
('EMP004', 'David', 'Tech', 'david.tech@company.com', '2020-11-05', 'Data Analyst', 'IT', 65000),
('EMP005', 'Emma', 'Finance', 'emma.finance@company.com', '2018-06-12', 'Financial Analyst', 'Finance', 70000);

-- Sample Orders
INSERT INTO sales.orders (customer_id, order_date, order_status, total_amount, payment_method, payment_status) VALUES
(1, '2024-01-15 10:30:00', 'Completed', 1299.99, 'Credit Card', 'Paid'),
(2, '2024-01-16 14:20:00', 'Completed', 289.98, 'PayPal', 'Paid'),
(3, '2024-01-17 09:15:00', 'Processing', 129.99, 'Credit Card', 'Paid'),
(4, '2024-01-18 16:45:00', 'Completed', 449.98, 'Credit Card', 'Paid'),
(5, '2024-01-19 11:30:00', 'Shipped', 79.99, 'Debit Card', 'Paid'),
(1, '2024-01-20 13:20:00', 'Completed', 699.99, 'Credit Card', 'Paid'),
(6, '2024-01-21 15:10:00', 'Processing', 349.98, 'PayPal', 'Paid'),
(7, '2024-01-22 08:45:00', 'Completed', 179.98, 'Credit Card', 'Paid'),
(8, '2024-01-23 12:30:00', 'Shipped', 249.99, 'Credit Card', 'Paid'),
(9, '2024-01-24 10:15:00', 'Pending', 399.99, 'Bank Transfer', 'Pending');

-- Sample Order Items
INSERT INTO sales.order_items (order_id, product_id, quantity, unit_price, line_total) VALUES
(1, 1, 1, 1299.99, 1299.99),
(2, 2, 1, 199.99, 199.99),
(2, 3, 1, 89.99, 89.99),
(3, 4, 1, 129.99, 129.99),
(4, 5, 1, 299.99, 299.99),
(4, 7, 3, 49.99, 149.97),
(5, 9, 1, 79.99, 79.99),
(6, 6, 1, 699.99, 699.99),
(7, 8, 1, 249.99, 249.99),
(7, 3, 1, 89.99, 89.99),
(8, 2, 1, 199.99, 199.99),
(8, 7, 1, 49.99, 49.99),
(9, 8, 1, 249.99, 249.99),
(10, 10, 1, 399.99, 399.99);

-- Sample Financial Transactions
INSERT INTO finance.transactions (transaction_number, transaction_type, reference_id, reference_type, amount, description, status, processed_by) VALUES
('TXN-001', 'Sale', 1, 'Order', 1299.99, 'Payment for Order #1', 'Completed', 1),
('TXN-002', 'Sale', 2, 'Order', 289.98, 'Payment for Order #2', 'Completed', 1),
('TXN-003', 'Sale', 3, 'Order', 129.99, 'Payment for Order #3', 'Completed', 2),
('TXN-004', 'Sale', 4, 'Order', 449.98, 'Payment for Order #4', 'Completed', 2),
('TXN-005', 'Sale', 5, 'Order', 79.99, 'Payment for Order #5', 'Completed', 1),
('TXN-006', 'Refund', 2, 'Order', -89.99, 'Refund for damaged item', 'Completed', 3),
('TXN-007', 'Sale', 6, 'Order', 699.99, 'Payment for Order #6', 'Completed', 2),
('TXN-008', 'Sale', 7, 'Order', 349.98, 'Payment for Order #7', 'Completed', 1),
('TXN-009', 'Sale', 8, 'Order', 179.98, 'Payment for Order #8', 'Completed', 2),
('TXN-010', 'Sale', 9, 'Order', 249.99, 'Payment for Order #9', 'Completed', 1);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_customers_email ON public.customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_last_updated ON public.customers(last_updated);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON sales.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON sales.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_last_updated ON sales.orders(last_updated);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON sales.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON sales.order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON finance.transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON finance.transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_products_category ON public.products(product_category);
CREATE INDEX IF NOT EXISTS idx_employees_department ON public.employees(department);

---

-- ==========================================
-- sql/target_init.sql
-- Target Database Initialization
-- ==========================================

-- Create target schemas for different data layers
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS mart;

-- Create staging tables (mirrors of source)
-- These will be populated by the ETL framework

-- Example staging table structure
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    date_of_birth DATE,
    address_line1 VARCHAR(100),
    address_line2 VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    customer_segment VARCHAR(20),
    registration_date TIMESTAMP,
    last_updated TIMESTAMP,
    is_active BOOLEAN,
    -- ETL metadata columns
    _ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _run_id UUID,
    _source_system VARCHAR(100)
);

-- DWH tables with SCD Type 2 support
CREATE TABLE IF NOT EXISTS dwh.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    full_address TEXT,
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    customer_segment VARCHAR(20),
    registration_date DATE,
    
    -- SCD Type 2 columns
    _valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _valid_to DATE DEFAULT '9999-12-31',
    _is_current BOOLEAN DEFAULT TRUE,
    _version INTEGER DEFAULT 1,
    
    -- ETL metadata
    _dwh_insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _ingestion_batch_id UUID,
    _source_row_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS dwh.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    product_subcategory VARCHAR(50),
    brand VARCHAR(50),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    margin_percent DECIMAL(5,2),
    weight_kg DECIMAL(8,3),
    
    -- SCD Type 2 columns
    _valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _valid_to DATE DEFAULT '9999-12-31',
    _is_current BOOLEAN DEFAULT TRUE,
    _version INTEGER DEFAULT 1,
    
    -- ETL metadata
    _dwh_insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _ingestion_batch_id UUID
);

CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    customer_key INTEGER REFERENCES dwh.dim_customers(customer_key),
    order_date DATE NOT NULL,
    order_year INTEGER,
    order_month INTEGER,
    order_quarter INTEGER,
    order_status VARCHAR(20),
    total_amount DECIMAL(12,2),
    discount_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    shipping_amount DECIMAL(8,2),
    net_amount DECIMAL(12,2),
    item_count INTEGER,
    payment_method VARCHAR(20),
    
    -- ETL metadata
    _dwh_insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _ingestion_batch_id UUID
);

-- Data mart tables for specific business needs
CREATE TABLE IF NOT EXISTS mart.customer_summary (
    customer_key INTEGER REFERENCES dwh.dim_customers(customer_key),
    customer_segment VARCHAR(20),
    total_orders INTEGER,
    total_spent DECIMAL(15,2),
    avg_order_value DECIMAL(10,2),
    first_order_date DATE,
    last_order_date DATE,
    days_since_last_order INTEGER,
    lifetime_value DECIMAL(15,2),
    customer_status VARCHAR(20),
    preferred_category VARCHAR(50),
    
    -- Refresh metadata
    _last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _data_date DATE DEFAULT CURRENT_DATE
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_dim_customers_id ON dwh.dim_customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customers_current ON dwh.dim_customers(_is_current);
CREATE INDEX IF NOT EXISTS idx_dim_products_id ON dwh.dim_products(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_products_current ON dwh.dim_products(_is_current);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON dwh.fact_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON dwh.fact_orders(customer_key);

-- Create views for easy access to current records
CREATE OR REPLACE VIEW dwh.current_customers AS
SELECT * FROM dwh.dim_customers WHERE _is_current = TRUE;

CREATE OR REPLACE VIEW dwh.current_products AS
SELECT * FROM dwh.dim_products WHERE _is_current = TRUE;

---

-- ==========================================
-- sql/metadata_ddl.sql (Enhanced)
-- Metadata Database Schema with Additional Tables
-- ==========================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ETL Control and Configuration Tables
CREATE TABLE IF NOT EXISTS etl_control_table (
    control_id SERIAL PRIMARY KEY,
    source_database VARCHAR(100) NOT NULL,
    source_schema VARCHAR(100) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    target_schema VARCHAR(100) DEFAULT 'staging',
    target_table VARCHAR(100),
    increment_column VARCHAR(100),
    business_key_columns TEXT[], -- Array of column names
    last_ingestion_time TIMESTAMP,
    is_scd_type_2_enabled BOOLEAN DEFAULT FALSE,
    staging_to_dwh_load_time TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    partition_column VARCHAR(100),
    custom_query TEXT,
    load_type VARCHAR(20) DEFAULT 'full', -- 'full', 'incremental', 'custom'
    priority INTEGER DEFAULT 50, -- 1-100, higher = more priority
    max_parallel_instances INTEGER DEFAULT 1,
    timeout_minutes INTEGER DEFAULT 60,
    retry_attempts INTEGER DEFAULT 3,
    notification_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system',
    tags TEXT[],
    UNIQUE(source_database, source_schema, source_table)
);

-- Comprehensive Audit Log
CREATE TABLE IF NOT EXISTS etl_audit_log (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_name VARCHAR(200) NOT NULL,
    run_id UUID NOT NULL,
    parent_run_id UUID, -- For hierarchical jobs
    source_table VARCHAR(300) NOT NULL,
    engine_type VARCHAR(20) DEFAULT 'python',
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    row_count BIGINT DEFAULT 0,
    rows_extracted BIGINT DEFAULT 0,
    rows_inserted BIGINT DEFAULT 0,
    rows_updated BIGINT DEFAULT 0,
    rows_deleted BIGINT DEFAULT 0,
    error_message TEXT,
    error_code VARCHAR(50),
    warning_count INTEGER DEFAULT 0,
    source_row_hash VARCHAR(64),
    target_row_hash VARCHAR(64),
    backup_path TEXT,
    ingestion_batch_id UUID,
    processing_duration_seconds INTEGER,
    data_size_mb DECIMAL(15,2),
    memory_usage_mb DECIMAL(10,2),
    cpu_usage_percent DECIMAL(5,2),
    triggered_by VARCHAR(100) DEFAULT 'scheduler',
    execution_host VARCHAR(100),
    spark_application_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enhanced Data Quality Metrics
CREATE TABLE IF NOT EXISTS etl_data_quality_metrics (
    quality_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES etl_audit_log(run_id),
    source_table VARCHAR(300) NOT NULL,
    layer VARCHAR(20) NOT NULL, -- 'bronze', 'silver', 'gold'
    total_records BIGINT,
    null_count_by_column JSONB,
    duplicate_count BIGINT,
    unique_count_by_column JSONB,
    data_type_violations JSONB,
    business_rule_violations JSONB,
    quality_score DECIMAL(5,2),
    quality_rules_applied JSONB,
    anomalies_detected JSONB,
    data_drift_score DECIMAL(5,2),
    completeness_score DECIMAL(5,2),
    validity_score DECIMAL(5,2),
    consistency_score DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Schema Evolution Tracking
CREATE TABLE IF NOT EXISTS table_schema_version (
    schema_version_id SERIAL PRIMARY KEY,
    source_database VARCHAR(100) NOT NULL,
    source_schema VARCHAR(100) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    schema_hash VARCHAR(64) NOT NULL,
    column_definitions JSONB NOT NULL,
    column_count INTEGER,
    version_number INTEGER NOT NULL,
    change_type VARCHAR(50), -- 'COLUMN_ADDED', 'COLUMN_REMOVED', etc.
    change_details JSONB,
    compatibility_level VARCHAR(20) DEFAULT 'COMPATIBLE', -- 'COMPATIBLE', 'BREAKING', 'WARNING'
    migration_required BOOLEAN DEFAULT FALSE,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    validated_by VARCHAR(100),
    approved_at TIMESTAMP,
    UNIQUE(source_database, source_schema, source_table, version_number)
);

-- Job Scheduling and Dependencies
CREATE TABLE IF NOT EXISTS etl_job_definitions (
    job_id VARCHAR(100) PRIMARY KEY,
    job_name VARCHAR(200) NOT NULL,
    description TEXT,
    job_type VARCHAR(50) DEFAULT 'etl', -- 'etl', 'quality_check', 'maintenance'
    engine_type VARCHAR(20) DEFAULT 'python',
    schedule_expression VARCHAR(100), -- Cron expression
    depends_on_jobs TEXT[], -- Array of job_ids
    configuration JSONB,
    is_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system'
);

-- Performance Metrics
CREATE TABLE IF NOT EXISTS etl_performance_metrics (
    metric_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES etl_audit_log(run_id),
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15,4),
    metric_unit VARCHAR(20),
    measurement_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tags JSONB
);

-- Error Catalog
CREATE TABLE IF NOT EXISTS etl_error_catalog (
    error_id SERIAL PRIMARY KEY,
    error_code VARCHAR(50) UNIQUE NOT NULL,
    error_category VARCHAR(50),
    error_description TEXT,
    resolution_steps TEXT,
    severity VARCHAR(20),
    auto_retry BOOLEAN DEFAULT FALSE,
    notification_required BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Configuration Management
CREATE TABLE IF NOT EXISTS etl_configuration (
    config_id SERIAL PRIMARY KEY,
    config_key VARCHAR(200) UNIQUE NOT NULL,
    config_value TEXT,
    config_type VARCHAR(50) DEFAULT 'string',
    environment VARCHAR(50) DEFAULT 'all',
    is_encrypted BOOLEAN DEFAULT FALSE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(100)
);

-- Data Lineage Tracking
CREATE TABLE IF NOT EXISTS etl_data_lineage (
    lineage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID REFERENCES etl_audit_log(run_id),
    source_table VARCHAR(300),
    target_table VARCHAR(300),
    transformation_type VARCHAR(50),
    transformation_logic TEXT,
    columns_mapping JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create comprehensive indexes
CREATE INDEX IF NOT EXISTS idx_audit_log_run_id ON etl_audit_log(run_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_source_table ON etl_audit_log(source_table);
CREATE INDEX IF NOT EXISTS idx_audit_log_start_time ON etl_audit_log(start_time);
CREATE INDEX IF NOT EXISTS idx_audit_log_status ON etl_audit_log(status);
CREATE INDEX IF NOT EXISTS idx_audit_log_engine_type ON etl_audit_log(engine_type);
CREATE INDEX IF NOT EXISTS idx_control_table_source ON etl_control_table(source_database, source_schema, source_table);
CREATE INDEX IF NOT EXISTS idx_control_table_active ON etl_control_table(is_active);
CREATE INDEX IF NOT EXISTS idx_schema_version_current ON table_schema_version(source_database, source_schema, source_table, is_current);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_run_id ON etl_data_quality_metrics(run_id);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_table ON etl_data_quality_metrics(source_table);
CREATE INDEX IF NOT EXISTS idx_performance_metrics_run_id ON etl_performance_metrics(run_id);
CREATE INDEX IF NOT EXISTS idx_data_lineage_run_id ON etl_data_lineage(run_id);

-- Create views for common queries
CREATE OR REPLACE VIEW etl_job_summary AS
SELECT 
    DATE(start_time) as execution_date,
    engine_type,
    status,
    COUNT(*) as job_count,
    SUM(row_count) as total_rows,
    AVG(processing_duration_seconds) as avg_duration_seconds,
    SUM(data_size_mb) as total_data_mb
FROM etl_audit_log 
WHERE start_time >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(start_time), engine_type, status
ORDER BY execution_date DESC, engine_type, status;

CREATE OR REPLACE VIEW etl_active_jobs AS
SELECT 
    ctl.source_database,
    ctl.source_schema,
    ctl.source_table,
    ctl.load_type,
    ctl.is_scd_type_2_enabled,
    ctl.last_ingestion_time,
    ctl.priority,
    COALESCE(latest_run.status, 'NEVER_RUN') as last_status,
    latest_run.end_time as last_execution,
    EXTRACT(EPOCH FROM (NOW() - ctl.last_ingestion_time))/3600 as hours_since_last_run
FROM etl_control_table ctl
LEFT JOIN LATERAL (
    SELECT status, end_time 
    FROM etl_audit_log 
    WHERE source_table = ctl.source_database||'.'||ctl.source_schema||'.'||ctl.source_table
    ORDER BY start_time DESC 
    LIMIT 1
) latest_run ON true
WHERE ctl.is_active = TRUE
ORDER BY ctl.priority DESC, ctl.last_ingestion_time ASC;