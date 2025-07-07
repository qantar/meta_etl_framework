-- =====================================================
-- SQL-Based Metadata-Driven ETL Pipeline
-- Pure PostgreSQL implementation with stored procedures
-- =====================================================

-- 1. CREATE STAGING SCHEMA FOR RAW DATA
-- =====================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- 2. UTILITY FUNCTIONS
-- ===================

-- Function to generate backup path
CREATE OR REPLACE FUNCTION generate_backup_path(
    p_database TEXT,
    p_schema TEXT,
    p_table TEXT
) RETURNS TEXT AS $$
DECLARE
    timestamp_str TEXT;
BEGIN
    SELECT TO_CHAR(NOW(), 'YYYYMMDD_HH24MISS') INTO timestamp_str;
    RETURN '/loaded_data_back_up/' || p_database || '/' || p_schema || '/' || p_table || '/' || timestamp_str || '/';
END;
$$ LANGUAGE plpgsql;

-- Function to calculate data hash for consistency
CREATE OR REPLACE FUNCTION calculate_data_hash(
    p_table_name TEXT,
    p_schema_name TEXT DEFAULT 'staging'
) RETURNS TEXT AS $$
DECLARE
    hash_value TEXT;
    sql_query TEXT;
BEGIN
    sql_query := FORMAT('SELECT MD5(STRING_AGG(md5_row, '''')) FROM (
        SELECT MD5(ROW(%s.*)::TEXT) as md5_row 
        FROM %I.%I 
        ORDER BY 1
    ) t', p_schema_name, p_schema_name, p_table_name);
    
    EXECUTE sql_query INTO hash_value;
    RETURN COALESCE(hash_value, '');
END;
$$ LANGUAGE plpgsql;

-- 3. MAIN INGESTION STORED PROCEDURE
-- ==================================

CREATE OR REPLACE FUNCTION ingest_table_to_staging(
    p_source_database TEXT,
    p_source_schema TEXT,
    p_source_table TEXT,
    p_connection_string TEXT DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    v_run_id UUID;
    v_job_name TEXT;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_increment_column TEXT;
    v_last_watermark TEXT;
    v_staging_table TEXT;
    v_backup_path TEXT;
    v_rows_extracted BIGINT := 0;
    v_rows_loaded BIGINT := 0;
    v_source_hash TEXT;
    v_error_message TEXT;
    v_sql_query TEXT;
    v_where_clause TEXT := '';
    v_control_record RECORD;
BEGIN
    -- Initialize run
    v_run_id := gen_random_uuid();
    v_job_name := 'ingest_' || p_source_table || '_staging';
    v_start_time := NOW();
    v_staging_table := 'staging.' || p_source_table;
    v_backup_path := generate_backup_path(p_source_database, p_source_schema, p_source_table);
    
    -- Get control table configuration
    SELECT increment_column, business_key_columns, is_scd_type_2_enabled, custom_query
    INTO v_control_record
    FROM etl_control_table 
    WHERE source_database = p_source_database 
    AND source_schema = p_source_schema 
    AND source_table = p_source_table
    AND is_active = TRUE;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table % not found in control table or is inactive', 
            p_source_database || '.' || p_source_schema || '.' || p_source_table;
    END IF;
    
    -- Start audit log
    INSERT INTO etl_audit_log (run_id, job_name, source_table, start_time, status)
    VALUES (v_run_id, v_job_name, p_source_database||'.'||p_source_schema||'.'||p_source_table, v_start_time, 'RUNNING');
    
    BEGIN
        -- Get last watermark for incremental loads
        IF v_control_record.increment_column IS NOT NULL THEN
            SELECT last_value INTO v_last_watermark
            FROM etl_watermarks 
            WHERE source_database = p_source_database 
            AND source_schema = p_source_schema 
            AND source_table = p_source_table 
            AND watermark_column = v_control_record.increment_column;
            
            IF v_last_watermark IS NOT NULL THEN
                v_where_clause := FORMAT(' WHERE %I > %L', v_control_record.increment_column, v_last_watermark);
            END IF;
        END IF;
        
        -- Build extraction query
        IF v_control_record.custom_query IS NOT NULL THEN
            v_sql_query := v_control_record.custom_query;
        ELSE
            v_sql_query := FORMAT('SELECT * FROM %I.%I.%I%s', 
                p_source_database, p_source_schema, p_source_table, v_where_clause);
        END IF;
        
        -- Create staging table if not exists (mirror source structure)
        -- Note: In real implementation, this would use dblink or foreign data wrapper
        v_sql_query := FORMAT('
            CREATE TABLE IF NOT EXISTS %s AS 
            SELECT * FROM %I.%I.%I WHERE 1=0', 
            v_staging_table, p_source_database, p_source_schema, p_source_table);
        EXECUTE v_sql_query;
        
        -- Add metadata columns to staging table if not exist
        BEGIN
            ALTER TABLE staging.tmp_table ADD COLUMN IF NOT EXISTS _ingestion_timestamp TIMESTAMP DEFAULT NOW();
            ALTER TABLE staging.tmp_table ADD COLUMN IF NOT EXISTS _run_id UUID;
            ALTER TABLE staging.tmp_table ADD COLUMN IF NOT EXISTS _source_system TEXT;
        EXCEPTION WHEN OTHERS THEN
            -- Columns already exist
            NULL;
        END;
        
        -- Extract data (simulated - in real implementation would use dblink/fdw)
        v_sql_query := FORMAT('
            INSERT INTO %s 
            SELECT *, NOW(), %L, %L 
            FROM external_source_simulation(%L, %L, %L, %L)', 
            v_staging_table, v_run_id, p_source_database||'.'||p_source_schema||'.'||p_source_table,
            p_source_database, p_source_schema, p_source_table, v_where_clause);
        
        -- Get row counts
        EXECUTE FORMAT('SELECT COUNT(*) FROM %s WHERE _run_id = %L', v_staging_table, v_run_id) INTO v_rows_loaded;
        v_rows_extracted := v_rows_loaded;
        
        -- Calculate source hash
        v_source_hash := calculate_data_hash(p_source_table, 'staging');
        
        -- Update watermark if incremental
        IF v_control_record.increment_column IS NOT NULL AND v_rows_loaded > 0 THEN
            EXECUTE FORMAT('SELECT MAX(%I) FROM %s WHERE _run_id = %L', 
                v_control_record.increment_column, v_staging_table, v_run_id) INTO v_last_watermark;
            
            INSERT INTO etl_watermarks (source_database, source_schema, source_table, watermark_column, last_value)
            VALUES (p_source_database, p_source_schema, p_source_table, v_control_record.increment_column, v_last_watermark)
            ON CONFLICT (source_database, source_schema, source_table, watermark_column)
            DO UPDATE SET last_value = EXCLUDED.last_value, last_updated = NOW();
        END IF;
        
        v_end_time := NOW();
        
        -- Complete audit log
        UPDATE etl_audit_log SET
            end_time = v_end_time,
            row_count = v_rows_loaded,
            rows_extracted = v_rows_extracted,
            rows_loaded = v_rows_loaded,
            status = 'SUCCESS',
            source_row_hash = v_source_hash,
            backup_path = v_backup_path,
            processing_duration_seconds = EXTRACT(EPOCH FROM (v_end_time - v_start_time))
        WHERE run_id = v_run_id;
        
        -- Update control table
        UPDATE etl_control_table SET
            last_ingestion_time = v_end_time,
            updated_at = NOW()
        WHERE source_database = p_source_database 
        AND source_schema = p_source_schema 
        AND source_table = p_source_table;
        
        RAISE NOTICE 'Successfully ingested % rows from %.%.% to staging', 
            v_rows_loaded, p_source_database, p_source_schema, p_source_table;
            
    EXCEPTION WHEN OTHERS THEN
        v_error_message := SQLERRM;
        v_end_time := NOW();
        
        -- Log error
        UPDATE etl_audit_log SET
            end_time = v_end_time,
            status = 'FAILED',
            error_message = v_error_message,
            processing_duration_seconds = EXTRACT(EPOCH FROM (v_end_time - v_start_time))
        WHERE run_id = v_run_id;
        
        RAISE EXCEPTION 'Ingestion failed for %.%.%: %', p_source_database, p_source_schema, p_source_table, v_error_message;
    END;
    
    RETURN v_run_id;
END;
$$ LANGUAGE plpgsql;

-- 4. STAGING TO DWH LOAD WITH SCD TYPE 2
-- ======================================

CREATE OR REPLACE FUNCTION load_staging_to_dwh_scd2(
    p_source_database TEXT,
    p_source_schema TEXT,
    p_source_table TEXT
) RETURNS UUID AS $$
DECLARE
    v_run_id UUID;
    v_job_name TEXT;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_staging_table TEXT;
    v_dwh_table TEXT;
    v_business_keys TEXT[];
    v_business_key_condition TEXT;
    v_rows_processed BIGINT := 0;
    v_control_record RECORD;
    v_batch_id UUID;
BEGIN
    v_run_id := gen_random_uuid();
    v_job_name := 'load_' || p_source_table || '_dwh_scd2';
    v_start_time := NOW();
    v_staging_table := 'staging.' || p_source_table;
    v_dwh_table := 'dwh.' || p_source_table;
    v_batch_id := gen_random_uuid();
    
    -- Get control configuration
    SELECT business_key_columns, is_scd_type_2_enabled
    INTO v_control_record
    FROM etl_control_table 
    WHERE source_database = p_source_database 
    AND source_schema = p_source_schema 
    AND source_table = p_source_table
    AND is_active = TRUE;
    
    -- Start audit
    INSERT INTO etl_audit_log (run_id, job_name, source_table, start_time, status)
    VALUES (v_run_id, v_job_name, p_source_database||'.'||p_source_schema||'.'||p_source_table, v_start_time, 'RUNNING');
    
    BEGIN
        -- Create DWH table if not exists
        EXECUTE FORMAT('
            CREATE TABLE IF NOT EXISTS %s AS 
            SELECT *, 
                NOW() as _dwh_insert_time,
                %L as _ingestion_batch_id,
                NOW() as _valid_from,
                ''9999-12-31''::DATE as _valid_to,
                TRUE as _is_current,
                1 as _version
            FROM %s WHERE 1=0', 
            v_dwh_table, v_batch_id, v_staging_table);
        
        -- Add SCD Type 2 columns if not exist
        BEGIN
            EXECUTE FORMAT('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _dwh_insert_time TIMESTAMP DEFAULT NOW()', v_dwh_table);
            EXECUTE FORMAT('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _ingestion_batch_id UUID', v_dwh_table);
            EXECUTE FORMAT('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _valid_from TIMESTAMP DEFAULT NOW()', v_dwh_table);
            EXECUTE FORMAT('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _valid_to DATE DEFAULT ''9999-12-31''', v_dwh_table);
            EXECUTE FORMAT('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _is_current BOOLEAN DEFAULT TRUE', v_dwh_table);
            EXECUTE FORMAT('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _version INTEGER DEFAULT 1', v_dwh_table);
        EXCEPTION WHEN OTHERS THEN
            NULL; -- Columns already exist
        END;
        
        IF v_control_record.is_scd_type_2_enabled AND v_control_record.business_key_columns IS NOT NULL THEN
            -- SCD Type 2 Logic
            v_business_keys := v_control_record.business_key_columns;
            
            -- Build business key join condition
            SELECT STRING_AGG(FORMAT('src.%I = tgt.%I', col, col), ' AND ')
            INTO v_business_key_condition
            FROM UNNEST(v_business_keys) AS col;
            
            -- Mark current records as historical where data changed
            EXECUTE FORMAT('
                UPDATE %s tgt SET 
                    _valid_to = CURRENT_DATE - INTERVAL ''1 day'',
                    _is_current = FALSE
                FROM %s src
                WHERE %s
                AND tgt._is_current = TRUE
                AND (
                    SELECT MD5(ROW(src.*)::TEXT) != MD5(ROW(tgt.*)::TEXT)
                    OR tgt._valid_from IS NULL
                )', 
                v_dwh_table, v_staging_table, v_business_key_condition);
            
            -- Insert new/changed records
            EXECUTE FORMAT('
                INSERT INTO %s 
                SELECT src.*, 
                    NOW() as _dwh_insert_time,
                    %L as _ingestion_batch_id,
                    NOW() as _valid_from,
                    ''9999-12-31''::DATE as _valid_to,
                    TRUE as _is_current,
                    COALESCE((
                        SELECT MAX(_version) + 1 
                        FROM %s tgt 
                        WHERE %s
                    ), 1) as _version
                FROM %s src
                WHERE NOT EXISTS (
                    SELECT 1 FROM %s tgt 
                    WHERE %s 
                    AND tgt._is_current = TRUE
                    AND MD5(ROW(src.*)::TEXT) = MD5(ROW(tgt.*)::TEXT)
                )', 
                v_dwh_table, v_batch_id, v_dwh_table, v_business_key_condition,
                v_staging_table, v_dwh_table, v_business_key_condition);
        ELSE
            -- Simple upsert (SCD Type 1 or full load)
            IF v_control_record.business_key_columns IS NOT NULL THEN
                -- Upsert based on business keys
                v_business_keys := v_control_record.business_key_columns;
                
                SELECT STRING_AGG(FORMAT('src.%I = tgt.%I', col, col), ' AND ')
                INTO v_business_key_condition
                FROM UNNEST(v_business_keys) AS col;
                
                -- Delete existing records
                EXECUTE FORMAT('
                    DELETE FROM %s tgt
                    WHERE EXISTS (
                        SELECT 1 FROM %s src WHERE %s
                    )', 
                    v_dwh_table, v_staging_table, v_business_key_condition);
            ELSE
                -- Full refresh
                EXECUTE FORMAT('TRUNCATE TABLE %s', v_dwh_table);
            END IF;
            
            -- Insert all staging records
            EXECUTE FORMAT('
                INSERT INTO %s 
                SELECT *, 
                    NOW() as _dwh_insert_time,
                    %L as _ingestion_batch_id,
                    NOW() as _valid_from,
                    ''9999-12-31''::DATE as _valid_to,
                    TRUE as _is_current,
                    1 as _version
                FROM %s', 
                v_dwh_table, v_batch_id, v_staging_table);
        END IF;
        
        -- Get processed row count
        EXECUTE FORMAT('SELECT COUNT(*) FROM %s WHERE _ingestion_batch_id = %L', v_dwh_table, v_batch_id) INTO v_rows_processed;
        
        v_end_time := NOW();
        
        -- Complete audit
        UPDATE etl_audit_log SET
            end_time = v_end_time,
            row_count = v_rows_processed,
            rows_loaded = v_rows_processed,
            status = 'SUCCESS',
            processing_duration_seconds = EXTRACT(EPOCH FROM (v_end_time - v_start_time))
        WHERE run_id = v_run_id;
        
        -- Update control table
        UPDATE etl_control_table SET
            staging_to_dwh_load_time = v_end_time,
            updated_at = NOW()
        WHERE source_database = p_source_database 
        AND source_schema = p_source_schema 
        AND source_table = p_source_table;
        
        RAISE NOTICE 'Successfully loaded % rows from staging to DWH for %.%.%', 
            v_rows_processed, p_source_database, p_source_schema, p_source_table;
            
    EXCEPTION WHEN OTHERS THEN
        UPDATE etl_audit_log SET
            end_time = NOW(),
            status = 'FAILED',
            error_message = SQLERRM,
            processing_duration_seconds = EXTRACT(EPOCH FROM (NOW() - v_start_time))
        WHERE run_id = v_run_id;
        
        RAISE EXCEPTION 'DWH load failed for %.%.%: %', p_source_database, p_source_schema, p_source_table, SQLERRM;
    END;
    
    RETURN v_run_id;
END;
$$ LANGUAGE plpgsql;

-- 5. COMPLETE PIPELINE ORCHESTRATOR
-- =================================

CREATE OR REPLACE FUNCTION run_complete_etl_pipeline(
    p_source_database TEXT DEFAULT NULL,
    p_source_schema TEXT DEFAULT NULL,
    p_source_table TEXT DEFAULT NULL
) RETURNS TABLE(
    table_name TEXT,
    staging_run_id UUID,
    dwh_run_id UUID,
    total_duration_seconds INTEGER,
    status TEXT
) AS $$
DECLARE
    table_rec RECORD;
    v_staging_run_id UUID;
    v_dwh_run_id UUID;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_status TEXT;
BEGIN
    FOR table_rec IN 
        SELECT source_database, source_schema, source_table
        FROM etl_control_table 
        WHERE is_active = TRUE
        AND (p_source_database IS NULL OR source_database = p_source_database)
        AND (p_source_schema IS NULL OR source_schema = p_source_schema)
        AND (p_source_table IS NULL OR source_table = p_source_table)
        ORDER BY source_database, source_schema, source_table
    LOOP
        v_start_time := NOW();
        v_status := 'SUCCESS';
        
        BEGIN
            -- Stage 1: Ingest to staging
            v_staging_run_id := ingest_table_to_staging(
                table_rec.source_database, 
                table_rec.source_schema, 
                table_rec.source_table
            );
            
            -- Stage 2: Load to DWH
            v_dwh_run_id := load_staging_to_dwh_scd2(
                table_rec.source_database, 
                table_rec.source_schema, 
                table_rec.source_table
            );
            
        EXCEPTION WHEN OTHERS THEN
            v_status := 'FAILED';
            RAISE NOTICE 'Pipeline failed for %.%.%: %', 
                table_rec.source_database, table_rec.source_schema, table_rec.source_table, SQLERRM;
        END;
        
        v_end_time := NOW();
        
        -- Return results
        table_name := table_rec.source_database || '.' || table_rec.source_schema || '.' || table_rec.source_table;
        staging_run_id := v_staging_run_id;
        dwh_run_id := v_dwh_run_id;
        total_duration_seconds := EXTRACT(EPOCH FROM (v_end_time - v_start_time));
        status := v_status;
        
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 6. DATA QUALITY CHECKS
-- ======================

CREATE OR REPLACE FUNCTION validate_data_quality(
    p_schema_name TEXT,
    p_table_name TEXT,
    p_run_id UUID
) RETURNS JSONB AS $$
DECLARE
    v_total_records BIGINT;
    v_null_counts JSONB := '{}';
    v_duplicate_count BIGINT;
    v_quality_score DECIMAL(5,2);
    v_column_rec RECORD;
    v_null_count BIGINT;
    v_unique_count BIGINT;
    v_quality_metrics JSONB;
BEGIN
    -- Get total record count
    EXECUTE FORMAT('SELECT COUNT(*) FROM %I.%I', p_schema_name, p_table_name) INTO v_total_records;
    
    -- Check nulls for each column
    FOR v_column_rec IN 
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = p_schema_name 
        AND table_name = p_table_name
        AND column_name NOT LIKE '\_%'  -- Skip metadata columns
    LOOP
        EXECUTE FORMAT('SELECT COUNT(*) FROM %I.%I WHERE %I IS NULL', 
            p_schema_name, p_table_name, v_column_rec.column_name) INTO v_null_count;
        
        v_null_counts := v_null_counts || jsonb_build_object(v_column_rec.column_name, v_null_count);
    END LOOP;
    
    -- Check duplicates (excluding metadata columns)
    EXECUTE FORMAT('
        SELECT COUNT(*) - COUNT(DISTINCT %s) 
        FROM %I.%I',
        (SELECT STRING_AGG(column_name, ', ') 
         FROM information_schema.columns 
         WHERE table_schema = p_schema_name AND table_name = p_table_name 
         AND column_name NOT LIKE '\_%'),
        p_schema_name, p_table_name
    ) INTO v_duplicate_count;
    
    -- Calculate quality score (simple algorithm)
    v_quality_score := CASE 
        WHEN v_total_records = 0 THEN 0
        ELSE GREATEST(0, 100 - 
            (SELECT SUM((value::INTEGER * 100.0 / v_total_records)) FROM jsonb_each_text(v_null_counts)) -
            (v_duplicate_count * 100.0 / v_total_records)
        )
    END;
    
    -- Build quality metrics
    v_quality_metrics := jsonb_build_object(
        'total_records', v_total_records,
        'null_count_by_column', v_null_counts,
        'duplicate_count', v_duplicate_count,
        'quality_score', v_quality_score,
        'quality_rules_applied', jsonb_build_object(
            'null_check', true,
            'duplicate_check', true,
            'completeness_check', true
        )
    );
    
    -- Log quality metrics
    INSERT INTO etl_data_quality_metrics (run_id, source_table, total_records, null_count_by_column, 
                                         duplicate_count, quality_score, quality_rules_applied)
    VALUES (p_run_id, p_schema_name || '.' || p_table_name, v_total_records, v_null_counts, 
            v_duplicate_count, v_quality_score, v_quality_metrics->'quality_rules_applied');
    
    RETURN v_quality_metrics;
END;
$$ LANGUAGE plpgsql;

-- 7. SAMPLE USAGE EXAMPLES
-- ========================

-- Register tables for ingestion
INSERT INTO etl_control_table (source_database, source_schema, source_table, increment_column, business_key_columns, is_scd_type_2_enabled)
VALUES 
    ('source_db', 'public', 'customers', 'updated_at', ARRAY['customer_id'], TRUE),
    ('source_db', 'public', 'orders', 'order_date', ARRAY['order_id'], FALSE),
    ('source_db', 'sales', 'products', NULL, ARRAY['product_id'], TRUE);

-- Run complete pipeline for all tables
SELECT * FROM run_complete_etl_pipeline();

-- Run pipeline for specific table
SELECT * FROM run_complete_etl_pipeline('source_db', 'public', 'customers');

-- Check recent ingestion statistics
SELECT 
    DATE(start_time) as ingestion_date,
    status,
    COUNT(*) as job_count,
    SUM(row_count) as total_rows,
    AVG(processing_duration_seconds) as avg_duration
FROM etl_audit_log 
WHERE start_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(start_time), status
ORDER BY ingestion_date DESC;