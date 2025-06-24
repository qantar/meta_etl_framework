# From current code:
#- class MetadataManager (entire class)
#- init_metadata_tables() method
#- get_active_jobs() method
#- create_job_execution() method
#- update_job_execution() method
#- log_data_quality() method

## Additional methods to add:
#- get_job_dependencies()
#- update_job_schedule()
#- archive_old_executions()
#- get_execution_statistics()

import json
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum

import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import delta
from delta.tables import DeltaTable

class MetadataManager:
    """Manages ETL metadata in PostgreSQL"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.init_metadata_tables()
    
    def get_connection(self):
        return psycopg2.connect(self.connection_string)
    
    def init_metadata_tables(self):
        """Initialize metadata tables"""
        
        create_tables_sql = """
        -- Data source connections
        CREATE TABLE IF NOT EXISTS etl_connections (
            connection_id SERIAL PRIMARY KEY,
            connection_name VARCHAR(100) UNIQUE NOT NULL,
            source_type VARCHAR(20) NOT NULL,
            host VARCHAR(255),
            port INTEGER,
            database_name VARCHAR(100),
            username VARCHAR(100),
            password_encrypted TEXT,
            file_path TEXT,
            file_format VARCHAR(20),
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- ETL job configurations
        CREATE TABLE IF NOT EXISTS etl_jobs (
            job_id SERIAL PRIMARY KEY,
            job_name VARCHAR(100) UNIQUE NOT NULL,
            connection_id INTEGER REFERENCES etl_connections(connection_id),
            source_schema VARCHAR(100),
            source_table VARCHAR(100),
            target_layer VARCHAR(20) NOT NULL,
            target_table VARCHAR(100) NOT NULL,
            load_type VARCHAR(20) DEFAULT 'full',
            delta_column VARCHAR(100),
            business_key VARCHAR(100),
            partition_columns TEXT[], -- Array of column names
            transformation_rules JSONB,
            is_active BOOLEAN DEFAULT true,
            schedule_cron VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Job execution audit
        CREATE TABLE IF NOT EXISTS etl_job_executions (
            execution_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_id INTEGER REFERENCES etl_jobs(job_id),
            status VARCHAR(20) DEFAULT 'pending',
            start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_time TIMESTAMP,
            records_processed INTEGER DEFAULT 0,
            records_inserted INTEGER DEFAULT 0,
            records_updated INTEGER DEFAULT 0,
            records_deleted INTEGER DEFAULT 0,
            error_message TEXT,
            execution_metadata JSONB
        );
        
        -- Data quality metrics
        CREATE TABLE IF NOT EXISTS etl_data_quality (
            quality_id SERIAL PRIMARY KEY,
            execution_id UUID REFERENCES etl_job_executions(execution_id),
            table_name VARCHAR(100),
            layer VARCHAR(20),
            total_records INTEGER,
            null_records INTEGER,
            duplicate_records INTEGER,
            quality_score DECIMAL(5,2),
            quality_checks JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_job_executions_job_id ON etl_job_executions(job_id);
        CREATE INDEX IF NOT EXISTS idx_job_executions_status ON etl_job_executions(status);
        CREATE INDEX IF NOT EXISTS idx_job_executions_start_time ON etl_job_executions(start_time);
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_tables_sql)
                conn.commit()
    
    def get_active_jobs(self) -> List[Dict]:
        """Get all active ETL jobs with their configurations"""
        
        query = """
        SELECT 
            j.job_id, j.job_name, j.source_schema, j.source_table,
            j.target_layer, j.target_table, j.load_type, j.delta_column,
            j.business_key, j.partition_columns, j.transformation_rules,
            c.connection_name, c.source_type, c.host, c.port, c.database_name,
            c.username, c.password_encrypted, c.file_path, c.file_format
        FROM etl_jobs j
        JOIN etl_connections c ON j.connection_id = c.connection_id
        WHERE j.is_active = true AND c.is_active = true
        ORDER BY j.job_id
        """
        
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query)
                return [dict(row) for row in cursor.fetchall()]
    
    def create_job_execution(self, job_id: int) -> str:
        """Create new job execution record"""
        
        query = """
        INSERT INTO etl_job_executions (job_id, status, start_time)
        VALUES (%s, %s, %s)
        RETURNING execution_id
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (job_id, JobStatus.RUNNING.value, datetime.now()))
                execution_id = cursor.fetchone()[0]
                conn.commit()
                return str(execution_id)
    
    def update_job_execution(self, execution_id: str, status: str, 
                           records_processed: int = 0, error_message: str = None,
                           metadata: Dict = None):
        """Update job execution status"""
        
        query = """
        UPDATE etl_job_executions 
        SET status = %s, end_time = %s, records_processed = %s, 
            error_message = %s, execution_metadata = %s
        WHERE execution_id = %s
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (
                    status, datetime.now(), records_processed, 
                    error_message, json.dumps(metadata) if metadata else None,
                    execution_id
                ))
                conn.commit()
    
    def log_data_quality(self, execution_id: str, table_name: str, layer: str,
                        quality_metrics: Dict):
        """Log data quality metrics"""
        
        query = """
        INSERT INTO etl_data_quality 
        (execution_id, table_name, layer, total_records, null_records, 
         duplicate_records, quality_score, quality_checks)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (
                    execution_id, table_name, layer,
                    quality_metrics.get('total_records', 0),
                    quality_metrics.get('null_records', 0),
                    quality_metrics.get('duplicate_records', 0),
                    quality_metrics.get('quality_score', 0.0),
                    json.dumps(quality_metrics.get('checks', {}))
                ))
                conn.commit()

